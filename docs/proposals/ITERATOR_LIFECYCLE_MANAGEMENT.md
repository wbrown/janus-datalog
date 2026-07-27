# Proposal: Iterator Lifecycle Management

**Status**: Not Implemented (documenting options for future consideration)
**Date**: February 2026
**Related**: `BUG_ITERATOR_LEAK_BUILTIN_EVALUATION.md` — missing `iter.Close()` in two executor functions

## Problem

The `executor.Iterator` interface has a `Close() error` method that releases underlying resources. When the iterator chain is backed by a `BadgerIterator`, `Close()` calls `it.Close()` + `txn.Discard()` on the BadgerDB transaction. Failure to call `Close()` leaves the transaction's skiplist ref count elevated, preventing WAL file cleanup during `db.Close()`.

This manifested as a concrete bug: two functions in `executor/helpers.go` called `rel.Iterator()` without `iter.Close()`, leaking 256 MB WAL files per query. The fix is two `defer iter.Close()` lines.

But the pattern is fragile. Every consumer of `Relation.Iterator()` must remember to close, and nothing in the type system or runtime enforces it. A comprehensive audit found ~70 call sites for `.Iterator()` across the executor and storage packages. Most close correctly. Some rely on wrapper iterators propagating `Close()` down the chain. The current count of missing closes is small, but the pattern invites future regressions.

### Scope of the Risk

The high-level `db` package API (`Query`, `QueryInto`, `Pull`, `GetString`, etc.) fully materializes results internally. External consumers using these methods never touch iterators and cannot leak.

The risk is in two places:

1. **Internal executor/storage code** — any new function that iterates a Relation and forgets `iter.Close()`. This is where the current bug lives.

2. **Advanced API users** — `db.Unwrap()` returns the raw `*storage.Database`, which exposes `ExecuteQueryRelation()` and `Match()`. Both return `executor.Relation`. If someone calls `rel.Iterator()` without `Close()`, they leak. The `executor.Iterator` interface documents `Close()`, but there's no compile-time enforcement.

## Option A: Finalizer Safety Net

Add `runtime.SetFinalizer` on `BadgerIterator` to call `Close()` if GC collects it without explicit closure.

### Design

```go
// badger_store.go — Scan()
func (s *BadgerStore) Scan(index IndexType, start, end []byte) (*BadgerIterator, error) {
    // ... existing code ...
    iter := &BadgerIterator{
        txn:   txn,
        it:    it,
        start: start,
        end:   end,
        index: index,
    }
    runtime.SetFinalizer(iter, (*BadgerIterator).closeFinalizer)
    return iter, nil
}

func (i *BadgerIterator) closeFinalizer() {
    // Only close if not already closed
    if i.txn != nil {
        i.it.Close()
        i.txn.Discard()
        i.txn = nil
    }
}

func (i *BadgerIterator) Close() error {
    if i.txn == nil {
        return nil // already closed
    }
    runtime.SetFinalizer(i, nil) // clear finalizer
    i.it.Close()
    i.txn.Discard()
    i.txn = nil
    return nil
}
```

### Pros

- Zero API changes — existing code works unmodified
- Safety net for internal and external iterator consumers
- Catches bugs automatically rather than requiring manual audit
- No performance overhead on the happy path (finalizer is cleared on explicit `Close()`)

### Cons

- Finalizer execution timing is nondeterministic — GC may not run for a long time, leaving resources held longer than necessary
- Finalizer ordering is not guaranteed — if BadgerIterator is finalized before or after related objects, the cleanup order may be wrong
- Masks bugs instead of surfacing them — leaked iterators silently work rather than failing visibly
- Adds complexity to `BadgerIterator` (closed-state tracking, nil checks)
- Go documentation discourages finalizers for resource management: "The finalizer for x is scheduled to run at some arbitrary time after x becomes unreachable. There is no guarantee that finalizers will run before a program exits."
- Does NOT help on long-running processes where GC pressure is low — the WAL file persists until the next GC cycle happens to collect the iterator

### Verdict

Useful as a defense-in-depth measure but should not be relied on as the primary mechanism. The real fix is always calling `Close()`. A finalizer is a backstop that reduces blast radius when someone forgets.

## Option B: Open Iterator Tracking on Database

Track all open iterators created by `BadgerStore.Scan()` and close any remaining ones during `db.Close()`.

### Design

```go
// badger_store.go
type BadgerStore struct {
    // ... existing fields ...
    openIterators sync.Map // map[*BadgerIterator]struct{}
}

func (s *BadgerStore) Scan(index IndexType, start, end []byte) (*BadgerIterator, error) {
    // ... existing code ...
    iter := &BadgerIterator{
        txn:   txn,
        it:    it,
        start: start,
        end:   end,
        index: index,
        store: s, // back-reference for deregistration
    }
    s.openIterators.Store(iter, struct{}{})
    return iter, nil
}

func (i *BadgerIterator) Close() error {
    if i.store != nil {
        i.store.openIterators.Delete(i)
    }
    i.it.Close()
    i.txn.Discard()
    return nil
}

func (s *BadgerStore) Close() error {
    // Close any leaked iterators before closing the store
    s.openIterators.Range(func(key, _ any) bool {
        if iter, ok := key.(*BadgerIterator); ok {
            iter.it.Close()
            iter.txn.Discard()
        }
        s.openIterators.Delete(key)
        return true
    })
    return s.db.Close()
}
```

### Pros

- Deterministic cleanup — all iterators closed during `db.Close()`, guaranteed
- Resources released promptly at a well-defined point
- Can add a warning log when closing leaked iterators (aids debugging)
- Works regardless of GC timing
- Could expose `OpenIteratorCount()` for diagnostics/testing

### Cons

- Adds overhead to every `Scan()` and `Close()` call (`sync.Map` store/delete)
- Back-reference from `BadgerIterator` to `BadgerStore` increases coupling
- `sync.Map` memory overhead per open iterator (small but non-zero)
- Does not prevent the leak — just cleans up at `db.Close()`. Between the leaked `Iterator()` call and `db.Close()`, the transaction still holds a skiplist ref and the WAL file still persists
- Doesn't help if the process runs indefinitely without calling `db.Close()`

### Performance Note

`sync.Map.Store` and `sync.Map.Delete` are O(1) amortized. With typical query patterns creating 1-10 iterators per query, the overhead is negligible. But it's still non-zero work on every storage scan, which is the hottest path in the engine.

### Variant: Debug-Only Tracking

Wrap the tracking in a build tag or option flag:

```go
func WithIteratorTracking() Option  // enable tracking for tests/debugging
```

This avoids production overhead while giving test suites a way to detect leaks:

```go
db, _ := db.Open(path, db.WithIteratorTracking())
// ... run queries ...
db.Close()
// Close() warns about any unclosed iterators
```

### Verdict

Solid for correctness but adds coupling and hot-path overhead. The debug-only variant is appealing — zero production cost, full leak detection in tests. Worth considering if iterator leaks recur.

## Option C: Lint / Static Analysis

Add a `go vet`-style check or custom linter that detects `rel.Iterator()` calls without matching `iter.Close()` or `defer iter.Close()` in the same scope.

### Design

A custom `analysis.Analyzer` (using `golang.org/x/tools/go/analysis`) that:

1. Finds all calls to methods named `Iterator()` that return a type implementing `Close() error`
2. Checks the enclosing function for a `defer iter.Close()` or explicit `iter.Close()` call
3. Reports violations

### Pros

- Catches bugs at build time, before they reach production
- Zero runtime overhead
- Works for internal and external code
- Can be integrated into CI

### Cons

- False positives when iterators are stored in structs (wrapper iterators that propagate `Close()` down the chain)
- Custom linters require maintenance
- Doesn't catch dynamic patterns (iterator returned from a function, closed by caller)
- Doesn't help external consumers unless they also run the linter

### Verdict

Good long-term investment if iterator leaks become a recurring pattern. Overkill for the current two-site bug.

## Option D: Streaming-First Public API

The deeper issue isn't just leaked iterators — it's that the public API forces full materialization, defeating the streaming architecture entirely.

The engine executes queries with streaming iterators throughout — hash joins, predicate pushdown, phase execution all operate on `Iterator` chains that never materialize intermediate results. Then at the boundary, `Query()` calls `relationToSlice()` which allocates a `[]any` per tuple and copies everything into a slice. For a 100K-tuple result, that's 100K allocations thrown away if the consumer only needs to iterate once.

`QueryInto` is doubly wasteful — it materializes the entire result set into `[][]any`, then iterates that again to map into structs. Two full materializations for one query.

Streaming should be the primitive. Materialization should be the convenience wrapper built on top.

### Design: `Query()` returns `*Tuples`

```go
// Tuples provides streaming access to query results.
// The caller must call Close when done, even if iteration completes.
type Tuples struct {
    rel     executor.Relation
    iter    executor.Iterator
    symbols []string
}

// Query executes a Datalog query and returns a streaming handle.
// Results are not materialized — tuples are read one at a time from
// the underlying iterator chain. The caller MUST call Close when done.
//
//   tuples, err := d.Query(`[:find ?name ?age :where ...]`)
//   if err != nil { ... }
//   defer tuples.Close()
//   for tuples.Next() {
//       name := tuples.Tuple()[0].(string)
//   }
func (d *DB) Query(queryInput any, inputs ...any) (*Tuples, error)

// Next advances to the next tuple. Returns false when iteration is complete.
func (t *Tuples) Next() bool

// Tuple returns the current tuple as []any. The slice is valid until the
// next call to Next() — callers must copy values they want to keep.
func (t *Tuples) Tuple() []any

// Scan copies the current tuple's values into the provided pointers.
//
//   var name string
//   var age int64
//   tuples.Scan(&name, &age)
func (t *Tuples) Scan(dest ...any) error

// ScanInto scans the current tuple into a struct using datalog tags.
//
//   var p Person
//   tuples.ScanInto(&p)
func (t *Tuples) ScanInto(dest any) error

// Symbols returns the symbol names (find variable names without ?).
func (t *Tuples) Symbols() []string

// Collect materializes all remaining tuples into a slice. This is the
// convenience method for when you want everything in memory.
//
//   tuples, _ := d.Query(`[:find ?name :where ...]`)
//   defer tuples.Close()
//   all := tuples.Collect()  // [][]any
func (t *Tuples) Collect() [][]any

// CollectInto materializes all remaining tuples into a typed slice.
//
//   tuples, _ := d.Query(`[:find ?name ?age :where ...]`)
//   defer tuples.Close()
//   var people []Person
//   tuples.CollectInto(&people)
func (t *Tuples) CollectInto(dest any) error

// Close releases all resources held by the Tuples handle. Must be called
// even if iteration completed naturally. Safe to call multiple times.
func (t *Tuples) Close() error
```

### Usage

```go
// Streaming — O(1) memory
tuples, err := d.Query(`[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
if err != nil { ... }
defer tuples.Close()

for tuples.Next() {
    var name string
    var age int64
    tuples.Scan(&name, &age)
    process(name, age)
}

// Materialize when you need it — explicit, not default
tuples, err := d.Query(`[:find ?name :where [?e :person/name ?name]]`)
if err != nil { ... }
defer tuples.Close()
all := tuples.Collect()  // [][]any — you asked for it

// Materialize into structs
tuples, err := d.Query(`[:find ?name ?age :where ...]`)
if err != nil { ... }
defer tuples.Close()
var people []Person
tuples.CollectInto(&people)

// Stream into structs — no intermediate [][]any
tuples, err := d.Query(`[:find ?name ?age :where ...]`)
if err != nil { ... }
defer tuples.Close()
for tuples.Next() {
    var p Person
    tuples.ScanInto(&p)
    process(p)
}

// First match only — reads one tuple, stops
tuples, err := d.Query(`[:find ?e :where [?e :person/name "Alice"]]`)
if err != nil { ... }
defer tuples.Close()
if tuples.Next() {
    alice := tuples.Tuple()[0].(datalog.Identity)
}
```

### Implementation

```go
func (d *DB) Query(queryInput any, inputs ...any) (*Tuples, error) {
    rel, err := d.ExecuteQueryRelation(queryInput, inputs...)
    if err != nil {
        return nil, err
    }
    symbols := make([]string, len(rel.Symbols()))
    for i, sym := range rel.Symbols() {
        symbols[i] = string(sym)
    }
    return &Tuples{
        rel:     rel,
        iter:    rel.Iterator(),
        symbols: symbols,
    }, nil
}

func (t *Tuples) Next() bool   { return t.iter.Next() }
func (t *Tuples) Tuple() []any { return t.iter.Tuple() }

func (t *Tuples) Collect() [][]any {
    var result [][]any
    for t.Next() {
        tuple := t.Tuple()
        cp := make([]any, len(tuple))
        copy(cp, tuple)
        result = append(result, cp)
    }
    return result
}

func (t *Tuples) Close() error {
    if t.iter != nil {
        err := t.iter.Close()
        t.iter = nil
        return err
    }
    return nil
}
```

### What changes

| Current API | New API | Notes |
|------------|---------|-------|
| `Query() ([][]any, error)` | `Query() (*Tuples, error)` | Streaming is the primitive |
| `QueryInto(&dest, q)` | `tuples.CollectInto(&dest)` | No double materialization |
| `QueryOneInto(&dest, q)` | `tuples.Next(); tuples.ScanInto(&dest)` | Reads one tuple, not all |
| `GetString(e, a)` | unchanged | Convenience stays, uses Query internally |
| `GetInt(e, a)` etc. | unchanged | Same |

The typed accessors (`GetString`, `GetInt`, etc.) keep their signatures — internally they call `Query`, iterate one tuple, close. The implementation changes but the consumer API doesn't.

### Interaction with iterator lifecycle

`Tuples.Close()` calls `iter.Close()` on the underlying iterator chain, which propagates down to `BadgerIterator.Close()`. Every `Query` call produces a `*Tuples` that must be closed. Go developers already know this pattern from `os.Open`, `sql.Query`, `http.Get` — anything that acquires resources returns something with `Close()`.

Options A/B/C from this proposal remain relevant as defense-in-depth for internal code.

### Pros

- Streaming is the default, not an afterthought
- One return type (`*Tuples`) for all query patterns
- Materialization is opt-in via `Collect()` / `CollectInto()`
- No double materialization for struct mapping
- `QueryOneInto` pattern reads one tuple instead of materializing all
- Familiar `Close()` contract
- The streaming architecture finally reaches the consumer

### Cons

- Breaking change to `Query()` signature
- Every `Query` call requires `defer tuples.Close()`
- Aggregations still materialize internally

### Verdict

This is the correct API. Streaming is the primitive. Materialization is a convenience method on top. The current design inverts this and wastes the entire streaming architecture at the API boundary.

## Recommendation

**Immediate (do now)** — ~~Fix the two missing `iter.Close()` calls in `helpers.go` and the one in `simple_batch_scanner.go`.~~ **Discharged (2026-07-26).** `simple_batch_scanner.go` was deleted in v0.15.0, taking its site with it; the `helpers.go` sites moved when that file was split under the no-helpers rule. Re-derive against the current tree before acting on the rows below — the file table is as of this document's date.

**Short-term (next feature)**: Option D — `Query()` returns `*Tuples`. This is the correct public API and addresses the real design gap. The streaming architecture currently stops at the API boundary. This carries it through to the consumer.

**Medium-term (consider if leaks recur)**: Option A (finalizer) as a defense-in-depth backstop.

**Medium-term (consider for test infrastructure)**: Option B debug-only variant. Add `WithIteratorTracking()` to the test harness so `go test` detects leaked iterators automatically.

**Long-term (if pattern persists)**: Option C (static analysis).

## Affected Files (for reference)

| File | Missing `Close()` | Risk |
|------|-------------------|------|
| `executor/helpers.go:80` | `filterWithPredicateAndLookup` | **High** — receives StreamingRelation from pattern match |
| `executor/helpers.go:178` | `evaluateExpressionWithLookup` | **High** — receives StreamingRelation from pattern match |
| `storage/simple_batch_scanner.go:94` | `buildBindingSet` | Low — binding relations are pre-materialized |
