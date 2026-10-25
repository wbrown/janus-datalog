# Enforce the Iterator Error Contract End-to-End

**Removes:** `BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES`, and the error-drop half of `BUG_UNION_RELATION_CONCURRENT_ITERATION_AND_ERROR_DROP`. **Out of scope:** changing the `Next()`/`Error()` protocol, and the union relation's concurrent-iteration race, which its own bug doc tracks.

## The contract is correct and deliberate

The iterator protocol is the Go-standard `Next() bool` + `Error() error` pair (`sql.Rows`, `bufio.Scanner`). It is documented on the interface itself. From the `Iterator` godoc in `datalog/storage/store.go`:

> `Error()` must be checked after `Next()` returns false. A nil result indicates normal exhaustion; non-nil indicates that iteration aborted partway through (storage scan failure, sub-scan failure inside a wrapping iterator, or a decode failure retained as a sticky Error after `Next()` returned false).

And on `Error()` itself:

> Wrapping iterators propagate from their inner iterator (return the first non-nil between the outer's own error and `inner.Error()`).

That last sentence is the structural justification: some failures are discovered _as_ `Next()` returns false, when there is no in-band return value left to carry them. `Error()` is the channel for exactly those. `datalog/storage/iterator_error_propagation_test.go` locks this contract in across the wrapping-iterator chain.

The alternative is inline errors, `Next() (bool, error)`, or range-over-func. It discards a deliberate, documented, idiomatic design that exists for a real reason, and it is an architectural reversal, not a bug fix.

## The failure this removes

There is exactly one legitimate error channel, `Iterator.Error()`. The defect was that not every consumer read it. It showed up in two places.

### 1. Boundary consumers dropped `Error()`

`executor.CollectTuples`, `storage.Database.QueryInto`, and `storage.Database.QueryOneInto` ran `for it.Next() { ... }` and then returned success without checking `it.Error()`. A storage decode, blob lookup, CRDT-resolution, or union failure became an empty result, a truncated result, or `found=false, nil`. The union iterator of the time was a sibling case: when an inner iterator exhausted, it called `Close()` without first checking the inner `Error()`, so the inner failure was lost even though the _outer_ contract was otherwise fine.

### 2. Caching relations dropped the source `Error()`

A terminology correction matters here. `Materialize()`, `Sorted()`, `Size()`, `Get()`, `IsEmpty()`, and a union relation's cache are **not** an "eager evaluation" phase. They are **caching consumers**: they drive the underlying iterator once and remember the realized tuples for reuse. The relation's identity is unchanged, and the cache is a performance and reuse mechanism.

Because they are consumers, the contract applies to them. Because the relations they produce are _replayed_ on later `Iterator()` calls, they are exactly the "wrapping iterators" the contract says must propagate the inner `Error()`. They did not:

```go
// datalog/executor/relation.go — collectTuplesInto, the collector behind Materialize()
func collectTuplesInto(dest *[]Tuple, rel Relation) {
    it := rel.Iterator()
    for it.Next() { /* append */ }
    it.Close() // <- source Error() never checked; Close() error discarded
}
```

`Materialize()` and `Sorted()` built a `MaterializedRelation` that had no place to hold the source error, so order-by, aggregation, joins (build side), and the union relation's `Materialize` laundered failures _before_ any boundary could see them. The union relation's own comment admitted it: "Errors from Close() are silently dropped (limitation of Relation interface)." That is a direct violation of the documented wrapping-iterator rule, not a missing feature.

## Principle

One error channel, honored everywhere. No second, parallel channel. Two parts:

1. Implement the consume-and-check-`Error()` protocol in exactly one audited place, and route every boundary through it.
2. Make caching relations comply with the contract they violated: a cache must replay the source's terminal error through its own `Iterator.Error()`, just as any wrapping iterator does.

---

## The contracts the tests pin

- A boundary consumer returns the iterator's terminal error, after no tuples and after partial results. Its tests: `TestCollectTuples_ReturnsIteratorError` and `TestCollectTuples_ReturnsIteratorErrorAfterPartialResults` (`datalog/executor/iterator_error_boundary_test.go`).
- A branch collected under a form returns its result's iteration error and its close error. Its test: `TestCollectBranchPreservesIteratorAndCloseErrors` (`datalog/executor/or_outer_selection_test.go`). A fallback form's subject errors are the result's the same way: `TestOrFallbackSurfacesTheSubjectsIteratorAndCloseErrors` beside it.
- `QueryInto` and `QueryOneInto` return the error on a first-`Next()` failure, a mid-stream failure and a second-`Next()` failure. A failing relation is driven through the public API by the canonical consumer.
- A relation materialized, sorted or aggregated from a failing stream reports the source error from `Iterator().Error()`, and a boundary over it returns the error.
- The `iterator_error_propagation_test.go` pattern holds from storage wrapping iterators through to the public boundaries.
- The contract is stated in the godoc at the public surface, `Database.Query` and the `Relation` and `Iterator` interfaces, since external callers receive `Relation`s directly (see _Public-surface reach_).

---

## Part 1: one canonical consumer of the contract

`executor.ForEach` owns the documented protocol, so no boundary re-derives it:

```go
// ForEach drives rel's iterator per the documented contract: it yields each
// tuple to fn, then — whether the loop ended by exhaustion, fn error, or
// iterator failure — resolves the outcome. Returns:
//   - fn's error if fn returned one (iteration stops),
//   - else it.Error() if iteration aborted,
//   - else any Close() error.
// An iteration/fn error always takes precedence over a Close() error so a
// cleanup failure can't mask the real cause.
func ForEach(rel Relation, fn func(Tuple) error) error
```

Semantics, spelled out so the one implementation is unambiguous:

- Drive `for it.Next()`, call `fn(it.Tuple())`. If `fn` returns an error, stop and return it, and still `Close()` the iterator.
- After the loop, call `it.Error()`. Non-nil is returned.
- `Close()` is deferred. Its error is returned only if no iteration or `fn` error already occurred. Close and Error are separate signals, and the iteration error is the more specific one and wins.

The boundaries route through it:

- `CollectTuples` is a thin wrapper: `ForEach` appending copies.
- `QueryInto`, the struct path and the scalar path, calls `ForEach`. The per-tuple `fn` does the reflect mapping and returns mapping errors directly, so mapping errors and iterator errors share one path.
- `QueryOneInto` uses a small "at most one" variant, or `ForEach` with a closure that captures the first tuple and returns `ErrMultipleResults` on the second. A first `Next()` that returns false consults `Error()` before reporting `found=false`.
- The union and fallback forms implement the capture directly inside their branch collectors, since they interleave inner iteration with dedup and cache-building. That is the same contract compliance inside a hand-written iterator rather than at a public boundary, and the tests above pin it.

---

## Part 2: caching relations obey the contract

A cache of a stream is transparent to _both_ the tuples and the termination. The cache replays the tuples and carries the source's error, so the cached relation's iterator reports it through `Error()`:

- `collectTuplesInto` consumes through the canonical protocol, capturing the source `Error()` and the Close error, and returns it to its caller.
- `MaterializedRelation` carries an `err error` field, and its `sliceIterator.Error()` returns it. `Materialize()` and `Sorted()` set it from what `collectTuplesInto` reports. A `MaterializedRelation` built from a failed stream therefore replays "here are the tuples I got, then iteration aborted with this error", a faithful implementation of the documented contract.
- A union relation's cache likewise records the builder's terminal error so replay iterators report it.

Consequence: errors propagate through _any_ number of caching layers, an order-by over an aggregate over a failing scan, because each layer faithfully forwards the one `Error()` channel, and the boundary (Part 1) checks it. No signature changes, no parallel channel.

### The lossy-signature methods stay as-is, and stay safe

`Size() int`, `Get(i) Tuple`, `IsEmpty() bool`, `String()`, `Table()` cannot return an error. They do not need to, _provided Part 2 holds_: they may return a wrong or partial answer on a failing relation, but the error is not destroyed. It re-surfaces the moment the relation's data is consumed at an `Error()`-checking boundary. `Size` and `IsEmpty` feed plan and strategy choices, where a wrong size yields a suboptimal plan and not a wrong result, and `String` and `Table` are debug formatting. None is the final correctness boundary, so leaving them lossy is acceptable as long as caching relations carry the error for the eventual real consumption.

### A field is not a side channel here

`MaterializedRelation.err` backing its iterator's `Error()` is the same kind of field as `KeyOnlyIterator.currentError` backing _its_ `Error()`. It is the _implementation_ of the one contract, not a bypass of it. A stash of the error on the relation is a side channel only beside a proposal to abolish `Error()`. With `Error()` as the documented contract, a caching relation is a replaying wrapper, and the contract already requires wrapping iterators to propagate the inner `Error()`, so carrying it is compliance, not invention.

The alternative is error-returning signatures on the caching methods, `Materialize() (Relation, error)` and the rest. It is more uniform, and it is a broad `Relation` interface change. Caching relations are contract-compliant `Error()` replayers, and the `Relation` interface does not change.

---

## What this does not do

- It does not change `Next()`/`Error()` to inline errors or range-over-func.
- It introduces no error path other than `Iterator.Error()` and the existing executor return values (`Execute(...) ([]Relation, error)`, `Query(...) (Relation, error)`).
- It does not fix the union relation's concurrent-iteration, one-shot-channel and cache-build race. That is a separate correctness issue, a single-builder state machine, tracked in `BUG_UNION_RELATION_CONCURRENT_ITERATION_AND_ERROR_DROP.md`.

## Public-surface reach

`Database.Query` returns `(executor.Relation, error)` and `Database.Match` returns `executor.Relation`. Both are public, handing the exported `Relation` interface to external callers. So external code can drive `Iterator()` itself or use the exported `executor.CollectTuples`. Implications:

- A lint or grep over this repo cannot cover external consumers, so it cannot enforce the contract on the public surface. It could only catch internal regressions, so there is no lint: it would give false confidence without covering the surface that matters.
- The lever that does reach external callers: the error-checked consumers (`CollectTuples`, `QueryInto`, `QueryOneInto`, `ForEach`) are the documented front door, and the `Error()` contract is stated in the godoc wherever a raw `Relation` is returned.
- Regression protection is the boundary contract tests plus the canonical primitive being the obvious path, not a lint.

Whether raw `Relation` remains the _primary_ public return, or the consumers become the front door with raw iteration as an expert path, is a separate question this document does not decide.
