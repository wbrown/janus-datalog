# BUG: Iterator Leak in Builtin Database Function Evaluation

**Status**: Open (reproduction test added)
**Severity**: Resource leak (256 MB WAL file per leaked query; blocks directory cleanup on Windows)
**Discovered**: 2026-02-23, reported by colleague with Windows reproduction
**Reproduction**: `TestIteratorLeak_BuiltinPatternDiscoveredEntity` in `datalog/storage/iterator_leak_test.go`

## Symptom

Queries using `get-some`, `get-else`, or `missing?` with pattern-discovered entities leak a BadgerDB memtable WAL file (`00001.mem`, 256 MB) after `db.Close()`. The file persists on disk on all platforms. On Windows, the held mmap handle additionally blocks `os.RemoveAll` on the database directory.

Standard pattern-match queries (`[?e :attr ?val]`) and `pull` expressions do not leak.

## Reproduction

```go
// Setup: 10 entities, half with :entity/code, half without
//
// Query (leaks):
//   [:find ?name :where [?e :entity/type _]
//                       [(get-some $ ?e :entity/name :entity/code) ?name]]
//
// Query (does NOT leak — entity bound via :in):
//   [:find ?name :in $ ?e
//    :where [(get-some $ ?e :entity/name :entity/code) ?name]]
//
// After db.Close():
//   00001.mem (256 MB) persists in database directory
```

Run:
```bash
go test -v -count=1 -run TestIteratorLeak_BuiltinPatternDiscoveredEntity ./datalog/storage/
```

All three subtests (`get-some`, `get-else`, `missing?`) fail — the WAL file persists after `db.Close()`.

## Root Cause

**File**: `datalog/executor/helpers.go`

Five functions in `helpers.go` call `rel.Iterator()`. Three of them call `iter.Close()`. The two that don't are exactly the two that handle database builtins:

| Line | Function | `iter.Close()`? | Handles |
|------|----------|-----------------|---------|
| 80 | `filterWithPredicateAndLookup` | **NO** | `missing?` |
| 178 | `evaluateExpressionWithLookup` | **NO** | `get-some`, `get-else` |
| 329 | `projectToColumns` | YES (line 338) | standard queries |
| 404 | `getUniqueCombinations` | YES (line 417) | NOT/OR |
| 482 | `unionRelations` | YES (line 495) | OR branches |

### How the leaked iterator prevents WAL cleanup

When `get-some`/`get-else`/`missing?` are evaluated on pattern-discovered entities, the input `rel` is a `StreamingRelation` backed by a BadgerDB storage iterator. The iterator chain is:

```
StreamingRelation
  → CachingIterator / CountingIterator
    → unboundIterator
      → CRDTResolvingIterator
        → BadgerIterator (holds badger.Txn + badger.Iterator → skiplist ref)
```

The sequence of failure:

1. Builtin query iterates a `StreamingRelation` from pattern match
2. `StreamingRelation` wraps `BadgerIterator` (holds `badger.Txn` + skiplist ref)
3. Loop exhausts `iter.Next()` but never calls `iter.Close()`
4. `BadgerIterator.Close()` never called → `txn.Discard()` never called
5. Skiplist ref count stays elevated (`DecrRef` never called)
6. `db.Close()` flushes memtable, calls `skiplist.DecrRef()`
7. Ref count goes from 2 → 1 (not 0) — leaked ref prevents zero
8. Skiplist `OnClose` callback never fires
9. `wal.Delete()` never called → `00001.mem` persists on disk (256 MB)
10. On Windows: held mmap handle also blocks `os.RemoveAll`

Go's GC collects the unreachable iterator chain but does NOT call `skiplist.DecrRef()` — Go has no destructors. The ref count stays elevated permanently. The WAL file is never cleaned up.

### Why standard queries don't leak

Standard pattern queries go through the join path (`HashJoinWithOptions`), which uses `defer buildIt.Close()` and `defer probeIt.Close()`. The final result extraction uses `projectToColumns`, which calls `iter.Close()` at line 338. Every iterator in the standard path is properly closed.

### Why `:in $` bound entities don't leak

When entities are provided via `:in $`, they arrive as `constantBindings`. The executor takes the "no relevant relations" branch and evaluates the function once per constant binding — no `StreamingRelation` iterator is ever created.

### Code evidence

```go
// helpers.go — filterWithPredicateAndLookup (handles missing?)
// Line 80: iterator opened, never closed
iter := rel.Iterator()
for iter.Next() {
    // ... evaluate predicate per tuple ...
}
// iter.Close() is never called

// helpers.go — evaluateExpressionWithLookup (handles get-some, get-else)
// Line 178: iterator opened, never closed
iter := rel.Iterator()
for iter.Next() {
    // ... evaluate expression per tuple ...
}
// iter.Close() is never called

// helpers.go — projectToColumns (standard path, correct)
// Line 329: iterator opened, closed at line 338
iter := rel.Iterator()
for iter.Next() {
    // ... project columns ...
}
iter.Close()  // ← correctly closed
```

## Affected Builtins

| Builtin | Leaks? | Condition |
|---------|--------|-----------|
| `get-some` (pattern-discovered entity) | **YES** | Entity from `[?e :attr _]` |
| `get-some` (bound via `:in $`) | NO | Entity pre-bound as input |
| `get-else` (pattern-discovered entity) | **YES** | Entity from `[?e :attr _]` |
| `missing?` (pattern-discovered entity) | **YES** | Entity from `[?e :attr _]` |
| Standard `[?e :attr ?val]` | NO | Join path closes iterators |
| `(pull ?e [...])` | NO | `projectToColumns` closes iterators |

## Impact

Each leaked query leaves a 256 MB WAL file on disk that should have been deleted during `db.Close()`. The file is not cleaned up by GC or any other mechanism — it persists until the process exits (or until the database directory is manually deleted).

On Windows, the held mmap handle prevents `os.RemoveAll` on the database directory. A test suite with 20+ tests using these builtins can leak several GB. The colleague's full test suite leaked 16 GB before tracing the root cause.

On all platforms, the undiscarded BadgerDB transaction holds a read snapshot, which may also interfere with BadgerDB's value log garbage collection.

## What We've Ruled Out

- **Not in BadgerDB itself**: Pure BadgerDB isolation tests (open/write/iterate/close) pass with zero leaked handles, even with 100 sequential transaction/iterator pairs.
- **Not in `LookupAttribute`**: All `LookupAttribute` code paths properly close iterators with `defer iter.Close()` — `matcher.go:850-854`, `matcher.go:889-893`, `cache_resolver.go:42-46`, `set_resolution.go:44-48`, `vector_resolution.go:64-68`.
- **Not mmap-backed values in cache**: All datom values are decoded from key copies (`item.KeyCopy(nil)` in `BadgerIterator.Datom()`).
- **Not a platform-specific issue**: The WAL leak reproduces on macOS and Linux (detectable via file persistence). The `os.RemoveAll` failure is Windows-only due to mmap handle semantics, but the underlying resource leak is cross-platform.

## Fix

Add `defer iter.Close()` in both functions:

```go
// helpers.go — filterWithPredicateAndLookup
iter := rel.Iterator()
defer iter.Close()  // ADD THIS

// helpers.go — evaluateExpressionWithLookup
iter := rel.Iterator()
defer iter.Close()  // ADD THIS
```

This matches the pattern already used by `projectToColumns` (line 338), `getUniqueCombinations` (line 417), and `unionRelations` (line 495) in the same file.

## Files Involved

| File | Role |
|------|------|
| `datalog/executor/helpers.go:80` | `filterWithPredicateAndLookup` — missing `iter.Close()` (fix location) |
| `datalog/executor/helpers.go:178` | `evaluateExpressionWithLookup` — missing `iter.Close()` (fix location) |
| `datalog/executor/helpers.go:338,417,495` | Three other functions that correctly call `iter.Close()` (reference pattern) |
| `datalog/executor/relation.go:129-139` | `Iterator` interface with `Close() error` method |
| `datalog/executor/relation.go:733-853` | `StreamingRelation` — wraps `BadgerIterator` via iterator chain |
| `datalog/storage/badger_store.go:470-475` | `BadgerIterator.Close()` — closes iterator + discards transaction |
| `datalog/storage/iterator_leak_test.go` | Reproduction test (asserts WAL file does not persist after `db.Close()`) |
