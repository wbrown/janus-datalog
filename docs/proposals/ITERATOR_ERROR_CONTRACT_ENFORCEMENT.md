# Plan: Enforce the Iterator Error Contract End-to-End

**Date**: 2026-05-24
**Status**: Proposal
**Fixes**: `BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES`, and the error-drop half of `BUG_UNION_RELATION_CONCURRENT_ITERATION_AND_ERROR_DROP`
**Out of scope**: changing the `Next()`/`Error()` protocol; the `UnionRelation` concurrent-iteration / cache-build race (tracked separately in its own bug doc)

## The contract is correct and deliberate

The iterator protocol is the Go-standard `Next() bool` + `Error() error` pair (`sql.Rows`, `bufio.Scanner`). It is documented on the interface itself. From `datalog/storage/store.go`:

> `Error()` must be checked after `Next()` returns false. A nil result indicates normal exhaustion; non-nil indicates that iteration aborted partway through (storage scan failure, sub-scan failure inside a wrapping iterator, or a `Datom()` decode error that couldn't be returned to the caller because `Next()` had already indicated "no next item"). [...] Wrapping iterators propagate from their inner iterator (return the first non-nil between the outer's own error and `inner.Error()`).

That last clause is the structural justification: some failures are discovered *as* `Next()` returns false, when there is no in-band return value left to carry them. `Error()` is the channel for exactly those. `datalog/storage/iterator_error_propagation_test.go` exists to lock this contract in across the wrapping-iterator chain.

**We are not changing this.** Replacing it with inline errors (`Next() (bool, error)`) or range-over-func was considered and rejected: it discards a deliberate, documented, idiomatic design that exists for a real reason, and it is an architectural reversal, not a bug fix.

## The defect: consumers don't honor the contract

There is exactly one legitimate error channel — `Iterator.Error()`. The bug is that not every consumer reads it. This shows up in two places.

### 1. Boundary consumers drop `Error()`

`executor.CollectTuples`, `storage.Database.QueryInto`, and `storage.Database.QueryOneInto` run `for it.Next() { ... }` and then return success without checking `it.Error()`. A storage decode, blob lookup, CRDT-resolution, or union failure becomes an empty result, a truncated result, or `found=false, nil`. `UnionIterator.Next()` is a sibling case: when an inner iterator exhausts it calls `Close()` without first checking `currentIter.Error()`, so the inner failure is lost even though the *outer* contract is otherwise fine.

### 2. Caching relations drop the source `Error()`

A terminology correction matters here. `Materialize()`, `Sorted()`, `Size()`, `Get()`, `IsEmpty()`, and `UnionRelation`'s cache are **not** an "eager evaluation" phase — they are **caching consumers**: they drive the underlying iterator once and remember the realized tuples for reuse. The relation's identity is unchanged; the cache is a performance/reuse mechanism.

Because they are consumers, the contract applies to them — and because the relations they produce are *replayed* on later `Iterator()` calls, they are exactly the "wrapping iterators" the contract says must propagate the inner `Error()`. They currently don't:

```go
// datalog/executor/relation.go — collectTuplesInto, the collector behind Materialize()
func collectTuplesInto(dest *[]Tuple, rel Relation) {
    it := rel.Iterator()
    for it.Next() { /* append */ }
    it.Close() // <- source Error() never checked; Close() error discarded
}
```

`Materialize()`/`Sorted()` build a `MaterializedRelation` that has no place to hold the source error, so order-by, aggregation, joins (build side), and `UnionRelation.Materialize` launder failures *before* any boundary can see them. `UnionRelation.Materialize`'s own comment admits it: "Errors from Close() are silently dropped (limitation of Relation interface)." This is a direct violation of the documented wrapping-iterator rule, not a missing feature.

## Principle

One error channel, honored everywhere. No second, parallel channel. Two parts:

1. Implement the consume-and-check-`Error()` protocol in exactly one audited place, and route every boundary through it.
2. Make caching relations comply with the contract they already violate: a cache must replay the source's terminal error through its own `Iterator.Error()`, just as any wrapping iterator does.

---

## Part 1 — One canonical consumer of the contract

Add a single function that owns the documented protocol so no boundary re-derives it (wrongly):

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

- Drive `for it.Next()`, call `fn(it.Tuple())`; if `fn` returns an error, stop and return it (still `Close()` the iterator).
- After the loop, call `it.Error()`. Non-nil → return it.
- `Close()` is deferred; its error is returned only if no iteration/`fn` error already occurred. (Close and Error are separate signals; the iteration error is the more specific one and must win.)

Route through it:

- `CollectTuples` becomes a thin wrapper: `ForEach` appending copies.
- `QueryInto` (struct path and scalar path) calls `ForEach`; the per-tuple `fn` does the reflect mapping and returns mapping errors directly, so both mapping errors and iterator errors share one path.
- `QueryOneInto` uses a small "at most one" variant (or `ForEach` with a closure that captures the first tuple and returns `ErrMultipleResults` on the second). Critically, a first `Next()` that returns false must consult `Error()` before reporting `found=false`.
- `UnionIterator.Next()`: before discarding an exhausted inner iterator, capture its error —

  ```go
  if err := it.currentIter.Error(); err != nil && it.firstError == nil {
      it.firstError = err
  }
  _ = it.currentIter.Close()
  ```

  This is the same contract-compliance fix; it just happens inside a hand-written iterator rather than at a public boundary.

The lone seam that can't use `ForEach` is `UnionIterator` itself, because it interleaves inner iteration with dedup and cache-building. That's fine: it implements the contract directly (the capture above), and the bug there was simply skipping the `Error()` check.

---

## Part 2 — Caching relations must obey the contract

A cache of a stream must be transparent to *both* the tuples and the termination. Today it replays the tuples and swallows the terminal error. The fix is to carry the source's error so the cached relation's iterator reports it via `Error()`:

- `collectTuplesInto` consumes via the canonical protocol, capturing the source `Error()` (and Close error) and returning it to its caller.
- `MaterializedRelation` gains an `err error` field; its `sliceIterator.Error()` returns it. `Materialize()`/`Sorted()` set it from what `collectTuplesInto` reports. A `MaterializedRelation` built from a failed stream therefore replays "here are the tuples I got, then iteration aborted with this error" — a faithful implementation of the documented contract.
- `UnionRelation`'s cache likewise records the builder's terminal error so replay iterators report it.

Consequence: errors propagate through *any* number of caching layers (order-by over an aggregate over a failing scan) because each layer faithfully forwards the one `Error()` channel, and the boundary (Part 1) checks it. No signature changes, no parallel channel.

### The lossy-signature methods stay as-is, and stay safe

`Size() int`, `Get(i) Tuple`, `IsEmpty() bool`, `String()`, `Table()` cannot return an error. They do not need to, *provided Part 2 holds*: they may return a wrong/partial answer on a failing relation, but the error is not destroyed — it re-surfaces the moment the relation's data is actually consumed at an `Error()`-checking boundary. `Size`/`IsEmpty` feed plan/strategy choices (a wrong size yields a suboptimal plan, not a wrong result), and `String`/`Table` are debug formatting. None are the final correctness boundary, so leaving them lossy is acceptable as long as caching relations carry the error for the eventual real consumption.

### Relationship to the previously-rejected "sticky error" (C)

Earlier I floated stashing the error on the relation (option "C") and it was correctly called a side channel. The objection was sound *in that context*: I was simultaneously proposing to abolish `Error()`, so a stash was a contradictory parallel mechanism.

With `Error()` affirmed as **the** documented contract, Part 2 is not a second channel. `MaterializedRelation.err` backing its iterator's `Error()` is the same kind of field as `KeyOnlyIterator.currentError` backing *its* `Error()` — it is the *implementation* of the one contract, not a bypass of it. The contract already requires wrapping iterators to propagate inner `Error()`; a caching relation is a replaying wrapper, so this is compliance, not invention.

If that distinction doesn't hold up under scrutiny, the only contract-honoring alternative is error-returning signatures on the caching methods (`Materialize() (Relation, error)`, etc.) — more uniform but a broad `Relation` interface change.

**Decided:** caching relations are contract-compliant `Error()` replayers. No `Relation` interface change. The caching-method-signature alternative is rejected.

---

## What we are explicitly not doing

- Not changing `Next()`/`Error()` to inline errors or range-over-func.
- Not introducing any error path other than `Iterator.Error()` and the existing executor return values (`Execute(...) ([]Relation, error)`, `Query(...) (Relation, error)`).
- Not fixing the `UnionRelation` concurrent-iteration / one-shot-channel / cache-build race here — that is a separate correctness issue (single-builder state machine) tracked in `BUG_UNION_RELATION_CONCURRENT_ITERATION_AND_ERROR_DROP.md`.

## Tests and guards

- Reproductions already written and currently red: `TestCollectTuples_ReturnsIteratorError`, `...AfterPartialResults`, `TestUnionIterator_InnerIteratorErrorPropagates` (`datalog/executor/iterator_error_boundary_test.go`, `union_relation_error_test.go`).
- Add: `QueryInto`/`QueryOneInto` boundary tests (first-`Next()` failure, mid-stream failure, second-`Next()` failure) — needs the canonical primitive so a failing relation can be exercised through the public API.
- Add: caching-replay tests — materialize/sort/aggregate a failing stream and assert the resulting relation's `Iterator().Error()` returns the source error, and that a boundary over it returns the error.
- Extend the existing `iterator_error_propagation_test.go` pattern from storage wrapping-iterators to the public boundaries.
- Document the contract in the godoc at the public surface (`Database.Query`, the `Relation`/`Iterator` interfaces), since external callers receive `Relation`s directly (see below).

## Public-surface reach

`Database.Query` returns `(executor.Relation, error)` and `Database.Match` returns `executor.Relation` — both public, handing the exported `Relation` interface to external callers. So external code can drive `Iterator()` itself or use the exported `executor.CollectTuples`. Implications:

- A lint/grep over this repo cannot cover external consumers, so it cannot enforce the contract on the public surface — only catch internal regressions. **Decided: no lint.** It would give false confidence without covering the surface that matters.
- The lever that does reach external callers: make the error-checked consumers (`CollectTuples`, `QueryInto`, `QueryOneInto`, `ForEach`) the documented front door, and state the `Error()` contract in the godoc wherever a raw `Relation` is returned.
- Regression protection is the boundary contract tests plus the canonical primitive being the obvious path, not a lint.

A larger, separate question (not part of this fix): whether raw `Relation` should remain the *primary* public return, or whether the helpers should be the front door with raw iteration as an expert path.

## Decisions

1. **Part 2 mechanism — decided: contract-compliant `Error()` replay** on caching relations. No `Relation` interface change.
2. **Lint guard — decided: none.** Rely on the canonical primitive, boundary contract tests, and godoc at the public surface (external callers receive `Relation`s, so a lint can't enforce the contract anyway).
