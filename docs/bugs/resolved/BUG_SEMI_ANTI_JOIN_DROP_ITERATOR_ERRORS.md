# BUG: SemiJoin and AntiJoin Drop Iterator Errors

**Date**: 2026-05-25 **Severity**: Correctness (Medium) **Status**: Open **Affected**: `executor.SemiJoin`, `executor.AntiJoin`, and query paths that use semi/anti joins, including negation-style execution

## Summary

`SemiJoin` and `AntiJoin` consume both input iterators but never check `Iterator.Error()` and never preserve `Close()` errors. If either side fails after yielding a prefix, the join returns a clean `MaterializedRelation` built from partial data.

This violates the iterator error contract used elsewhere in the executor. Other materialization boundaries now call `Iterator.Error()` after iteration and carry the failure on the resulting relation. These two join primitives still launder iterator failures into apparently successful results.

## Code Evidence

`SemiJoin` builds right-side keys, filters the left side, then returns a clean relation:

```go
// join.go
rightKeys := NewTupleKeyMapWithCapacity(right.Size())
rightIt := right.Iterator()
defer rightIt.Close()

for rightIt.Next() {
    tuple := rightIt.Tuple()
    key := NewTupleKey(tuple, rightIndices)
    rightKeys.Put(key, true)
}

var results []Tuple
leftIt := left.Iterator()
defer leftIt.Close()

for leftIt.Next() {
    tuple := leftIt.Tuple()
    key := NewTupleKey(tuple, leftIndices)
    if rightKeys.Exists(key) {
        results = append(results, tuple)
    }
}

return NewMaterializedRelationWithOptions(left.Symbols(), results, opts)
```

`AntiJoin` has the same structure and the same error loss:

```go
// join.go
rightKeys := NewTupleKeyMapWithCapacity(right.Size())
rightIt := right.Iterator()
defer rightIt.Close()

for rightIt.Next() {
    tuple := rightIt.Tuple()
    key := NewTupleKey(tuple, rightIndices)
    rightKeys.Put(key, true)
}

var results []Tuple
leftIt := left.Iterator()
defer leftIt.Close()

for leftIt.Next() {
    tuple := leftIt.Tuple()
    key := NewTupleKey(tuple, leftIndices)
    if !rightKeys.Exists(key) {
        results = append(results, tuple)
    }
}

return NewMaterializedRelationWithOptions(left.Symbols(), results, opts)
```

By contrast, `collectTuplesInto` explicitly checks `Error()` and `Close()`:

```go
// relation.go
for it.Next() {
    ...
}
err := it.Error()
if cerr := it.Close(); err == nil {
    err = cerr
}
return err
```

## Failure Mode

If the right iterator fails while building the key set, the join proceeds with an incomplete key set:

```text
right side yields keys: A, B
right side then fails before key C
rightIt.Error() is never checked

SemiJoin: rows matching C are incorrectly excluded
AntiJoin: rows matching C are incorrectly included
```

If the left iterator fails while filtering, the returned relation contains a clean prefix and no error:

```text
left side yields rows 1, 2
left side fails before row 3
leftIt.Error() is never checked
result relation reports success with rows 1, 2 only
```

This is especially risky for anti-join semantics because a missing right-side key caused by decode failure is indistinguishable from a real "no match."

## Reproduction Sketch

Use the existing failing iterator test pattern from the executor tests:

```go
func TestSemiJoin_SurfacesRightIteratorError(t *testing.T) {
    left := executor.NewMaterializedRelation(
        []query.Symbol{datalog.NewSymbol("?x")},
        []executor.Tuple{{int64(1)}, {int64(2)}},
    )
    right := newFailingRelation(
        1,
        executor.Tuple{int64(1)},
        executor.Tuple{int64(2)},
    )

    joined := executor.SemiJoin(left, right, []query.Symbol{datalog.NewSymbol("?x")})
    require.ErrorIs(t, driveErr(joined), errInjectedIterator)
}
```

Equivalent tests should cover:

- right-side failure in `SemiJoin`
- left-side failure in `SemiJoin`
- right-side failure in `AntiJoin`
- left-side failure in `AntiJoin`

## Impact

- Storage decode failures can become silently wrong query results.
- Missing Tier-3 blobs, corrupted keys, or iterator aborts can appear as valid anti-join matches.
- The bug undermines the repository's newer iterator error contract, which was added specifically to prevent partial-result laundering.

## Fix Direction

Both functions should collect and attach the first iterator error to the result. The pattern should match the rest of the relation layer:

1. Iterate right side.
2. Check `rightIt.Error()`.
3. Check `rightIt.Close()` if no earlier error.
4. Iterate left side only if the right side was clean, or carry the right-side error directly.
5. Check `leftIt.Error()` and `leftIt.Close()`.
6. Store the first error on the returned materialized relation.

If an error occurs while building the right-side key set, the safest behavior is to return a relation carrying that error and no trusted result rows. Returning a partial anti-join result is not semantically meaningful.

## Verification Plan

Add regression tests alongside the existing iterator error boundary tests:

- `TestSemiJoin_SurfacesRightIteratorError`
- `TestSemiJoin_SurfacesLeftIteratorError`
- `TestAntiJoin_SurfacesRightIteratorError`
- `TestAntiJoin_SurfacesLeftIteratorError`

Then run:

```bash
go test -count=1 ./...
```

---

## Resolution (2026-05-25)

**Resolved.** `SemiJoin` and `AntiJoin` now check both iterators' `Error()` and `Close()` and carry the first failure onto the result, matching the iterator contract used by `collectTuplesInto` and the rest of the relation layer.

- The `defer rightIt.Close()` / `defer leftIt.Close()` were replaced with explicit `Close()` so their errors can be captured before the result is built.
- **Right (key-set) side fails:** the key set is incomplete, which makes every filter decision untrustworthy — semi-join would drop real matches, and anti-join would emit false "no match" rows. So the join returns a relation carrying the error and **no result rows** (`res.err = rerr`, nil tuples).
- **Left (filtered) side fails:** the rows collected so far were filtered against a complete key set, so they are kept and the error is carried on the result (`res.err = lerr`), surfaced at the next boundary via `Iterator().Error()`.

This was a gap left by `BUG_ITERATOR_ERRORS_DROPPED_IN_MATERIALIZATION_PATHS`: that sweep covered `collectTuplesInto` call sites, but `SemiJoin`/`AntiJoin` build their own iterator loops, so the static guard there did not catch them.

### Tests

`semi_anti_join_error_test.go` — all four cases (right/left failure × semi/anti) assert the error is surfaced. Each fails before the fix and passes after.

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
