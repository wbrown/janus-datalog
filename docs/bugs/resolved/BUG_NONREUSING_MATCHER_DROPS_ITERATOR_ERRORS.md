# BUG: Non-Reusing Matcher Path Drops Deferred Iterator Errors

**Date**: 2026-05-30 **Severity**: High — storage/decode/CRDT failures can become successful partial results **Status**: ✅ RESOLVED (2026-05-31) **Affected**: `storage.matchWithoutIteratorReuse`, `storage.nonReusingIterator`

## Resolution

Both loss sites now preserve the iterator outcome per the `storage.Iterator` contract ("Error() must be checked after Next() returns false").

1. `matchWithoutIteratorReuse` (`matcher_relations.go`) drains the binding relation via `executor.ForEach`, which returns, in priority order, an iteration error then a `Close()` error; a non-nil result aborts the match (`return nil, err`) instead of materializing a truncated `bindingTuples`.

2. `nonReusingIterator.Next` (`matcher_iterator_nonreusing.go`) captures `currentScan.Error()` (then `Close()`'s error, which cannot mask the iteration error) into `it.err` before discarding the scan, and returns `false` immediately on any error rather than scanning further bindings. This mirrors the existing `validatingVBoundIterator` pattern in the same package.

Regression coverage lives in `storage/nonreusing_matcher_error_propagation_test.go`: a deferred-error storage scan double pins site #2 directly on `nonReusingIterator`, and a deferred-error binding relation pins site #1 through `matchWithoutIteratorReuse`. Both were verified to fail (empty error chain) before the fix.

## Summary

The non-reusing storage matcher path consumes iterators without checking `Iterator.Error()` after `Next()` returns false. This can launder deferred storage failures into clean query results.

This is the same bug class as the iterator-error propagation fixes documented in `BUG_ITERATOR_ERRORS_DROPPED_AT_PUBLIC_BOUNDARIES.md`, `BUG_ITERATOR_ERRORS_DROPPED_IN_MATERIALIZATION_PATHS.md`, and `BUG_RELATION_TRANSFORMS_DROP_ITERATOR_ERRORS.md`: a source iterator can stop because of a real error, but the consuming path treats it as normal exhaustion.

Two loops are affected:

1. materializing the binding relation before creating `nonReusingIterator`
2. consuming each per-binding storage scan inside `nonReusingIterator.Next`

## Code Evidence

### 1. Binding relation materialization ignores `Error()` and `Close()`

`matchWithoutIteratorReuse` drains the binding relation into `bindingTuples`, then closes the iterator without checking either the deferred iterator error or the close error:

```go
var bindingTuples []executor.Tuple
it := bindingRel.Iterator()
for it.Next() {
	tuple := it.Tuple()
	tupleCopy := make(executor.Tuple, len(tuple))
	copy(tupleCopy, tuple)
	bindingTuples = append(bindingTuples, tupleCopy)
}
it.Close()
```

If `bindingRel` is a streaming relation backed by storage, a decode/blob/CRDT failure may only appear through `it.Error()` after `Next()` returns false.

### 2. Per-tuple storage scans ignore deferred scan errors

`nonReusingIterator.Next` loops over the current scan and handles immediate `Datom()` errors, but when `Next()` returns false it closes the scan without checking `currentScan.Error()`:

```go
if it.currentScan != nil {
	for it.currentScan.Next() {
		datom, err := it.currentScan.Datom()
		if err != nil {
			it.err = err
			return false
		}
		// ...
	}

	// Done with current scan
	it.currentScan.Close()
	it.currentScan = nil
}
```

This misses the exact failure mode used by `KeyOnlyIterator` and `CRDTResolvingIterator`: `Next()` returns false and `Error()` carries the real failure.

## Why This Matters

The storage layer intentionally defers some failures to iterator `Error()`:

- Tier-3 blob lookup/decode failures
- key decode failures in `KeyOnlyIterator`
- CRDT resolving failures, including unique-walk sub-scans
- wrapped iterator failures that propagate through `Error()`

Most of the executor has been hardened to preserve these errors. This path is a remaining hand-rolled loop outside the guarded materialization utilities.

If this path is selected, a query can return:

1. an empty successful result,
2. a truncated successful result, or
3. a result missing rows for some binding tuples,

even though storage reported a real failure.

## Expected Behavior

Every iterator-consuming loop in this path should preserve the iterator outcome:

- after draining `bindingRel`, check `it.Error()` and `it.Close()`
- after each `currentScan` is exhausted, check `currentScan.Error()` and `currentScan.Close()`
- if either reports an error, store it on the returned iterator's `err` field so the query boundary sees it

## Actual Behavior

Both loops treat `Next() == false` as normal exhaustion and discard the only error channel for deferred failures.

## Suggested Fix

In `matchWithoutIteratorReuse`, replace the manual binding materialization with the same error-preserving pattern used elsewhere:

```go
if err := executor.ForEach(bindingRel, func(tuple executor.Tuple) error {
	tupleCopy := make(executor.Tuple, len(tuple))
	copy(tupleCopy, tuple)
	bindingTuples = append(bindingTuples, tupleCopy)
	return nil
}); err != nil {
	return nil, err
}
```

In `nonReusingIterator.Next`, before discarding `currentScan`, capture the first non-nil error from `currentScan.Error()` and `currentScan.Close()` into `it.err`.

Be careful not to let `Close()` mask a more specific iterator/decode error.

## Tests Needed

Add regression coverage for both loss sites:

1. A binding relation whose iterator yields one tuple and then reports a deferred error. `matchWithoutIteratorReuse` should return an error or produce a relation whose iterator reports that error.
2. A `nonReusingIterator` current scan that returns `Next() == false` with a non-nil `Error()`. The outer iterator must surface that error.
3. End-to-end storage-backed query coverage where the planner/matcher selects `NoReuse`, and a deferred storage error is injected after partial output.

This should follow the style of `relation_ops_error_propagation_test.go` and `relation_input_parallel_correctness_test.go`: use a small failing iterator that models the storage contract directly, then add an integration-level test if fault injection is practical.
