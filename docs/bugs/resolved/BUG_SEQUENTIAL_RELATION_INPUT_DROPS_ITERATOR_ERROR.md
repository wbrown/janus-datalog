# BUG: Sequential Relation-Input Iteration Drops Input Iterator Errors

**Date**: 2026-05-30 **Severity**: Medium — non-default execution path can report clean partial results **Status**: ✅ RESOLVED (2026-05-31) **Affected**: `executor.executeRealizedWithRelationInputIterationSequential`

## Resolution

The sequential path now follows the same error-priority policy as the parallel path. `Close()` is made explicit instead of deferred, so it can be inspected before per-tuple results are combined: after the iteration loop, `it.Error()` (deferred iteration failure) is checked first and `it.Close()` second, so a cleanup error cannot mask the real cause. On a per-tuple execution error the iterator is closed best-effort and that error is returned without consulting `Close()`.

Regression coverage in `executor/relation_input_sequential_error_propagation_test.go`, both verified to fail (`got nil`) before the fix:

- `TestRelationInputSequential_PropagatesDeferredInputIteratorError` — the iteration relation yields one tuple then defers `errInjectedIterator`; `ExecuteWithRelations` (parallel disabled) must surface it, not return the prefix's results as a clean success.
- `TestRelationInputSequential_PropagatesInputCloseError` — the iteration relation exhausts cleanly but its `Close()` reports an error, which must propagate rather than be discarded by the former deferred `Close()`.

## Summary

The sequential `RelationInput` execution path drains the input relation without checking `Iterator.Error()` after iteration completes. If the input iterator fails after yielding a prefix of tuples, the executor can return results for the prefix and silently ignore the failure.

The parallel `RelationInput` path already handles this correctly: it checks the producer iterator's `Error()` and `Close()` before returning. The sequential path should follow the same contract.

## Code Evidence

`executeRealizedWithRelationInputIterationSequential` iterates `iterationRelation` directly:

```go
it := iterationRelation.Iterator()
defer it.Close()

for it.Next() {
	result, err := prepared.Run(ctx, it.Tuple())
	if err != nil {
		return nil, fmt.Errorf("iteration execution failed: %w", err)
	}

	if result != nil {
		allResults = append(allResults, result)
	}
}
```

There is no `it.Error()` check after the loop. The deferred `it.Close()` return value is also discarded.

By contrast, the parallel path explicitly preserves both signals:

```go
iterErr := iter.Error()
closeErr := iter.Close()
// ...
if iterErr != nil {
	return nil, iterErr
}
// ...
if closeErr != nil {
	return nil, closeErr
}
```

## Why This Matters

`RelationInput` queries execute once per input tuple. If the input relation is streaming and fails partway through, the sequential path may:

1. execute the query for the tuples before the failure,
2. combine those per-tuple results,
3. return success,
4. never tell the caller the input stream failed.

That produces a plausible but incomplete result set.

This is especially risky because the rest of the codebase treats deferred iterator errors as a hard correctness contract. A path that bypasses the contract makes behavior depend on the parallel-subquery configuration.

## Expected Behavior

After the input loop finishes, the sequential path should check:

1. `it.Error()`
2. `it.Close()`, if no iteration error occurred

Any non-nil error should abort the query before combining and returning partial per-tuple results.

## Actual Behavior

`Next() == false` is treated as normal exhaustion, and `Close()` is deferred with its error ignored.

## Suggested Fix

Use the same priority policy as the parallel path:

```go
iterErr := it.Error()
closeErr := it.Close()
if iterErr != nil {
	return nil, iterErr
}
if closeErr != nil {
	return nil, closeErr
}
```

Because the current code uses `defer it.Close()`, the loop should be reshaped so the close error can be inspected before result combination.

Do not let `Close()` mask an earlier per-tuple execution error.

## Tests Needed

Add a sequential counterpart to `TestRelationInputParallel_PropagatesDeferredIteratorError`:

1. Build a `RelationInput` query over an input relation whose iterator yields at least one tuple and then reports a deferred error.
2. Disable parallel subqueries via `exec.DisableParallelSubqueries()`.
3. Verify `ExecuteWithRelations` returns an error that unwraps to the injected sentinel.
4. Verify the query does not return clean partial results.

Also add a close-error variant if there is already a reusable close-failing test iterator in the executor tests.
