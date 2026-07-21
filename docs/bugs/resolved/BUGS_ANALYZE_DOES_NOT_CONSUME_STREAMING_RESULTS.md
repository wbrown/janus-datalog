# BUG: `Analyze()` Returns Before Streaming Query Work Is Fully Executed

**Date**: 2026-05-31 **Severity**: Observability / Correctness (Medium) **Status**: ✅ RESOLVED (2026-05-31) **Affected**: `Database.Analyze`, annotation timing, error reporting for lazy query execution

## Resolution

`Analyze()` now fully drives the query before returning (behavior 1 from Expected Behavior — the least-surprising option given the `EXPLAIN ANALYZE` analogy). After `ExecuteWithRelations`, it consumes the lazy result via `executor.ForEach` — which enforces the deferred-iterator-error contract (returns the first iteration/close error) — copying each tuple into a slice, then builds an `executor.NewMaterializedRelation` from the drained tuples and records `TotalTime` *after* consumption. So the captured events, timing, and any error now cover the whole query (storage scans, joins, filters, decode/decompression), not just plan/pipeline construction, and the returned `Result` is materialized and re-iterable for the caller.

Note: `StreamingRelation.Materialize()` was NOT used — on a streaming relation it only sets a `shouldCache` flag and defers the actual consumption to the first `Iterator()` call, so it would not have driven execution inside `Analyze()`. `ForEach` is what forces the work.

Verification (`datalog/storage/`):
- `explain_test.go::TestAnalyze_ConsumesStreamingResults` — asserts `Analyze()` captures the storage-scan event and returns a correctly-sized (3), re-iterable result. Verified to FAIL against the unfixed code (no scan event; `Size()` returned -1).
- `query_boundary_error_test.go::TestAnalyze_SurfacesBlobDecodeError` — drives a real deferred Tier-3 blob-decode failure (existing `writeTier3ValueThenCorruptBlob` harness, already proven across nine query boundaries) and asserts `Analyze()` returns the error rather than a clean lazy result.
- Existing `TestAnalyze` (5 subtests) still pass; full `go test ./...` green.

The `String()` "Result tuples: (streaming...)" branch no longer triggers for `Analyze` results, since the result is now materialized — `Size()` returns the real count.

## Summary

`Database.Analyze()` is documented as an `EXPLAIN ANALYZE`-style API that executes the query and captures execution statistics. In the current streaming architecture, however, much of the actual work happens when the returned `Relation` is iterated.

`Analyze()` currently returns the lazy relation without consuming it. As a result, `TotalTime` and captured events can describe plan construction and lazy pipeline construction rather than full query execution. Deferred iterator errors can also surface only after `Analyze()` has returned.

## Root Cause

`Analyze()` calls `ExecuteWithRelations`, records elapsed time immediately, and returns the relation:

```go
result, err := exec.ExecuteWithRelations(executor.NewContext(collector.Handler()), q, inputRelations)
if err != nil {
    return nil, fmt.Errorf("query execution failed: %w", err)
}

totalTime := time.Since(startTime)

return &AnalyzeResult{
    Plan:      plan,
    Result:    result,
    Events:    events,
    TotalTime: totalTime,
}, nil
```

But `Relation` is intentionally lazy in the default configuration. A `StreamingRelation` reports unknown size instead of consuming its iterator:

```go
// Streaming behavior: return -1 to indicate unknown size
// DO NOT call Iterator() here - that would break single-use semantics
return -1
```

The execution work, storage decoding, joins, filters, and deferred iterator errors may not occur until a caller later iterates `AnalyzeResult.Result`.

## Why This Matters

The API name and documentation imply that `Analyze()` measures actual execution. In a lazy engine, returning before the result is consumed means:

1. **Timing can under-report**: `TotalTime` may exclude storage scans, iterator composition work, result materialization, sorting, aggregation finalization, or deferred decode/decompression failures.

2. **Events can be incomplete**: annotation events emitted during iteration can be absent from `AnalyzeResult.Events` at the time the result is returned.

3. **Errors can be delayed past the API boundary**: iterator errors are part of the engine's correctness contract. If they happen during result consumption, `Analyze()` can return nil error even though fully executing the query would fail.

4. **Tests can mask the problem**: a test that checks only that `Analyze()` returns events may pass even if scan/join events have not happened yet.

## Evidence In Existing Tests

`TestAnalyze` already has a soft expectation for storage scan events:

```go
if !foundScan {
    t.Log("Note: No storage scan events captured (may depend on executor path)")
}
```

That log is consistent with the bug: depending on the lazy path, the storage scan may not have occurred during `Analyze()` itself.

## Reproduction Sketch

Use a query whose result is backed by a streaming storage scan, and an annotation handler that records storage events.

```go
result, err := db.Analyze(`[:find ?e :where [?e :person/name ?name]]`)
require.NoError(t, err)

// At this point, storage scan events may be missing or incomplete.
eventsBefore := len(result.Events)

_, err = executor.CollectTuples(result.Result, nil)
require.NoError(t, err)

// Events emitted during iteration happened after Analyze returned.
eventsAfter := len(result.Events)
require.Greater(t, eventsAfter, eventsBefore)
```

A stronger reproducer would use a deferred-error iterator, such as a corrupted Tier-3 blob decode path, and verify that `Analyze()` returns nil while consuming `AnalyzeResult.Result` returns the actual error.

## Expected Behavior

`Analyze()` should either:

1. fully drive the query and include all execution work, timings, events, and iterator errors in its return value, or
2. explicitly document that it is a lazy analysis wrapper and that callers must consume `Result` before reading final timing/events.

Given the `EXPLAIN ANALYZE` analogy in the API docs, the first behavior is the least surprising.

## Fix Direction

Materialize or otherwise fully consume the result inside `Analyze()` while preserving a reusable result relation for the caller.

Possible shape:

```go
tuples := make([]executor.Tuple, 0)
if err := executor.ForEach(result, func(t executor.Tuple) error {
    tuples = append(tuples, copyTuple(t))
    return nil
}); err != nil {
    return nil, fmt.Errorf("query execution failed: %w", err)
}

materialized := executor.NewMaterializedRelation(result.Symbols(), tuples)
totalTime := time.Since(startTime)
```

The actual implementation should use existing relation materialization helpers so tuple-copy and deferred-error semantics stay consistent with the rest of the executor.

## Verification Plan

1. Add a test where storage scan annotations are only emitted during iteration; verify `Analyze()` captures them.
2. Add a deferred iterator error test; verify `Analyze()` returns that error instead of returning a successful lazy result.
3. Verify `AnalyzeResult.TotalTime` includes the forced consumption.
4. Verify the returned `AnalyzeResult.Result` remains reusable by callers.
5. Preserve current plan output and event formatting.
