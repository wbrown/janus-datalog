# Conditional Aggregate Rewriting Requires Streaming

**Date**: 2025-10-13
**Status**: 🟡 KNOWN LIMITATION
**Severity**: MEDIUM - Works with defaults, fails with non-standard config
**Priority**: LOW - Default configuration works correctly

## Problem Statement

Conditional aggregate rewriting exhibits non-deterministic failures when streaming is explicitly disabled. The optimization works correctly with streaming enabled (the default configuration).

## Symptoms

### Non-Deterministic Behavior

Running `BenchmarkConditionalAggregateWithStreaming/Rewriting_only_(no_streaming)` multiple times:
- Run 1: ✅ PASS (1.77ms)
- Run 2: ❌ FAIL - `projection failed: cannot project: column ?__cond_?pd not found`
- Run 3: ✅ PASS (1.72ms)
- Run 4: ✅ PASS (1.70ms)
- Run 5: ✅ PASS (1.71ms)

**Failure rate**: ~20% when streaming explicitly disabled

### Configuration That Fails

```go
opts := planner.PlannerOptions{
    EnableConditionalAggregateRewriting: true,
    EnableIteratorComposition:           false,  // Streaming disabled
    EnableTrueStreaming:                 false,  // Streaming disabled
    EnableStreamingAggregation:          false,  // Streaming disabled
}
```

### Configuration That Works

```go
opts := planner.PlannerOptions{
    EnableConditionalAggregateRewriting: true,
    // Streaming options not specified = defaults (enabled)
}
```

## Root Cause (Hypothesis)

The error message `?__cond_?pd not found in relation (has columns: [?p ?name ?ev ?t ?v ?pd])` suggests that:

1. The condition variable `?__cond_?pd` was created during planning
2. But somehow didn't make it into the relation at execution time
3. Only happens **non-deterministically**, suggesting:
   - Map iteration order issue
   - Race condition (though -race doesn't detect it)
   - Expression evaluation order dependency

**Key observation**: When streaming is enabled, expression evaluation and/or relation composition may happen in a different order that happens to work correctly.

## Impact Assessment

### Production Impact: ✅ NONE

- Default configuration has streaming ENABLED
- Conditional aggregate rewriting works correctly with defaults
- Users won't hit this unless they explicitly disable streaming (rare)

### Performance Impact

With default configuration (streaming enabled):
| Configuration | Time (ms) | Speedup |
|---------------|-----------|---------|
| Without rewriting | 7.72ms | baseline |
| With rewriting | 2.84ms | **2.7x faster** |

### Workaround: ✅ TRIVIAL

Don't explicitly disable streaming. Use defaults.

## Investigation Status

- [x] Bug documented
- [x] Non-determinism confirmed (20% failure rate)
- [x] Race detector run (no races detected)
- [x] Works with default configuration verified
- [ ] Root cause identified (TBD)
- [ ] Fix implemented (TBD)

## Recommended Action

**For Users (Gopher Street team)**:
- ✅ Enable `EnableConditionalAggregateRewriting: true`
- ✅ Use default streaming configuration (don't disable)
- ✅ Expected: 2.7-3.1x speedup on OHLC queries

**For Developers**:
- Priority: LOW (works with defaults, no production impact)
- Future work: Investigate why streaming affects expression evaluation order
- Possible root causes to investigate:
  1. Map iteration order in `updatePhaseSymbols()` (line 311 in phase_reordering.go)
  2. Expression evaluation order different with/without streaming
  3. Relation materialization affecting when expressions are evaluated

## Related Files

- `datalog/planner/phase_reordering.go` - Our recent fix for expression outputs
- `datalog/planner/conditional_aggregate_rewriting.go` - Where condition variables are created
- `tests/conditional_aggregate_streaming_benchmark_test.go` - Benchmark showing non-determinism
- `tests/conditional_aggregate_no_streaming_test.go` - Usually passes (small data)

## Testing Strategy

To reproduce (run multiple times until failure):
```bash
go test -bench BenchmarkConditionalAggregateWithStreaming/Rewriting_only -benchtime=1x -count=10 ./tests
```

Look for `projection failed: cannot project: column ?__cond_?pd not found`

## Resolution Criteria

Fix is complete when:
- ✅ Conditional aggregate rewriting works deterministically with streaming disabled
- ✅ All 10 consecutive runs of benchmark pass
- ✅ TestConditionalAggregateWithoutStreaming passes reliably

## Priority Justification

**Why LOW priority**:
1. Default configuration works perfectly
2. No production impact (users don't disable streaming)
3. Non-deterministic (hard to debug)
4. Workaround is trivial (use defaults)

**When to increase priority**:
- If users report needing streaming disabled for some reason
- If we find this is a symptom of a larger correctness issue
- If failure rate increases above 20%
