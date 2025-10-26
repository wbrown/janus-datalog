# BUG: Streaming Iterator Tuple Copying - Critical Data Loss

**Date**: 2025-10-24
**Severity**: CRITICAL - Data loss in query execution
**Status**: Partially fixed (table formatter only)
**Impact**: Queries return empty results or incorrect data with streaming enabled

## Summary

When `EnableTrueStreaming: true`, iterators may reuse tuple buffers for performance. Code that collects tuples into slices without copying ends up with all slice entries pointing to the same memory location, causing catastrophic data loss.

## Root Cause

The `Iterator.Tuple()` interface doesn't specify whether returned tuples are stable or may be reused. With streaming optimizations, many iterators return a pointer to a reusable buffer that changes on each `Next()` call.

When code collects tuples like this:
```go
for it.Next() {
    tuples = append(tuples, it.Tuple())  // BUG: No copy!
}
```

All entries in `tuples` point to the same memory location containing the last iteration's data (or garbage).

## Evidence from Production

### Symptom 1: CLI Returns Empty Results

**Test**: `test_ohlc_performance.sh` TEST 1
```bash
./datalog-cli -db datalog-db -query '[:find (count ?e) :where [?e ?a ?v]]'
```

**Before fix**: Returns empty table (but should show 28040)
**After fixing table_formatter.go**: Returns `28040` ✅

### Symptom 2: Query Execution Returns 0 Tuples

**Test**: `test_ohlc_performance.sh` TEST 2 with `-verbose`

**Annotations show**:
```
[0µs] === Phase 1 (:price/symbol) starting
[18µs] Pattern([?e :price/symbol ?s]) → Relation([?e ?s])
[23µs] Pattern([?e :price/symbol ?s]) → Relation([?e ?s], 0 Tuples)
[0µs] Scan([[?e :price/symbol ?s], AEVT, bound: ?) → 15552 datoms in <nil>
[10.6ms] Pattern([?e :price/time ?time]) → Relation([?e ?time])
[10.6ms] Pattern([?e :price/time ?time]) → Relation([?e ?time], 0 Tuples)
```

**Analysis**: Storage scan found **15552 datoms** but relation shows **0 tuples**!

This is not a display bug - the relation actually has no data because tuple collection failed.

### Symptom 3: Query Hangs Forever

**Query**:
```
[:find ?year ?month ?day (min ?open) (max ?high) (min ?low) (max ?close) (sum ?volume)
 :where [?s :symbol/ticker "CRWV"]
        [?e :price/symbol ?s]
        ...]
```

**Behavior**: Runs for 15+ minutes with no output, eventually times out

**Cause**: Pattern matching returns empty relations due to tuple copying bug, causing downstream operations to process empty data or hang waiting for results.

## Files Affected

### Fixed
1. **datalog/executor/table_formatter.go:43** ✅
   - Collects tuples for markdown table display
   - Fixed by adding proper tuple copying

### Critical - Breaks Query Execution
2. **datalog/storage/matcher_relations.go:241** ❌ CRITICAL
   - Collects binding tuples for pattern matching
   - Breaks all queries with multiple patterns
   - Line 241:
     ```go
     for it.Next() {
         bindingTuples = append(bindingTuples, it.Tuple())  // BUG!
     }
     ```

### Additional Files With Same Pattern (18 total)

Found via `grep -r "append(.*,\s*it\.Tuple())"`:

3. datalog/executor/subquery.go
4. datalog/executor/query_executor.go
5. datalog/executor/relation.go
6. datalog/executor/executor.go
7. datalog/executor/streaming_union.go
8. datalog/executor/union_relation.go
9. datalog/executor/executor_utils.go
10. datalog/executor/executor_iteration.go
11. tests/conditional_aggregate_rewriting_integration_test.go

And 7 more files (test files and documentation)

## The Fix

Replace all instances of:
```go
for it.Next() {
    tuples = append(tuples, it.Tuple())
}
```

With:
```go
for it.Next() {
    tuple := it.Tuple()
    tupleCopy := make(Tuple, len(tuple))
    copy(tupleCopy, tuple)
    tuples = append(tuples, tupleCopy)
}
```

## Why This Wasn't Caught Earlier

1. **Tests use small datasets**: Most tests have <100 tuples, making bugs less obvious
2. **Tests inspect tuples immediately**: Many tests call `it.Tuple()` and use values right away, before buffer is reused
3. **Materialized relations hide the bug**: When `EnableTrueStreaming: false`, iterators don't reuse buffers
4. **No contract documentation**: `Iterator.Tuple()` interface doesn't specify tuple lifetime

## How This Bug Was Introduced

The streaming optimizations (commits 4a394cb, 4f3b742, 15d196d, 78c930a) enabled `EnableTrueStreaming: true` by default to achieve 28× performance improvements. This activated iterator buffer reuse throughout the codebase, exposing latent tuple copying bugs that were harmless with materialized relations.

## Timeline

**2025-10-24**:
- User reported CLI returning empty results in gopher-street tests
- Investigation revealed TEST 1 returns empty (should be 28040 datoms)
- Fixed `table_formatter.go` - TEST 1 now works ✅
- TEST 2 still hangs - discovered deeper bug in query execution
- Annotations reveal relations have 0 tuples despite successful scans
- Grepped codebase: found 18 files with same pattern
- Identified `matcher_relations.go:241` as critical blocker

## Impact Assessment

**Severity**: CRITICAL

**Affected Operations**:
- ✅ Simple queries (1 pattern) - May work if no tuple collection needed
- ❌ Multi-pattern queries - Broken due to matcher_relations.go:241
- ❌ Aggregations - May return wrong results
- ❌ Subqueries - Likely broken
- ❌ Joins - Depends on whether tuples are collected
- ✅ Display/Table() - Fixed in table_formatter.go

**User Impact**:
- Queries return empty results
- Queries hang indefinitely
- Silent data corruption possible (tuples with wrong values)

## Next Steps

1. **Immediate**: Fix `matcher_relations.go:241` (blocks all multi-pattern queries)
2. **High Priority**: Audit and fix all 18 files found by grep
3. **Testing**: Add regression test that verifies tuple copying with streaming enabled
4. **Documentation**: Document `Iterator.Tuple()` lifetime contract
5. **Long Term**: Consider making tuples immutable or adding copy-on-write semantics

## Test Case

```go
// TestStreamingTupleCopying verifies tuple copying works with streaming
func TestStreamingTupleCopying(t *testing.T) {
    columns := []query.Symbol{"x", "y", "z"}
    tuples := []Tuple{
        {int64(1), int64(10), int64(100)},
        {int64(2), int64(20), int64(200)},
        {int64(3), int64(30), int64(300)},
    }

    rel := NewMaterializedRelationWithOptions(columns, tuples, ExecutorOptions{
        EnableTrueStreaming: true,
    })

    // Collect tuples
    var collected []Tuple
    it := rel.Iterator()
    defer it.Close()
    for it.Next() {
        tuple := it.Tuple()
        tupleCopy := make(Tuple, len(tuple))
        copy(tupleCopy, tuple)
        collected = append(collected, tupleCopy)
    }

    // Verify all tuples are different
    assert.Equal(t, 3, len(collected))
    assert.Equal(t, int64(1), collected[0][0])
    assert.Equal(t, int64(2), collected[1][0])
    assert.Equal(t, int64(3), collected[2][0])
}
```

## Related Issues

- Streaming architecture: docs/archive/2025-10/STREAMING_ARCHITECTURE_COMPLETE.md
- Options propagation fixes: commits 4a394cb, 4f3b742, 15d196d, 78c930a
- Performance improvements: PERFORMANCE_STATUS.md (28× streaming speedup)

## Lessons Learned

1. **Explicit contracts matter**: Iterator interfaces should document tuple lifetime
2. **Performance vs correctness**: Streaming optimizations exposed latent bugs
3. **Test coverage gaps**: Need tests with streaming enabled and large datasets
4. **Annotations are critical**: Only caught this via verbose query annotations
5. **Tuple immutability**: Consider making tuples immutable to prevent this class of bugs
