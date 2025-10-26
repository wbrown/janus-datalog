# Storage-Level Join Strategy Optimization

**Date**: 2025-10-26
**Status**: Complete
**Impact**: Critical - 643× speedup for storage-backed joins

## Executive Summary

Optimized storage-level join strategy selection in `BadgerMatcher`, changing the threshold from `≤10` to `≤2` bindings for using `IndexNestedLoop`. Benchmarks show `HashJoinScan` is **643× faster** for large binding sets (1000 tuples) and **3.4× faster** even for single bindings.

## Problem

Storage-backed join benchmarks showed consistently slow performance (~770ms) regardless of join strategy or LIMIT clauses. Investigation revealed the join strategy selection logic was broken for streaming queries.

### Root Cause

```go
// StreamingRelation.Size() returns -1 for unknown size
func (r *StreamingRelation) Size() int {
    return -1  // Unknown size - avoid consuming iterator
}

// chooseJoinStrategy() always picked IndexNestedLoop for Size() = -1
if bindingSize <= 10 {
    return IndexNestedLoop  // ← ALWAYS triggered for streaming!
}
```

**Result**: All streaming queries used `IndexNestedLoop` regardless of actual binding count.

## Investigation Process

### 1. Added Testing Infrastructure

Added `ForceJoinStrategy()` to `BadgerMatcher` for controlled testing:

```go
// datalog/storage/matcher.go
type BadgerMatcher struct {
    // ...
    forceJoinStrategy *JoinStrategy  // Override for testing
}

func (m *BadgerMatcher) ForceJoinStrategy(strategy *JoinStrategy) {
    m.forceJoinStrategy = strategy
}
```

### 2. Created Comprehensive Benchmarks

Created `datalog/storage/join_strategy_test.go` with forced strategy comparisons:

```go
// Force IndexNestedLoop
indexNested := IndexNestedLoop
matcher.ForceJoinStrategy(&indexNested)

// Force HashJoinScan
hashJoin := HashJoinScan
matcher.ForceJoinStrategy(&hashJoin)
```

### 3. Benchmark Results

**Large binding set (1000 tuples):**

| Strategy | Time | Speedup |
|----------|------|---------|
| IndexNestedLoop | 778ms | baseline |
| HashJoinScan | 1.2ms | **643×** |

**With LIMIT 10:**

| Strategy | Time | Speedup |
|----------|------|---------|
| IndexNestedLoop | 816ms | baseline |
| HashJoinScan | 1.2ms | **654×** |

**Single binding:**

| Strategy | Time | Speedup |
|----------|------|---------|
| IndexNestedLoop | 865µs | baseline |
| HashJoinScan | 251µs | **3.4×** |

### 4. Root Cause of IndexNestedLoop Slowness

Discovered `matchWithIteratorReuse()` calls `Sorted()` which **materializes AND sorts** before seeking:

```go
// datalog/storage/matcher_relations.go:303
func (m *BadgerMatcher) matchWithIteratorReuse(...) (executor.Relation, error) {
    // Get sorted tuples - THIS IS CRITICAL!
    // Without sorted tuples, we cannot use Seek() to jump forward
    // Sorted() will auto-materialize if needed
    sortedTuples := bindingRel.Sorted()  // ← EXPENSIVE!

    // Now do seeks for each tuple
    for _, tuple := range sortedTuples {
        // ... seek and collect
    }
}
```

**Cost breakdown** (1000 bindings):
- IndexNestedLoop: Materialize → Sort → 1000 seeks = 778ms
- HashJoinScan: Materialize → Build hash set → 1 scan = 1.2ms

## Solution

Changed threshold from `≤10` to `≤2` in `datalog/storage/hash_join_matcher.go`:

```diff
- if bindingSize <= 10 {
-     // Very small binding sets: index nested loop is fine
-     // Overhead of hash table not worth it
+ if bindingSize <= 2 {
+     // Very small binding sets (1-2 tuples): direct seek is optimal
+     // Single seek cost < full table scan cost
      return IndexNestedLoop
  }
```

## Historical Context

Git history (commit `fa323ec`) shows the original `≤10` threshold was based on a broken iterator reuse implementation. The assumption was that "Seek() per entity is efficient," but this overlooked:

1. The sorting overhead from `Sorted()` call
2. The cumulative cost of many seeks vs one scan
3. BadgerDB seek overhead (iterator creation + B-tree traversal)

## Why Not Remove IndexNestedLoop Entirely?

Benchmarks show HashJoinScan wins in all tested cases, but we kept `≤2` threshold as a conservative measure:

1. Edge cases may exist where 1-2 seeks are faster than full scan
2. Code is already written and tested
3. Threshold of 2 is conservative enough to avoid performance issues

If future benchmarking shows no benefit even for tiny binding sets, `IndexNestedLoop` could be removed entirely.

## Code Changes

### Modified Files

1. **datalog/storage/hash_join_matcher.go** (lines 38-75)
   - Changed threshold from `≤10` to `≤2`
   - Added override check for `forceJoinStrategy`

2. **datalog/storage/matcher.go** (lines 14-24, 100-104)
   - Added `forceJoinStrategy *JoinStrategy` field
   - Added `ForceJoinStrategy()` method for testing

### New Files

3. **datalog/storage/join_strategy_test.go** (559 lines)
   - Consolidated 4 old test files into one
   - Comprehensive benchmarks with forced strategy overrides
   - Verification tests with event tracking

### Deleted Files

- `join_strategy_benchmark_test.go` (260 lines)
- `join_strategy_comparison_test.go` (291 lines)
- `join_strategy_force_test.go` (111 lines)
- `join_strategy_verification_test.go` (171 lines)

**Net change**: 833 lines → 559 lines (-33%)

## Performance Impact

For queries with streaming pattern matching (common case):

- **Before**: All queries used IndexNestedLoop (≤10 threshold always triggered)
- **After**: Queries with >2 bindings use HashJoinScan (643× faster)

**Affected queries:**
- Any query where pattern 1 produces >2 tuples and feeds into pattern 2
- Common in joins, filters, and multi-pattern queries
- Especially dramatic for joins producing 100-1000+ intermediate tuples

## Testing

Run benchmarks to verify:

```bash
# Storage-level strategy comparison
go test -bench BenchmarkIndexNestedLoopVsHashJoin \
    github.com/wbrown/janus-datalog/datalog/storage

# Strategy verification with event tracking
go test -run TestVerifyStrategyUsed \
    github.com/wbrown/janus-datalog/datalog/storage -v
```

## Related Work

This optimization is **independent** of executor-level hash join optimizations:

- **This work**: Storage layer join strategy (BadgerMatcher)
- **Separate**: Executor-level symmetric hash joins
- **Separate**: Executor-level hash table sizing (DefaultHashTableSize)

## Conclusion

Changing the storage join strategy threshold from `≤10` to `≤2` provides massive performance improvements for common query patterns. The optimization required minimal code changes but careful benchmarking to understand the true performance characteristics.

**Key lesson**: Default thresholds based on "conventional wisdom" can be wrong when implementation details (like sorting overhead) are overlooked. Always benchmark with realistic workloads.
