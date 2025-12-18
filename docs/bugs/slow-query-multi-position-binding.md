# Slow Query Investigation: Multi-Position Binding Performance Issue

**Date**: 2025-12-18
**Status**: RESOLVED
**Severity**: High (was 40x performance regression, now fixed with 60x improvement)

## Resolution Summary

Two fixes were applied:

1. **chooseIndex fix** (`matcher.go:354-374`): When E+A+V are all bound, was creating exact key range including Tx=0, missing all real datoms. Fixed to use `EncodePrefixRange` with (A, E, V) prefix.

2. **Multi-position strategy** (`matcher_strategy.go:170-295`): Added `chooseBestMultiPositionStrategy` that chooses the position with most distinct values for iterator reuse, avoiding the NoReuse fallback that opened N separate iterators.

**Performance Result**: Query time improved from ~40ms to ~652µs (60x speedup)

## Problem Statement

A simple two-pattern query with bound input parameters takes ~41ms when it should take <1ms:

```datalog
[:find ?e :in $ ?map ?code
 :where [?e :entity/map ?map]
        [?e :entity/code ?code]]
```

With inputs: `mapID` (Identity) and `code` ("POI E" string)

## Key Finding: Pattern Order Matters (40x difference!)

| Query Pattern Order | Time |
|---------------------|------|
| `[?e :entity/map ?map]` then `[?e :entity/code ?code]` | **41ms** |
| `[?e :entity/code ?code]` then `[?e :entity/map ?map]` | **~1ms** |

Simply swapping the pattern order provides a 40x speedup.

## Root Cause Analysis

### 1. Multi-Position Binding Triggers NoReuse Strategy

In `datalog/storage/matcher_strategy.go:162-166`:
```go
// Multiple positions bound - future optimization
if len(boundPositions) > 1 {
    // For now, no reuse. Future: implement merge join
    return ReuseStrategy{Type: NoReuse}
}
```

For the second pattern `[?e :entity/code ?code]`:
- `?e` is bound from the first pattern (81 values)
- `?code` is bound from input (1 value)
- Both positions 0 (E) and 2 (V) are bound from the binding relation
- This triggers `NoReuse` strategy

### 2. NoReuse Opens 81 Separate Storage Iterators

The `nonReusingIterator` (in `matcher_iterator_nonreusing.go`) opens a fresh BadgerDB iterator for each binding tuple. With 81 entities from the first pattern, it opens 81 iterators.

### 3. Hash Join Materializes Eagerly

In `datalog/executor/join.go`, `EnableStreamingJoins` is `false` by default (line 273 of `database.go`). This means the hash join uses the materialized (eager) probe path, which iterates through ALL probe tuples immediately.

### 4. Timing Breakdown (from annotations)

```
[join/hash] 492.459µs    - First pattern collapse (fast)
[join/hash] 41.338584ms  - Second pattern collapse (SLOW!)
    left.size: 81
    right.size: -1  (streaming)
    result.size: 1
```

The second hash join takes 41ms to produce just 1 result tuple.

## Why Individual Lookups Are Fast But Query Is Slow

| Test | Time |
|------|------|
| 405 individual entity lookups (5 rounds × 81 entities) | 8ms |
| 5 iterations of slow query | 211ms |
| 5 iterations of optimized query (swapped order) | 5ms |

The individual lookups bypass the query machinery and go directly to storage. The slow query goes through:
1. Query parsing/planning
2. First pattern execution
3. Relation collapse (hash join #1)
4. Second pattern with NonReuse (81 iterator opens)
5. Relation collapse (hash join #2) - **THIS IS THE BOTTLENECK**

## Planner Doesn't Reorder by Selectivity

Both patterns get the same selectivity score (~500) because:
- `[?e :entity/map ?map]`: ?e unbound (+1000), ?map bound (-500) → ~500
- `[?e :entity/code ?code]`: ?e unbound (+1000), ?code bound (-500) → ~500

Without cardinality statistics, the planner can't distinguish that:
- `:entity/map` with bound value returns 81 entities
- `:entity/code` with bound value returns 1 entity

See `datalog/planner/planner_patterns.go:284-343` for selectivity scoring.

## Data Distribution

- Entities for the test map: 81
- Entities with code "POI E": 1 (globally)
- Total `:entity/code` datoms: 81

## Potential Fixes

### Short-term (User Workaround)
Manually order patterns so more selective ones come first.

### Medium-term Options

1. **Improve multi-position binding strategy** (`matcher_strategy.go:162-166`)
   - Instead of NoReuse, detect when one position is more selective
   - Use hash join or merge join approach for multi-position cases
   - Example: If V is bound to 1 value but E is bound to 81 values, scan by V first

2. **Enable schema-based selectivity hints**
   - Use uniqueness constraints from schema for selectivity estimation
   - Mark `:entity/code` as having higher selectivity than `:entity/map`

3. **Implement cardinality estimation**
   - Sample data to estimate attribute cardinalities
   - Use statistics for smarter pattern ordering

4. **Enable streaming joins by default**
   - Change `EnableStreamingJoins` from `false` to `true`
   - This would use lazy evaluation instead of eager materialization

### Long-term
- Statistics-based query optimization (like Selinger's algorithm)
- Cost-based join ordering within phases

## Files Involved

- `datalog/storage/matcher_strategy.go` - Strategy selection for binding patterns
- `datalog/storage/matcher_iterator_nonreusing.go` - NonReuse iterator implementation
- `datalog/storage/matcher_relations.go` - Match orchestration
- `datalog/executor/join.go` - Hash join implementation
- `datalog/storage/database.go:253-282` - Default planner/executor options
- `datalog/planner/planner_patterns.go` - Selectivity scoring

## Test Case Location

The failing test is in:
```
/Users/wbrown/go/src/github.com/NovelAI/narrative-generators/pkg/worldmap/data/query_perf_test.go
```

Run with:
```bash
go test -v -run 'TestSlowQueryByCode|TestPullIntoIsFast' ./pkg/worldmap/data/
```

## Current Investigation State

The investigation has identified that:
1. The multi-position NoReuse fallback is the trigger
2. The hash join's eager probe iteration is where time is spent
3. Pattern order dramatically affects performance
4. Planner selectivity scoring treats both patterns equally

**Next step**: Determine whether to:
- Fix the multi-position binding strategy
- Add selectivity hints from schema
- Or simply document pattern ordering as a best practice

## Reproduction Script

```go
// In narrative-generators repo
db.ExecuteQueryWithInputs(
    `[:find ?e :in $ ?map ?code
     :where [?e :entity/map ?map]
            [?e :entity/code ?code]]`,
    mapID, code)  // Takes ~41ms

// Swapped order - 40x faster
db.ExecuteQueryWithInputs(
    `[:find ?e :in $ ?map ?code
     :where [?e :entity/code ?code]
            [?e :entity/map ?map]]`,
    mapID, code)  // Takes ~1ms
```
