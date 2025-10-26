# Decorrelation Pure Aggregation Bug - Fix Summary

## The Bug

**Symptom**: Multiple aggregate subqueries return `nil` values instead of correct aggregates. Only the last subquery in a sequence returns correct values.

**Example** (from gopher-street OHLC query):
```
date=2025-7-31, open=<nil>, high=<nil>, low=<nil>, close=<nil>, volume=2.50250078e+08
```

**Root Cause**: The decorrelation optimization incorrectly added input parameters as `FindVariable` elements to pure aggregation subqueries, turning them from single aggregations into grouped aggregations.

### What Happened

1. **Original subquery** (high/low example):
   ```clojure
   [:find (max ?h) (min ?l)
    :in $ ?sym ?y ?m ?d
    :where [?b :price/symbol ?sym]
           [?b :price/time ?time]
           [(year ?time) ?py]
           [(= ?py ?y)]
           ...]
   ```

2. **Decorrelation incorrectly transformed it to**:
   ```clojure
   [:find ?sym ?py ?pm ?pd (max ?h) (min ?l)  ; <- WRONG!
    :where ...]
   ```

3. **What should have been**:
   ```clojure
   [:find (max ?h) (min ?l)  ; <- Pure aggregation, no grouping
    :where ...]
   ```

4. **The consequence**:
   - Pure aggregation: `[:find (max ?h)]` → Single aggregate value
   - Grouped aggregation: `[:find ?sym (max ?h)]` → Aggregate per group
   - Input vars became grouping vars → Wrong aggregation type → Join failures → `nil` values

## The Fix

**Location**: `datalog/planner/decorrelation.go` lines 64-84

**Change**: Modified `extractCorrelationSignature()` to distinguish between:
- **Pure aggregations**: ALL find elements are aggregates (e.g., `[:find (max ?x) (min ?y)]`)
- **Grouped aggregations**: Has both aggregates AND non-aggregate variables (e.g., `[:find ?group (max ?x)]`)

**Before**:
```go
// Check if query is aggregate
isAggregate := false
for _, findElem := range subqPlan.NestedPlan.Query.Find {
    if findElem.IsAggregate() {
        isAggregate = true
        break
    }
}
```

**After**:
```go
// Check if query is a GROUPED aggregate (has both aggregates AND non-aggregate variables)
// Pure aggregations (only aggregates, no grouping vars) should NOT be decorrelated
// because adding grouping keys changes them from single aggregation to grouped aggregation
hasAggregates := false
hasNonAggregateVars := false
for _, findElem := range subqPlan.NestedPlan.Query.Find {
    if findElem.IsAggregate() {
        hasAggregates = true
    } else if _, ok := findElem.(query.FindVariable); ok {
        hasNonAggregateVars = true
    }
}

// Only decorrelate GROUPED aggregations, not pure aggregations
isGroupedAggregate := hasAggregates && hasNonAggregateVars
```

**Result**: Pure aggregations are no longer decorrelated, preserving their semantics.

## Verification

### Reproduction Test
Created `datalog/executor/subquery_find_clause_bug_test.go`:
- **TestSubqueryFindClauseBugWithAnnotations**: Uses annotations to detect incorrect find clauses
- Checks: `groupby_count` should be `0` for pure aggregations
- **Before fix**: FAILED with `groupby_count:1`
- **After fix**: PASSED with `groupby_count:0`

### Gopher-street Test
```bash
# Before fix:
Sample: date=2025-7-31, open=<nil>, high=<nil>, low=<nil>, close=<nil>, volume=2.50250078e+08

# After fix:
Sample: date=2025-7-31, open=112.63, high=119.59, low=111.5, close=114.24, volume=2.4085954e+07
```

✅ **All values now correct!**

## Supporting Infrastructure

### Annotations Added
- New event type: `AggregationExecuted`
- Captures:
  - `aggregate_count`: Number of aggregates
  - `groupby_count`: Number of grouping variables
  - `groupby_vars`: List of grouping variable names
  - `find_elements`: String representation of find clause

### Context-Aware Aggregation
- Added `ExecuteAggregationsWithContext()` to capture metadata
- Wired up in:
  - `executor.go`
  - `executor_iteration.go`
  - `executor_parallel.go`

## Impact

### ✅ Fixed
- Pure aggregation subqueries now compute correctly
- No more `nil` aggregate values
- Gopher-street OHLC queries work correctly

### ⚠️ Side Effects
- CSE/decorrelation performance tests fail because they **expect** pure aggregations to be decorrelated
- These tests need updating - the queries still return correct results, just without the (broken) optimization
- Decorrelation still works for **grouped aggregations** (which is safe)

## Lessons Learned

1. **Decorrelation is incompatible with pure aggregations** - Adding grouping keys fundamentally changes query semantics
2. **Tests are essential** - The bug existed because we had no test detecting incorrect find clause structure
3. **Annotations are powerful** - They caught the root cause (wrong find clause) not just the symptom (nil values)
4. **Simple data hides bugs** - Reproduction test passed with simple data, only failed with annotations
