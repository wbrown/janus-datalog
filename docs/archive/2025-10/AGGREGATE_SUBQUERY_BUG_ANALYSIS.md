# Aggregate Subquery Nil Bug - Root Cause Analysis

**Date**: 2025-10-09
**Status**: 🔴 ROOT CAUSE IDENTIFIED
**Severity**: CRITICAL - Silent data corruption

## Summary

Multiple aggregate subqueries return `nil` values instead of correct aggregates. Only the last subquery in a sequence returns correct values.

## Root Cause

**Input variables are incorrectly added to the `:find` clause during subquery execution, causing grouped aggregation instead of single aggregation.**

### Expected Behavior

Original subquery (high/low example):
```clojure
[(q [:find (max ?h) (min ?l)
     :in $ ?sym ?y ?m ?d
     :where [?b :price/symbol ?sym]
            [?b :price/time ?time]
            [(year ?time) ?py]
            [(= ?py ?y)]
            ...]
    $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]
```

Should execute as:
- **Find clause**: `[:find (max ?h) (min ?l)]` - pure aggregation, no group-by
- **Aggregation type**: Single aggregation over all matching tuples
- **Expected result**: One row with two aggregate values

### Actual Behavior

Debug output shows the subquery executes with:
```
[ExecuteAggregations] Element 0: FindVariable - Symbol=?sym
[ExecuteAggregations] Element 1: FindVariable - Symbol=?py
[ExecuteAggregations] Element 2: FindVariable - Symbol=?pm
[ExecuteAggregations] Element 3: FindVariable - Symbol=?pd
[ExecuteAggregations] Element 4: FindAggregate - Function=max, Arg=?h
[ExecuteAggregations] Element 5: FindAggregate - Function=min, Arg=?l
```

This is treated as:
- **Find clause**: `[:find ?sym ?py ?pm ?pd (max ?h) (min ?l)]` - mixed variables and aggregates
- **Aggregation type**: Grouped aggregation by `[?sym ?py ?pm ?pd]`
- **Problem**: Input variables `?sym`, `?py`, `?pm`, `?pd` are treated as group-by columns

## Why This Causes nil Values

With grouped aggregation:

1. **First subquery execution** (for a specific date):
   - Groups by `[?sym=<identity> ?py=2025 ?pm=7 ?pd=31]`
   - Computes aggregates for that group
   - Returns result with those grouping columns

2. **Join with outer query**:
   - Outer query doesn't have columns `?py`, `?pm`, `?pd`
   - Join tries to match on these non-existent columns
   - **Result**: Empty join or mismatched columns
   - Aggregate columns end up as `nil`

3. **Last subquery** (volume) works because:
   - It's the final operation
   - No subsequent joins to corrupt the result
   - Values survive to final output

## Evidence

### Debug Output
```
[ExecuteAggregations] Extracted 2 aggregates, 4 groupByVars: [?sym ?py ?pm ?pd]
```

These variables should **NOT** be groupByVars - they're input parameters!

### Test Results

**With real data** (gopher-street):
```
date=2025-7-31, open=<nil>, high=<nil>, low=<nil>, close=<nil>, volume=2.50250078e+08
```

**With fresh test data** (our BadgerDB test):
```
date=2025-1-15, high=125, low=99, open=100, close=122, volume=10000
```
✅ Works correctly!

**Why the discrepancy?**
- Fresh test has different data distribution
- Bug may be triggered by specific join patterns in real data
- Or our test doesn't fully reproduce the production query structure

## Location of Bug

The transformation happens somewhere in the subquery execution path before aggregation:

**Likely culprit**:
- `datalog/executor/subquery.go` - subquery execution logic
- Specifically around where subquery results are prepared for aggregation
- Input variables from `:in` clause are incorrectly being added to `:find` clause

**Code path**:
1. `executeSubquery()` or `executeSubqueryParallel()` in `subquery.go`
2. Subquery query plan is modified or executed incorrectly
3. `ExecuteAggregations()` receives find elements that include input variables
4. Groups by input variables instead of treating as single aggregation

## Reproduction

**Consistent reproduction**: Run gopher-street OHLC tests
```bash
cd /Users/wbrown/go/src/github.com/wbrown/gopher-street
go test -run TestDailyOHLCPerformance -v
```

**Output**:
```
Sample: date=2025-7-31, open=<nil>, high=<nil>, low=<nil>, close=<nil>, volume=2.50250078e+08
```

**Debug mode** (with `debugAggregation = true` in aggregation.go):
```
[ExecuteAggregations] Extracted 2 aggregates, 4 groupByVars: [?sym ?py ?pm ?pd]
```

## Fix Required

**DO NOT** add input variables to the find clause when executing subqueries.

The `:find` clause should contain ONLY what was specified in the original subquery:
- `[:find (max ?h) (min ?l)]` - NOT `[:find ?sym ?py ?pm ?pd (max ?h) (min ?l)]`

Input variables are for **filtering/binding**, not for **grouping**.

## Related Code

- `datalog/executor/subquery.go:73-108` - `executeSubquerySequential`
- `datalog/executor/subquery.go:136-183` - `executeSubqueryParallel`
- `datalog/executor/aggregation.go:30-113` - `ExecuteAggregations`
- `datalog/executor/aggregation.go:175-268` - `executeSingleAggregation`
- `datalog/executor/aggregation.go:270-406` - `executeGroupedAggregation`

## Reproduction

**Test**: `datalog/executor/subquery_find_clause_bug_test.go`
- `TestSubqueryFindClauseBug`: Two subqueries with same inputs triggers decorrelation
- `TestSubqueryMultiValueFindClauseBug`: Two subqueries with intermediate variables (like OHLC)

**Debug output** showing the bug:
```
[ExecuteAggregations] Element 0: FindVariable - Symbol=?sym
[ExecuteAggregations] Element 1: FindVariable - Symbol=?pd
[ExecuteAggregations] Element 2: FindAggregate - Function=min, Arg=?l
[ExecuteAggregations] Extracted 1 aggregates, 2 groupByVars: [?sym ?pd]
```

**Expected**: `[:find (min ?l)]` with 0 groupByVars
**Actual**: `[:find ?sym ?pd (min ?l)]` with 2 groupByVars

## Bug Location

**File**: `datalog/planner/decorrelation.go`
**Function**: `mergeSubqueriesInGroup` (lines 406-520)
**Problematic code**: Lines 467-482

```go
// Add grouping keys to :find (use pattern variables, not formal parameters)
groupingVars := make([]query.Symbol, 0)
for _, key := range formalParams {
    if key == "$" {
        continue // Skip database marker
    }
    // Use the pattern variable that corresponds to this formal parameter
    if patternVar, found := formalToPattern[key]; found {
        groupingVars = append(groupingVars, patternVar)
        allFindElements = append(allFindElements, query.FindVariable{Symbol: patternVar})  // <-- BUG!
    } else {
        // Formal parameter is directly used in patterns
        groupingVars = append(groupingVars, key)
        allFindElements = append(allFindElements, query.FindVariable{Symbol: key})  // <-- BUG!
    }
}
```

This code **unconditionally** adds all input parameters and intermediate variables as `FindVariable` elements, turning pure aggregations into grouped aggregations.

## Next Steps

1. ✅ Root cause identified - decorrelation.go lines 467-482
2. ✅ Found exact location where input variables are added to find clause
3. ✅ Reproduction tests written and passing
4. ⏳ Fix: Only add grouping keys if original find clause already has FindVariable elements
5. ⏳ Verify fix with both reproduction tests and gopher-street tests

## Timeline

- **2025-10-08**: Bug reported by gopher-street team
- **2025-10-09**: Root cause identified - input vars treated as group-by vars
