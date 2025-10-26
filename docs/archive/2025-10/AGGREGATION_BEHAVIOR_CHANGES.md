# Impact of Aggregation Behavior Changes

**Date**: October 2025
**Status**: Critical Bug Fix - Production Impact
**Affected Component**: Query Planner - Decorrelation Optimization

---

## What Changed

We fixed a **critical correctness bug** where pure aggregation subqueries returned `nil` values. The fix changes how the query planner treats aggregations during decorrelation optimization.

## Behavioral Changes

### Before Fix (Broken)
```clojure
;; Pure aggregation subquery
[:find (max ?price) (min ?price)
 :in $ ?category
 :where [?p :product/category ?category]
        [?p :product/price ?price]]

;; Decorrelation INCORRECTLY transformed to:
[:find ?category (max ?price) (min ?price)  ; Added ?category!
 :where ...]

;; Result: Grouped aggregation instead of pure aggregation
;; → Join failures → nil values
```

### After Fix (Correct)
```clojure
;; Pure aggregation stays as-is (NOT decorrelated)
[:find (max ?price) (min ?price)  ; No grouping variables
 :where ...]

;; Result: Single aggregate value per execution
;; → Correct values returned
```

## Impact Categories

### 1. ✅ Correctness (FIXED)

**Problem Solved**: Queries with multiple pure aggregation subqueries now return correct values instead of `nil`.

**Real-world example** (gopher-street OHLC):
```
BEFORE: date=2025-7-31, open=<nil>, high=<nil>, low=<nil>, close=<nil>, volume=2.50250078e+08
AFTER:  date=2025-7-31, open=112.63, high=119.59, low=111.5, close=114.24, volume=2.4085954e+07
```

**Affected queries**:
- Any query with 2+ pure aggregation subqueries sharing inputs
- Common pattern: OHLC (Open-High-Low-Close) financial queries
- Analytics queries with multiple aggregates per group

### 2. ⚠️ Performance (Mixed)

#### Pure Aggregations: No Optimization (Correct Trade-off)

**Before**: Attempted CSE/decorrelation but **broke the query**
**After**: No decorrelation, sequential execution

**Performance impact**:
- **Correctness > Speed**: Working results are infinitely better than broken results
- Sequential execution of subqueries (slightly slower)
- No join overhead from incorrect grouping (removes pathological cases)

**Example workload** (gopher-street OHLC with 870 tuples):
```
Sequential WITHOUT decorrelation: ~400ms
Sequential WITH decorrelation:    ~415ms (broken results)
Parallel WITHOUT decorrelation:   ~400ms

Net impact: ~0ms (decorrelation didn't help anyway)
```

#### Grouped Aggregations: Still Optimized

**Good news**: Grouped aggregations (which CAN benefit from decorrelation) are still optimized:

```clojure
;; This STILL gets decorrelated (correct and safe)
[:find ?category (max ?price)  ; Has grouping variable ?category
 :in $ ?store
 :where ...]
```

### 3. 🎯 Semantic Correctness

The fix preserves the fundamental difference between aggregation types:

**Pure Aggregation** (global):
```clojure
[:find (max ?price)]  ; → Single value: 150.0
```

**Grouped Aggregation** (per group):
```clojure
[:find ?category (max ?price)]  ; → Multiple values:
;; ["Electronics", 150.0]
;; ["Books", 50.0]
```

**Why this matters**: Adding input variables as grouping keys changes semantics:
- Pure → Grouped changes from "global max" to "max per input"
- Join conditions expect specific column structure
- Wrong structure → join failures → `nil` values

### 4. 📊 Test Suite Changes

**Updated expectations** for 13 tests:
- `TestExtractCorrelationSignature` - Pure aggs not marked for decorrelation
- `TestDetectDecorrelationOpportunities` - Expect 0 opportunities
- `TestCSELargeScale` - Filter groups: 2,1,2,1 → 0,0,0,0
- `TestCSEWithAndWithoutParallel` - Filter groups: 2,1,2,1 → 0,0,0,0
- `TestCSEPerformanceImpact` - Expect 0 filter groups
- `TestCSEOpportunity` - Expect 0 merged queries
- `TestOHLCCSEOpportunity` - Expect 0 merged queries
- `TestSimpleDecorrelation` - Expect decorrelation NOT applied
- `TestDecorrelationActuallyWorks` - Remove 1.15x speedup requirement
- `TestDecorrelationAnnotations` - Expect NO decorrelation events

**Added new test**:
- `TestDetectDecorrelationOpportunities_GroupedAggregates` - Proves grouped aggs still work

### 5. 🔍 Detection Infrastructure

**New annotation support** added:
```go
// AggregationExecuted event captures:
- aggregate_count: 2
- groupby_count: 0  // Key detection - should be 0 for pure aggs
- groupby_vars: []
- find_elements: ["(max ?h)", "(min ?l)"]
```

**Why important**: Catches root cause (wrong find clause structure) not just symptom (nil values).

## User-Facing Impact

### ✅ Queries That Now Work

Any query matching this pattern now returns correct results:

```clojure
[:find ?date ?metric1 ?metric2 ?metric3
 :where
   [?e :entity/date ?date]

   ;; Multiple pure aggregation subqueries sharing inputs
   [(q [:find (max ?x) :in $ ?param :where ...] $ ?e) [[?metric1]]]
   [(q [:find (min ?y) :in $ ?param :where ...] $ ?e) [[?metric2]]]
   [(q [:find (sum ?z) :in $ ?param :where ...] $ ?e) [[?metric3]]]]
```

**Before**: `metric1=<nil>, metric2=<nil>, metric3=123.45` (last value works)
**After**: `metric1=100.0, metric2=50.0, metric3=123.45` (all correct)

### ⚠️ Queries With Different Performance

Queries with pure aggregations may execute **slightly slower** but with **correct results**.

**Trade-off is worth it**:
- Broken fast results: Useless
- Correct slower results: Valuable
- Performance can be optimized later (see below)

### ✅ Queries Still Optimized

Grouped aggregations continue to benefit from CSE/decorrelation:

```clojure
;; This pattern STILL gets optimized
[:find ?category ?max-price ?total-stock
 :where
   [?c :category/name ?category]
   [(q [:find ?cat (max ?p) :in $ ?cat :where ...] $ ?c) [[_ ?max-price]]]
   [(q [:find ?cat (sum ?s) :in $ ?cat :where ...] $ ?c) [[_ ?total-stock]]]]
```

## Technical Details

### Root Cause

The decorrelation optimization in `datalog/planner/decorrelation.go` was treating ALL aggregations the same way. It added input parameters to the `:find` clause to enable CSE (Common Subexpression Elimination).

**Problem**: This fundamentally changes query semantics for pure aggregations.

### The Fix

Modified `extractCorrelationSignature()` to distinguish:

```go
// BEFORE: Any aggregate triggers decorrelation
isAggregate := false
for _, findElem := range subqPlan.NestedPlan.Query.Find {
    if findElem.IsAggregate() {
        isAggregate = true
        break
    }
}

// AFTER: Only GROUPED aggregates trigger decorrelation
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

### Verification

**Created reproduction test**: `datalog/executor/subquery_find_clause_bug_test.go`
- Uses annotations to detect incorrect find clause structure
- Before fix: FAILED with `groupby_count:1` (incorrect)
- After fix: PASSED with `groupby_count:0` (correct)

**Real-world validation**: gopher-street OHLC queries
- Before: All aggregates except last returned `nil`
- After: All aggregates return correct values

## Future Optimization Opportunities

The fix prioritizes **correctness over performance**. Performance can be recovered through safer optimizations:

### 1. Parallel Subquery Execution
Execute independent pure aggregation subqueries in parallel:
```
Sequential: SubQ1 → SubQ2 → SubQ3 (300ms + 300ms + 300ms = 900ms)
Parallel:   SubQ1 ∥ SubQ2 ∥ SubQ3 (max(300ms, 300ms, 300ms) = 300ms)
```

**Status**: Already implemented in `executor_iteration.go` with `enableParallelSubqueries` flag.

### 2. Query Plan Reuse
Share compiled plans across iterations:
```
Current:  Plan 870 times (slow)
Optimized: Plan once, execute 870 times (fast)
```

**Status**: Proposed in TODO.md

### 3. Smarter CSE for Pure Aggregations
Decorrelation can't work, but **other** CSE techniques might:
- Shared base relation scanning
- Materialized intermediate results
- Batch aggregation computation

**Status**: Research needed

## Migration Guide

### If You Have Pure Aggregation Queries

**Good news**: Your queries now return correct results automatically. No code changes needed.

**Performance note**: If you notice slower execution, consider:
1. Enabling parallel subquery execution (if not already enabled)
2. Restructuring as a single subquery with multiple aggregates:

```clojure
;; BEFORE: Multiple pure aggregation subqueries
[(q [:find (max ?p) :in $ ?cat :where ...] $ ?c) [[?max-price]]]
[(q [:find (min ?p) :in $ ?cat :where ...] $ ?c) [[?min-price]]]
[(q [:find (sum ?s) :in $ ?cat :where ...] $ ?c) [[?total-stock]]]

;; AFTER: Single subquery with multiple aggregates (more efficient)
[(q [:find (max ?p) (min ?p) (sum ?s)
     :in $ ?cat
     :where [?prod :product/category ?cat]
            [?prod :product/price ?p]
            [?prod :product/stock ?s]]
    $ ?c) [[?max-price ?min-price ?total-stock]]]
```

### If You Have Grouped Aggregation Queries

**No changes needed**: Grouped aggregations continue to be optimized via decorrelation.

## Summary

| Aspect | Before Fix | After Fix |
|--------|-----------|-----------|
| **Correctness** | ❌ `nil` values | ✅ Correct values |
| **Pure Agg Performance** | 🐛 Broken | ⚠️ No optimization |
| **Grouped Agg Performance** | ✅ Optimized | ✅ Still optimized |
| **Query Semantics** | 🐛 Changed incorrectly | ✅ Preserved correctly |
| **Test Coverage** | ❌ Bug undetected | ✅ Reproduction test added |
| **User Experience** | 💔 Data corruption | ✅ Reliable results |

**Bottom line**: This is a **critical bug fix** that trades a broken optimization for correct results. Performance can be recovered through safer optimizations that don't violate query semantics.

## Related Documentation

- `DECORRELATION_BUG_FIX.md` - Detailed technical fix summary
- `datalog/executor/subquery_find_clause_bug_test.go` - Reproduction test
- `datalog/planner/decorrelation.go` - Implementation
- `TODO.md` - Future optimization plans

## Questions?

If you encounter issues related to this change:
1. Check if your query uses pure aggregations (`:find` with only aggregates)
2. Verify results are now correct (no more `nil` values)
3. If performance is critical, see "Future Optimization Opportunities" above
4. File an issue at https://github.com/wbrown/janus-datalog/issues
