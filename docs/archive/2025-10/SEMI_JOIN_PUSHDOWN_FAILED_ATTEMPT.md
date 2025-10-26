# Semi-Join Pushdown Optimization - Failed Attempt

**Date**: 2025-10-07
**Status**: ❌ FAILED - Design fundamentally flawed
**Performance Impact**: -32% (regression from 41s to 54s)

## Motivation

Hourly OHLC query performance showed only 1.89× speedup vs promised 10-20×:
- **Daily OHLC**: 22 days, 88 subqueries → 242ms (16.5× speedup) ✅
- **Hourly OHLC**: 260 hours, 1,040 subqueries → 41,331ms (1.89× speedup) ❌

### Root Cause Analysis

Decorrelation computes ALL groups in database (1000+) then filters with hash join to the 260 needed:
- Evidence: Hourly subqueries are 14.4× slower per-execution than daily (39.7ms vs 2.75ms)
- Hypothesis: Computing unnecessary groups wastes time
- Desired: Transform "compute 1000 groups, filter to 260" → "compute only 260 groups"

## Attempted Solution: Semi-Join Pushdown

### Design

Pass the input relation's grouping keys to constrain which groups the merged queries compute:

```go
// In executeDecorrelatedSubqueries:
// 1. Project input relation to correlation keys
projected, err := inputRelation.Project(correlationKeys) // e.g., [?s ?year ?month ?day ?hour]

// 2. Deduplicate to get distinct groups
projected = deduplicateRelation(projected) // 3397 → 260 distinct groups

// 3. Pass as RelationInput to merged queries
constrainedInputs := []Relation{projected}
result, err := executePhasesWithInputs(ctx, exec, mergedPlan, constrainedInputs)
```

### Implementation Changes

**1. Modified merged query `:in` clause** (`datalog/planner/decorrelation.go:574`):
```go
// Before:
In: []query.InputSpec{query.DatabaseInput{}}

// After:
inputSpecs := []query.InputSpec{query.DatabaseInput{}}
if len(groupingVars) > 0 {
    inputSpecs = append(inputSpecs, query.RelationInput{Symbols: groupingVars})
}
```

**2. Added `IsDecorrelatedMerged` flag** (`datalog/planner/types.go:28`):
```go
type QueryPlan struct {
    Query  *query.Query
    Phases []Phase
    IsDecorrelatedMerged bool // Bypass iteration for semi-join pushdown
}
```

**3. Bypassed RelationInput iteration** (`datalog/executor/executor.go:146`):
```go
// Before:
if hasRelationInput(plan.Query) && len(inputRelations) > 0 {
    return e.executeWithRelationInputIteration(ctx, plan, inputRelations)
}

// After:
if hasRelationInput(plan.Query) && len(inputRelations) > 0 && !plan.IsDecorrelatedMerged {
    return e.executeWithRelationInputIteration(ctx, plan, inputRelations)
}
```

**4. Filtered `$` from correlation keys** (`datalog/executor/subquery_decorrelation.go:63`):
```go
var projectionKeys []query.Symbol
for _, key := range decorPlan.CorrelationKeys {
    if key != "$" {
        projectionKeys = append(projectionKeys, key)
    }
}
projected, err := inputRelation.Project(projectionKeys)
```

## Why It Failed: Massive Cross-Product

### Expected Behavior
Merged query should **filter** to only compute groups present in input relation:
```
Input: 260 distinct [?sym ?year ?month ?day ?hour] tuples
Expected: Process only these 260 groups
```

### Actual Behavior
Merged query **cross-joined** input relation with patterns, creating massive explosion:

```
⚠️ Relation([?sym ?py ?pm ?pd ?ph], 260 Tuples) × Relation([?b ?sym], 8256 Tuples)
   → Relation([?sym ?py ?pm ?pd ?ph ?b], 2,146,560 Tuples)
```

**Full cascade**:
1. `260 × 8,256 = 2,146,560` tuples (cross-product on ?sym)
2. `2,146,560 × 8,256 = 2,146,560` tuples (join on ?b with ?time)
3. `2,146,560 × 8,481 = 2,205,060` tuples (join on ?b with ?high)
4. `2,205,060 × 8,483 = 2,301,000` tuples (join on ?b with ?low)
5. Time extraction expressions process **2.3 MILLION tuples** instead of ~13K
6. Filtering by `[(= ?py ?y)]` etc. happens AFTER explosion

### Performance Impact

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| **Hourly OHLC** | 41s | 54s | **-32% slower** ❌ |
| **Daily OHLC** | 242ms | 982ms | **-305% slower** ❌ |
| **Merged query 0** | ~60ms | 7,852ms | **-130× slower** ❌ |
| **Merged query 1** | ~65ms | 5,115ms | **-78× slower** ❌ |

## Root Cause: Semantic Mismatch

### The Problem

`RelationInput` has **iteration semantics**, not **filter semantics**:

```clojure
;; RelationInput means: "Execute query ONCE PER TUPLE"
[:find ?result
 :in $ [[?x ?y] ...]  ; Iterate over each [?x ?y] tuple
 :where ...]

;; What we needed: "JOIN with this relation to FILTER groups"
[:find ?result
 :in $ [?x ?y]  ; Use as filter/constraint
 :where ...]
```

### Why Bypassing Iteration Failed

When we bypassed `executeWithRelationInputIteration`:
- Input relation went to `BindQueryInputs`
- Created a relation with correlation key columns: `Relation([?sym ?py ?pm ?pd ?ph], 260 tuples)`
- This relation entered the phase as **just another relation**
- Phase executor naturally **joined** it with pattern results
- Since `?sym` is the ONLY shared column, got a cross-product on the other columns

### What We Actually Need

The merged query needs to **use the input as a filter**, not a join partner:

**Option 1: WHERE clause constraints**
```clojure
[:find (max ?h) (min ?l)
 :in $ [[?sym ?py ?pm ?pd ?ph] ...]
 :where [?b :price/symbol ?sym]
        ;; CRITICAL: Need predicates that constrain to input tuples
        [(contains-tuple? [[?sym ?py ?pm ?pd ?ph] ...] [?sym ?py ?pm ?pd ?ph])]
        ...]
```
But we don't have `contains-tuple?` predicate!

**Option 2: Predicate pushdown**
Transform equality predicates `[(= ?py ?y)]` into range scans BEFORE pattern execution.
But this requires index support for multi-column constraints.

**Option 3: Iteration IS correct (current approach)**
Execute query once per distinct group. This IS semantically correct - each tuple represents one aggregation group to compute.

## Why Iteration Isn't The Problem

Re-examining the performance:
- **Daily**: 22 iterations × ~11ms = 242ms ✅
- **Hourly**: 260 iterations × ~159ms = 41s ⚠️

The real bottleneck is **per-iteration cost**, not iteration count:
- Daily: 11ms per iteration
- Hourly: 159ms per iteration (14.5× slower!)

This suggests the hourly queries are fundamentally more expensive due to:
1. More correlation keys (5-7 vs 4-5)
2. Additional input parameters (?smod, ?emod)
3. Complex outer relation feeding more intermediate tuples

## Lessons Learned

### 1. Datalog ≠ SQL

SQL's `WHERE x IN (SELECT ...)` has clear semi-join semantics.
Datalog's `RelationInput` means iteration, not filtering.

### 2. Join Semantics Are Natural

When you give the executor a relation, it will join it.
There's no "use this as a filter constraint" mechanism.

### 3. Cross-Products Are Evil

Going from 260 tuples to 2.3M tuples destroys performance.
The join explosion happened because most correlation keys weren't in the first pattern.

### 4. The Real Bottleneck

It's not "iteration vs vectorization" - it's **per-query cost**.
Hourly queries are 14.5× slower per-iteration than daily.
Fix THAT, not the iteration mechanism.

## Next Steps

### Reverted Changes

All semi-join pushdown code has been reverted:
- `IsDecorrelatedMerged` flag
- `RelationInput` in merged queries
- Iteration bypass logic
- Projection and deduplication code

### Better Approaches

**1. Analyze Per-Query Cost** ✅ PRIORITY
- Why are hourly iterations 14.5× slower than daily?
- Profile a single hourly subquery execution
- Compare with a single daily subquery execution

**2. Predicate Pushdown (Real)**
- Push `[(= ?py ?y)]` predicates into storage layer as range scans
- Requires multi-column index support
- Much more complex than our failed attempt

**3. Parallel Iteration** ✅ ALREADY EXISTS
- The iteration IS parallelized with worker pools
- Already getting 2-4× speedup from this
- Further optimization: better work distribution

**4. Accept Current Performance**
- 41s for 260 hours = 158ms per hour
- This is actually reasonable for complex OHLC calculations
- The "10-20×" promise was based on simpler queries (daily OHLC)

## Conclusion

Semi-join pushdown via `RelationInput` is **fundamentally incompatible** with Datalog execution semantics. The massive cross-product shows we misunderstood how the executor would use the input relation.

The real optimization opportunity is **reducing per-iteration cost**, not eliminating iteration.

---

**Files Modified** (all reverted):
- `datalog/planner/decorrelation.go` - Added RelationInput to merged queries
- `datalog/planner/types.go` - Added IsDecorrelatedMerged flag
- `datalog/executor/executor.go` - Bypassed iteration for decorrelated queries
- `datalog/executor/subquery_decorrelation.go` - Added projection/deduplication logic

**Commits**: None (changes reverted before commit)
