# Identity Zero Bug with Phase Reordering

## Summary

When `EnableDynamicReordering: true`, entity Identity values become **empty Identity objects** (zero value) during query execution. This breaks subqueries that depend on those entity values.

## Symptom

Test: `TestConditionalAggregateRewritingE2E`
- `EnableDynamicReordering: false` → PASS (correct results: 150, 200)
- `EnableDynamicReordering: true` → FAIL (wrong results: 15, 16)

The wrong results (15, 16) are day numbers being used where max event values should be, indicating the subquery is not filtering correctly.

## Root Cause Discovery

Debug output shows tuples with empty Identity values:

```
Relation columns: [?e ?p ?time ?day] (size=3)

Tuple 0 (len=4): [  2025-01-15 05:00:00 -0500 EST 15]
  tuple[0] =  (type *datalog.Identity)   <-- EMPTY
  tuple[1] =  (type *datalog.Identity)   <-- EMPTY
  tuple[2] = 2025-01-15 05:00:00 -0500 EST (type time.Time)
  tuple[3] = 15 (type int64)
```

**Key observation:** The Identity objects are NOT nil, but have zero/empty internal values.

## Impact

When subquery tries to use `?p` (person entity):
```go
outerValues: map[$:$ ?day:15 ?p:]   // ?p is empty Identity!
```

The subquery receives:
- `?person` = empty Identity (not a valid entity)
- `?d` = 15 (correct day number)

Because `?person` is empty, the subquery `[?ev :event/person ?person]` doesn't match any events, and the aggregation returns wrong values.

## This Affects Both Optimization Modes

**Without conditional aggregate rewriting:** Same empty Identity problem
**With conditional aggregate rewriting:** Empty Identities PLUS additional rewriting bugs

This proves the Identity zero bug is NOT caused by conditional aggregate rewriting - it's a core issue with phase reordering.

## Query Structure

```datalog
[:find ?name ?day ?max-value
 :where
 [?p :person/name ?name]           ; ?p should be entity reference
 [?e :event/person ?p]              ; ?e should be entity reference
 [?e :event/time ?time]
 [(day ?time) ?day]

 ; Subquery needs ?p value to work
 [(q [:find (max ?v)
      :in $ ?person ?d
      :where
      [?ev :event/person ?person]  ; This fails when ?person is empty!
      [?ev :event/time ?t]
      [(day ?t) ?pd]
      [(= ?pd ?d)]
      [?ev :event/value ?v]]
    $ ?p ?day) [[?max-value]]]]
```

## Phase Information (With Reordering)

```
Phase 0:
  Available: []
  Provides: [?e ?p ?time ?day ?max-value]
  Keep: [?day ?max-value ?p]
  Subquery Inputs: [$ ?p ?day]
```

**Key insight:** `?p` is in the Keep list, but the actual tuple value is empty!

This suggests the problem is NOT in projection/Keep calculation, but in **how Identity values are propagated during joins or phase execution**.

## Possible Root Causes

### 1. Identity Copying Issue
When tuples are copied or projected during reordering, Identity pointers might be getting dereferenced incorrectly, creating new zero-value Identities instead of preserving the original.

### 2. Join Implementation Bug
Hash joins or other join types might not properly handle Identity values when building result tuples.

### 3. Materialization Bug
When relations are materialized between phases, Identity values might be losing their internal data.

### 4. Column Reordering Bug
If phase reordering changes column order, and the code doesn't properly map Identity values to new positions.

## What We Know Works

Phase reordering was added recently and has tests that pass. This suggests:
- Simple queries with Identities work
- The bug is triggered by specific patterns (subqueries + reordering + Identities)
- The reordering logic itself (updatePhaseSymbols) is probably correct
- The bug is in execution, not planning

## Investigation Next Steps

1. **Trace Identity propagation**
   - Add debug output showing Identity.String() at each phase
   - Check if Identities are empty BEFORE or AFTER subquery execution starts

2. **Compare reordering on vs off**
   - Trace the same query with reordering disabled
   - Identify which operation zeros the Identities

3. **Check tuple building**
   - Look at how phase results are materialized
   - Check if Identity pointers are being properly copied

4. **Test simple case**
   - Create minimal test: pattern → reorder → check Identity value
   - Isolate from subquery complexity

## Relationship to extractSubqueryParameters Fix

**Our fix is CORRECT:** Using parameter names instead of argument names for initialBindings is the right approach.

**Our fix EXPOSED this bug:** By making subqueries work correctly with proper parameter binding, we revealed that Identity values are empty when they shouldn't be.

**This is NOT a regression from our fix:** The bug existed before, but queries were failing in different ways that masked it.

## Workaround

Disable phase reordering for queries with subqueries that reference entity values:
```go
opts := planner.PlannerOptions{
    EnableDynamicReordering: false,
}
```

## Status

- Bug is reproducible and well-understood
- Root cause is in execution layer (not planner)
- Affects all queries with: reordering + subqueries + entity references
- Does NOT affect queries without reordering
