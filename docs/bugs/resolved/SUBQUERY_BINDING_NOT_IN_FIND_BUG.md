# Bug: Subquery Binding Variable Not in Find Causes Filter to be Skipped

**Status: FIXED** (2025-12-17)

## Summary

When a subquery binding variable is used in a subsequent where clause for filtering, but is NOT included in the `:find` clause, the filter appears to be optimized away. The query returns results that should have been filtered out.

## Reproduction

```datalog
;; Setup: A task with two status values at different tx times
;; - status=:status/complete at tx1
;; - status=:status/invalidated at tx2 (tx2 > tx1)

;; BROKEN QUERY: ?maxTx not in :find
[:find ?charName ?type
 :in $ ?scenario ?completeStatus
 :where 
 [?task :task/scenario ?scenario]
 [?task :task/type ?type]
 [?task :task/character ?char]
 [?char :character/name ?charName]
 [(q [:find (max ?tx) :in $ ?t :where [?t :task/status _ ?tx]] $ ?task) [[?maxTx]]]
 [?task :task/status ?completeStatus ?maxTx]]

;; Expected: 0 results (max tx has status=invalidated, not complete)
;; Actual: Returns the task (filter is skipped)
```

```datalog
;; WORKING QUERY: ?maxTx included in :find
[:find ?charName ?type ?maxTx
 :in $ ?scenario ?completeStatus
 :where 
 [?task :task/scenario ?scenario]
 [?task :task/type ?type]
 [?task :task/character ?char]
 [?char :character/name ?charName]
 [(q [:find (max ?tx) :in $ ?t :where [?t :task/status _ ?tx]] $ ?task) [[?maxTx]]]
 [?task :task/status ?completeStatus ?maxTx]]

;; Expected: 0 results
;; Actual: 0 results (correct)
```

## Analysis

The bug appears to be in the query planner/optimizer. When `?maxTx` is not in the `:find` clause:

1. The planner sees `?maxTx` is only used internally
2. It may incorrectly optimize away phases that depend on `?maxTx`
3. The final pattern `[?task :task/status ?completeStatus ?maxTx]` is not properly evaluated with the subquery result

When `?maxTx` IS in `:find`:
1. The planner must project `?maxTx`
2. All phases that produce `?maxTx` are properly evaluated
3. The filter works correctly

## Workaround

Include the subquery binding variable in the `:find` clause, even if you don't need it in the result:

```datalog
[:find ?charName ?type ?maxTx  ;; Include ?maxTx even though we don't use it
 ...]
```

Then ignore the extra column in the result.

## Impact

Any query that:
1. Uses a subquery with a binding
2. Uses that binding in a subsequent where clause for filtering
3. Does NOT project the binding in `:find`

...will silently return incorrect results (the filter is skipped).

## Discovered

2024-12-14, while implementing `GetCharacterTaskStatuses` in narrative-generators.

## Root Cause

The bug was in the planner's `Keep` calculation logic. When determining which symbols a phase needs to keep for later phases, the code correctly kept symbols that were **inputs** to subqueries, but did **not** keep symbols that were **outputs** of subqueries when those output symbols already existed in the current phase.

When Phase 1 had a pattern `[?task :task/status ?completeStatus ?maxTx]` that bound `?maxTx`, and Phase 2 had a subquery that also produced `?maxTx`, the planner should have kept `?maxTx` in Phase 1's `Keep` list so it could be **joined** with the subquery's output in Phase 2.

Without `?maxTx` in Keep, it was dropped between phases, so Phase 2's subquery output couldn't join with Phase 1's value.

## Fix

The fix was applied to two locations:
1. `datalog/planner/planner_phases.go` - `determinePhaseKeepSymbols` function
2. `datalog/planner/phase_reordering.go` - `updatePhaseSymbols` function

Both locations now check if a subquery's output symbols (from TupleBinding, RelationBinding, or CollectionBinding) already exist in the current phase's available symbols, and if so, keep them.

## Test

See `datalog/storage/subquery_binding_not_in_find_bug_test.go` for the regression test.

## Files Modified

- `datalog/planner/planner_phases.go` - Added subquery output handling to Keep calculation
- `datalog/planner/phase_reordering.go` - Same fix in `updatePhaseSymbols`
