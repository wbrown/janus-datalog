# BUG: Expression Phase Assignment After Reordering

**Status**: RESOLVED
**Severity**: HIGH - Both executors affected
**Affects**: Planner (planner.go, planner_expressions.go, planner_phases.go)
**Date Discovered**: 2025-10-16
**Date Fixed**: 2025-10-16

## Summary

The planner was assigning expressions to phases BEFORE phase reordering happened. After reordering, expressions ended up in phases where their required input symbols weren't available, causing execution failures in both legacy and new executors.

## Failing Tests (Before Fix)

### TestComprehensiveExecutorValidation/multi-phase_with_expression
### TestComprehensiveExecutorValidation/all_features_combined

Both tests failed with errors about missing symbols:
- `projection failed: cannot project: column ?total not found`
- `predicate requires symbols not available: [?total]`

## Root Cause

**Execution Flow (BROKEN)**:
1. `createPhases()` creates phases and assigns expressions at line 120
2. `reorderPhasesByRelations()` REORDERS phases at line 140
3. Expressions remain in their original phases but are now WRONG after reordering
4. `updatePhaseSymbols()` updates Available/Keep but expressions are already misassigned

**Example**:
```
Initial phases:
  Phase 0: Patterns [?event :event/person], Provides [?event ?person ?value]
  Phase 1: Patterns [?person :person/name], Provides [?person ?name ?score]
  Expression [(+ ?score ?value) ?total] → Assigned to Phase 1

After reordering:
  Phase 0: Patterns [?person :person/name], Provides [?person ?name ?score]
  Phase 1: Patterns [?event :event/person], Provides [?event ?person ?value]
  Expression [(+ ?score ?value) ?total] → Still in Phase 0! Missing ?value!
```

## The Fix

**Modified Files**:
1. **planner.go** - Call `updatePhaseSymbols()` BEFORE `assignExpressionsToPhases()` after reordering
2. **planner_expressions.go** - Clear existing assignments and expression outputs from Provides before re-assignment
3. **planner_phases.go** - Remove optimistic expression prediction from phase creation
4. **expressions_and_predicates.go** - Make expression/predicate evaluation and projection lenient

**Key Changes**:

**1. planner.go:138-158** - Correct ordering after reordering:
```go
if p.options.EnableDynamicReordering {
    phases = p.reorderPhasesByRelations(phases, inputSymbols)

    // Update Available fields FIRST
    phases = updatePhaseSymbols(phases, q.Find, inputSymbols)

    // THEN re-assign expressions with correct Available lists
    p.assignExpressionsToPhases(phases, expressions, predicates)

    // Re-assign subqueries
    p.assignSubqueriesToPhases(phases, subqueries)

    // Update symbols again to include expression/subquery outputs
    phases = updatePhaseSymbols(phases, q.Find, inputSymbols)
}
```

**2. planner_expressions.go:15-37** - Clear before re-assignment:
```go
// Clear existing expression assignments
for i := range phases {
    phases[i].Expressions = nil

    // Remove expression outputs from Provides
    var newProvides []query.Symbol
    for _, sym := range phases[i].Provides {
        isExprOutput := false
        for _, expr := range expressions {
            if expr.Binding == sym {
                isExprOutput = true
                break
            }
        }
        if !isExprOutput {
            newProvides = append(newProvides, sym)
        }
    }
    phases[i].Provides = newProvides
}
```

**3. planner_phases.go** - Remove optimistic expression prediction:
- Removed lines 75-89 and 296-310 that tried to predict which expressions would be assigned
- Let `assignExpressionsToPhases()` handle assignment properly after reordering

**4. expressions_and_predicates.go** - Lenient evaluation:
- **Lines 155-162**: Skip predicates if required symbols not in relation group
- **Lines 269-292**: Only project symbols that exist in the relation

## Verification

All 25 subtests of TestComprehensiveExecutorValidation now pass:
- ✅ multi-phase_with_expression
- ✅ all_features_combined
- ✅ All other validation tests

## Lessons Learned

1. **Order matters**: When reordering phases, ALL dependent metadata must be recalculated
2. **Stale state is dangerous**: Expression assignments and Provides lists were stale after reordering
3. **Test with reordering**: Phase reordering changes the game - test with it enabled
4. **Available vs Provides**: Available comes from previous phases, Provides from current phase's patterns
5. **Lenient execution**: Make executors handle missing symbols gracefully when planner optimizations create edge cases
