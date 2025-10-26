# BUG: Expression-Only Phases Received Empty Relations

**Date**: 2025-10-14
**Status**: FIXED
**Severity**: Critical - Caused conditional aggregate rewriting to return 0 results
**Commit**: TBD

## Summary

When a query phase contained **only expressions** (no patterns), the phase execution logic failed to pass the previous phase's results to `applyExpressionsAndPredicates()`, resulting in expressions being evaluated on empty relations.

## Symptoms

- Phase completes with 0 tuples when it should have N tuples from previous phase
- No `expression/begin` or `expression/complete` annotations appear in logs
- Phase annotations show `success:true tuple.count:0` despite previous phase having tuples
- Query returns empty results even though data exists

## Root Cause

In `executor_sequential.go`, the phase execution logic:

1. Initializes `availableRelations` with previous phase's results (line 28-30)
2. Executes patterns and builds `independentGroups` through progressive joining (line 33-147)
3. Passes `independentGroups` to `applyExpressionsAndPredicates()` (line 150-157)

**The bug**: When a phase has **zero patterns**, the pattern loop (line 35-147) never executes, leaving `independentGroups` empty. The code then passed this empty slice to `applyExpressionsAndPredicates()` instead of the `availableRelations` containing the previous phase's results.

## Example Query

```datalog
[:find ?name ?day ?max-value
 :where
 [?p :person/name ?name]
 [?e :event/person ?p]
 [?e :event/time ?time]
 [(day ?time) ?day]

 ;; This subquery gets rewritten to conditional aggregate
 [(q [:find (max ?v)
      :in $ ?person ?d
      :where
      [?ev :event/person ?person]
      [?ev :event/time ?t]
      [(day ?t) ?pd]
      [(= ?pd ?d)]
      [?ev :event/value ?v]]
    $ ?p ?day) [[?max-value]]]]
```

After conditional aggregate rewriting, this produces:

- **Phase 2**: Pattern matching (produces 2 tuples: `[?name ?p ?e ?time ?v]`)
- **Phase 3**: Expression-only `[(day ?time) ?day]` and `[(day ?time) ?pd]` ← **BUG HERE**
- **Phase 4**: Expression-only `[[(= ?pd ?day)] ?__cond_?pd]` ← **AND HERE**

Phase 3 and 4 have no patterns, only expressions. Without the fix, they received empty relations.

## The Fix

```go
// Use the collapsed groups
// If phase has no patterns, use availableRelations (results from previous phase)
collapsed := independentGroups
if len(phase.Patterns) == 0 && len(collapsed) == 0 {
    collapsed = availableRelations
}

// Handle expressions and predicates
return e.applyExpressionsAndPredicates(ctx, phase, collapsed)
```

**Location**: `datalog/executor/executor_sequential.go:149-157`

## Detection Method

The bug was found using **annotations**:

1. Added expression annotations (`expression/begin`, `expression/complete`)
2. Ran test and searched for `grep "expression/"`
3. Found **zero** expression annotations despite phase having expressions
4. This revealed expressions weren't being evaluated at all
5. Traced through code to find `independentGroups` was empty

**Key insight**: Absence of expected annotations is as important as presence of error annotations.

## Similar Bug Classes

### General Pattern: "Pure-Type Phases"

Any phase that contains only ONE type of operation (patterns, expressions, predicates, subqueries) is vulnerable to this class of bug.

**Checklist for phase execution**:
- [ ] What if phase has zero patterns?
- [ ] What if phase has zero expressions?
- [ ] What if phase has zero predicates?
- [ ] What if phase has zero subqueries?
- [ ] Does each case properly receive previous phase's results?

### Where to Look

If a phase completes with unexpected tuple counts:

1. **Check annotations first**:
   ```bash
   go test -v ./tests -run YourTest 2>&1 | grep "phase/\|pattern/\|expression/"
   ```
   Look for missing annotation types.

2. **Check phase composition**:
   - Look at test output showing phase structure
   - Count patterns, expressions, predicates, subqueries
   - If any count is zero, suspect this bug class

3. **Check phase boundaries**:
   - What gets passed FROM previous phase? (`previousResult`, `availableRelations`)
   - What gets passed TO next operation? (`collapsed`, `independentGroups`)
   - Are there code paths where these get skipped?

4. **Add debug output**:
   ```go
   fmt.Printf("DEBUG Phase %d: patterns=%d, expressions=%d, collapsed.len=%d, available.len=%d\n",
       phaseIndex, len(phase.Patterns), len(phase.Expressions), len(collapsed), len(availableRelations))
   ```

## Prevention

### Testing Strategy

Every optimization or rewriting that can create new phase structures should test:

1. **Pure pattern phases** (no expressions/predicates)
2. **Pure expression phases** (no patterns) ← **This bug**
3. **Pure predicate phases** (no patterns/expressions)
4. **Empty phases** (edge case - should probably error)

### Code Review Checklist

When modifying phase execution:
- [ ] Does code assume patterns always exist?
- [ ] Does code assume expressions always exist?
- [ ] Is there a code path where `Relations` can be empty when it shouldn't be?
- [ ] Are annotations in place to observe execution?
- [ ] Do tests cover pure-type phases?

## Related Issues

- **Conditional Aggregate Rewriting**: This bug blocked the entire feature because rewriting creates expression-only phases
- **Phase Reordering**: Could create expression-only phases by grouping expressions together
- **CSE (Common Subexpression Elimination)**: Might create dedicated expression phases

## Lessons Learned

1. **Annotations are diagnostic tools**: Use them to understand execution flow, not just performance
2. **Test phase composition edge cases**: Don't just test "normal" multi-operation phases
3. **Absence of expected events is a bug symptom**: If you expect `expression/` events but see none, investigate
4. **Initialize-and-accumulate patterns are fragile**: Code that builds up state in a loop needs careful handling of the "zero iterations" case
5. **Document invariants**: What MUST be true at each phase boundary?

## Phase Execution Invariants

These should ALWAYS be true:

1. **Input Invariant**: Every phase receives either:
   - `nil` (if first phase with no inputs)
   - Previous phase's `Keep` symbols (projected result)

2. **Output Invariant**: Every phase produces:
   - A Relation with columns matching `phase.Provides` (or subset in `Keep`)
   - Never returns `nil` for successful execution

3. **Data Flow Invariant**:
   - If Phase N produces K tuples with symbols S
   - And Phase N+1 needs symbols S' ⊆ S
   - Then Phase N+1 receives K tuples with S' available

4. **Composition Invariant**:
   - Patterns, expressions, predicates, subqueries can appear in any combination
   - Each can appear zero or more times
   - Phase execution MUST handle all combinations correctly

**Violation of invariant #1 caused this bug** - expression-only phases received empty relations instead of previous phase's results.

## Testing This Bug

The fix is tested by:
- `TestConditionalAggregatePlanningDebug` - End-to-end test with expression-only phases
- All `TestConditionalAggregate*` tests now pass
- `TestPhaseReordering*` tests still pass (no regression)

## References

- Implementation: `datalog/executor/executor_sequential.go`
- Expression execution: `datalog/executor/expressions_and_predicates.go`
- Annotations: `datalog/annotations/`
- Related optimization: `datalog/planner/subquery_rewriter.go` (creates expression-only phases)
