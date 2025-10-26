# BUG: Legacy Executor Incorrectly Rejects Expression Symbols

**Status**: ACTIVE
**Severity**: MEDIUM - Validation failures, legacy executor needs fixing
**Affects**: Legacy executor (executePhaseSequential in executor_sequential.go)
**Date Discovered**: 2025-10-16

## Summary

The legacy executor incorrectly rejects valid queries where expression-generated symbols are used in subsequent phases or predicates. The new executor handles these queries correctly.

This breaks executor validation because both executors must behave identically for proper testing.

## Failing Tests

### TestComprehensiveExecutorValidation/multi-phase_with_expression

**Query:**
```clojure
[:find ?name ?total
 :where [?event :event/person ?person]
        [?person :person/name ?name]
        [?person :person/score ?score]
        [?event :event/value ?value]
        [(+ ?score ?value) ?total]]
```

**Error:**
```
Legacy error: phase 1 failed: projection failed: cannot project:
             column ?total not found in relation (has columns: [?person ?name ?score])
New error: <nil>
```

**Analysis:**
- Query generates `?total` via expression `[(+ ?score ?value) ?total]` in phase 1
- Legacy executor tries to project `?total` before evaluating expressions
- New executor correctly evaluates expressions first, making `?total` available
- **Root cause**: Legacy executor has incorrect evaluation order (projection before expression)

### TestComprehensiveExecutorValidation/all_features_combined

**Query:**
```clojure
[:find ?name ?total
 :where [?event :event/person ?person]
        [?person :person/name ?name]
        [?person :person/score ?score]
        [?event :event/value ?value]
        [(+ ?score ?value) ?total]
        [(> ?total 100)]]
```

**Error:**
```
Legacy error: phase 1 failed: predicate [(> ?total 100)] requires symbols
             not available in relation group: [?total]
New error: <nil>
```

**Analysis:**
- Query generates `?total` via expression, then filters with `[(> ?total 100)]`
- Legacy executor evaluates predicates before expressions
- Predicate sees `?total` as unavailable (not in pattern symbols)
- New executor correctly evaluates expressions first, then predicates
- **Root cause**: Legacy executor has incorrect evaluation order (predicate before expression)

## Expected Behavior

**Correct evaluation order within a phase:**
1. **Pattern matching** - Bind variables from storage/bindings
2. **Expressions** - Evaluate and bind output symbols
3. **Predicates** - Filter using pattern AND expression symbols
4. **Projection** - Keep only required symbols

The new executor implements this correctly. The legacy executor does not.

## Impact

**Why this matters for validation:**
- Validation tests require both executors to produce identical results
- These queries are **valid** and should work in both executors
- Currently blocking full test suite passing (2 subtests fail)
- Users might rely on legacy executor and hit these bugs

**Not a critical bug because:**
- New executor works correctly
- Legacy executor is being phased out
- Workaround: Use new executor (SetUseQueryExecutor(true))

## Root Cause

The legacy executor in `executor_sequential.go` has incorrect phase execution order:

**Legacy (incorrect):**
```go
// Phase execution in legacy
1. Match patterns
2. Apply predicates  ❌ Too early - expression symbols not available
3. Project Keep symbols  ❌ Too early - expression symbols not available
4. Evaluate expressions  ❌ Too late - results already projected
```

**New (correct):**
```go
// Phase execution in new executor
1. Match patterns
2. Evaluate expressions  ✅ Makes output symbols available
3. Apply predicates  ✅ Can use expression symbols
4. Project Keep symbols  ✅ Can include expression symbols
```

## The Fix

**Option 1: Fix legacy executor** (recommended for validation)
- Reorder phase execution in `executor_sequential.go`
- Match the new executor's evaluation order
- Ensures both executors handle identical queries

**Option 2: Update validation tests** (acceptable alternative)
- Document known differences between executors
- Skip these specific test cases
- Mark legacy executor as deprecated

**Option 3: Remove legacy executor** (long-term solution)
- Finish validating new executor
- Remove legacy code entirely
- Single code path = no validation issues

## Temporary Workaround

Users hitting these issues can use the new executor:
```go
executor.SetUseQueryExecutor(true)
```

## Related Issues

This is part of the broader legacy → new executor migration tracked in:
- TestComprehensiveExecutorValidation validation suite
- Executor equivalence testing framework

## Next Steps

1. **Investigate legacy executor phase execution order**
   - Location: `datalog/executor/executor_sequential.go`
   - Find where expressions, predicates, projection are applied

2. **Determine fix approach**
   - Can legacy executor be fixed without breaking other tests?
   - Or should we deprecate it and update validation tests?

3. **Choose path forward**
   - Fix legacy to match new (best for validation)
   - Update tests to allow differences (pragmatic)
   - Remove legacy entirely (cleanest long-term)
