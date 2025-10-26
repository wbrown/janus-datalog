# Bug: String Predicates Cannot Use Input Parameters

**Status**: ✅ **FIXED**
**Date Reported**: October 13, 2025
**Date Fixed**: October 13, 2025
**Severity**: Medium - Blocked parameterization of queries with string operations
**Affected**: String predicates like `str/starts-with?` and other function predicates

## Summary

String operation predicates such as `str/starts-with?` cannot accept input parameters. When attempting to use a parameter variable as the second argument to `str/starts-with?`, the query planner panics with "CRITICAL: predicates could not be assigned to any phase."

## Error Message

```
panic: CRITICAL: 1 predicates could not be assigned to any phase: [[(str/starts-with? ?timeStr ?month)]]. Query cannot be executed correctly.
```

## Minimal Reproduction

### Query That FAILS with Parameters

```datalog
[:find (count ?p)
 :in $ ?symbol ?month
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]
        [(str ?time) ?timeStr]
        [(str/starts-with? ?timeStr ?month)]]  ; ❌ FAILS - ?month is a parameter
```

Calling with:
```go
results, err := ctx.Storage.ExecuteQueryWithInputs(query, "CRWV", "2025-10")
// panic: CRITICAL: 1 predicates could not be assigned to any phase
```

### Workaround: Embed as Literal

```datalog
[:find (count ?p)
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]
        [(str ?time) ?timeStr]
        [(str/starts-with? ?timeStr "2025-10")]]  ; ✅ WORKS - literal string
```

Calling with:
```go
// Must use fmt.Sprintf to embed the month as a literal
query := fmt.Sprintf(`[:find (count ?p)
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]
        [(str ?time) ?timeStr]
        [(str/starts-with? ?timeStr "%s")]]`, month)

results, err := ctx.Storage.ExecuteQueryWithInputs(query, "CRWV")
// ✅ SUCCESS
```

## Impact

**MEDIUM** - This limitation forces queries with string operations to use fmt.Sprintf for parts of the query, preventing full parameterization. This means:
- Cannot benefit from query plan caching when only the string pattern changes
- Must construct partial query strings with fmt.Sprintf (mixing parameterized and non-parameterized approaches)
- Less safe than pure parameterized queries

## Expected Behavior

Input parameters should be usable as arguments to string predicates:

```datalog
[:find (count ?p)
 :in $ ?symbol ?month
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]
        [(str ?time) ?timeStr]
        [(str/starts-with? ?timeStr ?month)]]  ; Should work!
```

## Pattern Analysis

### Works ✅
- Comparison predicates with parameters: `[(< ?t ?cutoff)]` where `?cutoff` is input parameter
- Entity attribute lookups: `[?e :attr ?param]` where `?param` is input parameter
- Literal strings in predicates: `[(str/starts-with? ?str "literal")]`

### Fails ❌
- String predicates with parameter arguments: `[(str/starts-with? ?str ?param)]`

## Planner Analysis

The error occurs in `planner_expressions.go:281` in `assignExpressionsToPhases()`. The planner cannot determine how to schedule predicates that use input parameters as arguments to function expressions.

Hypothesis: The planner may be treating input parameters in expression contexts differently than in pattern contexts, preventing it from understanding the dependency chain needed for phase assignment.

## Test Environment

- **Gopher Street**: github.com/wbrown/gopher-street @ main branch
- **Janus Datalog**: github.com/wbrown/janus-datalog @ main branch
- **Go Version**: 1.24.2
- **OS**: macOS (Darwin 24.6.0)

## Reproduction Steps

1. Clone https://github.com/wbrown/gopher-street
2. Run: `go test ./claude -run TestToolEnsureDataValidation -v`
3. Observe panic from planner

## Related Issues

- BUG_PARAMETERIZED_QUERY_CARTESIAN_PRODUCT.md (resolved) - Similar issue with parameter scheduling in planner
- FEATURE_IMPLEMENTED_STORAGE_INPUT_PARAMS.md - Documentation of parameterized query feature

## Resolution

**Root Cause**: The predicate assignment logic in `planner_expressions.go` only added input parameters to the `available` map for phase 0. For phases > 0, it only looked at previous phases' `Provides`, missing input parameters that should be available throughout query execution.

**Key Insight**: Input parameters are "environment" symbols that should be available in ALL phases for filtering/correlation (see INPUT_PARAMETER_KEEP_BUG.md). While they're not columns in output relations (Provides), they must be accessible for predicate evaluation (Available).

**The Fix**: Modified `assignExpressionsToPhases()` to use `phases[i].Available` for each phase, which correctly includes:
- Input parameters (from `:in` clause)
- Symbols from previous phases

**Before (BROKEN)**:
```go
// Only added Available for phase 0
if i == 0 {
    for _, sym := range phases[0].Available {
        available[sym] = true
    }
}
// For phase > 0, only looked at previous Provides
for j := 0; j < i; j++ {
    for _, sym := range phases[j].Provides {
        available[sym] = true
    }
}
```

**After (FIXED)**:
```go
// Use phases[i].Available which includes input parameters + previous provides
for _, sym := range phases[i].Available {
    available[sym] = true
}
```

**Files Changed**:
1. `datalog/planner/planner_expressions.go:138-142` - Use phases[i].Available for predicate assignment
2. `tests/string_predicate_with_parameters_test.go` - Added regression test

**Verification**:
- ✅ New test case passes: `tests/string_predicate_with_parameters_test.go`
- ✅ All Janus Datalog tests pass: `go test ./...`
- ✅ String predicates with parameters now work correctly

## Workaround (No Longer Needed)

~~For queries requiring string operations with variable patterns:~~
~~1. Use `:in $ ?symbol` for the symbol parameter~~
~~2. Use `fmt.Sprintf` to embed the string pattern as a literal in the query~~
~~3. Call with `ExecuteQueryWithInputs(query, symbol)`~~

**UPDATE**: This workaround is no longer necessary after the fix.
