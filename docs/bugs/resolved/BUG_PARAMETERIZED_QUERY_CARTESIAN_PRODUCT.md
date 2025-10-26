# Bug: Parameterized Queries Cause "Cartesian Product" Errors on Queries That Work with fmt.Sprintf

**Status**: ✅ **FIXED**
**Date Reported**: October 13, 2025
**Date Fixed**: October 13, 2025
**Severity**: High - Blocked adoption of parameterized query API
**Affected**: ExecuteQueryWithInputs API

## Summary

Converting working queries from `fmt.Sprintf` string formatting to the parameterized `ExecuteQueryWithInputs` API causes "Cartesian product not supported" errors, even though the query structure is identical. The planner appears to incorrectly identify disjoint relation groups when parameters are used.

## Error Message

```
query execution failed: phase 1 failed: phase resulted in 2 disjoint relation groups - Cartesian products not supported
```

## Minimal Reproduction

### Query That WORKS with fmt.Sprintf

```go
query := fmt.Sprintf(`[:find (max ?time)
 :where
        [?s :symbol/ticker "%s"]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day 960]]`, ctx.Symbol)

results, err := ctx.Storage.ExecuteQuery(query)
// ✅ SUCCESS
```

### Same Query FAILS with ExecuteQueryWithInputs

```go
query := `[:find (max ?time)
 :in $ ?symbol
 :where
        [?s :symbol/ticker ?symbol]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day 960]]`

results, err := ctx.Storage.ExecuteQueryWithInputs(query, ctx.Symbol)
// ❌ ERROR: phase resulted in 2 disjoint relation groups - Cartesian products not supported
```

## Pattern Analysis

### Queries That WORK with Parameters

✅ **Simple nested subquery** (ExtractLatestPriceFromDatalog):
```datalog
[:find ?max-time ?open ?high ?low ?close ?volume
 :in $ ?symbol
 :where
        [?s :symbol/ticker ?symbol]
        [?latest-bar :price/symbol ?s]
        [?latest-bar :price/time ?max-time]
        [(q [:find (max ?t)
             :in $ ?sym
             :where [?b :price/symbol ?sym]
                    [?b :price/time ?t]]
            $ ?s) [[?max-time]]]]
```

✅ **Time.Time parameter in comparison** (ExtractPreviousCloseFromDatalog):
```datalog
[:find ?close
 :in $ ?symbol ?cutoff
 :where
        [?s :symbol/ticker ?symbol]
        [(q [:find (max ?t)
             :in $ ?sym ?cutoff
             :where [?bar :price/symbol ?sym]
                    [?bar :price/time ?t]
                    [(< ?t ?cutoff)]]
            ?s ?cutoff) [[?max-time]]]]
```

### Queries That FAIL with Parameters

❌ **Simple aggregation with filter**:
```datalog
[:find (max ?time)
 :in $ ?symbol
 :where
        [?s :symbol/ticker ?symbol]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day 960]]
```

❌ **Aggregation by date components**:
```datalog
[:find ?year ?month ?day (min ?open) (max ?close)
 :in $ ?symbol
 :where
        [?s :symbol/ticker ?symbol]
        [?e :price/symbol ?s]
        [?e :price/time ?time]
        [(year ?time) ?year]
        [(month ?time) ?month]
        [(day ?time) ?day]
        [?e :price/open ?open]
        [?e :price/close ?close]]
```

❌ **Multiple parallel subqueries**:
```datalog
[:find ?date ?open ?high ?low ?close ?volume
 :in $ ?symbol
 :where
        [?s :symbol/ticker ?symbol]
        [?morning-bar :price/symbol ?s]
        [?morning-bar :price/minute-of-day 570]
        [(year ?t) ?year]

        ; Multiple subqueries using ?year ?month ?day
        [(q [:find (max ?h) ...] $ ?s ?year ?month ?day) [[?high ?low]]]
        [(q [:find (min ?o) ...] $ ?s ?year ?month ?day) [[?open]]]
        [(q [:find (max ?c) ...] $ ?s ?year ?month ?day) [[?close]]]]
```

## Hypothesis

The planner may be treating parameterized input variables differently during the relation graph analysis phase, causing it to incorrectly identify separate relation groups where none exist. The string-formatted queries work because the symbol is substituted before parsing/planning.

## Impact

**HIGH** - This bug blocks migration to the safer parameterized query API. String formatting with `fmt.Sprintf` works for all queries, but is:
- Less safe (no type checking)
- More error-prone (manual escaping)
- Doesn't benefit from query plan caching with different parameter values

## Test Environment

- **Gopher Street**: github.com/wbrown/gopher-street @ commit f98fc73
- **Janus Datalog**: github.com/wbrown/janus-datalog @ main branch
- **Go Version**: 1.21+
- **OS**: macOS (Darwin 24.6.0)

## Reproduction Steps

1. Clone https://github.com/wbrown/gopher-street
2. Checkout commit f98fc73
3. Run: `go test ./storage -run TestExtractDaily -v`
4. Observe Cartesian product errors

## Failing Tests

```bash
$ go test ./storage -run TestExtractDaily -v
=== FAIL: TestExtractDailyBug (0.69s)
    extract_datalog_bug_test.go:157: ExtractDailyFromDatalogAccurate failed:
    failed to query daily OHLC data: query execution failed: phase 1 failed:
    phase resulted in 2 disjoint relation groups - Cartesian products not supported

=== FAIL: TestExtractDailyBug_ShortPeriod (0.16s)
    extract_datalog_bug_test.go:481: ExtractDaily failed:
    failed to query daily OHLC data: query execution failed: phase 1 failed:
    phase resulted in 2 disjoint relation groups - Cartesian products not supported

=== FAIL: TestExtractLatestDailyBarFromDatalog/ExtractLatestDaily (0.00s)
    extract_datalog_test.go:228: Failed to extract latest daily bar:
    failed to query max date: query execution failed: phase 1 failed:
    phase resulted in 2 disjoint relation groups - Cartesian products not supported
```

## Workaround

Continue using `fmt.Sprintf` for queries that fail with parameterization:

```go
// WORKAROUND: Use fmt.Sprintf until bug is fixed
query := fmt.Sprintf(`[:find (max ?time)
 :where
        [?s :symbol/ticker "%s"]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day 960]]`, ctx.Symbol)

results, err := ctx.Storage.ExecuteQuery(query)
```

## Related Issues

- FEATURE_IMPLEMENTED_STORAGE_INPUT_PARAMS.md - Documentation of the parameterized query feature
- This bug was discovered during systematic migration of all queries in Gopher Street

## Resolution

**Root Cause**: The planner's pattern selectivity scoring treated input parameters as unbound variables, giving them poor selectivity scores. This caused patterns using input parameters to be scheduled in later phases, creating disjoint relation groups.

**The Fix**: Modified `planner_patterns.go:scorePattern()` to treat bound variables (especially input parameters) as selective as constants:
- Changed bound value score from `+5` to `-500` (same as constants)
- Modified `estimatePatternSelectivity()` to accept and pass `resolved` parameter containing available symbols (including input parameters)
- Updated call site in `planner_phases.go` to pass `availableSymbols` to selectivity scoring

**Files Changed**:
1. `datalog/planner/planner_patterns.go:276-332` - Updated selectivity scoring
2. `datalog/planner/planner_phases.go:192-196` - Pass availableSymbols to scoring
3. `tests/parameterized_cartesian_product_test.go` - Added regression test

**Result**: Patterns with input parameters are now correctly prioritized in phase ordering, preventing disjoint relation groups.

**Verification**:
- ✅ New test case passes: `tests/parameterized_cartesian_product_test.go`
- ✅ All Gopher Street tests pass: `TestExtractDaily`, `TestExtractLatestDailyBarFromDatalog`
- ✅ All Janus Datalog tests pass: `go test ./...`
