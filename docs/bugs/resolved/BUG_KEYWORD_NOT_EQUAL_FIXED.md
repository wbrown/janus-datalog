# Bug Report: `not=` Predicate Not Working With Keywords [FIXED]

**Status**: FIXED (October 2025)
**Reported**: October 5, 2025
**Fixed**: October 8, 2025

## Summary

The `not=` predicate was filtering out ALL results when used to compare a bound keyword variable with a literal keyword, instead of just filtering out matching values.

## Expected Behavior

Given an entity with two attributes:
- `:symbol/ticker` = "TEST"
- `:symbol/name` = "Test Company"

The query:
```clojure
[:find ?attr ?val
 :where [?s :symbol/ticker "TEST"]
        [?s ?attr ?val]
        [(not= ?attr :symbol/ticker)]]
```

Should return:
```
[[:symbol/name "Test Company"]]
```

## Actual Behavior (Before Fix)

The query returned **no results** (empty result set).

## Root Cause

The `not=` predicate was being incorrectly parsed or evaluated when comparing Keyword types, causing it to reject all tuples instead of just those where the keywords matched.

## Fix

The bug was fixed by ensuring proper Keyword comparison in the NotEqualPredicate evaluation logic.

## Regression Tests

Comprehensive regression tests were added to prevent this bug from reoccurring:

1. **datalog/query/predicate_extended_test.go:123-216** - `TestNotEqualPredicate`
   - Test case: "Keyword not equal to different keyword" (should be true)
   - Test case: "Keyword not equal to same keyword" (should be false)

2. **datalog/planner/function_validation_test.go:95-123** - `TestNotEqualIsNotFunction`
   - Ensures `not=` is parsed as NotEqualPredicate, not FunctionPredicate

3. **datalog/parser/predicate_syntax_test.go:156-185** - `TestNotEqualKeywordIntegration`
   - End-to-end test with the exact query pattern from this bug report
   - Tests both `!=` and `not=` syntax variants

## Impact

This affected any query that needed to filter attributes based on their keyword name, which is a common pattern when:
- Extracting all attributes except a known one
- Building generic metadata extraction queries
- Implementing attribute-based access control

## Related Files

- Original bug report: `repro/KEYWORD_NOT_EQUAL_BUG.md` (deleted after archiving)
- Reproduction code: `repro/test_keyword_not_equal.go` (deleted after proper tests added)
