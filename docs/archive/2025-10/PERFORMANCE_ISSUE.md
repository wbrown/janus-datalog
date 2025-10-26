# Performance Issue: Subquery Date Constraints

**Status**: ✅ Fixed by October 2025 optimizations
- Time range extraction optimization (4× speedup)
- Parallel subquery execution (6.26× speedup)
- See `/PERFORMANCE_STATUS.md` for current performance profile

---

## Problem (Historical - June 2025)
After migrating from Bindings to Relations, subqueries are fetching all data and then filtering, rather than using input constraints to limit initial pattern matches.

### Example
In the daily high query:
```clojure
[(q [:find (max ?h)
     :in $ ?sym ?y ?m ?d
     :where [?b :price/symbol ?sym]
            [?b :price/time ?time]
            [(year ?time) ?py]
            [(month ?time) ?pm] 
            [(day ?time) ?pd]
            [(= ?py ?y)]
            [(= ?pm ?m)]
            [(= ?pd ?d)]
            ...
```

The pattern `[?b :price/symbol ?sym]` fetches ALL 7088 bars for the symbol, even though we have `?y ?m ?d` constraints available.

## Root Causes

1. **Projection strips constraints**: The sequential executor projects binding relations to only include symbols used in the pattern. For `[?b :price/symbol ?sym]`, this keeps only `?sym` and discards `?y ?m ?d`.

2. **Matcher doesn't understand cross-attribute constraints**: Even if we kept the date values, BadgerMatcher doesn't know that `?y ?m ?d` relate to `:price/time` attribute.

3. **Query structure**: The query doesn't have a pattern that directly uses the date constraints to limit the initial match.

## Workarounds

### 1. Rewrite queries to use date constraints earlier
Instead of fetching all bars and filtering, add patterns that use dates directly:
```clojure
:where [?b :price/symbol ?sym]
       [?b :price/time ?time]
       [(>= ?time ?start-time)]  ; Computed from ?y ?m ?d
       [(<= ?time ?end-time)]    ; Before fetching other attributes
       [?b :price/high ?h]
```

### 2. Add time-range patterns
Create derived attributes like `:price/date` that can be queried directly:
```clojure
:where [?b :price/symbol ?sym]
       [?b :price/date ?date]
       [(= ?date ?target-date)]  ; Computed from ?y ?m ?d
```

## Proper Fix

The proper fix requires:

1. **Query optimization**: Reorder patterns to use available constraints early
2. **Constraint propagation**: Understand relationships between attributes and constraints
3. **Index support**: Add composite indices for common query patterns (symbol+date)
4. **Single-row optimization**: Special handling for single-row binding relations

## Impact

Current: Fetching 7088 rows per subquery execution (37 times = 261,256 rows)
Optimal: Fetching ~200 rows per subquery execution (37 times = 7,400 rows)
Performance impact: ~35x more data fetched than necessary