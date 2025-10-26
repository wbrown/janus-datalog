# Bug Report: Parallel Decorrelation Breaking :find Clause Tuple Order

## Summary

Enabling `EnableParallelDecorrelation: true` causes result tuples to be returned in incorrect order, violating the `:find` clause variable ordering guarantee.

## Severity

**HIGH** - Breaks fundamental Datalog semantics. The `:find` clause variable order is part of the query contract and must be preserved.

## Environment

- **Janus Datalog commit**: 27c67d4 (feat(planner): Implement semantic rewriting optimization)
- **Affected optimization**: Parallel decorrelation (commit 2c07421)
- **Discovery context**: gopher-street market data application

## Reproduction

### Query
```datalog
[:find ?date ?open-price ?daily-high ?daily-low ?close-price ?total-volume
 :where
   [?s :symbol/ticker "TEST"]

   ; Get distinct dates
   [?morning-bar :price/symbol ?s]
   [?morning-bar :price/minute-of-day 570]
   [?morning-bar :price/time ?t]
   [(year ?t) ?year]
   [(month ?t) ?month]
   [(day ?t) ?day]
   [(str ?year "-" ?month "-" ?day) ?date]

   ; Subquery 1: Daily high/low
   [(q [:find (max ?h) (min ?l)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py]
               [(month ?time) ?pm]
               [(day ?time) ?pd]
               [(= ?py ?y)]
               [(= ?pm ?m)]
               [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 570)]
               [(<= ?mod 960)]
               [?b :price/high ?h]
               [?b :price/low ?l]]
       $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]

   ; Subquery 2: Open price
   [(q [:find (min ?o)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py]
               [(month ?time) ?pm]
               [(day ?time) ?pd]
               [(= ?py ?y)]
               [(= ?pm ?m)]
               [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 570)]
               [(<= ?mod 575)]
               [?b :price/open ?o]]
       $ ?s ?year ?month ?day) [[?open-price]]]

   ; Subquery 3: Close price
   [(q [:find (max ?c)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py]
               [(month ?time) ?pm]
               [(day ?time) ?pd]
               [(= ?py ?y)]
               [(= ?pm ?m)]
               [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 955)]
               [(<= ?mod 960)]
               [?b :price/close ?c]]
       $ ?s ?year ?month ?day) [[?close-price]]]

   ; Subquery 4: Total volume
   [(q [:find (sum ?v)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py]
               [(month ?time) ?pm]
               [(day ?time) ?pd]
               [(= ?py ?y)]
               [(= ?pm ?m)]
               [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 570)]
               [(<= ?mod 960)]
               [?b :price/volume ?v]]
       $ ?s ?year ?month ?day) [[?total-volume]]]

 :order-by [?date]]
```

### Test Data

Three 5-minute price bars for 2025-01-10:
- 09:30: open=100.00, high=101.50, low=99.50, close=101.00, volume=1000000
- 12:00: open=101.00, high=103.00, low=100.50, close=102.00, volume=950000
- 16:00: open=102.00, high=103.00, low=101.50, close=102.50, volume=1100000

### Expected Behavior

Result tuple should match `:find` clause order:
```
[0] = "2025-1-10"        (?date)
[1] = 100.00             (?open-price)
[2] = 103.00             (?daily-high)
[3] = 99.50              (?daily-low)
[4] = 102.50             (?close-price)
[5] = 3050000            (?total-volume)
```

### Actual Behavior

With `EnableParallelDecorrelation: true`, tuple order is scrambled:
```
[0] = "2025-1-10"        (?date) ✓
[1] = 3050000            (?total-volume) ✗ Expected ?open-price
[2] = 100.00             (?open-price) ✗ Expected ?daily-high
[3] = 102.50             (?close-price) ✗ Expected ?daily-low
[4] = 103.00             (?daily-high) ✗ Expected ?close-price
[5] = 99.50              (?daily-low) ✗ Expected ?total-volume
```

**Pattern observed**: Variables appear to be reordered based on subquery execution order or completion time, not `:find` clause order.

## Planner Options Used

```go
planner.PlannerOptions{
    EnableDynamicReordering:     false,
    EnablePredicatePushdown:     true,
    EnableSubqueryDecorrelation: true,
    EnableParallelDecorrelation: true,  // ← BUG TRIGGER
    EnableSemanticRewriting:     true,
    EnableCSE:                   false,
    EnableFineGrainedPhases:     true,
    MaxPhases:                   10,
}
```

## Behavior With Different Settings

| Setting | Result Tuple Order |
|---------|-------------------|
| `EnableParallelDecorrelation: false` | **Correct** (matches `:find` clause) |
| `EnableParallelDecorrelation: true` | **Incorrect** (scrambled) |

## Test Case

From `gopher-street/restore_test.go`:

```go
func TestRestore5MinData(t *testing.T) {
    // ... setup test data ...

    t.Run("SynthesizeDailyFromRestored5Min", func(t *testing.T) {
        dailyBars, err := ExtractDailyFromDatalogAccurate(ctx)
        if err != nil {
            t.Fatalf("ExtractDailyFromDatalogAccurate failed: %v", err)
        }

        daily := dailyBars[0]

        // These assertions fail with parallel decorrelation enabled
        if daily.Open != 100.00 {
            t.Errorf("Expected daily open=100.00, got %.2f", daily.Open)
        }
        if daily.Close != 102.50 {
            t.Errorf("Expected daily close=102.50, got %.2f", daily.Close)
        }
        // etc...
    })
}
```

**Test result:**
- ✅ PASSES with `EnableParallelDecorrelation: false`
- ❌ FAILS with `EnableParallelDecorrelation: true`

## Root Cause Hypothesis

When parallel decorrelation executes multiple subqueries concurrently, the results appear to be merged in the order they **complete** rather than the order they're **declared** in the `:find` clause.

Possible locations in code:
1. **Parallel execution result merging** - Results from goroutines may be collected in completion order
2. **Tuple projection logic** - Final projection may not preserve `:find` clause variable order
3. **Phase result binding** - Variable bindings from parallel phases may be applied in wrong order

## Impact

**Critical for production use:**
- Breaks all queries relying on `:find` clause variable ordering
- Makes parallel decorrelation unusable despite its excellent performance benefits
- Requires application code to parse variable names from schema (if even available)

**Performance vs Correctness trade-off:**
- Parallel decorrelation provides 2-4× speedup on multi-aggregation queries
- But correctness violations make it unusable

## Workaround

Disable parallel decorrelation:
```go
planner.PlannerOptions{
    EnableSubqueryDecorrelation: true,  // Keep this - still gives 10-100× speedup
    EnableParallelDecorrelation: false, // Disable to preserve tuple order
    // ... other options ...
}
```

This loses the 2-4× parallel speedup but maintains correctness.

## Recommendation

**Fix Priority: HIGH**

The `:find` clause variable order is a fundamental Datalog semantic guarantee. Applications depend on this ordering to map result tuples to domain objects. Breaking this violates the query language contract.

Suggested fix approach:
1. After parallel subqueries complete, reorder result bindings to match `:find` clause order
2. Add integration test that verifies `:find` clause ordering with parallel execution
3. Consider adding metadata to track original variable order through optimization passes

## Related Performance Data

Despite the bug, parallel decorrelation shows excellent speedup potential when working correctly:

**gopher-street OHLC query performance:**
- Daily OHLC (22 days, 88 subqueries): 4,000ms → **245ms** (16.3× speedup) ✓
- Hourly OHLC (260 hours, 1,040 subqueries): 78,000ms → **41,594ms** (1.87× speedup) ✓

*Note: These tests don't depend on tuple order, so they pass. The bug only manifests when applications consume results positionally.*

## Files Referenced

- **Bug reproduction**: `gopher-street/restore_test.go::TestRestore5MinData`
- **Query implementation**: `gopher-street/extract_datalog.go::ExtractDailyFromDatalogAccurate()`
- **Optimization code**: `janus-datalog/datalog/planner/` (parallel decorrelation implementation)

---

**Discovered**: October 7, 2025
**Reported by**: gopher-street integration test suite
**Impact**: Blocks adoption of parallel decorrelation optimization
