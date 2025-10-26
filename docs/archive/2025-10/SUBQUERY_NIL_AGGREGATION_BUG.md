# Nil Values in Subquery Aggregation Results

**Date**: October 10, 2025
**Severity**: High - Data Integrity Issue
**Component**: Subquery execution with aggregate bindings
**Discovered**: During gopher-street test suite execution

---

## Bug Summary

Subqueries with aggregate functions return tuples containing **nil values** when the subquery matches no data, instead of either:
1. Returning no tuple (failing the pattern match), or
2. Returning a sensible default value

This violates the datalog contract that tuple values should never be nil.

## Error Message

```
panic: interface conversion: interface {} is nil, not float64

goroutine 64548 [running]:
github.com/wbrown/gopher-street.ExtractDailyFromDatalogAccurate(0xc000ee6500)
	/Users/wbrown/go/src/github.com/wbrown/gopher-street/extract_datalog.go:930 +0x730
```

Line 930: `High: row[2].(float64)` - panics because `row[2]` is nil

## Reproduction

### Query Pattern

```clojure
[:find ?date ?daily-high ?daily-low ?open-price ?close-price ?total-volume
 :in $ ?s
 :where
   ; Main pattern establishes date
   [?morning-bar :price/symbol ?s]
   [?morning-bar :price/minute-of-day 570]
   [?morning-bar :price/time ?t]
   [(year ?t) ?year]
   [(month ?t) ?month]
   [(day ?t) ?day]
   [(str ?year "-" ?month "-" ?day) ?date]

   ; Subquery for daily high/low
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
               [?b :price/high ?h]
               [?b :price/low ?l]]
       $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]

   ; Additional subqueries for open, close, volume...
]
```

### Trigger Condition

The bug occurs when:
1. Main query pattern matches (establishes `?year`, `?month`, `?day`)
2. Subquery executes with those parameters as input
3. Subquery WHERE clause matches **zero tuples** (no price data for that day)
4. Aggregate functions return empty result
5. Tuple binding `[[?daily-high ?daily-low]]` gets bound to `[nil, nil]`
6. Outer query continues with nil values in the tuple

### Expected Behavior

**Option A (Fail-fast)**: If subquery returns no results, the entire pattern should fail to match. The outer query should not produce a tuple for that date.

**Option B (Default values)**: Aggregates on empty sets should return sensible defaults:
- `(max x)` on empty → nil (or fail pattern)
- `(min x)` on empty → nil (or fail pattern)
- `(sum x)` on empty → 0
- `(count x)` on empty → 0
- `(avg x)` on empty → nil (or fail pattern)

**Actual Behavior**: Returns tuple with nil values, violating datalog semantics.

## Impact

### Severity: High
- **Data integrity**: Queries can return invalid data (nil values)
- **Type safety**: Violates assumption that tuple values are non-nil
- **Silent corruption**: No error reported, just bad data
- **Production risk**: Applications crash when trying to use results

### Affected Queries
Any query with:
- Subqueries that use aggregation functions
- TupleBinding or RelationBinding forms `[[?var1 ?var2]]`
- Possibility of empty subquery results

### Current Workarounds
None - applications must defensively check for nil values after every query, which defeats the purpose of strong typing.

## Root Cause Analysis

Located in subquery execution path (likely `executor/subquery.go`):

1. **Subquery executes** with input parameters
2. **Empty result set** returned from aggregate query
3. **applyBindingForm()** creates tuple with nil values instead of:
   - Not creating a tuple at all (Option A)
   - Using default values for aggregates (Option B)
4. **Outer query** receives and uses the invalid tuple

The issue is in the binding application logic, which doesn't handle empty aggregate results correctly.

## Proposed Fix

### Option A: Fail Pattern on Empty Subquery (Recommended)

**Behavior**: If a subquery returns an empty result, treat it as a failed pattern match. The outer query tuple should not be produced.

**Implementation**:
```go
// In applyBindingForm() for TupleBinding
func applyBindingForm(result Relation, binding query.TupleBinding, ...) (Relation, error) {
    if result.Size() == 0 {
        // Empty subquery result = no tuple produced
        return NewMaterializedRelation(outputColumns, []Tuple{}), nil
    }

    if result.Size() != 1 {
        return nil, fmt.Errorf("tuple binding expects exactly 1 result, got %d", result.Size())
    }

    // Check for nil values in result
    resultTuple := result.Get(0)
    for i, val := range resultTuple {
        if val == nil {
            return nil, fmt.Errorf("subquery result contains nil value at position %d", i)
        }
    }

    // Continue with normal binding...
}
```

**Pros**:
- Maintains datalog semantics (no nil values ever)
- Matches Datomic behavior (empty subquery = no match)
- Type safety preserved
- Clear failure mode

**Cons**:
- May filter out more results than expected
- Requires queries to handle missing dates explicitly

### Option B: Default Values for Aggregates

**Behavior**: Empty aggregate results return type-appropriate defaults:
- `sum` → 0
- `count` → 0
- `min`/`max`/`avg` → fail pattern (or user-specified default)

**Pros**:
- More intuitive for SQL users
- sum/count have natural defaults

**Cons**:
- min/max/avg don't have sensible defaults
- Mixes two approaches (defaults vs. failure)
- More complex implementation

## Recommended Solution

**Use Option A** because:
1. Consistent with Datomic semantics
2. Preserves type safety (no nil values)
3. Clear failure semantics
4. Simpler implementation
5. Forces explicit handling of missing data

## Testing Strategy

### Unit Tests
```go
func TestEmptySubqueryAggregation(t *testing.T) {
    // Query with subquery that returns no results
    query := `[:find ?name ?max-price
               :where
                 [?e :person/name ?name]
                 [(q [:find (max ?p)
                      :in $ ?person
                      :where [?o :order/person ?person]
                             [?o :order/price ?p]]
                    $ ?e) [[?max-price]]]]`

    // Person exists but has no orders
    results := exec.Execute(query)

    // Should return empty (not a tuple with [name, nil])
    assert.Equal(0, len(results))
}
```

### Integration Tests
- Test all aggregate functions on empty sets
- Test RelationBinding with empty results
- Test nested subqueries with empty intermediate results

### Regression Tests
Use gopher-street query that triggered this bug as a regression test.

## Verification

After fix, verify:
1. `go test ./datalog/executor -run TestSubquery`
2. `cd ../gopher-street && go test -v` should pass TestToolCalculateISOStrategy
3. No nil values in any query results

## Related Issues

- Subquery implementation: `executor/subquery.go`
- Binding application: `applyBindingForm()` function
- Aggregation semantics: Should align with Datomic behavior

---

**Priority**: High - Blocks gopher-street production use
**Assignee**: TBD
**Estimated Fix Time**: 2-4 hours (simple logic fix + comprehensive tests)
