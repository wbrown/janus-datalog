# Pure Aggregations Returning Nil Instead of Values

**Date**: October 10, 2025
**Severity**: CRITICAL - Complete aggregation failure
**Component**: `datalog/executor/subquery.go` - Pure aggregation handling
**Introduced By**: October 10, 2025 fixes for nil aggregation values

---

## Bug Summary

All pure aggregation queries return `nil` instead of computed values, even when data exists. This is a regression introduced by the fix for empty subquery aggregations.

## Evidence

### Query 1: Non-aggregated query (WORKS)
```clojure
[:find ?h
 :where [?s :symbol/ticker "CRWV"]
        [?p :price/symbol ?s]
        [?p :price/high ?h]]
```

**Result**: ✅ Returns values
```
|   ?h   |
|--------|
| 99.50  |
| 98.58  |
| ...    |
```

8256 rows exist in database.

### Query 2: Pure aggregation (BROKEN)
```clojure
[:find (max ?h)
 :where [?s :symbol/ticker "CRWV"]
        [?p :price/symbol ?s]
        [?p :price/high ?h]]
```

**Result**: ❌ Returns nil
```
| (max ?h) |
|----------|
| nil      |
```

Expected: `119.59` (or similar maximum value)

### Query 3: Multiple aggregations (BROKEN)
```clojure
[:find (max ?h) (min ?l)
 :where [?s :symbol/ticker "CRWV"]
        [?p :price/symbol ?s]
        [?p :price/high ?h]
        [?p :price/low ?l]]
```

**Result**: ❌ Both nil
```
| (max ?h) | (min ?l) |
|----------|----------|
| nil      | nil      |
```

### Query 4: Time aggregations (BROKEN)
```clojure
[:find (min ?t) (max ?t)
 :where [?s :symbol/ticker "CRWV"]
        [?p :price/symbol ?s]
        [?p :price/time ?t]]
```

**Result**: ❌ Both nil
```
| (min ?t) | (max ?t) |
|----------|----------|
| nil      | nil      |
```

Expected: Date range like `2025-03-28` to `2025-10-10`

## Reproduction

```bash
cd /Users/wbrown/go/src/github.com/wbrown/gopher-street

# Verify data exists
./datalog-cli -db datalog-db -query '[:find (count ?p) :where [?s :symbol/ticker "CRWV"] [?p :price/symbol ?s]]'
# Returns: 8256 rows

# Try simple aggregation - FAILS
./datalog-cli -db datalog-db -query '[:find (max ?h) :where [?s :symbol/ticker "CRWV"] [?p :price/symbol ?s] [?p :price/high ?h]]'
# Returns: nil (WRONG)

# Try non-aggregated query - WORKS
./datalog-cli -db datalog-db -query '[:find ?h :where [?s :symbol/ticker "CRWV"] [?p :price/symbol ?s] [?p :price/high ?h]]' | head -10
# Returns: 99.50, 98.58, ... (CORRECT)
```

## Environment

- **Janus Commit**: `0ffa954` (atomic operations for PlanCache)
- **Previous Commit**: `ce789af` (thread-safe tuple builder cache)
- **Go Version**: 1.24.2
- **Platform**: Darwin 24.6.0 (macOS)
- **Database**: gopher-street datalog-db with 8256 CRWV price bars

## Root Cause Analysis

### What Changed

The October 10 fix for nil aggregation values (documented in `FIXES_2025_10_10.md`) modified `datalog/executor/subquery.go` to handle empty aggregation results:

```go
// From FIXES_2025_10_10.md, Bug 3 fix:
case query.TupleBinding:
    // EMPTY RESULT = PATTERN FAILS TO MATCH
    // Return empty relation instead of error (datalog semantics)
    if result.Size() == 0 {
        columns := make([]query.Symbol, len(inputSymbols)+len(b.Variables))
        copy(columns, inputSymbols)
        copy(columns[len(inputSymbols):], b.Variables)
        return NewMaterializedRelation(columns, []Tuple{}), nil
    }

    // ... proceed with binding

    // Check for nil values in result (INVARIANT: should never happen)
    for i, val := range resultTuple {
        if val == nil {
            return nil, fmt.Errorf("subquery result contains nil value at position %d - this violates datalog semantics", i)
        }
    }
```

### The Problem

**Hypothesis 1**: The aggregation executor is returning `Size() == 0` for non-empty aggregations, causing the empty relation path to be taken.

**Hypothesis 2**: The aggregation executor is correctly returning `Size() == 1` with a result tuple, but the values in the tuple are nil (triggering the invariant check).

**Hypothesis 3**: Top-level pure aggregations aren't going through subquery execution at all, but are being handled by a different code path that has a similar bug.

### Testing Hypothesis 3

Let me check if this is a top-level aggregation issue vs subquery issue:

```clojure
; Top-level pure aggregation (BROKEN)
[:find (max ?h)
 :where [?s :symbol/ticker "CRWV"]
        [?p :price/symbol ?s]
        [?p :price/high ?h]]

; Grouped aggregation (NEED TO TEST)
[:find ?s (max ?h)
 :where [?p :price/symbol ?s]
        [?p :price/high ?h]]
```

If grouped aggregations work but pure aggregations don't, the issue is in how pure aggregations are executed at the top level.

## Impact Assessment

### Completely Broken
- ❌ All pure aggregation queries (max, min, sum, avg, count with single result)
- ❌ OHLC calculations (rely on max, min, sum)
- ❌ Statistical analysis (averages, ranges, totals)
- ❌ Date range queries (min/max timestamps)
- ❌ Any analytics using top-level aggregations

### Possibly Working
- ✅ Non-aggregated queries
- ✅ Grouped aggregations (need verification)
- ✅ Count queries (returns int64, not float64 - may have different code path)

### Production Impact
**BLOCKING** - This makes the October 10 fixes unusable. Cannot deploy.

## Regression Details

### Before October 10 Fixes
- Pure aggregations with data: ✅ Returned computed values
- Pure aggregations without data: ❌ Returned nil (bug we tried to fix)

### After October 10 Fixes
- Pure aggregations with data: ❌ Return nil (NEW REGRESSION)
- Pure aggregations without data: ❌ Still return nil (original bug not fixed properly)

**Status**: Made it worse - now ALL aggregations broken, not just empty ones.

## Diagnostic Queries

### Query to understand relation size:

We need to add debug logging to understand what's happening:

```go
// In datalog/executor/subquery.go or aggregation executor
case query.TupleBinding:
    log.Printf("DEBUG: Aggregation result size: %d", result.Size())
    if result.Size() > 0 {
        log.Printf("DEBUG: First tuple: %+v", result.Tuples()[0])
        for i, val := range result.Tuples()[0] {
            log.Printf("DEBUG: Position %d: %T = %v (isNil: %v)", i, val, val, val == nil)
        }
    }
```

### Test both code paths:

```bash
# Test top-level pure aggregation
./datalog-cli -db datalog-db -query '[:find (max ?h) :where [?s :symbol/ticker "CRWV"] [?p :price/symbol ?s] [?p :price/high ?h]]'

# Test subquery pure aggregation
./datalog-cli -db datalog-db -query '[:find ?s :where [?s :symbol/ticker "CRWV"] [(q [:find (max ?h) :in $ ?sym :where [?p :price/symbol ?sym] [?p :price/high ?h]] $ ?s) [[?max-high]]]]'

# Test grouped aggregation
./datalog-cli -db datalog-db -query '[:find ?s (max ?h) :where [?p :price/symbol ?s] [?p :price/high ?h]]'
```

## Expected Behavior

### Pure Aggregation With Data
```clojure
[:find (max ?h) :where [?p :price/high ?h]]
```

**Expected**: `| (max ?h) |` → `| 119.59 |` (single row with computed value)

### Pure Aggregation Without Data
```clojure
[:find (max ?h) :where [?p :price/high ?h] [?p :price/symbol ?no-such-symbol]]
```

**Expected**: 0 rows (pattern fails to match, per Datalog semantics)
**NOT**: 1 row with nil value

### Grouped Aggregation With Data
```clojure
[:find ?s (max ?h) :where [?p :price/symbol ?s] [?p :price/high ?h]]
```

**Expected**: Multiple rows, one per symbol, with computed max values

### Grouped Aggregation With Some Empty Groups
```clojure
[:find ?person (max ?price)
 :where [?person :person/name ?name]
        [(q [:find (max ?p) :in $ ?person :where [?o :order/person ?person] [?o :price ?p]] $ ?person) [[?max-price]]]]
```

**Expected**: Only rows for people with orders (people without orders excluded)

## Proposed Fix Strategy

### Step 1: Identify where aggregations execute

Need to determine if pure aggregations are:
- A) Executed in subquery executor (goes through `applyBindingForm`)
- B) Executed in main executor with special aggregation handling
- C) Executed in planner with different logic

### Step 2: Verify aggregation result construction

Check where aggregation results are created:
- Is the relation size correct? (Should be 1 for pure aggregations with results)
- Are the tuple values correct? (Should NOT be nil if data exists)
- Is the empty-result check correct? (Should only trigger for truly empty results)

### Step 3: Fix the logic

The fix for empty subqueries should NOT affect aggregations that have data:

```go
// CORRECT LOGIC:
if result.Size() == 0 {
    // No results from subquery - pattern fails to match
    return NewMaterializedRelation(columns, []Tuple{}), nil
}

if result.Size() == 1 {
    resultTuple := result.Tuples()[0]

    // Check for nil values (SHOULD NEVER HAPPEN with valid data)
    for i, val := range resultTuple {
        if val == nil {
            // This means aggregation executor has a bug
            return nil, fmt.Errorf("BUG: aggregation returned nil for position %d - check aggregation executor", i)
        }
    }

    // Normal tuple binding
    // ...
}
```

### Step 4: Fix the aggregation executor

If aggregation executor is creating nil values for valid aggregations:

```go
// In aggregation executor
func executeAggregation(fn AggFunc, values []Value) Value {
    if len(values) == 0 {
        // EMPTY INPUT - should return empty relation, NOT nil value
        // Don't create a result tuple at all
        return nil  // Signal to caller: no result
    }

    // Normal aggregation
    return fn(values)  // Should never return nil
}
```

## Testing Strategy

### Unit Test for Pure Aggregations

```go
func TestPureAggregationWithData(t *testing.T) {
    db := setupTestDB()

    // Insert test data
    db.Insert(`:price/high`, 100.0)
    db.Insert(`:price/high`, 200.0)
    db.Insert(`:price/high`, 150.0)

    // Query
    query := `[:find (max ?h) :where [?p :price/high ?h]]`
    results := db.ExecuteQuery(query)

    // Assertions
    assert.Equal(t, 1, len(results), "Pure aggregation should return 1 row")
    assert.NotNil(t, results[0][0], "Aggregation value must not be nil")
    assert.Equal(t, 200.0, results[0][0].(float64), "Max should be 200.0")
}

func TestPureAggregationWithoutData(t *testing.T) {
    db := setupTestDB()

    // NO data inserted

    // Query
    query := `[:find (max ?h) :where [?p :price/high ?h]]`
    results := db.ExecuteQuery(query)

    // Assertions
    assert.Equal(t, 0, len(results), "Empty aggregation should return 0 rows")
}
```

### Integration Test

```bash
# After fix, all these should work:
./datalog-cli -db datalog-db -query '[:find (max ?h) :where [?s :symbol/ticker "CRWV"] [?p :price/symbol ?s] [?p :price/high ?h]]'
# Expected: Single row with numeric value

./datalog-cli -db datalog-db -query '[:find (min ?t) (max ?t) :where [?s :symbol/ticker "CRWV"] [?p :price/symbol ?s] [?p :price/time ?t]]'
# Expected: Single row with two timestamp values

./datalog-cli -db datalog-db -query '[:find (max ?h) :where [?s :symbol/ticker "NOSUCHSYMBOL"] [?p :price/symbol ?s] [?p :price/high ?h]]'
# Expected: Zero rows (not one row with nil)
```

## Urgency

**CRITICAL BLOCKER** - This regression must be fixed before October 10 fixes can be deployed.

**Recommended Action**:
1. Revert October 10 subquery nil fixes temporarily
2. Investigate and fix properly with comprehensive test coverage
3. Ensure fix handles BOTH cases:
   - Empty aggregations → 0 rows (not nil)
   - Non-empty aggregations → computed values (not nil)

## Related Files

- `datalog/executor/subquery.go` - Subquery binding logic
- `datalog/executor/aggregation.go` - Aggregation execution (if exists)
- `datalog/executor/executor.go` - Main query execution
- Test suite: Need to add comprehensive pure aggregation tests

---

**Reported by**: gopher-street team
**Blocking**: All OHLC queries, statistical analysis, date range queries
**Priority**: P0 - Fix immediately before any October 10 changes can be deployed
