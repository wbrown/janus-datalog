# Feature Implemented: Storage Layer Parameterized Query Support

**Status**: ✅ **COMPLETED**
**Commit**: `5c5edaa`
**Date**: October 12, 2025

## Summary

The Storage layer (Database) now supports parameterized queries with full `:in` clause support, eliminating the need for unsafe string formatting.

## API Added

### 1. ExecuteQuery - Simple Query Execution

```go
func (d *Database) ExecuteQuery(queryStr string) ([][]interface{}, error)
```

**Example:**
```go
results, err := db.ExecuteQuery(`[:find ?name :where [?e :person/name ?name]]`)
```

### 2. ExecuteQueryWithInputs - Parameterized Queries

```go
func (d *Database) ExecuteQueryWithInputs(queryStr string, inputs ...interface{}) ([][]interface{}, error)
```

**Examples:**

**Scalar Input:**
```go
results, err := db.ExecuteQueryWithInputs(
    `[:find ?e
      :in $ ?name
      :where [?e :person/name ?name]]`,
    "Alice",
)
```

**Multiple Scalar Inputs:**
```go
results, err := db.ExecuteQueryWithInputs(
    `[:find (max ?time)
      :in $ ?symbol
      :where [?s :symbol/ticker ?symbol]
             [?p :price/symbol ?s]
             [?p :price/time ?time]]`,
    "CRWV",
)
```

**Collection Input:**
```go
results, err := db.ExecuteQueryWithInputs(
    `[:find ?name ?food
      :in $ [?food ...]
      :where [?e :person/name ?name]
             [?e :person/likes ?food]]`,
    []string{"pizza", "pasta", "gelato"},
)
```

**Relation Input:**
```go
results, err := db.ExecuteQueryWithInputs(
    `[:find ?e
      :in $ [[?name ?age] ...]
      :where [?e :person/name ?name]
             [?e :person/age ?age]]`,
    [][]interface{}{
        {"Alice", int64(30)},
        {"Bob", int64(25)},
    },
)
```

**Time Input:**
```go
startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
results, err := db.ExecuteQueryWithInputs(
    `[:find ?time ?close
      :in $ ?symbol
      :where [?s :symbol/ticker ?symbol]
             [?p :price/symbol ?s]
             [?p :price/time ?time]
             [?p :price/close ?close]]`,
    "CRWV",
)
```

### 3. GetExecutor - Power User Access

```go
func (d *Database) GetExecutor() *executor.Executor
```

Direct access to the executor for advanced use cases.

## Benefits for Gopher-Street

### Before (Unsafe)
```go
// ❌ String formatting - vulnerable to injection
query := fmt.Sprintf(`[:find (max ?time)
 :where [?s :symbol/ticker "%s"] ...]`, userInput)

results, err := storage.ExecuteQuery(query)
```

### After (Safe)
```go
// ✅ Parameterized - type-safe, no injection
query := `[:find (max ?time)
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol] ...]`

results, err := storage.ExecuteQueryWithInputs(query, symbol)
```

## Input Type Support

All Datomic `:in` clause types are supported:

| Type | Syntax | Go Type | Example |
|------|--------|---------|---------|
| Database | `$` | - | Always first, no input needed |
| Scalar | `?var` | `interface{}` | `"Alice"`, `int64(25)`, `time.Now()` |
| Collection | `[?var ...]` | `[]T` | `[]string{"a", "b"}` |
| Tuple | `[[?v1 ?v2]]` | `[]interface{}` | `[]interface{}{"Alice", 30}` |
| Relation | `[[?v1 ?v2] ...]` | `[][]interface{}` | `[][]interface{}{{"Alice", 30}, {"Bob", 25}}` |

## Error Handling

Comprehensive error messages for:
- Input count mismatch (too few/too many)
- Input type mismatch (expected slice, got string)
- Collection/tuple length mismatch
- Query parsing errors
- Query execution errors

## Performance

- **Query plan caching**: Parsed queries automatically cached by Database
- **No string formatting overhead**: Direct value passing
- **Streaming results**: Iterator-based processing
- **Zero overhead**: Uses existing executor infrastructure

## Test Coverage

8 comprehensive test cases covering:
- ✅ Simple query execution
- ✅ Scalar inputs (single and multiple)
- ✅ Collection inputs
- ✅ Tuple inputs
- ✅ Relation inputs
- ✅ Time.Time inputs
- ✅ Error handling (wrong count, wrong types)

## Migration Guide

### Existing Code Pattern
```go
// OLD: String formatting
query := fmt.Sprintf(`[:find (max ?time)
 :where [?s :symbol/ticker "%s"]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]]`, ctx.Symbol)

results, err := storage.ExecuteQuery(query)
```

### New Pattern
```go
// NEW: Parameterized
query := `[:find (max ?time)
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]]`

results, err := storage.ExecuteQueryWithInputs(query, ctx.Symbol)
```

## Implementation Details

### Type Conversion
- Uses `reflect` package to handle Go slices/arrays
- Converts Go values to `executor.Relation` internally
- Supports all standard Go types (string, int64, float64, bool, time.Time)
- Identity/Reference values passed through directly

### Architecture
```
User Code
    ↓
ExecuteQueryWithInputs(queryStr, inputs...)
    ↓
parser.ParseQuery(queryStr) → Query AST
    ↓
convertInputsToRelations(q, inputs) → []Relation
    ↓
executor.ExecuteWithRelations(ctx, q, inputRelations) → Relation
    ↓
relationToSlice(result) → [][]interface{}
    ↓
Return to User
```

### Backward Compatibility

✅ **Fully backward compatible** - existing code using the Matcher/Executor directly continues to work unchanged.

## Files Changed

- `datalog/storage/database.go` (+204 lines)
  - ExecuteQuery method
  - ExecuteQueryWithInputs method
  - GetExecutor method
  - convertInputsToRelations helper
  - relationToSlice helper
- `datalog/storage/query_inputs_test.go` (+455 lines)
  - 8 comprehensive test cases

## Related Work

This feature leverages existing infrastructure:
- Parser: `datalog/parser/in_clause_test.go` - Already had full `:in` parsing
- Executor: `datalog/executor/executor.go` - Already had `ExecuteWithRelations`
- Query Planner: Already handled input parameter binding

**This feature simply exposes existing capabilities at the Storage layer.**

## Next Steps for Gopher-Street

1. **Update imports**: No changes needed if already using `datalog/storage`
2. **Refactor queries**: Replace string formatting with parameterized queries
3. **Add `:in` clauses**: Update query strings to include `:in $` and parameter placeholders
4. **Pass parameters**: Call `ExecuteQueryWithInputs` with values instead of formatting strings
5. **Test**: Existing queries should work with minimal changes

## Example: OHLC Query Migration

### Before
```go
query := fmt.Sprintf(`[:find (max ?time)
 :where [?s :symbol/ticker "%s"]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day ?mod]
        [(>= ?mod 570)]
        [(<= ?mod 960)]]`, ctx.Symbol)

maxResults, err := ctx.Storage.ExecuteQuery(query)
```

### After
```go
query := `[:find (max ?time)
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day ?mod]
        [(>= ?mod 570)]
        [(<= ?mod 960)]]`

maxResults, err := ctx.Storage.ExecuteQueryWithInputs(query, ctx.Symbol)
```

**Benefits:**
- ✅ Type-safe (no string escaping needed)
- ✅ Faster (query plan cached)
- ✅ Cleaner (no fmt.Sprintf)
- ✅ Safer (no injection risks)

## Questions?

See the original feature request: `FEATURE_REQUEST_STORAGE_INPUT_PARAMS.md`

Or check the test suite for complete examples: `datalog/storage/query_inputs_test.go`
