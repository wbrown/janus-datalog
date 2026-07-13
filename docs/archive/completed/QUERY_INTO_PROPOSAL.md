# Proposal: QueryInto[T] - Typed Query Results

## Summary

Add a `QueryInto` method that automatically populates typed Go structs from query results, eliminating the manual tuple iteration boilerplate that every downstream consumer currently writes.

## Motivation

### Current Pain Point

Every query consumer writes the same boilerplate:

```go
results, err := db.Query(`
    [:find ?symbol ?price ?volume ?date
     :where [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]
            [?t :trade/volume ?volume]
            [?t :trade/date ?date]]
`)
if err != nil {
    return err
}

// Every. Single. Consumer. Writes. This.
trades := make([]Trade, 0, len(results))
for _, tuple := range results {
    trade := Trade{
        Symbol: tuple[0].(string),
        Price:  tuple[1].(float64),
        Volume: tuple[2].(int64),
        Date:   tuple[3].(time.Time),
    }
    trades = append(trades, trade)
}
```

Problems with this approach:
- **Duplicated code** - Same iteration logic at every call site
- **Runtime panics** - Type assertions fail at runtime, not compile time
- **Index errors** - Easy to get symbol order wrong (`tuple[2]` vs `tuple[3]`)
- **Type mismatches** - Was it `int` or `int64`? `float32` or `float64`?
- **No IDE support** - Magic indices, no autocomplete

### Desired Experience

```go
var trades []Trade
err := db.QueryInto(ctx, &trades, `
    [:find ?symbol ?price ?volume ?date
     :where [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]
            [?t :trade/volume ?volume]
            [?t :trade/date ?date]]
`)
```

One line. Typed. Safe.

## Design

### API Surface

```go
// QueryInto executes a query and populates dest with typed results.
// dest must be a pointer to a slice of structs.
// Struct fields are matched to query results via `datalog` tags.
func (db *Database) QueryInto(ctx context.Context, dest interface{}, query string, args ...interface{}) error

// QueryOneInto executes a query expecting exactly one result.
// Returns ErrNotFound if no results, ErrMultipleResults if more than one.
func (db *Database) QueryOneInto(ctx context.Context, dest interface{}, query string, args ...interface{}) error
```

### Struct Tag Mapping

Fields map to query variables via the `datalog` struct tag:

```go
type TradeResult struct {
    Symbol string    `datalog:"?symbol"`
    High   float64   `datalog:"?high"`
    Date   time.Time `datalog:"?date"`
}
```

The tag value matches the variable name in the `:find` clause.

### Mapping Strategies

#### Strategy 1: Positional (Simple)

Match struct fields to `:find` clause by position:

```go
// Query: [:find ?symbol ?price ?date ...]
type Result struct {
    Symbol string    // position 0 -> ?symbol
    Price  float64   // position 1 -> ?price
    Date   time.Time // position 2 -> ?date
}
```

**Pros:** No tags required for simple cases
**Cons:** Fragile, order-dependent

#### Strategy 2: Tag-Based (Recommended)

Match struct fields to `:find` variables by tag:

```go
// Query: [:find ?symbol ?price ?date ...]
type Result struct {
    Date   time.Time `datalog:"?date"`   // matches ?date regardless of position
    Symbol string    `datalog:"?symbol"` // matches ?symbol
    Price  float64   `datalog:"?price"`  // matches ?price
}
```

**Pros:** Order-independent, self-documenting, refactoring-safe
**Cons:** Requires tags

#### Strategy 3: Hybrid (Recommended Default)

1. If struct has `datalog` tags, use tag-based mapping
2. If no tags, fall back to positional mapping
3. Mixed (some tagged, some not) returns an error

### Type Coercion

The implementation handles common type conversions:

| Query Result | Struct Field | Conversion |
|--------------|--------------|------------|
| `int64` | `int` | Safe narrowing with overflow check |
| `int64` | `int64` | Direct |
| `float64` | `float32` | Narrowing |
| `float64` | `float64` | Direct |
| `string` | `string` | Direct |
| `time.Time` | `time.Time` | Direct |
| `time.Time` | `int64` | Unix timestamp |
| `Identity` | `Identity` | Direct |
| `Identity` | `string` | String representation |
| `nil` | `*T` | nil pointer |
| `nil` | `T` | Error (required field) |

### Nullable Fields

Use pointers for optional values:

```go
type Result struct {
    Name  string   `datalog:"?name"`           // required
    Email *string  `datalog:"?email,optional"` // nil if NULL/missing
}
```

### Aggregation Results

Works naturally with aggregations:

```go
type DeptStats struct {
    Dept       string  `datalog:"?dept"`
    TotalPay   float64 `datalog:"?total"`
    HeadCount  int64   `datalog:"?count"`
    AvgSalary  float64 `datalog:"?avg"`
}

var stats []DeptStats
err := db.QueryInto(ctx, &stats, `
    [:find ?dept (sum ?salary) (count ?emp) (avg ?salary)
     :where [?emp :employee/dept ?dept]
            [?emp :employee/salary ?salary]]
`)
```

Note: Aggregation result variables use the pattern `(agg ?var)` -> `?var` for tag matching.

## Implementation

### Location

`datalog/storage/database.go` - alongside existing `Query` method

### Dependencies

- Existing `Query` execution
- Reflection machinery from `datalog/reflect/` package
- Type conversion utilities

### Implementation Sketch

```go
func (db *Database) QueryInto(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
    // 1. Validate dest is *[]T where T is a struct
    destVal := reflect.ValueOf(dest)
    if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
        return fmt.Errorf("dest must be pointer to slice, got %T", dest)
    }

    sliceVal := destVal.Elem()
    elemType := sliceVal.Type().Elem()
    if elemType.Kind() != reflect.Struct {
        return fmt.Errorf("slice element must be struct, got %v", elemType.Kind())
    }

    // 2. Execute query normally
    results, err := db.Query(query, args...)
    if err != nil {
        return err
    }

    // 3. Parse :find clause to get variable names
    findVars, err := parseQueryFindVars(query)
    if err != nil {
        return err
    }

    // 4. Build field mapping (tag -> field index)
    fieldMap, err := buildFieldMapping(elemType, findVars)
    if err != nil {
        return err
    }

    // 5. Populate slice
    newSlice := reflect.MakeSlice(sliceVal.Type(), 0, len(results))
    for _, tuple := range results {
        elem := reflect.New(elemType).Elem()
        if err := populateStruct(elem, tuple, fieldMap); err != nil {
            return fmt.Errorf("tuple %d: %w", i, err)
        }
        newSlice = reflect.Append(newSlice, elem)
    }

    sliceVal.Set(newSlice)
    return nil
}
```

### Error Handling

```go
var (
    ErrNotFound        = errors.New("query returned no results")
    ErrMultipleResults = errors.New("query returned multiple results")
    ErrTypeMismatch    = errors.New("cannot convert query result to field type")
    ErrMissingField    = errors.New("required field has no value")
    ErrUnmappedSymbol  = errors.New("query symbol has no matching struct field")
)
```

## Examples

### Basic Usage

```go
type Person struct {
    Name string `datalog:"?name"`
    Age  int64  `datalog:"?age"`
}

var people []Person
err := db.QueryInto(ctx, &people, `
    [:find ?name ?age
     :where [?p :person/name ?name]
            [?p :person/age ?age]
            [(> ?age 21)]]
`)
```

### With Input Parameters

```go
type Trade struct {
    Price float64   `datalog:"?price"`
    Time  time.Time `datalog:"?time"`
}

var trades []Trade
err := db.QueryInto(ctx, &trades, `
    [:find ?price ?time
     :in $ ?symbol ?min-price
     :where [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]
            [?t :trade/time ?time]
            [(> ?price ?min-price)]]
`, "NVDA", 100.0)
```

### Single Result

```go
type MaxPrice struct {
    Symbol string  `datalog:"?symbol"`
    High   float64 `datalog:"?high"`
}

var result MaxPrice
err := db.QueryOneInto(ctx, &result, `
    [:find ?symbol (max ?price)
     :in $ ?symbol
     :where [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]]
`, "NVDA")
```

### Anonymous Structs

For one-off queries:

```go
var results []struct {
    Symbol string  `datalog:"?symbol"`
    Volume float64 `datalog:"?volume"`
}
err := db.QueryInto(ctx, &results, query)
```

## Testing Strategy

1. **Unit tests** for type coercion (each type combination)
2. **Unit tests** for mapping strategies (positional, tagged, hybrid)
3. **Unit tests** for error cases (wrong types, missing fields, etc.)
4. **Integration tests** with real queries against BadgerDB
5. **Benchmark** vs manual iteration (should be comparable)

## Migration Path

- `Query` remains unchanged for power users and complex cases
- `QueryInto` is additive, no breaking changes
- Documentation recommends `QueryInto` for new code

## Future Extensions

1. **QueryBuilder integration** - `db.QueryInto(ctx, &results, queryBuilder)`
2. **Streaming results** - `db.QueryIntoChannel(ctx, resultChan, query)`
3. **Custom type handlers** - Register converters for custom types

## Estimated Effort

- **Core implementation**: 200-300 lines
- **Tests**: 300-400 lines
- **Documentation**: 100 lines

**Timeline**: 1-2 days

## Open Questions

1. Should unmapped query symbols be an error or silently ignored?
   - Recommendation: Warning log, not error (allows SELECT * style flexibility)

2. Should we support maps as dest (`[]map[string]interface{}`)?
   - Recommendation: Not initially, structs only

3. Should positional mapping require explicit opt-in?
   - Recommendation: Yes, tag-based is safer default
