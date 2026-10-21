# Proposal: Query Builder - Fluent API for Datalog Queries

## Summary

Add a fluent query builder API that allows constructing Datalog queries programmatically with Go method chains, providing IDE autocomplete, compile-time method validation, and a more familiar experience for Go developers.

## Motivation

### Current Experience

Go developers must write Datalog queries as opaque strings:

```go
results, err := db.Query(`
    [:find ?symbol (max ?price)
     :in $ ?min-date
     :where [?t :trade/symbol ?symbol]
            [?t :trade/price ?price]
            [?t :trade/date ?date]
            [(> ?date ?min-date)]
            [(> ?price 100)]]
`, minDate)
```

Problems:
- **No IDE support** - No autocomplete, no syntax highlighting
- **No compile-time validation** - Typos discovered at runtime
- **Foreign syntax** - Go developers must learn Clojure/EDN
- **String manipulation** - Dynamic queries require error-prone concatenation
- **Refactoring hazard** - Renaming attributes requires manual string search

### Desired Experience

```go
q := datalog.Find("?symbol", datalog.Max("?price")).
    In("$", "?min-date").
    Where(
        datalog.Pattern("?t", ":trade/symbol", "?symbol"),
        datalog.Pattern("?t", ":trade/price", "?price"),
        datalog.Pattern("?t", ":trade/date", "?date"),
        datalog.Gt("?date", datalog.Var("?min-date")),
        datalog.Gt("?price", 100),
    )

var results []TradeResult
err := db.QueryInto(ctx, &results, q, minDate)
```

Benefits:
- Method autocomplete in IDE
- Compile-time validation of method names
- Familiar fluent API pattern
- Easy dynamic query construction
- Clear, readable query structure

## Design

### Core Types

```go
package datalog

// Query represents a built Datalog query
type Query struct {
    find    []FindElement
    in      []string
    where   []Clause
    orderBy []OrderElement
    limit   *int
    offset  *int
}

// FindElement represents an element in :find clause
type FindElement interface {
    findElement()
    String() string
}

// Clause represents a :where clause element
type Clause interface {
    clause()
    String() string
}
```

### Builder Entry Points

```go
// Find starts a new query with the given find elements
func Find(elements ...interface{}) *QueryBuilder

// From starts a query from a schema type (Phase 2)
func From[T any]() *TypedQueryBuilder[T]
```

### QueryBuilder Methods

```go
type QueryBuilder struct {
    query Query
}

// In specifies input parameters
func (qb *QueryBuilder) In(params ...string) *QueryBuilder

// Where adds where clauses
func (qb *QueryBuilder) Where(clauses ...Clause) *QueryBuilder

// OrderBy adds ordering
func (qb *QueryBuilder) OrderBy(elements ...OrderElement) *QueryBuilder

// Limit sets result limit
func (qb *QueryBuilder) Limit(n int) *QueryBuilder

// Offset sets result offset
func (qb *QueryBuilder) Offset(n int) *QueryBuilder

// Build returns the completed Query
func (qb *QueryBuilder) Build() (*Query, error)

// String returns the Datalog string representation
func (qb *QueryBuilder) String() string
```

### Pattern Construction

```go
// Pattern creates a data pattern clause [e a v] or [e a v tx]
func Pattern(elements ...interface{}) Clause

// Examples:
Pattern("?e", ":person/name", "?name")           // [?e :person/name ?name]
Pattern("?e", ":person/age", "?age", "?tx")      // [?e :person/age ?age ?tx]
Pattern("?e", ":person/name", "Alice")           // [?e :person/name "Alice"]
```

### Aggregation Functions

```go
// Aggregation functions for :find clause
func Count(variable string) FindElement
func Sum(variable string) FindElement
func Avg(variable string) FindElement
func Min(variable string) FindElement
func Max(variable string) FindElement

// Examples:
Find("?dept", Count("?emp"), Sum("?salary"))
```

### Predicate Functions

```go
// Comparison predicates
func Eq(args ...interface{}) Clause   // [(= ?x ?y)]
func NotEq(a, b interface{}) Clause   // [(!= ?x ?y)]
func Lt(args ...interface{}) Clause   // [(< ?x ?y)] or [(< ?x ?y ?z)]
func Lte(args ...interface{}) Clause  // [(<= ?x ?y)]
func Gt(args ...interface{}) Clause   // [(> ?x ?y)]
func Gte(args ...interface{}) Clause  // [(>= ?x ?y)]

// Variadic comparisons supported:
Lt("?min", "?x", "?max")  // [(< ?min ?x ?max)]
```

### Expression Functions

```go
// Arithmetic with output binding
func Add(a, b interface{}, output string) Clause      // [(+ ?a ?b) ?output]
func Sub(a, b interface{}, output string) Clause      // [(- ?a ?b) ?output]
func Mul(a, b interface{}, output string) Clause      // [(* ?a ?b) ?output]
func Div(a, b interface{}, output string) Clause      // [(/ ?a ?b) ?output]

// String operations
func Str(parts ...interface{}) Clause                 // [(str ?a " " ?b) ?output]

// Utility
func Ground(value interface{}, output string) Clause  // [(ground 42) ?x]
func Identity(input, output string) Clause            // [(identity ?x) ?y]
```

### Time Functions

```go
// Time extraction
func Year(input, output string) Clause    // [(year ?date) ?year]
func Month(input, output string) Clause   // [(month ?date) ?month]
func Day(input, output string) Clause     // [(day ?date) ?day]
func Hour(input, output string) Clause    // [(hour ?time) ?hour]
func Minute(input, output string) Clause  // [(minute ?time) ?min]
func Second(input, output string) Clause  // [(second ?time) ?sec]
```

### Logical Operations

```go
// NOT clauses
func Not(clauses ...Clause) Clause
func NotJoin(vars []string, clauses ...Clause) Clause

// OR clauses
func Or(branches ...[]Clause) Clause
func OrJoin(vars []string, branches ...[]Clause) Clause

// Examples:
Not(Pattern("?p", ":person/deceased", true))

Or(
    []Clause{Pattern("?p", ":person/role", "admin")},
    []Clause{Pattern("?p", ":person/role", "superuser")},
)
```

### Order By

```go
// Ordering elements
func Asc(variable string) OrderElement
func Desc(variable string) OrderElement

// Example:
OrderBy(Desc("?price"), Asc("?symbol"))
```

### Variable References

```go
// Var wraps a variable name for use in predicates with literal values
func Var(name string) Variable

// Distinguishes variable from string literal:
Gt("?price", 100)           // [(> ?price 100)]
Gt("?price", Var("?min"))   // [(> ?price ?min)]
```

## Examples

### Basic Query

```go
q := datalog.Find("?name", "?age").
    Where(
        datalog.Pattern("?p", ":person/name", "?name"),
        datalog.Pattern("?p", ":person/age", "?age"),
        datalog.Gt("?age", 21),
    )

// Generates:
// [:find ?name ?age
//  :where [?p :person/name ?name]
//         [?p :person/age ?age]
//         [(> ?age 21)]]
```

### With Input Parameters

```go
q := datalog.Find("?symbol", "?price").
    In("$", "?min-price", "?max-price").
    Where(
        datalog.Pattern("?t", ":trade/symbol", "?symbol"),
        datalog.Pattern("?t", ":trade/price", "?price"),
        datalog.Gt("?price", datalog.Var("?min-price")),
        datalog.Lt("?price", datalog.Var("?max-price")),
    )

results, err := db.Execute(ctx, q, 100.0, 200.0)
```

### Aggregation with Grouping

```go
q := datalog.Find("?dept", datalog.Count("?emp"), datalog.Avg("?salary")).
    Where(
        datalog.Pattern("?emp", ":employee/dept", "?dept"),
        datalog.Pattern("?emp", ":employee/salary", "?salary"),
    ).
    OrderBy(datalog.Desc("?salary"))

// Generates:
// [:find ?dept (count ?emp) (avg ?salary)
//  :where [?emp :employee/dept ?dept]
//         [?emp :employee/salary ?salary]
//  :order-by [(desc ?salary)]]
```

### Complex Query with NOT/OR

```go
q := datalog.Find("?name", "?email").
    Where(
        datalog.Pattern("?p", ":person/name", "?name"),
        datalog.Pattern("?p", ":person/email", "?email"),
        datalog.Not(
            datalog.Pattern("?p", ":person/status", "inactive"),
        ),
        datalog.Or(
            []datalog.Clause{datalog.Pattern("?p", ":person/role", "admin")},
            []datalog.Clause{datalog.Pattern("?p", ":person/role", "moderator")},
        ),
    )
```

### Dynamic Query Construction

```go
func buildTradeQuery(filters TradeFilters) *datalog.Query {
    qb := datalog.Find("?symbol", "?price", "?date").
        Where(
            datalog.Pattern("?t", ":trade/symbol", "?symbol"),
            datalog.Pattern("?t", ":trade/price", "?price"),
            datalog.Pattern("?t", ":trade/date", "?date"),
        )

    if filters.MinPrice > 0 {
        qb = qb.Where(datalog.Gte("?price", filters.MinPrice))
    }
    if filters.MaxPrice > 0 {
        qb = qb.Where(datalog.Lte("?price", filters.MaxPrice))
    }
    if !filters.StartDate.IsZero() {
        qb = qb.Where(datalog.Gte("?date", filters.StartDate))
    }
    if filters.Symbol != "" {
        qb = qb.Where(datalog.Pattern("?t", ":trade/symbol", filters.Symbol))
    }

    return qb.Build()
}
```

### With QueryInto

```go
type TradeResult struct {
    Symbol string    `datalog:"?symbol"`
    Price  float64   `datalog:"?price"`
    Date   time.Time `datalog:"?date"`
}

q := datalog.Find("?symbol", "?price", "?date").
    Where(
        datalog.Pattern("?t", ":trade/symbol", "?symbol"),
        datalog.Pattern("?t", ":trade/price", "?price"),
        datalog.Pattern("?t", ":trade/date", "?date"),
        datalog.Gt("?price", 100),
    ).
    OrderBy(datalog.Desc("?price")).
    Limit(10)

var results []TradeResult
err := db.QueryInto(ctx, &results, q)
```

## Implementation

### Phase 1: Core Builder (MVP)

Location: `datalog/builder/` package

```
datalog/builder/
├── query.go        // Query type and String() method
├── builder.go      // QueryBuilder with fluent methods
├── find.go         // Find elements and aggregations
├── clause.go       // Clause interface and Pattern
├── predicate.go    // Comparison predicates
├── expression.go   // Arithmetic and string expressions
├── logical.go      // NOT, OR clauses
├── order.go        // OrderBy elements
└── builder_test.go // Tests
```

### Query.String() Implementation

```go
func (q *Query) String() string {
    var buf strings.Builder

    // :find clause
    buf.WriteString("[:find ")
    for i, elem := range q.find {
        if i > 0 {
            buf.WriteString(" ")
        }
        buf.WriteString(elem.String())
    }

    // :in clause (if present)
    if len(q.in) > 0 {
        buf.WriteString("\n :in ")
        buf.WriteString(strings.Join(q.in, " "))
    }

    // :where clause
    buf.WriteString("\n :where ")
    for _, clause := range q.where {
        buf.WriteString("\n        ")
        buf.WriteString(clause.String())
    }

    // :order-by clause (if present)
    if len(q.orderBy) > 0 {
        buf.WriteString("\n :order-by [")
        for i, elem := range q.orderBy {
            if i > 0 {
                buf.WriteString(" ")
            }
            buf.WriteString(elem.String())
        }
        buf.WriteString("]")
    }

    buf.WriteString("]")
    return buf.String()
}
```

### Database Integration

```go
// In datalog/storage/database.go

// Execute runs a built query
func (db *Database) Execute(ctx context.Context, q *builder.Query, args ...interface{}) ([][]interface{}, error) {
    return db.Query(q.String(), args...)
}

// QueryInto with builder support (via interface)
func (db *Database) QueryInto(ctx context.Context, dest interface{}, query interface{}, args ...interface{}) error {
    var queryStr string
    switch q := query.(type) {
    case string:
        queryStr = q
    case *builder.Query:
        queryStr = q.String()
    case fmt.Stringer:
        queryStr = q.String()
    default:
        return fmt.Errorf("query must be string or *builder.Query, got %T", query)
    }
    // ... rest of implementation
}
```

### Phase 2: Schema-Aware Builder (Future)

```go
// Schema registration
type TradeSchema struct {
    Symbol datalog.Field[string]
    Price  datalog.Field[float64]
    Date   datalog.Field[time.Time]
}

var Trade = datalog.Register[TradeSchema](":trade")

// Usage with compile-time field validation
q := datalog.From(Trade).
    Find(Trade.Symbol, datalog.Max(Trade.Price)).
    Where(
        Trade.Price.Gt(100),
        Trade.Date.After(startDate),
    )
```

This phase requires more design work and is deferred.

## Testing Strategy

1. **Unit tests** for each builder method
2. **String output tests** - verify generated Datalog syntax
3. **Round-trip tests** - build query, execute, verify results
4. **Error case tests** - invalid combinations, missing required elements
5. **Integration tests** - full queries against BadgerDB

## Validation

The builder performs validation at `Build()` time:

```go
func (qb *QueryBuilder) Build() (*Query, error) {
    // Must have at least one find element
    if len(qb.query.find) == 0 {
        return nil, errors.New("query must have at least one :find element")
    }

    // Must have at least one where clause
    if len(qb.query.where) == 0 {
        return nil, errors.New("query must have at least one :where clause")
    }

    // Validate variable references (optional, can be expensive)
    if err := qb.validateVariables(); err != nil {
        return nil, err
    }

    return &qb.query, nil
}
```

## Comparison with Alternatives

### vs. Raw Strings

| Aspect | Raw String | Query Builder |
|--------|------------|---------------|
| IDE autocomplete | None | Full |
| Typo detection | Runtime | Compile-time (methods) |
| Dynamic queries | String concat | Method chaining |
| Learning curve | Must learn EDN | Familiar Go patterns |
| Flexibility | Full Datalog | Supported subset |

### vs. ORM-style (GORM, Ent)

| Aspect | ORM | Query Builder |
|--------|-----|---------------|
| Query power | Limited | Full Datalog |
| Joins | Explicit/eager | Implicit via patterns |
| Aggregations | Basic | Full support |
| Temporal queries | Manual | Native |
| Learning curve | Familiar | New but intuitive |

## Escape Hatch

Raw Datalog strings remain available for:
- Queries not expressible with builder (edge cases)
- Rules (when implemented)
- Migration of existing queries
- Power users who prefer the syntax

```go
// Builder for common cases
q := datalog.Find("?x").Where(datalog.Pattern("?x", ":foo", "bar"))

// Raw string for complex cases
results, err := db.Query(`[:find ?x :where (recursive-rule ?x ?y)]`)
```

## Estimated Effort

### Phase 1: Core Builder
- **Core types and builder**: 300-400 lines
- **Pattern/predicate/expression**: 200-300 lines
- **Tests**: 400-500 lines
- **Documentation**: 150 lines

**Timeline**: 3-5 days

### Phase 2: Schema-Aware (Future)
- Additional 500-700 lines
- Requires generics design work

**Timeline**: 5-7 days (deferred)

## Open Questions

1. **Package location**: `datalog/builder/` or `datalog/query/builder/`?
   - Recommendation: `datalog/builder/` for clean imports

2. **Method chaining style**: Return `*QueryBuilder` or `QueryBuilder`?
   - Recommendation: Pointer for consistency with Go conventions

3. **Validation timing**: At each method call or only at `Build()`?
   - Recommendation: `Build()` only, for flexibility during construction

4. **Subquery support**: How to express nested queries?
   - Recommendation: Defer to Phase 2, use raw strings for now

## References

- [Datomic Query Documentation](https://docs.datomic.com/query/query-data-reference.html)
- [GORM Query Builder](https://gorm.io/docs/query.html) - inspiration for Go patterns
- [Ent Query Builder](https://entgo.io/docs/crud) - inspiration for type safety
