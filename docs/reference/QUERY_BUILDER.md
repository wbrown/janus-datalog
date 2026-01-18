# Query Builder (qb)

The `qb` package provides an idiomatic Go API for building Datalog queries without string manipulation. The key insight: **Go variable identity IS the join condition** - using the same `*Var` in multiple patterns creates a join.

## Overview

```go
import "github.com/wbrown/janus-datalog/datalog/qb"

// Define attributes as package-level constants - this is the recommended pattern
var (
    PersonName = qb.Kw(":person/name")
    PersonAge  = qb.Kw(":person/age")
    PersonCity = qb.Kw(":person/city")
)

func FindAdults(db *storage.Database) ([][]interface{}, error) {
    // Variables are created per-query - same pointer = same logical variable
    e := qb.NewVar("e")
    name := qb.NewVar("name")
    age := qb.NewVar("age")

    q := qb.Query().
        Find(name, age).
        Where(
            qb.Pat(e, PersonName, name),
            qb.Pat(e, PersonAge, age),  // same `e` = join!
            qb.Gt(age, 21),
        ).
        MustBuild()

    return db.ExecuteQuery(q)
}
```

## Attributes

**Define attributes as package-level constants.** This prevents typos, enables IDE completion, and makes refactoring safe.

```go
// schema.go - define your schema's attributes once
package myapp

import "github.com/wbrown/janus-datalog/datalog/qb"

// Person attributes
var (
    PersonName   = qb.Kw(":person/name")
    PersonAge    = qb.Kw(":person/age")
    PersonEmail  = qb.Kw(":person/email")
    PersonCity   = qb.Kw(":person/city")
    PersonActive = qb.Kw(":person/active")
)

// Order attributes
var (
    OrderCustomer = qb.Kw(":order/customer")
    OrderTotal    = qb.Kw(":order/total")
    OrderDate     = qb.Kw(":order/date")
    OrderStatus   = qb.Kw(":order/status")
)
```

Then use them throughout your code:

```go
// queries.go
q := qb.Query().
    Find(name, total).
    Where(
        qb.Pat(p, PersonName, name),
        qb.Pat(o, OrderCustomer, p),
        qb.Pat(o, OrderTotal, total),
    ).
    MustBuild()
```

**Why this matters:**
- Typo in `":person/naem"` silently returns no results
- Constant `PersonName` catches typos at compile time
- Refactoring attribute names is safe with find-and-replace
- IDE autocompletion shows available attributes

## Variables

Variables represent unknowns in your query. The same `*Var` pointer used in multiple places creates a join condition.

```go
e := qb.NewVar("e")
name := qb.NewVar("name")
age := qb.NewVar("age")

// Same variable in multiple patterns = join
qb.Pat(e, PersonName, name)
qb.Pat(e, PersonAge, age)  // joins on e
```

## Patterns

`Pat` creates data patterns of any arity. Patterns can match against:
- The database (EAVT storage)
- Input relations of any arity

```go
// Database patterns
qb.Pat(e, a, v)              // [e a v] - standard pattern
qb.Pat(e, a, v, tx)          // [e a v tx] - with transaction
qb.Pat(e, a, v, tx, op)      // [e a v tx op] - history pattern

// Input relation patterns (any arity)
qb.Pat(name, age)            // 2-tuple
qb.Pat(a, b, c, d, e, f)     // 6-tuple

// Wildcards
qb.Pat(e, qb.Blank(), name)  // ignore attribute
```

### Pattern Arguments

Patterns accept:
- `*Var` - query variables
- `qb.Kw(":attr/name")` - keyword attributes
- `qb.V(value)` - constant values
- `qb.Blank()` - wildcard (ignore position)
- Raw Go values - converted to constants

```go
// Using Kw for attributes
PersonName := qb.Kw(":person/name")
qb.Pat(e, PersonName, name)

// Using V for constants
qb.Pat(e, PersonName, qb.V("Alice"))

// Raw values work too
qb.Pat(e, PersonName, "Alice")
```

## Predicates

Comparison predicates filter results:

```go
qb.Lt(age, 30)       // age < 30
qb.Lte(age, 30)      // age <= 30
qb.Gt(age, 18)       // age > 18
qb.Gte(age, 18)      // age >= 18
qb.Eq(city, "NYC")   // city = "NYC"
qb.Ne(status, "X")   // status != "X"
```

### Chained Comparisons

```go
// Exclusive range: 18 < age < 65
qb.Range(18, age, 65)

// Inclusive range: 1 <= rating <= 5
qb.RangeInclusive(1, rating, 5)

// General chained comparison with any operator
qb.Chained(query.OpLT, a, b, c, d)  // a < b < c < d
```

### Comparison Binding

Comparisons can also **bind their boolean result** to a variable using `.As()`:

```go
hasItems := qb.NewVar("hasItems")

q := qb.Query().
    Find(name, count, hasItems).
    Where(
        qb.Pat(e, ItemName, name),
        qb.Pat(e, ItemCount, count),
        qb.Gt(count, 0).As(hasItems),  // binds true/false to hasItems
    ).
    MustBuild()

// Results include the boolean:
// [["Widget", 5, true], ["Gadget", 0, false], ...]
```

This works with all comparison types:

```go
qb.Gt(x, 0).As(isPositive)           // [(> ?x 0) ?is-positive]
qb.Lt(x, 100).As(isSmall)            // [(< ?x 100) ?is-small]
qb.Eq(status, "active").As(isActive) // [(= ?status "active") ?is-active]
qb.Range(0, x, 100).As(inRange)      // [(< 0 ?x 100) ?in-range]
```

## Aggregations

```go
qb.Query().
    Find(dept, qb.Sum(salary)).
    Where(
        qb.Pat(e, PersonDept, dept),
        qb.Pat(e, PersonSalary, salary),
    ).
    MustBuild()
```

Available aggregations:
- `qb.Sum(v)` - sum of values
- `qb.Count(v)` - count of values
- `qb.Avg(v)` - average of values
- `qb.Min(v)` - minimum value
- `qb.Max(v)` - maximum value

## Expressions

Expressions compute values and bind them to variables:

### Arithmetic

```go
total := qb.NewVar("total")
qb.Add(price, tax).As(total)
qb.Sub(gross, deductions).As(net)
qb.Mul(quantity, unitPrice).As(lineTotal)
qb.Div(total, count).As(average)
```

### String Concatenation

```go
fullName := qb.NewVar("fullName")
qb.Str(firstName, " ", lastName).As(fullName)
```

### Ground Values

```go
constant := qb.NewVar("constant")
qb.Ground(42).As(constant)
```

### Time Extraction

```go
y := qb.NewVar("y")
qb.Year(timestamp).As(y)
qb.Month(timestamp).As(m)
qb.Day(timestamp).As(d)
qb.Hour(timestamp).As(h)
qb.Minute(timestamp).As(min)
qb.Second(timestamp).As(sec)
```

## Database Functions

Database functions access entity attributes with special semantics for missing values.

### GetElse - Default Values

Returns an attribute value, or a default if the attribute is missing:

```go
nickname := qb.NewVar("nickname")

q := qb.Query().
    Find(name, nickname).
    Where(
        qb.Pat(e, PersonName, name),
        qb.GetElse(e, PersonNickname, "Anonymous").As(nickname),
    ).
    MustBuild()

// Results:
// [["Alice", "Ali"], ["Bob", "Anonymous"], ...]
// Bob has no nickname, so gets the default
```

Equivalent EDN: `[(get-else $ ?e :person/nickname "Anonymous") ?nickname]`

### Missing - Check Attribute Absence

**As a predicate** (filter rows where attribute is missing):

```go
q := qb.Query().
    Find(name).
    Where(
        qb.Pat(e, PersonName, name),
        qb.Missing(e, PersonEmail),  // only people without email
    ).
    MustBuild()
```

Equivalent EDN: `[(missing? $ ?e :person/email)]`

**As an expression** (bind boolean result):

```go
needsEmail := qb.NewVar("needsEmail")

q := qb.Query().
    Find(name, needsEmail).
    Where(
        qb.Pat(e, PersonName, name),
        qb.Missing(e, PersonEmail).As(needsEmail),  // true if missing
    ).
    MustBuild()

// Results:
// [["Alice", false], ["Bob", true], ...]
// Alice has email, Bob doesn't
```

Equivalent EDN: `[(missing? $ ?e :person/email) ?needs-email]`

### GetSome - Fallback Attribute Chain

Returns the first available attribute from a list (useful for display names, fallbacks):

```go
displayName := qb.NewVar("displayName")

q := qb.Query().
    Find(name, displayName).
    Where(
        qb.Pat(e, PersonName, name),
        qb.GetSome(e, PersonNickname, PersonFullName, PersonEmail).As(displayName),
    ).
    MustBuild()

// Returns first available: nickname > fullname > email
// [["Alice", "Ali"], ["Bob", "Robert Jones"], ["Charlie", "charlie@example.com"]]
```

Equivalent EDN: `[(get-some $ ?e :person/nickname :person/fullname :person/email) ?display-name]`

## Input Parameters

Input parameters allow parameterized queries:

```go
// Database source (always first)
qb.DB

// Scalar input - single value
qb.Scalar(nameVar)

// Collection input - iterate over values
qb.Collection(nameVar)

// Tuple input - single row of values
qb.Tuple(nameVar, ageVar)

// Relation input - multiple rows
qb.Relation(nameVar, ageVar)
```

### Scalar Input Example

```go
inputName := qb.NewVar("inputName")
age := qb.NewVar("age")

q := qb.Query().
    Find(inputName, age).
    In(qb.DB, qb.Scalar(inputName)).
    Where(
        qb.Pat(e, PersonName, inputName),
        qb.Pat(e, PersonAge, age),
    ).
    MustBuild()

// Execute with input value
results, err := db.ExecuteQueryWithInputs(q, "Alice")
```

### Collection Input Example

```go
inputName := qb.NewVar("inputName")
age := qb.NewVar("age")

q := qb.Query().
    Find(inputName, age).
    In(qb.DB, qb.Collection(inputName)).
    Where(
        qb.Pat(e, PersonName, inputName),
        qb.Pat(e, PersonAge, age),
    ).
    MustBuild()

// Execute with multiple values - returns results for each
results, err := db.ExecuteQueryWithInputs(q, []string{"Alice", "Bob", "Charlie"})
```

### Relation Input Example

```go
inputName := qb.NewVar("inputName")
inputCity := qb.NewVar("inputCity")
age := qb.NewVar("age")

q := qb.Query().
    Find(inputName, inputCity, age).
    In(qb.DB, qb.Relation(inputName, inputCity)).
    Where(
        qb.Pat(e, PersonName, inputName),
        qb.Pat(e, PersonCity, inputCity),
        qb.Pat(e, PersonAge, age),
    ).
    MustBuild()

// Execute with relation - finds matching (name, city) pairs
results, err := db.ExecuteQueryWithInputs(q, [][]interface{}{
    {"Alice", "NYC"},
    {"Bob", "LA"},
})
```

## Logical Clauses

### NOT

Exclude results matching patterns:

```go
// Exclude inactive people
qb.Not(
    qb.Pat(e, PersonActive, false),
)

// NOT with join variable
qb.NotJoin([]*qb.Var{e},
    qb.Pat(e, PersonStatus, qb.V("banned")),
)
```

### OR

Match any of several alternatives:

```go
// Match NYC or LA
qb.Or().
    Branch(qb.Pat(e, PersonCity, qb.V("NYC"))).
    Branch(qb.Pat(e, PersonCity, qb.V("LA")))

// OR with join variables
qb.OrJoin(e, city).
    Branch(qb.Pat(e, PersonCity, city), qb.Eq(city, "NYC")).
    Branch(qb.Pat(e, PersonCity, city), qb.Eq(city, "LA"))
```

## Ordering

```go
qb.Query().
    Find(name, age).
    Where(...).
    OrderBy(qb.Desc(age), qb.Asc(name)).
    MustBuild()
```

## Subqueries

```go
// Inner query finds max salary per department
innerQ := qb.Query().
    Find(qb.Max(innerSalary)).
    In(qb.DB, qb.Scalar(dept)).
    Where(
        qb.Pat(emp, EmpDept, dept),
        qb.Pat(emp, EmpSalary, innerSalary),
    )

// Outer query uses subquery
maxSalary := qb.NewVar("maxSalary")
q := qb.Query().
    Find(name, maxSalary).
    Where(
        qb.Pat(p, PersonName, name),
        qb.Pat(p, PersonDept, dept),
        qb.Subquery(innerQ, dept).BindTuple(maxSalary),
    ).
    MustBuild()
```

Binding forms:
- `BindTuple(vars...)` - single row result `[[?a ?b]]`
- `BindRelation(vars...)` - multiple rows `[[?a ?b] ...]`
- `BindCollection(v)` - single column `[?a ...]`

## Building Queries

```go
// Build returns (*query.Query, error)
q, err := qb.Query().
    Find(name).
    Where(qb.Pat(e, PersonName, name)).
    Build()
if err != nil {
    return err
}

// MustBuild panics on error - useful for static queries
var FindAllPeople = qb.Query().
    Find(name).
    Where(qb.Pat(e, PersonName, name)).
    MustBuild()
```

## Database Integration

All database query methods accept both `*query.Query` and EDN strings:

```go
// Basic execution
results, err := db.ExecuteQuery(q)
results, err := db.ExecuteQueryWithInputs(q, inputs...)

// Explain query plan
plan, err := db.Explain(q)
```

### QueryInto - Typed Results

Query directly into Go structs. With the query builder, **fields map positionally** to your `Find()` elements - no tags needed:

```go
// Define attributes
var (
    PersonName = qb.Kw(":person/name")
    PersonAge  = qb.Kw(":person/age")
)

// Result struct - fields map positionally to Find() order
type PersonResult struct {
    Name string  // maps to first Find() element
    Age  int64   // maps to second Find() element
}

func FindAdults(db *storage.Database) ([]PersonResult, error) {
    e := qb.NewVar("e")
    name := qb.NewVar("name")
    age := qb.NewVar("age")

    q := qb.Query().
        Find(name, age).
        Where(
            qb.Pat(e, PersonName, name),
            qb.Pat(e, PersonAge, age),
            qb.Gte(age, 18),
        ).
        MustBuild()

    var results []PersonResult
    err := db.QueryInto(&results, q)
    return results, err
}
```

### QueryOneInto - Single Result

For queries that return exactly one result:

```go
func FindPerson(db *storage.Database, personName string) (*PersonResult, error) {
    e := qb.NewVar("e")
    name := qb.NewVar("name")
    age := qb.NewVar("age")

    q := qb.Query().
        Find(name, age).
        In(qb.DB, qb.Scalar(name)).
        Where(
            qb.Pat(e, PersonName, name),
            qb.Pat(e, PersonAge, age),
        ).
        MustBuild()

    var result PersonResult
    found, err := db.QueryOneInto(&result, q, personName)
    if err != nil {
        return nil, err
    }
    if !found {
        return nil, nil // Not found
    }
    return &result, nil
}
```

### Aggregations with QueryInto

Positional mapping works with aggregations too - fields map by position:

```go
type DeptStats struct {
    Dept      string   // maps to first Find() element (dept)
    AvgSalary float64  // maps to second Find() element (avg salary)
    Count     int64    // maps to third Find() element (count emp)
}

func GetDeptStats(db *storage.Database) ([]DeptStats, error) {
    emp := qb.NewVar("emp")
    dept := qb.NewVar("dept")
    salary := qb.NewVar("salary")

    q := qb.Query().
        Find(dept, qb.Avg(salary), qb.Count(emp)).
        Where(
            qb.Pat(emp, EmpDept, dept),
            qb.Pat(emp, EmpSalary, salary),
        ).
        MustBuild()

    var stats []DeptStats
    err := db.QueryInto(&stats, q)
    return stats, err
}
```

## Complete Example

```go
package main

import (
    "fmt"
    "github.com/wbrown/janus-datalog/datalog/qb"
    "github.com/wbrown/janus-datalog/datalog/storage"
)

// Define attributes once
var (
    PersonName = qb.Kw(":person/name")
    PersonAge  = qb.Kw(":person/age")
    PersonCity = qb.Kw(":person/city")
)

func main() {
    db, _ := storage.NewDatabase("example.db")
    defer db.Close()

    // Find adults in specific cities
    e := qb.NewVar("e")
    name := qb.NewVar("name")
    age := qb.NewVar("age")
    city := qb.NewVar("city")

    q := qb.Query().
        Find(name, age, city).
        In(qb.DB, qb.Collection(city)).
        Where(
            qb.Pat(e, PersonName, name),
            qb.Pat(e, PersonAge, age),
            qb.Pat(e, PersonCity, city),
            qb.Gte(age, 18),
        ).
        OrderBy(qb.Desc(age)).
        MustBuild()

    results, err := db.ExecuteQueryWithInputs(q, []string{"NYC", "LA"})
    if err != nil {
        panic(err)
    }

    for _, row := range results {
        fmt.Printf("%s (%d) - %s\n", row[0], row[1], row[2])
    }
}
```

## Quick Reference

### Core Types

| Type | Purpose | Example |
|------|---------|---------|
| `*Var` | Query variable | `e := qb.NewVar("e")` |
| `Attr` | Keyword attribute | `PersonName := qb.Kw(":person/name")` |
| `Val` | Constant value | `qb.V("NYC")`, `qb.V(42)` |

### Pattern Building

| Function | EDN Equivalent | Description |
|----------|----------------|-------------|
| `qb.Pat(e, a, v)` | `[?e ?a ?v]` | 3-element pattern |
| `qb.Pat(e, a, v, tx)` | `[?e ?a ?v ?tx]` | With transaction |
| `qb.Pat(e, a, v, tx, op)` | `[?e ?a ?v ?tx ?op]` | History pattern |
| `qb.Blank()` | `_` | Wildcard |

### Comparisons

| Function | EDN Equivalent | With Binding |
|----------|----------------|--------------|
| `qb.Lt(a, b)` | `[(< ?a ?b)]` | `qb.Lt(a, b).As(result)` |
| `qb.Lte(a, b)` | `[(<= ?a ?b)]` | `qb.Lte(a, b).As(result)` |
| `qb.Gt(a, b)` | `[(> ?a ?b)]` | `qb.Gt(a, b).As(result)` |
| `qb.Gte(a, b)` | `[(>= ?a ?b)]` | `qb.Gte(a, b).As(result)` |
| `qb.Eq(a, b)` | `[(= ?a ?b)]` | `qb.Eq(a, b).As(result)` |
| `qb.Ne(a, b)` | `[(!= ?a ?b)]` | `qb.Ne(a, b).As(result)` |
| `qb.Range(lo, x, hi)` | `[(< lo ?x hi)]` | `qb.Range(lo, x, hi).As(result)` |

### Aggregations

| Function | EDN Equivalent |
|----------|----------------|
| `qb.Sum(v)` | `(sum ?v)` |
| `qb.Count(v)` | `(count ?v)` |
| `qb.Avg(v)` | `(avg ?v)` |
| `qb.Min(v)` | `(min ?v)` |
| `qb.Max(v)` | `(max ?v)` |

### Expressions

| Function | EDN Equivalent |
|----------|----------------|
| `qb.Add(a, b).As(r)` | `[(+ ?a ?b) ?r]` |
| `qb.Sub(a, b).As(r)` | `[(- ?a ?b) ?r]` |
| `qb.Mul(a, b).As(r)` | `[(* ?a ?b) ?r]` |
| `qb.Div(a, b).As(r)` | `[(/ ?a ?b) ?r]` |
| `qb.Str(a, b, c).As(r)` | `[(str ?a ?b ?c) ?r]` |
| `qb.Ground(42).As(r)` | `[(ground 42) ?r]` |
| `qb.Year(t).As(y)` | `[(year ?t) ?y]` |

### Database Functions

| Function | EDN Equivalent | Description |
|----------|----------------|-------------|
| `qb.GetElse(e, attr, default).As(r)` | `[(get-else $ ?e :attr default) ?r]` | Default for missing |
| `qb.Missing(e, attr)` | `[(missing? $ ?e :attr)]` | Filter: attr missing |
| `qb.Missing(e, attr).As(r)` | `[(missing? $ ?e :attr) ?r]` | Bind: is attr missing? |
| `qb.GetSome(e, a1, a2, a3).As(r)` | `[(get-some $ ?e :a1 :a2 :a3) ?r]` | First available attr |

### Input Parameters

| Function | EDN Equivalent | Description |
|----------|----------------|-------------|
| `qb.DB` | `$` | Database source |
| `qb.Scalar(v)` | `?v` | Single value |
| `qb.Collection(v)` | `[?v ...]` | Multiple values |
| `qb.Tuple(a, b)` | `[?a ?b]` | Single tuple |
| `qb.Relation(a, b)` | `[[?a ?b] ...]` | Multiple tuples |

### Logical Clauses

| Function | EDN Equivalent |
|----------|----------------|
| `qb.Not(clauses...)` | `(not ...)` |
| `qb.NotJoin(vars, clauses...)` | `(not-join [vars] ...)` |
| `qb.Or().Branch(...).Branch(...)` | `(or ...)` |
| `qb.OrJoin(vars...).Branch(...).Branch(...)` | `(or-join [vars] ...)` |

### Ordering

| Function | EDN Equivalent |
|----------|----------------|
| `qb.Asc(v)` | `:asc` |
| `qb.Desc(v)` | `:desc` |
