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
    e := qb.NewVar()
    name := qb.NewVar()
    age := qb.NewVar()

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
e := qb.NewVar()
name := qb.NewVar()
age := qb.NewVar()

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
total := qb.NewVar()
qb.Add(price, tax).As(total)
qb.Sub(gross, deductions).As(net)
qb.Mul(quantity, unitPrice).As(lineTotal)
qb.Div(total, count).As(average)
```

### String Concatenation

```go
fullName := qb.NewVar()
qb.Str(firstName, " ", lastName).As(fullName)
```

### Ground Values

```go
constant := qb.NewVar()
qb.Ground(42).As(constant)
```

### Time Extraction

```go
y := qb.NewVar()
qb.Year(timestamp).As(y)
qb.Month(timestamp).As(m)
qb.Day(timestamp).As(d)
qb.Hour(timestamp).As(h)
qb.Minute(timestamp).As(min)
qb.Second(timestamp).As(sec)
```

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
inputName := qb.NewVar()
age := qb.NewVar()

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
inputName := qb.NewVar()
age := qb.NewVar()

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
inputName := qb.NewVar()
inputCity := qb.NewVar()
age := qb.NewVar()

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
qb.Or(
    []interface{}{qb.Pat(e, PersonCity, qb.V("NYC"))},
    []interface{}{qb.Pat(e, PersonCity, qb.V("LA"))},
)

// OR with join variables
qb.OrJoin([]*qb.Var{e, city},
    []interface{}{qb.Pat(e, PersonCity, city), qb.Eq(city, "NYC")},
    []interface{}{qb.Pat(e, PersonCity, city), qb.Eq(city, "LA")},
)
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
maxSalary := qb.NewVar()
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
    e := qb.NewVar()
    name := qb.NewVar()
    age := qb.NewVar()

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
    e := qb.NewVar()
    name := qb.NewVar()
    age := qb.NewVar()

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
    emp := qb.NewVar()
    dept := qb.NewVar()
    salary := qb.NewVar()

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
    e := qb.NewVar()
    name := qb.NewVar()
    age := qb.NewVar()
    city := qb.NewVar()

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
