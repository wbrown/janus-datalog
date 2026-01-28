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

// Named source - additional database or PatternMatcher
qb.Source("$users")

// Source-qualified pattern
qb.PatFrom(source, e, attr, val)
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

### Source Input Example

Named sources let you query across multiple data sources. Use `qb.Source()` to declare a source and `qb.PatFrom()` to create source-qualified patterns:

```go
users := qb.Source("$users")
perms := qb.Source("$perms")
e := qb.NewVar("e")
name := qb.NewVar("name")
uid := qb.NewVar("uid")
p := qb.NewVar("p")
role := qb.NewVar("role")

q := qb.Query().
    Find(name, role).
    In(users, perms).
    Where(
        qb.PatFrom(users, e, qb.Kw(":user/name"), name),
        qb.PatFrom(users, e, qb.Kw(":user/id"), uid),
        qb.PatFrom(perms, p, qb.Kw(":perm/user-id"), uid),
        qb.PatFrom(perms, p, qb.Kw(":perm/role"), role),
    ).
    MustBuild()

// Execute with named sources
results, err := db.ExecuteQueryWithInputs(q,
    storage.WithSources(map[query.Symbol]executor.PatternMatcher{
        query.Symbol("$users"): usersDB,
        query.Symbol("$perms"): permsDB,
    }),
)
```

To mix the default database with named sources, include `qb.DB`:

```go
cache := qb.Source("$cache")

q := qb.Query().
    Find(name, score).
    In(qb.DB, cache).
    Where(
        qb.Pat(e, qb.Kw(":user/name"), name),       // default database
        qb.PatFrom(cache, e, qb.Kw(":score"), score), // named source
    ).
    MustBuild()
```

See [MULTI_SOURCE.md](MULTI_SOURCE.md) for the complete multi-source reference.

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

### Variable Scoping in Subqueries

Datalog subqueries have **lexical scoping** - variables inside a subquery are independent of variables in other subqueries, even if they have the same name. This means you can reuse natural variable names like `?t` and `?s` across subqueries:

```go
// Good: Reuse Datalog variable names, reassign Go variables between subqueries
t, s := qb.NewVar("t"), qb.NewVar("s")
tok, dur := qb.NewVar("tok"), qb.NewVar("dur")

taskStatsQuery := qb.Query().
    Find(qb.Count(t), qb.Sum(tok), qb.Sum(dur)).
    In(qb.DB, qb.Scalar(s)).
    Where(
        qb.Pat(t, TaskScenario, s),
        qb.Pat(t, TaskTokens, tok),
        qb.Pat(t, TaskDuration, dur),
    )

// Reassign Go variables for second subquery - Datalog names stay the same
t, s = qb.NewVar("t"), qb.NewVar("s")

openingCountQuery := qb.Query().
    Find(qb.Count(t)).
    In(qb.DB, qb.Scalar(s)).
    Where(
        qb.Pat(t, TaskScenario, s),
        qb.Pat(t, TaskKey, KeyOpening),
    )
```

Both subqueries generate `?t` and `?s` in their EDN. Datalog's lexical scoping keeps them separate.

**Avoid** creating artificial unique names - this is unnecessary and clutters code:

```go
// Bad: Unnecessary unique variable names
t1, s1 := qb.NewVar("t1"), qb.NewVar("s1")
t2, s2 := qb.NewVar("t2"), qb.NewVar("s2")
t3, s3 := qb.NewVar("t3"), qb.NewVar("s3")
```

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

### QueryFor[T] - Type-Safe Variables

The manual approach above has a subtle problem: variable names in `qb.NewVar()` must exactly match the `datalog` struct tags, but nothing enforces this at compile time. Typos cause silent failures.

**QueryFor[T]** solves this by deriving variables directly from struct tags:

```go
// Define result struct once - tags drive BOTH query building AND result mapping
type PersonResult struct {
    Name string `datalog:"?name"`
    Age  int64  `datalog:"?age"`
}

func FindAdults(db *storage.Database) ([]PersonResult, error) {
    // QueryFor derives variables from struct tags
    q := qb.QueryFor[PersonResult]()
    f := &q.F
    e := qb.NewVar("e")

    query := q.Where(
        qb.Pat(e, PersonName, q.Find(&f.Name)),  // &f.Name -> ?name
        qb.Pat(e, PersonAge, q.Find(&f.Age)),    // &f.Age -> ?age
        qb.Gt(q.V(&f.Age), 18),                  // V() references without adding to Find
    ).MustBuild()

    // Results map directly to struct - tags guaranteed to match
    var results []PersonResult
    err := db.QueryInto(&results, query)
    return results, err
}
```

**Key methods:**
- `q.Find(&f.Field)` - Returns `*Var` AND adds to Find clause
- `q.V(&f.Field)` - Returns `*Var` without adding to Find (for predicates, join patterns)

**Why this is safer:**
- Rename struct field → compile error forces you to update all usages
- Typo in `datalog` tag → caught when QueryInto tests fail
- No string duplication between query and result mapping

**With aggregations:**

```go
type DeptStats struct {
    Dept      string  `datalog:"?dept"`
    Salary    int64   `datalog:"?salary"`  // base variable
    AvgSalary float64 // no tag - positional mapping from Find order
    Count     int64   // no tag - positional mapping from Find order
    Emp       int64   `datalog:"?emp"`     // base variable
}

func GetDeptStats(db *storage.Database) ([]DeptStats, error) {
    q := qb.QueryFor[DeptStats]()
    f := &q.F
    e := qb.NewVar("e")

    // V() gives the variable, you wrap it in aggregation
    query := qb.Query().
        Find(q.V(&f.Dept), qb.Avg(q.V(&f.Salary)), qb.Count(q.V(&f.Emp))).
        Where(
            qb.Pat(e, EmpDept, q.V(&f.Dept)),
            qb.Pat(e, EmpSalary, q.V(&f.Salary)),
            qb.Pat(e, EmpID, q.V(&f.Emp)),
        ).
        MustBuild()

    var stats []DeptStats
    err := db.QueryInto(&stats, query)
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
| `*TypedQueryBuilder[T]` | Type-safe builder | `q := qb.QueryFor[PersonResult]()` |

### Pattern Building

| Function | EDN Equivalent | Description |
|----------|----------------|-------------|
| `qb.Pat(e, a, v)` | `[?e ?a ?v]` | 3-element pattern |
| `qb.Pat(e, a, v, tx)` | `[?e ?a ?v ?tx]` | With transaction |
| `qb.Pat(e, a, v, tx, op)` | `[?e ?a ?v ?tx ?op]` | History pattern |
| `qb.PatFrom(src, e, a, v)` | `[$src ?e ?a ?v]` | Source-qualified pattern |
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
| `qb.DB` | `$` | Default database source |
| `qb.Source("$name")` | `$name` | Named source |
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
