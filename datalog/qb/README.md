# qb - Go-Native Query Builder

The `qb` package is the **recommended way** to write Datalog queries in Go. It provides compile-time safety, IDE support, and eliminates string manipulation errors.

## Why Use the Query Builder?

| | Query Builder | EDN Strings |
|---|---|---|
| **Typos** | Compile-time error | Silent empty results |
| **Refactoring** | Safe find-replace | Manual text search |
| **IDE Support** | Full autocomplete | None |
| **Type Safety** | Go compiler checks | Runtime parse errors |
| **Joins** | Variable identity | String matching |

**EDN strings are still supported** for REPL exploration, porting existing queries, or when you prefer the Datalog syntax directly.

## Quick Start

```go
import "github.com/wbrown/janus-datalog/datalog/qb"

// 1. Define your schema attributes as constants
var (
    PersonName = qb.Kw(":person/name")
    PersonAge  = qb.Kw(":person/age")
    PersonCity = qb.Kw(":person/city")
)

// 2. Build queries with Go variables
func FindAdultsInCity(db *storage.Database, city string) ([][]interface{}, error) {
    e := qb.NewVar()
    name := qb.NewVar()
    age := qb.NewVar()
    inputCity := qb.NewVar()

    q := qb.Query().
        Find(name, age).
        In(qb.DB, qb.Scalar(inputCity)).
        Where(
            qb.Pat(e, PersonName, name),
            qb.Pat(e, PersonAge, age),
            qb.Pat(e, PersonCity, inputCity),
            qb.Gte(age, 18),
        ).
        MustBuild()

    return db.ExecuteQueryWithInputs(q, city)
}
```

## The Key Insight

**Go variable identity IS the join condition.**

```go
e := qb.NewVar()    // This pointer...
name := qb.NewVar()
age := qb.NewVar()

qb.Pat(e, PersonName, name)  // ...used here
qb.Pat(e, PersonAge, age)    // ...and here = JOIN
```

Same `*Var` pointer in multiple patterns = join on that variable. No string matching, no typos possible.

## Comparison

### Query Builder (Recommended)

```go
var (
    PersonName = qb.Kw(":person/name")
    PersonAge  = qb.Kw(":person/age")
)

e := qb.NewVar()
name := qb.NewVar()
age := qb.NewVar()

q := qb.Query().
    Find(name, age).
    Where(
        qb.Pat(e, PersonName, name),
        qb.Pat(e, PersonAge, age),
        qb.Gt(age, 21),
    ).
    MustBuild()

results, err := db.ExecuteQuery(q)
```

### EDN String (Fallback)

```go
results, err := db.ExecuteQuery(`
    [:find ?name ?age
     :where
     [?e :person/name ?name]
     [?e :person/age ?age]
     [(> ?age 21)]]
`)
```

Both produce identical results. The query builder version catches errors at compile time.

## When to Use EDN Strings

- **REPL/exploration**: Quick ad-hoc queries during development
- **Migration**: Porting queries from Clojure/Datomic codebases
- **Dynamic queries**: Building query strings from user input (with proper escaping)
- **Documentation**: Showing equivalent Datalog syntax

## Full Documentation

See [docs/reference/QUERY_BUILDER.md](../../docs/reference/QUERY_BUILDER.md) for complete API documentation including:

- Patterns (any arity)
- Predicates and comparisons
- Aggregations
- Expressions (arithmetic, string, time)
- Input parameters (scalar, collection, tuple, relation)
- Logical clauses (NOT, OR)
- Subqueries
- Ordering
