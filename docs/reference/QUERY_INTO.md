# QueryInto API

This document describes janus-datalog's QueryInto API for mapping Datalog query results directly into Go types.

## Overview

The QueryInto API eliminates manual tuple iteration by populating typed Go values from query results:

- **`QueryInto()`** - Query into a slice of structs or scalars
- **`QueryOneInto()`** - Query into a single struct or scalar (expects at most one result)

Both APIs support:
- **Structs** for multi-column queries (map columns to fields via tags)
- **Scalars** for single-column queries (`string`, `int64`, `float64`, `bool`, `time.Time`, `Identity`, `Keyword`, `[]byte`)

## Basic Usage

### Define Result Struct

Use the `datalog` tag to map struct fields to query columns:

```go
type PersonResult struct {
    Name string `datalog:"?name"`
    Age  int64  `datalog:"?age"`
}
```

### Query Into Slice

```go
var results []PersonResult
err := db.QueryInto(&results, `
    [:find ?name ?age
     :where [?e :person/name ?name]
            [?e :person/age ?age]]
`)

for _, r := range results {
    fmt.Printf("%s is %d years old\n", r.Name, r.Age)
}
```

### Query Into Single Result

```go
var result PersonResult
found, err := db.QueryOneInto(&result, `
    [:find ?name ?age
     :where [?e :person/name ?name]
            [?e :person/age ?age]
            [(= ?name "Alice")]]
`)
if err != nil {
    return err
}
if !found {
    fmt.Println("No matching person")  // Not an error - valid empty result
}
```

### Scalar Queries (Single Column)

For single-column queries, use scalar slices or values directly without structs:

```go
// Get all names as []string
var names []string
err := db.QueryInto(&names, `
    [:find ?name
     :where [?e :person/name ?name]]
`)

// Get all entity IDs as []datalog.Identity
var entities []datalog.Identity
err := db.QueryInto(&entities, `
    [:find ?e
     :where [?e :person/name ?name]]
`)

// Get a single value
var age int64
found, err := db.QueryOneInto(&age, `
    [:find ?age
     :where [?e :person/name "Alice"]
            [?e :person/age ?age]]
`)
```

Supported scalar types: `string`, `int64`, `int`, `float64`, `bool`, `time.Time`, `datalog.Identity`, `datalog.Keyword`, `[]byte`

**Note:** Scalar queries require exactly one column in the `:find` clause. Multi-column queries require a struct.

## Tag Mapping

### Variable Tags

Tags match the query's `:find` clause exactly:

```go
type Result struct {
    Symbol string  `datalog:"?symbol"`  // matches :find ?symbol
    Price  float64 `datalog:"?price"`   // matches :find ?price
}

// Query
[:find ?symbol ?price :where ...]
```

### Aggregate Tags

For aggregates, the tag must match the full aggregate expression:

```go
type DeptStats struct {
    Dept      string  `datalog:"?dept"`          // matches :find ?dept
    TotalPay  float64 `datalog:"(sum ?salary)"`  // matches :find (sum ?salary)
    HeadCount int64   `datalog:"(count ?emp)"`   // matches :find (count ?emp)
    MaxSalary float64 `datalog:"(max ?salary)"`  // matches :find (max ?salary)
}

// Query
[:find ?dept (sum ?salary) (count ?emp) (max ?salary)
 :where [?e :employee/dept ?dept]
        [?e :employee/salary ?salary]]
```

### Positional Mapping

If no fields have `datalog` tags, positional mapping is used:

```go
type PositionalResult struct {
    Name string  // maps to column 0
    Age  int64   // maps to column 1
}

// Query: [:find ?name ?age :where ...]
// Column 0 (?name) -> Name, Column 1 (?age) -> Age
```

**Note:** Mixing tagged and untagged fields in the same struct is an error.

## Query Inputs

Use `:in` clause with additional arguments:

```go
var results []PersonResult
err := db.QueryInto(&results, `
    [:find ?name ?age
     :in $ ?min-age
     :where [?e :person/name ?name]
            [?e :person/age ?age]
            [(>= ?age ?min-age)]]
`, int64(21))  // Bind ?min-age to 21
```

## Type Coercion

QueryInto automatically converts between compatible types:

| Query Result | Struct Field | Conversion |
|--------------|--------------|------------|
| `int64` | `int`, `int32`, `int64` | Numeric conversion |
| `int64` | `uint64` | Numeric conversion |
| `float64` | `float32`, `float64` | Numeric conversion |
| `string` | `string` | Direct |
| `bool` | `bool` | Direct |
| `time.Time` | `time.Time` | Direct |
| `[]byte` | `[]byte` | Direct |
| `Identity` | `Identity` | Direct |
| `Identity` | `*Identity` | Pointer set |
| `Keyword` | `Keyword` | Direct |
| `nil` | `*T` (pointer) | Leave nil |
| `nil` | `T` (non-pointer) | Error |

## Pointer Fields

Use pointer fields for optional values:

```go
type WithOptional struct {
    Name  string  `datalog:"?name"`
    Email *string `datalog:"?email"`  // nil if not in query result
}
```

## Error Types

```go
import dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"

// ErrMultipleResults - QueryOneInto returned more than one result
if errors.Is(err, dlreflect.ErrMultipleResults) {
    // Handle unexpected multiple results
}

// ErrSymbolNotFound - Struct tag references symbol not in query
if errors.Is(err, dlreflect.ErrSymbolNotFound) {
    // Check struct tags match query :find clause
}
```

**Note:** `QueryOneInto` no longer returns `ErrNotFound`. Empty results are indicated by `found=false` with `err=nil`, because an empty result is a valid relational answer, not an error condition.

## Aggregates on Empty Result Sets

**Important:** Following Datomic semantics, aggregate functions over empty result sets return an empty result, not a row with default values.

```go
// If no entities match the where clause, found=false (not an error)
var result struct {
    Count int64 `datalog:"(count ?e)"`
}
found, err := db.QueryOneInto(&result, `
    [:find (count ?e)
     :where [?e :person/name "NonExistent"]]
`)
// found == false, err == nil (not result.Count == 0)
```

This differs from SQL where `SELECT COUNT(*) FROM t WHERE false` returns `[(0)]`.

### Why This Behavior?

In Datomic (and relational algebra), aggregates operate on the set of tuples matched by the `:where` clause. If the `:where` clause matches nothing, there is no set to aggregate over, so the aggregate function never executes. An empty result is a valid answer, not an error.

### Recommended Pattern

Handle the `found=false` case explicitly:

```go
type CountResult struct {
    Count int64 `datalog:"(count ?e)"`
}

var result CountResult
found, err := db.QueryOneInto(&result, `
    [:find (count ?e)
     :where [?e :user/status "active"]]
`)
if err != nil {
    return err
}

count := int64(0)
if found {
    count = result.Count
}
```

Or as a helper:

```go
func countActiveUsers(db *Database) (int64, error) {
    var result struct {
        Count int64 `datalog:"(count ?e)"`
    }
    found, err := db.QueryOneInto(&result, `
        [:find (count ?e)
         :where [?e :user/status "active"]]
    `)
    if err != nil {
        return 0, err
    }
    if !found {
        return 0, nil
    }
    return result.Count, nil
}
```

## Complete Example

```go
package main

import (
    "errors"
    "fmt"
    "os"

    "github.com/wbrown/janus-datalog/datalog"
    dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
    "github.com/wbrown/janus-datalog/datalog/storage"
)

type EmployeeStats struct {
    Dept      string  `datalog:"?dept"`
    AvgSalary float64 `datalog:"(avg ?salary)"`
    Count     int64   `datalog:"(count ?emp)"`
}

func main() {
    tmpDir, _ := os.MkdirTemp("", "example")
    defer os.RemoveAll(tmpDir)

    db, _ := storage.NewDatabase(tmpDir)
    defer db.Close()

    // Add test data
    tx := db.NewTransaction()
    for _, emp := range []struct {
        name   string
        dept   string
        salary float64
    }{
        {"Alice", "Engineering", 100000},
        {"Bob", "Engineering", 120000},
        {"Charlie", "Sales", 80000},
    } {
        id := datalog.NewIdentity("emp:" + emp.name)
        tx.Add(id, datalog.NewKeyword(":employee/name"), emp.name)
        tx.Add(id, datalog.NewKeyword(":employee/dept"), emp.dept)
        tx.Add(id, datalog.NewKeyword(":employee/salary"), emp.salary)
    }
    tx.Commit()

    // Query with aggregates into typed slice
    var stats []EmployeeStats
    err := db.QueryInto(&stats, `
        [:find ?dept (avg ?salary) (count ?emp)
         :where [?emp :employee/dept ?dept]
                [?emp :employee/salary ?salary]]
    `)
    if err != nil {
        panic(err)
    }

    for _, s := range stats {
        fmt.Printf("%s: %.0f avg salary, %d employees\n",
            s.Dept, s.AvgSalary, s.Count)
    }
}
```

Output:
```
Engineering: 110000 avg salary, 2 employees
Sales: 80000 avg salary, 1 employees
```

## Comparison with Manual Iteration

**Before (manual):**
```go
results, err := db.ExecuteQuery(`[:find ?name ?age :where ...]`)
if err != nil {
    return err
}

type Person struct {
    Name string
    Age  int64
}

people := make([]Person, 0, len(results))
for _, tuple := range results {
    name, ok := tuple[0].(string)
    if !ok {
        return fmt.Errorf("expected string for name")
    }
    age, ok := tuple[1].(int64)
    if !ok {
        return fmt.Errorf("expected int64 for age")
    }
    people = append(people, Person{Name: name, Age: age})
}
```

**After (QueryInto):**
```go
type Person struct {
    Name string `datalog:"?name"`
    Age  int64  `datalog:"?age"`
}

var people []Person
err := db.QueryInto(&people, `[:find ?name ?age :where ...]`)
```

## Performance

- **Reflection caching**: QueryResultMapper caches field mappings per struct type
- **Single pass**: Results are mapped in a single iteration
- **No intermediate allocation**: Results populate the destination slice directly
- **Type coercion**: Handled at map time with minimal overhead

## Relationship to Other APIs

| API | Purpose | Use Case |
|-----|---------|----------|
| `QueryInto` | Query results → struct slice | Typed query results |
| `QueryOneInto` | Query results → single struct | Single-row queries |
| `PullInto` | Entity → struct | Load entity by ID |
| `ExecuteQuery` | Query → `[][]interface{}` | Dynamic/untyped results |

## Package Reference

```go
import "github.com/wbrown/janus-datalog/datalog/storage"
import dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"

// Database methods
db.QueryInto(dest interface{}, queryStr string, inputs ...interface{}) error
db.QueryOneInto(dest interface{}, queryStr string, inputs ...interface{}) (found bool, err error)

// Error types
dlreflect.ErrMultipleResults
dlreflect.ErrSymbolNotFound
dlreflect.ErrMixedTags

// Low-level API (for advanced use)
dlreflect.NewQueryResultMapper(elemType reflect.Type, findColumns []string) (*QueryResultMapper, error)
mapper.MapTuple(tuple []interface{}, dest reflect.Value) error
mapper.MapAll(tuples [][]interface{}, destSlice reflect.Value) error
```
