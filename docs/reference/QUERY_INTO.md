# QueryInto API

This document describes janus-datalog's QueryInto API for mapping Datalog query results directly into Go structs.

## Overview

The QueryInto API eliminates manual tuple iteration by populating typed Go structs from query results:

- **`QueryInto()`** - Query into a slice of structs
- **`QueryOneInto()`** - Query into a single struct (expects exactly one result)

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
err := db.QueryOneInto(&result, `
    [:find ?name ?age
     :where [?e :person/name ?name]
            [?e :person/age ?age]
            [(= ?name "Alice")]]
`)

if errors.Is(err, dlreflect.ErrNotFound) {
    fmt.Println("No matching person")
} else if err != nil {
    return err
}
```

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

// ErrNotFound - QueryOneInto returned no results
if errors.Is(err, dlreflect.ErrNotFound) {
    // Handle missing result
}

// ErrMultipleResults - QueryOneInto returned more than one result
if errors.Is(err, dlreflect.ErrMultipleResults) {
    // Handle unexpected multiple results
}

// ErrSymbolNotFound - Struct tag references symbol not in query
if errors.Is(err, dlreflect.ErrSymbolNotFound) {
    // Check struct tags match query :find clause
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
db.QueryOneInto(dest interface{}, queryStr string, inputs ...interface{}) error

// Error types
dlreflect.ErrNotFound
dlreflect.ErrMultipleResults
dlreflect.ErrSymbolNotFound
dlreflect.ErrMixedTags

// Low-level API (for advanced use)
dlreflect.NewQueryResultMapper(elemType reflect.Type, findColumns []string) (*QueryResultMapper, error)
mapper.MapTuple(tuple []interface{}, dest reflect.Value) error
mapper.MapAll(tuples [][]interface{}, destSlice reflect.Value) error
```
