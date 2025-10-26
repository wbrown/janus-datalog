# Feature Request: Storage Layer Support for Query Input Parameters

## Summary

Janus Datalog fully supports Datomic-style `:in` clauses at the executor level, but the `Storage` interface lacks a convenient API for passing input parameters to queries. This forces users to either:
1. Use string formatting (unsafe, inefficient)
2. Directly access the executor (bypasses storage abstraction)

This document proposes adding parameterized query support to the Storage interface.

## Current State

### ✅ What Works: Executor Level

Janus fully implements `:in` clause support with all Datomic input types:

```go
// From datalog/query/types.go
type Query struct {
    Find    []FindElement
    In      []InputSpec      // Full :in clause support
    Where   []Clause
    OrderBy []OrderByClause
}

// Supported input types:
// - DatabaseInput:    $
// - ScalarInput:      ?name
// - CollectionInput:  [?foods ...]
// - TupleInput:       [[?name ?age]]
// - RelationInput:    [[?name ?age] ...]
```

**Example from tests** (`datalog/parser/in_clause_test.go`):
```datalog
[:find ?e
 :in $ ?name ?min-age ?max-age
 :where [?e :person/name ?name]
        [?e :person/age ?age]
        [(>= ?age ?min-age)]
        [(<= ?age ?max-age)]]
```

**Execution API** (`datalog/executor/executor.go`):
```go
func (e *Executor) ExecuteWithRelations(
    ctx Context,
    q *query.Query,
    inputRelations []Relation,
) (Relation, error)
```

**Working example** (`datalog/storage/aevt_bug_test.go`):
```go
// Create input relation for ?e variable
inputRel := executor.NewMaterializedRelation(
    []query.Symbol{"?e"},
    []executor.Tuple{{entity1}, {entity2}, {entity3}},
)

// Execute with inputs
result, err := exec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
```

### ❌ What's Missing: Storage Layer

The `Storage` interface only provides string-based queries:

```go
// From types/types.go (or equivalent in gopher-street)
type Storage interface {
    ExecuteQuery(query string) ([][]interface{}, error)
    // ... other methods
}
```

**Current workaround** (unsafe, inefficient):
```go
// String formatting - vulnerable to injection, no query reuse
query := fmt.Sprintf(`[:find (max ?time)
 :where [?s :symbol/ticker "%s"] ...]`, userInput)

results, err := storage.ExecuteQuery(query)
```

## The Gap

Users cannot leverage `:in` clauses without:
1. Breaking the Storage abstraction
2. Directly accessing `executor.Executor`
3. Manually creating `executor.Relation` objects
4. Managing parser/planner lifecycle

This defeats the purpose of the Storage interface and forces unsafe string formatting.

## Proposed Solutions

### Option 1: Add Parameterized Query Method (Recommended)

Add a simple, type-safe API to the Storage interface:

```go
type Storage interface {
    ExecuteQuery(query string) ([][]interface{}, error)

    // NEW: Execute query with input parameters
    ExecuteQueryWithInputs(
        query string,
        inputs ...interface{},
    ) ([][]interface{}, error)
}
```

**Usage:**
```go
query := `[:find (max ?time)
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/time ?time]]`

results, err := storage.ExecuteQueryWithInputs(query, "CRWV")
```

**Multi-parameter example:**
```go
query := `[:find ?e
 :in $ ?name ?min-age ?max-age
 :where [?e :person/name ?name]
        [?e :person/age ?age]
        [(>= ?age ?min-age)]
        [(<= ?age ?max-age)]]`

results, err := storage.ExecuteQueryWithInputs(query, "Alice", 25, 65)
```

**Collection input example:**
```go
query := `[:find ?e ?food
 :in $ [?food ...]
 :where [?e :person/likes ?food]]`

results, err := storage.ExecuteQueryWithInputs(query, []string{"pizza", "pasta", "gelato"})
```

**Implementation sketch:**
```go
func (ds *DatalogStorage) ExecuteQueryWithInputs(
    queryStr string,
    inputs ...interface{},
) ([][]interface{}, error) {
    // Parse query
    q, err := parser.ParseQuery(queryStr)
    if err != nil {
        return nil, fmt.Errorf("failed to parse query: %w", err)
    }

    // Convert inputs to Relations based on :in clause types
    inputRelations := make([]executor.Relation, 0, len(inputs))
    inputIdx := 0

    for _, inputSpec := range q.In {
        switch spec := inputSpec.(type) {
        case query.DatabaseInput:
            // Skip $ - doesn't consume an input
            continue

        case query.ScalarInput:
            if inputIdx >= len(inputs) {
                return nil, fmt.Errorf("not enough inputs: expected input for %s", spec.Symbol)
            }

            // Create single-value relation
            rel := executor.NewMaterializedRelation(
                []query.Symbol{spec.Symbol},
                []executor.Tuple{{inputs[inputIdx]}},
            )
            inputRelations = append(inputRelations, rel)
            inputIdx++

        case query.CollectionInput:
            if inputIdx >= len(inputs) {
                return nil, fmt.Errorf("not enough inputs: expected collection for %s", spec.Symbol)
            }

            // Convert slice to relation
            slice := reflect.ValueOf(inputs[inputIdx])
            if slice.Kind() != reflect.Slice {
                return nil, fmt.Errorf("expected slice for collection input %s", spec.Symbol)
            }

            tuples := make([]executor.Tuple, slice.Len())
            for i := 0; i < slice.Len(); i++ {
                tuples[i] = executor.Tuple{slice.Index(i).Interface()}
            }

            rel := executor.NewMaterializedRelation(
                []query.Symbol{spec.Symbol},
                tuples,
            )
            inputRelations = append(inputRelations, rel)
            inputIdx++

        case query.TupleInput:
            // Similar handling for tuple input
            // ...

        case query.RelationInput:
            // Similar handling for relation input
            // ...
        }
    }

    // Execute with converted inputs
    result, err := ds.executor.ExecuteWithRelations(
        executor.NewContext(nil),
        q,
        inputRelations,
    )
    if err != nil {
        return nil, err
    }

    // Convert Relation back to [][]interface{}
    return relationToSlice(result), nil
}

func relationToSlice(rel executor.Relation) [][]interface{} {
    rows := make([][]interface{}, 0, rel.Size())
    it := rel.Iterator()
    for it.Next() {
        tuple := it.Tuple()
        row := make([]interface{}, len(tuple))
        for i, v := range tuple {
            row[i] = v
        }
        rows = append(rows, row)
    }
    return rows
}
```

### Option 2: Expose Executor (More Flexible)

Add method to get the underlying executor:

```go
type Storage interface {
    ExecuteQuery(query string) ([][]interface{}, error)

    // NEW: Get underlying executor for advanced usage
    GetExecutor() (*executor.Executor, error)
}
```

**Pros:**
- Maximum flexibility
- Users can use full executor API
- No need to maintain parallel APIs

**Cons:**
- Breaks abstraction
- Exposes implementation details
- Users need to understand executor lifecycle

### Option 3: Both

Provide convenience method AND escape hatch:

```go
type Storage interface {
    ExecuteQuery(query string) ([][]interface{}, error)
    ExecuteQueryWithInputs(query string, inputs ...interface{}) ([][]interface{}, error)
    GetExecutor() (*executor.Executor, error)
}
```

## Benefits

1. **Safety**: No string formatting, no injection risks
2. **Performance**: Query parsing/planning can be cached and reused
3. **Ergonomics**: Simple API matches Datomic/Datalog conventions
4. **Correctness**: Type checking at the storage boundary
5. **Consistency**: Same API across all input types (scalar, collection, tuple, relation)

## Use Cases

### Financial Data Queries (gopher-street example)

**Before** (string formatting):
```go
query := fmt.Sprintf(`[:find (max ?time)
 :where [?s :symbol/ticker "%s"]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day ?mod]
        [(>= ?mod 570)]
        [(<= ?mod 960)]]`, ctx.Symbol)  // UNSAFE

maxResults, err := ctx.Storage.ExecuteQuery(query)
```

**After** (parameterized):
```go
query := `[:find (max ?time)
 :in $ ?symbol
 :where [?s :symbol/ticker ?symbol]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [?bar :price/minute-of-day ?mod]
        [(>= ?mod 570)]
        [(<= ?mod 960)]]`

maxResults, err := ctx.Storage.ExecuteQueryWithInputs(query, ctx.Symbol)  // SAFE
```

### Time Range Queries

```go
query := `[:find ?time ?open ?high ?low ?close ?volume
 :in $ ?symbol ?start-date ?end-date
 :where [?s :symbol/ticker ?symbol]
        [?bar :price/symbol ?s]
        [?bar :price/time ?time]
        [(>= ?time ?start-date)]
        [(<= ?time ?end-date)]
        [?bar :price/open ?open]
        [?bar :price/high ?high]
        [?bar :price/low ?low]
        [?bar :price/close ?close]
        [?bar :price/volume ?volume]]`

startDate := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
endDate := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)

results, err := storage.ExecuteQueryWithInputs(query, "CRWV", startDate, endDate)
```

### Multi-Symbol Queries

```go
query := `[:find ?symbol (max ?price)
 :in $ [?symbol ...]
 :where [?s :symbol/ticker ?symbol]
        [?p :price/symbol ?s]
        [?p :price/close ?price]]`

symbols := []string{"CRWV", "NVDA", "AAPL", "MSFT"}
results, err := storage.ExecuteQueryWithInputs(query, symbols)
```

## Backward Compatibility

Adding new methods is backward compatible. Existing code using `ExecuteQuery` continues to work unchanged.

## Implementation Considerations

1. **Type conversion**: Need to handle Go types → Datalog values
   - Primitives: string, int, float64, bool
   - Time: time.Time
   - Slices: []interface{} for collections
   - Nested: [][]interface{} for relations

2. **Error handling**: Clear errors when:
   - Input count doesn't match `:in` clause
   - Input types don't match expected types
   - Conversion fails

3. **Query caching**: Parsed queries could be cached by query string

4. **Context support**: Consider `ExecuteQueryWithInputsContext` for cancellation

## Related Code

- Parser: `datalog/parser/in_clause_test.go` - Full test coverage of `:in` parsing
- Executor: `datalog/executor/executor.go` - `ExecuteWithRelations` implementation
- Types: `datalog/query/types.go` - Input type definitions
- Tests: `datalog/storage/aevt_bug_test.go` - Working example of ExecuteWithRelations

## Questions for Maintainers

1. Which option do you prefer (parameterized method, executor access, or both)?
2. Should we support query caching/preparation (like SQL prepared statements)?
3. Should there be a `Context`-aware variant for cancellation?
4. Any concerns about type conversion edge cases?

## References

- [Datomic Query Reference](https://docs.datomic.com/query/query-data-reference.html#inputs) - Original `:in` clause specification
- Janus Datalog test suite demonstrates full `:in` support already exists
- This request is just about exposing that capability at the Storage layer
