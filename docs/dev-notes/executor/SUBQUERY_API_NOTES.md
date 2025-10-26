# Subquery API and Data Flow Documentation

## Overview
Subqueries in this Datalog implementation allow nested queries that can reference variables from the outer query. The implementation follows Datomic's subquery pattern.

## Query Syntax
```clojure
[(q [:find ?result
     :in $ ?input-var
     :where ...]
    ?outer-var) [[?binding]]]
```

- `q` - subquery operator
- First argument: the nested query
- Following arguments: variables/values to pass from outer query
- Binding form: how to bind results (e.g., `[[?binding]]` for single value)

## Key Data Structures

### SubqueryPlan (planner/types.go)
```go
type SubqueryPlan struct {
    Subquery    *query.SubqueryPattern  // The parsed subquery pattern
    Inputs      []query.Symbol          // Outer variables needed (e.g., [?s])
    NestedPlan  *QueryPlan              // Pre-planned nested query
}
```

### SubqueryPattern (query/types.go)
```go
type SubqueryPattern struct {
    Query   *Query           // The nested query
    Inputs  []PatternElement // Variables/constants passed to subquery
    Binding BindingForm      // How to bind results
}
```

### Binding Forms
- `TupleBinding`: `[[?var ...]]` - Expects single result row
- `RelationBinding`: `[[?a ?b] ...]` - Multiple result rows
- `CollectionBinding`: `?coll` - Collect all values (not implemented)

## Data Flow

### 1. Planning Phase (planner)
- Subquery is parsed as a `SubqueryPattern`
- Planner identifies which outer variables are needed (`Inputs`)
- Nested query is pre-planned into `NestedPlan`
- Subquery execution is scheduled in appropriate phase

### 2. Execution Phase (executor)

#### Step 1: Get Unique Input Combinations
```go
inputCombinations := e.getUniqueInputCombinations(inputRelation, subqPlan.Inputs)
```
- Extract unique combinations of outer variables (e.g., distinct ?s values)
- Avoids duplicate subquery executions
- Returns: `[]map[query.Symbol]interface{}` (e.g., `[{?s: "symbol:aapl"}, {?s: "symbol:goog"}]`)

#### Step 2: For Each Input Combination

##### 2a. Create Input Relations
```go
inputRelations := e.createInputRelations(subqPlan.Subquery.Query, inputValues, subqPlan.Inputs)
```
- Maps outer variables to inner `:in` clause variables **by position**
- Creates Relations for each `:in` specification:
  - `ScalarInput`: Single value relation
  - `TupleInput`: Multi-column single row
  - `RelationInput`: Multi-column multi-row (treated as single row currently)

**Critical Mapping**: The subquery pattern `(q [...] ?s)` passes `?s` which maps to the second `:in` element (after `$`).

Example:
- Outer: `?s = "symbol:aapl"`
- Inner `:in $ ?sym` 
- Creates: Relation with column `?sym` and value `"symbol:aapl"`

##### 2b. Execute Nested Query
```go
result, err := e.executePhasesWithInputs(ctx, subqPlan.NestedPlan, inputRelations)
```
- Runs the pre-planned nested query with input relations
- Input relations provide initial bindings for the query

##### 2c. Apply Binding Form
```go
boundResult, err := e.applyBindingForm(result, binding, inputValues, subqPlan.Inputs)
```
- Transforms subquery results according to binding form
- Adds input values as columns (for correlation)
- TupleBinding: Expects exactly 1 result row
- RelationBinding: Accepts multiple rows

#### Step 3: Combine Results
- Union all results from different input combinations
- Returns final relation with columns: `[input-vars... binding-vars...]`

## Example Walkthrough

Query:
```clojure
[:find ?symbol ?max-price
 :where 
 [?s :symbol/ticker ?symbol]
 [(q [:find (max ?price)
      :in $ ?sym
      :where [?p :price/symbol ?sym]
             [?p :price/value ?price]]
     ?s) [[?max-price]]]]
```

1. **Outer query** matches symbols:
   - `?s = symbol:aapl, ?symbol = "AAPL"`
   - `?s = symbol:goog, ?symbol = "GOOG"`

2. **For each symbol**, subquery executes:
   - Input: `?sym = symbol:aapl`
   - Finds all prices for that symbol
   - Aggregates with `max`
   - Returns single value (e.g., `155.0`)

3. **Binding** creates result:
   - TupleBinding `[[?max-price]]` binds single result
   - Final tuple: `[?s, ?symbol, ?max-price]`

4. **Union** combines all results:
   - `[symbol:aapl, "AAPL", 155.0]`
   - `[symbol:goog, "GOOG", 2800.0]`

## Key Implementation Details

### Position-Based Mapping
The mapping between outer and inner variables is **positional**, not name-based:
1. Subquery inputs after the query (e.g., `?s`) are ordered
2. `:in` clause variables (excluding `$`) are ordered
3. Mapping is by position: first input → first `:in` var, etc.

### Context Wrapping
`subqueryContext` wraps the parent context to provide input bindings. Currently delegates most operations to parent.

### Input Relations vs Input Values
- `inputRelations`: Relations passed to `executePhasesWithInputs`
- `inputValues`: Map of symbol→value for binding form application
- Both derived from same outer query variables but used differently

## Common Pitfalls

1. **Variable Name Confusion**: Outer `?s` maps to inner `?sym` by position, not name
2. **Binding Form Mismatch**: TupleBinding expects exactly 1 result
3. **Missing Correlation**: Forgetting to include input vars in final result
4. **Database Input**: `$` in `:in` clause doesn't consume an input value

## Testing Considerations

Tests should cover:
1. Single vs multiple input values
2. Empty subquery results
3. Different binding forms (tuple, relation)
4. Aggregations in subqueries
5. Multiple inputs to subquery
6. Nested subqueries
7. Error cases (no results with tuple binding, etc.)