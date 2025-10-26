# Subquery Implementation Plan for Janus Datalog

## Overview

This plan outlines the implementation of subqueries in Janus Datalog to solve the aggregation scoping problem identified in the OHLC bug report. Subqueries will allow queries to compute aggregates over specific scopes without creating Cartesian products.

## Motivation

The current aggregation model (following Datomic) has limitations when mixing aggregated and non-aggregated values from different scopes. For example, computing daily high/low while also fetching specific open/close values creates unwanted Cartesian products. Subqueries provide a clean solution that feels natural to Datomic users.

## Syntax Design

### Basic Subquery Pattern

```clojure
[(q <query-form> <input1> <input2> ...) <binding-form>]
```

Where:
- `q` is the subquery function
- `<query-form>` is a complete Datalog query map
- `<input1>, <input2>, ...` are variables from the outer query passed as inputs
- `<binding-form>` describes how to bind the results

### Examples

```clojure
;; Simple aggregation subquery
[(q [:find (max ?price)
     :in $ ?sym
     :where [?p :price/symbol ?sym]
            [?p :price/value ?price]]
    ?symbol) [[?max-price]]]

;; Multiple return values
[(q [:find ?open ?close
     :in $ ?sym ?date
     :where [?p-open :price/symbol ?sym]
            [?p-open :price/minute-of-day 570]
            [?p-open :price/time ?t-open]
            [(same-date? ?t-open ?date)]
            [?p-open :price/open ?open]
            
            [?p-close :price/symbol ?sym]
            [?p-close :price/minute-of-day 960]
            [?p-close :price/time ?t-close]
            [(same-date? ?t-close ?date)]
            [?p-close :price/close ?close]]
    ?s ?d) [[?o ?c]]]

;; Subquery returning multiple rows (collection)
[(q [:find ?price ?time
     :in $ ?sym
     :where [?p :price/symbol ?sym]
            [?p :price/value ?price]
            [?p :price/time ?time]]
    ?symbol) ?price-collection]
```

## Implementation Steps

### 1. Query Parser Updates

**File**: `datalog/parser/query_parser.go`

Add support for recognizing subquery patterns:
```go
// In ParsePattern
case *edn.List:
    if list.Len() >= 2 {
        if sym, ok := list.Get(0).(*edn.Symbol); ok && sym.Name == "q" {
            return parseSubqueryPattern(list)
        }
    }
    // ... existing function pattern parsing
```

New function:
```go
func parseSubqueryPattern(list *edn.List) (*SubqueryPattern, error) {
    // Extract:
    // - Query map (position 1)
    // - Input arguments (positions 2 to n-1)
    // - Binding form (position n)
}
```

### 2. Query Types Extension

**File**: `datalog/query/types.go`

```go
// SubqueryPattern represents a nested query
type SubqueryPattern struct {
    Query   *Query          // The nested query
    Inputs  []PatternElement // Variables/constants to pass as inputs
    Binding BindingForm     // How to bind results
}

// BindingForm describes how subquery results are bound
type BindingForm interface {
    isBindingForm()
}

// TupleBinding binds a single row: [[?a ?b]]
type TupleBinding struct {
    Variables []Variable
}

// CollectionBinding binds all rows: ?coll
type CollectionBinding struct {
    Variable Variable
}

// RelationBinding binds as relation: [[?a ?b] ...]
type RelationBinding struct {
    Variables []Variable
}
```

### 3. Planner Updates

**File**: `datalog/planner/planner.go`

The planner needs to handle subqueries as special patterns:

```go
func (p *Planner) planPattern(pattern query.Pattern) (PatternPlan, error) {
    switch pat := pattern.(type) {
    case *query.SubqueryPattern:
        return p.planSubquery(pat)
    // ... existing cases
    }
}

func (p *Planner) planSubquery(sq *query.SubqueryPattern) (SubqueryPlan, error) {
    // 1. Recursively plan the nested query
    nestedPlan, err := p.Plan(sq.Query)
    
    // 2. Determine which phase this subquery belongs to
    // (based on which variables it needs as inputs)
    
    // 3. Return SubqueryPlan with:
    //    - Nested plan
    //    - Input mappings
    //    - Output bindings
}
```

### 4. Executor Updates

**File**: `datalog/executor/executor.go`

Add subquery execution to pattern execution:

```go
func (e *Executor) executePattern(ctx Context, pattern PatternPlan) (Relation, error) {
    switch p := pattern.Pattern.(type) {
    case *query.SubqueryPattern:
        return e.executeSubquery(ctx, pattern.(*SubqueryPlan))
    // ... existing cases
    }
}

func (e *Executor) executeSubquery(ctx Context, plan *SubqueryPlan) (Relation, error) {
    // 1. Gather input values from current context
    inputs := make(map[query.Symbol]interface{})
    for i, input := range plan.Inputs {
        if v, ok := input.(query.Variable); ok {
            inputs[v.Name] = ctx.GetBinding(v.Name)
        }
    }
    
    // 2. Create new context with inputs
    subCtx := ctx.WithInputs(inputs)
    
    // 3. Execute nested query
    result, err := e.ExecuteWithContext(subCtx, plan.NestedQuery)
    
    // 4. Bind results according to binding form
    return bindSubqueryResults(result, plan.Binding)
}
```

### 5. Context Updates

**File**: `datalog/executor/context.go`

Add support for query inputs:

```go
type Context interface {
    // ... existing methods
    
    // WithInputs creates a context with input bindings
    WithInputs(inputs map[Symbol]interface{}) Context
    
    // GetBinding retrieves a bound value
    GetBinding(sym Symbol) interface{}
}
```

## Testing Strategy

### Unit Tests

1. **Parser tests** - Verify subquery patterns parse correctly
2. **Planner tests** - Ensure subqueries are assigned to correct phases
3. **Executor tests** - Test subquery execution with various binding forms

### Integration Tests

1. **OHLC query** - The motivating example should work correctly
2. **Nested aggregations** - Subqueries within subqueries
3. **Multiple row returns** - Collection and relation bindings
4. **Performance tests** - Ensure subqueries don't create performance issues

### Example Test Case

```go
func TestSubqueryOHLC(t *testing.T) {
    query := `[:find ?date ?high ?low ?open ?close
               :where 
                [?s :symbol/ticker "CRWV"]
                [(ground "2025-06-02") ?date]
                
                [(q [:find (max ?h)
                     :in $ ?symbol ?date  
                     :where [?p :price/symbol ?symbol]
                            [?p :price/time ?t]
                            [(same-date? ?t ?date)]
                            [?p :price/high ?h]]
                    ?s ?date) [[?high]]]
                    
                [(q [:find (min ?l)
                     :in $ ?symbol ?date
                     :where [?p :price/symbol ?symbol]
                            [?p :price/time ?t]
                            [(same-date? ?t ?date)]
                            [?p :price/low ?l]]
                    ?s ?date) [[?low]]]
                
                [?p-open :price/symbol ?s]
                [?p-open :price/minute-of-day 570]
                [?p-open :price/open ?open]
                
                [?p-close :price/symbol ?s]
                [?p-close :price/minute-of-day 960]
                [?p-close :price/close ?close]]`
    
    // Should return exactly 1 row with correct values
    result, err := executor.Execute(parseQuery(query))
    assert.NoError(t, err)
    assert.Equal(t, 1, result.Size())
    assert.Equal(t, 123.66, result.Get(0)[1]) // high
    assert.Equal(t, 109.01, result.Get(0)[2]) // low
    assert.Equal(t, 116.30, result.Get(0)[3]) // open
    assert.Equal(t, 119.75, result.Get(0)[4]) // close
}
```

## Implementation Order

1. **Phase 1**: Basic subquery support
   - Parse subquery patterns
   - Simple tuple binding `[[?var]]`
   - Single return value only
   - No nested subqueries

2. **Phase 2**: Full binding forms
   - Collection binding `?coll`
   - Relation binding `[[?a ?b] ...]`
   - Multiple return values

3. **Phase 3**: Advanced features
   - Nested subqueries
   - Subqueries in expression positions
   - Performance optimizations (caching repeated subqueries)

## Alternative Considerations

### Why not rules?
Rules would require more extensive changes and don't solve the scoping problem as cleanly.

### Why not special aggregate functions?
Less general and would require many domain-specific functions.

### Why not fix aggregations?
The current behavior matches Datomic semantics. Changing it would break compatibility.

## Migration Path

Existing queries continue to work unchanged. Subqueries are purely additive - users can adopt them when they hit the limitations of the current aggregation model.

## Documentation

Update CLAUDE.md with:
- Subquery syntax and examples
- When to use subqueries vs regular aggregations
- Performance considerations
- Common patterns (OHLC, running totals, etc.)