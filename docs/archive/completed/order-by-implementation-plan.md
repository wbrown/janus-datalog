# Order-By Implementation Plan

## Datomic Syntax

Datomic supports `:order-by` as a query clause that specifies how to sort the result set. The syntax is:

```clojure
[:find ?customer ?product
 :where [?order :order/customer ?customer]
        [?order :order/product ?product]
 :order-by [[?customer :asc] [?product :desc]]]
```

### Key Features:
1. **Multiple sort keys**: Can order by multiple variables in sequence
2. **Direction specifiers**: `:asc` (ascending) or `:desc` (descending)
3. **Default direction**: If no direction specified, defaults to `:asc`
4. **Position**: Appears after `:where` clause, before any `:limit` clause

### Examples:
```clojure
; Simple ascending order
[:find ?name ?age
 :where [?p :person/name ?name]
        [?p :person/age ?age]
 :order-by [[?age :asc]]]

; Multiple sort keys
[:find ?date ?symbol ?price
 :where [?t :trade/date ?date]
        [?t :trade/symbol ?symbol]
        [?t :trade/price ?price]
 :order-by [[?date :desc] [?symbol :asc] [?price :desc]]]

; Default direction (ascending)
[:find ?name
 :where [?p :person/name ?name]
 :order-by [?name]]  ; Same as [[?name :asc]]
```

## Implementation Plan

### 1. Query Structure Updates

Add OrderBy field to the Query struct:

```go
// datalog/query/types.go

type Query struct {
    Find    []FindElement
    In      []InputSpec
    Where   []Pattern
    OrderBy []OrderByClause  // New field
}

type OrderByClause struct {
    Variable  Symbol
    Direction OrderDirection
}

type OrderDirection string

const (
    OrderAsc  OrderDirection = "asc"
    OrderDesc OrderDirection = "desc"
)
```

### 2. Parser Updates

Update the parser to handle `:order-by` clause:

```go
// datalog/parser/parser.go

// In parseQuery function, add after :where parsing:
case ":order-by":
    orderBy, err := parseOrderBy(tokens[i+1])
    if err != nil {
        return nil, fmt.Errorf("invalid :order-by clause: %w", err)
    }
    query.OrderBy = orderBy
    i++
```

Parser needs to handle:
- `[[?var :asc]]` - Explicit direction
- `[[?var :desc]]` - Explicit direction  
- `[?var]` - Implicit ascending (shorthand)
- `[[?var1 :asc] [?var2 :desc]]` - Multiple sort keys

### 3. Executor Implementation

The executor needs to sort results after query execution but before returning:

```go
// datalog/executor/executor.go

// In executePhases or Execute method, after getting final results:
if len(plan.Query.OrderBy) > 0 {
    finalResult = sortRelation(finalResult, plan.Query.OrderBy)
}
```

Sort implementation:
```go
func sortRelation(rel Relation, orderBy []query.OrderByClause) Relation {
    // Materialize if streaming
    mat := materializeIfNeeded(rel)
    
    // Get column indices for sort variables
    sortIndices := make([]int, len(orderBy))
    for i, clause := range orderBy {
        idx := mat.ColumnIndex(clause.Variable)
        if idx < 0 {
            // Variable not in results, skip sorting by it
            continue
        }
        sortIndices[i] = idx
    }
    
    // Sort tuples
    tuples := mat.Tuples()
    sort.Slice(tuples, func(i, j int) bool {
        for k, clause := range orderBy {
            if sortIndices[k] < 0 {
                continue
            }
            
            cmp := compareTupleValues(
                tuples[i][sortIndices[k]], 
                tuples[j][sortIndices[k]]
            )
            
            if cmp < 0 {
                return clause.Direction != OrderDesc
            } else if cmp > 0 {
                return clause.Direction == OrderDesc
            }
            // Equal, continue to next sort key
        }
        return false
    })
    
    return NewMaterializedRelation(mat.Columns(), tuples)
}
```

### 4. Value Comparison

Need a generic comparison function that handles all Datalog value types:

```go
func compareTupleValues(a, b interface{}) int {
    // Handle nil
    if a == nil && b == nil {
        return 0
    }
    if a == nil {
        return -1
    }
    if b == nil {
        return 1
    }
    
    // Type-specific comparison
    switch va := a.(type) {
    case string:
        if vb, ok := b.(string); ok {
            return strings.Compare(va, vb)
        }
    case int64:
        if vb, ok := b.(int64); ok {
            if va < vb {
                return -1
            } else if va > vb {
                return 1
            }
            return 0
        }
    case float64:
        if vb, ok := b.(float64); ok {
            if va < vb {
                return -1
            } else if va > vb {
                return 1
            }
            return 0
        }
    case time.Time:
        if vb, ok := b.(time.Time); ok {
            return va.Compare(vb)
        }
    case datalog.Identity:
        if vb, ok := b.(datalog.Identity); ok {
            return strings.Compare(va.String(), vb.String())
        }
    case datalog.Keyword:
        if vb, ok := b.(datalog.Keyword); ok {
            return strings.Compare(string(va), string(vb))
        }
    }
    
    // Fallback to string comparison
    return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}
```

### 5. Formatter Updates

Update the query formatter to include order-by clauses:

```go
// datalog/parser/formatter.go

// In FormatQuery:
if len(q.OrderBy) > 0 {
    fmt.Fprintf(&buf, "\n :order-by [")
    for i, clause := range q.OrderBy {
        if i > 0 {
            buf.WriteString(" ")
        }
        if clause.Direction == "" || clause.Direction == query.OrderAsc {
            fmt.Fprintf(&buf, "%s", clause.Variable)
        } else {
            fmt.Fprintf(&buf, "[%s :%s]", clause.Variable, clause.Direction)
        }
    }
    buf.WriteString("]")
}
```

## Testing

Test cases should include:
1. Single column ascending/descending
2. Multiple columns with mixed directions
3. Sorting different data types (strings, numbers, dates, identities)
4. Handling nil/missing values
5. Variables not in find clause (should be ignored)
6. Empty order-by clause
7. Integration with aggregations and subqueries

## Future Enhancements

1. **Collation support**: Language-specific string sorting
2. **Custom comparators**: User-defined sorting functions
3. **Null handling**: NULLS FIRST/NULLS LAST options
4. **Performance**: Index-aware sorting for large results
5. **Limit clause**: Often used with order-by for top-N queries