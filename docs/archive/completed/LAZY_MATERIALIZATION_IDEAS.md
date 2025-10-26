# Lazy Materialization Ideas for Datalog Relations

## Current State: Eager Materialization

Currently, our Datalog implementation materializes (loads into memory) relations at many points:

### Where We Materialize
1. **Subquery Results**: All subquery results are collected into `[]Tuple` before returning
2. **Join Operations**: Hash joins materialize the build side completely
3. **Aggregations**: Must see all tuples to compute aggregates
4. **Sorting**: Must see all tuples to sort
5. **Union Operations**: Collect all tuples from multiple relations

### Example: Subquery Execution
```go
// Current eager approach in ExecuteSubqueryWithExecutor
var allTuples []Tuple
for _, rel := range allResults {
    it := rel.Iterator()
    for it.Next() {
        allTuples = append(allTuples, it.Tuple())
    }
    it.Close()
}
result := NewMaterializedRelation(columns, allTuples)
```

## The Clojure/Lazy Approach

In a lazy evaluation model (like Clojure's sequences), operations would chain without materializing:

### Conceptual Lazy Pipeline
```
Input Combinations (lazy seq)
  → Execute Subquery (returns lazy iterator)
    → Apply Binding Form (streaming transformation)
      → Union/Concat (lazy concatenation)
        → Parent Query Pulls As Needed
```

### Benefits
1. **Memory Efficiency**: Process millions of rows without loading all into memory
2. **Early Termination**: If parent only needs first N results, don't compute rest
3. **Pipeline Parallelism**: Different stages could run concurrently
4. **Composability**: Operations naturally compose without intermediate collections

## Implementation Ideas

### 1. Lazy Subquery Iterator
```go
type LazySubqueryRelation struct {
    inputCombinations []map[Symbol]interface{}
    parentExec        *Executor
    plan              *QueryPlan
    binding           BindingForm
}

func (r *LazySubqueryRelation) Iterator() RelationIterator {
    return &lazySubqueryIterator{
        relation:          r,
        currentInputIdx:   0,
        currentIterator:   nil,
    }
}

type lazySubqueryIterator struct {
    relation          *LazySubqueryRelation
    currentInputIdx   int
    currentIterator   RelationIterator
    currentInput      map[Symbol]interface{}
}

func (it *lazySubqueryIterator) Next() bool {
    // Try current iterator
    if it.currentIterator != nil && it.currentIterator.Next() {
        return true
    }
    
    // Advance to next input combination
    for it.currentInputIdx < len(it.relation.inputCombinations) {
        input := it.relation.inputCombinations[it.currentInputIdx]
        it.currentInputIdx++
        
        // Execute subquery lazily for this input
        inputRelations := createInputRelations(input)
        result, _ := it.relation.parentExec.executePhasesWithInputs(
            ctx, it.relation.plan, inputRelations)
        
        // Get iterator for this result
        it.currentIterator = result.Iterator()
        it.currentInput = input
        
        if it.currentIterator.Next() {
            return true
        }
    }
    
    return false
}

func (it *lazySubqueryIterator) Tuple() Tuple {
    // Get tuple from current subquery
    tuple := it.currentIterator.Tuple()
    
    // Apply binding form (augment with input values)
    return applyBindingLazy(tuple, it.currentInput)
}
```

### 2. Lazy Union/Concatenation
```go
type LazyUnionRelation struct {
    relations []Relation
}

func (r *LazyUnionRelation) Iterator() RelationIterator {
    return &lazyUnionIterator{
        relations:   r.relations,
        currentIdx:  0,
        currentIter: nil,
    }
}

type lazyUnionIterator struct {
    relations   []Relation
    currentIdx  int
    currentIter RelationIterator
}

func (it *lazyUnionIterator) Next() bool {
    // Exhaust current iterator
    if it.currentIter != nil && it.currentIter.Next() {
        return true
    }
    
    // Move to next relation
    for it.currentIdx < len(it.relations) {
        if it.currentIter != nil {
            it.currentIter.Close()
        }
        
        it.currentIter = it.relations[it.currentIdx].Iterator()
        it.currentIdx++
        
        if it.currentIter.Next() {
            return true
        }
    }
    
    return false
}
```

### 3. Streaming Transformations
```go
// Project columns without materializing
type ProjectedRelation struct {
    source      Relation
    projection  []int  // Column indices to keep
}

func (r *ProjectedRelation) Iterator() RelationIterator {
    return &projectedIterator{
        sourceIter: r.source.Iterator(),
        projection: r.projection,
    }
}

// Apply predicates without materializing
type FilteredRelation struct {
    source    Relation
    predicate func(Tuple) bool
}

func (r *FilteredRelation) Iterator() RelationIterator {
    return &filteredIterator{
        sourceIter: r.source.Iterator(),
        predicate:  r.predicate,
    }
}
```

## Challenges

### 1. Operations That Require Materialization
Some operations inherently need all data:
- **Aggregations**: Need all tuples to compute sum, avg, etc.
- **Sorting**: Need all tuples to determine order
- **Hash Join Build Side**: Need all tuples to build hash table
- **Duplicate Elimination**: Need to track seen tuples

### 2. Iterator Lifecycle Management
- Who closes iterators?
- What happens if iteration is abandoned?
- Resource cleanup for database cursors

### 3. Error Handling
- Lazy evaluation delays error detection
- Errors occur during iteration, not during query setup
- Hard to provide good error context

### 4. Debugging
- Stack traces become complex with lazy evaluation
- Hard to inspect intermediate results
- Performance profiling more difficult

## Hybrid Approach

A practical approach might be:

1. **Lazy by Default**: Relations are lazy iterators
2. **Explicit Materialization**: Operations that need it call `.Materialize()`
3. **Smart Materialization**: Detect when materialization helps (small relations for hash joins)
4. **Configurable**: Let users choose eager vs lazy for different operations

```go
type Relation interface {
    Iterator() RelationIterator
    Columns() []Symbol
    
    // Lazy operations return lazy relations
    Filter(predicate) Relation
    Project(columns) Relation
    
    // Operations that might materialize
    Join(other Relation) Relation  // Might materialize build side
    Sort(orderBy) Relation          // Must materialize
    
    // Explicit materialization
    Materialize() MaterializedRelation
    IsLazy() bool
}
```

## Performance Implications

### When Lazy Wins
- Large datasets with selective filters
- Queries that only need first N results
- Subqueries with many input combinations
- Pipeline operations (filter → project → filter)

### When Eager Wins
- Small datasets that fit in memory
- Multiple consumers of same relation
- Operations that need random access
- Debugging and development

## Next Steps

1. **Benchmark Current Materialization Points**: Measure memory usage and performance
2. **Prototype Lazy Iterators**: Start with simple cases like projection/filtering
3. **Identify Critical Paths**: Focus on operations that process most data
4. **Gradual Migration**: Convert one operation at a time to lazy evaluation
5. **Configuration Options**: Let users control materialization strategy

## Inspiration from Other Systems

### Clojure
- Everything is a lazy seq by default
- Explicit `doall` for materialization
- Chunked sequences for efficiency

### Apache Spark
- RDD transformations are lazy
- Actions trigger computation
- Intelligent materialization decisions

### Java Streams
- Pipeline of lazy operations
- Terminal operations trigger evaluation
- Parallel streams for free

### PostgreSQL
- Cursor-based iteration for large results
- Work_mem controls when to spill to disk
- Sophisticated cost-based decisions

## Conclusion

Moving to lazy evaluation would be a significant architectural change but could provide major benefits for large-scale data processing. The key is to:

1. Keep the Relation interface simple
2. Allow both lazy and eager implementations
3. Make materialization points explicit
4. Provide good debugging tools
5. Benchmark real workloads to guide decisions

The current eager approach is simpler and works well for smaller datasets. As the system grows, lazy evaluation becomes more important for handling larger data volumes efficiently.