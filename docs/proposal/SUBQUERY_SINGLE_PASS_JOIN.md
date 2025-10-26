# Proposal: Single-Pass Subquery Join Optimization

**Status**: Proposed
**Date**: 2025-10-25
**Priority**: Medium (Performance optimization)

## Problem

The current subquery implementation in `expressions_and_predicates.go` requires iterating the input relation twice:

1. **Pass 1**: `executeSubquery()` iterates `result` to extract unique input combinations
2. **Pass 2**: `result.Join(subqResult)` iterates `result` again to perform the join

This double-iteration has several issues:

- **Violates single-use semantics**: StreamingRelations can only be iterated once
- **Requires materialization**: We must call `result.Materialize()` before subqueries, forcing eager evaluation
- **Performance overhead**: Two full scans of the relation instead of one
- **Memory pressure**: Materialization stores all tuples in memory

## Current Workaround

### Location 1: applyExpressionsAndPredicates()

```go
// In applyExpressionsAndPredicates()
if sr, ok := result.(*StreamingRelation); ok {
    result = sr.Materialize()  // Force materialization
}

subqResult, err := e.executeSubquery(ctx, subqPlan, result)
result = result.Join(subqResult)  // Second iteration
```

### Location 2: executeSubquery() - Added 2025-10-25

**Bug Found**: `TestSubqueryWithInputParameter` failed with panic:
```
BUG: HashJoin received a StreamingRelation that was already consumed.
```

**Root Cause**: In `query_executor.go:executeSubquery()`:
1. Line 292: `combinedRel = groups[0]` creates reference to StreamingRelation
2. Line 314: `getUniqueInputCombinations(combinedRel, ...)` consumes it (calls `Iterator()`)
3. Line 104-105: Later collapse tries to join with consumed relation → panic

**Workaround Applied** (query_executor.go:315):
```go
// CRITICAL: Materialize combinedRel since getUniqueInputCombinations will consume it
// This ensures the original relations in groups remain unconsumed for later joining
combinedRel = combinedRel.Materialize()
```

This patches the immediate issue but demonstrates the **same double-iteration problem** exists in multiple places. The single-pass solution would eliminate all of these materializations.

## Proposed Solution

### Single-Pass Hash Join

Combine input extraction and hash table building into a single pass:

```go
// New signature for executeSubquery
type SubqueryResult struct {
    HashTable  *TupleKeyMap    // Hash table for join
    SubqResult Relation         // Subquery results
}

func (e *Executor) executeSubqueryWithHashTable(
    ctx Context,
    subqPlan *SubqueryPlan,
    inputRel Relation,
) (*SubqueryResult, error) {
    // Single pass over inputRel
    inputIndices := findInputColumns(inputRel, subqPlan.InputVariables)
    hashTable := NewTupleKeyMap()
    inputCombinations := NewTupleKeyMap()

    it := inputRel.Iterator()
    defer it.Close()

    for it.Next() {
        tuple := it.Tuple()

        // Extract input for subquery
        inputValues := extractValues(tuple, inputIndices)
        inputKey := NewTupleKey(inputValues, ...)
        inputCombinations.Put(inputKey, inputValues)

        // Build hash table for join (keyed by join columns)
        joinKey := NewTupleKey(tuple, joinIndices)
        hashTable.Put(joinKey, tuple)  // Store full tuple
    }

    // Execute subquery once per unique input combination
    subqResults := []Tuple{}
    for _, inputVals := range inputCombinations.Values() {
        results := executeSubqueryForInput(ctx, subqPlan, inputVals)
        subqResults = append(subqResults, results...)
    }

    return &SubqueryResult{
        HashTable:  hashTable,
        SubqResult: NewMaterializedRelation(subqCols, subqResults),
    }, nil
}
```

### Join Using Hash Table

```go
// In applyExpressionsAndPredicates()
subqResult, err := e.executeSubqueryWithHashTable(ctx, subqPlan, result)

// Probe hash table instead of iterating result again
joinedTuples := []Tuple{}
subqIt := subqResult.SubqResult.Iterator()
for subqIt.Next() {
    subqTuple := subqIt.Tuple()
    joinKey := NewTupleKey(subqTuple, joinIndices)

    if matches, ok := subqResult.HashTable.Get(joinKey); ok {
        for _, inputTuple := range matches.([]Tuple) {
            joinedTuples = append(joinedTuples, combine(inputTuple, subqTuple))
        }
    }
}

result = NewMaterializedRelation(outputCols, joinedTuples)
```

## Benefits

1. **Single iteration**: Input relation scanned only once
2. **No forced materialization**: Works with streaming relations
3. **Better performance**: ~2× faster for subquery-heavy queries
4. **Lower memory**: No need to materialize input before processing
5. **Cleaner semantics**: Respects single-use iterator contract

## Implementation Plan

1. **Phase 1**: Add `executeSubqueryWithHashTable()` method
   - Extract input combinations during iteration
   - Build hash table simultaneously
   - Return both structures

2. **Phase 2**: Update `applyExpressionsAndPredicates()`
   - Call new method instead of old
   - Probe hash table for join
   - Remove materialization workaround

3. **Phase 3**: Testing
   - Verify all subquery tests pass
   - Add performance benchmarks
   - Test with large relations

## Alternative Considered

**Keep materialization approach**: Simple but inefficient. Forces eager evaluation and double-scans.

## Open Questions

1. **Memory trade-off**: Hash table vs materialized relation - which uses less memory?
2. **Streaming output**: Can we make the join itself streaming instead of materializing?
3. **Multiple subqueries**: How does this interact with multiple subqueries in same phase?

## Related Issues

- `docs/bugs/BUG_STREAMING_RELATION_PREMATURE_MATERIALIZATION.md` - Root cause of this investigation
- Single-use iterator semantics introduced 2025-10-25
- **NEW**: `TestSubqueryWithInputParameter` failure (2025-10-25) - Found second instance of double-iteration bug in `query_executor.go:executeSubquery()`, confirming this is a systemic issue requiring architectural fix

## References

- Current implementation Location 1: `datalog/executor/expressions_and_predicates.go:218-236`
- Current implementation Location 2: `datalog/executor/query_executor.go:287-318` (executeSubquery)
- Subquery executor: `datalog/executor/subquery.go`
- Input extraction: `datalog/executor/subquery.go:421-474` (getUniqueInputCombinations)
