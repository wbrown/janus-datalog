# Decorrelation in New Executor - Implementation Plan

**Goal**: Add decorrelation optimization to the new QueryExecutor while preserving its clause-by-clause simplicity for common queries.

**Status**: Implemented (Basic Batching)

**Last Updated**: October 2025

---

## Overview

Decorrelation batches multiple subqueries with identical correlation signatures into a single execution, dramatically reducing iteration count (e.g., 870 iterations → 1 batched execution for OHLC queries).

**Key Constraint**: Must preserve "Datalog + Relations → Relations" tenet - no custom clause types, only valid Datalog.

**Strategy**: Isolated execution path - simple clause-by-clause for most queries, decorrelation path only when beneficial.

---

## Architecture

### Entry Point Decision

```go
func (e *DefaultQueryExecutor) Execute(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error) {
    // Check if decorrelation is beneficial
    if e.options.EnableSubqueryDecorrelation && shouldDecorrelate(q.Where) {
        return e.executeWithDecorrelation(ctx, q, inputs)
    }

    // Simple path: clause-by-clause (unchanged)
    for i, clause := range q.Where {
        // ... existing logic
    }
}
```

### Decorrelation Decision Logic

```go
func shouldDecorrelate(clauses []query.Clause) bool {
    // Count SubqueryPattern clauses
    subqueryCount := 0
    for _, clause := range clauses {
        if _, ok := clause.(*query.SubqueryPattern); ok {
            subqueryCount++
        }
    }

    // Only worth decorrelating if multiple subqueries exist
    return subqueryCount >= 2
}
```

---

## Implementation Steps

### Phase 1: Analysis & Grouping

**File**: `datalog/executor/decorrelation_executor.go` (new file)

1. **Extract subqueries and group by signature**
   ```go
   func analyzeSubqueries(clauses []query.Clause) (
       subqueries []*query.SubqueryPattern,
       otherClauses []query.Clause,
       groups map[string][]int, // signature → clause indices
   )
   ```

2. **Correlation signature detection**
   ```go
   func extractCorrelationSignature(subq *query.SubqueryPattern) string {
       // Analyze:
       // - Input variables (correlation vars)
       // - Query pattern fingerprint
       // - Aggregate vs non-aggregate
       // Return: Hash for grouping
   }
   ```

3. **Filter criteria**
   - Only group subqueries with 2+ instances
   - Skip pure aggregations (no grouping vars) - can't decorrelate
   - Only group queries with identical structure

### Phase 2: Two-Phase Execution

**The decorrelation path processes clauses in TWO phases:**

#### Phase A: Execute non-subquery clauses
```go
func (e *DefaultQueryExecutor) executeWithDecorrelation(ctx Context, q *query.Query, inputs []Relation) ([]Relation, error) {
    // 1. Analyze query structure
    subqueries, otherClauses, groups := analyzeSubqueries(q.Where)

    // 2. Execute all non-subquery clauses first (patterns, expressions, predicates)
    //    This builds up the relation groups that provide correlation variables
    groups := Relations(inputs)
    for _, clause := range otherClauses {
        // Execute pattern/expression/predicate
        // Collapse after each step
    }

    // 3. At this point, groups contain all correlation variables
    //    Now execute subqueries in batched form
    groups, err := e.executeBatchedSubqueries(ctx, subqueries, groups, decorrelationGroups)

    // 4. Apply final :find clause
    return e.applyFindClause(ctx, q.Find, groups)
}
```

#### Phase B: Batched subquery execution
```go
func (e *DefaultQueryExecutor) executeBatchedSubqueries(
    ctx Context,
    subqueries []*query.SubqueryPattern,
    groups []Relation,
    decorrelationGroups map[string][]int,
) ([]Relation, error) {

    // For each decorrelation group (same signature):
    for signature, indices := range decorrelationGroups {
        if len(indices) == 1 {
            // Single subquery - execute normally
            continue
        }

        // Multiple subqueries with same signature - batch them
        batchResults := e.executeBatchedGroup(ctx, subqueries, indices, groups)

        // Distribute results back to individual bindings
        // Each subquery gets its slice of the batch results
    }

    // Single subqueries execute via normal path
    for idx, subq := range subqueries {
        if !isBatched(idx) {
            // Normal execution
        }
    }
}
```

### Phase 3: Batch Execution Logic

**Key insight**: Batch execution means running the subquery ONCE with ALL input combinations, then distributing results.

```go
func (e *DefaultQueryExecutor) executeBatchedGroup(
    ctx Context,
    subqueries []*query.SubqueryPattern,
    indices []int, // Which subqueries to batch
    groups []Relation,
) map[int]Relation {

    // 1. Extract ALL unique input combinations from groups
    //    (This is what we'd iterate over N times normally)
    representative := subqueries[indices[0]]
    allInputCombinations := getUniqueInputCombinations(groups, representative.Inputs)

    // 2. Create ONE input relation with ALL combinations
    batchInputRelation := createBatchInputRelation(allInputCombinations)

    // 3. Execute subquery ONCE with batched input
    //    The subquery's :in clause expects RelationInput, we provide all tuples
    batchResult, err := e.Execute(ctx, representative.Query, []Relation{batchInputRelation})

    // 4. Split results by correlation variables
    //    Each subquery gets its portion based on correlation values
    resultsBySubquery := distributeBatchResults(batchResult, indices, allInputCombinations)

    return resultsBySubquery
}
```

### Phase 4: Result Distribution

**The tricky part**: After batch execution, we need to map results back to individual subqueries.

```go
func distributeBatchResults(
    batchResult Relation,
    subqueryIndices []int,
    inputCombinations []map[query.Symbol]interface{},
) map[int]Relation {

    // The batch result contains columns:
    // [correlation_vars..., agg_result_1, agg_result_2, ..., agg_result_N]

    // For each subquery:
    results := make(map[int]Relation)
    for i, subqIdx := range subqueryIndices {
        // Extract just the columns for this subquery
        // Join back with correlation variables for binding
        results[subqIdx] = extractSubqueryResults(batchResult, i)
    }

    return results
}
```

---

## Execution Flow Comparison

### Without Decorrelation (Simple Path)

```
Query: [:find ?datetime ?high ?low :where ... SubQ1 ... SubQ2 ...]

Execute pattern clauses → groups = [tuples with ?s, ?year, ?hour]
  │
  ├─ 870 input combinations
  │
  ├─ For each combination (870 iterations):
  │    │
  │    ├─ Execute SubQ1 → ?high
  │    └─ Execute SubQ2 → ?low
  │
  └─ Combine all 870 results

Total subquery executions: 870 × 2 = 1740
```

### With Decorrelation (Batched Path)

```
Query: [:find ?datetime ?high ?low :where ... SubQ1 ... SubQ2 ...]

Execute pattern clauses → groups = [tuples with ?s, ?year, ?hour]
  │
  ├─ 870 input combinations detected
  │
  ├─ Group SubQ1 and SubQ2 by signature
  │    └─ Both have signature: (?s, ?year, ?hour) with grouped aggregation
  │
  ├─ Execute ONCE with all 870 combinations as RelationInput
  │    │
  │    └─ Batch query returns: [?s ?year ?hour ?high ?low] with 870 rows
  │
  └─ Distribute results:
       ├─ SubQ1 gets ?high column
       └─ SubQ2 gets ?low column

Total subquery executions: 1 (batch with 2 aggregations)
```

---

## Implementation Checklist

### Step 1: Core Infrastructure
- [ ] Create `datalog/executor/decorrelation_executor.go`
- [ ] Implement `shouldDecorrelate(clauses []query.Clause) bool`
- [ ] Implement `analyzeSubqueries()` - extract and group subqueries
- [ ] Implement `extractCorrelationSignature()` - hash subquery pattern

### Step 2: Batched Execution Path
- [ ] Implement `executeWithDecorrelation()` - main entry point
- [ ] Implement two-phase execution (non-subquery clauses, then batched subqueries)
- [ ] Implement `executeBatchedGroup()` - execute N subqueries as one
- [ ] Implement `createBatchInputRelation()` - bundle all input combinations

### Step 3: Result Distribution
- [ ] Implement `distributeBatchResults()` - split batch results back to subqueries
- [ ] Handle binding forms (TupleBinding, RelationBinding)
- [ ] Join results with correlation variables for proper binding

### Step 4: Integration
- [ ] Add decision point in `QueryExecutor.Execute()`
- [ ] Wire up `EnableSubqueryDecorrelation` option
- [ ] Add annotations for decorrelation path (debugging)

### Step 5: Testing
- [ ] Test simple case: 2 subqueries, same signature
- [ ] Test OHLC query: 4 subqueries, mixed signatures
- [ ] Test edge cases:
  - [ ] Single subquery (no decorrelation)
  - [ ] Pure aggregations (should NOT decorrelate)
  - [ ] Grouped aggregations (SHOULD decorrelate)
  - [ ] Mixed - some batchable, some not
- [ ] Performance comparison: old executor vs new with decorrelation
- [ ] Correctness: Verify identical results to old executor

### Step 6: Documentation
- [ ] Update ARCHITECTURE.md with decorrelation in new executor
- [ ] Document when decorrelation applies vs doesn't
- [ ] Add performance benchmarks
- [ ] Migration guide: old executor → new executor with decorrelation

---

## Key Design Decisions

### Decision 1: Two-Phase Execution
**Rationale**: We must execute non-subquery clauses first to build up correlation variables. Can't batch subqueries until we know what input combinations exist.

### Decision 2: Isolated Execution Path
**Rationale**: Preserves simplicity of clause-by-clause execution for 90% of queries. Decorrelation complexity is opt-in and isolated.

### Decision 3: No Custom Clause Types
**Rationale**: Maintains "Datalog + Relations → Relations" tenet. Decorrelation is an execution strategy, not a query transformation.

### Decision 4: Use Existing Subquery Infrastructure
**Rationale**: Batch execution still goes through `QueryExecutor.Execute()` - just with different input relations. Reuses all existing logic.

---

## Performance Expectations

### OHLC Query (Current Bottleneck)
- **Old executor with decorrelation**: ~1 second (1 batch + 870 simple iterations)
- **New executor without decorrelation**: ~15-20 seconds (1740 full iterations)
- **New executor WITH decorrelation**: ~1-2 seconds (1 batch, similar to old)

**Target**: Within 2× of old executor performance on decorrelation-friendly queries.

### Simple Queries (No Subqueries)
- **New executor**: No change (decorrelation path not taken)
- **Performance**: Identical to current simple path

---

## Risks & Mitigations

### Risk 1: Increased Complexity
**Impact**: New executor loses its primary advantage (simplicity)
**Mitigation**:
- Keep decorrelation path completely isolated
- Extensive comments explaining batch execution
- Clear separation: `query_executor.go` (simple) vs `decorrelation_executor.go` (complex)

### Risk 2: Subtle Bugs in Result Distribution
**Impact**: Wrong results returned to users
**Mitigation**:
- Comprehensive test suite comparing to old executor
- Test with various binding forms
- Annotations/logging to trace result distribution

### Risk 3: Not Actually Faster
**Impact**: Complexity without benefit
**Mitigation**:
- Benchmark early in implementation
- Verify batch execution is actually more efficient
- Consider aborting if benefits don't materialize

---

## Success Criteria

1. ✅ All existing tests pass
2. ✅ New decorrelation tests pass (correctness vs old executor)
3. ✅ OHLC query performance within 2× of old executor
4. ✅ Simple queries (no subqueries) unchanged in performance
5. ✅ Code remains maintainable (decorrelation isolated to one file)
6. ✅ Can retire old executor with confidence

---

## Timeline Estimate

- **Step 1-2 (Infrastructure + Basic Batching)**: 1-2 days
- **Step 3 (Result Distribution)**: 1-2 days (complex)
- **Step 4 (Integration)**: 0.5 day
- **Step 5 (Testing)**: 1-2 days (thorough testing critical)
- **Step 6 (Documentation)**: 0.5 day

**Total**: 4-7 days of focused work

---

## Open Questions

1. **CSE (Common Subexpression Elimination)**: Should we also implement CSE in the new executor?
   - Tests show minimal benefit with parallel decorrelation
   - **Recommendation**: Skip CSE for now, revisit if needed

2. **Semantic Rewriting**: Should we tackle time predicate rewriting too?
   - More straightforward than decorrelation (pure clause transform)
   - **Recommendation**: Separate plan after decorrelation works

3. **Parallel Decorrelation**: Should batch execution use worker pools?
   - Old executor has `EnableParallelDecorrelation` option
   - **Recommendation**: Start sequential, add parallel later if needed

---

## Implementation Summary (October 2025)

### What Was Implemented

The basic decorrelation infrastructure has been implemented with the following components:

#### 1. Core Infrastructure (`decorrelation_executor.go`)

- **`shouldDecorrelate()`**: Determines if a query has 2+ subqueries (decorrelation threshold)
- **`analyzeSubqueries()`**: Separates subqueries from other clauses and groups by signature
- **`extractCorrelationSignature()`**: Creates hash from input variables + pattern fingerprint
- **`isGroupedAggregation()`**: Identifies grouped vs pure aggregations
- **`getBatchableGroups()`**: Filters groups that should be batched (2+ subqueries, grouped aggs)

#### 2. Batched Execution Path

- **`executeWithDecorrelation()`**: Two-phase execution (non-subquery clauses, then batched subqueries)
- **`executeBatchedGroup()`**: Executes group of subqueries with batched inputs
- **`executeBatchedSubquery()`**: Executes single subquery with batched RelationInput
- **`createBatchedInputRelation()`**: Creates relation with all input combinations
- **`canUseBatchedInput()`**: Checks if subquery accepts RelationInput

#### 3. Integration

- Added `EnableSubqueryDecorrelation` option to `ExecutorOptions`
- Decision point in `QueryExecutor.Execute()` routes to decorrelation path when beneficial
- Transparent to callers - no API changes

#### 4. Testing

- **Infrastructure tests**: `decorrelation_executor_test.go` validates core functions
- **End-to-end test**: `decorrelation_end_to_end_test.go` validates full decorrelation path with OHLC-style query
- All existing tests pass - no regressions

### Performance Characteristics

**Current Implementation**:
- **Without decorrelation**: N × M subquery executions (N = input combinations, M = subqueries)
- **With decorrelation**: M batched executions (each subquery executed once with all N inputs)

**Example (OHLC query)**:
- Input combinations: 870 (e.g., hour tuples)
- Subqueries: 4 (open, high, low, close)
- **Before**: 870 × 4 = 3,480 executions
- **After**: 4 batched executions

**Comparison to Old Executor**:
- Old executor with full decorrelation: 1-3 merged executions (queries are merged + GROUP BY)
- New executor with batching: M executions (one per subquery)
- **Trade-off**: New executor is simpler (no query merging) but does M executions vs 1-3

### Current Limitations

1. **No Query Merging**: Unlike the old executor, we don't merge multiple subqueries into a single query with GROUP BY
2. **RelationInput Required**: Subqueries must accept `[[?vars] ...]` syntax for batching to work
3. **Grouped Aggregations Only**: Pure aggregations (only aggregates, no grouping vars) are NOT decorrelated

### Future Enhancements

1. **Query Merging**: Merge M subqueries into 1 combined query (requires query rewriting, violates "Datalog purity")
2. **Parallel Batching**: Execute batched subqueries in parallel using worker pools
3. **Time Range Pushdown**: Extract time ranges from correlation variables for semi-join optimization
4. **Performance Benchmarking**: Compare to old executor on real-world OHLC queries

### Files Modified/Created

- **Created**: `datalog/executor/decorrelation_executor.go` (577 lines)
- **Created**: `datalog/executor/decorrelation_executor_test.go` (230 lines)
- **Created**: `datalog/executor/decorrelation_end_to_end_test.go` (200 lines)
- **Modified**: `datalog/executor/options.go` (added EnableSubqueryDecorrelation)
- **Modified**: `datalog/executor/query_executor.go` (added decision point)

### Next Steps

1. Benchmark performance on real OHLC queries
2. Compare to old executor to validate performance targets
3. Consider adding parallel execution for batched subqueries
4. Consider query merging if performance gap is too large
5. Document when to use decorrelation vs when to avoid it
