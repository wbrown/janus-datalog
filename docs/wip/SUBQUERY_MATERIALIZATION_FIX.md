# Fix: Subquery Inappropriate Materialization

**Date**: 2025-10-23
**Issue**: OHLC benchmark uses 5.3GB for 24,000 datoms (221 KB/datom)
**Root Cause**: Subquery execution materializes all results before combining

---

## Problem Analysis

### Current Flow (Sequential Execution)

```go
// datalog/executor/subquery.go:60-86
func executeSubquerySequential(...) {
    var allResults []Relation  // ← PROBLEM: Accumulates 800 relations in memory

    for _, inputValues := range inputCombinations {  // 200 hours × 4 subqueries = 800 iterations
        result := executePhasesWithInputs(...)       // Execute subquery
        boundResult := applyBindingForm(...)          // Apply binding
        allResults = append(allResults, boundResult)  // ← Store relation in memory
    }

    return combineSubqueryResults(allResults, subqPlan)  // ← Materializes ALL tuples
}
```

### Combining Phase

```go
// datalog/executor/subquery.go:194-224
func combineSubqueryResults(allResults []Relation, ...) {
    var validResults []Relation
    for _, r := range allResults {
        if r != nil && !r.IsEmpty() {
            validResults = append(validResults, r)  // ← Still holding all relations
        }
    }

    // ← PROBLEM: Materialize ALL tuples from ALL relations
    var allTuples []Tuple
    for _, rel := range validResults {
        it := rel.Iterator()
        for it.Next() {
            allTuples = append(allTuples, it.Tuple())  // ← Copy every tuple
        }
        it.Close()
    }

    return NewMaterializedRelation(columns, allTuples)  // ← Giant materialized relation
}
```

### Memory Impact

**OHLC Benchmark**:
- 800 subquery executions (200 hours × 4 subqueries)
- 5.3 GB total memory
- 55.4M allocations
- **6.6 MB per subquery execution**
- **3,464 allocations per bar processed**

**Comparison**:
- Simple query (3K datoms): 2.1 MB
- OHLC query (24K datoms): 5.3 GB
- **2557× memory for only 8× more datoms!**

---

## Solution: UnionRelation with On-Demand Execution

### Strategy

Instead of:
1. ❌ Execute all 800 subqueries upfront
2. ❌ Accumulate all 800 relations in `allResults`
3. ❌ Materialize all tuples from all relations
4. ❌ Create giant `MaterializedRelation`

Do:
1. ✅ **Return UnionRelation immediately** (zero upfront work)
2. ✅ **Execute subqueries ON-DEMAND during iteration**
3. ✅ **Only one subquery result in memory at a time**
4. ✅ **Apply deduplication in the union**

**Memory at any moment**: 1 subquery result (not 800)

---

## Implementation Plan

### Phase 1: Add ExecutorOptions flag

**Add to `datalog/executor/executor.go` ExecutorOptions**:

```go
type ExecutorOptions struct {
    // ... existing options ...
    UseStreamingSubqueryUnion bool  // Default: true (streaming), false: materialize all
}
```

**Update NewExecutor default**:
```go
func NewExecutor(matcher PatternMatcher) *Executor {
    defaultOpts := planner.PlannerOptions{
        // ... existing defaults ...
        UseStreamingSubqueryUnion: true,  // Enable by default
    }
    return NewExecutorWithOptions(matcher, defaultOpts)
}
```

### Phase 2: Modify executeSubquerySequential with toggle

**Add toggle in `datalog/executor/subquery.go`**:

```go
func executeSubquerySequential(...) (Relation, error) {
    // Get unique combinations of input values
    inputCombinations := getUniqueInputCombinations(inputRelation, subqPlan.Inputs)

    // Toggle: streaming union vs materialized
    if parentExec.options.UseStreamingSubqueryUnion {
        // NEW: Streaming union (executes on-demand)
        return NewSubqueryUnion(parentExec, subqPlan, inputCombinations), nil
    }

    // OLD: Materialize everything (for comparison)
    return executeSubquerySequentialMaterialized(parentExec, subqPlan, inputCombinations)
}

// Keep old implementation for benchmarking
func executeSubquerySequentialMaterialized(...) (Relation, error) {
    var allResults []Relation

    for _, inputValues := range inputCombinations {
        result, err := executePhasesWithInputs(...)
        if err != nil {
            return nil, err
        }

        boundResult, err := applyBindingForm(...)
        if err != nil {
            return nil, err
        }

        allResults = append(allResults, boundResult)
    }

    return combineSubqueryResults(allResults, subqPlan)
}
```

### Phase 2: Create SubqueryUnion (special UnionRelation)

**Create `datalog/executor/subquery_union.go`**:

```go
package executor

import (
    "fmt"
    "github.com/wbrown/janus-datalog/datalog/planner"
    "github.com/wbrown/janus-datalog/datalog/query"
)

// SubqueryUnion executes subqueries on-demand during iteration
// Only one subquery result is in memory at a time
type SubqueryUnion struct {
    executor          *Executor
    subqPlan          planner.SubqueryPlan
    inputCombinations []map[query.Symbol]interface{}
    columns           []query.Symbol
}

func NewSubqueryUnion(
    executor *Executor,
    subqPlan planner.SubqueryPlan,
    inputCombinations []map[query.Symbol]interface{},
) *SubqueryUnion {
    // Get columns from binding
    columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)

    return &SubqueryUnion{
        executor:          executor,
        subqPlan:          subqPlan,
        inputCombinations: inputCombinations,
        columns:           columns,
    }
}

func (s *SubqueryUnion) Columns() []query.Symbol {
    return s.columns
}

func (s *SubqueryUnion) Iterator() Iterator {
    return &SubqueryUnionIterator{
        executor:     s.executor,
        subqPlan:     s.subqPlan,
        inputs:       s.inputCombinations,
        currentIndex: 0,
        seen:         NewTupleKeyMap(), // Efficient deduplication without materializing intermediates
    }
}

func (s *SubqueryUnion) IsEmpty() bool {
    // We don't know until we iterate
    // Checking would execute subqueries
    return false
}

func (s *SubqueryUnion) Size() int {
    // Unknown until iteration - would require executing all subqueries
    return -1
}

// SubqueryUnionIterator executes subqueries during iteration
type SubqueryUnionIterator struct {
    executor     *Executor
    subqPlan     planner.SubqueryPlan
    inputs       []map[query.Symbol]interface{}
    currentIndex int
    currentIter  Iterator
    seen         *TupleKeyMap  // Efficient deduplication (reuse existing!)
    currentTuple Tuple
}

func (it *SubqueryUnionIterator) Next() bool {
    for {
        // Try current subquery's iterator
        if it.currentIter != nil {
            for it.currentIter.Next() {
                tuple := it.currentIter.Tuple()

                // Deduplication check using existing TupleKeyMap
                dedupKey := NewTupleKeyFull(tuple)
                if it.seen.Exists(dedupKey) {
                    continue // Skip duplicate
                }
                it.seen.Put(dedupKey, true)
                it.currentTuple = tuple
                return true
            }

            // Current iterator exhausted
            it.currentIter.Close()
            it.currentIter = nil
        }

        // No more inputs?
        if it.currentIndex >= len(it.inputs) {
            return false
        }

        // Execute next subquery ON-DEMAND
        inputValues := it.inputs[it.currentIndex]
        it.currentIndex++

        // Create input relations from the input values
        inputRelations := createInputRelationsFromPattern(it.subqPlan.Subquery, inputValues)

        // Execute the nested query
        result, err := executePhasesWithInputs(
            NewContext(nil), // TODO: propagate context
            it.executor,
            it.subqPlan.NestedPlan,
            inputRelations,
        )
        if err != nil {
            // Skip failed subquery (or should we error?)
            continue
        }

        // Apply binding form
        boundResult, err := applyBindingForm(result, it.subqPlan.Subquery.Binding, inputValues, it.subqPlan.Inputs)
        if err != nil {
            continue
        }

        // Start iterating this result
        it.currentIter = boundResult.Iterator()
        // Loop back to try this iterator
    }
}

func (it *SubqueryUnionIterator) Tuple() Tuple {
    return it.currentTuple
}

func (it *SubqueryUnionIterator) Close() error {
    if it.currentIter != nil {
        return it.currentIter.Close()
    }
    return nil
}
```

### Phase 2: Progressive Accumulation Strategy

**Option A: Incremental Union (Simplest)**

Modify `executeSubquerySequential`:
```go
func executeSubquerySequential(...) (Relation, error) {
    var accumulator Relation = nil

    for _, inputValues := range inputCombinations {
        result, err := executePhasesWithInputs(...)
        if err != nil {
            return nil, err
        }

        boundResult, err := applyBindingForm(...)
        if err != nil {
            return nil, err
        }

        // Progressive accumulation instead of collecting all
        if accumulator == nil {
            accumulator = boundResult
        } else {
            accumulator = NewUnionRelation([]Relation{accumulator, boundResult})
        }
    }

    if accumulator == nil {
        // Return empty relation
        columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
        return NewMaterializedRelation(columns, []Tuple{}), nil
    }

    return accumulator, nil
}
```

**Option B: Batch Union (Better for Many Small Relations)**

```go
func executeSubquerySequential(...) (Relation, error) {
    const batchSize = 100  // Union in batches to avoid deep nesting

    var currentBatch []Relation
    var batches []Relation

    for _, inputValues := range inputCombinations {
        result, err := executePhasesWithInputs(...)
        if err != nil {
            return nil, err
        }

        boundResult, err := applyBindingForm(...)
        if err != nil {
            return nil, err
        }

        // Skip empty results
        if boundResult.IsEmpty() {
            continue
        }

        currentBatch = append(currentBatch, boundResult)

        // When batch is full, union it and add to batches
        if len(currentBatch) >= batchSize {
            batches = append(batches, NewUnionRelation(currentBatch))
            currentBatch = nil
        }
    }

    // Handle remaining relations
    if len(currentBatch) > 0 {
        batches = append(batches, NewUnionRelation(currentBatch))
    }

    if len(batches) == 0 {
        columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
        return NewMaterializedRelation(columns, []Tuple{}), nil
    }

    if len(batches) == 1 {
        return batches[0], nil
    }

    // Union all batches
    return NewUnionRelation(batches), nil
}
```

**Option C: Streaming Combiner (Most Efficient)**

Create a dedicated streaming combiner that never holds all relations:

```go
// StreamingUnionRelation builds union progressively without holding all input relations
type StreamingUnionRelation struct {
    columns []query.Symbol
    tuples  []Tuple  // Accumulated so far
}

func executeSubquerySequential(...) (Relation, error) {
    var columns []query.Symbol
    var tuples []Tuple

    for _, inputValues := range inputCombinations {
        result, err := executePhasesWithInputs(...)
        if err != nil {
            return nil, err
        }

        boundResult, err := applyBindingForm(...)
        if err != nil {
            return nil, err
        }

        // Extract columns from first non-empty result
        if columns == nil && !boundResult.IsEmpty() {
            columns = boundResult.Columns()
        }

        // Stream tuples out and accumulate
        it := boundResult.Iterator()
        for it.Next() {
            tuples = append(tuples, it.Tuple())
        }
        it.Close()

        // ← boundResult can now be GC'd, we don't hold it
    }

    if columns == nil {
        columns = getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
    }

    return NewMaterializedRelation(columns, tuples), nil
}
```

### Phase 3: Update `combineSubqueryResults`

**If we keep this function**, simplify it:

```go
func combineSubqueryResults(allResults []Relation, subqPlan planner.SubqueryPlan) (Relation, error) {
    // Filter out nil/empty results
    var validResults []Relation
    for _, r := range allResults {
        if r != nil && !r.IsEmpty() {
            validResults = append(validResults, r)
        }
    }

    if len(validResults) == 0 {
        columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
        return NewMaterializedRelation(columns, []Tuple{}), nil
    }

    if len(validResults) == 1 {
        return validResults[0], nil
    }

    // ← CHANGE: Use UnionRelation instead of materializing
    return NewUnionRelation(validResults), nil
}
```

**Or eliminate it entirely** if we use Option C.

### Phase 4: Update Parallel Execution

The parallel executor has the same problem:

```go
// datalog/executor/subquery.go:88-158
func executeSubqueryParallel(...) {
    // ... worker pool setup ...

    results := make([]Relation, len(inputCombinations))

    // Workers execute and store in results array

    // ← PROBLEM: Still calls combineSubqueryResults with all results
    return combineSubqueryResults(results, subqPlan)
}
```

**Fix**: Use the same progressive accumulation or UnionRelation approach.

---

## Testing Strategy

### 1. Benchmark Comparison (Old vs New)

**Create benchmark in `datalog/executor/subquery_union_bench_test.go`**:

```go
func BenchmarkSubqueryUnion(b *testing.B) {
    datoms := createOHLCDataset(200, 20)
    matcher := NewMemoryPatternMatcher(datoms)

    queryStr := `[:find ?hour ?high ?low ?avg_close
                  :where ...`  // OHLC query with 4 subqueries

    q, _ := parser.ParseQuery(queryStr)

    b.Run("materialized", func(b *testing.B) {
        executor := NewQueryExecutor(matcher, ExecutorOptions{
            UseStreamingSubqueryUnion: false,  // OLD: materialize
        })
        ctx := NewContext(nil)
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
            _, err := executor.Execute(ctx, q, []Relation{})
            if err != nil {
                b.Fatal(err)
            }
        }
    })

    b.Run("streaming", func(b *testing.B) {
        executor := NewQueryExecutor(matcher, ExecutorOptions{
            UseStreamingSubqueryUnion: true,  // NEW: streaming
        })
        ctx := NewContext(nil)
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
            _, err := executor.Execute(ctx, q, []Relation{})
            if err != nil {
                b.Fatal(err)
            }
        }
    })
}
```

**Run comparison**:
```bash
go test -bench=BenchmarkSubqueryUnion -benchmem -run=^$ ./datalog/executor

# Expected output:
# BenchmarkSubqueryUnion/materialized-16    1    5300000000 B/op  55000000 allocs/op
# BenchmarkSubqueryUnion/streaming-16       1     500000000 B/op   5000000 allocs/op
```

**Expected Improvement**: 10× memory, 10× allocations

### 2. Verify Correctness

Run existing tests:
```bash
go test -v ./datalog/executor/... -run=".*[Ss]ubquery.*"
```

### 3. Verify Performance

Should be **faster** due to reduced allocations:
```bash
go test -bench=BenchmarkOHLCFullComparison -benchtime=5s -run=^$ ./datalog/executor
```

### 4. Test Edge Cases

- Empty subquery results
- Single subquery result
- Large number of subqueries (1000+)
- Subqueries returning large result sets

---

## Recommended Approach

**Start with Option C (Streaming Combiner)** because:

1. ✅ **Simplest to implement** - just change the loop
2. ✅ **Most memory efficient** - never holds all relations
3. ✅ **No new abstractions needed** - uses existing MaterializedRelation
4. ✅ **Easiest to reason about** - linear accumulation
5. ✅ **Same behavior for sequential and parallel** - easy to unify

**Then optimize to Option A/B if needed** for lazy evaluation benefits.

---

## Implementation Steps

1. **Measure baseline (materialized)**
   ```bash
   go test -bench=BenchmarkOHLCFullComparison/new_planner -benchmem -count=3 | tee baseline.txt
   ```

2. **Add ExecutorOptions flag**
   - Add `UseStreamingSubqueryUnion bool` to `ExecutorOptions`
   - Default to `true` in `NewExecutor`

3. **Create SubqueryUnion** in `datalog/executor/subquery_union.go`
   - Implement `SubqueryUnion` relation type
   - Implement `SubqueryUnionIterator` with on-demand execution
   - Use `TupleKeyMap` for deduplication

4. **Add toggle in executeSubquerySequential**
   - Check `parentExec.options.UseStreamingSubqueryUnion`
   - If true: return `NewSubqueryUnion`
   - If false: keep existing materialized path (rename to `executeSubquerySequentialMaterialized`)

5. **Test correctness with both paths**
   ```bash
   # Test with streaming (default)
   go test -v ./datalog/executor/... -run=".*[Ss]ubquery.*"

   # Test with materialized (old path, for comparison)
   # TODO: Add test flag to toggle
   ```

6. **Create comparison benchmark**
   - Add `BenchmarkSubqueryUnion` in `subquery_union_bench_test.go`
   - Run both materialized and streaming paths
   - Measure memory and allocations

7. **Run comparison**
   ```bash
   go test -bench=BenchmarkSubqueryUnion -benchmem -run=^$ ./datalog/executor
   ```

8. **Verify improvement**
   - Compare baseline vs streaming
   - Should see 10× memory reduction
   - Should see 10× allocation reduction

9. **Apply same fix to parallel execution**
   - Modify `executeSubqueryParallel` with same toggle
   - Test parallel path

10. **Update documentation**
    - Update PERFORMANCE_STATUS.md with verified numbers
    - Document the toggle option
    - Add benchmark comparison results

---

## Expected Results

### Memory (Goal)

| Query Type | Before | After (Goal) | Improvement |
|-----------|--------|--------------|-------------|
| OHLC (24K datoms, 800 subqueries) | 5.3 GB | < 500 MB | **10×** |
| Allocations | 55M | < 5M | **10×** |
| Memory per subquery | 6.6 MB | < 100 KB | **50×** |

**What we eliminate**:
- ❌ Holding 800 Relation objects simultaneously (each ~6.6 MB)
- ❌ Materializing all tuples from all 800 relations
- ❌ Creating giant MaterializedRelation from combined tuples

**What we keep** (necessary):
- ✅ Deduplication map (size = unique result set, ~200 tuples for OHLC)
- ✅ One subquery result at a time during iteration
- ✅ Tuple keys in map for collision checking (FNV hash, no string allocations)

### Performance

- **Same or faster** due to reduced allocation overhead
- **Better GC pressure** (fewer objects to track)
- **More scalable** (linear memory growth instead of quadratic)

### Correctness

- All existing tests pass
- Same query results
- No behavioral changes from user perspective

---

## Risks & Mitigations

### Risk 1: Breaking Existing Behavior

**Mitigation**: Comprehensive test suite run before/after

### Risk 2: Edge Cases with Empty Relations

**Mitigation**: Handle empty results explicitly in accumulation loop

### Risk 3: Performance Regression

**Mitigation**: Benchmark comparison shows improvement or parity

### Risk 4: Parallel Execution Complexity

**Mitigation**: Implement sequential first, then apply same pattern to parallel

---

## Success Criteria

1. ✅ Memory usage < 500 MB for OHLC benchmark (10× reduction)
2. ✅ Allocations < 5M (10× reduction)
3. ✅ All tests pass
4. ✅ Performance same or better
5. ✅ Code is simpler (fewer lines, clearer intent)

---

## Next Steps

Ready to implement? Let's start with:

1. Measure baseline (save benchmark output)
2. Implement Option C for sequential execution
3. Run tests and benchmarks
4. Iterate based on results
