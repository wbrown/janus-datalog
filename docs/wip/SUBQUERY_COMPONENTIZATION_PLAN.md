# Subquery Optimization Componentization Plan (Option C)

**Goal**: Extract subquery optimization logic into reusable, testable components that both legacy Executor and QueryExecutor can use.

**Status**: PLANNED
**Estimated LOC**: ~500 new, ~150 refactored
**Complexity**: Medium

---

## Motivation

### Current Problem
- QueryExecutor.executeSubquery() materializes all subquery results (10.1GB in OHLC benchmark)
- Legacy Executor has optimized execution (batching, parallel, streaming) but it's monolithic
- Two separate implementations with no shared optimization infrastructure

### Desired State
- Reusable components for subquery optimization strategies
- Both executor paths can use the same optimized infrastructure
- Each component has single responsibility and is independently testable
- Foundation for future optimizations (parallel pattern matching, streaming aggregations, etc.)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Subquery Execution                          │
│                                                                 │
│  ┌────────────────┐  ┌──────────────────┐  ┌────────────────┐ │
│  │   Strategy     │  │  SubqueryBatcher │  │  Parallel      │ │
│  │   Selector     │  │                  │  │  Executor      │ │
│  └────────────────┘  └──────────────────┘  └────────────────┘ │
│          │                    │                     │          │
│          └────────────────────┴─────────────────────┘          │
│                              │                                  │
│                    ┌─────────▼──────────┐                      │
│                    │  UnionBuilder      │                      │
│                    │  (streaming/mat.)  │                      │
│                    └────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

**Key Principle**: Each component does ONE thing and can be tested independently.

---

## Component Specifications

### 1. SubqueryStrategySelector
**File**: `datalog/executor/subquery_strategy.go`
**Purpose**: Select optimal execution strategy based on query structure and configuration

```go
package executor

type SubqueryExecutionStrategy int

const (
	StrategyBatched   SubqueryExecutionStrategy = iota // RelationInput - execute once with all inputs
	StrategyParallel                                    // >= 100 inputs - worker pool
	StrategySequential                                  // < 100 inputs - simple loop
)

type SubqueryStrategySelector struct {
	parallelThreshold int
}

func NewSubqueryStrategySelector(parallelThreshold int) *SubqueryStrategySelector {
	if parallelThreshold <= 0 {
		parallelThreshold = 100 // Default
	}
	return &SubqueryStrategySelector{
		parallelThreshold: parallelThreshold,
	}
}

// SelectStrategy chooses the optimal execution strategy
func (s *SubqueryStrategySelector) SelectStrategy(
	subq *query.Query,
	inputCount int,
	opts ExecutorOptions,
) SubqueryExecutionStrategy {
	// Check if subquery can be batched (has RelationInput in :in clause)
	if CanBatchSubquery(subq) {
		return StrategyBatched
	}

	// Check if parallel execution is enabled and threshold is met
	if opts.EnableParallelSubqueries && inputCount >= s.parallelThreshold {
		return StrategyParallel
	}

	return StrategySequential
}

// CanBatchSubquery checks if query accepts RelationInput (:in $ [[?sym] ...])
func CanBatchSubquery(q *query.Query) bool {
	hasDatabase := false
	for _, input := range q.In {
		switch input.(type) {
		case query.DatabaseInput:
			hasDatabase = true
		case query.RelationInput:
			return hasDatabase // Must have database before relation
		}
	}
	return false
}
```

**Tests**:
- `TestStrategySelector_Batched` - Detects RelationInput
- `TestStrategySelector_Parallel` - Respects threshold and flag
- `TestStrategySelector_Sequential` - Default path

---

### 2. SubqueryBatcher
**File**: `datalog/executor/subquery_batcher.go`
**Purpose**: Build batched input relation from multiple input combinations

```go
package executor

import "github.com/wbrown/janus-datalog/datalog/query"

type SubqueryBatcher struct{}

func NewSubqueryBatcher() *SubqueryBatcher {
	return &SubqueryBatcher{}
}

// BuildBatchedInput creates a single relation containing all input combinations
// This relation is passed as RelationInput to the subquery
func (b *SubqueryBatcher) BuildBatchedInput(
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
) Relation {
	if len(combinations) == 0 {
		return NewMaterializedRelation(inputSymbols, []Tuple{})
	}

	// Filter to only the symbols we're passing (exclude $)
	var columns []query.Symbol
	for _, sym := range inputSymbols {
		if sym != "$" {
			columns = append(columns, sym)
		}
	}

	// Build tuples from all combinations
	var tuples []Tuple
	for _, values := range combinations {
		tuple := make(Tuple, len(columns))
		for i, col := range columns {
			if val, ok := values[col]; ok {
				tuple[i] = val
			}
		}
		tuples = append(tuples, tuple)
	}

	return NewMaterializedRelation(columns, tuples)
}

// ExtractInputSymbols extracts variable symbols from subquery inputs (excludes $ and constants)
func (b *SubqueryBatcher) ExtractInputSymbols(inputs []query.Input) []query.Symbol {
	var symbols []query.Symbol
	for _, input := range inputs {
		switch inp := input.(type) {
		case query.Variable:
			symbols = append(symbols, inp.Name)
		case query.Constant:
			// Check if it's the database marker
			if sym, ok := inp.Value.(query.Symbol); ok && sym == "$" {
				symbols = append(symbols, sym)
			}
		}
	}
	return symbols
}
```

**Tests**:
- `TestBatcher_BuildBatchedInput` - Creates correct relation
- `TestBatcher_EmptyInput` - Handles empty combinations
- `TestBatcher_ExtractInputSymbols` - Filters $ and constants correctly

---

### 3. ParallelExecutor (Generic!)
**File**: `datalog/executor/parallel_executor.go`
**Purpose**: Generic parallel execution framework with worker pool

```go
package executor

import (
	"context"
	"runtime"
	"sync"
)

// ParallelExecutor provides generic parallel execution with worker pool
// This is intentionally generic so it can be reused for:
// - Parallel subquery execution
// - Parallel pattern matching (future)
// - Parallel aggregations (future)
// - Any embarrassingly parallel operation
type ParallelExecutor struct {
	workerCount int
}

func NewParallelExecutor(workerCount int) *ParallelExecutor {
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
	}
	return &ParallelExecutor{
		workerCount: workerCount,
	}
}

// ExecuteParallel executes operation on all inputs using worker pool
// Returns results in same order as inputs (order-preserving)
func (p *ParallelExecutor) ExecuteParallel(
	ctx context.Context,
	inputs []interface{},
	operation func(context.Context, interface{}) (interface{}, error),
) ([]interface{}, error) {
	if len(inputs) == 0 {
		return []interface{}{}, nil
	}

	// Create result slice with same length as inputs
	results := make([]interface{}, len(inputs))
	errors := make([]error, len(inputs))

	// Create job channel
	jobs := make(chan int, len(inputs))

	// Worker pool
	var wg sync.WaitGroup
	for w := 0; w < p.workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				// Check context cancellation
				select {
				case <-ctx.Done():
					errors[idx] = ctx.Err()
					return
				default:
				}

				// Execute operation
				result, err := operation(ctx, inputs[idx])
				results[idx] = result
				errors[idx] = err
			}
		}()
	}

	// Enqueue all jobs
	for i := range inputs {
		jobs <- i
	}
	close(jobs)

	// Wait for completion
	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// GetWorkerCount returns the number of workers
func (p *ParallelExecutor) GetWorkerCount() int {
	return p.workerCount
}
```

**Tests**:
- `TestParallelExecutor_OrderPreserving` - Results in correct order
- `TestParallelExecutor_ErrorHandling` - Stops on first error
- `TestParallelExecutor_ContextCancellation` - Respects context
- `TestParallelExecutor_WorkerCount` - Uses correct number of workers

---

### 4. StreamingUnionBuilder
**File**: `datalog/executor/streaming_union.go`
**Purpose**: Combine multiple relations efficiently (streaming or materialized)

```go
package executor

type StreamingUnionBuilder struct {
	opts ExecutorOptions
}

func NewStreamingUnionBuilder(opts ExecutorOptions) *StreamingUnionBuilder {
	return &StreamingUnionBuilder{opts: opts}
}

// Union combines multiple relations into one
// Uses streaming if enabled, otherwise materializes
func (s *StreamingUnionBuilder) Union(relations []Relation) Relation {
	if len(relations) == 0 {
		return nil
	}
	if len(relations) == 1 {
		return relations[0]
	}

	// Check if streaming is enabled
	if s.opts.UseStreamingSubqueryUnion {
		return s.unionStreaming(relations)
	}
	return s.unionMaterialized(relations)
}

// unionStreaming creates a streaming union via channel
func (s *StreamingUnionBuilder) unionStreaming(relations []Relation) Relation {
	columns := relations[0].Columns()

	// Create channel for streaming
	unionChan := make(chan relationItem, 1)

	go func() {
		defer close(unionChan)
		for _, rel := range relations {
			unionChan <- relationItem{relation: rel}
		}
	}()

	return NewUnionRelation(unionChan, columns, s.opts)
}

// unionMaterialized combines all relations by materializing
func (s *StreamingUnionBuilder) unionMaterialized(relations []Relation) Relation {
	columns := relations[0].Columns()
	var allTuples []Tuple

	for _, rel := range relations {
		it := rel.Iterator()
		defer it.Close()
		for it.Next() {
			allTuples = append(allTuples, it.Tuple())
		}
	}

	return NewMaterializedRelation(columns, allTuples)
}
```

**Tests**:
- `TestUnionBuilder_Streaming` - Uses channel-based union
- `TestUnionBuilder_Materialized` - Materializes all tuples
- `TestUnionBuilder_SingleRelation` - Returns relation unchanged
- `TestUnionBuilder_Empty` - Handles empty input

---

## Integration Plan

### Phase 1: Create Components (No Integration)
**Goal**: Build and test each component independently

1. Create `subquery_strategy.go` with tests
2. Create `subquery_batcher.go` with tests
3. Create `parallel_executor.go` with tests
4. Create `streaming_union.go` with tests

**Exit Criteria**: All component tests pass, 100% coverage on new code

---

### Phase 2: Integrate with QueryExecutor
**Goal**: Make QueryExecutor use the new components

**Changes to** `query_executor.go`:

```go
type DefaultQueryExecutor struct {
	matcher          PatternMatcher
	options          ExecutorOptions
	strategySelector *SubqueryStrategySelector
	batcher          *SubqueryBatcher
	parallel         *ParallelExecutor
	unionBuilder     *StreamingUnionBuilder
}

func NewQueryExecutor(matcher PatternMatcher, options ExecutorOptions) *DefaultQueryExecutor {
	return &DefaultQueryExecutor{
		matcher:          matcher,
		options:          options,
		strategySelector: NewSubqueryStrategySelector(100),
		batcher:          NewSubqueryBatcher(),
		parallel:         NewParallelExecutor(options.MaxSubqueryWorkers),
		unionBuilder:     NewStreamingUnionBuilder(options),
	}
}

func (e *DefaultQueryExecutor) executeSubquery(
	ctx Context,
	subq *query.SubqueryPattern,
	groups []Relation,
) (Relation, error) {
	// Combine groups and extract input combinations (existing logic)
	combinedRel := e.combineGroups(groups)
	inputSymbols := e.batcher.ExtractInputSymbols(subq.Inputs)
	inputCombinations := getUniqueInputCombinations(combinedRel, inputSymbols)

	// Select strategy
	strategy := e.strategySelector.SelectStrategy(
		subq.Query,
		len(inputCombinations),
		e.options,
	)

	// Execute based on strategy
	switch strategy {
	case StrategyBatched:
		return e.executeBatched(ctx, subq, inputCombinations, inputSymbols)

	case StrategyParallel:
		return e.executeParallel(ctx, subq, inputCombinations, inputSymbols)

	case StrategySequential:
		return e.executeSequential(ctx, subq, inputCombinations, inputSymbols)
	}

	return nil, fmt.Errorf("unknown strategy: %v", strategy)
}

func (e *DefaultQueryExecutor) executeBatched(
	ctx Context,
	subq *query.SubqueryPattern,
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
) (Relation, error) {
	// Build batched input using batcher
	batchedInput := e.batcher.BuildBatchedInput(combinations, inputSymbols)

	// Execute once with batched input
	inputRelations := []Relation{batchedInput}
	nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
	if err != nil {
		return nil, err
	}

	// Apply binding form and return
	if len(nestedGroups) != 1 {
		return nil, fmt.Errorf("batched subquery returned %d groups, expected 1", len(nestedGroups))
	}

	return applyBindingForm(nestedGroups[0], subq.Binding, nil, inputSymbols)
}

func (e *DefaultQueryExecutor) executeParallel(
	ctx Context,
	subq *query.SubqueryPattern,
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
) (Relation, error) {
	// Convert combinations to []interface{} for parallel executor
	inputs := make([]interface{}, len(combinations))
	for i, combo := range combinations {
		inputs[i] = combo
	}

	// Execute in parallel
	results, err := e.parallel.ExecuteParallel(
		ctx,
		inputs,
		func(ctx context.Context, input interface{}) (interface{}, error) {
			inputValues := input.(map[query.Symbol]interface{})

			// Create input relations and execute
			inputRelations := createInputRelationsForSubquery(subq, inputValues)
			nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
			if err != nil {
				return nil, err
			}

			if len(nestedGroups) != 1 {
				return nil, fmt.Errorf("subquery returned %d groups, expected 1", len(nestedGroups))
			}

			// Apply binding form
			return applyBindingForm(nestedGroups[0], subq.Binding, inputValues, inputSymbols)
		},
	)

	if err != nil {
		return nil, err
	}

	// Convert results back to []Relation
	relations := make([]Relation, len(results))
	for i, r := range results {
		relations[i] = r.(Relation)
	}

	// Union results
	return e.unionBuilder.Union(relations), nil
}

func (e *DefaultQueryExecutor) executeSequential(
	ctx Context,
	subq *query.SubqueryPattern,
	combinations []map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
) (Relation, error) {
	var results []Relation

	for _, inputValues := range combinations {
		// Create input relations and execute
		inputRelations := createInputRelationsForSubquery(subq, inputValues)
		nestedGroups, err := e.Execute(ctx, subq.Query, inputRelations)
		if err != nil {
			return nil, err
		}

		if len(nestedGroups) != 1 {
			return nil, fmt.Errorf("subquery returned %d groups, expected 1", len(nestedGroups))
		}

		// Apply binding form
		boundResult, err := applyBindingForm(nestedGroups[0], subq.Binding, inputValues, inputSymbols)
		if err != nil {
			return nil, err
		}

		results = append(results, boundResult)
	}

	// Union results
	return e.unionBuilder.Union(results), nil
}
```

**Exit Criteria**:
- OHLC benchmark memory drops from 5.5GB to < 2GB
- All QueryExecutor tests pass
- Benchmarks show performance improvement

---

### Phase 3: Optionally Refactor Legacy Executor
**Goal**: Legacy Executor can use components too (optional)

**Changes to** `subquery.go`:

Replace monolithic functions with component-based approach:

```go
func ExecuteSubquery(
	ctx Context,
	parentExec *Executor,
	subqPlan planner.SubqueryPlan,
	inputRelation Relation,
) (Relation, error) {
	// Initialize components (could be cached on Executor)
	selector := NewSubqueryStrategySelector(ParallelSubqueryThreshold)
	batcher := NewSubqueryBatcher()
	parallel := NewParallelExecutor(parentExec.maxSubqueryWorkers)
	unionBuilder := NewStreamingUnionBuilder(parentExec.options)

	// Get input combinations
	inputCombinations := getUniqueInputCombinations(inputRelation, subqPlan.Inputs)
	inputSymbols := batcher.ExtractInputSymbols(subqPlan.Subquery.Inputs)

	// Select strategy
	strategy := selector.SelectStrategy(
		subqPlan.Subquery.Query,
		len(inputCombinations),
		parentExec.options,
	)

	// Delegate to strategy-specific helper
	// (Similar structure to QueryExecutor)
}
```

**Note**: This phase is OPTIONAL. If legacy Executor is being deprecated, skip this.

**Exit Criteria**: Legacy Executor tests still pass, uses new components

---

## Testing Strategy

### Unit Tests (Per Component)
Each component gets comprehensive unit tests:

- `subquery_strategy_test.go` - Strategy selection logic
- `subquery_batcher_test.go` - Batch input construction
- `parallel_executor_test.go` - Concurrency, error handling, ordering
- `streaming_union_test.go` - Streaming vs materialized behavior

**Target**: 100% line coverage on new code

### Integration Tests
Test QueryExecutor with all strategies:

```go
func TestQueryExecutor_SubqueryStrategies(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		strategy SubqueryExecutionStrategy
		expected int
	}{
		{
			name:     "batched_with_relation_input",
			query:    `[:find ?ticker ?max :where ... [(q [...] $ [[?sym] ...]) [[?max]]]]`,
			strategy: StrategyBatched,
		},
		{
			name:     "parallel_with_many_inputs",
			query:    `[:find ?ticker ?max :where ... [(q [...] $ ?sym) [[?max]]]]`,
			strategy: StrategyParallel,
		},
		// ...
	}
}
```

### Performance Tests
Compare before/after on OHLC benchmark:

```go
func BenchmarkOHLC_BeforeComponentization(b *testing.B) {
	// Old monolithic path
}

func BenchmarkOHLC_AfterComponentization(b *testing.B) {
	// New component-based path
}
```

**Target**:
- Memory: 5.5GB → < 2GB (60% reduction)
- Time: No regression (within 5%)

---

## Rollout Plan

### Step 1: Create Components (Week 1)
- Implement 4 component files
- Write comprehensive unit tests
- Verify components work in isolation

**Deliverable**: 4 new files, ~500 LOC, all tests green

### Step 2: Integrate with QueryExecutor (Week 2)
- Refactor `QueryExecutor.executeSubquery()`
- Add strategy-specific helper methods
- Run full test suite
- Run OHLC benchmark

**Deliverable**: QueryExecutor uses components, OHLC memory < 2GB

### Step 3: Documentation and Cleanup (Week 3)
- Update ARCHITECTURE.md
- Add component usage examples
- Document when to use each strategy
- Clean up old code (if deprecating legacy Executor)

**Deliverable**: Docs updated, code clean

---

## Success Criteria

1. **Performance**: OHLC memory drops from 5.5GB to < 2GB
2. **Correctness**: All existing tests pass
3. **Reusability**: ParallelExecutor can be used for non-subquery operations
4. **Testability**: 100% coverage on new component code
5. **Maintainability**: Each component < 200 LOC, single responsibility

---

## Future Extensions

Once components exist:

1. **Parallel Pattern Matching**: Use ParallelExecutor for multi-shard storage
2. **Streaming Aggregations**: Use StreamingUnionBuilder for incremental aggregates
3. **Query Caching**: Cache results from ParallelExecutor
4. **Adaptive Strategies**: Collect stats to tune parallelThreshold automatically

---

## Open Questions

1. Should we cache component instances on Executor/QueryExecutor?
   - **Proposal**: Yes, create once in constructor

2. Should ParallelExecutor support generic types (Go 1.18+)?
   - **Proposal**: Use interface{} for compatibility, can add generics later

3. Should we extract more utilities (e.g., getUniqueInputCombinations)?
   - **Proposal**: Yes, but do it incrementally as we see duplication

---

## Risk Mitigation

**Risk**: Components add abstraction overhead
**Mitigation**: Benchmark performance, ensure no regression

**Risk**: Incomplete migration leaves two code paths
**Mitigation**: Feature flag for new path, comprehensive testing

**Risk**: Components too generic, hard to use
**Mitigation**: Clear interfaces, good documentation, usage examples

**Risk**: Breaking changes to existing code
**Mitigation**: Components are new files, existing code unchanged until Phase 2
