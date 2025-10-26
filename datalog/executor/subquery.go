package executor

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Note: Parallel subquery settings are now managed by ExecutorOptions

// SubqueryWorkerCount is the number of goroutines to use for parallel subquery execution
// Default is runtime.NumCPU() for optimal CPU utilization
var SubqueryWorkerCount = runtime.NumCPU()

// ParallelSubqueryThreshold is the minimum number of iterations to use parallel execution
// For small iteration counts, sequential execution is faster due to lower overhead
const ParallelSubqueryThreshold = 10

// executeSubquery executes a subquery using the parent executor's optimizations.
func (e *Executor) executeSubquery(ctx Context, subqPlan planner.SubqueryPlan, inputRelation Relation) (Relation, error) {
	// Pass the full executor to ensure optimizations are inherited
	return ExecuteSubquery(ctx, e, subqPlan, inputRelation)
}

// ExecuteSubquery executes a subquery using the parent executor to inherit optimizations.
// This ensures subqueries benefit from parallel execution, predicate pushdown, plan cache, etc.
func ExecuteSubquery(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputRelation Relation) (Relation, error) {
	// CRITICAL: Extract input combinations ONCE before trying batched vs sequential paths
	// Both paths need the combinations, and calling Iterator() twice on a StreamingRelation will panic
	inputCombinations := getUniqueInputCombinations(inputRelation, subqPlan.Inputs)

	// Check if we can batch execute with RelationInput
	if canBatchSubquery(subqPlan.Subquery.Query) {
		// Try batched execution with pre-extracted combinations
		batchedResult, err := executeBatchedSubqueryWithCombinations(ctx, parentExec, subqPlan, inputCombinations)
		if err == nil {
			return batchedResult, nil
		}
		// Fall back to sequential if batching fails
	}

	// Check if we should use parallel execution
	useParallel := parentExec.enableParallelSubqueries && len(inputCombinations) >= ParallelSubqueryThreshold

	if useParallel {
		return executeSubqueryParallel(ctx, parentExec, subqPlan, inputCombinations)
	}

	// Sequential execution (original code)
	return executeSubquerySequential(ctx, parentExec, subqPlan, inputCombinations)
}

// executeSubquerySequential executes subquery iterations sequentially
func executeSubquerySequential(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	// Check if we should use streaming union
	if parentExec.options.UseStreamingSubqueryUnion {
		return executeSubquerySequentialStreaming(ctx, parentExec, subqPlan, inputCombinations)
	}
	return executeSubquerySequentialMaterialized(ctx, parentExec, subqPlan, inputCombinations)
}

// executeSubquerySequentialStreaming executes subqueries sequentially and streams results via channel
func executeSubquerySequentialStreaming(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	// Create buffered channel to avoid blocking producer before consumer starts
	// Buffer size = 1 is enough since we only have one producer goroutine
	unionChan := make(chan relationItem, 1)

	// Start goroutine to produce results
	go func() {
		defer close(unionChan)

		for _, inputValues := range inputCombinations {
			// Create input relations from the input values
			inputRelations := createInputRelationsFromPattern(subqPlan.Subquery, inputValues)

			// Execute the nested query with input relations
			result, err := executePhasesWithInputs(ctx, parentExec, subqPlan.NestedPlan, inputRelations)
			if err != nil {
				unionChan <- relationItem{err: fmt.Errorf("nested query execution failed: %w", err)}
				continue
			}

			// Apply the binding form to the result
			boundResult, err := applyBindingForm(result, subqPlan.Subquery.Binding, inputValues, subqPlan.Inputs)
			if err != nil {
				unionChan <- relationItem{err: fmt.Errorf("binding form application failed: %w", err)}
				continue
			}

			// Send result to union channel
			unionChan <- relationItem{relation: boundResult}
		}
	}()

	// Peek at first result to detect early errors
	firstItem, ok := <-unionChan
	if !ok {
		// No results at all - empty
		columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(columns, []Tuple{}), nil
	}
	if firstItem.err != nil {
		// First result is an error - return it immediately
		return nil, firstItem.err
	}

	// Create a new channel that includes the first item
	newChan := make(chan relationItem, 1)
	newChan <- firstItem // Put first item back
	go func() {
		// Forward remaining items
		for item := range unionChan {
			newChan <- item
		}
		close(newChan)
	}()

	// Return UnionRelation that will consume from channel
	columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
	return NewUnionRelation(newChan, columns, parentExec.options), nil
}

// executeSubquerySequentialMaterialized executes subqueries sequentially and materializes all results
func executeSubquerySequentialMaterialized(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	// Collect results from all subquery executions
	var allResults []Relation

	for _, inputValues := range inputCombinations {
		// Create input relations from the input values
		inputRelations := createInputRelationsFromPattern(subqPlan.Subquery, inputValues)

		// Execute the nested query with input relations using the parent executor
		// This ensures all optimizations are inherited
		result, err := executePhasesWithInputs(ctx, parentExec, subqPlan.NestedPlan, inputRelations)
		if err != nil {
			return nil, fmt.Errorf("nested query execution failed: %w", err)
		}

		// Apply the binding form to the result
		boundResult, err := applyBindingForm(result, subqPlan.Subquery.Binding, inputValues, subqPlan.Inputs)
		if err != nil {
			return nil, fmt.Errorf("binding form application failed: %w", err)
		}

		allResults = append(allResults, boundResult)
	}

	return combineSubqueryResults(allResults, subqPlan)
}

// executeSubqueryParallel executes subquery iterations in parallel using a worker pool
func executeSubqueryParallel(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	// Check if we should use streaming union
	if parentExec.options.UseStreamingSubqueryUnion {
		return executeSubqueryParallelStreaming(ctx, parentExec, subqPlan, inputCombinations)
	}
	return executeSubqueryParallelMaterialized(ctx, parentExec, subqPlan, inputCombinations)
}

// executeSubqueryParallelStreaming executes subqueries in parallel and streams results via channel
func executeSubqueryParallelStreaming(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	numWorkers := SubqueryWorkerCount
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// Create channels
	type workItem struct {
		inputValues map[query.Symbol]interface{}
	}

	workChan := make(chan workItem, len(inputCombinations))
	// Buffered channel to avoid blocking workers
	unionChan := make(chan relationItem, numWorkers)

	// Create cancellable context for early termination on error
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker needs its own context to avoid concurrent map writes
			var workerCtx Context
			if ctx != nil && ctx.Collector() != nil {
				workerCtx = NewContext(ctx.Collector().Handler())
			} else {
				workerCtx = NewContext(nil)
			}

			for work := range workChan {
				// Check for cancellation
				select {
				case <-cancelCtx.Done():
					return
				default:
				}

				// Create input relations from the input values
				inputRelations := createInputRelationsFromPattern(subqPlan.Subquery, work.inputValues)

				// Execute the nested query with input relations
				result, err := executePhasesWithInputs(workerCtx, parentExec, subqPlan.NestedPlan, inputRelations)
				if err != nil {
					unionChan <- relationItem{err: fmt.Errorf("nested query execution failed: %w", err)}
					continue
				}

				// Apply the binding form to the result
				boundResult, err := applyBindingForm(result, subqPlan.Subquery.Binding, work.inputValues, subqPlan.Inputs)
				if err != nil {
					unionChan <- relationItem{err: fmt.Errorf("binding form application failed: %w", err)}
					continue
				}

				// Send result to union channel (non-blocking for worker)
				unionChan <- relationItem{relation: boundResult}
			}
		}()
	}

	// Send work items
	for _, inputValues := range inputCombinations {
		workChan <- workItem{inputValues: inputValues}
	}
	close(workChan)

	// Close union channel when all workers finish
	go func() {
		wg.Wait()
		close(unionChan)
	}()

	// Peek at first result to detect early errors
	// This ensures error tests that expect immediate failures work correctly
	firstItem, ok := <-unionChan
	if !ok {
		// No results at all - empty
		columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(columns, []Tuple{}), nil
	}
	if firstItem.err != nil {
		// First result is an error - return it immediately
		return nil, firstItem.err
	}

	// Create a new channel that includes the first item
	newChan := make(chan relationItem, len(inputCombinations))
	newChan <- firstItem // Put first item back
	go func() {
		// Forward remaining items
		for item := range unionChan {
			newChan <- item
		}
		close(newChan)
	}()

	// Return UnionRelation that will consume from channel
	columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
	return NewUnionRelation(newChan, columns, parentExec.options), nil
}

// executeSubqueryParallelMaterialized executes subqueries in parallel and materializes all results
func executeSubqueryParallelMaterialized(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	numWorkers := SubqueryWorkerCount
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// Create channels for work distribution
	type workItem struct {
		index       int
		inputValues map[query.Symbol]interface{}
	}

	type resultItem struct {
		index  int
		result Relation
		err    error
	}

	workChan := make(chan workItem, len(inputCombinations))
	resultChan := make(chan resultItem, len(inputCombinations))

	// Create cancellable context for early termination on error
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker needs its own context to avoid concurrent map writes
			// Copy the annotation handler from parent context if present
			var workerCtx Context
			if ctx != nil && ctx.Collector() != nil {
				workerCtx = NewContext(ctx.Collector().Handler())
			} else {
				workerCtx = NewContext(nil)
			}

			for work := range workChan {
				// Check for cancellation
				select {
				case <-cancelCtx.Done():
					return
				default:
				}

				// Create input relations from the input values
				inputRelations := createInputRelationsFromPattern(subqPlan.Subquery, work.inputValues)

				// Execute the nested query with input relations using worker's own context
				result, err := executePhasesWithInputs(workerCtx, parentExec, subqPlan.NestedPlan, inputRelations)
				if err != nil {
					resultChan <- resultItem{index: work.index, err: fmt.Errorf("nested query execution failed: %w", err)}
					cancel() // Cancel other workers
					continue
				}

				// Apply the binding form to the result
				boundResult, err := applyBindingForm(result, subqPlan.Subquery.Binding, work.inputValues, subqPlan.Inputs)
				if err != nil {
					resultChan <- resultItem{index: work.index, err: fmt.Errorf("binding form application failed: %w", err)}
					cancel() // Cancel other workers
					continue
				}

				resultChan <- resultItem{index: work.index, result: boundResult}
			}
		}()
	}

	// Send work items
	for i, inputValues := range inputCombinations {
		workChan <- workItem{index: i, inputValues: inputValues}
	}
	close(workChan)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results in order
	results := make([]Relation, len(inputCombinations))
	var firstError error

	for result := range resultChan {
		if result.err != nil && firstError == nil {
			firstError = result.err
		}
		if result.result != nil {
			results[result.index] = result.result
		}
	}

	if firstError != nil {
		return nil, firstError
	}

	return combineSubqueryResults(results, subqPlan)
}

// combineSubqueryResults combines all subquery results into a single relation
func combineSubqueryResults(allResults []Relation, subqPlan planner.SubqueryPlan) (Relation, error) {
	// Filter out nil results (from errors) and empty results (failed pattern matches)
	var validResults []Relation
	for _, r := range allResults {
		if r != nil && !r.IsEmpty() {
			validResults = append(validResults, r)
		}
	}

	// If no results - return empty relation with expected columns
	if len(validResults) == 0 {
		columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(columns, []Tuple{}), nil
	}

	// Union all results by collecting all tuples
	var allTuples []Tuple
	columns := validResults[0].Columns()

	for _, rel := range validResults {
		it := rel.Iterator()
		for it.Next() {
			allTuples = append(allTuples, it.Tuple())
		}
		it.Close()
	}

	result := NewMaterializedRelation(columns, allTuples)

	return result, nil
}

// executePhasesWithInputs executes query phases with additional input relations.
// This function needs the full parent executor to inherit its optimizations.
func executePhasesWithInputs(ctx Context, parentExec *Executor, plan *planner.QueryPlan, inputRelations []Relation) (Relation, error) {
	// Use the parent executor directly to inherit all its optimizations
	// (parallel execution, predicate pushdown, plan cache, etc.)
	return parentExec.executePhasesWithInputs(ctx, plan, inputRelations)
}

// getUniqueInputCombinations extracts unique combinations of input values.
// This is a pure function that performs data transformation.
func getUniqueInputCombinations(rel Relation, inputSymbols []query.Symbol) []map[query.Symbol]interface{} {
	// Find column indices for input symbols
	indices := make([]int, len(inputSymbols))
	for i, sym := range inputSymbols {
		if sym == "$" {
			// Database marker - not a column, use special index
			indices[i] = -1
		} else {
			indices[i] = ColumnIndex(rel, sym)
			if indices[i] < 0 {
				// Symbol not found - should not happen if planner is correct
				return nil
			}
		}
	}

	// Collect unique combinations
	seen := make(map[string]bool)
	var combinations []map[query.Symbol]interface{}

	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()

		// Extract input values
		values := make(map[query.Symbol]interface{})
		keyParts := make([]string, len(inputSymbols))

		for i, sym := range inputSymbols {
			if sym == "$" {
				// Database marker - pass it through as-is
				values[sym] = query.Symbol("$")
				keyParts[i] = "$"
			} else {
				idx := indices[i]
				if idx < len(tuple) {
					values[sym] = tuple[idx]
					// Create unique key for this combination
					keyParts[i] = fmt.Sprintf("%v", tuple[idx])
				}
			}
		}

		key := strings.Join(keyParts, "|")
		if !seen[key] {
			seen[key] = true
			combinations = append(combinations, values)
		}
	}

	return combinations
}

// createInputRelationsFromPattern creates relations from a subquery pattern's inputs.
// This handles ALL inputs including constants, not just variables from the outer query.
func createInputRelationsFromPattern(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}) []Relation {
	return createInputRelationsFromPatternWithOptions(subq, outerValues, ExecutorOptions{})
}

func createInputRelationsFromPatternWithOptions(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}, opts ExecutorOptions) []Relation {
	// Process the subquery's actual inputs in order
	var orderedValues []interface{}
	for _, input := range subq.Inputs {
		switch inp := input.(type) {
		case query.Variable:
			// Get value from outer query
			if val, ok := outerValues[inp.Name]; ok {
				orderedValues = append(orderedValues, val)
			} else {
				// Variable not found - shouldn't happen
				orderedValues = append(orderedValues, nil)
			}
		case query.Constant:
			// Check if it's the database marker
			if sym, ok := inp.Value.(query.Symbol); ok && sym == "$" {
				// Database marker - pass through
				orderedValues = append(orderedValues, sym)
			} else {
				// Regular constant - pass the value directly
				orderedValues = append(orderedValues, inp.Value)
			}
		default:
			// Unknown input type
			orderedValues = append(orderedValues, nil)
		}
	}

	// Now create relations based on the :in clause
	relations := createInputRelationsFromValuesWithOptions(subq.Query, orderedValues, opts)
	return relations
}

// createInputRelationsFromValues creates relations from ordered input values.
func createInputRelationsFromValues(q *query.Query, orderedValues []interface{}) []Relation {
	return createInputRelationsFromValuesWithOptions(q, orderedValues, ExecutorOptions{})
}

// createInputRelationsFromValuesWithOptions creates relations from ordered input values with options.
func createInputRelationsFromValuesWithOptions(q *query.Query, orderedValues []interface{}, opts ExecutorOptions) []Relation {
	var relations []Relation

	// Check if we have the correct number of inputs
	expectedInputs := 0
	for _, input := range q.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			expectedInputs++ // Database REQUIRES explicit $
		case query.ScalarInput:
			expectedInputs++
		case query.RelationInput:
			expectedInputs += len(inp.Symbols)
		case query.TupleInput:
			expectedInputs += len(inp.Symbols)
		case query.CollectionInput:
			expectedInputs++
		}
	}

	if len(orderedValues) != expectedInputs {
		return nil // Wrong number of inputs - this is an error
	}

	// Process :in clause to create appropriate relations
	valueIndex := 0
	for _, input := range q.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Expect an explicit $ symbol at this position
			if valueIndex < len(orderedValues) {
				// Check if it's the database marker
				if sym, ok := orderedValues[valueIndex].(query.Symbol); ok && sym == "$" {
					// Database marker present - skip it
					valueIndex++
				} else {
					// Not a database marker - this is an error
					return nil
				}
			}

		case query.ScalarInput:
			// Create a single-value relation
			if valueIndex < len(orderedValues) {
				rel := NewMaterializedRelationWithOptions(
					[]query.Symbol{inp.Symbol},
					[]Tuple{{orderedValues[valueIndex]}},
					opts,
				)
				relations = append(relations, rel)
				valueIndex++
			}

		case query.RelationInput:
			// For now, treat as scalar inputs (will be enhanced later)
			// This handles the common case where subqueries use scalar inputs
			if valueIndex+len(inp.Symbols) <= len(orderedValues) {
				tuple := make(Tuple, len(inp.Symbols))
				for i := range inp.Symbols {
					tuple[i] = orderedValues[valueIndex+i]
				}
				rel := NewMaterializedRelationWithOptions(inp.Symbols, []Tuple{tuple}, opts)
				relations = append(relations, rel)
				valueIndex += len(inp.Symbols)
			}

		case query.TupleInput:
			// Create a single-tuple relation
			if valueIndex+len(inp.Symbols) <= len(orderedValues) {
				tuple := make(Tuple, len(inp.Symbols))
				for i := range inp.Symbols {
					tuple[i] = orderedValues[valueIndex+i]
				}
				rel := NewMaterializedRelationWithOptions(inp.Symbols, []Tuple{tuple}, opts)
				relations = append(relations, rel)
				valueIndex += len(inp.Symbols)
			}

		case query.CollectionInput:
			// Create a single-column relation
			if valueIndex < len(orderedValues) {
				rel := NewMaterializedRelationWithOptions(
					[]query.Symbol{inp.Symbol},
					[]Tuple{{orderedValues[valueIndex]}},
					opts,
				)
				relations = append(relations, rel)
				valueIndex++
			}
		}
	}

	return relations
}

// createSubqueryContext creates a context with input bindings for subquery execution.
// This is a pure function that wraps contexts.
func createSubqueryContext(parentCtx Context, inputs []query.InputSpec, inputValues map[query.Symbol]interface{}, outerInputs []query.Symbol) Context {
	// Map outer query variables to inner query :in clause variables based on position
	mappedValues := make(map[query.Symbol]interface{})

	// Create ordered list of input variables from outer query
	orderedValues := make([]interface{}, len(outerInputs))
	for i, sym := range outerInputs {
		orderedValues[i] = inputValues[sym]
	}

	// Map to :in clause variables by position
	valueIndex := 0
	for _, input := range inputs {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Skip database
			continue
		case query.ScalarInput:
			// Map by position
			if valueIndex < len(orderedValues) {
				mappedValues[inp.Symbol] = orderedValues[valueIndex]
				// fmt.Printf("DEBUG: Mapping position %d (%v) to :in variable %v = %v\n",
				// 	valueIndex, outerInputs[valueIndex], inp.Symbol, orderedValues[valueIndex])
				valueIndex++
			}
			// Add other input types as needed
		}
	}

	return &subqueryContext{
		parent:      parentCtx,
		inputValues: mappedValues,
		inputs:      inputs,
	}
}

// augmentWithInputValues adds input values as constant columns to a relation.
// This is a pure function that performs relation transformation.
func augmentWithInputValues(rel Relation, inputSymbols []query.Symbol, inputValues []interface{}) Relation {
	// Create new columns list
	newColumns := append(rel.Columns(), inputSymbols...)

	// Create augmented tuples
	var augmentedTuples []Tuple
	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()
		// Append the constant input values to each tuple
		augmentedTuple := append(tuple, inputValues...)
		augmentedTuples = append(augmentedTuples, augmentedTuple)
	}

	return NewMaterializedRelation(newColumns, augmentedTuples)
}

// applyBindingForm applies the binding form to transform subquery results.
// This is a pure function that handles different binding forms (TupleBinding, RelationBinding).
func applyBindingForm(result Relation, binding query.BindingForm, inputValues map[query.Symbol]interface{}, inputSymbols []query.Symbol) (Relation, error) {
	// fmt.Printf("DEBUG: applyBindingForm - binding type: %T, binding: %v\n", binding, binding)
	// fmt.Printf("DEBUG: Result columns: %v\n", result.Columns)

	switch b := binding.(type) {
	case query.TupleBinding:
		// [[?var]] - expect single result, bind to variable

		// EMPTY RESULT = PATTERN FAILS TO MATCH
		// Return empty relation instead of error (datalog semantics)
		if result.Size() == 0 {
			columns := make([]query.Symbol, len(inputSymbols)+len(b.Variables))
			copy(columns, inputSymbols)
			copy(columns[len(inputSymbols):], b.Variables)
			return NewMaterializedRelation(columns, []Tuple{}), nil
		}

		if result.Size() != 1 {
			return nil, fmt.Errorf("tuple binding expects exactly 1 result, got %d", result.Size())
		}

		// fmt.Printf("DEBUG: TupleBinding variables: %v\n", b.Variables)

		// Create relation with input columns + binding columns
		columns := make([]query.Symbol, len(inputSymbols)+len(b.Variables))
		copy(columns, inputSymbols)
		copy(columns[len(inputSymbols):], b.Variables)

		// Create tuple with input values + result values
		tuple := make(Tuple, len(columns))
		for i, sym := range inputSymbols {
			tuple[i] = inputValues[sym]
		}

		// Add result values
		resultTuple := result.Get(0)
		// fmt.Printf("DEBUG: Result tuple: %v\n", resultTuple)

		// Check for nil values in result (INVARIANT: should never happen)
		for i, val := range resultTuple {
			if val == nil {
				return nil, fmt.Errorf("subquery result contains nil value at position %d - this violates datalog semantics", i)
			}
		}

		for i := range b.Variables {
			// For aggregates, the result column is the aggregate expression (e.g., "(max ?price)")
			// We need to match this with the binding variable
			if i < len(resultTuple) {
				tuple[len(inputSymbols)+i] = resultTuple[i]
				// fmt.Printf("DEBUG: Binding %v to position %d: %v\n", sym, len(inputSymbols)+i, resultTuple[i])
			} else {
				// fmt.Printf("DEBUG: No value for binding variable %v at index %d\n", sym, i)
			}
		}

		// fmt.Printf("DEBUG: Final tuple: %v\n", tuple)
		return NewMaterializedRelation(columns, []Tuple{tuple}), nil

	case query.CollectionBinding:
		// ?coll - collect all values into a collection
		// For now, implement as a simple relation
		return nil, fmt.Errorf("collection binding not yet implemented")

	case query.RelationBinding:
		// [[?a ?b] ...] - bind as relation with multiple columns
		resultCols := result.Columns()
		if len(b.Variables) != len(resultCols) {
			return nil, fmt.Errorf("relation binding expects %d columns, got %d", len(b.Variables), len(resultCols))
		}

		// Create relation with input columns + binding columns
		columns := make([]query.Symbol, len(inputSymbols)+len(b.Variables))
		copy(columns, inputSymbols)
		copy(columns[len(inputSymbols):], b.Variables)

		// Create tuples with input values + each result row
		var tuples []Tuple
		for i := 0; i < result.Size(); i++ {
			tuple := make(Tuple, len(columns))

			// Add input values
			for j, sym := range inputSymbols {
				tuple[j] = inputValues[sym]
			}

			// Add result values
			resultTuple := result.Get(i)
			for j := range b.Variables {
				tuple[len(inputSymbols)+j] = resultTuple[j]
			}

			tuples = append(tuples, tuple)
		}

		return NewMaterializedRelation(columns, tuples), nil

	default:
		return nil, fmt.Errorf("unsupported binding form: %T", binding)
	}
}

// getBindingColumns returns the expected columns for a binding form.
// This is a pure function that computes output schema.
func getBindingColumns(binding query.BindingForm, inputSymbols []query.Symbol) []query.Symbol {
	columns := make([]query.Symbol, len(inputSymbols))
	copy(columns, inputSymbols)

	switch b := binding.(type) {
	case query.TupleBinding:
		columns = append(columns, b.Variables...)
	case query.CollectionBinding:
		columns = append(columns, b.Variable)
	case query.RelationBinding:
		columns = append(columns, b.Variables...)
	}

	return columns
}

// subqueryContext wraps a parent context and provides input bindings
type subqueryContext struct {
	parent      Context
	inputValues map[query.Symbol]interface{}
	inputs      []query.InputSpec
}

// Delegate all methods to parent for now
func (sc *subqueryContext) QueryBegin(query string) {
	sc.parent.QueryBegin(query)
}

func (sc *subqueryContext) QueryPlanCreated(plan string) {
	sc.parent.QueryPlanCreated(plan)
}

func (sc *subqueryContext) QueryComplete(relationCount, tupleCount int, err error) {
	sc.parent.QueryComplete(relationCount, tupleCount, err)
}

func (sc *subqueryContext) ExecutePhase(name string, phase interface{}, fn func() (Relation, error)) (Relation, error) {
	return sc.parent.ExecutePhase(name, phase, fn)
}

func (sc *subqueryContext) MatchPatterns(patterns []query.Pattern, fn func() ([]Relation, error)) ([]Relation, error) {
	return sc.parent.MatchPatterns(patterns, fn)
}

func (sc *subqueryContext) MatchPattern(pattern query.Pattern, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error) {
	return sc.parent.MatchPattern(pattern, fn)
}

// matchesWithRelation checks if a datom matches pattern with given relation
func (sc *subqueryContext) matchesWithRelation(datom datalog.Datom, pattern *query.DataPattern, rel Relation) bool {
	// Get columns and build a map of values from the first tuple
	if rel == nil || rel.IsEmpty() {
		return true // No constraints
	}

	cols := rel.Columns()
	it := rel.Iterator()
	if !it.Next() {
		it.Close()
		return true // No constraints
	}
	tuple := it.Tuple()
	it.Close()

	// Build value map from relation
	valueMap := make(map[query.Symbol]interface{})
	for i, col := range cols {
		if i < len(tuple) {
			valueMap[col] = tuple[i]
		}
	}

	// Check each element of the pattern
	elements := []struct {
		patternElem query.PatternElement
		datomValue  interface{}
	}{
		{pattern.GetE(), datom.E},
		{pattern.GetA(), datom.A},
		{pattern.GetV(), datom.V},
		{pattern.GetT(), datom.Tx},
	}

	for _, elem := range elements {
		if elem.patternElem == nil {
			continue
		}

		if v, ok := elem.patternElem.(query.Variable); ok {
			if boundValue, hasBind := valueMap[v.Name]; hasBind {
				// Compare values - handle Identity specially
				if id1, ok1 := boundValue.(datalog.Identity); ok1 {
					if id2, ok2 := elem.datomValue.(datalog.Identity); ok2 {
						if id1.L85() != id2.L85() {
							return false
						}
					} else {
						return false
					}
				} else if boundValue != elem.datomValue {
					return false
				}
			}
		}
	}

	return true
}

func (sc *subqueryContext) MatchPatternWithBindings(pattern query.Pattern, inputBindings map[query.Symbol]int, fn func() ([]datalog.Datom, error)) ([]datalog.Datom, error) {
	return sc.parent.MatchPatternWithBindings(pattern, inputBindings, fn)
}

func (sc *subqueryContext) CombineRelations(oldRels, newRels []Relation, fn func() []Relation) []Relation {
	return sc.parent.CombineRelations(oldRels, newRels, fn)
}

func (sc *subqueryContext) JoinRelations(left, right Relation, fn func() Relation) Relation {
	return sc.parent.JoinRelations(left, right, fn)
}

func (sc *subqueryContext) FilterRelation(rel Relation, predicate string, fn func() Relation) Relation {
	return sc.parent.FilterRelation(rel, predicate, fn)
}

func (sc *subqueryContext) CollapseRelations(rels []Relation, fn func() []Relation) []Relation {
	return sc.parent.CollapseRelations(rels, fn)
}

func (sc *subqueryContext) EvaluateExpression(expr string, tupleCount int, fn func() error) error {
	return sc.parent.EvaluateExpression(expr, tupleCount, fn)
}

func (sc *subqueryContext) EvaluateExpressionRelation(rel Relation, expr string, fn func() Relation) Relation {
	return sc.parent.EvaluateExpressionRelation(rel, expr, fn)
}

func (sc *subqueryContext) Collector() *annotations.Collector {
	return sc.parent.Collector()
}

func (sc *subqueryContext) SetMetadata(key string, value interface{}) {
	sc.parent.SetMetadata(key, value)
}

func (sc *subqueryContext) GetMetadata(key string) (interface{}, bool) {
	return sc.parent.GetMetadata(key)
}

// canBatchSubquery checks if a subquery can be executed with batched inputs.
// This requires the subquery to have a RelationInput in its :in clause.
func canBatchSubquery(q *query.Query) bool {
	// Check if the query has a RelationInput after the database
	hasDatabase := false
	for _, input := range q.In {
		switch input.(type) {
		case query.DatabaseInput:
			hasDatabase = true
		case query.RelationInput:
			// Found a RelationInput - we can batch!
			return hasDatabase // Must have database before relation
		}
	}
	return false
}

// executeBatchedSubquery executes a subquery with all input combinations at once.
// This requires the subquery to accept RelationInput (e.g., :in $ [[?sym ?d]]).
// IMPORTANT: This doesn't work correctly for aggregating subqueries because
// the aggregation will compute over the entire dataset instead of per input tuple.
func executeBatchedSubqueryWithCombinations(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	if len(inputCombinations) == 0 {
		columns := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(columns, []Tuple{}), nil
	}

	// Build a relation with all input combinations
	// The columns should match the input symbols from the subquery
	var columns []query.Symbol
	var allTuples []Tuple

	// Extract the symbols we're passing (excluding $)
	for _, input := range subqPlan.Subquery.Inputs {
		switch inp := input.(type) {
		case query.Variable:
			columns = append(columns, inp.Name)
		case query.Constant:
			// Skip constants like $
			if sym, ok := inp.Value.(query.Symbol); ok && sym == "$" {
				continue
			}
		}
	}

	// Build tuples from all combinations
	for _, values := range inputCombinations {
		var tuple Tuple
		for _, col := range columns {
			if val, ok := values[col]; ok {
				tuple = append(tuple, val)
			}
		}
		if len(tuple) == len(columns) {
			allTuples = append(allTuples, tuple)
		}
	}

	// Create the batched input relation
	batchedInputRel := NewMaterializedRelation(columns, allTuples)

	// Create input relations for the subquery
	// We need to pass $ and the batched relation
	var inputRelations []Relation

	// The subquery should have :in $ [[?sym ?d]] format
	// We pass the batched relation as the second input
	for _, input := range subqPlan.Subquery.Query.In {
		switch input.(type) {
		case query.DatabaseInput:
			// Database doesn't need a relation
			continue
		case query.RelationInput:
			// This is where we pass our batched relation
			inputRelations = append(inputRelations, batchedInputRel)
		}
	}

	// Execute the subquery once with all inputs
	result, err := executePhasesWithInputs(ctx, parentExec, subqPlan.NestedPlan, inputRelations)
	if err != nil {
		return nil, fmt.Errorf("batched subquery execution failed: %w", err)
	}

	// For batched execution, we can't apply the binding form per-input
	// The result should already have all rows
	// Just apply the binding with empty input values (no scalar substitution needed)
	boundResult, err := applyBindingForm(result, subqPlan.Subquery.Binding, nil, subqPlan.Inputs)
	if err != nil {
		return nil, fmt.Errorf("binding form application failed: %w", err)
	}

	return boundResult, nil
}
