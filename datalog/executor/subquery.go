package executor

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

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
	inputCombinations, err := getUniqueInputCombinations(inputRelation, subqPlan.Inputs)
	if err != nil {
		return nil, fmt.Errorf("subquery input extraction failed: %w", err)
	}

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
			inputRelations, err := createInputRelationsFromPattern(subqPlan.Subquery, inputValues)
			if err != nil {
				unionChan <- relationItem{err: fmt.Errorf("subquery input binding failed: %w", err)}
				continue
			}

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
		symbols := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(symbols, []Tuple{}), nil
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
	symbols := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
	return NewUnionRelation(newChan, symbols, parentExec.options), nil
}

// executeSubquerySequentialMaterialized executes subqueries sequentially and materializes all results
func executeSubquerySequentialMaterialized(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	// Collect results from all subquery executions
	var allResults []Relation

	for _, inputValues := range inputCombinations {
		// Create input relations from the input values
		inputRelations, err := createInputRelationsFromPattern(subqPlan.Subquery, inputValues)
		if err != nil {
			return nil, fmt.Errorf("subquery input binding failed: %w", err)
		}

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
	numWorkers := parentExec.maxSubqueryWorkers
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
				inputRelations, err := createInputRelationsFromPattern(subqPlan.Subquery, work.inputValues)
				if err != nil {
					unionChan <- relationItem{err: fmt.Errorf("subquery input binding failed: %w", err)}
					continue
				}

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
		symbols := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(symbols, []Tuple{}), nil
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
	symbols := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
	return NewUnionRelation(newChan, symbols, parentExec.options), nil
}

// executeSubqueryParallelMaterialized executes subqueries in parallel and materializes all results
func executeSubqueryParallelMaterialized(ctx Context, parentExec *Executor, subqPlan planner.SubqueryPlan, inputCombinations []map[query.Symbol]interface{}) (Relation, error) {
	numWorkers := parentExec.maxSubqueryWorkers
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
				inputRelations, err := createInputRelationsFromPattern(subqPlan.Subquery, work.inputValues)
				if err != nil {
					resultChan <- resultItem{index: work.index, err: fmt.Errorf("subquery input binding failed: %w", err)}
					cancel() // Cancel other workers
					continue
				}

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

	// If no results - return empty relation with expected symbols
	if len(validResults) == 0 {
		symbols := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(symbols, []Tuple{}), nil
	}

	// Union all results by collecting all tuples
	var allTuples []Tuple
	symbols := validResults[0].Symbols()

	for _, rel := range validResults {
		if err := collectTuplesInto(&allTuples, rel); err != nil {
			return nil, fmt.Errorf("subquery result union failed: %w", err)
		}
	}

	result := NewMaterializedRelation(symbols, allTuples)

	return result, nil
}

// executePhasesWithInputs executes query phases with additional input relations.
// This function needs the full parent executor to inherit its optimizations.
func executePhasesWithInputs(ctx Context, parentExec *Executor, plan *planner.QueryPlan, inputRelations []Relation) (Relation, error) {
	// Convert QueryPlan to RealizedPlan
	realizedPlan := plan.Realize()
	// Use the parent executor's ExecuteRealized to inherit all its optimizations
	// (parallel execution, predicate pushdown, plan cache, etc.)
	return parentExec.ExecuteRealized(ctx, realizedPlan, inputRelations)
}

// getUniqueInputCombinations extracts unique combinations of input values.
// This is a pure function that performs data transformation.
func getUniqueInputCombinations(rel Relation, inputSymbols []query.Symbol) ([]map[query.Symbol]interface{}, error) {
	// Find symbol indices for input symbols
	indices := make([]int, len(inputSymbols))
	for i, sym := range inputSymbols {
		if sym.IsSource() {
			// Source marker - not a symbol, use special index
			indices[i] = -1
		} else {
			indices[i] = SymbolIndex(rel, sym)
			if indices[i] < 0 {
				return nil, fmt.Errorf("subquery input symbol %s not found in outer relation (available: %v)", sym, rel.Symbols())
			}
		}
	}

	// Collect unique combinations. Dedup by typed value identity via TupleKeyMap
	// (which compares values with datalog.ValuesEqual on hash collision), never by
	// string rendering — fmt.Sprintf("%v")+"|" is not injective and collapses
	// distinct combinations (e.g. int64(5) vs "5", or "a|b"+"c" vs "a"+"b|c").
	// Source markers are constant execution context, identical on every tuple, so
	// they are excluded from the dedup key.
	seen := NewTupleKeyMap()
	var combinations []map[query.Symbol]interface{}

	it := rel.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()

		// Extract input values; build the dedup key from data values only.
		values := make(map[query.Symbol]interface{})
		var keyValues Tuple

		for i, sym := range inputSymbols {
			if sym.IsSource() {
				// Source marker - pass it through as-is; not part of the key.
				values[sym] = sym
			} else {
				idx := indices[i]
				if idx < len(tuple) {
					values[sym] = tuple[idx]
					keyValues = append(keyValues, tuple[idx])
				}
			}
		}

		key := NewTupleKeyFull(keyValues)
		if !seen.Exists(key) {
			seen.Put(key, struct{}{})
			combinations = append(combinations, values)
		}
	}

	return combinations, nil
}

// createInputRelationsFromPattern creates relations from a subquery pattern's inputs.
// This handles ALL inputs including constants, not just variables from the outer query.
func createInputRelationsFromPattern(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}) ([]Relation, error) {
	return createInputRelationsFromPatternWithOptions(subq, outerValues, ExecutorOptions{})
}

func createInputRelationsFromPatternWithOptions(subq *query.SubqueryPattern, outerValues map[query.Symbol]interface{}, opts ExecutorOptions) ([]Relation, error) {
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
			// Check if it's a source marker
			if sym, ok := inp.Value.(query.Symbol); ok && sym.IsSource() {
				// Source marker - pass through
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
	return createInputRelationsFromValuesWithOptions(subq.Query, orderedValues, opts)
}

// createInputRelationsFromValuesWithOptions creates relations from ordered input values with options.
func createInputRelationsFromValuesWithOptions(q *query.Query, orderedValues []interface{}, opts ExecutorOptions) ([]Relation, error) {
	var relations []Relation

	// Datomic semantics: an omitted :in defaults to [$] (the default database
	// source), not zero inputs. Apply that default before validating arity so a
	// subquery whose nested query has no :in still accepts the supplied source
	// marker instead of being rejected as an over-supply.
	inputs := q.In
	if len(inputs) == 0 {
		inputs = []query.InputSpec{query.DatabaseInput{Name: datalog.NewSymbol("$")}}
	}

	// Check if we have the correct number of inputs
	expectedInputs := 0
	for _, input := range inputs {
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
		return nil, fmt.Errorf("subquery input arity mismatch: nested query :in declares %d required value(s) (%v) "+
			"but the call supplied %d (%v)", expectedInputs, inputs, len(orderedValues), orderedValues)
	}

	// Process :in clause to create appropriate relations
	valueIndex := 0
	for _, input := range inputs {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Expect an explicit $ symbol at this position
			if valueIndex < len(orderedValues) {
				// Check if it's a source marker
				if sym, ok := orderedValues[valueIndex].(query.Symbol); ok && sym.IsSource() {
					// Source marker present - skip it
					valueIndex++
				} else {
					return nil, fmt.Errorf("subquery input: expected a database source ($) at position %d "+
						"of nested :in (%v), got %v", valueIndex, inputs, orderedValues[valueIndex])
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
			// Create a single-symbol relation with one tuple per collection element
			if valueIndex < len(orderedValues) {
				var tuples []Tuple

				// Use reflection to detect and unpack slices
				val := reflect.ValueOf(orderedValues[valueIndex])
				if val.Kind() == reflect.Slice {
					// Unpack slice into individual tuples (pre-allocate to avoid reallocation)
					tuples = make([]Tuple, val.Len())
					for i := 0; i < val.Len(); i++ {
						tuples[i] = Tuple{val.Index(i).Interface()}
					}
				} else {
					// Single value - wrap in tuple
					tuples = []Tuple{{orderedValues[valueIndex]}}
				}

				rel := NewMaterializedRelationWithOptions(
					[]query.Symbol{inp.Symbol},
					tuples,
					opts,
				)
				relations = append(relations, rel)
				valueIndex++
			}
		}
	}

	return relations, nil
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

// augmentWithInputValues adds input values as constant symbols to a relation.
// This is a pure function that performs relation transformation.
func augmentWithInputValues(rel Relation, inputSymbols []query.Symbol, inputValues []interface{}) Relation {
	// Create new symbols list
	newColumns := append(rel.Symbols(), inputSymbols...)

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
// The input relation may be streaming (Size() == -1), so this function
// iterates rather than indexing — Size()/Get() are only safe on already
// materialized relations.
//
// Output shape by binding form:
//   - TupleBinding, ScalarBinding: 1-tuple MaterializedRelation on match,
//     empty MaterializedRelation when subquery returns no rows (datalog
//     "pattern fails to match" semantics). Cardinality is validated by
//     reading at most one extra tuple after the first.
//   - RelationBinding: StreamingRelation that wraps the input iterator
//     and emits inputValues++tuple per Next(). Preserves end-to-end
//     streaming through the subquery → union boundary.
func applyBindingForm(result Relation, binding query.BindingForm, inputValues map[query.Symbol]interface{}, inputSymbols []query.Symbol) (Relation, error) {
	switch b := binding.(type) {
	case query.TupleBinding:
		// TupleBinding [[?a ?b]]: subquery must return exactly one
		// tuple; its N columns bind to the N variables. Arity of the
		// subquery's schema must match len(Variables).
		return applyExactlyOneBinding(result, inputValues, inputSymbols, b.Variables, "tuple", len(b.Variables))

	case query.ScalarBinding:
		// ScalarBinding ?x: subquery must return exactly one tuple
		// with exactly one column; ScalarBinding is the arity-1 case
		// of TupleBinding.
		return applyExactlyOneBinding(result, inputValues, inputSymbols, []query.Symbol{b.Variable}, "scalar", 1)

	case query.CollectionBinding:
		// [?coll ...] - collect all values from a single symbol into a collection.
		return nil, fmt.Errorf("collection binding not yet implemented")

	case query.RelationBinding:
		resultCols := result.Symbols()
		if len(b.Variables) != len(resultCols) {
			return nil, fmt.Errorf("relation binding expects %d symbols, got %d", len(b.Variables), len(resultCols))
		}

		realInputSymbols := filterSourceSymbols(inputSymbols)
		outSymbols := make([]query.Symbol, len(realInputSymbols)+len(b.Variables))
		copy(outSymbols, realInputSymbols)
		copy(outSymbols[len(realInputSymbols):], b.Variables)

		// Pre-compute the input-value prefix — it's constant for this
		// applyBindingForm call, applied to every row of the subquery.
		prefix := make([]interface{}, len(realInputSymbols))
		for i, sym := range realInputSymbols {
			prefix[i] = inputValues[sym]
		}

		// Stream: wrap the subquery's iterator and emit prefix++row per
		// Next(). No buffering.
		wrapped := &prefixingIterator{
			inner:    result.Iterator(),
			prefix:   prefix,
			bodyLen:  len(b.Variables),
		}
		return NewStreamingRelationWithOptions(outSymbols, wrapped, result.Options()), nil

	default:
		return nil, fmt.Errorf("unsupported binding form: %T", binding)
	}
}

// filterSourceSymbols returns inputSymbols with source markers ($, $foo)
// removed — those are execution context, not data variables.
func filterSourceSymbols(inputSymbols []query.Symbol) []query.Symbol {
	out := make([]query.Symbol, 0, len(inputSymbols))
	for _, s := range inputSymbols {
		if !s.IsSource() {
			out = append(out, s)
		}
	}
	return out
}

// applyExactlyOneBinding performs the binding transform for TupleBinding
// and ScalarBinding — both require the subquery to return exactly one
// tuple, and differ only in error-message phrasing and expected arity.
//
// Empty subquery result → returns an empty relation (datalog "pattern
// fails to match" semantics, not an error).
// More than one tuple → returns an error naming the binding form.
// Arity mismatch against expectedArity → returns an error upfront
// without iterating; the schema check is a pure property of the
// subquery's find spec.
func applyExactlyOneBinding(
	result Relation,
	inputValues map[query.Symbol]interface{},
	inputSymbols []query.Symbol,
	bindingVars []query.Symbol,
	label string,
	expectedArity int,
) (Relation, error) {
	realInputSymbols := filterSourceSymbols(inputSymbols)
	outSymbols := make([]query.Symbol, len(realInputSymbols)+len(bindingVars))
	copy(outSymbols, realInputSymbols)
	copy(outSymbols[len(realInputSymbols):], bindingVars)

	// Schema check upfront — pure property of the subquery's find
	// spec, no iteration required.
	if got := len(result.Symbols()); got != expectedArity {
		return nil, fmt.Errorf("%s binding expects %d symbol(s), got %d", label, expectedArity, got)
	}

	first, moreThanOne, err := readAtMostTwo(result)
	if err != nil {
		return nil, err
	}
	if first == nil {
		return NewMaterializedRelation(outSymbols, []Tuple{}), nil
	}
	if moreThanOne {
		return nil, fmt.Errorf("%s binding expects exactly 1 result, got more than 1", label)
	}

	// INVARIANT: subquery tuples contain no nil values.
	for i, val := range first {
		if val == nil {
			return nil, fmt.Errorf("subquery result contains nil value at position %d - this violates datalog semantics", i)
		}
	}

	tuple := make(Tuple, len(outSymbols))
	for i, sym := range realInputSymbols {
		tuple[i] = inputValues[sym]
	}
	for i := range bindingVars {
		tuple[len(realInputSymbols)+i] = first[i]
	}
	return NewMaterializedRelation(outSymbols, []Tuple{tuple}), nil
}

// readAtMostTwo advances the relation's iterator by up to two tuples,
// returning a copy of the first (safe to retain after the iterator is
// closed) and a cardinality indicator:
//
//	moreThanOne == false, first == nil: empty relation.
//	moreThanOne == false, first != nil: exactly one tuple.
//	moreThanOne == true:                more than one tuple; iteration
//	                                    stops immediately after seeing
//	                                    the second.
//
// Used by TupleBinding / ScalarBinding to enforce cardinality without
// draining the subquery result.
func readAtMostTwo(rel Relation) (first Tuple, moreThanOne bool, err error) {
	it := rel.Iterator()
	defer it.Close()

	if !it.Next() {
		return nil, false, it.Error()
	}

	// Copy the first tuple — the underlying iterator may reuse its
	// workspace on the next Next() call.
	src := it.Tuple()
	first = make(Tuple, len(src))
	copy(first, src)

	if it.Next() {
		return first, true, it.Error()
	}
	return first, false, it.Error()
}

// prefixingIterator wraps an Iterator and emits [prefix... inner...] on
// each Next(). Used by RelationBinding to preserve streaming — the
// subquery's iterator flows through without buffering.
//
// Reuses its output buffer across Next() calls: callers that cache
// tuples must copy (same contract as other streaming iterators in the
// codebase; StreamingRelation.RequiresCopy() returns true).
type prefixingIterator struct {
	inner   Iterator
	prefix  []interface{}
	bodyLen int
	buf     Tuple
}

func (p *prefixingIterator) Next() bool {
	if !p.inner.Next() {
		return false
	}
	innerTuple := p.inner.Tuple()
	if p.buf == nil {
		p.buf = make(Tuple, len(p.prefix)+p.bodyLen)
		copy(p.buf, p.prefix)
	}
	for i := 0; i < p.bodyLen; i++ {
		if i < len(innerTuple) {
			p.buf[len(p.prefix)+i] = innerTuple[i]
		}
	}
	return true
}

func (p *prefixingIterator) Tuple() Tuple { return p.buf }

func (p *prefixingIterator) Close() error { return p.inner.Close() }

func (p *prefixingIterator) Error() error { return p.inner.Error() }

// getBindingColumns returns the expected symbols for a binding form.
// This is a pure function that computes output schema.
func getBindingColumns(binding query.BindingForm, inputSymbols []query.Symbol) []query.Symbol {
	symbols := make([]query.Symbol, len(inputSymbols))
	copy(symbols, inputSymbols)

	switch b := binding.(type) {
	case query.TupleBinding:
		symbols = append(symbols, b.Variables...)
	case query.ScalarBinding:
		symbols = append(symbols, b.Variable)
	case query.CollectionBinding:
		symbols = append(symbols, b.Variable)
	case query.RelationBinding:
		symbols = append(symbols, b.Variables...)
	}

	return symbols
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
	// Get symbols and build a map of values from the first tuple
	if rel == nil || rel.IsEmpty() {
		return true // No constraints
	}

	syms := rel.Symbols()
	it := rel.Iterator()
	if !it.Next() {
		it.Close()
		return true // No constraints
	}
	tuple := it.Tuple()
	it.Close()

	// Build value map from relation
	valueMap := make(map[query.Symbol]interface{})
	for i, sym := range syms {
		if i < len(tuple) {
			valueMap[sym] = tuple[i]
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

func (sc *subqueryContext) ScanRegistry() *ScanRegistry {
	return sc.parent.ScanRegistry()
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
		symbols := getBindingColumns(subqPlan.Subquery.Binding, subqPlan.Inputs)
		return NewMaterializedRelation(symbols, []Tuple{}), nil
	}

	// Build a relation with all input combinations
	// The symbols should match the input symbols from the subquery
	var symbols []query.Symbol
	var allTuples []Tuple

	// Extract the symbols we're passing (excluding $)
	for _, input := range subqPlan.Subquery.Inputs {
		switch inp := input.(type) {
		case query.Variable:
			symbols = append(symbols, inp.Name)
		case query.Constant:
			// Skip source markers like $, $users, etc.
			if sym, ok := inp.Value.(query.Symbol); ok && sym.IsSource() {
				continue
			}
		}
	}

	// Build tuples from all combinations
	for _, values := range inputCombinations {
		var tuple Tuple
		for _, sym := range symbols {
			if val, ok := values[sym]; ok {
				tuple = append(tuple, val)
			}
		}
		if len(tuple) == len(symbols) {
			allTuples = append(allTuples, tuple)
		}
	}

	// Create the batched input relation
	batchedInputRel := NewMaterializedRelation(symbols, allTuples)

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
	// The result should already have all tuples
	// Just apply the binding with empty input values (no scalar substitution needed)
	boundResult, err := applyBindingForm(result, subqPlan.Subquery.Binding, nil, subqPlan.Inputs)
	if err != nil {
		return nil, fmt.Errorf("binding form application failed: %w", err)
	}

	return boundResult, nil
}
