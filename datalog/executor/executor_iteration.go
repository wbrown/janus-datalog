package executor

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// hasRelationInput checks if a query has a RelationInput in its :in clause
func hasRelationInput(q *query.Query) bool {
	for _, input := range q.In {
		if _, ok := input.(query.RelationInput); ok {
			return true
		}
	}
	return false
}

// executeWithRelationInputIteration executes a query by iterating over RelationInput tuples
func (e *Executor) executeWithRelationInputIteration(ctx Context, plan *planner.QueryPlan, inputRelations []Relation) (Relation, error) {
	// Find the RelationInput and its corresponding relation
	var relationInput query.RelationInput
	var iterationRelation Relation
	relationIndex := 0

	for _, input := range plan.Query.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Skip database
		case query.RelationInput:
			relationInput = inp
			if relationIndex < len(inputRelations) {
				iterationRelation = inputRelations[relationIndex]
			}
			relationIndex++
		case query.ScalarInput, query.TupleInput, query.CollectionInput:
			// These would be handled as regular inputs
			relationIndex++
		}
	}

	if iterationRelation == nil || iterationRelation.Size() == 0 {
		// No iteration needed or empty input
		return NewMaterializedRelation(extractFindColumns(plan.Query.Find), []Tuple{}), nil
	}

	// Dispatch to parallel or sequential implementation
	if e.enableParallelSubqueries {
		return e.executeWithRelationInputIterationParallel(ctx, plan, inputRelations, relationInput, iterationRelation)
	}
	return e.executeWithRelationInputIterationSequential(ctx, plan, inputRelations, relationInput, iterationRelation)
}

// executeWithRelationInputIterationSequential executes queries sequentially (original implementation)
func (e *Executor) executeWithRelationInputIterationSequential(ctx Context, plan *planner.QueryPlan, inputRelations []Relation, relationInput query.RelationInput, iterationRelation Relation) (Relation, error) {

	// Collect results from each tuple iteration
	var allResults []Relation

	// Iterate over each tuple in the relation
	it := iterationRelation.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()

		// Create a modified query plan with scalar inputs instead of RelationInput
		// We need to modify the query's :in clause to have scalar inputs
		modifiedQuery := *plan.Query // Copy the query
		var newIn []query.InputSpec

		// Replace the RelationInput with ScalarInputs for this iteration
		for _, input := range modifiedQuery.In {
			if _, isRelInput := input.(query.RelationInput); isRelInput {
				// Replace with scalar inputs
				for _, sym := range relationInput.Symbols {
					newIn = append(newIn, query.ScalarInput{Symbol: sym})
				}
			} else {
				newIn = append(newIn, input)
			}
		}
		modifiedQuery.In = newIn

		// Create scalar input relations for this tuple
		var tupleInputRelations []Relation
		for i, sym := range relationInput.Symbols {
			if i < len(tuple) {
				scalarRel := NewMaterializedRelation(
					[]query.Symbol{sym},
					[]Tuple{{tuple[i]}},
				)
				tupleInputRelations = append(tupleInputRelations, scalarRel)
			}
		}

		// Create a new plan with the modified query
		modifiedPlan := *plan
		modifiedPlan.Query = &modifiedQuery

		// Execute the query with these scalar inputs
		result, err := e.executePhasesWithInputsNonIterating(ctx, &modifiedPlan, tupleInputRelations)
		if err != nil {
			return nil, fmt.Errorf("iteration execution failed: %w", err)
		}

		if result.Size() > 0 {
			allResults = append(allResults, result)
		}
	}

	// Combine all results
	if len(allResults) == 0 {
		return NewMaterializedRelation(extractFindColumns(plan.Query.Find), []Tuple{}), nil
	}

	// Union all results
	var allTuples []Tuple
	columns := allResults[0].Columns()

	for _, rel := range allResults {
		it := rel.Iterator()
		for it.Next() {
			allTuples = append(allTuples, it.Tuple())
		}
		it.Close()
	}

	return NewMaterializedRelation(columns, allTuples), nil
}

// executeWithRelationInputIterationParallel executes queries in parallel using a worker pool
func (e *Executor) executeWithRelationInputIterationParallel(ctx Context, plan *planner.QueryPlan, inputRelations []Relation, relationInput query.RelationInput, iterationRelation Relation) (Relation, error) {
	// Determine number of workers
	numWorkers := e.maxSubqueryWorkers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}

	// Collect all tuples first (needed for worker pool)
	var tuples []Tuple
	it := iterationRelation.Iterator()
	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}
	it.Close()

	if len(tuples) == 0 {
		return NewMaterializedRelation(extractFindColumns(plan.Query.Find), []Tuple{}), nil
	}

	// Pre-compute modified query structure (shared across all workers)
	// This eliminates N redundant allocations where N = len(tuples)
	modifiedQuery := *plan.Query
	var newIn []query.InputSpec

	// Replace the RelationInput with ScalarInputs
	for _, input := range modifiedQuery.In {
		if _, isRelInput := input.(query.RelationInput); isRelInput {
			// Replace with scalar inputs
			for _, sym := range relationInput.Symbols {
				newIn = append(newIn, query.ScalarInput{Symbol: sym})
			}
		} else {
			newIn = append(newIn, input)
		}
	}
	modifiedQuery.In = newIn

	// Create modified plan (shared across all workers)
	modifiedPlan := *plan
	modifiedPlan.Query = &modifiedQuery

	// Result collection with mutex for thread safety
	var resultsMu sync.Mutex
	var allResults []Relation
	var firstError error

	// Worker pool using semaphore pattern
	var wg sync.WaitGroup
	sem := make(chan struct{}, numWorkers)

	for tupleIdx, tuple := range tuples {
		wg.Add(1)

		go func(idx int, tup Tuple) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Create scalar input relations for this tuple (only per-tuple work)
			var tupleInputRelations []Relation
			for i, sym := range relationInput.Symbols {
				if i < len(tup) {
					scalarRel := NewMaterializedRelation(
						[]query.Symbol{sym},
						[]Tuple{{tup[i]}},
					)
					tupleInputRelations = append(tupleInputRelations, scalarRel)
				}
			}

			// Execute the query with the shared plan and per-tuple inputs
			result, err := e.executePhasesWithInputsNonIterating(ctx, &modifiedPlan, tupleInputRelations)
			if err != nil {
				resultsMu.Lock()
				if firstError == nil {
					firstError = fmt.Errorf("iteration %d execution failed: %w", idx, err)
				}
				resultsMu.Unlock()
				return
			}

			// Only append non-empty results
			if result.Size() > 0 {
				resultsMu.Lock()
				allResults = append(allResults, result)
				resultsMu.Unlock()
			}
		}(tupleIdx, tuple)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Check for errors
	if firstError != nil {
		return nil, firstError
	}

	// Combine all results
	if len(allResults) == 0 {
		return NewMaterializedRelation(extractFindColumns(plan.Query.Find), []Tuple{}), nil
	}

	// Union all results
	var allTuples []Tuple
	columns := allResults[0].Columns()

	for _, rel := range allResults {
		it := rel.Iterator()
		for it.Next() {
			allTuples = append(allTuples, it.Tuple())
		}
		it.Close()
	}

	return NewMaterializedRelation(columns, allTuples), nil
}

// executePhasesWithInputsNonIterating is the original execution without RelationInput iteration
func (e *Executor) executePhasesWithInputsNonIterating(ctx Context, plan *planner.QueryPlan, inputRelations []Relation) (Relation, error) {
	// Keep track of all bindings across phases
	bindings := make(map[query.Symbol]Relation)

	// Start with input relations if provided
	var currentResult Relation
	if len(inputRelations) > 0 {
		// Process :in clause to bind input relations
		currentResult = BindQueryInputs(plan.Query, inputRelations)

		// Add input bindings to the bindings map
		for _, col := range currentResult.Columns() {
			bindings[col] = currentResult
		}
	}

	// Execute each phase
	for i, phase := range plan.Phases {
		// Use sequential execution with new Relations interface
		phaseResult, err := e.executePhaseSequential(ctx, &phase, i, currentResult)
		if err != nil {
			ctx.QueryComplete(0, 0, err)
			return nil, fmt.Errorf("phase %d failed: %w", i+1, err)
		}

		// Empty result - short circuit
		if phaseResult.Size() == 0 {
			ctx.QueryComplete(0, 0, nil)
			return NewMaterializedRelation(
				extractFindColumns(plan.Query.Find),
				[]Tuple{},
			), nil
		}

		// Use phase result as current result (it already includes join with previous)
		currentResult = phaseResult

		// Track bindings from this phase
		for _, col := range phaseResult.Columns() {
			bindings[col] = phaseResult
		}
	}

	// Apply final processing (projection, aggregation, etc.)
	// Handle aggregations or project to find variables
	hasAggregates := false
	for _, elem := range plan.Query.Find {
		if elem.IsAggregate() {
			hasAggregates = true
			break
		}
	}

	var finalResult Relation
	if hasAggregates {
		finalResult = ExecuteAggregationsWithContext(ctx, currentResult, plan.Query.Find)
	} else {
		var findVars []query.Symbol
		for _, elem := range plan.Query.Find {
			if v, ok := elem.(query.FindVariable); ok {
				findVars = append(findVars, v.Symbol)
			}
		}
		projected, err := currentResult.Project(findVars)
		if err != nil {
			ctx.QueryComplete(0, 0, err)
			return nil, fmt.Errorf("projection failed: %w", err)
		}
		finalResult = projected.Materialize()
	}

	// Apply ordering if specified
	if len(plan.Query.OrderBy) > 0 {
		finalResult = finalResult.Sort(plan.Query.OrderBy)
	}

	ctx.QueryComplete(len(plan.Phases), finalResult.Size(), nil)
	return finalResult, nil
}
