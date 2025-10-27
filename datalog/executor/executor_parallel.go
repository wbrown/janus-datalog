package executor

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ParallelExecutor extends Executor with parallel pattern matching
type ParallelExecutor struct {
	*Executor
	maxWorkers int
}

// NewParallelExecutor creates an executor that can run independent patterns in parallel
func NewParallelExecutor(matcher PatternMatcher, maxWorkers int) *ParallelExecutor {
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU()
	}
	return &ParallelExecutor{
		Executor:   NewExecutor(matcher),
		maxWorkers: maxWorkers,
	}
}

// SetPlanCache sets the plan cache for this parallel executor
func (pe *ParallelExecutor) SetPlanCache(cache *planner.PlanCache) {
	pe.Executor.SetPlanCache(cache)
}

// ExecuteWithRelations overrides to use parallel phase execution
func (pe *ParallelExecutor) ExecuteWithRelations(ctx Context, q *query.Query, inputRelations []Relation) (Relation, error) {
	// First, get the plan using the base executor's logic
	ctx.QueryBegin(q.String())

	// Build initial bindings from input relations (reuse base logic)
	initialBindings := make(map[query.Symbol]bool)
	relationIndex := 0
	for _, input := range q.In {
		switch input.(type) {
		case query.DatabaseInput:
			continue
		case query.ScalarInput, query.CollectionInput, query.TupleInput, query.RelationInput:
			if relationIndex < len(inputRelations) {
				for _, col := range inputRelations[relationIndex].Columns() {
					initialBindings[col] = true
				}
				relationIndex++
			}
		}
	}

	// Create the query plan using the planner interface
	var realizedPlan *planner.RealizedPlan
	var err error
	if len(initialBindings) == 0 {
		realizedPlan, err = pe.planner.PlanQuery(q)
	} else {
		realizedPlan, err = pe.planner.PlanQueryWithBindings(q, initialBindings)
	}
	if err != nil {
		ctx.QueryComplete(0, 0, err)
		return nil, fmt.Errorf("query planning failed: %w", err)
	}

	ctx.QueryPlanCreated(realizedPlan.String())

	// Note: Parallel executor currently only works with legacy QueryPlan
	// For now, we use the standard ExecuteRealized path
	return pe.Executor.ExecuteRealized(ctx, realizedPlan, inputRelations)
}

// executePhasesWithInputs overrides the base to inject parallel execution
func (pe *ParallelExecutor) executePhasesWithInputs(ctx Context, plan *planner.QueryPlan, inputRelations []Relation) (Relation, error) {
	// Process input relations (same as base)
	bindings := make(map[query.Symbol]Relation)
	relationIndex := 0
	for _, input := range plan.Query.In {
		switch inp := input.(type) {
		case query.ScalarInput:
			if relationIndex < len(inputRelations) {
				bindings[inp.Symbol] = inputRelations[relationIndex]
				relationIndex++
			}
		case query.CollectionInput:
			if relationIndex < len(inputRelations) {
				bindings[inp.Symbol] = inputRelations[relationIndex]
				relationIndex++
			}
		case query.TupleInput:
			if relationIndex < len(inputRelations) && len(inp.Symbols) > 0 {
				bindings[inp.Symbols[0]] = inputRelations[relationIndex]
				relationIndex++
			}
		case query.RelationInput:
			if relationIndex < len(inputRelations) {
				bindings[inp.Symbols[0]] = inputRelations[relationIndex]
				relationIndex++
			}
		}
	}

	var currentResult Relation

	// Execute each phase
	for i, phase := range plan.Phases {
		var phaseResult Relation
		var err error

		// Check if we can parallelize this phase
		if len(phase.Patterns) >= 2 {
			// Try parallel execution
			phaseResult, err = pe.executePhaseParallel(ctx, &phase, i, currentResult)
		} else {
			// Use sequential for single pattern phases
			phaseResult, err = pe.executePhaseSequential(ctx, &phase, i, currentResult)
		}

		if err != nil {
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

		currentResult = phaseResult

		// Update bindings
		for _, sym := range phase.Provides {
			if idx := ColumnIndex(phaseResult, sym); idx >= 0 {
				bindings[sym] = phaseResult
			}
		}
	}

	// Handle aggregations and projections (same as base)
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

// executePhaseParallel executes patterns in parallel where possible
func (pe *ParallelExecutor) executePhaseParallel(ctx Context, phase *planner.Phase, phaseIndex int, previousResult Relation) (Relation, error) {
	phaseName := fmt.Sprintf("Phase %d (parallel)", phaseIndex+1)

	return ctx.ExecutePhase(phaseName, phase, func() (Relation, error) {
		// Prepare available relations
		var availableRelations Relations
		if previousResult != nil && !previousResult.IsEmpty() {
			availableRelations = append(availableRelations, previousResult)
		}

		// Execute all patterns in parallel
		results := make([]Relation, len(phase.Patterns))
		errors := make([]error, len(phase.Patterns))
		var wg sync.WaitGroup

		// Semaphore to limit concurrent workers
		sem := make(chan struct{}, pe.maxWorkers)

		for i, patternPlan := range phase.Patterns {
			wg.Add(1)
			go func(idx int, pp planner.PatternPlan) {
				defer wg.Done()

				// Acquire semaphore
				sem <- struct{}{}
				defer func() { <-sem }()

				// Execute pattern
				pattern, ok := pp.Pattern.(*query.DataPattern)
				if !ok {
					return
				}

				// Match pattern with available relations
				rel, err := pe.matchPatternWithRelations(ctx, pattern, availableRelations)
				if err != nil {
					errors[idx] = err
					return
				}
				results[idx] = rel
			}(i, patternPlan)
		}

		wg.Wait()

		// Check for errors
		for i, err := range errors {
			if err != nil {
				return nil, fmt.Errorf("pattern %d failed: %w", i, err)
			}
		}

		// Collect non-nil results
		var independentGroups Relations
		for _, rel := range results {
			if rel != nil {
				independentGroups = append(independentGroups, rel)
			}
		}

		// Collapse results
		collapsed := independentGroups.Collapse(ctx)

		// Apply expressions and predicates using the base executor's method
		return pe.applyExpressionsAndPredicates(ctx, phase, collapsed)
	})
}
