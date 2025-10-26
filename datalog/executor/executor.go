package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Executor is the main query execution engine
type Executor struct {
	matcher                  PatternMatcher
	planner                  planner.QueryPlanner
	options                  ExecutorOptions
	enableParallelSubqueries bool
	maxSubqueryWorkers       int
}

// NewExecutor creates a new query executor with default options
func NewExecutor(matcher PatternMatcher) *Executor {
	// Use default options from storage package
	defaultOpts := planner.PlannerOptions{
		EnableDynamicReordering:     true,
		EnablePredicatePushdown:     true,
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableCSE:                   false,
		UseStreamingSubqueryUnion:   false, // TEMP: disable to compare
		MaxPhases:                   10,
		EnableFineGrainedPhases:     true,
		// Executor options
		EnableIteratorComposition:  true,
		EnableTrueStreaming:        true,
		EnableSymmetricHashJoin:    false,
		EnableParallelSubqueries:   true,
		MaxSubqueryWorkers:         0,
		EnableStreamingJoins:       false,
		EnableStreamingAggregation: true,
		EnableDebugLogging:         false,
	}
	return NewExecutorWithOptions(matcher, defaultOpts)
}

// NewExecutorWithOptions creates a new query executor with custom planner options
func NewExecutorWithOptions(matcher PatternMatcher, opts planner.PlannerOptions) *Executor {
	// Convert to executor options
	execOpts := convertToExecutorOptions(opts)

	// Configure matcher with executor options if it supports it
	if indexedMatcher, ok := matcher.(*IndexedMemoryMatcher); ok {
		indexedMatcher.WithOptions(execOpts)
	}

	// Create the appropriate planner based on options
	queryPlanner := planner.CreatePlanner(nil, opts)

	return &Executor{
		matcher:                  matcher,
		planner:                  queryPlanner,
		options:                  execOpts,
		enableParallelSubqueries: opts.EnableParallelSubqueries,
		maxSubqueryWorkers:       opts.MaxSubqueryWorkers,
	}
}

// convertToExecutorOptions extracts executor-specific options from PlannerOptions
func convertToExecutorOptions(opts planner.PlannerOptions) ExecutorOptions {
	return ExecutorOptions{
		EnableIteratorComposition:       opts.EnableIteratorComposition,
		EnableTrueStreaming:             opts.EnableTrueStreaming,
		EnableSymmetricHashJoin:         opts.EnableSymmetricHashJoin,
		EnableParallelSubqueries:        opts.EnableParallelSubqueries,
		MaxSubqueryWorkers:              opts.MaxSubqueryWorkers,
		EnableSubqueryDecorrelation:     opts.EnableSubqueryDecorrelation,
		UseStreamingSubqueryUnion:       opts.UseStreamingSubqueryUnion,
		UseComponentizedSubquery:        opts.UseComponentizedSubquery,
		EnableStreamingJoins:            opts.EnableStreamingJoins,
		EnableStreamingAggregation:      opts.EnableStreamingAggregation,
		EnableStreamingAggregationDebug: opts.EnableStreamingAggregationDebug,
		EnableDebugLogging:              opts.EnableDebugLogging,
	}
}

// EnableParallelSubqueries enables parallel execution of subquery iterations
func (e *Executor) EnableParallelSubqueries(maxWorkers int) {
	e.enableParallelSubqueries = true
	e.maxSubqueryWorkers = maxWorkers
}

// DisableParallelSubqueries disables parallel execution of subquery iterations
func (e *Executor) DisableParallelSubqueries() {
	e.enableParallelSubqueries = false
}

// SetUseQueryExecutor enables or disables the new QueryExecutor (for testing)
func (e *Executor) SetUseQueryExecutor(use bool) {
	e.options.UseQueryExecutor = use
}

// Execute runs a parsed query and returns the results
func (e *Executor) Execute(q *query.Query) (Relation, error) {
	// Use a no-op context for backward compatibility
	return e.ExecuteWithContext(NewContext(nil), q)
}

// ExecuteWithContext runs a parsed query with annotation support
func (e *Executor) ExecuteWithContext(ctx Context, q *query.Query) (Relation, error) {
	// Delegate to ExecuteWithRelations with empty input relations
	return e.ExecuteWithRelations(ctx, q, []Relation{})
}

// ExecuteWithRelations runs a query with input relations
// This is the unified query execution method that treats regular queries and subqueries the same way.
// For regular queries, pass an empty slice for inputRelations.
// For subqueries, pass the relations corresponding to the :in clause variables.
func (e *Executor) ExecuteWithRelations(ctx Context, q *query.Query, inputRelations []Relation) (Relation, error) {
	// Apply decorator pattern: wrap matcher with annotations if context has a handler
	matcher := e.matcher
	if collector := ctx.Collector(); collector != nil {
		matcher = WrapMatcher(matcher, collector.Handler())
	}

	// Create a temporary executor with the wrapped matcher
	executor := &Executor{
		matcher:                  matcher,
		planner:                  e.planner,
		options:                  e.options, // Preserve executor options including UseQueryExecutor flag
		enableParallelSubqueries: e.enableParallelSubqueries,
		maxSubqueryWorkers:       e.maxSubqueryWorkers,
	}

	ctx.QueryBegin(q.String())

	// Build initial bindings from input relations
	initialBindings := make(map[query.Symbol]bool)

	// Process :in clause to map relations to variables
	relationIndex := 0
	for _, input := range q.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Skip database input
			continue
		case query.ScalarInput:
			// For scalar inputs from relations, mark the symbol as bound
			if relationIndex < len(inputRelations) && inputRelations[relationIndex].Size() > 0 {
				initialBindings[inp.Symbol] = true
			}
			relationIndex++
		case query.RelationInput:
			// For relation inputs, mark all variables as bound
			if relationIndex < len(inputRelations) {
				for _, sym := range inp.Symbols {
					initialBindings[sym] = true
				}
			}
			relationIndex++
		case query.TupleInput:
			// For tuple inputs, mark all variables as bound
			if relationIndex < len(inputRelations) && inputRelations[relationIndex].Size() > 0 {
				for _, sym := range inp.Symbols {
					initialBindings[sym] = true
				}
			}
			relationIndex++
		case query.CollectionInput:
			// For collection inputs, mark the variable as bound
			if relationIndex < len(inputRelations) && inputRelations[relationIndex].Size() > 0 {
				initialBindings[inp.Symbol] = true
			}
			relationIndex++
		}
	}

	// Choose execution path based on options
	if executor.options.UseQueryExecutor {
		// New path: Use QueryExecutor (Stage B) with RealizedPlan
		var realizedPlan *planner.RealizedPlan
		var err error
		if len(initialBindings) == 0 {
			realizedPlan, err = executor.planner.PlanQuery(q)
		} else {
			realizedPlan, err = executor.planner.PlanQueryWithBindings(q, initialBindings)
		}
		if err != nil {
			ctx.QueryComplete(0, 0, err)
			return nil, fmt.Errorf("query planning failed: %w", err)
		}
		ctx.QueryPlanCreated(realizedPlan.String())
		return executor.ExecuteRealized(ctx, realizedPlan)
	} else {
		// Old path: Use legacy phase executor (only works with PlannerAdapter)
		adapter, ok := executor.planner.(*planner.PlannerAdapter)
		if !ok {
			return nil, fmt.Errorf("legacy executor path requires old planner; set UseQueryExecutor=true or UseClauseBasedPlanner=false")
		}

		oldPlanner := adapter.GetUnderlyingPlanner()
		var oldPlan *planner.QueryPlan
		var err error
		if len(initialBindings) == 0 {
			oldPlan, err = oldPlanner.Plan(q)
		} else {
			oldPlan, err = oldPlanner.PlanWithBindings(q, initialBindings)
		}
		if err != nil {
			ctx.QueryComplete(0, 0, err)
			return nil, fmt.Errorf("query planning failed: %w", err)
		}
		ctx.QueryPlanCreated(oldPlan.String())
		return executor.executePhasesWithInputs(ctx, oldPlan, inputRelations)
	}
}

// ExecuteRealized executes a RealizedPlan (Stage B: Query-based execution)
// This is the simplified executor that consumes Query fragments from the planner.
//
// Key semantics:
// - Each phase executes as independent Query returning []Relation (disjoint groups)
// - Groups are projected to Keep columns and passed to next phase
// - Final phase must collapse to single relation or error on Cartesian product
func (e *Executor) ExecuteRealized(ctx Context, plan *planner.RealizedPlan) (Relation, error) {
	// Create QueryExecutor
	queryExecutor := NewQueryExecutor(e.matcher, e.options)

	var currentGroups []Relation

	// Execute each phase as an independent query
	for i, phase := range plan.Phases {
		phaseIndex := i
		isLastPhase := (i == len(plan.Phases)-1)

		// DEBUG: Log phase execution
		if collector := ctx.Collector(); collector != nil {
			collector.Add(annotations.Event{
				Name: "realized/phase-begin",
				Data: map[string]interface{}{
					"phase":        phaseIndex + 1,
					"input_groups": len(currentGroups),
					"keep":         phase.Keep,
					"query":        phase.Query.String(),
				},
			})
		}

		// Execute phase query
		groups, err := queryExecutor.Execute(ctx, phase.Query, currentGroups)
		if err != nil {
			return nil, fmt.Errorf("phase %d failed: %w", phaseIndex+1, err)
		}

		// DEBUG: Log phase output before projection
		if collector := ctx.Collector(); collector != nil {
			collector.Add(annotations.Event{
				Name: "realized/phase-output",
				Data: map[string]interface{}{
					"phase":  phaseIndex + 1,
					"groups": len(groups),
				},
			})
		}

		// Project each group to Keep columns (what passes to next phase)
		// Skip for last phase - QueryExecutor already projected to :find symbols
		if !isLastPhase && len(phase.Keep) > 0 {
			for i, group := range groups {
				// Materialize first to avoid iterator consumption issues
				// Collect all tuples to create a reusable relation
				var tuples []Tuple
				it := group.Iterator()
				for it.Next() {
					tuples = append(tuples, it.Tuple())
				}
				it.Close()

				opts := group.Options()
				materialized := NewMaterializedRelationWithOptions(group.Columns(), tuples, opts)

				projected, err := materialized.Project(phase.Keep)
				if err != nil {
					return nil, fmt.Errorf("phase %d projection of group %d failed: %w", phaseIndex+1, i, err)
				}
				groups[i] = projected
			}
		}

		// DEBUG: Log after projection
		if collector := ctx.Collector(); collector != nil && !isLastPhase {
			collector.Add(annotations.Event{
				Name: "realized/phase-projected",
				Data: map[string]interface{}{
					"phase":  phaseIndex + 1,
					"groups": len(groups),
					"keep":   phase.Keep,
				},
			})
		}

		// Early termination on empty
		if len(groups) == 0 {
			return nil, nil
		}

		// For last phase, must collapse to single relation (error on Cartesian product)
		if isLastPhase && len(groups) > 1 {
			return nil, fmt.Errorf("phase %d resulted in %d disjoint relation groups - Cartesian products not supported", phaseIndex+1, len(groups))
		}

		currentGroups = groups
	}

	// Return the final single relation
	if len(currentGroups) == 0 {
		return nil, nil
	}

	return currentGroups[0], nil
}

// executePhasesWithInputs executes a query plan with input relations
func (e *Executor) executePhasesWithInputs(ctx Context, plan *planner.QueryPlan, inputRelations []Relation) (Relation, error) {
	// Pass plan metadata to context for optimization hints (e.g., time ranges)
	if plan.Metadata != nil {
		for key, value := range plan.Metadata {
			ctx.SetMetadata(key, value)
		}
	}

	// Check if we need to iterate over a RelationInput
	if hasRelationInput(plan.Query) && len(inputRelations) > 0 {
		return e.executeWithRelationInputIteration(ctx, plan, inputRelations)
	}

	// Keep track of all bindings across phases
	bindings := make(map[query.Symbol]Relation)

	// Start with input relations if provided
	var currentResult Relation
	var inputParameterRelation Relation // Save input parameters for final projection
	if len(inputRelations) > 0 {
		// Process :in clause to bind input relations
		currentResult = BindQueryInputs(plan.Query, inputRelations)
		inputParameterRelation = currentResult // Save for later

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

		// CRITICAL: Don't call Size() - it consumes streaming iterators!
		// Empty results will be handled naturally by subsequent phases
		// The final result will be empty if any phase produces no tuples

		// ARCHITECTURAL NOTE: This materialization is likely unnecessary
		//
		// The ProjectIterator fix (commit 5e64b1e) makes relations safely reusable via lazy caching.
		// applyExpressionsAndPredicates() returns StreamingRelation with shouldCache=true, which means:
		// 1. First Iterator() call builds cache via CachingIterator
		// 2. Subsequent Iterator() calls replay from cache
		// 3. No manual materialization needed
		//
		// We could simplify this to: currentResult = phaseResult
		//
		// However, this requires careful testing as the legacy executor is the default path.
		// See: docs/bugs/INVESTIGATION_JOIN_RETURNS_ZERO.md (Architectural Observation section)
		//
		// For now, materialize to maintain existing behavior and preserve options
		var tuples []Tuple
		it := phaseResult.Iterator()
		for it.Next() {
			tuples = append(tuples, it.Tuple())
		}
		it.Close()

		opts := phaseResult.Options()
		currentResult = NewMaterializedRelationWithOptions(phaseResult.Columns(), tuples, opts)

		// Update bindings with new symbols from this phase
		for _, sym := range phase.Provides {
			if idx := ColumnIndex(currentResult, sym); idx >= 0 {
				bindings[sym] = currentResult
			}
		}
	}

	// Check for conditional aggregates from query rewriting
	// These need to be injected into the Find clause before aggregation
	findClause := plan.Query.Find
	hasConditionalAggregates := false
	var allCondAggs []planner.ConditionalAggregate

	for i, phase := range plan.Phases {
		if phase.Metadata != nil {
			if condAggs, ok := phase.Metadata["conditional_aggregates"].([]planner.ConditionalAggregate); ok && len(condAggs) > 0 {
				hasConditionalAggregates = true
				allCondAggs = append(allCondAggs, condAggs...)
				if collector := ctx.Collector(); collector != nil {
					collector.Add(annotations.Event{
						Name: "debug/conditional-agg-found",
						Data: map[string]interface{}{
							"phase": i,
							"count": len(condAggs),
						},
					})
				}
			}
		}
	}

	if hasConditionalAggregates {
		// Create a modified find clause with conditional aggregates
		findClause = injectConditionalAggregates(findClause, allCondAggs)

		// Annotate the rewriting
		if collector := ctx.Collector(); collector != nil {
			data := collector.GetDataMap()
			data["rewritten.subquery.count"] = len(allCondAggs)
			data["optimization"] = "conditional-aggregate-rewriting"
			collector.Add(annotations.Event{
				Name:  "query/rewrite.conditional-aggregates",
				Start: time.Now(),
				Data:  data,
			})
		}
	}

	// Handle aggregations or project to find variables
	hasAggregates := hasConditionalAggregates
	if !hasAggregates {
		for _, elem := range findClause {
			if elem.IsAggregate() {
				hasAggregates = true
				break
			}
		}
	}

	var finalResult Relation
	if hasAggregates {
		finalResult = ExecuteAggregationsWithContext(ctx, currentResult, findClause)
	} else {
		var findVars []query.Symbol
		for _, elem := range plan.Query.Find {
			if v, ok := elem.(query.FindVariable); ok {
				findVars = append(findVars, v.Symbol)
			}
		}

		// Check if any find variables are missing from currentResult but exist in input parameters
		// This handles the case where input parameters appear in :find but aren't used in patterns
		var missingFromCurrent []query.Symbol
		currentCols := make(map[query.Symbol]bool)
		for _, col := range currentResult.Columns() {
			currentCols[col] = true
		}
		for _, findVar := range findVars {
			if !currentCols[findVar] {
				missingFromCurrent = append(missingFromCurrent, findVar)
			}
		}

		// If we have missing symbols and they're in input parameters, join them back
		if len(missingFromCurrent) > 0 && inputParameterRelation != nil {
			inputCols := make(map[query.Symbol]bool)
			for _, col := range inputParameterRelation.Columns() {
				inputCols[col] = true
			}

			// Check if all missing symbols are in input parameters
			var inputSymbolsToAdd []query.Symbol
			for _, missing := range missingFromCurrent {
				if inputCols[missing] {
					inputSymbolsToAdd = append(inputSymbolsToAdd, missing)
				}
			}

			// If we have input symbols to add, join them back
			if len(inputSymbolsToAdd) > 0 {
				// Project input relation to only the symbols we need
				inputToJoin, err := inputParameterRelation.Project(inputSymbolsToAdd)
				if err == nil {
					// Join with current result (cross product since no shared columns)
					currentResult = currentResult.Join(inputToJoin)
				}
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

// SetPlanCache sets the plan cache for this executor
func (e *Executor) SetPlanCache(cache *planner.PlanCache) {
	if e.planner != nil {
		e.planner.SetCache(cache)
	}
}

// GetPlanner returns the executor's planner for direct access
func (e *Executor) GetPlanner() planner.QueryPlanner {
	return e.planner
}

// Options returns the executor's configuration options
func (e *Executor) Options() ExecutorOptions {
	return e.options
}

// HashJoin performs a hash join using the executor's options
func (e *Executor) HashJoin(left, right Relation, joinCols []query.Symbol) Relation {
	return HashJoinWithOptions(left, right, joinCols, e.options)
}

// SymmetricHashJoin performs a symmetric hash join using the executor's options
func (e *Executor) SymmetricHashJoin(left, right Relation, joinCols []query.Symbol) Relation {
	return SymmetricHashJoinWithOptions(left, right, joinCols, e.options)
}
