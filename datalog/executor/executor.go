package executor

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Executor is the main query execution engine
type Executor struct {
	matcher                  PatternMatcher
	entityResolver           EntityResolver
	planner                  planner.QueryPlanner
	options                  ExecutorOptions
	enableParallelSubqueries bool
	maxSubqueryWorkers       int
}

// NewExecutor creates a new query executor with default options
func NewExecutor(matcher PatternMatcher, resolver EntityResolver) *Executor {
	defaultOpts := planner.PlannerOptions{
		UseStreamingSubqueryUnion:  false,
		EnableIteratorComposition:  true,
		EnableTrueStreaming:        true,
		EnableSymmetricHashJoin:    false,
		EnableParallelSubqueries:   true,
		MaxSubqueryWorkers:         0,
		EnableStreamingJoins:       false,
		EnableStreamingAggregation: true,
		EnableDebugLogging:         false,
	}
	return NewExecutorWithOptions(matcher, resolver, defaultOpts)
}

// NewExecutorWithOptions creates a new query executor with custom planner options
func NewExecutorWithOptions(matcher PatternMatcher, resolver EntityResolver, opts planner.PlannerOptions) *Executor {
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
		entityResolver:           resolver,
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
		UseStreamingSubqueryUnion:       opts.UseStreamingSubqueryUnion,
		UseComponentizedSubquery:        opts.UseComponentizedSubquery,
		EnableStreamingJoins:            opts.EnableStreamingJoins,
		EnableStreamingAggregation:      opts.EnableStreamingAggregation,
		EnableStreamingAggregationDebug: opts.EnableStreamingAggregationDebug,
		EnableDebugLogging:              opts.EnableDebugLogging,
		EnableScanSharing:               opts.EnableScanSharing,
		EnableEntityPrefetch:            opts.EnableEntityPrefetch,
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

	// Wrap with scan sharing if enabled — must come before annotation wrapping
	// so that annotations see the sharing layer's decisions
	if e.options.EnableScanSharing {
		reg := ctx.ScanRegistry()
		var handler annotations.Handler
		if collector := ctx.Collector(); collector != nil {
			handler = collector.Handler()
		}
		matcher = NewScanSharingMatcher(matcher, reg, handler)
	}

	if collector := ctx.Collector(); collector != nil {
		matcher = WrapMatcher(matcher, collector.Handler())
	}

	// Create a temporary executor with the wrapped matcher
	executor := &Executor{
		matcher:                  matcher,
		entityResolver:           e.entityResolver,
		planner:                  e.planner,
		options:                  e.options,
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

	// Execute using QueryExecutor (Stage B) with RealizedPlan. The annotation
	// handler is threaded per-query into planning (not stored on the shared
	// planner), so concurrent annotated queries neither race on it nor
	// cross-route algebra-bridge events.
	var planHandler annotations.Handler
	if collector := ctx.Collector(); collector != nil {
		planHandler = collector.Handler()
	}
	var realizedPlan *planner.RealizedPlan
	var err error
	if len(initialBindings) == 0 {
		realizedPlan, err = executor.planner.PlanQuery(q, planHandler)
	} else {
		realizedPlan, err = executor.planner.PlanQueryWithBindings(q, initialBindings, planHandler)
	}
	if err != nil {
		ctx.QueryComplete(0, 0, err)
		return nil, fmt.Errorf("query planning failed: %w", err)
	}
	ctx.QueryPlanCreated(realizedPlan.String())
	return executor.ExecuteRealized(ctx, realizedPlan, inputRelations)
}

// ExecuteRealized executes a RealizedPlan (Stage B: Query-based execution)
// This is the simplified executor that consumes Query fragments from the planner.
//
// Key semantics:
// - Each phase executes as independent Query returning []Relation (disjoint groups)
// - Groups are projected to Keep symbols and passed to next phase
// - Final phase must collapse to single relation or error on Cartesian product
func (e *Executor) ExecuteRealized(ctx Context, plan *planner.RealizedPlan, inputRelations []Relation) (Relation, error) {
	// Check if we need to iterate over a RelationInput
	// RelationInput (e.g., :in [[?a ?b]]) requires executing the query once per tuple
	if hasRelationInput(plan.Query) && len(inputRelations) > 0 {
		return e.executeRealizedWithRelationInputIteration(ctx, plan, inputRelations)
	}

	// Create QueryExecutor with collector from context for annotations
	opts := e.options
	if collector := ctx.Collector(); collector != nil {
		opts.Collector = collector
	}
	queryExecutor := newQueryExecutor(e.matcher, e.entityResolver, opts)

	var currentGroups []Relation

	// If we have input relations, bind them before the first phase
	if len(inputRelations) > 0 && len(plan.Phases) > 0 {
		// Bind input relations using the query's :in clause
		boundRelation := BindQueryInputs(plan.Query, inputRelations)
		currentGroups = []Relation{boundRelation}
	}

	// Extract constant-bindable scalar inputs from phase metadata.
	// These are scalar inputs that only appear in predicates/expressions, not data patterns.
	// Resolving them as constants prevents disjoint relation groups and Product() panics.
	if len(currentGroups) > 0 {
		allConstBindable := make(map[query.Symbol]bool)
		for _, phase := range plan.Phases {
			if cbInputs, ok := phase.Metadata["constant_bindable_inputs"]; ok {
				if syms, ok := cbInputs.([]query.Symbol); ok {
					for _, sym := range syms {
						allConstBindable[sym] = true
					}
				}
			}
		}
		if len(allConstBindable) > 0 {
			// Extract constant values from the bound input relation
			boundRel := currentGroups[0]
			syms := boundRel.Symbols()

			// Find symbol indices for constant-bindable symbols
			constValues := make(map[query.Symbol]interface{})
			constSymIndices := make(map[int]bool)
			for i, sym := range syms {
				if allConstBindable[sym] {
					constSymIndices[i] = true
				}
			}

			// Read first tuple to get the constant values (scalar inputs have exactly 1 tuple)
			if len(constSymIndices) > 0 {
				it := boundRel.Iterator()
				if it.Next() {
					tuple := it.Tuple()
					for i, sym := range syms {
						if allConstBindable[sym] && i < len(tuple) {
							constValues[sym] = tuple[i]
						}
					}
				}
				it.Close()

				if len(constValues) > 0 {
					queryExecutor.constantBindings = constValues

					// Project out constant symbols from the bound relation
					var keepSyms []query.Symbol
					for i, sym := range syms {
						if !constSymIndices[i] {
							keepSyms = append(keepSyms, sym)
						}
					}

					if len(keepSyms) == 0 {
						// All symbols were constants — no input relation needed
						currentGroups = nil
					} else if len(keepSyms) < len(syms) {
						// Re-materialize without constant symbols
						projected, err := boundRel.Materialize().Project(keepSyms)
						if err == nil {
							currentGroups = []Relation{projected}
						}
					}
					// else: no constant symbols to remove (shouldn't happen but safe)
				}
			}
		}
	}

	// Check for conditional aggregates and emit annotation for observability
	// The planner emits two representations of conditional aggregates:
	// 1. Metadata: phase.Metadata["conditional_aggregates"] - used by legacy executor
	// 2. Find clause: phase.Find contains FindAggregate with Predicate - used by QueryExecutor
	// This dual approach maintains backward compatibility while following "Datalog is the IR"
	var condAggCount int
	for _, phase := range plan.Phases {
		if phase.Metadata != nil {
			if condAggs, ok := phase.Metadata["conditional_aggregates"].([]planner.ConditionalAggregate); ok {
				condAggCount += len(condAggs)
			}
		}
	}
	if condAggCount > 0 {
		if collector := ctx.Collector(); collector != nil {
			data := collector.GetDataMap()
			data["rewritten.subquery.count"] = condAggCount
			data["optimization"] = "conditional-aggregate-rewriting"
			collector.Add(annotations.Event{
				Name: "query/rewrite.conditional-aggregates",
				Data: data,
			})
		}
	}

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

		// Project each group to Keep symbols (what passes to next phase)
		// Skip for last phase - QueryExecutor already projected to :find symbols
		if !isLastPhase && len(phase.Keep) > 0 {
			for i, group := range groups {
				// Materialize first to avoid iterator consumption issues
				// Collect all tuples to create a reusable relation
				var tuples []Tuple
				// A non-last phase materializes each group to pass Keep symbols
				// forward. Carry any deferred scan error onto the materialized
				// relation so Project propagates it into the next phase instead of
				// laundering a failed scan into an empty result.
				keepErr := collectTuplesInto(&tuples, group)

				opts := group.Options()
				materialized := NewMaterializedRelationWithOptions(group.Symbols(), tuples, opts)
				materialized.err = keepErr

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
			return emptyRelationForQuery(plan.Query), nil
		}

		// For last phase, must collapse to single relation (error on Cartesian product)
		if isLastPhase && len(groups) > 1 {
			return nil, fmt.Errorf("phase %d resulted in %d disjoint relation groups - Cartesian products not supported", phaseIndex+1, len(groups))
		}

		currentGroups = groups
	}

	// Return the final single relation
	if len(currentGroups) == 0 {
		return emptyRelationForQuery(plan.Query), nil
	}

	finalResult := currentGroups[0]

	// Apply ordering if specified
	if len(plan.Query.OrderBy) > 0 {
		finalResult = finalResult.Sort(plan.Query.OrderBy)
	}

	return finalResult, nil
}

// executeRealizedWithRelationInputIteration handles RelationInput iteration for QueryExecutor path
// This iterates over each tuple in the RelationInput and executes the plan once per tuple.
func (e *Executor) executeRealizedWithRelationInputIteration(ctx Context, plan *planner.RealizedPlan, inputRelations []Relation) (Relation, error) {
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
		return NewMaterializedRelation(extractFindSymbols(plan.Query.Find), []Tuple{}), nil
	}

	// Dispatch to parallel or sequential implementation
	if e.enableParallelSubqueries {
		return e.executeRealizedWithRelationInputIterationParallel(ctx, plan, inputRelations, relationInput, iterationRelation)
	}
	return e.executeRealizedWithRelationInputIterationSequential(ctx, plan, inputRelations, relationInput, iterationRelation)
}

// executeRealizedWithRelationInputIterationSequential executes QueryExecutor path sequentially for RelationInput
func (e *Executor) executeRealizedWithRelationInputIterationSequential(
	ctx Context,
	plan *planner.RealizedPlan,
	inputRelations []Relation,
	relationInput query.RelationInput,
	iterationRelation Relation,
) (Relation, error) {
	// Collect results from each tuple iteration
	var allResults []Relation

	// Iterate over each tuple in the relation
	it := iterationRelation.Iterator()
	defer it.Close()

	for it.Next() {
		tuple := it.Tuple()

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

		// Execute the plan with these scalar inputs using QueryExecutor
		result, err := e.executeRealizedNonIterating(ctx, plan, tupleInputRelations, relationInput)
		if err != nil {
			return nil, fmt.Errorf("iteration execution failed: %w", err)
		}

		if result != nil {
			allResults = append(allResults, result)
		}
	}

	// Combine all results
	if len(allResults) == 0 {
		return NewMaterializedRelation(extractFindSymbols(plan.Query.Find), []Tuple{}), nil
	}

	// Union all results, propagating any per-tuple scan error rather than
	// dropping it (a failed iteration must not look like an empty result).
	var allTuples []Tuple
	symbols := allResults[0].Symbols()

	for _, rel := range allResults {
		if err := collectTuplesInto(&allTuples, rel); err != nil {
			return nil, fmt.Errorf("iteration execution failed: %w", err)
		}
	}

	return NewMaterializedRelation(symbols, allTuples), nil
}

// executeRealizedWithRelationInputIterationParallel executes QueryExecutor path in parallel for RelationInput
func (e *Executor) executeRealizedWithRelationInputIterationParallel(
	ctx Context,
	plan *planner.RealizedPlan,
	inputRelations []Relation,
	relationInput query.RelationInput,
	iterationRelation Relation,
) (Relation, error) {
	// Determine number of workers
	numWorkers := e.maxSubqueryWorkers
	if numWorkers <= 0 {
		numWorkers = 4 // Default to 4 workers
	}

	// Collect all tuples first (needed for worker pool)
	var tuples []Tuple
	collectTuplesInto(&tuples, iterationRelation)

	if len(tuples) == 0 {
		return NewMaterializedRelation(extractFindSymbols(plan.Query.Find), []Tuple{}), nil
	}

	// Result collection with mutex for thread safety
	type iterationResult struct {
		result Relation
		err    error
	}
	results := make([]iterationResult, len(tuples))

	// Worker pool using semaphore pattern
	sem := make(chan struct{}, numWorkers)
	done := make(chan struct{})

	for tupleIdx, tuple := range tuples {
		go func(idx int, tup Tuple) {
			// Acquire semaphore
			sem <- struct{}{}
			defer func() {
				<-sem
				// Signal completion after releasing semaphore
				done <- struct{}{}
			}()

			// Create scalar input relations for this tuple
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

			// Execute the plan with these scalar inputs. Each worker gets its
			// own forked context: a Context's per-query state (queryStart,
			// metadata, scanRegistry) is not safe for concurrent mutation.
			result, err := e.executeRealizedNonIterating(forkContext(ctx), plan, tupleInputRelations, relationInput)
			results[idx] = iterationResult{result: result, err: err}
		}(tupleIdx, tuple)
	}

	// Wait for all workers to complete
	for range tuples {
		<-done
	}

	// Check for errors and collect results. Consume each per-worker result via
	// collectTuplesInto so a failed scan's deferred error is propagated rather
	// than silently dropped by Size() materialization.
	var allTuples []Tuple
	var symbols []query.Symbol
	for _, r := range results {
		if r.err != nil {
			return nil, fmt.Errorf("parallel iteration execution failed: %w", r.err)
		}
		if r.result == nil {
			continue
		}
		if symbols == nil {
			symbols = r.result.Symbols()
		}
		if err := collectTuplesInto(&allTuples, r.result); err != nil {
			return nil, fmt.Errorf("parallel iteration execution failed: %w", err)
		}
	}

	// Combine all results
	if symbols == nil {
		return NewMaterializedRelation(extractFindSymbols(plan.Query.Find), []Tuple{}), nil
	}

	return NewMaterializedRelation(symbols, allTuples), nil
}

// executeRealizedNonIterating executes a RealizedPlan without RelationInput iteration
// This is the core QueryExecutor path, called once per RelationInput tuple during iteration.
func (e *Executor) executeRealizedNonIterating(
	ctx Context,
	plan *planner.RealizedPlan,
	scalarInputRelations []Relation,
	relationInput query.RelationInput,
) (Relation, error) {
	// Create QueryExecutor
	queryExecutor := newQueryExecutor(e.matcher, e.entityResolver, e.options)

	var currentGroups []Relation

	// Bind scalar input relations (one per symbol from RelationInput)
	if len(scalarInputRelations) > 0 {
		// Create a modified query with scalar inputs instead of RelationInput
		modifiedQuery := *plan.Query
		var newIn []query.InputSpec

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

		// Bind the scalar input relations
		boundRelation := BindQueryInputs(&modifiedQuery, scalarInputRelations)
		currentGroups = []Relation{boundRelation}
	}

	// Execute each phase as an independent query
	for i, phase := range plan.Phases {
		phaseIndex := i
		isLastPhase := (i == len(plan.Phases)-1)

		// Execute phase query
		groups, err := queryExecutor.Execute(ctx, phase.Query, currentGroups)
		if err != nil {
			return nil, fmt.Errorf("phase %d failed: %w", phaseIndex+1, err)
		}

		// Project each group to Keep symbols (what passes to next phase)
		// Skip for last phase - QueryExecutor already projected to :find symbols
		if !isLastPhase && len(phase.Keep) > 0 {
			for i, group := range groups {
				// Materialize first to avoid iterator consumption issues
				var tuples []Tuple
				// A non-last phase materializes each group to pass Keep symbols
				// forward. Carry any deferred scan error onto the materialized
				// relation so Project propagates it into the next phase instead of
				// laundering a failed scan into an empty result.
				keepErr := collectTuplesInto(&tuples, group)

				opts := group.Options()
				materialized := NewMaterializedRelationWithOptions(group.Symbols(), tuples, opts)
				materialized.err = keepErr

				projected, err := materialized.Project(phase.Keep)
				if err != nil {
					return nil, fmt.Errorf("phase %d projection of group %d failed: %w", phaseIndex+1, i, err)
				}
				groups[i] = projected
			}
		}

		// Early termination on empty
		if len(groups) == 0 {
			return emptyRelationForQuery(plan.Query), nil
		}

		// For last phase, must collapse to single relation (error on Cartesian product)
		if isLastPhase && len(groups) > 1 {
			return nil, fmt.Errorf("phase %d resulted in %d disjoint relation groups", phaseIndex+1, len(groups))
		}

		currentGroups = groups
	}

	// Return the final single relation
	if len(currentGroups) == 0 {
		return emptyRelationForQuery(plan.Query), nil
	}

	finalResult := currentGroups[0]

	// Apply ordering if specified
	if len(plan.Query.OrderBy) > 0 {
		finalResult = finalResult.Sort(plan.Query.OrderBy)
	}

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
func (e *Executor) HashJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	return HashJoinWithOptions(left, right, joinSyms, e.options)
}

// SymmetricHashJoin performs a symmetric hash join using the executor's options
func (e *Executor) SymmetricHashJoin(left, right Relation, joinSyms []query.Symbol) Relation {
	return SymmetricHashJoinWithOptions(left, right, joinSyms, e.options)
}
