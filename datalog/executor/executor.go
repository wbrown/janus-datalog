package executor

import (
	"fmt"
	"sync"

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
		EnableAttributeFetchFusion: true,
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
		EnableAttributeFetchFusion:      opts.EnableAttributeFetchFusion,
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
	// Find the RelationInput, its corresponding relation, and that relation's index
	// within inputRelations. The index lets each per-tuple execution forward the
	// OTHER input relations (scalars, collections) in their original positions
	// rather than dropping them
	// (see docs/bugs/resolved/BUG_SCALAR_PLUS_RELATION_INPUT_DROPS_OTHER_INPUTS.md).
	var relationInput query.RelationInput
	var iterationRelation Relation
	iterationIndex := -1
	relationIndex := 0

	for _, input := range plan.Query.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// Skip database
		case query.RelationInput:
			relationInput = inp
			if relationIndex < len(inputRelations) {
				iterationRelation = inputRelations[relationIndex]
				iterationIndex = relationIndex
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
		return e.executeRealizedWithRelationInputIterationParallel(ctx, plan, inputRelations, relationInput, iterationRelation, iterationIndex)
	}
	return e.executeRealizedWithRelationInputIterationSequential(ctx, plan, inputRelations, relationInput, iterationRelation, iterationIndex)
}

// perTupleInputBuilder pre-computes the layout of the per-tuple input-relation
// list once. Per RelationInput iteration: every original input relation is
// forwarded in its original position, except the iteration relation (at
// iterationIndex), which is replaced by one single-value relation per
// RelationInput symbol drawn from the tuple. Forwarding the non-iteration
// inputs (scalars, collections) in place keeps them aligned with
// executeRealizedNonIterating's in-place :in rewrite; without it they are
// dropped and BindQueryInputs binds the wrong values
// (see docs/bugs/resolved/BUG_SCALAR_PLUS_RELATION_INPUT_DROPS_OTHER_INPUTS.md).
//
// Use Session() to obtain a workspace-reusable handle for repeated per-tuple
// builds — that's the path used by both the parallel (per worker) and
// sequential paths, and it allocates the scratch relations once instead of
// per-tuple. Direct Build() exists only for one-shot use.
type perTupleInputBuilder struct {
	prefix   []Relation     // inputRelations[:iterationIndex]
	suffix   []Relation     // inputRelations[iterationIndex+1:]
	symbols  []query.Symbol // relationInput.Symbols
	capacity int            // len(prefix) + len(suffix) + len(symbols)
}

// newPerTupleInputBuilder splits inputRelations around iterationIndex once.
// If iterationIndex is out of range, no substitution is performed (the input
// relations pass through unchanged).
func newPerTupleInputBuilder(inputRelations []Relation, iterationIndex int, relationInput query.RelationInput) *perTupleInputBuilder {
	if iterationIndex < 0 || iterationIndex >= len(inputRelations) {
		return &perTupleInputBuilder{
			prefix:   inputRelations,
			symbols:  relationInput.Symbols,
			capacity: len(inputRelations) + len(relationInput.Symbols),
		}
	}
	return &perTupleInputBuilder{
		prefix:   inputRelations[:iterationIndex],
		suffix:   inputRelations[iterationIndex+1:],
		symbols:  relationInput.Symbols,
		capacity: len(inputRelations) - 1 + len(relationInput.Symbols),
	}
}

// perTupleInputSession is a workspace-reusable handle for repeated per-tuple
// input-list builds, mirroring the BuildTupleInternedInto / it.workspace
// pattern used by the storage iterators (matcher_iterator_reusing.go,
// hash_join_matcher.go): all per-iteration state is pre-allocated, and each
// step does an in-place update instead of an allocation.
//
// Pre-allocated once per worker:
//   - valueSlots: N single-element Tuple slices, one per RelationInput symbol;
//     the caller mutates valueSlots[i][0] per tuple to set the scalar value.
//   - inputList: the full input-relation list (prefix + scratch relations +
//     suffix), wired up to the scratch relations whose internal tuples
//     ARE valueSlots[i]. Mutating valueSlots[i][0] changes what the relation
//     yields on its next iteration.
//
// Safety contract: callers must not retain the returned Input slice or the
// scratch relations within it across Update calls. executeRealizedNonIterating
// satisfies this — it calls BindQueryInputs eagerly, which iterates each
// input relation once and copies the value out via Tuple{tuple[0]}.
//
// Each worker creates its own session via builder.Session(); sessions are
// not safe for concurrent use.
type perTupleInputSession struct {
	valueSlots []Tuple    // valueSlots[i] is the (mutable) 1-element Tuple slice for symbol i
	inputList  []Relation // pre-wired: prefix + scratch relations + suffix
}

// Session pre-builds the per-symbol scratch relations and the full input
// list once, so per-tuple work is reduced to N value writes (no allocation).
func (b *perTupleInputBuilder) Session() *perTupleInputSession {
	valueSlots := make([]Tuple, len(b.symbols))
	scratchRels := make([]Relation, len(b.symbols))
	for i, sym := range b.symbols {
		// Each scratch tuple is a 1-element slice whose value is updated
		// per tuple. Wrapping it in a MaterializedRelation means the
		// relation's tuples[0] IS valueSlots[i] (deduplicateTuples on a
		// 1-tuple input returns a fresh outer slice whose element shares
		// the inner Tuple — confirmed by reading deduplicateTuples).
		valueSlots[i] = make(Tuple, 1)
		scratchRels[i] = NewMaterializedRelation([]query.Symbol{sym}, []Tuple{valueSlots[i]})
	}
	inputList := make([]Relation, 0, b.capacity)
	inputList = append(inputList, b.prefix...)
	inputList = append(inputList, scratchRels...)
	inputList = append(inputList, b.suffix...)
	return &perTupleInputSession{
		valueSlots: valueSlots,
		inputList:  inputList,
	}
}

// Update writes the tuple's values into the session's scratch slots. No
// allocation: this is the per-tuple equivalent of BuildTupleInternedInto
// writing into a reused workspace.
func (s *perTupleInputSession) Update(tuple Tuple) {
	for i := range s.valueSlots {
		if i < len(tuple) {
			s.valueSlots[i][0] = tuple[i]
		}
	}
}

// Input returns the session's pre-wired input-relation list. The returned
// slice (and the scratch relations within it) MUST be fully consumed before
// the next Update call; executeRealizedNonIterating via BindQueryInputs
// satisfies that.
func (s *perTupleInputSession) Input() []Relation {
	return s.inputList
}

// executeRealizedWithRelationInputIterationSequential executes QueryExecutor path sequentially for RelationInput
func (e *Executor) executeRealizedWithRelationInputIterationSequential(
	ctx Context,
	plan *planner.RealizedPlan,
	inputRelations []Relation,
	relationInput query.RelationInput,
	iterationRelation Relation,
	iterationIndex int,
) (Relation, error) {
	// Collect results from each tuple iteration
	var allResults []Relation

	// Hoist all per-query setup out of the iteration loop. preparedIteration
	// owns the QueryExecutor, the rewritten modifiedQuery, and either the
	// pre-built bound relation (fast path) or a perTupleInputSession
	// (fallback). The per-tuple inner loop is just prepared.Run(ctx, tuple).
	prepared := e.prepareIteration(plan, relationInput, inputRelations, iterationIndex)

	// Iterate over each tuple in the relation. Close() is explicit (not
	// deferred) so its error can be inspected before results are combined, and
	// so a deferred input-iteration failure — which surfaces only through
	// Error() after Next() returns false — is not laundered into a clean
	// partial result. This mirrors the parallel path's policy.
	it := iterationRelation.Iterator()

	for it.Next() {
		result, err := prepared.Run(ctx, it.Tuple())
		if err != nil {
			// Per-tuple execution error is the most specific cause: close
			// best-effort and do not let a Close() error mask it.
			it.Close()
			return nil, fmt.Errorf("iteration execution failed: %w", err)
		}

		if result != nil {
			allResults = append(allResults, result)
		}
	}

	// Error priority: a deferred iteration error (the input may have truncated,
	// so anything past the failure point was never seen) wins over a Close()
	// error, so cleanup cannot mask the real cause.
	iterErr := it.Error()
	closeErr := it.Close()
	if iterErr != nil {
		return nil, iterErr
	}
	if closeErr != nil {
		return nil, closeErr
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

// executeRealizedWithRelationInputIterationParallel executes the
// QueryExecutor path in parallel for RelationInput. The producer (this
// goroutine) streams tuples directly from iterationRelation's iterator
// into the jobs channel; a fixed pool of numWorkers worker goroutines
// drains jobs, runs per-tuple queries, and appends results into its own
// slot in workerResults — no cross-worker synchronization on output.
//
// Earlier shape: spawn len(tuples) goroutines (one per input tuple),
// each acquiring a slot of a numWorkers-slot semaphore. That paid a
// goroutine creation + two channel sends + a done-channel send per
// tuple. At hundreds of input tuples the scheduler primitives
// (runtime.lock2, runtime.usleep, pthread_cond_*) dominated profiles
// — see docs/perf/README.md for the baseline. The current shape
// uses numWorkers goroutines regardless of input size, one channel
// (jobs), and no inter-worker synchronization on output: each worker
// appends to workerResults[wIdx], a slot only that worker writes.
func (e *Executor) executeRealizedWithRelationInputIterationParallel(
	ctx Context,
	plan *planner.RealizedPlan,
	inputRelations []Relation,
	relationInput query.RelationInput,
	iterationRelation Relation,
	iterationIndex int,
) (Relation, error) {
	numWorkers := e.maxSubqueryWorkers
	if numWorkers <= 0 {
		numWorkers = 4 // Default to 4 workers
	}

	type iterationResult struct {
		result Relation
		err    error
	}

	// Small fixed buffer: producer doesn't run far ahead of workers,
	// workers don't starve waiting for the next item. Size scales with
	// worker count, not input size.
	jobs := make(chan Tuple, numWorkers*2)

	// Each worker appends only to its own slot. The slice-of-slices header
	// is shared, but each slot is written by exactly one worker, so the
	// writes do not race. wg.Wait below is the only synchronization point
	// between workers and the aggregator.
	workerResults := make([][]iterationResult, numWorkers)

	// Workers: long-lived, each consumes from jobs until closed. Each
	// worker forks the context once at startup — a Context's per-query
	// state (queryStart, metadata, scanRegistry) is not safe for concurrent
	// mutation between parallel workers, but it is safe for sequential
	// reuse within one worker (and beneficial when scan sharing is on,
	// since the worker's ScanRegistry then deduplicates across its tuples).
	//
	// Each worker owns its own preparedIteration: the QueryExecutor, the
	// rewritten modifiedQuery, and (fast path) the pre-built bound relation
	// + tuple workspace, or (fallback) a perTupleInputSession. All of
	// these are allocated once at worker startup; the per-tuple inner loop
	// is just `prepared.Run(ctx, tuple)`.
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		wIdx := w
		go func() {
			defer wg.Done()
			workerCtx := forkContext(ctx)
			prepared := e.prepareIteration(plan, relationInput, inputRelations, iterationIndex)
			for tuple := range jobs {
				result, err := prepared.Run(workerCtx, tuple)
				workerResults[wIdx] = append(workerResults[wIdx], iterationResult{result: result, err: err})
			}
		}()
	}

	// Producer (this goroutine): stream iterationRelation → jobs.
	// Backpressure flows naturally — if workers are slow, the bounded
	// jobs buffer fills and iter.Next() stalls, pacing the storage scan
	// to query throughput. Without a per-tuple results channel there is
	// no deadlock risk from running the producer in this goroutine.
	//
	// The iterator-workspace-reuse contract (relation.go:14-23): when the
	// source relation reports RequiresCopy()==true, its iterator's
	// Tuple() returns a workspace slice that subsequent Next() calls
	// overwrite. Sending that slice across the channel without copying
	// would race the producer's next overwrite against workers reading
	// their tuple. MaterializedRelation returns false (stable tuples,
	// no copy needed); StreamingRelation returns true (default for
	// storage-backed inputs).
	needsCopy := iterationRelation.RequiresCopy()
	iter := iterationRelation.Iterator()
	for iter.Next() {
		t := iter.Tuple()
		if needsCopy {
			t = copyTuple(t)
		}
		jobs <- t
	}
	iterErr := iter.Error()
	closeErr := iter.Close()
	close(jobs)

	wg.Wait()

	// Aggregate from per-worker slices. Consume each per-worker result via
	// collectTuplesInto so a failed scan's deferred error propagates rather
	// than being laundered into a clean result by Size() materialization.
	var allTuples []Tuple
	var symbols []query.Symbol
	var firstResultErr error
	for _, ws := range workerResults {
		for _, r := range ws {
			if r.err != nil {
				if firstResultErr == nil {
					firstResultErr = r.err
				}
				continue
			}
			if r.result == nil {
				continue
			}
			if symbols == nil {
				symbols = r.result.Symbols()
			}
			if err := collectTuplesInto(&allTuples, r.result); err != nil {
				if firstResultErr == nil {
					firstResultErr = err
				}
			}
		}
	}

	// Error priority: producer's iterator error first (it may have truncated
	// the input — anything past the failure point was never seen by workers),
	// then any per-worker error, then iter.Close() error.
	if iterErr != nil {
		return nil, iterErr
	}
	if firstResultErr != nil {
		return nil, fmt.Errorf("parallel iteration execution failed: %w", firstResultErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}

	if symbols == nil {
		return NewMaterializedRelation(extractFindSymbols(plan.Query.Find), []Tuple{}), nil
	}
	return NewMaterializedRelation(symbols, allTuples), nil
}

// preparedIteration encapsulates ALL the per-query state needed to execute
// a RelationInput iteration loop's per-tuple inner body, so callers (the
// parallel worker loop and the sequential path) reduce to:
//
//	prepared := e.prepareIteration(plan, relationInput, inputRelations, iterationIndex)
//	for tuple := range tuples {
//	    result, err := prepared.Run(ctx, tuple)
//	}
//
// Owned state, all pre-allocated once at prepare time:
//   - queryExecutor: the DefaultQueryExecutor (C1 hoist)
//   - modifiedQuery: plan.Query rewritten with RelationInput replaced by
//     N ScalarInputs (C1 hoist)
//   - Fast path (canMutateBindings == true): boundRelation +
//     boundTuple + iterationSlots — the bound relation is pre-built with
//     constant values baked in; per-tuple Run mutates iteration-derived
//     slots in boundTuple, then runs the phases (C2 workspace reuse)
//   - Fallback path (canMutateBindings == false, used when the query's
//     :in contains shapes the fast path can't handle, e.g. CollectionInput):
//     fallbackSession — a perTupleInputSession that Run feeds and passes
//     through BindQueryInputs per call
//
// Safe to reuse across sequential per-tuple calls within a single worker:
//   - DefaultQueryExecutor's mutable state (constantBindings) is only set
//     by ExecuteRealized, never inside this code path; reads of a nil map
//     return zero values in Go.
//   - modifiedQuery is read-only after construction.
//   - boundTuple mutation: phase execution reads boundTuple's interface{}
//     values into freshly-allocated result tuples (joins/projections build
//     new tuples by combining columns; aggregations build new summary
//     tuples). For primitive values (int64, string, bool, time.Time) the
//     interface{} carries the value; for pointer types (Identity, Keyword)
//     it carries the pointer. In both cases, copy-on-store at the consumer
//     means a previously-stored interface{} snapshot is independent of
//     subsequent boundTuple mutations.
//
// NOT safe to share across goroutines. Each worker creates its own.
type preparedIteration struct {
	executor      *Executor
	queryExecutor *DefaultQueryExecutor
	plan          *planner.RealizedPlan
	modifiedQuery *query.Query

	// Fast-path state (when canMutateBindings is true): pre-built bound
	// relation with mutable tuple slots. iterationSlots maps positions in
	// the iteration tuple to positions in boundTuple — iterationSlots[i]
	// is the slot in boundTuple where iteration tuple value [i] should be
	// written; -1 if that iteration position doesn't appear in the bound
	// relation (shouldn't happen in normal flow but handled defensively).
	boundRelation     *MaterializedRelation
	boundTuple        Tuple
	iterationSlots    []int
	canMutateBindings bool

	// Fallback-path state (when canMutateBindings is false): a
	// perTupleInputSession that Run feeds and then passes through
	// BindQueryInputs per call. Nil when the fast path is active — no
	// session is allocated in that case.
	fallbackSession *perTupleInputSession
}

// prepareIteration builds the reusable per-query state for one worker's
// (or the sequential path's) per-tuple iteration loop. All allocations
// performed here happen ONCE; the returned struct's Run method does the
// per-tuple work without repeating any of this setup.
//
// The inputRelations and iterationIndex are used to read CONSTANT scalar
// values (those whose symbols are not in relationInput.Symbols) at prepare
// time, so the fast path can bake them into boundTuple instead of
// re-reading per call.
//
// Fast path activates only when modifiedQuery.In contains exclusively
// DatabaseInputs, ScalarInputs, and TupleInputs — all of which produce a
// single-tuple bound relation. CollectionInput or any other RelationInput
// would produce a multi-tuple bound relation; in those cases
// canMutateBindings stays false and Run uses a fallbackSession +
// BindQueryInputs per call. The session is only allocated when needed.
func (e *Executor) prepareIteration(plan *planner.RealizedPlan, relationInput query.RelationInput, inputRelations []Relation, iterationIndex int) *preparedIteration {
	queryExecutor := newQueryExecutor(e.matcher, e.entityResolver, e.options)

	// Rewrite plan.Query's :in clause: replace the RelationInput with one
	// ScalarInput per RelationInput symbol.
	modifiedQuery := *plan.Query
	newIn := make([]query.InputSpec, 0, len(modifiedQuery.In)+len(relationInput.Symbols)-1)
	for _, input := range modifiedQuery.In {
		if _, isRelInput := input.(query.RelationInput); isRelInput {
			for _, sym := range relationInput.Symbols {
				newIn = append(newIn, query.ScalarInput{Symbol: sym})
			}
		} else {
			newIn = append(newIn, input)
		}
	}
	modifiedQuery.In = newIn

	p := &preparedIteration{
		executor:      e,
		queryExecutor: queryExecutor,
		plan:          plan,
		modifiedQuery: &modifiedQuery,
	}

	// Try to pre-build the bound relation (fast path). On success,
	// canMutateBindings is set and the fallback session is NOT allocated.
	p.buildBoundRelation(relationInput, inputRelations, iterationIndex)

	// If the fast path didn't activate, build the fallback session so Run
	// can take the per-call BindQueryInputs path.
	if !p.canMutateBindings {
		builder := newPerTupleInputBuilder(inputRelations, iterationIndex, relationInput)
		p.fallbackSession = builder.Session()
	}

	return p
}

// buildBoundRelation tries to pre-build a workspace-reusable bound relation
// for the fast path. Sets p.boundRelation, p.boundTuple, p.iterationSlots,
// and p.canMutateBindings on success; leaves them at zero values on
// fallback.
//
// The walk: modifiedQuery.In and inputRelations are kept in lockstep
// (DatabaseInputs in modifiedQuery.In don't consume an inputRelations
// slot). For each ScalarInput / TupleInput a slot is appended to
// boundTuple. ScalarInputs whose symbol is in relationInput.Symbols are
// iteration-derived (slot tracked in iterationSlots). Other ScalarInputs
// and all TupleInputs are constant — their values are read from
// inputRelations now and baked into boundTuple. CollectionInput or any
// other shape aborts the optimization.
func (p *preparedIteration) buildBoundRelation(relationInput query.RelationInput, inputRelations []Relation, iterationIndex int) {
	// Map RelationInput symbols to their position in the iteration tuple,
	// so we can resolve ScalarInputs derived from RelationInput rewrite
	// back to the iteration tuple position they'll be written from.
	iterSymToPos := make(map[query.Symbol]int, len(relationInput.Symbols))
	for i, sym := range relationInput.Symbols {
		iterSymToPos[sym] = i
	}

	// The inputRelations slice matches the ORIGINAL plan.Query.In layout
	// (RelationInput at iterationIndex, scalars/collections around it).
	// We need to look up scalar/tuple values from inputRelations using
	// the original layout, not the rewritten modifiedQuery.In.
	// originalInIndex walks plan.Query.In; relationIndex walks the
	// (DatabaseInput-skipped) inputRelations slice.
	var boundSymbols []query.Symbol
	var boundTuple Tuple
	iterationSlots := make([]int, len(relationInput.Symbols))
	for i := range iterationSlots {
		iterationSlots[i] = -1
	}

	relationIndex := 0
	for _, input := range p.plan.Query.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			// DatabaseInput doesn't consume an inputRelations slot.
			continue
		case query.RelationInput:
			// The RelationInput is the iteration source. It expands to
			// len(inp.Symbols) ScalarInputs in modifiedQuery.In, each
			// iteration-derived (slot mutated per Run).
			for i, sym := range inp.Symbols {
				slotIdx := len(boundTuple)
				boundSymbols = append(boundSymbols, sym)
				boundTuple = append(boundTuple, nil) // filled by Run
				if pos, ok := iterSymToPos[sym]; ok {
					iterationSlots[pos] = slotIdx
				}
				_ = i
			}
			relationIndex++
		case query.ScalarInput:
			// Constant scalar input — read value from inputRelation now.
			slotIdx := len(boundTuple)
			boundSymbols = append(boundSymbols, inp.Symbol)
			var v interface{}
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if rel != nil && rel.Size() > 0 {
					it := rel.Iterator()
					if it.Next() {
						t := it.Tuple()
						if len(t) > 0 {
							v = t[0]
						}
					}
					it.Close()
				}
			}
			boundTuple = append(boundTuple, v)
			_ = slotIdx
			relationIndex++
		case query.TupleInput:
			// Constant tuple input — read values from inputRelation now.
			if relationIndex < len(inputRelations) {
				rel := inputRelations[relationIndex]
				if rel != nil && rel.Size() > 0 && len(inp.Symbols) == len(rel.Symbols()) {
					it := rel.Iterator()
					if it.Next() {
						t := it.Tuple()
						for i, sym := range inp.Symbols {
							boundSymbols = append(boundSymbols, sym)
							var v interface{}
							if i < len(t) {
								v = t[i]
							}
							boundTuple = append(boundTuple, v)
						}
					}
					it.Close()
				}
			}
			relationIndex++
		default:
			// CollectionInput, or any other unhandled shape — fall back to
			// per-call BindQueryInputs.
			return
		}
	}

	// If anything was bound, construct the workspace-reusable bound
	// relation. Direct struct construction bypasses NewMaterializedRelation's
	// deduplicateTuples — which would allocate a TupleKeyMap + TupleKey +
	// result slice for a known-singleton input. The relation's tuples[0]
	// IS boundTuple (the slice header is wrapped in a fresh outer slice,
	// but tuples[0] points to the same Tuple), so mutating boundTuple
	// mutates what the relation yields.
	if len(boundSymbols) > 0 {
		p.boundRelation = &MaterializedRelation{
			symbols: boundSymbols,
			tuples:  []Tuple{boundTuple},
			options: ExecutorOptions{},
		}
		p.boundTuple = boundTuple
		p.iterationSlots = iterationSlots
		p.canMutateBindings = true
	}
}

// Run executes one per-tuple query using the prepared state.
// iterationTuple is the current tuple from the iteration relation; Run
// internally picks the fast path (mutate pre-built bound relation) or
// the fallback path (session.Update + BindQueryInputs) based on the
// flags set at prepare time.
func (p *preparedIteration) Run(ctx Context, iterationTuple Tuple) (Relation, error) {
	var currentGroups []Relation

	if p.canMutateBindings {
		// Fast path: mutate the pre-built boundTuple's slots from the
		// iteration tuple values.
		for i, slot := range p.iterationSlots {
			if slot >= 0 && i < len(iterationTuple) {
				p.boundTuple[slot] = iterationTuple[i]
			}
		}
		currentGroups = []Relation{p.boundRelation}
	} else if p.fallbackSession != nil {
		// Fallback: feed the session, run per-call BindQueryInputs.
		p.fallbackSession.Update(iterationTuple)
		boundRelation := BindQueryInputs(p.modifiedQuery, p.fallbackSession.Input())
		currentGroups = []Relation{boundRelation}
	}

	// Execute each phase as an independent query.
	for i, phase := range p.plan.Phases {
		phaseIndex := i
		isLastPhase := (i == len(p.plan.Phases)-1)

		groups, err := p.queryExecutor.Execute(ctx, phase.Query, currentGroups)
		if err != nil {
			return nil, fmt.Errorf("phase %d failed: %w", phaseIndex+1, err)
		}

		// Project each group to Keep symbols. Skip for the last phase —
		// QueryExecutor already projected to :find symbols.
		if !isLastPhase && len(phase.Keep) > 0 {
			for i, group := range groups {
				// Materialize first to avoid iterator consumption issues, and
				// carry any deferred scan error onto the materialized relation
				// so Project propagates it into the next phase instead of
				// laundering a failed scan into an empty result.
				var tuples []Tuple
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

		// Early termination on empty.
		if len(groups) == 0 {
			return emptyRelationForQuery(p.plan.Query), nil
		}

		// Last phase must collapse to a single relation (error on Cartesian).
		if isLastPhase && len(groups) > 1 {
			return nil, fmt.Errorf("phase %d resulted in %d disjoint relation groups", phaseIndex+1, len(groups))
		}

		currentGroups = groups
	}

	if len(currentGroups) == 0 {
		return emptyRelationForQuery(p.plan.Query), nil
	}

	finalResult := currentGroups[0]

	if len(p.plan.Query.OrderBy) > 0 {
		finalResult = finalResult.Sort(p.plan.Query.OrderBy)
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
