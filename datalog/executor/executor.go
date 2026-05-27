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

	// Hoist the prefix/suffix split AND the scratch input relations out of
	// the iteration loop; only the per-symbol values change per tuple (see
	// perTupleInputSession for the workspace-reuse pattern). Likewise hoist
	// the QueryExecutor and the rewritten modifiedQuery via prepared.
	builder := newPerTupleInputBuilder(inputRelations, iterationIndex, relationInput)
	session := builder.Session()
	prepared := e.prepareNonIterating(plan, relationInput)

	// Iterate over each tuple in the relation
	it := iterationRelation.Iterator()
	defer it.Close()

	for it.Next() {
		// Per-tuple inputs: update the session's scratch value slots; the
		// session's pre-wired inputList already references the scratch
		// MaterializedRelations.
		session.Update(it.Tuple())

		// Execute the plan with these scalar inputs using the prepared
		// non-iterating executor.
		result, err := prepared.Run(ctx, session.Input())
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

	// Hoist per-query work out of the inner loop: the prefix/suffix split
	// of inputRelations and the relation-input symbol list are identical
	// for every tuple, so compute them once.
	builder := newPerTupleInputBuilder(inputRelations, iterationIndex, relationInput)

	// Workers: long-lived, each consumes from jobs until closed. Each
	// worker forks the context once at startup — a Context's per-query
	// state (queryStart, metadata, scanRegistry) is not safe for concurrent
	// mutation between parallel workers, but it is safe for sequential
	// reuse within one worker (and beneficial when scan sharing is on,
	// since the worker's ScanRegistry then deduplicates across its tuples).
	//
	// Each worker owns its own perTupleInputSession and prepared executor:
	// the N scratch MaterializedRelations, the wired-up inputList, the
	// QueryExecutor, and the rewritten modifiedQuery are all allocated
	// once at worker startup. The per-tuple inner loop only does N value
	// writes (session.Update) and one Run call — no allocation for setup.
	// This mirrors the BuildTupleInternedInto / it.workspace workspace-reuse
	// pattern used by the storage iterators.
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		wIdx := w
		go func() {
			defer wg.Done()
			workerCtx := forkContext(ctx)
			session := builder.Session()
			prepared := e.prepareNonIterating(plan, relationInput)
			for tuple := range jobs {
				session.Update(tuple)
				result, err := prepared.Run(workerCtx, session.Input())
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

// preparedNonIteratingExecution holds the per-query state that doesn't vary
// across per-tuple calls during RelationInput iteration: the QueryExecutor,
// the modifiedQuery (with RelationInput rewritten to ScalarInputs), and the
// plan reference. Pre-allocating these once per worker — instead of once
// per tuple — eliminates the per-call QueryExecutor allocation, the
// modifiedQuery struct copy, and the newIn slice + N ScalarInput struct
// allocations from the hot path.
//
// Safe to reuse across sequential per-tuple calls within a single worker:
//   - DefaultQueryExecutor's mutable state (constantBindings) is only set
//     by ExecuteRealized, never inside this code path; reads of a nil map
//     return zero values in Go.
//   - modifiedQuery is read-only after construction.
//
// NOT safe to share across goroutines. Each worker creates its own.
type preparedNonIteratingExecution struct {
	executor      *Executor
	queryExecutor *DefaultQueryExecutor
	plan          *planner.RealizedPlan
	modifiedQuery *query.Query
}

// prepareNonIterating builds the reusable per-query state for one worker's
// (or the sequential path's) per-tuple iteration loop. Allocations here
// happen ONCE; the returned struct's Run method does the per-tuple work
// without repeating any of this setup.
func (e *Executor) prepareNonIterating(plan *planner.RealizedPlan, relationInput query.RelationInput) *preparedNonIteratingExecution {
	queryExecutor := newQueryExecutor(e.matcher, e.entityResolver, e.options)

	// Rewrite plan.Query's :in clause: replace the RelationInput with one
	// ScalarInput per RelationInput symbol. The rewritten query is what
	// BindQueryInputs binds per call.
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

	return &preparedNonIteratingExecution{
		executor:      e,
		queryExecutor: queryExecutor,
		plan:          plan,
		modifiedQuery: &modifiedQuery,
	}
}

// Run executes one per-tuple query using the prepared state. The caller
// supplies the per-tuple scalarInputRelations (typically from a
// perTupleInputSession's Input()), which BindQueryInputs binds against
// the prepared modifiedQuery.
func (p *preparedNonIteratingExecution) Run(ctx Context, scalarInputRelations []Relation) (Relation, error) {
	var currentGroups []Relation

	// Bind scalar input relations against the prepared modifiedQuery.
	if len(scalarInputRelations) > 0 {
		boundRelation := BindQueryInputs(p.modifiedQuery, scalarInputRelations)
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
