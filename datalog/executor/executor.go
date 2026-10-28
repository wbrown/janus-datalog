package executor

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Executor is the main query execution engine
type Executor struct {
	matcher        PatternMatcher
	entityResolver EntityResolver
	planner        planner.QueryPlanner
	options        ExecutorOptions
	// unit is the relation of one empty tuple over no symbol, the identity
	// for joins: the verdict that holds, the run of a branch over no required
	// run, the subject of a form over no group. A relation is immutable, so
	// every clause executor this executor builds reads this one.
	unit *MaterializedRelation
}

// defaultPlannerOptions is the planner profile NewExecutor constructs with —
// the single definition, consumed by NewExecutor and by the test suite's
// optimizer mode axis so the two can never drift.
func defaultPlannerOptions() planner.PlannerOptions {
	return planner.PlannerOptions{
		EnableTrueStreaming:        true,
		EnableSymmetricHashJoin:    false,
		EnableStreamingJoins:       false,
		EnableStreamingAggregation: true,
		EnableAttributeFetchFusion: true,
	}
}

// NewExecutor creates a new query executor with default options
func NewExecutor(matcher PatternMatcher, resolver EntityResolver) *Executor {
	return NewExecutorWithOptions(matcher, resolver, defaultPlannerOptions())
}

// NewExecutorWithOptions creates a new query executor with custom planner
// options. Its plans carry their rosters when a source the matcher routes to
// refuses attributes its schema does not declare, so the check at the
// executor's entry reads a roster from every plan its own planner makes.
func NewExecutorWithOptions(matcher PatternMatcher, resolver EntityResolver, opts planner.PlannerOptions) *Executor {
	// Convert to executor options
	execOpts := ExecutorOptionsFromPlanner(opts)

	// Configure matcher with executor options if it supports it
	if indexedMatcher, ok := matcher.(*IndexedMemoryMatcher); ok {
		indexedMatcher.WithOptions(execOpts)
	}

	opts.CollectRosters = opts.CollectRosters || anySourceRefusesUnknownAttributes(matcher)

	// Create the appropriate planner based on options
	queryPlanner := planner.CreatePlanner(nil, opts)

	return &Executor{
		matcher:        matcher,
		entityResolver: resolver,
		planner:        queryPlanner,
		options:        execOpts,
		unit:           NewUnitRelation(execOpts),
	}
}

// anySourceRefusesUnknownAttributes reports whether a source the matcher
// routes to refuses attributes its schema does not declare: a router is
// asked about the sources it holds, and any other matcher about itself as $.
// A matcher without the capability refuses nothing.
func anySourceRefusesUnknownAttributes(matcher PatternMatcher) bool {
	switch m := matcher.(type) {
	case *SourceRouter:
		return m.refusesAnySource()
	case AttributeVocabulary:
		return m.RefusesUnknownAttributes(datalog.SymDollar)
	}
	return false
}

// ExecutorOptionsFromPlanner is the single source of truth for deriving
// ExecutorOptions from PlannerOptions. Both the executor (NewExecutorWithOptions)
// and the storage default-source matcher (storage.Database.Matcher) convert
// through this function so they cannot disagree on which fields cross the
// boundary — every PlannerOptions field that has an ExecutorOptions counterpart
// is copied here. DefaultHashTableSize has no PlannerOptions counterpart and is
// left at its zero value.
func ExecutorOptionsFromPlanner(opts planner.PlannerOptions) ExecutorOptions {
	return ExecutorOptions{
		Handler:                    opts.Handler,
		EnableTrueStreaming:        opts.EnableTrueStreaming,
		EnableSymmetricHashJoin:    opts.EnableSymmetricHashJoin,
		EnableStreamingJoins:       opts.EnableStreamingJoins,
		EnableStreamingAggregation: opts.EnableStreamingAggregation,
		EnableScanSharing:          opts.EnableScanSharing,
		EnableEntityPrefetch:       opts.EnableEntityPrefetch,
		EnableAttributeFetchFusion: opts.EnableAttributeFetchFusion,
		MaxRuleIterations:          opts.MaxRuleIterations,
		MaxDerivedTuples:           opts.MaxDerivedTuples,
		ScopeWindowCap:             opts.ScopeWindowCap,
		ScopeWindowGrowth:          opts.ScopeWindowGrowth,
	}
}

// Execute runs a parsed query and returns the results
func (e *Executor) Execute(q *query.Query) (Relation, error) {
	return e.ExecuteWithContext(NewContext(), q)
}

// ExecuteWithContext runs a parsed query with annotation support
func (e *Executor) ExecuteWithContext(ctx *Context, q *query.Query) (Relation, error) {
	// Delegate to ExecuteWithRelations with empty input relations
	return e.ExecuteWithRelations(ctx, q, []Relation{})
}

// refuseUnknownAttributes asks each source the query runs over, $ and each
// named source its :in declares, when that source refuses attributes its
// schema does not declare, about the attributes the plan's roster names on
// it and the ones the rule set's bodies name on it. The roster is the
// plan's, collected at planning from the prepared query under
// PlannerOptions.CollectRosters and read here after it, so the check runs on
// both planner modes alike and reads nothing of the execution. A source that
// does not refuse is asked about nothing, and its roster is not read, so a
// plan over sources that refuse nothing carries none. The first refusal is
// returned.
func refuseUnknownAttributes(vocabulary AttributeVocabulary, ctx *Context, plan *planner.RealizedPlan) error {
	sources := []query.Symbol{datalog.SymDollar}
	for _, input := range plan.Query.In {
		if source, ok := input.(query.DatabaseInput); ok && source.Name != datalog.SymDollar {
			sources = append(sources, source.Name)
		}
	}
	for _, source := range sources {
		if !vocabulary.RefusesUnknownAttributes(source) {
			continue
		}
		if err := vocabulary.UnknownAttributes(source, plan.Rosters.SourceAttributes(source)); err != nil {
			return err
		}
		if ctx.rules != nil {
			if err := vocabulary.UnknownAttributes(source, ctx.rules.SourceAttributes(source)); err != nil {
				return err
			}
		}
	}
	return nil
}

// ExecuteWithRelations runs a query with its input relations: one per :in
// entry that binds variables (a scalar, collection, tuple or relation
// input), in the :in's order, and none for a source or the rule set. Any
// other count is refused before planning, naming the count given and the
// count the :in binds. A query whose :in binds nothing takes an empty slice.
func (e *Executor) ExecuteWithRelations(ctx *Context, q *query.Query, inputRelations []Relation) (Relation, error) {
	// Apply decorator pattern: wrap matcher with annotations if one is registered
	matcher := e.matcher

	// Registered on the options, so every executor, matcher, and relation built
	// under them already has it. Read once here: it governs the wrapping below
	// and every emit on this path.
	handler := e.options.Handler

	// Wrap with scan sharing if enabled — must come before annotation wrapping
	// so that annotations see the sharing layer's decisions
	if e.options.EnableScanSharing {
		matcher = NewScanSharingMatcher(matcher, ctx.ScanRegistry(), handler)
	}

	matcher = WrapMatcher(matcher, handler)

	// Create a temporary executor with the wrapped matcher
	executor := &Executor{
		matcher:        matcher,
		entityResolver: e.entityResolver,
		planner:        e.planner,
		options:        e.options,
		unit:           e.unit,
	}

	// q.String() renders the whole query, so the guard is here rather than
	// inside an emit that would have received it already rendered.
	var queryStart time.Time
	if handler != nil {
		queryStart = time.Now()
		handler(annotations.Event{
			Name:  annotations.QueryInvoked,
			Start: queryStart,
			Data:  map[string]interface{}{annotations.KeyQuery: q.String()},
		})
	}

	// A query that fails after it was invoked, refused at this entry or failed
	// in execution, completes with its error.
	failed := func(err error) (Relation, error) {
		if handler != nil {
			handler(annotations.TimedEvent(annotations.QueryComplete, queryStart,
				map[string]interface{}{
					annotations.KeyRelationsCount: 0,
					annotations.KeyTuplesCount:    0,
					annotations.KeySuccess:        false,
					annotations.KeyError:          err.Error(),
				}))
		}
		return nil, err
	}

	// A query whose context is already done is refused before any work.
	if err := ctx.cancelled(); err != nil {
		return failed(err)
	}

	// The query is prepared once, before planning: its shapes checked, every
	// nested form stating its header, every invocation resolved under the
	// rule set. Nothing after this point sees a headerless form. The
	// preparation is reported with the prepared query, whether the inference
	// rewrote a form, which InferHeaders states by returning the query it was
	// given when it rewrote none, and preparation's time; prepared.String()
	// renders the whole query, so the guard is here.
	var prepareStart time.Time
	if handler != nil {
		prepareStart = time.Now()
	}
	prepared, err := query.Prepare(q, ctx.rules)
	if err != nil {
		return failed(err)
	}
	if handler != nil {
		handler(annotations.TimedEvent(annotations.QueryPrepared, prepareStart,
			map[string]interface{}{
				annotations.KeyQuery: prepared.String(),
				"rewritten":          prepared != q,
			}))
	}

	// The relations given are the relations the :in binds, one per scalar,
	// collection, tuple or relation input in the :in's order and none for a
	// source or the rule set. Any other count is refused here, before the
	// binding walk and before planning, so a short slice never leaves an
	// input's variables free.
	bindingInputs := query.BindingInputs(q.In)
	if len(inputRelations) != len(bindingInputs) {
		return failed(fmt.Errorf("%d input relations were given, and the query's :in binds %d", len(inputRelations), len(bindingInputs)))
	}

	// Build initial bindings from the input relations: a relation input binds
	// its symbols whatever its size, and a scalar, tuple or collection input
	// binds its symbols when its relation holds a tuple.
	initialBindings := make(map[query.Symbol]bool)
	for i, input := range bindingInputs {
		switch inp := input.(type) {
		case query.ScalarInput:
			if inputRelations[i].Size() > 0 {
				initialBindings[inp.Symbol] = true
			}
		case query.RelationInput:
			for _, sym := range inp.Symbols {
				initialBindings[sym] = true
			}
		case query.TupleInput:
			if inputRelations[i].Size() > 0 {
				for _, sym := range inp.Symbols {
					initialBindings[sym] = true
				}
			}
		case query.CollectionInput:
			if inputRelations[i].Size() > 0 {
				initialBindings[inp.Symbol] = true
			}
		}
	}

	// Execute using QueryExecutor (Stage B) with RealizedPlan. The annotation
	// handler is passed per-query into planning (not stored on the shared
	// planner), so concurrent annotated queries neither race on it nor
	// cross-route algebra-bridge events.
	realizedPlan, err := executor.planner.PlanQuery(prepared, query.Bindings{Symbols: initialBindings, Rules: ctx.rules}, handler)
	if err != nil {
		return failed(fmt.Errorf("query planning failed: %w", err))
	}
	// realizedPlan.String() renders the whole plan, so the guard is here.
	if handler != nil {
		handler(annotations.Event{
			Name:  annotations.QueryPlanCreated,
			Start: time.Now(),
			Data:  map[string]interface{}{"plan": realizedPlan.String()},
		})
	}

	// An attribute a source's schema does not declare is refused against
	// the roster the plan holds, before any phase runs. The matcher owns the
	// schema, so the decision is its own.
	if vocabulary, ok := e.matcher.(AttributeVocabulary); ok {
		if err := refuseUnknownAttributes(vocabulary, ctx, realizedPlan); err != nil {
			return failed(err)
		}
	}
	result, err := executor.ExecuteRealized(ctx, realizedPlan, inputRelations)
	if err != nil {
		return failed(err)
	}
	return result, nil
}

// ExecuteRealized executes a RealizedPlan (Stage B: Query-based execution)
// This is the simplified executor that consumes Query fragments from the planner.
//
// Key semantics:
// - Each phase executes as independent Query returning []Relation (disjoint groups)
// - Groups are projected to Keep symbols and passed to next phase
// - Final phase must collapse to single relation or error on Cartesian product
//
// Finalization (:order-by then :limit) is applied here, at the single outer
// boundary, on the fully-assembled result, whatever the :in binds: the
// inputs enter the phases as one bound group, so the result is one relation
// ordered and capped once. Sorting before limiting yields a correct global
// top-N.
func (e *Executor) ExecuteRealized(ctx *Context, plan *planner.RealizedPlan, inputRelations []Relation) (Relation, error) {
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("invalid realized plan: %w", err)
	}
	result, err := e.executeRealizedPlan(ctx, plan, inputRelations)
	if err != nil {
		return nil, err
	}
	limitApplied := false
	if len(plan.Query.OrderBy) > 0 {
		// Sort keys on constant inputs (scalar/tuple :in) are identity
		// sorts — dropped here, observably rather than silently.
		effective := query.EffectiveOrderBy(plan.Query)
		if dropped := len(plan.Query.OrderBy) - len(effective); dropped > 0 {
			if handler := e.options.Handler; handler != nil {
				handler(annotations.Event{
					Name: annotations.SortConstantKeysDropped,
					Data: map[string]interface{}{
						"count": dropped,
					},
				})
			}
		}
		if len(effective) > 0 {
			retained := query.RetainedSortSymbols(plan.Query)
			orderSatisfied := plan.Query.Limit != nil &&
				result.Properties().satisfiesOrdering(effective)
			if orderSatisfied {
				// The physical relation already supplies the requested Datalog
				// order. Leave it streaming; the limit applied below can stop
				// the source iterator after N tuples. If retained sort symbols
				// are projected away, projection/dedup runs before that limit.
			} else if plan.Query.Limit != nil && len(retained) == 0 {
				// No post-sort projection can collapse tuples, so bounded Top-N
				// is equivalent to full sort followed by limit.
				result = TopNRelation(result, effective, *plan.Query.Limit)
				limitApplied = true
			} else {
				result = result.Sort(effective)
			}
			// The planner retained non-projected sort symbols through the
			// final projection so the sort above could resolve them; strip
			// the result back to the declared :find shape. The projection
			// deduplicates (set semantics), keeping each duplicate's first
			// occurrence in sorted order.
			if len(retained) > 0 {
				result, err = result.Project(extractFindSymbols(plan.Query.Find))
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if plan.Query.Limit != nil && !limitApplied {
		result = NewLimitRelation(result, *plan.Query.Limit)
	}
	// Pull rendering is result presentation, not a relational operation:
	// pulled maps are not datalog values, so they are produced here, at the
	// terminal boundary — after sort, strip, and limit — for exactly the
	// tuples the query returns. Entity bindings are Identity values through
	// every relational operation above. Aggregate find clauses do not apply
	// pulls. See docs/bugs/resolved/BUG_PULL_WITH_ORDER_BY_PANICS.md.
	if hasPulls(plan.Query.Find) {
		hasAggregates := false
		for _, elem := range plan.Query.Find {
			if elem.IsAggregate() {
				hasAggregates = true
				break
			}
		}
		if !hasAggregates {
			result, err = applyFindPulls(e.matcher, e.entityResolver, result, plan.Query.Find)
			if err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

// queryExecutor builds the clause executor for one run of plan. It carries
// the executor itself, which an invocation clause needs to run a fixpoint,
// and the plan, whose evaluations an invocation clause reads.
func (e *Executor) queryExecutor(plan *planner.RealizedPlan) *DefaultQueryExecutor {
	return &DefaultQueryExecutor{
		matcher:        e.matcher,
		entityResolver: e.entityResolver,
		options:        e.options,
		executor:       e,
		plan:           plan,
	}
}

// executeRealizedPlan runs the phase pipeline and returns the assembled result
// (un-ordered, un-limited). ExecuteRealized applies :order-by and :limit to it.
func (e *Executor) executeRealizedPlan(ctx *Context, plan *planner.RealizedPlan, inputRelations []Relation) (Relation, error) {
	var currentGroups []Relation

	// The inputs enter as one bound group, the relation the first phase
	// joins, whatever the form of each :in entry.
	if len(inputRelations) > 0 && len(plan.Phases) > 0 {
		boundRelation, err := BindQueryInputs(plan.Query, inputRelations)
		if err != nil {
			return nil, err
		}
		currentGroups = []Relation{boundRelation}
	}

	// Bind the query's environment and extract constant-only scalar inputs.
	//
	// The environment is one single-tuple relation over the query's
	// single-valued :in parameters (scalar and tuple inputs), projected out
	// of the bound input relation and carried on the execution context: it
	// is ambient in every clause scope of this query, including or-branch
	// bodies, and reaches consumers by join. Projection's set semantics
	// collapse collection-input multiplicity — the environment's symbols take
	// one value across every tuple of the bound relation. Scalars the planner
	// proved constant-only (declared as phase ScalarInputs) are additionally
	// projected out of the data relation so they don't form disjoint groups;
	// pattern-consumed scalars keep their relation symbols for
	// index-narrowed matching, and the environment retains what
	// projection-out removes.
	if len(currentGroups) > 0 {
		envBindable := environmentSymbolsOf(plan.Query.In)
		if len(envBindable) > 0 {
			boundRel := currentGroups[0]
			syms := boundRel.Symbols()

			var present []query.Symbol
			for _, sym := range envBindable {
				if query.ContainsSymbol(syms, sym) {
					present = append(present, sym)
				}
			}

			var env Relation
			if len(present) > 0 {
				projected, err := boundRel.Project(present)
				if err != nil {
					return nil, fmt.Errorf("environment projection failed: %w", err)
				}
				if projected.Size() == 1 {
					env = projected
				}
				// Size 0: the bound inputs are empty (an empty collection
				// input) — no joint parameter binding exists; the main
				// binding path reports the empty result.
			}

			if env != nil {
				ctx = ctx.WithEnvironment(env)

				// Constant-only scalars per the phase Datalog :in clauses.
				allConstBindable := make(map[query.Symbol]bool)
				for _, phase := range plan.Phases {
					if phase.Query == nil {
						continue
					}
					for _, input := range phase.Query.In {
						if scalar, ok := input.(query.ScalarInput); ok {
							allConstBindable[scalar.Symbol] = true
						}
					}
				}
				constSymIndices := make(map[int]bool)
				for i, sym := range syms {
					if allConstBindable[sym] {
						constSymIndices[i] = true
					}
				}

				if len(constSymIndices) > 0 {
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

	return e.runPhases(ctx, plan, currentGroups, e.options.Handler)
}

// runPhases runs a plan's phases over the groups that enter the first and
// returns the last phase's one relation, reporting the plan's conditional
// aggregates and each phase's begin and completion to handler. A query's run
// reports to the executor's handler. A scope's run reports to none: the
// form's own events report a branch's execution, and a phase event per
// window would be a handler call per window beside the window's own event.
func (e *Executor) runPhases(ctx *Context, plan *planner.RealizedPlan, currentGroups []Relation, handler annotations.Handler) (Relation, error) {
	queryExecutor := e.queryExecutor(plan)

	// Count conditional aggregates directly from the Datalog phase fragments.
	if handler != nil {
		var condAggCount int
		for _, phase := range plan.Phases {
			if phase.Query == nil {
				continue
			}
			for _, findElement := range phase.Query.Find {
				if aggregate, ok := findElement.(query.FindAggregate); ok && aggregate.IsConditional() {
					condAggCount++
				}
			}
		}
		if condAggCount > 0 {
			handler(annotations.Event{
				Name: annotations.QueryRewriteConditionalAggregates,
				Data: map[string]interface{}{
					"rewritten.subquery.count": condAggCount,
					"optimization":             "conditional-aggregate-rewriting",
				},
			})
		}
	}

	// Execute each phase as an independent query
	for i, phase := range plan.Phases {
		phaseIndex := i
		isLastPhase := (i == len(plan.Phases)-1)

		// One question, asked once: its answer governs both the clock read and
		// the two emits below.
		var phaseStart time.Time
		if handler != nil {
			phaseStart = time.Now()
			handler(annotations.Event{
				Name:  annotations.PhaseBegin,
				Start: phaseStart,
				Data: map[string]interface{}{
					"phase":        phaseIndex + 1,
					"input_groups": len(currentGroups),
					"keep":         phase.Keep,
					"query":        phase.Query,
				},
			})
		}

		groups, err := queryExecutor.executePhase(ctx, phaseIndex+1, phase, currentGroups)
		if err != nil {
			return nil, err
		}

		// Non-final Query fragments already emit their exact Keep schema. Make
		// each result reusable for the next phase without projecting it again.
		if !isLastPhase && len(phase.Keep) > 0 {
			for i, group := range groups {
				groups[i] = materializePhaseBoundary(group)
			}
		}

		// Completion is reported after boundary materialization, so the phase's
		// duration covers the whole of its work and its size is free to ask for
		// on every phase that materialized. A streaming final phase declines
		// with -1 rather than being consumed to produce a number.
		if handler != nil {
			handler(annotations.TimedEvent(annotations.PhaseComplete, phaseStart, map[string]interface{}{
				"phase":       phaseIndex + 1,
				"groups":      len(groups),
				"keep":        phase.Keep,
				"tuple.count": declaredTupleCount(groups),
			}))
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

	// :order-by and :limit are applied by ExecuteRealized on the final result.
	return currentGroups[0], nil
}

// declaredTupleCount totals the tuples across a phase's output groups, or
// returns -1 if any group declines to answer.
//
// It only ever asks Size(), never iterates: a streaming relation answers from
// its cache or its completed-consumption counter and otherwise returns -1
// rather than touching its source. Reporting a phase's size must not be the
// call that spends the phase's tuples, so one declining group makes the whole
// count unknown instead of provoking a realization to fill the gap.
func declaredTupleCount(groups []Relation) int {
	total := 0
	for _, group := range groups {
		size := group.Size()
		if size < 0 {
			return -1
		}
		total += size
	}
	return total
}

// executePhase runs one phase's fragment over the groups entering it and
// returns the groups the phase provides, checked against its Provides. A
// fragment finding nothing yields the existence verdict over its result,
// the schema a phase providing nothing declares; otherwise the phase
// provides the fragment's own result. A failure of the fragment or of the
// verdict is named by the phase's number.
func (e *DefaultQueryExecutor) executePhase(ctx *Context, phaseNumber int, phase planner.RealizedPhase, groups []Relation) ([]Relation, error) {
	result, err := e.Execute(ctx, phase.Query, groups)
	if err != nil {
		return nil, fmt.Errorf("phase %d failed: %w", phaseNumber, err)
	}
	if len(phase.Query.Find) == 0 {
		result, err = existenceVerdict(result, e.executor.unit)
		if err != nil {
			return nil, fmt.Errorf("phase %d failed: %w", phaseNumber, err)
		}
	}
	if err := validatePhaseOutput(phaseNumber, phase, result); err != nil {
		return nil, err
	}
	return result, nil
}

func validatePhaseOutput(phaseNumber int, phase planner.RealizedPhase, groups []Relation) error {
	for groupIndex, group := range groups {
		actual := group.Symbols()
		if len(actual) != len(phase.Provides) {
			return fmt.Errorf("phase %d group %d physical output schema %v does not match provides schema %v",
				phaseNumber, groupIndex, actual, phase.Provides)
		}
		for i := range actual {
			if actual[i] != phase.Provides[i] {
				return fmt.Errorf("phase %d group %d physical output schema %v does not match provides schema %v",
					phaseNumber, groupIndex, actual, phase.Provides)
			}
		}
	}
	return nil
}

// SetPlanCache sets the plan cache on the executor's planner, through which
// every scope, the rewrite's included, is planned.
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
