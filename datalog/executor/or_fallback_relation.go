package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// OrFallbackRelation is the form's result, the relation the two branch
// forms yield: a streaming relation over a subject, each branch planned once
// as a scope under the form's header, run once per window of the subject
// and probed per window tuple through its index, which is kept across the
// windows (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2 and §4.6).
// Under shortCircuit the first branch with a result for a subject tuple
// answers it, the or-default-join; otherwise every branch's result is
// unioned, the or-join. It reads the header for what a branch sees, what
// the result carries and what a branch's index is keyed on.
type OrFallbackRelation struct {
	executor       *DefaultQueryExecutor
	ctx            *Context
	required       []query.Symbol // the header's required run
	outputs        []query.Symbol // the header's outputs
	branches       [][]query.Clause
	outerRel       Relation       // the subject
	symbols        []query.Symbol // the subject's symbols, then the outputs the subject lacks
	options        ExecutorOptions
	properties     RelationProperties
	deduplicate    bool
	iteratorCount  int            // how many iterators have been created, for the created event
	joinSyms       []query.Symbol // the run symbols the subject carries: a built branch's probe key
	prefetched     bool           // true when a prefetch has warmed the EA cache for the subject's entities
	shortCircuit   bool           // true = fallback (first match wins), false = correlated union (all branches)
	consumedGroups []int          // the groups the subject was joined from, which the result replaces

	// runSyms is what a branch holding no invocation is given, the frame it
	// runs over: the required run, and under the union the outputs the
	// subject or the environment binds, its seek. The subject's symbols lead,
	// in the subject's order, and the symbols the environment alone binds
	// follow.
	runSyms []query.Symbol

	// carried is the outputs the subject or the environment binds. Under the
	// fallback they are joined after the decision: a branch tuple disagreeing
	// with the subject's value is dropped. Under the union a branch holding
	// no invocation runs over them as its seek, and a branch holding one is
	// joined with the subject on them through its index's key, or narrowed
	// to the environment's value when the environment alone binds one.
	carried []carriedOutput

	// plans holds each branch's frame and plan, made at the first iterator
	// and kept for every later one.
	plans []branchRun
}

// carriedOutput is an output the subject or the environment binds, with
// where its value is read per subject tuple: the subject tuple's position,
// or the environment's value when the environment alone binds it.
type carriedOutput struct {
	symbol   query.Symbol
	outerPos int
	envValue interface{}
}

// branchRun is one branch's frame and the plan of its scope. A branch
// reading nothing of what it is given runs once, at the first window, over
// the environment's tuple or the unit relation, and its result is probed
// with no key; every other branch runs once per window over the window's
// keys.
type branchRun struct {
	frame        scope
	plan         *planner.RealizedPlan
	readsNothing bool
}

// NewOrFallbackRelation creates a streaming relation over outerRel, the
// subject, running branches under the header of required and outputs.
// When shortCircuit=true, uses fallback semantics (first match wins).
// When shortCircuit=false, uses correlated union (all branches contribute).
func NewOrFallbackRelation(
	executor *DefaultQueryExecutor,
	ctx *Context,
	required []query.Symbol,
	outputs []query.Symbol,
	branches [][]query.Clause,
	outerRel Relation,
	options ExecutorOptions,
	shortCircuit bool,
) *OrFallbackRelation {
	outerSyms := outerRel.Symbols()
	var envSyms []query.Symbol
	var envTuple Tuple
	if env := ctx.Environment(); env != nil {
		envSyms, envTuple = environmentTuple(env)
	}

	// The result's schema: the subject's symbols, then the outputs the
	// subject lacks.
	symbols := make([]query.Symbol, len(outerSyms))
	copy(symbols, outerSyms)
	for _, sym := range outputs {
		if !query.ContainsSymbol(outerSyms, sym) {
			symbols = append(symbols, sym)
		}
	}

	// The outputs the subject or the environment binds. The fallback joins
	// them after its decision, which is non-monotone. The union binds them
	// into a branch run per tuple as its seek, a semi-join that answers the
	// same tuples for a monotone union, and joins a branch built once with
	// the subject on them through its cache's key.
	var carried []carriedOutput
	for _, sym := range outputs {
		outerPos := query.SymbolIndex(outerSyms, sym)
		envPos := query.SymbolIndex(envSyms, sym)
		if outerPos < 0 && envPos < 0 {
			continue
		}
		output := carriedOutput{symbol: sym, outerPos: outerPos}
		if outerPos < 0 {
			output.envValue = envTuple[envPos]
		}
		carried = append(carried, output)
	}

	// What a branch is given: the subject's symbols lead, in the subject's
	// order, and are a built branch's probe key; the symbols the environment
	// alone binds follow.
	given := givenSymbols(required, carried, shortCircuit)
	var runSyms []query.Symbol
	for _, sym := range outerSyms {
		if query.ContainsSymbol(given, sym) {
			runSyms = append(runSyms, sym)
		}
	}
	joinSyms := append([]query.Symbol(nil), runSyms...)
	for _, sym := range given {
		if !query.ContainsSymbol(outerSyms, sym) {
			runSyms = append(runSyms, sym)
		}
	}

	// The properties: whether every branch answers at most one tuple per
	// subject tuple is judged against the header symbols the subject
	// carries, the grain the result is produced at, a pattern branch through
	// the matcher, and nothing is overwritten, since a bound output is
	// joined. The result deduplicates under the union when the form has more
	// than one branch, since two branches can answer one subject tuple, and
	// under the fallback only over a subject carrying no key, since one
	// branch answers each subject tuple and a branch's answer is a set.
	var headerCarried []query.Symbol
	for _, sym := range append(append([]query.Symbol(nil), required...), outputs...) {
		if query.ContainsSymbol(outerSyms, sym) {
			headerCarried = append(headerCarried, sym)
		}
	}
	branchesAtMostOne := orBranchesEmitAtMostOne(branches, required, headerCarried, executor.matcher)
	properties := orProperties(
		outerRel.Properties(),
		outputs,
		nil,
		symbols,
		shortCircuit,
		branchesAtMostOne,
	)
	deduplicate := len(branches) > 1
	if shortCircuit {
		deduplicate = len(outerRel.Properties().Keys) == 0
	}
	if options.Handler != nil {
		options.Handler(annotations.Event{
			Name: annotations.OrPropertiesDerived,
			Data: map[string]interface{}{
				"short_circuit":           shortCircuit,
				"branches_at_most_one":    branchesAtMostOne,
				"deduplicate":             deduplicate,
				"outer_candidate_keys":    outerRel.Properties().clone().Keys,
				"result_candidate_keys":   properties.clone().Keys,
				"result_ordering":         properties.clone().Ordering,
				"branch_output_symbols":   append([]query.Symbol(nil), outputs...),
				"relation_output_symbols": append([]query.Symbol(nil), symbols...),
			},
		})
	}

	return &OrFallbackRelation{
		executor:     executor,
		ctx:          ctx,
		required:     required,
		outputs:      outputs,
		branches:     branches,
		outerRel:     outerRel,
		symbols:      symbols,
		options:      options,
		properties:   properties,
		deduplicate:  deduplicate,
		joinSyms:     joinSyms,
		shortCircuit: shortCircuit,
		runSyms:      runSyms,
		carried:      carried,
	}
}

// orBranchesEmitAtMostOne reports whether every branch answers at most one
// tuple per subject tuple, judged against the header symbols the subject
// carries: an expression binding one value, a subquery binding one tuple,
// a relation binding whose find variables the subject carries, grouped by
// them, and a data pattern the matcher serves by one lookup, which
// patternAnswersAtMostOne states.
func orBranchesEmitAtMostOne(
	branches [][]query.Clause,
	required []query.Symbol,
	headerCarried []query.Symbol,
	matcher PatternMatcher,
) bool {
	if len(branches) == 0 {
		return false
	}
	carriedSet := make(map[query.Symbol]bool, len(headerCarried))
	for _, symbol := range headerCarried {
		carriedSet[symbol] = true
	}
	for _, branch := range branches {
		if len(branch) == 0 {
			return false
		}
		for _, clause := range branch {
			switch typed := clause.(type) {
			case *query.Expression:
				if typed.Function == nil || typed.Function.ReturnType() == "tuples" {
					return false
				}
			case *query.SubqueryPattern:
				switch binding := typed.Binding.(type) {
				case query.ScalarBinding, query.TupleBinding:
				case query.RelationBinding:
					if !relationBindingGroupsAreOuterBound(typed.Query, binding.Variables, carriedSet) {
						return false
					}
				default:
					return false
				}
			case *query.DataPattern:
				if !patternAnswersAtMostOne(typed, required, carriedSet, matcher) {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

func relationBindingGroupsAreOuterBound(
	subquery *query.Query,
	bindingVariables []query.Symbol,
	outerSymbols map[query.Symbol]bool,
) bool {
	if subquery == nil || len(subquery.Find) != len(bindingVariables) {
		return false
	}
	for i, element := range subquery.Find {
		switch element.(type) {
		case query.FindVariable:
			if !outerSymbols[bindingVariables[i]] {
				return false
			}
		case query.FindAggregate:
		default:
			return false
		}
	}
	return true
}

// patternAnswersAtMostOne reports whether a data pattern answers at most
// one tuple per subject tuple: its entity position is a required symbol
// the subject carries, its attribute is a constant, and the matcher serves
// the attribute on the pattern's source by one lookup, cardinality one on a
// current handle, which CanFuseAttributeFetch states; a pattern naming no
// source reads the default source.
func patternAnswersAtMostOne(
	pattern *query.DataPattern,
	required []query.Symbol,
	carried map[query.Symbol]bool,
	matcher PatternMatcher,
) bool {
	entity, ok := pattern.GetE().(query.Variable)
	if !ok || !carried[entity.Name] || !query.ContainsSymbol(required, entity.Name) {
		return false
	}
	attrConstant, ok := pattern.GetA().(query.Constant)
	if !ok {
		return false
	}
	attr, ok := attrConstant.Value.(datalog.Keyword)
	if !ok {
		return false
	}
	fusable, ok := matcher.(AttributeFetchFusable)
	if !ok {
		return false
	}
	source := pattern.Source
	if source == nil {
		source = datalog.SymDollar
	}
	return fusable.CanFuseAttributeFetch(source, attr)
}

// holdsInvocation reports whether the clauses invoke a predicate at any
// depth, a subquery's inner query included.
func holdsInvocation(clauses []query.Clause) bool {
	found := false
	query.WalkClauses(clauses, func(clause query.Clause) bool {
		if found {
			return false
		}
		switch typed := clause.(type) {
		case *query.RuleInvocation:
			found = true
			return false
		case *query.SubqueryPattern:
			if typed.Query != nil && holdsInvocation(typed.Query.Where) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// branchProvides is the symbols a branch's clauses provide, read under the
// rule set, in first-appearance order.
func branchProvides(clauses []query.Clause, rules *query.RuleSet) []query.Symbol {
	var provided []query.Symbol
	for _, clause := range clauses {
		for _, sym := range query.ScopeOf(clause, rules).Provides {
			if !query.ContainsSymbol(provided, sym) {
				provided = append(provided, sym)
			}
		}
	}
	return provided
}

// givenSymbols is what a branch is given: the required run under the
// fallback, and the required run with the carried outputs under the union.
func givenSymbols(required []query.Symbol, carried []carriedOutput, shortCircuit bool) []query.Symbol {
	given := append([]query.Symbol(nil), required...)
	if shortCircuit {
		return given
	}
	for _, output := range carried {
		if !query.ContainsSymbol(given, output.symbol) {
			given = append(given, output.symbol)
		}
	}
	return given
}

// branchReads is the given symbols a branch reads, in the given order: a
// symbol a clause consumes that no clause of the branch provides, or a data
// pattern mentions.
func branchReads(branch []query.Clause, given []query.Symbol, rules *query.RuleSet) []query.Symbol {
	var provided, consumed, mentioned []query.Symbol
	for _, clause := range branch {
		clauseScope := query.ScopeOf(clause, rules)
		provided = append(provided, clauseScope.Provides...)
		consumed = append(consumed, clauseScope.Correlates...)
		if _, isPattern := clause.(*query.DataPattern); isPattern {
			mentioned = append(mentioned, clauseScope.Provides...)
		}
	}
	var reads []query.Symbol
	for _, sym := range given {
		if query.ContainsSymbol(mentioned, sym) || (query.ContainsSymbol(consumed, sym) && !query.ContainsSymbol(provided, sym)) {
			reads = append(reads, sym)
		}
	}
	return reads
}

// reads is the given symbols a branch reads, under the rule set the form
// runs with.
func (r *OrFallbackRelation) reads(branch []query.Clause) []query.Symbol {
	return branchReads(branch, givenSymbols(r.required, r.carried, r.shortCircuit), r.ctx.Rules())
}

// planBranches frames each branch as a scope under the header and plans it
// once. A branch holding an invocation is framed over the required run, so
// the invocation is seeded by the header; a branch reading nothing of what
// it is given is framed over the required symbols the environment alone
// binds, so its one run carries the input's values; every other branch is
// framed over the run symbols, the required run and, under the union, the
// carried outputs, its seek. The find is the given symbols the branch
// provides beyond its frame, and the outputs. Under the union a branch
// holding no invocation with carried outputs is planned first over the
// required run alone, the language's scope, and that plan's refusal is the
// form's: the seek that then binds the carried outputs into it never runs a
// branch the language refuses.
func (r *OrFallbackRelation) planBranches() ([]branchRun, error) {
	given := givenSymbols(r.required, r.carried, r.shortCircuit)
	// The required symbols the subject lacks, which the environment alone
	// binds: the planner refuses a required run nothing binds.
	var fromEnvironment []query.Symbol
	for _, sym := range r.required {
		if !query.ContainsSymbol(r.outerRel.Symbols(), sym) {
			fromEnvironment = append(fromEnvironment, sym)
		}
	}
	plans := make([]branchRun, len(r.branches))
	for i, branch := range r.branches {
		var run branchRun
		switch {
		case holdsInvocation(branch):
			// The required run alone, in the run symbols' order, so every
			// keyed branch of a fallback is framed alike and the keys one
			// branch answers are read at the same positions by the next.
			for _, sym := range r.runSyms {
				if query.ContainsSymbol(r.required, sym) {
					run.frame.required = append(run.frame.required, sym)
				}
			}
		case len(r.reads(branch)) == 0:
			run.frame.required = fromEnvironment
			run.readsNothing = true
		default:
			if !r.shortCircuit && len(r.carried) > 0 {
				if _, err := r.executor.planScope(r.ctx, scope{required: r.required, outputs: r.outputs, clauses: branch}); err != nil {
					return nil, fmt.Errorf("branch %d: %w", i+1, err)
				}
			}
			run.frame.required = r.runSyms
		}
		for _, sym := range branchProvides(branch, r.ctx.Rules()) {
			if query.ContainsSymbol(given, sym) && !query.ContainsSymbol(run.frame.required, sym) {
				run.frame.outputs = append(run.frame.outputs, sym)
			}
		}
		for _, sym := range r.outputs {
			if !query.ContainsSymbol(run.frame.required, sym) && !query.ContainsSymbol(run.frame.outputs, sym) {
				run.frame.outputs = append(run.frame.outputs, sym)
			}
		}
		run.frame.clauses = branch
		plan, err := r.executor.planScope(r.ctx, run.frame)
		if err != nil {
			return nil, fmt.Errorf("branch %d: %w", i+1, err)
		}
		run.plan = plan
		plans[i] = run
	}
	return plans, nil
}

func (r *OrFallbackRelation) Iterator() Iterator {
	r.iteratorCount++

	var setupErr error
	if r.plans == nil {
		r.plans, setupErr = r.planBranches()
	}

	outer := r.outerRel

	var seen *TupleKeyMap
	if r.deduplicate {
		seen = NewTupleKeyMap()
	}

	// The environment's tuple over the run symbols the environment alone
	// binds.
	var envRel Relation
	if env := r.ctx.Environment(); env != nil {
		var fromEnvironment []query.Symbol
		for _, sym := range r.runSyms {
			if !query.ContainsSymbol(outer.Symbols(), sym) && query.ContainsSymbol(env.Symbols(), sym) {
				fromEnvironment = append(fromEnvironment, sym)
			}
		}
		if len(fromEnvironment) > 0 {
			projected, err := env.Project(fromEnvironment)
			if err != nil {
				if setupErr == nil {
					setupErr = err
				}
			} else {
				envRel = projected
			}
		}
	}
	// Every subject is read into windows of one tuple doubling to the cap,
	// and the workspace grows with the window, so a result abandoned after
	// its first tuples has paid for the windows read and nothing more.
	windowCap := r.options.windowCap()
	// The positions in the subject tuple of the required run's symbols it
	// carries: the window event's distinct key count is taken over them.
	var keyPos []int
	for i, sym := range outer.Symbols() {
		if query.ContainsSymbol(r.required, sym) {
			keyPos = append(keyPos, i)
		}
	}
	frames, frameOf := windowFrames(r.plans, outer.Symbols(), envRel)
	// The evaluations the branches' invocations continue across the windows:
	// the enclosing form execution's, shared through the context, or this
	// iterator's own, completed at the subject's exhaustion and at Close.
	ctx := r.ctx
	continued := ctx.continued
	ownsContinued := continued == nil
	if ownsContinued {
		continued = newRuleContinuations()
		ctx = ctx.WithContinuations(continued)
	}

	if r.options.Handler != nil {
		r.options.Handler(annotations.Event{
			Name:  annotations.OrFallbackIteratorCreated,
			Start: time.Now(),
			Data: map[string]interface{}{
				"iterator_count": r.iteratorCount,
				"outer_symbols":  outer.Symbols(),
				"outer_type":     fmt.Sprintf("%T", outer),
			},
		})
	}

	it := &OrFallbackIterator{
		executor:      r.executor,
		ctx:           ctx,
		continued:     continued,
		ownsContinued: ownsContinued,
		branches:      r.branches,
		plans:         r.plans,
		shortCircuit:  r.shortCircuit,
		outerSyms:     outer.Symbols(),
		outputSyms:    r.symbols,
		options:       r.options,
		joinSyms:      r.joinSyms,
		envRel:        envRel,
		carried:       r.carried,
		windowSize:    1,
		windowCap:     windowCap,
		windowGrowth:  r.options.windowGrowth(),
		keyPos:        keyPos,
		copies:        outer.RequiresCopy(),
		frames:        frames,
		frameOf:       frameOf,
		answered:      make([]*answeredKeys, len(r.branches)),
		prefetched:    r.prefetched,
		seen:          seen,
		err:           setupErr,
		done:          setupErr != nil,
	}
	if setupErr != nil {
		return it
	}
	it.outerIter = outer.Iterator()
	return it
}

func (r *OrFallbackRelation) Symbols() []query.Symbol {
	// Symbols are computed at construction time - no peeking needed
	return r.symbols
}

func (r *OrFallbackRelation) Properties() RelationProperties {
	return r.properties
}

func (r *OrFallbackRelation) Size() int {
	return -1 // Streaming - unknown size
}

func (r *OrFallbackRelation) Get(i int) Tuple {
	return nil // Not supported for streaming
}

func (r *OrFallbackRelation) String() string {
	var symStrs []string
	for _, sym := range r.Symbols() {
		symStrs = append(symStrs, sym.String())
	}
	return fmt.Sprintf("OrFallbackRelation([%s], streaming)", strings.Join(symStrs, " "))
}

func (r *OrFallbackRelation) Table() string {
	return r.Materialize().Table()
}

func (r *OrFallbackRelation) ProjectFromPattern(pattern *query.DataPattern) Relation {
	return r.Materialize().ProjectFromPattern(pattern)
}

func (r *OrFallbackRelation) Sorted() ([]Tuple, error) {
	return r.Materialize().Sorted()
}

func (r *OrFallbackRelation) Project(symbols []query.Symbol) (Relation, error) {
	return r.Materialize().Project(symbols)
}

func (r *OrFallbackRelation) Materialize() Relation {
	var tuples []Tuple
	err := collectTuplesInto(&tuples, r)

	syms := r.symbols
	if syms == nil {
		syms = r.Symbols()
	}
	mat := newMaterializedRelationFromSet(syms, tuples, r.options, r.properties)
	if err != nil {
		mat.err = err
	}
	return mat
}

func (r *OrFallbackRelation) Sort(orderBy []query.OrderByClause) Relation {
	return r.Materialize().Sort(orderBy)
}

func (r *OrFallbackRelation) FilterWithPredicate(pred query.Predicate) Relation {
	return r.Materialize().FilterWithPredicate(pred)
}

func (r *OrFallbackRelation) EvaluateFunction(fn query.Function, outputSymbol query.Symbol) Relation {
	return r.Materialize().EvaluateFunction(fn, outputSymbol)
}

func (r *OrFallbackRelation) Select(pred func(Tuple) bool) Relation {
	return Select(r, pred)
}

func (r *OrFallbackRelation) Join(other Relation) Relation {
	common := CommonSymbols(r, other)
	if len(common) == 0 {
		return crossProduct(r, other)
	}
	return r.HashJoin(other, common)
}

func (r *OrFallbackRelation) HashJoin(other Relation, joinSyms []query.Symbol) Relation {
	return HashJoin(r, other, joinSyms)
}

func (r *OrFallbackRelation) SemiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return SemiJoin(r, other, joinSyms)
}

func (r *OrFallbackRelation) AntiJoin(other Relation, joinSyms []query.Symbol) Relation {
	return AntiJoin(r, other, joinSyms)
}

func (r *OrFallbackRelation) Aggregate(findElements []query.FindElement) Relation {
	return ExecuteAggregations(r, findElements)
}

func (r *OrFallbackRelation) Options() ExecutorOptions {
	return r.options
}

// RequiresCopy returns false because OrFallbackIterator copies tuples from
// sources that have RequiresCopy() = true at the boundary.
func (r *OrFallbackRelation) RequiresCopy() bool {
	return false
}

// OrFallbackIterator reads the subject in windows, runs each branch once per
// window over the window's keys, and emits the result's tuples lazily by
// probing the branches' indexes per window tuple.
type OrFallbackIterator struct {
	executor     *DefaultQueryExecutor
	ctx          *Context
	branches     [][]query.Clause
	plans        []branchRun
	shortCircuit bool // true = fallback, false = correlated union
	outerIter    Iterator
	outerSyms    []query.Symbol
	options      ExecutorOptions
	joinSyms     []query.Symbol // the run symbols the subject carries: a branch's index key

	// envRel is the single retained source of the environment's values over
	// the run symbols the environment alone binds, nil when there are none.
	// Constant for the query's lifetime; consumers read Symbols()/Get(0)
	// from it.
	envRel Relation

	// carried is the outputs the fallback joins after the decision.
	carried []carriedOutput

	// The window: the subject's tuples the branches are run over and probed
	// for, held in a workspace allocated once, at the smaller of the
	// subject's size and the cap, and nothing of the subject beyond them.
	// windowSize is the next window's size, growing by windowGrowth to
	// windowCap; windowPos is the tuple being probed; windows counts the
	// windows read, the window event's ordinal; keyPos is the positions in
	// the subject tuple of the required run's symbols it carries, which the
	// event's distinct key count is taken over; copies records that the
	// subject reuses its tuple workspace, so a tuple is copied on entry.
	window       []Tuple
	windowPos    int
	windowSize   int
	windowCap    int
	windowGrowth int
	windows      int
	keyPos       []int
	copies       bool

	// frames is each distinct frame of the plans resolved against the
	// subject and the environment, holding the keys its windows have run and
	// the window's run; frameOf is each branch's frame by index into frames;
	// exhausted records that the subject is read to its end; answered holds,
	// per fallback branch, the keys its runs answered, which a later branch
	// is never run for.
	frames    []windowFrame
	frameOf   []int
	exhausted bool
	answered  []*answeredKeys

	// Current state
	currentTuple Tuple
	outputSyms   []query.Symbol
	seen         *TupleKeyMap
	done         bool
	err          error

	// Cached-branch emit state: a probe's matches being emitted, a key's
	// group of a branch's index, which is already a set.
	cachedMatches  []Tuple
	cachedMatchPos int
	cachedPlan     projectionPlan
	cachedOuter    Tuple

	// branchPlans memoizes each branch's output projection, keyed by branch
	// index with the branch's result symbols as the plan's cache key. Per
	// branch the symbols are fixed for the iterator's lifetime (same clauses,
	// same input shape), so the projection is planned once.
	branchPlans map[int]branchPlan

	// The branches' indexes, kept across the windows and grown by each
	// window's build. Key: branch index. A branch no window has built yet
	// has none and answers nothing. See ALGEBRA.md "OR-Fallback Branch
	// Caching" for specification.
	branchCache map[int]*cachedBranch
	prefetched  bool // True when PrefetchEntities has warmed the EA cache for outer entities

	// continued is the evaluations the invocations under this iterator
	// continue across its windows: the enclosing form execution's, shared
	// through the context, or this iterator's own, which ownsContinued
	// records and the iterator completes at the subject's exhaustion and at
	// Close.
	continued     *ruleContinuations
	ownsContinued bool

	// unionBranchIdx is the next branch to probe for the window tuple being
	// probed under the union.
	unionBranchIdx int
}

// windowFrame is a branch frame resolved against the subject and the
// environment: for each required symbol of the frame, its position in the
// subject tuple, or the environment's value when the environment alone
// binds it. A frame carrying no subject symbol has one run for every
// window, the environment's tuple or the unit relation, so its branches
// are built at the first window alone. ran holds the keys the frame's
// windows have run, over the subject positions, so a key a later window
// carries again is left out of its run; run is the window's run, computed
// once per window for the branches framed alike.
type windowFrame struct {
	required   []query.Symbol
	subjectPos []int // the subject tuple's positions of the required symbols it carries
	framePos   []int // the frame-run position of each subjectPos entry
	envValues  Tuple // the environment's value per frame position, nil where the subject carries the symbol
	ran        *TupleKeyMap
	run        *MaterializedRelation
	runWindow  int
}

// windowFrames resolves each distinct frame of plans against the subject's
// symbols and the environment's tuple over the symbols it alone binds, and
// returns the frames with each branch's frame by index.
func windowFrames(plans []branchRun, outerSyms []query.Symbol, envRel Relation) ([]windowFrame, []int) {
	var frames []windowFrame
	frameOf := make([]int, len(plans))
	for i, plan := range plans {
		required := plan.frame.required
		found := -1
		for j := range frames {
			if symbolsMatch(frames[j].required, required) {
				found = j
				break
			}
		}
		if found >= 0 {
			frameOf[i] = found
			continue
		}
		frame := windowFrame{required: required, envValues: make(Tuple, len(required))}
		for pos, sym := range required {
			if outerPos := query.SymbolIndex(outerSyms, sym); outerPos >= 0 {
				frame.subjectPos = append(frame.subjectPos, outerPos)
				frame.framePos = append(frame.framePos, pos)
				continue
			}
			frame.envValues[pos] = envRel.Get(0)[query.SymbolIndex(envRel.Symbols(), sym)]
		}
		if len(frame.subjectPos) > 0 {
			frame.ran = NewTupleKeyMap()
		}
		frameOf[i] = len(frames)
		frames = append(frames, frame)
	}
	return frames, frameOf
}

// environmentRun is the run of a frame carrying no subject symbol: the
// environment's tuple over the frame's symbols, or nil, the unit relation,
// for a frame with none.
func (f *windowFrame) environmentRun(opts ExecutorOptions) *MaterializedRelation {
	if len(f.required) == 0 {
		return nil
	}
	return newMaterializedRelationFromSet(f.required, []Tuple{f.envValues}, opts, deduplicatedProperties(f.required))
}

// windowRun is the frame's run over a window: the window's distinct
// projection onto the frame's symbols, each tuple decorated with the
// environment's values, less the keys an earlier window ran, computed once
// per window for the branches framed alike.
func (f *windowFrame) windowRun(window []Tuple, ordinal int, opts ExecutorOptions) *MaterializedRelation {
	if f.runWindow == ordinal {
		return f.run
	}
	var tuples []Tuple
	for _, tuple := range window {
		if f.ran.PutIfAbsentPositions(tuple, f.subjectPos, true) {
			continue
		}
		key := make(Tuple, len(f.required))
		copy(key, f.envValues)
		for i, pos := range f.subjectPos {
			key[f.framePos[i]] = tuple[pos]
		}
		tuples = append(tuples, key)
	}
	f.run = newMaterializedRelationFromSet(f.required, tuples, opts, deduplicatedProperties(f.required))
	f.runWindow = ordinal
	return f.run
}

// cachedBranch is a branch's index, kept across the windows: a JoinIndex
// keyed probe-side by the subject tuple's key positions and stored-side by
// the branch tuple's, carrying the branch's result symbols for output
// projection.
type cachedBranch struct {
	*JoinIndex
	branchSyms []query.Symbol
}

// buildBranchFromEACache builds a window's tuples for a branch of one data
// pattern on the default source whose entity position is a run symbol the
// subject carries and whose attribute the matcher can serve by lookup, from
// the entities of run, the window's keys, through LookupAttribute (EA
// cache): one lookup per distinct entity, each found value one tuple over
// the entity and value symbols. Returns the tuples and their symbols, nil
// symbols when the prerequisites are not met (wrong branch shape, no
// LookupAttribute, an attribute the matcher cannot serve by lookup), and
// sets it.err on a failed lookup.
func (it *OrFallbackIterator) buildBranchFromEACache(branch []query.Clause, run *MaterializedRelation) ([]Tuple, []query.Symbol) {
	if len(branch) != 1 || run == nil {
		return nil, nil
	}
	dp, ok := branch[0].(*query.DataPattern)
	if !ok || dp.Source != nil {
		return nil, nil
	}

	// Need E as variable (join key) and A as constant keyword. The E
	// variable must be a run symbol the subject carries: the branch is keyed
	// on it.
	eVar, eIsVar := dp.GetE().(query.Variable)
	if !eIsVar {
		return nil, nil
	}
	if !query.ContainsSymbol(it.joinSyms, eVar.Name) {
		return nil, nil
	}
	var aKw datalog.Keyword
	if aConst, ok := dp.GetA().(query.Constant); ok {
		aKw, _ = aConst.Value.(datalog.Keyword)
	}
	if aKw == nil {
		return nil, nil
	}

	// Need V as variable
	vVar, vIsVar := dp.GetV().(query.Variable)
	if !vIsVar {
		return nil, nil
	}

	// Need matcher to support LookupAttribute, and to serve this attribute
	// by one lookup: cardinality one on a current handle.
	lookupMatcher, ok := it.executor.matcher.(EntityLookupMatcher)
	if !ok {
		return nil, nil
	}
	fusable, ok := it.executor.matcher.(AttributeFetchFusable)
	if !ok || !fusable.CanFuseAttributeFetch(datalog.SymDollar, aKw) {
		return nil, nil
	}

	eIdx := query.SymbolIndex(run.Symbols(), eVar.Name)
	if eIdx < 0 {
		return nil, nil
	}

	branchSyms := []query.Symbol{eVar.Name, vVar.Name}
	var collected []Tuple
	if n := run.Size(); n >= 0 {
		collected = make([]Tuple, 0, n)
	}
	// The run's tuples are distinct but the value at their entity position
	// can repeat; one lookup and one tuple per distinct entity keeps the
	// collected tuples a set.
	seenEntities := make(map[datalog.Identity]struct{})
	runIt := run.Iterator()
	for runIt.Next() {
		entity, ok := runIt.Tuple()[eIdx].(datalog.Identity)
		if !ok {
			continue
		}
		if _, dup := seenEntities[entity]; dup {
			continue
		}
		seenEntities[entity] = struct{}{}
		value, found, err := lookupMatcher.LookupAttribute(entity, aKw)
		if err != nil {
			it.err = err
			_ = runIt.Close()
			return nil, nil
		}
		if !found {
			continue
		}
		collected = append(collected, Tuple{entity, value})
	}
	runErr := runIt.Error()
	closeErr := runIt.Close()
	if runErr != nil {
		it.err = runErr
		return nil, nil
	}
	if closeErr != nil {
		it.err = closeErr
		return nil, nil
	}

	// An environment-bound V narrows the branch: only tuples carrying the
	// environment's value may match, the same narrowing the scan path
	// receives through the run, expressed as a SemiJoin against the
	// environment projected onto the V symbol.
	if it.envRel != nil && query.ContainsSymbol(it.envRel.Symbols(), vVar.Name) {
		vEnv, err := it.envRel.Project([]query.Symbol{vVar.Name})
		if err != nil {
			it.err = err
			return nil, nil
		}
		branchRel := newMaterializedRelationFromSet(
			branchSyms,
			collected,
			it.options,
			deduplicatedProperties(branchSyms),
		)
		narrowed := SemiJoin(branchRel, vEnv, []query.Symbol{vVar.Name})
		var filtered []Tuple
		nit := narrowed.Iterator()
		for nit.Next() {
			filtered = append(filtered, nit.Tuple())
		}
		narrowErr := nit.Error()
		if cErr := nit.Close(); narrowErr == nil {
			narrowErr = cErr
		}
		if narrowErr != nil {
			it.err = narrowErr
			return nil, nil
		}
		collected = filtered
	}

	return collected, branchSyms
}

// isDataPatternOnlyBranch reports whether a branch consists solely of
// DataPatterns (and is non-empty). Under a fallback with a non-empty
// required run, such a branch is the one a window's keys narrow: it runs
// with the run symbols bound to the window's values. Subquery branches, or
// branches containing expressions/predicates, are not narrowable this way.
func isDataPatternOnlyBranch(branch []query.Clause) bool {
	if len(branch) == 0 {
		return false
	}
	for _, c := range branch {
		if _, ok := c.(*query.DataPattern); !ok {
			return false
		}
	}
	return true
}

// newCachedBranch is an empty index over a branch's result symbols, keyed
// probe-side by the subject tuple's positions of the key symbols the result
// carries and stored-side by the branch tuple's. A result carrying none of
// them is grouped under one key, so every subject tuple probes the whole
// result: the shape of a branch reading nothing.
func newCachedBranch(branchSyms, outerSyms, keySyms []query.Symbol) *cachedBranch {
	var bIdx, oIdx []int
	for _, ksym := range keySyms {
		oi := query.SymbolIndex(outerSyms, ksym)
		if oi < 0 {
			continue
		}
		if bi := query.SymbolIndex(branchSyms, ksym); bi >= 0 {
			oIdx = append(oIdx, oi)
			bIdx = append(bIdx, bi)
		}
	}
	return &cachedBranch{
		JoinIndex:  NewJoinIndex(oIdx, bIdx, 0),
		branchSyms: branchSyms,
	}
}

// collectBranch reads a branch's result whole. The index retains the tuples
// for the query's lifetime, so a tuple a source reusing its workspace
// yields is copied.
func collectBranch(branchResult Relation) ([]Tuple, error) {
	needsCopy := branchResult.RequiresCopy()
	var collected []Tuple
	if n := branchResult.Size(); n >= 0 {
		collected = make([]Tuple, 0, n)
	}
	iter := branchResult.Iterator()
	for iter.Next() {
		t := iter.Tuple()
		if needsCopy {
			t = copyTuple(t)
		}
		collected = append(collected, t)
	}
	iterErr := iter.Error()
	if closeErr := iter.Close(); iterErr == nil {
		iterErr = closeErr
	}
	if iterErr != nil {
		return nil, iterErr
	}
	return collected, nil
}

// runBranch runs a branch's plan over run, the unit relation when run is
// nil.
func (it *OrFallbackIterator) runBranch(branchIdx int, run *MaterializedRelation) (Relation, error) {
	var input Relation = NewUnitRelation(it.options)
	if run != nil {
		input = run
	}
	return it.executor.runPlan(it.ctx, it.plans[branchIdx].plan, input)
}

// buildBranch runs a branch over run, the window's keys for it, and adds the
// result to the branch's index, created at the first build and kept across
// the windows, reporting the narrowing decision of a data-pattern-only
// fallback branch and the build. A narrowable branch on a warm EA cache is
// built from the cache. Under the fallback the keys the result answers are
// recorded, so a later branch is never run for them. Returns false with
// it.err set when the build fails.
func (it *OrFallbackIterator) buildBranch(branchIdx int, run *MaterializedRelation) bool {
	branch := it.branches[branchIdx]
	narrowable := it.shortCircuit && isDataPatternOnlyBranch(branch)
	// Report the narrowing decision, so the choice and any fall-back to
	// a full scan are observable.
	// See docs/bugs/BUG_GETELSE_SCAN_REWRITE_NOT_NARROWED_BY_BOUND_CHILD.md.
	if narrowable && it.options.Handler != nil {
		data := map[string]interface{}{
			"branch":   branchIdx,
			"narrowed": run != nil,
		}
		if run != nil {
			data["join_keys"] = run.Size()
		} else {
			data["reason"] = "the branch's frame names no required symbol"
		}
		it.options.Handler(annotations.Event{
			Name:  annotations.OrFallbackBranchNarrowed,
			Start: time.Now(),
			Data:  data,
		})
	}
	var tuples []Tuple
	var branchSyms []query.Symbol
	// A narrowable branch on a warm EA cache is built from the cache
	// (LookupAttribute).
	if narrowable && it.prefetched {
		tuples, branchSyms = it.buildBranchFromEACache(branch, run)
		if it.err != nil {
			return false
		}
	}
	if branchSyms == nil {
		branchResult, err := it.runBranch(branchIdx, run)
		if err != nil {
			it.err = err
			return false
		}
		branchSyms = branchResult.Symbols()
		tuples, err = collectBranch(branchResult)
		if err != nil {
			it.err = err
			return false
		}
	}
	it.reportCacheBuild(branchIdx, branchSyms, len(tuples))
	cb, built := it.branchCache[branchIdx]
	if !built {
		cb = newCachedBranch(branchSyms, it.outerSyms, it.joinSyms)
		it.cache(branchIdx, cb)
	}
	for _, tuple := range tuples {
		cb.Add(tuple)
	}
	// Under the fallback the keys the result answers are recorded: a result
	// carrying a run symbol the subject carries answers the keys it holds,
	// whatever the branch was framed over, and one carrying none answers
	// every key or none, which its index says.
	if it.shortCircuit {
		it.recordAnswered(branchIdx, branchSyms, tuples)
	}
	return true
}

// answeredKeys is the keys a fallback branch's runs have answered: each
// result tuple's projection onto the run symbols the subject carries that
// the result holds, recorded at the tuple's positions and probed at a keyed
// branch's run tuple, which those symbols lead in the subject's order.
type answeredKeys struct {
	keys      *TupleKeyMap
	branchPos []int // the key symbols' positions in the branch's result tuple
	runPos    []int // the same symbols' positions in a keyed branch's run tuple
}

// recordAnswered records the keys a fallback branch's tuples answer. A
// result carrying no run symbol the subject carries stays unrecorded.
func (it *OrFallbackIterator) recordAnswered(branchIdx int, branchSyms []query.Symbol, tuples []Tuple) {
	answered := it.answered[branchIdx]
	if answered == nil {
		answered = &answeredKeys{keys: NewTupleKeyMap()}
		for i, sym := range it.joinSyms {
			if bi := query.SymbolIndex(branchSyms, sym); bi >= 0 {
				answered.branchPos = append(answered.branchPos, bi)
				answered.runPos = append(answered.runPos, i)
			}
		}
		if len(answered.branchPos) == 0 {
			return
		}
		it.answered[branchIdx] = answered
	}
	for _, tuple := range tuples {
		answered.keys.PutIfAbsentPositions(tuple, answered.branchPos, true)
	}
}

// unanswered is the run tuples of keys none of the branches before branchIdx
// answered: an unrecorded branch, one whose result carries no run symbol the
// subject carries, answers every key when its index holds a tuple, and a
// recorded branch answers the keys it recorded. A run none of them answered
// is returned as it is.
func (it *OrFallbackIterator) unanswered(run *MaterializedRelation, branchIdx int) *MaterializedRelation {
	for j := 0; j < branchIdx; j++ {
		if it.answered[j] == nil {
			if cb := it.branchCache[j]; cb != nil && cb.Len() > 0 {
				return newMaterializedRelationFromSet(run.Symbols(), nil, it.options, deduplicatedProperties(run.Symbols()))
			}
		}
	}
	var kept []Tuple
	for _, tuple := range run.tuples {
		answered := false
		for j := 0; j < branchIdx && !answered; j++ {
			if earlier := it.answered[j]; earlier != nil {
				_, answered = earlier.keys.GetPositions(tuple, earlier.runPos)
			}
		}
		if !answered {
			kept = append(kept, tuple)
		}
	}
	if len(kept) == len(run.tuples) {
		return run
	}
	return newMaterializedRelationFromSet(run.Symbols(), kept, it.options, deduplicatedProperties(run.Symbols()))
}

// reportCacheBuild emits the cache-build event for a branch's window build.
func (it *OrFallbackIterator) reportCacheBuild(branchIdx int, branchSyms []query.Symbol, size int) {
	if it.options.Handler == nil {
		return
	}
	it.options.Handler(annotations.Event{
		Name: annotations.OrFallbackCacheBuild,
		Data: map[string]interface{}{
			"branch":      branchIdx,
			"branch_syms": branchSyms,
			"outer_syms":  it.outerSyms,
			"branch_size": size,
		},
	})
}

// cache records a built branch for probing.
func (it *OrFallbackIterator) cache(branchIdx int, cb *cachedBranch) {
	if it.branchCache == nil {
		it.branchCache = make(map[int]*cachedBranch)
	}
	it.branchCache[branchIdx] = cb
}

// agreeing filters a probe's matches to the tuples agreeing with the
// subject tuple on the carried outputs, allocating only when one disagrees.
// With no carried output every match agrees.
func (it *OrFallbackIterator) agreeing(matches []Tuple, branchSyms []query.Symbol, outerTuple Tuple) []Tuple {
	if len(it.carried) == 0 {
		return matches
	}
	kept := matches
	filtered := false
	for i, match := range matches {
		if it.agrees(match, branchSyms, outerTuple) {
			if filtered {
				kept = append(kept, match)
			}
			continue
		}
		if !filtered {
			kept = append([]Tuple(nil), matches[:i]...)
			filtered = true
		}
	}
	return kept
}

// agrees reports whether a branch tuple carries the subject tuple's value
// at every carried output it carries.
func (it *OrFallbackIterator) agrees(branchTuple Tuple, branchSyms []query.Symbol, outerTuple Tuple) bool {
	for _, output := range it.carried {
		bi := query.SymbolIndex(branchSyms, output.symbol)
		if bi < 0 {
			continue
		}
		want := output.envValue
		if output.outerPos >= 0 {
			want = outerTuple[output.outerPos]
		}
		if !datalog.ValuesEqual(branchTuple[bi], want) {
			return false
		}
	}
	return true
}

func (it *OrFallbackIterator) Next() bool {
	for {
		if it.done {
			return false
		}

		var advanced bool
		switch {
		case it.cachedMatches != nil:
			// Drain an in-progress emit: the remaining probe matches come
			// straight off the branch's index.
			if it.cachedMatchPos >= len(it.cachedMatches) {
				it.cachedMatches = nil
				it.cachedOuter = nil
				continue
			}
			matched := it.cachedMatches[it.cachedMatchPos]
			it.cachedMatchPos++
			if it.cachedPlan.identity {
				it.currentTuple = matched
			} else {
				it.currentTuple = it.cachedPlan.project(matched, it.cachedOuter)
			}
			advanced = true
		case it.windowPos >= len(it.window):
			// The window is answered: read the next one and run the
			// branches over it, or end at the subject's end.
			if !it.fillWindow() {
				return false
			}
			continue
		case it.shortCircuit:
			advanced = it.nextShortCircuit()
		default:
			advanced = it.nextCorrelatedUnion()
		}
		if !advanced {
			// The window is answered; the next iteration reads the next.
			continue
		}
		if it.seen == nil {
			return true
		}
		if !it.seen.PutIfAbsent(NewTupleKeyFull(it.currentTuple), struct{}{}) {
			return true
		}
	}
}

// reportOuterTuple emits the event for a subject tuple about to be run.
func (it *OrFallbackIterator) reportOuterTuple(outerTuple Tuple) {
	if it.options.Handler == nil {
		return
	}
	it.options.Handler(annotations.Event{
		Name:  annotations.OrFallbackOuterTuple,
		Start: time.Now(),
		Data: map[string]interface{}{
			"tuple": fmt.Sprintf("%v", outerTuple),
		},
	})
}

// reportOuterExhausted emits the event for the subject read to its end.
func (it *OrFallbackIterator) reportOuterExhausted() {
	if it.options.Handler == nil {
		return
	}
	it.options.Handler(annotations.Event{
		Name:  annotations.OrFallbackOuterExhausted,
		Start: time.Now(),
		Data:  map[string]interface{}{},
	})
}

// nextCorrelatedUnion answers the window's tuples in turn from every
// branch: each branch's index is probed for the tuple's key, the required
// run and the carried outputs the subject carries, and its matches agreeing
// on the carried outputs are emitted, an output the environment alone binds
// being joined with the environment's tuple by that filter. Returns false
// when the window is answered.
func (it *OrFallbackIterator) nextCorrelatedUnion() bool {
	for it.windowPos < len(it.window) {
		outerTuple := it.window[it.windowPos]
		for it.unionBranchIdx < len(it.branches) {
			branchIdx := it.unionBranchIdx
			it.unionBranchIdx++
			cb, built := it.branchCache[branchIdx]
			if !built {
				continue
			}
			matches := it.agreeing(cb.Probe(outerTuple), cb.branchSyms, outerTuple)
			if it.emitCachedMatches(branchIdx, cb, matches, outerTuple) {
				return true
			}
		}
		it.unionBranchIdx = 0
		it.windowPos++
	}
	return false
}

// fillWindow reads the next window of the subject, up to windowSize tuples,
// into a workspace grown to the window's size, reports it, and runs each
// branch over it. It returns false at the subject's end, and on a failure of
// the subject or of a branch, with it.err set and the iterator done.
func (it *OrFallbackIterator) fillWindow() bool {
	if cap(it.window) < it.windowSize {
		it.window = make([]Tuple, 0, it.windowSize)
	} else {
		it.window = it.window[:0]
	}
	it.windowPos = 0
	for !it.exhausted && len(it.window) < it.windowSize {
		if !it.outerIter.Next() {
			it.exhausted = true
			if err := it.outerIter.Error(); err != nil {
				it.err = err
				it.done = true
				return false
			}
			break
		}
		tuple := it.outerIter.Tuple()
		if it.copies {
			tuple = copyTuple(tuple)
		}
		it.reportOuterTuple(tuple)
		it.window = append(it.window, tuple)
	}
	if len(it.window) == 0 {
		it.reportOuterExhausted()
		if it.ownsContinued {
			it.continued.complete()
		}
		it.done = true
		return false
	}
	it.windows++
	it.reportWindow()
	if !it.runWindow() {
		it.done = true
		return false
	}
	if it.windowSize < it.windowCap {
		it.windowSize *= it.windowGrowth
		if it.windowSize > it.windowCap {
			it.windowSize = it.windowCap
		}
	}
	return true
}

// reportWindow emits the window event: the window's ordinal, its tuple
// count, and its distinct key count over the required run's symbols the
// subject carries.
func (it *OrFallbackIterator) reportWindow() {
	if it.options.Handler == nil {
		return
	}
	keys := NewTupleKeyMapWithCapacity(len(it.window))
	distinct := 0
	for _, tuple := range it.window {
		if !keys.PutIfAbsentPositions(tuple, it.keyPos, true) {
			distinct++
		}
	}
	it.options.Handler(annotations.Event{
		Name:  annotations.OrFallbackWindow,
		Start: time.Now(),
		Data: map[string]interface{}{
			"ordinal":       it.windows,
			"tuple_count":   len(it.window),
			"distinct_keys": distinct,
		},
	})
}

// runWindow runs each branch over the window: a branch framed over no
// subject symbol once, at the first window, over the environment's tuple or
// the unit relation; every other branch over the window's keys its frame
// has not run, and under the fallback over those the earlier branches left
// unanswered. A branch whose run holds no key is not run. Returns false
// with it.err set when a build fails.
func (it *OrFallbackIterator) runWindow() bool {
	for branchIdx := range it.branches {
		frame := &it.frames[it.frameOf[branchIdx]]
		if len(frame.subjectPos) == 0 {
			if it.windows == 1 && !it.buildBranch(branchIdx, frame.environmentRun(it.options)) {
				return false
			}
			continue
		}
		run := frame.windowRun(it.window, it.windows, it.options)
		if it.shortCircuit {
			run = it.unanswered(run, branchIdx)
		}
		if run.Size() == 0 {
			continue
		}
		if !it.buildBranch(branchIdx, run) {
			return false
		}
	}
	return true
}

// nextShortCircuit answers the window's tuples in turn: the first branch
// whose index holds the tuple's key answers it, and a tuple no branch
// answers is dropped. The decision reads the branch's result for the key
// before the carried-output filter: a tuple whose carried output disagrees
// with every match is answered with nothing, and the later branches never
// see it. Returns false when the window is answered.
func (it *OrFallbackIterator) nextShortCircuit() bool {
	for it.windowPos < len(it.window) {
		outerTuple := it.window[it.windowPos]
		it.windowPos++
		for branchIdx := range it.branches {
			cb, built := it.branchCache[branchIdx]
			if !built {
				continue
			}
			matches := cb.Probe(outerTuple)
			if len(matches) == 0 {
				continue
			}
			if it.emitCachedMatches(branchIdx, cb, it.agreeing(matches, cb.branchSyms, outerTuple), outerTuple) {
				return true
			}
			break
		}
	}
	return false
}

func (it *OrFallbackIterator) Tuple() Tuple {
	return it.currentTuple
}

func (it *OrFallbackIterator) Close() error {
	var closeErr error
	if it.outerIter != nil {
		closeErr = it.outerIter.Close()
	}
	if it.ownsContinued {
		it.continued.complete()
	}
	it.done = true
	return closeErr
}

// Error returns any error encountered during iteration
func (it *OrFallbackIterator) Error() error {
	return it.err
}

// symbolsMatch checks if two symbol slices have the same symbols in the same order
func symbolsMatch(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// projectionPlan precomputes, per output position, where a branch tuple's
// value comes from: a branch-tuple position, else an outer-tuple position
// (a branch such as a ground default doesn't produce every output symbol —
// the outer tuple's values fill the gaps), else nothing. The mapping depends
// only on the branch, output, and outer symbol lists — per-branch constants —
// so one plan serves every tuple the branch emits. identity means the branch
// symbols already match the output symbols exactly and tuples pass through
// unprojected; the emit sites own the copy-vs-alias decision for that case.
// A symbol both sides carry reads the branch's value, which the carried
// output filter has already matched to the outer tuple's.
type projectionPlan struct {
	identity  bool
	branchPos []int // per output position; -1 = not in the branch tuple
	outerPos  []int // per output position; -1 = not in the outer tuple
}

func newProjectionPlan(srcSyms, dstSyms, outerSyms []query.Symbol) projectionPlan {
	if symbolsMatch(srcSyms, dstSyms) {
		return projectionPlan{identity: true}
	}
	branchPos := make([]int, len(dstSyms))
	outerPos := make([]int, len(dstSyms))
	for i, sym := range dstSyms {
		branchPos[i] = query.SymbolIndex(srcSyms, sym)
		outerPos[i] = query.SymbolIndex(outerSyms, sym)
	}
	return projectionPlan{branchPos: branchPos, outerPos: outerPos}
}

// project maps a branch tuple to the output symbols, filling positions the
// branch doesn't produce from the outer tuple. Not called on identity plans.
func (p projectionPlan) project(tuple, outerTuple Tuple) Tuple {
	result := make(Tuple, len(p.branchPos))
	for i, bp := range p.branchPos {
		if bp >= 0 {
			result[i] = tuple[bp]
			continue
		}
		if op := p.outerPos[i]; op >= 0 && op < len(outerTuple) {
			result[i] = outerTuple[op]
		}
	}
	return result
}

// branchPlan is a memoized projectionPlan together with the branch symbols
// it was computed from — the symbols are the memo's validity key.
type branchPlan struct {
	srcSyms []query.Symbol
	plan    projectionPlan
}

// projectionFor returns the memoized output projection for a branch,
// computing and storing it on first sight of the branch's result symbols.
func (it *OrFallbackIterator) projectionFor(branchIdx int, branchSyms []query.Symbol) projectionPlan {
	if bp, ok := it.branchPlans[branchIdx]; ok && symbolsMatch(bp.srcSyms, branchSyms) {
		return bp.plan
	}
	plan := newProjectionPlan(branchSyms, it.outputSyms, it.outerSyms)
	if it.branchPlans == nil {
		it.branchPlans = make(map[int]branchPlan)
	}
	it.branchPlans[branchIdx] = branchPlan{srcSyms: branchSyms, plan: plan}
	return plan
}

// emitCachedMatches begins emitting a probe's matches from the branch's
// index, setting the first output tuple as current. Returns false when
// there are no matches (the caller decides what follows). The matches are a
// key's group of a branch result that is already a set, so nothing
// re-deduplicates here, and identity-shaped tuples alias the index's
// tuples.
func (it *OrFallbackIterator) emitCachedMatches(branchIdx int, cb *cachedBranch, matches []Tuple, outerTuple Tuple) bool {
	if len(matches) == 0 {
		return false
	}
	if it.options.Handler != nil {
		it.options.Handler(annotations.Event{
			Name:  annotations.OrFallbackBranchSuccess,
			Start: time.Now(),
			Data: map[string]interface{}{
				"branch_index": branchIdx,
				"branch_syms":  cb.branchSyms,
				"first_tuple":  fmt.Sprintf("%v", matches[0]),
			},
		})
	}
	plan := it.projectionFor(branchIdx, cb.branchSyms)
	if plan.identity {
		it.currentTuple = matches[0]
	} else {
		it.currentTuple = plan.project(matches[0], outerTuple)
	}
	it.cachedMatches = matches
	it.cachedMatchPos = 1
	it.cachedPlan = plan
	it.cachedOuter = outerTuple
	return true
}
