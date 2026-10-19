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
	executor      *DefaultQueryExecutor
	ctx           *Context
	required      []query.Symbol // the header's required run
	outputs       []query.Symbol // the header's outputs
	branches      [][]query.Clause
	outerRel      Relation       // the subject
	symbols       []query.Symbol // the subject's symbols, then the outputs the subject lacks
	options       ExecutorOptions
	properties    RelationProperties
	deduplicate   bool
	iteratorCount int            // how many iterators have been created, for the created event
	joinSyms      []query.Symbol // the run symbols the subject carries: a built branch's probe key
	prefetched    bool           // true when a prefetch has warmed the EA cache for the subject's entities
	shortCircuit  bool           // true = fallback (first match wins), false = correlated union (all branches)

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

	// What a branch is given, ordered against the subject: the subject's
	// symbols lead and are a built branch's probe key.
	runSyms, joinSyms := runSymbols(givenSymbols(required, carried, shortCircuit), outerSyms)

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

// framing is the branch forms' framing over the subject, with environment
// as the frames' environment relation: a branch is given the required run
// under the fallback, and the required run with the carried outputs, its
// seek, under the union; under the union a branch holding no invocation
// with carried outputs is planned first over the required run alone, the
// language's scope, so that plan's refusal is the form's and the seek that
// then binds the carried outputs into it never runs a branch the language
// refuses; the find is the outputs as variables; and the key is the run
// symbols the subject carries.
func (r *OrFallbackRelation) framing(environment Relation) framing {
	form := "or-join"
	if r.shortCircuit {
		form = "or-default-join"
	}
	find := make([]query.FindElement, len(r.outputs))
	for i, sym := range r.outputs {
		find[i] = query.FindVariable{Symbol: sym}
	}
	return framing{
		form:             form,
		subject:          r.outerRel,
		required:         r.required,
		find:             find,
		given:            givenSymbols(r.required, r.carried, r.shortCircuit),
		runSyms:          r.runSyms,
		key:              r.joinSyms,
		environment:      environment,
		planOverRequired: !r.shortCircuit && len(r.carried) > 0,
	}
}

// planBranches frames each branch under the header through frameBranches,
// with the branch forms' framing.
func (r *OrFallbackRelation) planBranches() ([]branchRun, error) {
	return frameBranches(r.executor, r.ctx, r.branches, r.framing(nil))
}

// Iterator composes the modules over the subject under the branch forms'
// policy: the emit rule of the union or the fallback, and under the
// fallback the narrowing of a branch's run to the keys the earlier branches
// left unanswered, the build from the EA cache and the record of the keys a
// build answered. A planning failure is the iterator's error, with the
// subject unopened.
func (r *OrFallbackRelation) Iterator() Iterator {
	r.iteratorCount++

	var setupErr error
	if r.plans == nil {
		r.plans, setupErr = r.planBranches()
	}

	outer := r.outerRel
	var envRel Relation
	if setupErr == nil {
		envRel, setupErr = environmentRelation(r.ctx, r.runSyms, outer.Symbols())
	}

	var seen *TupleKeyMap
	if r.deduplicate {
		seen = NewTupleKeyMap()
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
		matchEmit:    matchEmit{outerSyms: outer.Symbols(), outputSyms: r.symbols, carried: r.carried},
		branches:     r.branches,
		shortCircuit: r.shortCircuit,
		answered:     make([]*answeredKeys, len(r.branches)),
		prefetched:   r.prefetched,
		seen:         seen,
	}
	if setupErr != nil {
		it.windowedIterator = windowedIterator{err: setupErr, done: true}
		return it
	}
	it.windowedIterator = newWindowedIterator(r.executor, r.ctx, r.framing(envRel), r.plans, r.options)
	it.policy = formPolicy{answer: it.answer}
	if r.shortCircuit {
		it.policy.narrow = it.unanswered
		it.policy.build = it.buildFromCache
		it.policy.built = it.recordAnswered
	}
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

// Rename is the form's result under the substitution. The form is not
// re-derived under the new names, since an output the environment binds is
// joined by name at construction and would not be found under another.
func (r *OrFallbackRelation) Rename(subst query.Substitution) (Relation, error) {
	symbols, err := subst.Rename(r.symbols)
	if err != nil {
		return nil, err
	}
	return &LazySeqRelation{
		seq:        NewTupleSeq(r.Iterator(), r.RequiresCopy()),
		symbols:    symbols,
		options:    r.options,
		properties: r.properties.renameSymbols(subst),
	}, nil
}

// OrFallbackIterator is the composition under the branch forms' policy: the
// windowed iterator reads the subject in windows and builds each branch
// once per window over the window's keys, and the iterator emits the
// result's tuples lazily by probing the branches' indexes per window tuple,
// every branch's matches under the union and the first branch's under the
// fallback.
type OrFallbackIterator struct {
	windowedIterator

	// matchEmit is the emit shared with the other forms with outputs: the
	// carried outputs the fallback joins after the decision and the union
	// filters a branch's matches on, the result's symbols, the projection
	// per branch, and the probe's matches being drained.
	matchEmit

	branches     [][]query.Clause
	shortCircuit bool // true = fallback, false = correlated union

	// answered holds, per fallback branch, the keys its runs answered, which
	// a later branch is never run for.
	answered []*answeredKeys

	seen *TupleKeyMap

	prefetched bool // True when PrefetchEntities has warmed the EA cache for outer entities

	// unionBranchIdx is the next branch to probe for the window tuple being
	// probed under the union.
	unionBranchIdx int
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
	if !query.ContainsSymbol(it.framing.key, eVar.Name) {
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
	if environment := it.framing.environment; environment != nil && query.ContainsSymbol(environment.Symbols(), vVar.Name) {
		vEnv, err := environment.Project([]query.Symbol{vVar.Name})
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

// buildFromCache is the fallback's build of a branch of data patterns alone,
// the branch a window's keys narrow: it reports the narrowing decision, and
// on a warm EA cache builds the branch from the cache through
// buildBranchFromEACache. A branch holding another clause, or a cold cache,
// answers with no symbols, so the composition builds from the plan.
func (it *OrFallbackIterator) buildFromCache(branchIdx int, run *MaterializedRelation) ([]Tuple, []query.Symbol, error) {
	branch := it.branches[branchIdx]
	if !isDataPatternOnlyBranch(branch) {
		return nil, nil, nil
	}
	// Report the narrowing decision, so the choice and any fall-back to
	// a full scan are observable.
	// See docs/bugs/BUG_GETELSE_SCAN_REWRITE_NOT_NARROWED_BY_BOUND_CHILD.md.
	if it.options.Handler != nil {
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
	if !it.prefetched {
		return nil, nil, nil
	}
	tuples, branchSyms := it.buildBranchFromEACache(branch, run)
	return tuples, branchSyms, it.err
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

// recordAnswered records the keys a fallback branch's build answers, so a
// later branch is never run for them: a result carrying a run symbol the
// subject carries answers the keys it holds, whatever the branch was framed
// over, and one carrying none answers every key or none, which its index
// says, so it stays unrecorded.
func (it *OrFallbackIterator) recordAnswered(branchIdx int, branchSyms []query.Symbol, tuples []Tuple) {
	answered := it.answered[branchIdx]
	if answered == nil {
		answered = &answeredKeys{keys: NewTupleKeyMap()}
		for i, sym := range it.framing.key {
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

// answer is the branch forms' emit rule over the window in hand: a probe's
// matches being emitted are drained first, straight off the branch's index;
// then the window's next tuple is answered, by the first branch with a
// result for it under the fallback and by every branch under the union; and
// a tuple the form deduplicates that was emitted before is skipped. Returns
// false when the window is answered.
func (it *OrFallbackIterator) answer() bool {
	for {
		var advanced bool
		switch {
		case it.matches != nil:
			tuple, ok := it.next()
			if !ok {
				continue
			}
			it.currentTuple = tuple
			advanced = true
		case it.windowPos >= len(it.reader.window):
			return false
		case it.shortCircuit:
			advanced = it.nextShortCircuit()
		default:
			advanced = it.nextCorrelatedUnion()
		}
		if !advanced {
			continue
		}
		if it.seen == nil || !it.seen.PutIfAbsent(NewTupleKeyFull(it.currentTuple), struct{}{}) {
			return true
		}
	}
}

// nextCorrelatedUnion answers the window's tuples in turn from every
// branch: each branch's index is probed for the tuple's key, the required
// run and the carried outputs the subject carries, and its matches agreeing
// on the carried outputs are emitted, an output the environment alone binds
// being joined with the environment's tuple by that filter. Returns false
// when the window is answered.
func (it *OrFallbackIterator) nextCorrelatedUnion() bool {
	for it.windowPos < len(it.reader.window) {
		outerTuple := it.reader.window[it.windowPos]
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

// nextShortCircuit answers the window's tuples in turn: the first branch
// whose index holds the tuple's key answers it, and a tuple no branch
// answers is dropped. The decision reads the branch's result for the key
// before the carried-output filter: a tuple whose carried output disagrees
// with every match is answered with nothing, and the later branches never
// see it. Returns false when the window is answered.
func (it *OrFallbackIterator) nextShortCircuit() bool {
	for it.windowPos < len(it.reader.window) {
		outerTuple := it.reader.window[it.windowPos]
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

// emitCachedMatches reports a branch's answer for the window tuple and
// begins draining a probe's matches from the branch's index, setting the
// first output tuple as current. Returns false when there are no matches
// (the caller decides what follows). The matches are a key's group of a
// branch result that is already a set, so nothing re-deduplicates here, and
// an identity-shaped tuple aliases the index's.
func (it *OrFallbackIterator) emitCachedMatches(branchIdx int, cb *cachedBranch, matches []Tuple, outerTuple Tuple) bool {
	first, ok := it.begin(branchIdx, cb.branchSyms, matches, outerTuple)
	if !ok {
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
	it.currentTuple = first
	return true
}
