package executor

import (
	"errors"
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// executeRuleInvocation evaluates an invocation clause: the invoked
// predicate's relation, read or derived as invocationSource says, under the
// invocation's arguments. Each argument unifies with the head symbol at its
// position, and the head's symbols are found in the source by symbol, once,
// at construction; a source lacking one is the invocation's error. A variable
// argument renames its head symbol, a repeated variable selects the tuples
// whose two head positions agree, a constant selects on its value, a
// variable the environment binds selects on the environment's value, and a
// blank projects its head symbol away. The selection and projection are
// applied as the consumer pulls. An invocation whose arguments are distinct
// free variables is a rename of a materialized predicate: a relation over
// its tuples under the arguments' names. With no variable left the result is
// the zero-symbol verdict: one empty tuple when a tuple passes, none
// otherwise.
func (e *DefaultQueryExecutor) executeRuleInvocation(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, error) {
	source, head, err := e.invocationSource(ctx, inv, groups)
	if err != nil {
		return nil, err
	}
	positions, err := query.SymbolIndexTable(source.Symbols(), head)
	if err != nil {
		return nil, fmt.Errorf("rule invocation %s: %w", inv, err)
	}
	envSymbols, envTuple := environmentTuple(ctx.Environment())

	var symbols []query.Symbol
	var keep []int
	var equalities []positionEquality
	var fixings []positionFixing
	blankDropped := false
	for i, arg := range inv.Args {
		position := positions[i]
		switch a := arg.(type) {
		case query.Variable:
			if j := query.SymbolIndex(symbols, a.Name); j >= 0 {
				equalities = append(equalities, positionEquality{keep[j], position})
				continue
			}
			if k := query.SymbolIndex(envSymbols, a.Name); k >= 0 {
				fixings = append(fixings, positionFixing{position, envTuple[k]})
			}
			symbols = append(symbols, a.Name)
			keep = append(keep, position)
		case query.Blank:
			blankDropped = true
		case query.Constant:
			fixings = append(fixings, positionFixing{position, a.Value})
		case query.VectorConstant:
			fixings = append(fixings, positionFixing{position, a.Values})
		default:
			return nil, fmt.Errorf("rule invocation %s: argument %d is %T", inv, i+1, arg)
		}
	}
	passes := func(tuple Tuple) bool {
		for _, f := range fixings {
			if !datalog.ValuesEqual(tuple[f.position], f.value) {
				return false
			}
		}
		for _, eq := range equalities {
			if !datalog.ValuesEqual(tuple[eq.a], tuple[eq.b]) {
				return false
			}
		}
		return true
	}

	if len(symbols) == 0 {
		held := false
		err := ForEach(source, func(tuple Tuple) error {
			if passes(tuple) {
				held = true
				return errInvocationHeld
			}
			return nil
		})
		if err != nil && !errors.Is(err, errInvocationHeld) {
			return nil, fmt.Errorf("rule invocation %s: %w", inv, err)
		}
		if !held {
			return NewMaterializedRelationWithOptions([]query.Symbol{}, nil, e.options), nil
		}
		return NewUnitRelation(e.options), nil
	}

	renames := len(keep) == len(inv.Args)
	if mat, ok := source.(*MaterializedRelation); ok && renames && len(fixings) == 0 {
		subst, err := query.SubstitutionOf(head, symbols)
		if err != nil {
			return nil, fmt.Errorf("rule invocation %s: %w", inv, err)
		}
		view, err := mat.Rename(subst)
		if err != nil {
			return nil, fmt.Errorf("rule invocation %s: %w", inv, err)
		}
		return view, nil
	}
	asIs := renames && len(source.Symbols()) == len(head)
	for i := 0; asIs && i < len(positions); i++ {
		asIs = positions[i] == i
	}
	var it Iterator = &invocationIterator{inner: source.Iterator(), passes: passes, keep: keep, asIs: asIs}
	if blankDropped {
		// Dropping a free position can fold two tuples into one. A dropped
		// constant, environment-bound or repeated position cannot, since the
		// kept positions determine it.
		it = NewDedupIterator(it, 0, false)
	}
	return NewStreamingRelationWithProperties(symbols, it, e.options, deduplicatedProperties(symbols)), nil
}

// positionFixing fixes a position of the source to a value: a constant
// argument's, or the environment's for a variable it binds.
type positionFixing struct {
	position int
	value    interface{}
}

// positionEquality equates two positions of the source one variable names.
type positionEquality struct{ a, b int }

// errInvocationHeld stops a verdict's pull at the first passing tuple.
var errInvocationHeld = errors.New("rule invocation: a tuple passed")

// invocationIterator selects and projects the predicate's relation under the
// invocation as the consumer pulls: a tuple that passes the fixings and
// equalities is read out at the kept positions, in the arguments' order.
// When the source's layout is the head's and every position is kept, the
// source's tuple is yielded as it is.
type invocationIterator struct {
	inner   Iterator
	passes  func(Tuple) bool
	keep    []int
	asIs    bool
	current Tuple
}

func (it *invocationIterator) Next() bool {
	for it.inner.Next() {
		tuple := it.inner.Tuple()
		if !it.passes(tuple) {
			continue
		}
		if it.asIs {
			it.current = tuple
			return true
		}
		out := make(Tuple, len(it.keep))
		for j, position := range it.keep {
			out[j] = tuple[position]
		}
		it.current = out
		return true
	}
	return false
}

func (it *invocationIterator) Tuple() Tuple { return it.current }

func (it *invocationIterator) Close() error { return it.inner.Close() }

func (it *invocationIterator) Error() error { return it.inner.Error() }

// invocationSource is the relation an invocation reads, with its head, the
// symbols the invocation's arguments unify with by position. An invocation
// in a body the fixpoint is running, or one a form hoisted, reads the
// relation the scope holds under the symbol it invokes: the required run
// under scope/required, held over the required symbols by name, so its head
// is the invocation's own arguments; or a predicate's relation, or the
// stratum's last delta under the predicate's delta symbol, whose head is the
// rule set's. Any other invocation, the query's own or one in a subquery, is
// derived here.
func (e *DefaultQueryExecutor) invocationSource(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, []query.Symbol, error) {
	rel, held := ctx.Derived(inv.Name)
	if !held {
		return e.deriveInvocation(ctx, inv, groups)
	}
	if inv.Name == query.ScopeRequired {
		head := make([]query.Symbol, len(inv.Args))
		for i, arg := range inv.Args {
			head[i] = arg.(query.Variable).Name
		}
		return rel, head, nil
	}
	rs := ctx.Rules()
	if rs == nil {
		return nil, nil, fmt.Errorf("rule invocation %s: no rule set was given for %%", inv)
	}
	head, ok := rs.Head(inv.Name)
	if !ok {
		return nil, nil, fmt.Errorf("rule invocation %s: the rule set declares no predicate %s", inv, inv.Name)
	}
	return rel, head, nil
}

// deriveInvocation evaluates an invocation's predicate at its clause: the
// clause's evaluation, continued across the windows of the form execution
// the clause stands in when the context carries that execution's
// continuations, and otherwise an evaluation of this one run, completed at
// once. The evaluation is created at the clause's first run through
// invocationEvaluation and extended by every run with the seed the run's
// bindings give. The relation is returned with its head, the evaluation's
// set's for the predicate it derives: the predicate's own, or the adorned
// one the rewrite derives.
func (e *DefaultQueryExecutor) deriveInvocation(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, []query.Symbol, error) {
	rs := ctx.Rules()
	if rs == nil {
		return nil, nil, fmt.Errorf("rule invocation %s: no rule set was given for %%", inv)
	}
	bound := boundPositions(ctx, inv, groups)
	if err := rs.CheckRequired(inv.Name, bound, nil); err != nil {
		return nil, nil, err
	}
	var eval *ruleEvaluation
	if ctx.continued != nil {
		eval = ctx.continued.byClause[inv]
	}
	if eval == nil {
		var err error
		if eval, err = e.invocationEvaluation(rs, inv, bound); err != nil {
			return nil, nil, err
		}
		if ctx.continued != nil {
			ctx.continued.byClause[inv] = eval
		}
	}
	var seed Relation
	if eval.seed != nil {
		seedHead, _ := eval.rs.Head(eval.seed)
		var err error
		if seed, err = e.seedRelation(ctx, inv, bound, seedHead, groups); err != nil {
			return nil, nil, err
		}
	}
	rel, err := eval.extend(ctx, seed)
	if err != nil {
		return nil, nil, err
	}
	if ctx.continued == nil {
		eval.complete()
	}
	head, _ := eval.rs.Head(eval.predicate)
	return rel, head, nil
}

// invocationEvaluation is the evaluation an invocation runs: its predicate
// whole when nothing binds its arguments and no body the predicate needs
// invokes a predicate with required positions, and through the magic-set
// rewrite otherwise, seeded by the bound arguments when there are any, so
// the fixpoint derives only the tuples those arguments reach. A free
// invocation reaching a predicate with required positions is rewritten with
// its head all free, so that predicate is adorned from the clauses before
// each of its invocations and derived in the rewritten set, never drawn
// whole into an evaluation without its bindings and never derived at its
// clause on every run of the body invoking it
// (NESTED_SCOPES_AS_QUERIES.md §5.5). The rewrite reads each scope in the
// order the executor's planner gives it, and every rewrite is accepted: a
// site under a non-monotone form the rewritten set cannot carry adorned is
// left unadorned (planner.Rewrite).
func (e *DefaultQueryExecutor) invocationEvaluation(rs *query.RuleSet, inv *query.RuleInvocation, bound []bool) (*ruleEvaluation, error) {
	if !anyBound(bound) {
		requiring := false
		for p := range neededPredicates(rs, inv.Name) {
			for _, rule := range rs.Rules(p) {
				for _, invoked := range query.Invocations(rule.Body) {
					if anyBound(rs.Required(invoked.Name)) {
						requiring = true
					}
				}
			}
		}
		if !requiring {
			return newRuleEvaluation(e.executor, rs, inv.Name, nil), nil
		}
	}
	magic, err := planner.Rewrite(e.executor.planner, rs, inv.Name, bound, e.options.Handler)
	if err != nil {
		return nil, err
	}
	if e.options.Handler != nil {
		e.options.Handler(magicSetsEvent(inv.Name, bound))
	}
	return newRuleEvaluation(e.executor, magic.Rules, magic.Predicate, magic.Seed), nil
}

// magicSetsEvent is the rewrite made for one invocation: the predicate and
// its adornment.
func magicSetsEvent(name query.Symbol, bound []bool) annotations.Event {
	return annotations.Event{
		Name:  annotations.RulesMagicSets,
		Start: time.Now(),
		Data: map[string]interface{}{
			"predicate": name,
			"adornment": bound,
		},
	}
}

// boundPositions marks the invocation's positions the clause binds: a
// constant, or a variable the environment or a relation group binds, a
// repeated variable at its first position only.
func boundPositions(ctx *Context, inv *query.RuleInvocation, groups []Relation) []bool {
	envSymbols, _ := environmentTuple(ctx.Environment())
	bound := make([]bool, len(inv.Args))
	seen := map[query.Symbol]bool{}
	for i, arg := range inv.Args {
		switch a := arg.(type) {
		case query.Constant, query.VectorConstant:
			bound[i] = true
		case query.Variable:
			if seen[a.Name] {
				continue
			}
			seen[a.Name] = true
			bound[i] = query.SymbolIndex(envSymbols, a.Name) >= 0 || groupBinding(groups, a.Name) >= 0
		}
	}
	return bound
}

func anyBound(bound []bool) bool {
	for _, b := range bound {
		if b {
			return true
		}
	}
	return false
}

// groupBinding is the index of the group whose symbols hold sym, -1 when
// none does.
func groupBinding(groups []Relation, sym query.Symbol) int {
	for i, g := range groups {
		if query.ContainsSymbol(g.Symbols(), sym) {
			return i
		}
	}
	return -1
}

// seedRelation builds the seed of a rewritten set's magic predicate, over
// the seed predicate's head: one tuple per combination of the bound
// variables' values, the environment's value for a variable it binds and a
// group's values otherwise, with a constant at its position, each bound
// position's value at the head position the rewrite gave it.
func (e *DefaultQueryExecutor) seedRelation(ctx *Context, inv *query.RuleInvocation, bound []bool, head []query.Symbol, groups []Relation) (Relation, error) {
	envSymbols, envTuple := environmentTuple(ctx.Environment())
	byGroup := map[int][]query.Symbol{}
	var groupOrder []int
	for i, arg := range inv.Args {
		v, ok := arg.(query.Variable)
		if !bound[i] || !ok || query.SymbolIndex(envSymbols, v.Name) >= 0 {
			continue
		}
		g := groupBinding(groups, v.Name)
		if _, seen := byGroup[g]; !seen {
			groupOrder = append(groupOrder, g)
		}
		byGroup[g] = append(byGroup[g], v.Name)
	}
	// The groups' contribution is the product of each group's projection
	// onto the bound variables it binds; a projection is a set, so the
	// product is one too.
	combos := []Tuple{{}}
	var comboSymbols []query.Symbol
	for _, g := range groupOrder {
		projected, err := groups[g].Materialize().Project(byGroup[g])
		if err != nil {
			return nil, fmt.Errorf("rule invocation %s: seed projection: %w", inv, err)
		}
		copies := projected.RequiresCopy()
		var tuples []Tuple
		if err := ForEach(projected, func(tuple Tuple) error {
			if copies {
				tuple = copyTuple(tuple)
			}
			tuples = append(tuples, tuple)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("rule invocation %s: seed projection: %w", inv, err)
		}
		next := make([]Tuple, 0, len(combos)*len(tuples))
		for _, c := range combos {
			for _, t := range tuples {
				joined := make(Tuple, 0, len(c)+len(t))
				joined = append(append(joined, c...), t...)
				next = append(next, joined)
			}
		}
		combos = next
		comboSymbols = append(comboSymbols, projected.Symbols()...)
	}
	tuples := make([]Tuple, 0, len(combos))
	for _, c := range combos {
		t := make(Tuple, 0, len(head))
		for i, arg := range inv.Args {
			if !bound[i] {
				continue
			}
			switch a := arg.(type) {
			case query.Variable:
				if k := query.SymbolIndex(envSymbols, a.Name); k >= 0 {
					t = append(t, envTuple[k])
				} else {
					t = append(t, c[query.SymbolIndex(comboSymbols, a.Name)])
				}
			case query.Constant:
				t = append(t, a.Value)
			case query.VectorConstant:
				t = append(t, a.Values)
			}
		}
		tuples = append(tuples, t)
	}
	return newMaterializedRelationFromSet(head, tuples, e.options, deduplicatedProperties(head)), nil
}

// ruleEvaluation is an invocation's evaluation continued across the windows
// of the scope the invocation stands in: the needed strata of its rule set in
// order, each with its sets and its planned bodies, the derived relations,
// and the derived-tuple count, carried from window to window. A window's seed
// enters the seed predicate's stratum as a delta, and the strata run again
// from the lowest one that changed, so the evaluation derives each tuple once
// however the seeds are split across the windows
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2).
type ruleEvaluation struct {
	executor  *Executor
	rs        *query.RuleSet
	predicate query.Symbol // the predicate the invocation reads
	seed      query.Symbol // the seed predicate the windows fill, nil for a whole evaluation
	strata    []*stratumState
	derived   map[query.Symbol]Relation
	total     int // the tuples derived, every window's together
	windows   int
}

// stratumState is one stratum of an evaluation across its windows: the
// stratum, its planned bodies, its sets, whether it began, when, its
// iterations summed over the windows, and whether it completed.
type stratumState struct {
	stratum    query.Stratum
	eval       *stratumEvaluation
	full       map[query.Symbol]*derivedSet
	begun      bool
	start      time.Time
	iterations int
	completed  bool
}

// newRuleEvaluation is the evaluation of predicate under rs, fed through the
// seed predicate by the windows of the scope it stands in; a nil seed is a
// whole evaluation, which a later window leaves as it is.
func newRuleEvaluation(e *Executor, rs *query.RuleSet, predicate, seed query.Symbol) *ruleEvaluation {
	return &ruleEvaluation{executor: e, rs: rs, predicate: predicate, seed: seed, derived: map[query.Symbol]Relation{}}
}

// extend runs one window: seed, the window's tuples of the seed predicate,
// enters that predicate's stratum as a delta; each needed stratum runs when
// it holds the seed predicate and the window carries a seed, or when a
// lower-stratum predicate its bodies invoke gained tuples in the window; and
// the predicate's relation as it then stands is returned. The first window
// prepares the strata, the seed relation's symbols naming a seed predicate
// no rule derives, and runs every one of them.
func (ev *ruleEvaluation) extend(ctx *Context, seed Relation) (Relation, error) {
	ev.windows++
	if ev.windows == 1 {
		needed := neededPredicates(ev.rs, ev.predicate)
		for _, stratum := range ev.rs.Strata() {
			wanted := false
			for _, p := range stratum.Predicates {
				if needed[p] {
					wanted = true
					break
				}
			}
			if !wanted {
				continue
			}
			eval, err := ev.executor.prepareStratum(ctx, ev.rs, stratum)
			if err != nil {
				return nil, err
			}
			s := &stratumState{stratum: stratum, eval: eval, full: map[query.Symbol]*derivedSet{}}
			for _, p := range stratum.Predicates {
				s.full[p] = newDerivedSet(eval.symbols[p])
			}
			ev.strata = append(ev.strata, s)
		}
	}
	// added holds, per predicate, the tuples the window added, which the
	// strata above read as their lower deltas.
	added := map[query.Symbol][]Tuple{}
	for _, s := range ev.strata {
		var stratumSeed Relation
		if s.full[ev.seed] != nil {
			stratumSeed = seed
		}
		changed := ev.windows == 1 || stratumSeed != nil
		for _, body := range s.eval.lower {
			if len(added[body.lower]) > 0 {
				changed = true
				break
			}
		}
		if !changed {
			continue
		}
		grown, err := ev.runStratum(ctx, s, stratumSeed, added)
		if err != nil {
			return nil, err
		}
		for p, tuples := range grown {
			added[p] = tuples
		}
	}
	return ev.derived[ev.predicate], nil
}

// neededPredicates is the set of predicates the invoked one depends on,
// itself included: its rules' invocations, transitively. A predicate with
// required positions is not among them and is not walked through: it is
// evaluated at its invocation, under the invoking body's bindings, never
// whole.
func neededPredicates(rs *query.RuleSet, invoked query.Symbol) map[query.Symbol]bool {
	needed := map[query.Symbol]bool{}
	pending := []query.Symbol{invoked}
	for len(pending) > 0 {
		p := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if needed[p] || anyBound(rs.Required(p)) {
			continue
		}
		needed[p] = true
		for _, rule := range rs.Rules(p) {
			for _, inv := range query.Invocations(rule.Body) {
				if !needed[inv.Name] {
					pending = append(pending, inv.Name)
				}
			}
		}
	}
	return needed
}

// derivedSet accumulates a predicate's tuples over its canonical symbols,
// each once.
type derivedSet struct {
	symbols []query.Symbol
	tuples  []Tuple
	keys    *TupleKeyMap
}

func newDerivedSet(symbols []query.Symbol) *derivedSet {
	return &derivedSet{symbols: symbols, keys: NewTupleKeyMap()}
}

// absorb adds the tuples of rel the set does not hold, as they are pulled,
// and returns them, each held over the set's symbols. rel carries the set's
// symbols in any layout, found by symbol once; a relation lacking one is the
// set's error. A tuple in the set's layout is probed before anything else,
// so one the set holds is rejected without a copy, and is held as it is from
// a stable source or copied once, on admission, from a reusing one. A tuple
// in another layout is read out at the set's positions into a copy before it
// is probed. Every key holds the held tuple by reference.
func (s *derivedSet) absorb(rel Relation) ([]Tuple, error) {
	positions, err := query.SymbolIndexTable(rel.Symbols(), s.symbols)
	if err != nil {
		return nil, fmt.Errorf("absorbing into %v: %w", s.symbols, err)
	}
	sameLayout := len(rel.Symbols()) == len(s.symbols)
	for i := 0; sameLayout && i < len(positions); i++ {
		sameLayout = positions[i] == i
	}
	copies := rel.RequiresCopy()
	var added []Tuple
	err = ForEach(rel, func(tuple Tuple) error {
		if !sameLayout {
			held := make(Tuple, len(positions))
			for j, position := range positions {
				held[j] = tuple[position]
			}
			tuple = held
		}
		if s.keys.Exists(NewTupleKeyFull(tuple)) {
			return nil
		}
		if sameLayout && copies {
			tuple = copyTuple(tuple)
		}
		s.keys.PutIfAbsent(NewTupleKeyFull(tuple), struct{}{})
		s.tuples = append(s.tuples, tuple)
		added = append(added, tuple)
		return nil
	})
	return added, err
}

// relation is the set as a relation over the tuples added so far.
func (s *derivedSet) relation(opts ExecutorOptions) Relation {
	n := len(s.tuples)
	return newMaterializedRelationFromSet(s.symbols, s.tuples[:n:n], opts, deduplicatedProperties(s.symbols))
}

// plannedBody is one rule body planned for the fixpoint, with, for a copy
// marked on a lower-stratum predicate, that predicate.
type plannedBody struct {
	rule  *query.Rule
	plan  *planner.RealizedPlan
	lower query.Symbol
}

// stratumEvaluation is a stratum's planned bodies, in three kinds, under the
// rule set they were planned with, and the canonical symbols of each of its
// predicates, the set's head for it. plain holds each body with no
// same-stratum invocation at its top level, planned once and run at the
// first window's iteration zero over the view; a body whose same-stratum
// occurrences all sit inside or and or-join branches or subquery bodies is
// among them, since a branch or an inner query invoking no same-stratum
// predicate produces without any delta. delta holds, per same-stratum
// occurrence of a body, at the top level, in a branch or in a subquery body,
// the copy whose occurrence invokes the predicate's delta symbol, run each
// iteration after the first. lower holds, per occurrence of a seed-restricted predicate of a
// lower stratum, the copy whose occurrence invokes that predicate's delta
// symbol, run at a later window's iteration zero over the tuples the window
// added to that predicate.
type stratumEvaluation struct {
	executor *Executor
	rs       *query.RuleSet
	stratum  query.Stratum
	symbols  map[query.Symbol][]query.Symbol
	plain    []plannedBody
	delta    []plannedBody
	lower    []plannedBody
}

// prepareStratum plans every body of the stratum under the rule set. Each
// plan is a query's plan under its own text, which carries % and the delta
// symbols, so the planner serves and stores it through the plan cache as it
// does any query's.
func (e *Executor) prepareStratum(ctx *Context, rs *query.RuleSet, stratum query.Stratum) (*stratumEvaluation, error) {
	inStratum := map[query.Symbol]bool{}
	for _, p := range stratum.Predicates {
		inStratum[p] = true
	}
	eval := &stratumEvaluation{executor: e, rs: rs, stratum: stratum, symbols: map[query.Symbol][]query.Symbol{}}
	for _, p := range stratum.Predicates {
		head, _ := rs.Head(p)
		eval.symbols[p] = head
		for _, rule := range rs.Rules(p) {
			occurrences := 0
			var lowerOrder []query.Symbol
			lowerCount := map[query.Symbol]int{}
			for _, inv := range query.Invocations(rule.Body) {
				switch {
				case inStratum[inv.Name]:
					occurrences++
				case query.SeedRestricted(inv.Name):
					if lowerCount[inv.Name] == 0 {
						lowerOrder = append(lowerOrder, inv.Name)
					}
					lowerCount[inv.Name]++
				}
			}
			topLevel := 0
			for _, clause := range rule.Body {
				if inv, ok := clause.(*query.RuleInvocation); ok && inStratum[inv.Name] {
					topLevel++
				}
			}
			if occurrences == 0 || topLevel == 0 {
				plan, err := e.planBody(rs, rule, rule.Body)
				if err != nil {
					return nil, err
				}
				eval.plain = append(eval.plain, plannedBody{rule: rule, plan: plan})
			}
			for j := 0; j < occurrences; j++ {
				plan, err := e.planBody(rs, rule, markDelta(rule.Body, inStratum, j))
				if err != nil {
					return nil, err
				}
				eval.delta = append(eval.delta, plannedBody{rule: rule, plan: plan})
			}
			for _, name := range lowerOrder {
				marked := map[query.Symbol]bool{name: true}
				for j := 0; j < lowerCount[name]; j++ {
					plan, err := e.planBody(rs, rule, markDelta(rule.Body, marked, j))
					if err != nil {
						return nil, err
					}
					eval.lower = append(eval.lower, plannedBody{rule: rule, plan: plan, lower: name})
				}
			}
		}
	}
	return eval, nil
}

// planBody plans a rule body as a query finding the rule's head, under the
// rule set the body's invocations resolve against. The planner reads from
// each invoked predicate's name whether a seed restricts its relation: a
// magic or adorned predicate, or the delta of one, leads the body's patterns,
// and any other invocation joins them at collapse.
func (e *Executor) planBody(rs *query.RuleSet, rule *query.Rule, body []query.Clause) (*planner.RealizedPlan, error) {
	find := make([]query.FindElement, len(rule.Head))
	for i, head := range rule.Head {
		find[i] = query.FindVariable{Symbol: head}
	}
	q := &query.Query{
		Find:  find,
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
		Where: body,
	}
	plan, err := e.planner.PlanQuery(q, query.Bindings{Rules: rs}, e.options.Handler)
	if err != nil {
		return nil, fmt.Errorf("planning rule %s: %w", rule.Name, err)
	}
	return plan, nil
}

// markDelta copies clauses with the target-th invocation of a predicate in
// marked, in clause order at the top level, inside or-join branches and
// inside subquery bodies at any depth, replaced by a copy invoking the
// predicate's delta symbol. The or-join forms and the subqueries on the path
// are copied, a subquery over its inner query derived over the walked
// clauses, and every other clause is shared: a same-stratum invocation
// stands nowhere else, since the bodies walked are the inferred ones the
// rule set holds, whose every form carries a header.
func markDelta(clauses []query.Clause, marked map[query.Symbol]bool, target int) []query.Clause {
	counter := 0
	return markDeltaWalk(clauses, marked, target, &counter)
}

func markDeltaWalk(clauses []query.Clause, marked map[query.Symbol]bool, target int, counter *int) []query.Clause {
	out := make([]query.Clause, len(clauses))
	for i, clause := range clauses {
		switch c := clause.(type) {
		case *query.RuleInvocation:
			out[i] = c
			if marked[c.Name] {
				if *counter == target {
					out[i] = &query.RuleInvocation{Name: query.DeltaSymbol(c.Name), Args: c.Args}
				}
				*counter++
			}
		case *query.OrJoinClause:
			out[i] = c
			if branches, found := markDeltaBranches(c.Branches, marked, target, counter); found {
				out[i] = &query.OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}
			}
		case *query.SubqueryPattern:
			out[i] = c
			before := *counter
			walked := markDeltaWalk(c.Query.Where, marked, target, counter)
			if before <= target && target < *counter {
				out[i] = &query.SubqueryPattern{Query: c.Query.WithWhere(walked), Inputs: c.Inputs, Binding: c.Binding}
			}
		default:
			out[i] = clause
		}
	}
	return out
}

// markDeltaBranches walks a form's branches and reports whether the target
// occurrence fell inside them, which is when the walked copy replaces the
// form; otherwise the form is shared.
func markDeltaBranches(branches [][]query.Clause, marked map[query.Symbol]bool, target int, counter *int) ([][]query.Clause, bool) {
	before := *counter
	walked := make([][]query.Clause, len(branches))
	for b, branch := range branches {
		walked[b] = markDeltaWalk(branch, marked, target, counter)
	}
	return walked, before <= target && target < *counter
}

// run executes bodies under a scope holding held, the relations by predicate
// symbol, under the rule set the bodies were planned with, whose heads the
// held relations are over, and returns each body's result relation by
// predicate, renamed under the predicate's canonical symbols through the
// substitution of the rule's head for them, for the sets to absorb. A body
// is a query scope of its own, with no bound inputs: the invoking query's
// environment does not reach it.
func (s *stratumEvaluation) run(ctx *Context, held map[query.Symbol]Relation, bodies []plannedBody) (map[query.Symbol][]Relation, error) {
	scope := ctx.WithEnvironment(nil).WithRules(s.rs).WithDerived(held)
	results := map[query.Symbol][]Relation{}
	for _, body := range bodies {
		rel, err := s.executor.executeRealizedPlan(scope, body.plan, nil)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", body.rule.Name, err)
		}
		if canonical := s.symbols[body.rule.Name]; !symbolsMatch(body.rule.Head, canonical) {
			subst, err := query.SubstitutionOf(body.rule.Head, canonical)
			if err != nil {
				return nil, fmt.Errorf("rule %s: %w", body.rule.Name, err)
			}
			if rel, err = rel.Rename(subst); err != nil {
				return nil, fmt.Errorf("rule %s: %w", body.rule.Name, err)
			}
		}
		results[body.rule.Name] = append(results[body.rule.Name], rel)
	}
	return results, nil
}

// runStratum runs one window of a stratum and returns the tuples the window
// added per predicate. The seed, when the stratum holds the seed predicate,
// is absorbed into that predicate's set as the window's first delta. At the
// first window the plain bodies run over the view, and the stratum's begin
// event fires; at a later window the copies marked on a lower predicate that
// gained tuples in the window run over those tuples, held under the
// predicate's delta symbol, and a window that adds no seed tuple and has no
// such copy runs nothing. Then each iteration runs the same-stratum copies
// over the previous iteration's deltas until none is left, the iteration
// bound counted from the window's first iteration and the tuple bound over
// the evaluation's total; a seed's tuples are given, not derived, and are
// not counted.
func (ev *ruleEvaluation) runStratum(ctx *Context, s *stratumState, seed Relation, lower map[query.Symbol][]Tuple) (map[query.Symbol][]Tuple, error) {
	e := ev.executor
	handler := e.options.Handler
	stratum := s.stratum
	if !s.begun {
		s.begun = true
		if handler != nil {
			s.start = time.Now()
			handler(annotations.Event{
				Name:  annotations.RulesStratumBegin,
				Start: s.start,
				Data: map[string]interface{}{
					"predicates": stratum.Predicates,
					"recursive":  stratum.Recursive,
				},
			})
		}
	}
	// view is the derived map a run reads: every earlier stratum's relation
	// and this stratum's sets as they stand.
	view := func() map[query.Symbol]Relation {
		m := make(map[query.Symbol]Relation, len(ev.derived)+len(s.full))
		for p, rel := range ev.derived {
			m[p] = rel
		}
		for p, set := range s.full {
			m[p] = set.relation(e.options)
		}
		return m
	}
	added := map[query.Symbol][]Tuple{}
	// absorb pulls a run's results into the sets and returns the new tuples
	// per predicate, counting them into the window's additions and the
	// evaluation's total.
	absorb := func(results map[query.Symbol][]Relation) (map[query.Symbol][]Tuple, error) {
		next := map[query.Symbol][]Tuple{}
		for p, rels := range results {
			for _, rel := range rels {
				tuples, err := s.full[p].absorb(rel)
				if err != nil {
					return nil, fmt.Errorf("rule %s: %w", p, err)
				}
				if len(tuples) > 0 {
					next[p] = append(next[p], tuples...)
					added[p] = append(added[p], tuples...)
					ev.total += len(tuples)
				}
			}
		}
		if limit := e.options.MaxDerivedTuples; limit > 0 && ev.total > limit {
			return nil, &query.DerivedTupleLimitError{Predicates: stratum.Predicates, Limit: limit, Reached: ev.total}
		}
		return next, nil
	}
	iteration := 0
	emitIteration := func(delta map[query.Symbol][]Tuple) {
		if handler == nil {
			return
		}
		sizes := make(map[query.Symbol]int, len(delta))
		for p, tuples := range delta {
			sizes[p] = len(tuples)
		}
		handler(annotations.Event{
			Name:  annotations.RulesIteration,
			Start: time.Now(),
			Data: map[string]interface{}{
				"predicates": stratum.Predicates,
				"iteration":  iteration,
				"delta":      sizes,
			},
		})
	}

	delta := map[query.Symbol][]Tuple{}
	if seed != nil {
		tuples, err := s.full[ev.seed].absorb(seed)
		if err != nil {
			return nil, fmt.Errorf("seeding %s: %w", ev.seed, err)
		}
		if len(tuples) > 0 {
			delta[ev.seed] = tuples
			added[ev.seed] = tuples
		}
	}
	held := view()
	bodies := s.eval.plain
	if ev.windows > 1 {
		bodies = nil
		for _, body := range s.eval.lower {
			if len(lower[body.lower]) > 0 {
				bodies = append(bodies, body)
			}
		}
		if len(delta) == 0 && len(bodies) == 0 {
			return nil, nil
		}
		for p, tuples := range lower {
			symbols := ev.derived[p].Symbols()
			held[query.DeltaSymbol(p)] = newMaterializedRelationFromSet(symbols, tuples, e.options, deduplicatedProperties(symbols))
		}
	}
	seeding, err := s.eval.run(ctx, held, bodies)
	if err != nil {
		return nil, err
	}
	grown, err := absorb(seeding)
	if err != nil {
		return nil, err
	}
	for p, tuples := range grown {
		delta[p] = append(delta[p], tuples...)
	}
	emitIteration(delta)
	for stratum.Recursive && len(delta) > 0 {
		if err := ctx.cancelled(); err != nil {
			return nil, err
		}
		iteration++
		if limit := e.options.MaxRuleIterations; limit > 0 && iteration > limit {
			return nil, &query.RuleIterationLimitError{Predicates: stratum.Predicates, Limit: limit, Reached: iteration}
		}
		// The run holds the last iteration's delta of each predicate under
		// the predicate's delta symbol, beside the relations as they stand.
		held = view()
		for _, p := range stratum.Predicates {
			held[query.DeltaSymbol(p)] = newMaterializedRelationFromSet(s.eval.symbols[p], delta[p], e.options, deduplicatedProperties(s.eval.symbols[p]))
		}
		results, err := s.eval.run(ctx, held, s.eval.delta)
		if err != nil {
			return nil, err
		}
		delta, err = absorb(results)
		if err != nil {
			return nil, err
		}
		emitIteration(delta)
	}
	s.iterations += iteration
	for _, p := range stratum.Predicates {
		ev.derived[p] = s.full[p].relation(e.options)
	}
	return added, nil
}

// complete emits the complete event of every stratum that began and has not
// completed, with its iterations summed over the windows and its sets'
// tuples; a stratum completes once.
func (ev *ruleEvaluation) complete() {
	handler := ev.executor.options.Handler
	for _, s := range ev.strata {
		if !s.begun || s.completed {
			continue
		}
		s.completed = true
		if handler == nil {
			continue
		}
		tuples := 0
		for _, p := range s.stratum.Predicates {
			tuples += len(s.full[p].tuples)
		}
		handler(annotations.TimedEvent(annotations.RulesStratumComplete, s.start, map[string]interface{}{
			"predicates": s.stratum.Predicates,
			"iterations": s.iterations,
			"tuples":     tuples,
		}))
	}
}

// ruleContinuations is the evaluations a form execution continues across its
// windows, by the invocation clause each evaluates: the outermost form's
// iterator creates them, every scope under it shares them through the
// context, and that iterator completes them at the subject's exhaustion and
// at its close.
type ruleContinuations struct {
	byClause map[*query.RuleInvocation]*ruleEvaluation
}

// newRuleContinuations is a form execution's continuations, holding none.
func newRuleContinuations() *ruleContinuations {
	return &ruleContinuations{byClause: map[*query.RuleInvocation]*ruleEvaluation{}}
}

// complete completes every evaluation held.
func (c *ruleContinuations) complete() {
	for _, ev := range c.byClause {
		ev.complete()
	}
}
