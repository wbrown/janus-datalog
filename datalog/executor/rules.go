package executor

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// executeRuleInvocation evaluates an invocation clause: the invoked
// predicate's relation, read or derived as invocationSource says, projected
// onto the arguments. A variable argument names its position, a repeated
// variable equates its positions, a constant filters its position to the
// value, a blank drops it, and a variable the environment binds filters its
// position to the environment's value. The projection is applied as the
// consumer pulls. An invocation whose arguments are distinct free variables
// reads a materialized predicate in place, under the arguments' names. With
// no variable left the result is the zero-symbol verdict: one empty tuple
// when a tuple passes, none otherwise.
func (e *DefaultQueryExecutor) executeRuleInvocation(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, error) {
	source, err := e.invocationSource(ctx, inv, groups)
	if err != nil {
		return nil, err
	}
	envSymbols, envTuple := environmentTuple(ctx.Environment())

	var symbols []query.Symbol
	var keep []int
	var equalities []positionEquality
	var fixings []positionFixing
	blankDropped := false
	for i, arg := range inv.Args {
		switch a := arg.(type) {
		case query.Variable:
			if j := query.SymbolIndex(symbols, a.Name); j >= 0 {
				equalities = append(equalities, positionEquality{keep[j], i})
				continue
			}
			if k := query.SymbolIndex(envSymbols, a.Name); k >= 0 {
				fixings = append(fixings, positionFixing{i, envTuple[k]})
			}
			symbols = append(symbols, a.Name)
			keep = append(keep, i)
		case query.Blank:
			blankDropped = true
		case query.Constant:
			fixings = append(fixings, positionFixing{i, a.Value})
		case query.VectorConstant:
			fixings = append(fixings, positionFixing{i, a.Values})
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
		view := newMaterializedRelationFromSet(symbols, mat.tuples, e.options, mat.properties.renameSymbols(mat.symbols, symbols))
		view.err = mat.err
		return view, nil
	}
	var it Iterator = &invocationIterator{inner: source.Iterator(), passes: passes, keep: keep, renames: renames}
	if blankDropped {
		// Dropping a free position can fold two tuples into one. A dropped
		// constant, environment-bound or repeated position cannot, since the
		// kept positions determine it.
		it = NewDedupIterator(it, 0, false)
	}
	return NewStreamingRelationWithProperties(symbols, it, e.options, deduplicatedProperties(symbols)), nil
}

// positionFixing fixes an invocation position to a value: a constant
// argument's, or the environment's for a variable it binds.
type positionFixing struct {
	position int
	value    interface{}
}

// positionEquality equates two positions one variable names.
type positionEquality struct{ a, b int }

// errInvocationHeld stops a verdict's pull at the first passing tuple.
var errInvocationHeld = errors.New("rule invocation: a tuple passed")

// invocationIterator projects the predicate's relation onto the invocation
// as the consumer pulls: a tuple that passes the fixings and equalities is
// read out at the kept positions, in the arguments' order. When every
// position is kept in place the source's tuple is yielded as it is.
type invocationIterator struct {
	inner   Iterator
	passes  func(Tuple) bool
	keep    []int
	renames bool
	current Tuple
}

func (it *invocationIterator) Next() bool {
	for it.inner.Next() {
		tuple := it.inner.Tuple()
		if !it.passes(tuple) {
			continue
		}
		if it.renames {
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

// invocationSource is the relation an invocation reads. An invocation in a
// body the fixpoint is running, or one a form hoisted, reads the relation
// the scope holds under the symbol it invokes: a predicate's relation, or
// the stratum's last delta under the predicate's delta symbol. Any other
// invocation, the query's own or one in a subquery, is derived here.
func (e *DefaultQueryExecutor) invocationSource(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, error) {
	if rel, ok := ctx.Derived(inv.Name); ok {
		return rel, nil
	}
	rel, _, err := e.deriveInvocation(ctx, inv, groups)
	return rel, err
}

// deriveInvocation evaluates an invocation's predicate: whole when nothing
// binds its arguments, and through the magic-set rewrite seeded by the bound
// arguments otherwise, so the fixpoint derives only the tuples those
// arguments reach. A rewrite the stratifier refuses is declined, recorded,
// and the predicate evaluated whole; a predicate with required positions is
// never evaluated whole, so its decline is the refusal, returned naming the
// invocation. The relation is returned with the symbol it is the relation
// of: the predicate's own, or the adorned one the rewrite derives.
func (e *DefaultQueryExecutor) deriveInvocation(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, query.Symbol, error) {
	rs := ctx.Rules()
	if rs == nil {
		return nil, nil, fmt.Errorf("rule invocation %s: no rule set was given for %%", inv)
	}
	bound := boundPositions(ctx, inv, groups)
	if err := rs.CheckRequired(inv.Name, bound, nil); err != nil {
		return nil, nil, err
	}
	if !anyBound(bound) {
		derived, err := e.executor.evaluateRules(ctx, rs, inv.Name, nil)
		if err != nil {
			return nil, nil, err
		}
		return derived[inv.Name], inv.Name, nil
	}
	magic, err := rs.Magic(inv.Name, bound)
	var unstratifiable *query.UnstratifiableError
	if errors.As(err, &unstratifiable) {
		if e.options.Handler != nil {
			e.options.Handler(magicSetsEvent(inv.Name, bound, false, unstratifiable.Error()))
		}
		if anyBound(rs.Required(inv.Name)) {
			return nil, nil, fmt.Errorf("rule invocation %s: the rewrite for its bindings is declined, %w, and a predicate requiring bindings has no whole evaluation to fall back to", inv, err)
		}
		derived, err := e.executor.evaluateRules(ctx, rs, inv.Name, nil)
		if err != nil {
			return nil, nil, err
		}
		return derived[inv.Name], inv.Name, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if e.options.Handler != nil {
		e.options.Handler(magicSetsEvent(inv.Name, bound, true, ""))
	}
	seed, err := e.seedRelation(ctx, inv, bound, magic.Seed, groups)
	if err != nil {
		return nil, nil, err
	}
	derived, err := e.executor.evaluateRules(ctx, magic.Rules, magic.Predicate, map[query.Symbol]Relation{magic.Seed: seed})
	if err != nil {
		return nil, nil, err
	}
	return derived[magic.Predicate], magic.Predicate, nil
}

// hoistInvocations derives, once, the predicate of every invocation the
// clauses of a form hold at any depth outside a subquery, bound by the
// environment and the outer relation the form runs against per combination,
// and returns the clauses with each such invocation naming the symbol its
// relation is held under, and the scope holding those relations. The forms
// on the path to a replaced invocation are copied; every other clause is
// shared. An invocation of a predicate the scope already holds stands as it
// is.
func (e *DefaultQueryExecutor) hoistInvocations(ctx *Context, clauses []query.Clause, outer Relation) ([]query.Clause, *Context, error) {
	held := map[query.Symbol]Relation{}
	out, changed, err := e.hoistWalk(ctx, clauses, outer, held)
	if err != nil {
		return nil, nil, err
	}
	if len(held) == 0 {
		return clauses, ctx, nil
	}
	merged := make(map[query.Symbol]Relation, len(ctx.derived)+len(held))
	for name, rel := range ctx.derived {
		merged[name] = rel
	}
	for name, rel := range held {
		merged[name] = rel
	}
	if !changed {
		out = clauses
	}
	return out, ctx.WithDerived(merged), nil
}

// hoistBranches hoists the invocations of each branch of a form, sharing
// the branches that hold none.
func (e *DefaultQueryExecutor) hoistBranches(ctx *Context, branches [][]query.Clause, outer Relation) ([][]query.Clause, *Context, error) {
	held := map[query.Symbol]Relation{}
	out := make([][]query.Clause, len(branches))
	changed := false
	for b, branch := range branches {
		walked, branchChanged, err := e.hoistWalk(ctx, branch, outer, held)
		if err != nil {
			return nil, nil, err
		}
		out[b] = branch
		if branchChanged {
			out[b] = walked
			changed = true
		}
	}
	if len(held) == 0 {
		return branches, ctx, nil
	}
	merged := make(map[query.Symbol]Relation, len(ctx.derived)+len(held))
	for name, rel := range ctx.derived {
		merged[name] = rel
	}
	for name, rel := range held {
		merged[name] = rel
	}
	if !changed {
		out = branches
	}
	return out, ctx.WithDerived(merged), nil
}

// hoistWalk walks one clause list for hoistInvocations, deriving each
// invocation into held and reporting whether any clause was replaced. The
// walk is its own, as markDeltaWalk's is: query.WalkClauses visits clauses
// and yields no list, and the hoist yields the list with an invocation
// replaced by one naming the symbol its relation is held under.
func (e *DefaultQueryExecutor) hoistWalk(ctx *Context, clauses []query.Clause, outer Relation, held map[query.Symbol]Relation) ([]query.Clause, bool, error) {
	out := make([]query.Clause, len(clauses))
	changed := false
	for i, clause := range clauses {
		out[i] = clause
		switch c := clause.(type) {
		case *query.RuleInvocation:
			if _, ok := ctx.Derived(c.Name); ok {
				continue
			}
			rel, under, err := e.deriveInvocation(ctx, c, []Relation{outer})
			if err != nil {
				return nil, false, err
			}
			// An adorned symbol names the predicate and its binding pattern,
			// and the relation held under it is a function of the seed as
			// well, so two invocations adorned alike but seeded from
			// different variables hold the union of their relations, which
			// each invocation's projection narrows to its own bound values.
			if prior, ok := held[under]; ok {
				rel = unionRelations([]Relation{prior, rel}, rel.Symbols(), e.options)
			}
			held[under] = rel
			if under != c.Name {
				out[i] = &query.RuleInvocation{Name: under, Args: c.Args}
				changed = true
			}
		case *query.NotClause:
			inner, innerChanged, err := e.hoistWalk(ctx, c.Clauses, outer, held)
			if err != nil {
				return nil, false, err
			}
			if innerChanged {
				out[i] = &query.NotClause{Clauses: inner}
				changed = true
			}
		case *query.NotJoinClause:
			inner, innerChanged, err := e.hoistWalk(ctx, c.Clauses, outer, held)
			if err != nil {
				return nil, false, err
			}
			if innerChanged {
				out[i] = &query.NotJoinClause{JoinVars: c.JoinVars, Clauses: inner}
				changed = true
			}
		case *query.OrClause:
			branches, branchesChanged, err := e.hoistWalkBranches(ctx, c.Branches, outer, held)
			if err != nil {
				return nil, false, err
			}
			if branchesChanged {
				out[i] = &query.OrClause{Branches: branches}
				changed = true
			}
		case *query.OrJoinClause:
			branches, branchesChanged, err := e.hoistWalkBranches(ctx, c.Branches, outer, held)
			if err != nil {
				return nil, false, err
			}
			if branchesChanged {
				out[i] = &query.OrJoinClause{JoinVars: c.JoinVars, Branches: branches}
				changed = true
			}
		case *query.OrDefaultClause:
			branches, branchesChanged, err := e.hoistWalkBranches(ctx, c.Branches, outer, held)
			if err != nil {
				return nil, false, err
			}
			if branchesChanged {
				out[i] = &query.OrDefaultClause{Branches: branches}
				changed = true
			}
		case *query.OrDefaultJoinClause:
			branches, branchesChanged, err := e.hoistWalkBranches(ctx, c.Branches, outer, held)
			if err != nil {
				return nil, false, err
			}
			if branchesChanged {
				out[i] = &query.OrDefaultJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}
				changed = true
			}
		}
	}
	return out, changed, nil
}

// hoistWalkBranches walks a form's branches for hoistWalk.
func (e *DefaultQueryExecutor) hoistWalkBranches(ctx *Context, branches [][]query.Clause, outer Relation, held map[query.Symbol]Relation) ([][]query.Clause, bool, error) {
	out := make([][]query.Clause, len(branches))
	changed := false
	for b, branch := range branches {
		walked, branchChanged, err := e.hoistWalk(ctx, branch, outer, held)
		if err != nil {
			return nil, false, err
		}
		out[b] = branch
		if branchChanged {
			out[b] = walked
			changed = true
		}
	}
	return out, changed, nil
}

// magicSetsEvent is the rewrite's decision for one invocation.
func magicSetsEvent(name query.Symbol, bound []bool, applied bool, reason string) annotations.Event {
	return annotations.Event{
		Name:  annotations.RulesMagicSets,
		Start: time.Now(),
		Data: map[string]interface{}{
			"predicate": name,
			"adornment": bound,
			"applied":   applied,
			"reason":    reason,
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

// seedRelation builds the seed of a rewritten set's magic predicate: one
// tuple per combination of the bound variables' values, the environment's
// value for a variable it binds and a group's values otherwise, with a
// constant at its position. Its symbols name the seed predicate's positions,
// which are its canonical symbols when no rule derives into it.
func (e *DefaultQueryExecutor) seedRelation(ctx *Context, inv *query.RuleInvocation, bound []bool, seed query.Symbol, groups []Relation) (Relation, error) {
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
	var symbols []query.Symbol
	for i := range inv.Args {
		if bound[i] {
			symbols = append(symbols, datalog.NewSymbol("?"+seed.String()+"."+strconv.Itoa(i)))
		}
	}
	tuples := make([]Tuple, 0, len(combos))
	for _, c := range combos {
		t := make(Tuple, 0, len(symbols))
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
	return newMaterializedRelationFromSet(symbols, tuples, e.options, deduplicatedProperties(symbols)), nil
}

// evaluateRules derives the relation of every stratum the invoked predicate
// depends on, in stratum order, and returns the relations by predicate.
// seeds carries the relations the caller fills, by predicate, each absorbed
// at its stratum's iteration zero.
func (e *Executor) evaluateRules(ctx *Context, rs *query.RuleSet, invoked query.Symbol, seeds map[query.Symbol]Relation) (map[query.Symbol]Relation, error) {
	needed := neededPredicates(rs, invoked)
	derived := map[query.Symbol]Relation{}
	total := 0
	for _, stratum := range rs.Strata() {
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
		if err := e.evaluateStratum(ctx, rs, stratum, derived, seeds, &total); err != nil {
			return nil, err
		}
	}
	return derived, nil
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
// and returns them. Each tuple is probed before anything else, so a tuple
// the set holds is rejected without a copy. A tuple a stable source yields
// is held as it is; one a reusing source yields is copied once, on
// admission, and the key holds that copy by reference.
func (s *derivedSet) absorb(rel Relation) ([]Tuple, error) {
	copies := rel.RequiresCopy()
	var added []Tuple
	err := ForEach(rel, func(tuple Tuple) error {
		if s.keys.Exists(NewTupleKeyFull(tuple)) {
			return nil
		}
		if copies {
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

// plannedBody is one rule body planned for the fixpoint. A body with no
// same-stratum invocation is planned once, with delta false, and runs once to
// seed the stratum. A body with k such occurrences is planned k times, each
// with one occurrence invoking its predicate's delta symbol and delta true,
// and every copy runs each iteration. A body whose occurrences all sit inside
// or and or-join branches is planned once more with delta false, since a
// branch invoking no same-stratum predicate produces without any delta.
type plannedBody struct {
	rule  *query.Rule
	plan  *planner.RealizedPlan
	delta bool
}

// stratumEvaluation is a stratum's planned bodies and the canonical symbols
// of each of its predicates: the first rule's head, or the seed relation's
// symbols for a predicate no rule derives.
type stratumEvaluation struct {
	executor *Executor
	stratum  query.Stratum
	symbols  map[query.Symbol][]query.Symbol
	bodies   []plannedBody
}

// prepareStratum plans every body of the stratum under the rule set. Each
// plan is a query's plan under its own text, which carries % and the delta
// symbols, so the planner serves and stores it through the plan cache as it
// does any query's.
func (e *Executor) prepareStratum(ctx *Context, rs *query.RuleSet, stratum query.Stratum, seeds map[query.Symbol]Relation) (*stratumEvaluation, error) {
	inStratum := map[query.Symbol]bool{}
	for _, p := range stratum.Predicates {
		inStratum[p] = true
	}
	eval := &stratumEvaluation{executor: e, stratum: stratum, symbols: map[query.Symbol][]query.Symbol{}}
	for _, p := range stratum.Predicates {
		rules := rs.Rules(p)
		if len(rules) == 0 {
			eval.symbols[p] = seeds[p].Symbols()
			continue
		}
		eval.symbols[p] = rules[0].Head
		for _, rule := range rules {
			occurrences := 0
			for _, inv := range query.Invocations(rule.Body) {
				if inStratum[inv.Name] {
					occurrences++
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
				eval.bodies = append(eval.bodies, plannedBody{rule: rule, plan: plan})
			}
			for j := 0; j < occurrences; j++ {
				plan, err := e.planBody(rs, rule, markDelta(rule.Body, inStratum, j))
				if err != nil {
					return nil, err
				}
				eval.bodies = append(eval.bodies, plannedBody{rule: rule, plan: plan, delta: true})
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
	plan, err := e.planner.PlanQuery(q, planner.Bindings{Rules: rs}, e.options.Handler)
	if err != nil {
		return nil, fmt.Errorf("planning rule %s: %w", rule.Name, err)
	}
	return plan, nil
}

// markDelta copies clauses with the target-th same-stratum invocation, in
// clause order at any depth, replaced by a copy invoking the predicate's
// delta symbol. The or and or-join forms on the path are copied; every other
// clause is shared, since a same-stratum invocation stands nowhere else.
func markDelta(clauses []query.Clause, inStratum map[query.Symbol]bool, target int) []query.Clause {
	counter := 0
	return markDeltaWalk(clauses, inStratum, target, &counter)
}

func markDeltaWalk(clauses []query.Clause, inStratum map[query.Symbol]bool, target int, counter *int) []query.Clause {
	out := make([]query.Clause, len(clauses))
	for i, clause := range clauses {
		switch c := clause.(type) {
		case *query.RuleInvocation:
			out[i] = c
			if inStratum[c.Name] {
				if *counter == target {
					out[i] = &query.RuleInvocation{Name: query.DeltaSymbol(c.Name), Args: c.Args}
				}
				*counter++
			}
		case *query.OrClause:
			out[i] = c
			if branches, marked := markDeltaBranches(c.Branches, inStratum, target, counter); marked {
				out[i] = &query.OrClause{Branches: branches}
			}
		case *query.OrJoinClause:
			out[i] = c
			if branches, marked := markDeltaBranches(c.Branches, inStratum, target, counter); marked {
				out[i] = &query.OrJoinClause{JoinVars: c.JoinVars, Branches: branches}
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
func markDeltaBranches(branches [][]query.Clause, inStratum map[query.Symbol]bool, target int, counter *int) ([][]query.Clause, bool) {
	before := *counter
	walked := make([][]query.Clause, len(branches))
	for b, branch := range branches {
		walked[b] = markDeltaWalk(branch, inStratum, target, counter)
	}
	return walked, before <= target && target < *counter
}

// run executes the bodies whose delta flag equals recursive under a scope
// holding held, the relations by predicate symbol, and returns each body's
// result relation by predicate, over the predicate's canonical symbols, for
// the sets to absorb. A body is a query scope of its own, with no bound
// inputs: the invoking query's environment does not reach it.
func (s *stratumEvaluation) run(ctx *Context, held map[query.Symbol]Relation, recursive bool) (map[query.Symbol][]Relation, error) {
	scope := ctx.WithEnvironment(nil).WithDerived(held)
	results := map[query.Symbol][]Relation{}
	for _, body := range s.bodies {
		if body.delta != recursive {
			continue
		}
		rel, err := s.executor.executeRealizedPlan(scope, body.plan, nil)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", body.rule.Name, err)
		}
		results[body.rule.Name] = append(results[body.rule.Name], rel)
	}
	return results, nil
}

// evaluateStratum runs one stratum to its fixpoint by semi-naive iteration
// and records each predicate's relation in derived. Iteration zero absorbs
// each predicate's seed, when seeds carries one, and executes the bodies
// with no same-stratum invocation; each later iteration executes every
// marked copy with the previous iteration's deltas, and the tuples the sets
// did not hold become the next deltas. The stratum ends when every delta is
// empty. The iteration bound is per stratum and the tuple bound per
// evaluation, counted through total; a seed's tuples are given, not
// derived, and are not counted.
func (e *Executor) evaluateStratum(ctx *Context, rs *query.RuleSet, stratum query.Stratum, derived, seeds map[query.Symbol]Relation, total *int) error {
	handler := e.options.Handler
	var start time.Time
	if handler != nil {
		start = time.Now()
		handler(annotations.Event{
			Name:  annotations.RulesStratumBegin,
			Start: start,
			Data: map[string]interface{}{
				"predicates": stratum.Predicates,
				"recursive":  stratum.Recursive,
			},
		})
	}

	eval, err := e.prepareStratum(ctx, rs, stratum, seeds)
	if err != nil {
		return err
	}
	full := map[query.Symbol]*derivedSet{}
	for _, p := range stratum.Predicates {
		full[p] = newDerivedSet(eval.symbols[p])
	}
	// view is the derived map a run reads: every earlier stratum's relation
	// and this stratum's sets as they stand.
	view := func() map[query.Symbol]Relation {
		m := make(map[query.Symbol]Relation, len(derived)+len(full))
		for p, rel := range derived {
			m[p] = rel
		}
		for p, set := range full {
			m[p] = set.relation(e.options)
		}
		return m
	}
	// absorb pulls a run's results into the sets and returns the new tuples
	// per predicate, counting them into the evaluation's total.
	absorb := func(results map[query.Symbol][]Relation) (map[query.Symbol][]Tuple, error) {
		next := map[query.Symbol][]Tuple{}
		for p, rels := range results {
			for _, rel := range rels {
				added, err := full[p].absorb(rel)
				if err != nil {
					return nil, fmt.Errorf("rule %s: %w", p, err)
				}
				if len(added) > 0 {
					next[p] = append(next[p], added...)
					*total += len(added)
				}
			}
		}
		if limit := e.options.MaxDerivedTuples; limit > 0 && *total > limit {
			return nil, &query.DerivedTupleLimitError{Predicates: stratum.Predicates, Limit: limit, Reached: *total}
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
	for _, p := range stratum.Predicates {
		seed, ok := seeds[p]
		if !ok {
			continue
		}
		added, err := full[p].absorb(seed)
		if err != nil {
			return fmt.Errorf("seeding %s: %w", p, err)
		}
		if len(added) > 0 {
			delta[p] = added
		}
	}
	seeding, err := eval.run(ctx, view(), false)
	if err != nil {
		return err
	}
	grown, err := absorb(seeding)
	if err != nil {
		return err
	}
	for p, tuples := range grown {
		delta[p] = append(delta[p], tuples...)
	}
	emitIteration(delta)
	for stratum.Recursive && len(delta) > 0 {
		if err := ctx.cancelled(); err != nil {
			return err
		}
		iteration++
		if limit := e.options.MaxRuleIterations; limit > 0 && iteration > limit {
			return &query.RuleIterationLimitError{Predicates: stratum.Predicates, Limit: limit, Reached: iteration}
		}
		// The run holds the last iteration's delta of each predicate under
		// the predicate's delta symbol, beside the relations as they stand.
		held := view()
		for _, p := range stratum.Predicates {
			held[query.DeltaSymbol(p)] = newMaterializedRelationFromSet(eval.symbols[p], delta[p], e.options, deduplicatedProperties(eval.symbols[p]))
		}
		results, err := eval.run(ctx, held, true)
		if err != nil {
			return err
		}
		delta, err = absorb(results)
		if err != nil {
			return err
		}
		emitIteration(delta)
	}

	tuples := 0
	for _, p := range stratum.Predicates {
		derived[p] = full[p].relation(e.options)
		tuples += len(full[p].tuples)
	}
	if handler != nil {
		handler(annotations.TimedEvent(annotations.RulesStratumComplete, start, map[string]interface{}{
			"predicates": stratum.Predicates,
			"iterations": iteration,
			"tuples":     tuples,
		}))
	}
	return nil
}
