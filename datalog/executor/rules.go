package executor

import (
	"errors"
	"fmt"
	"slices"
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
// otherwise. A rename is answered with the set that keeps the predicate when
// invocationSource answers one, so a join at the invocation probes the set's
// index; every other result is answered with no set.
func (e *DefaultQueryExecutor) executeRuleInvocation(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, *derivedSet, error) {
	source, head, set, err := e.invocationSource(ctx, inv, groups)
	if err != nil {
		return nil, nil, err
	}
	positions, err := query.SymbolIndexTable(source.Symbols(), head)
	if err != nil {
		return nil, nil, fmt.Errorf("rule invocation %s: %w", inv, err)
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
			return nil, nil, fmt.Errorf("rule invocation %s: argument %d is %T", inv, i+1, arg)
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
			return nil, nil, fmt.Errorf("rule invocation %s: %w", inv, err)
		}
		if !held {
			return NewMaterializedRelationWithOptions([]query.Symbol{}, nil, e.options), nil, nil
		}
		return e.executor.unit, nil, nil
	}

	renames := len(keep) == len(inv.Args)
	if mat, ok := source.(*MaterializedRelation); ok && renames && len(fixings) == 0 {
		subst, err := query.SubstitutionOf(head, symbols)
		if err != nil {
			return nil, nil, fmt.Errorf("rule invocation %s: %w", inv, err)
		}
		renamed, err := mat.Rename(subst)
		if err != nil {
			return nil, nil, fmt.Errorf("rule invocation %s: %w", inv, err)
		}
		return renamed, set, nil
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
	return NewStreamingRelationWithProperties(symbols, it, e.options, deduplicatedProperties(symbols)), nil, nil
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

func (it *invocationIterator) RequiresCopy() bool { return it.asIs && it.inner.RequiresCopy() }

func (it *invocationIterator) Close() error { return it.inner.Close() }

func (it *invocationIterator) Error() error { return it.inner.Error() }

// invocationSource is the relation an invocation reads, with its head, the
// symbols the invocation's arguments unify with by position. An invocation
// in a body the fixpoint is running, or one a form hoisted, reads the
// relation the scope holds under the symbol it invokes: the required run
// under scope/required, held over the required symbols by name, so its head
// is the invocation's own arguments; or a predicate's relation, or under the
// predicate's delta symbol the tuples the predicate gained since the copy
// last ran, whose head is the rule set's. Any other invocation, the query's
// own or one in a subquery, is derived here. The set is the one the running evaluation keeps for a
// predicate's relation the fixpoint holds (Context.sets), whose tuples up to
// its count the relation is; it is nil for the required run, a delta, and a
// derived invocation, whose relation leaves the evaluation.
func (e *DefaultQueryExecutor) invocationSource(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, []query.Symbol, *derivedSet, error) {
	rel, held := ctx.Derived(inv.Name)
	if !held {
		derived, head, err := e.deriveInvocation(ctx, inv, groups)
		return derived, head, nil, err
	}
	if inv.Name == query.ScopeRequired {
		head := make([]query.Symbol, len(inv.Args))
		for i, arg := range inv.Args {
			head[i] = arg.(query.Variable).Name
		}
		return rel, head, nil, nil
	}
	rs := ctx.Rules()
	if rs == nil {
		return nil, nil, nil, fmt.Errorf("rule invocation %s: no rule set was given for %%", inv)
	}
	head, ok := rs.Head(inv.Name)
	if !ok {
		return nil, nil, nil, fmt.Errorf("rule invocation %s: the rule set declares no predicate %s", inv, inv.Name)
	}
	return rel, head, ctx.sets[inv.Name], nil
}

// deriveInvocation evaluates an invocation's predicate at its clause: the
// clause's evaluation, continued across the windows of the form execution
// the clause stands in when the context carries that execution's
// continuations, under the clause it stands for (ruleContinuations), and
// otherwise an evaluation of this one run, completed at once. The
// evaluation is created at the clause's first run through
// invocationEvaluation, over what the plan planned for the clause, and
// extended by every run with the seed the run's bindings give at the
// positions the plan has bound. The relation is returned with its head, the
// evaluation's set's for the predicate it derives: the predicate's own, or
// the adorned one the rewrite derives.
func (e *DefaultQueryExecutor) deriveInvocation(ctx *Context, inv *query.RuleInvocation, groups []Relation) (Relation, []query.Symbol, error) {
	rs := ctx.Rules()
	if rs == nil {
		return nil, nil, fmt.Errorf("rule invocation %s: no rule set was given for %%", inv)
	}
	planned := e.plan.Evaluations[inv]
	var eval *ruleEvaluation
	var under *query.RuleInvocation
	if ctx.continued != nil {
		under = ctx.continued.clauseOf(inv)
		eval = ctx.continued.byClause[under]
	}
	if eval == nil {
		eval = e.invocationEvaluation(inv, planned)
		if ctx.continued != nil {
			ctx.continued.byClause[under] = eval
		}
	}
	var seed Relation
	if eval.seed != nil {
		seedHead, _ := eval.rs.Head(eval.seed)
		var err error
		if seed, err = e.seedRelation(ctx, inv, planned.Bound, seedHead, groups); err != nil {
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

// invocationEvaluation is the evaluation an invocation runs, over what the
// plan planned for its clause (planner.InvocationEvaluation): the set it
// evaluates, whether that set is a rewrite, and the planned bodies of each
// stratum the predicate needs. A rewrite is reported through the magic-sets
// event as the record states it, whichever equal set the plan was made
// under.
func (e *DefaultQueryExecutor) invocationEvaluation(inv *query.RuleInvocation, planned *planner.InvocationEvaluation) *ruleEvaluation {
	if e.options.Handler != nil && planned.Rewritten {
		e.options.Handler(magicSetsEvent(inv.Name, planned.Bound))
	}
	return newRuleEvaluation(e.executor, planned)
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
	planned   *planner.InvocationEvaluation // what the plan planned for the clause
	rs        *query.RuleSet
	predicate query.Symbol // the predicate the invocation reads
	seed      query.Symbol // the seed predicate the windows fill, nil for a whole evaluation
	strata    []*stratumState
	derived   map[query.Symbol]Relation
	total     int // the tuples derived, every window's together
	windows   int
}

// stratumState is one stratum of an evaluation across its windows: the
// stratum, its planned bodies, its sets, the marks of its delta copies,
// whether it began, when, its iterations summed over the windows, and
// whether it completed.
type stratumState struct {
	stratum query.Stratum
	eval    *stratumEvaluation
	full    map[query.Symbol]*derivedSet
	// marks holds, per delta copy in the pass order of eval.bodies.Delta,
	// the count of the copy's marked predicate's set at the start of the
	// copy's last run, zero before its first: the copy's delta at a run is
	// the set's tuples from its mark to the set's count, and the marks
	// persist across the windows with the sets
	// (docs/proposals/A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md,
	// §1, L4).
	marks      []int
	begun      bool
	start      time.Time
	iterations int
	completed  bool
}

// newRuleEvaluation is the evaluation planned for an invocation: of its
// predicate under its set, fed through the seed predicate by the windows of
// the scope the invocation stands in; a nil seed is a whole evaluation,
// which a later window leaves as it is.
func newRuleEvaluation(e *Executor, planned *planner.InvocationEvaluation) *ruleEvaluation {
	return &ruleEvaluation{
		executor:  e,
		planned:   planned,
		rs:        planned.Magic.Rules,
		predicate: planned.Magic.Predicate,
		seed:      planned.Magic.Seed,
		derived:   map[query.Symbol]Relation{},
	}
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
		for _, planned := range ev.planned.Strata {
			eval := ev.executor.prepareStratum(ev.rs, planned)
			s := &stratumState{stratum: planned.Stratum, eval: eval, full: map[query.Symbol]*derivedSet{}, marks: make([]int, len(eval.bodies.Delta))}
			for _, p := range planned.Stratum.Predicates {
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
		for _, body := range s.eval.bodies.Lower {
			if len(added[body.Marked]) > 0 {
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

// derivedSet accumulates a predicate's tuples over its canonical symbols,
// each once, each held in the set under its whole-tuple key
// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T4), and keeps, per key its
// joins read, an index over its tuples by ordinal
// (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md). The
// table's members are the set's tuples in admission order, the one slice of
// their headers the set holds (docs/proposals/FIXPOINT_MEMORY.md, S2); a
// reader of the tuples through a count takes the members capped at it, so an
// append cannot reach a member past the count.
type derivedSet struct {
	symbols []query.Symbol
	keys    *TupleSet
	indexes []*ordinalIndex
}

func newDerivedSet(symbols []query.Symbol) *derivedSet {
	positions := allPositions(len(symbols))
	return &derivedSet{symbols: symbols, keys: NewTupleSet(positions, positions, 0)}
}

// index is the set's index at the key positions, asked for at a count of the
// set's tuples: made at the first ask over the tuples then held, and
// extended to the set's count when the count asked for is past the tuples it
// holds, so a probe bounded at that count finds every tuple below it
// (A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md, L1, L5).
func (s *derivedSet) index(positions []int, count int) *ordinalIndex {
	var ix *ordinalIndex
	for _, kept := range s.indexes {
		if slices.Equal(kept.positions, positions) {
			ix = kept
			break
		}
	}
	if ix == nil {
		ix = newOrdinalIndex(slices.Clone(positions))
		s.indexes = append(s.indexes, ix)
	}
	if ix.held < count {
		ix.extend(s.keys.members)
	}
	return ix
}

// absorb adds the tuples of rel the set does not hold, as they are pulled,
// each held over the set's symbols, and answers how many it added: the
// tuples added are the set's members from its count before the call to its
// count after it. rel carries the set's symbols in any layout, found by
// symbol once; a relation lacking one is the set's error. A tuple in the
// set's layout is probed before anything else, so one the set holds is
// rejected without a copy, and is held as it is from a stable source or
// copied once, on admission, from a reusing one. A tuple in another layout
// is read out at the set's positions into a copy before it is probed. The
// held tuple is the set's member.
func (s *derivedSet) absorb(rel Relation) (int, error) {
	positions, err := query.SymbolIndexTable(rel.Symbols(), s.symbols)
	if err != nil {
		return 0, fmt.Errorf("absorbing into %v: %w", s.symbols, err)
	}
	sameLayout := len(rel.Symbols()) == len(s.symbols)
	for i := 0; sameLayout && i < len(positions); i++ {
		sameLayout = positions[i] == i
	}
	copies := rel.RequiresCopy()
	added := 0
	err = ForEach(rel, func(tuple Tuple) error {
		if !sameLayout {
			held := make(Tuple, len(positions))
			for j, position := range positions {
				held[j] = tuple[position]
			}
			tuple = held
		}
		_, hash, held := s.keys.Find(tuple)
		if held {
			return nil
		}
		if sameLayout && copies {
			tuple = copyTuple(tuple)
		}
		s.keys.Add(tuple, hash)
		added++
		return nil
	})
	return added, err
}

// relation is the set as a relation over the tuples added so far, and
// carries nothing of the set. A join at a held invocation of the predicate
// reaches the set's index through the context the body runs under
// (Context.sets), so a relation the evaluation hands out does not change when
// it is joined (A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md, T2).
func (s *derivedSet) relation(opts ExecutorOptions) *MaterializedRelation {
	n := s.keys.Len()
	return newMaterializedRelationFromSet(s.symbols, s.keys.members[:n:n], opts, deduplicatedProperties(s.symbols))
}

// stratumEvaluation is a stratum's planned bodies (planner.Bodies), under
// the rule set they were planned with, and the canonical symbols of each of
// its predicates, the set's head for it.
type stratumEvaluation struct {
	executor *Executor
	rs       *query.RuleSet
	stratum  query.Stratum
	symbols  map[query.Symbol][]query.Symbol
	bodies   planner.Bodies
}

// prepareStratum is a stratum's evaluation over the bodies the plan carries
// for it, the ones the walk at PlanQuery planned, and the canonical symbols
// of its predicates.
func (e *Executor) prepareStratum(rs *query.RuleSet, planned planner.PlannedStratum) *stratumEvaluation {
	eval := &stratumEvaluation{executor: e, rs: rs, stratum: planned.Stratum, symbols: map[query.Symbol][]query.Symbol{}, bodies: planned.Bodies}
	for _, p := range planned.Stratum.Predicates {
		head, _ := rs.Head(p)
		eval.symbols[p] = head
	}
	return eval
}

// run executes bodies under a scope holding held, the relations by predicate
// symbol, under the rule set the bodies were planned with, whose heads the
// held relations are over, and returns each body's result relation by
// predicate, renamed under the predicate's canonical symbols through the
// substitution of the rule's head for them, for the sets to absorb. A body
// is a query scope of its own, with no bound inputs: the invoking query's
// environment does not reach it. Its predicates without required positions
// are held by the fixpoint, so a scope inside it is planned under
// query.HeldByFixpoint, as the walk at PlanQuery planned it.
func (s *stratumEvaluation) run(ctx *Context, held map[query.Symbol]Relation, bodies []planner.PlannedBody) (map[query.Symbol][]Relation, error) {
	scope := ctx.WithEnvironment(nil).WithRules(s.rs).WithDerived(held).WithHeld(query.HeldByFixpoint)
	results := map[query.Symbol][]Relation{}
	for _, body := range bodies {
		rel, err := s.executor.executeRealizedPlan(scope, body.Plan, nil)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", body.Rule.Name, err)
		}
		if canonical := s.symbols[body.Rule.Name]; !symbolsMatch(body.Rule.Head, canonical) {
			subst, err := query.SubstitutionOf(body.Rule.Head, canonical)
			if err != nil {
				return nil, fmt.Errorf("rule %s: %w", body.Rule.Name, err)
			}
			if rel, err = rel.Rename(subst); err != nil {
				return nil, fmt.Errorf("rule %s: %w", body.Rule.Name, err)
			}
		}
		results[body.Rule.Name] = append(results[body.Rule.Name], rel)
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
// such copy runs nothing. Then the stratum runs passes until one runs no
// copy: a pass visits each delta copy once, in the pass order, and runs it
// when its marked predicate's set holds tuples past the copy's mark, over
// those tuples under the predicate's delta symbol and the sets as they
// stand, absorbing its result before the next copy is visited
// (docs/proposals/A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md). The
// iteration event reports what each pass added, the iteration bound counts
// the passes from the window's first, and the tuple bound counts over the
// evaluation's total; a seed's tuples are given, not derived, and are not
// counted.
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
	// start is each predicate's count at the window's start: the tuples the
	// window adds to a predicate are its members from there to its count at
	// the end, which the strata above read as their lower deltas.
	start := make(map[query.Symbol]int, len(stratum.Predicates))
	for _, p := range stratum.Predicates {
		start[p] = s.full[p].keys.Len()
	}
	// absorb pulls a run's results into the sets and answers how many tuples
	// each predicate gained, counting them into the evaluation's total.
	absorb := func(results map[query.Symbol][]Relation) (map[query.Symbol]int, error) {
		gained := map[query.Symbol]int{}
		for p, rels := range results {
			for _, rel := range rels {
				n, err := s.full[p].absorb(rel)
				if err != nil {
					return nil, fmt.Errorf("rule %s: %w", p, err)
				}
				if n > 0 {
					gained[p] += n
					ev.total += n
				}
			}
		}
		if limit := e.options.MaxDerivedTuples; limit > 0 && ev.total > limit {
			return nil, &query.DerivedTupleLimitError{Predicates: stratum.Predicates, Limit: limit, Reached: ev.total}
		}
		return gained, nil
	}
	iteration := 0
	emitIteration := func(delta map[query.Symbol]int) {
		if handler == nil {
			return
		}
		handler(annotations.Event{
			Name:  annotations.RulesIteration,
			Start: time.Now(),
			Data: map[string]interface{}{
				"predicates": stratum.Predicates,
				"iteration":  iteration,
				"delta":      delta,
			},
		})
	}

	delta := map[query.Symbol]int{}
	if seed != nil {
		n, err := s.full[ev.seed].absorb(seed)
		if err != nil {
			return nil, fmt.Errorf("seeding %s: %w", ev.seed, err)
		}
		if n > 0 {
			delta[ev.seed] = n
		}
	}
	// The bodies run under a context carrying every set the evaluation keeps,
	// by predicate, so a held invocation of a predicate joins through its
	// set's index (Context.sets).
	sets := map[query.Symbol]*derivedSet{}
	for _, st := range ev.strata {
		for p, set := range st.full {
			sets[p] = set
		}
	}
	scope := ctx.WithSets(sets)
	held := view()
	bodies := s.eval.bodies.Plain
	if ev.windows > 1 {
		bodies = nil
		for _, body := range s.eval.bodies.Lower {
			if len(lower[body.Marked]) > 0 {
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
	seeding, err := s.eval.run(scope, held, bodies)
	if err != nil {
		return nil, err
	}
	grown, err := absorb(seeding)
	if err != nil {
		return nil, err
	}
	for p, n := range grown {
		delta[p] += n
		held[p] = s.full[p].relation(e.options)
	}
	emitIteration(delta)
	for stratum.Recursive {
		if err := ctx.cancelled(); err != nil {
			return nil, err
		}
		// A pass. A copy runs when its marked predicate's set holds tuples
		// past its mark, over those tuples under the delta symbol, with the
		// sets as they stand after the copies before it; its result is
		// absorbed, and the view of each predicate it added to is taken
		// again, before the next copy is visited. The pass that runs no copy
		// ends the stratum: every mark is then at its set's count, so the
		// sets are closed under the bodies
		// (A_COPY_READS_THE_TUPLES_ADDED_SINCE_IT_RAN_PROOF.md, L2, L5, L6).
		ran := false
		passAdded := map[query.Symbol]int{}
		for i, body := range s.eval.bodies.Delta {
			set := s.full[body.Marked]
			count := set.keys.Len()
			if s.marks[i] >= count {
				continue
			}
			if !ran {
				iteration++
				if limit := e.options.MaxRuleIterations; limit > 0 && iteration > limit {
					return nil, &query.RuleIterationLimitError{Predicates: stratum.Predicates, Limit: limit, Reached: iteration}
				}
				ran = true
			}
			symbols := s.eval.symbols[body.Marked]
			held[query.DeltaSymbol(body.Marked)] = newMaterializedRelationFromSet(symbols, set.keys.members[s.marks[i]:count:count], e.options, deduplicatedProperties(symbols))
			s.marks[i] = count
			results, err := s.eval.run(scope, held, []planner.PlannedBody{body})
			if err != nil {
				return nil, err
			}
			grown, err := absorb(results)
			if err != nil {
				return nil, err
			}
			for p, n := range grown {
				passAdded[p] += n
				held[p] = s.full[p].relation(e.options)
			}
		}
		if !ran {
			break
		}
		emitIteration(passAdded)
	}
	s.iterations += iteration
	added := map[query.Symbol][]Tuple{}
	for _, p := range stratum.Predicates {
		set := s.full[p]
		ev.derived[p] = set.relation(e.options)
		if n := set.keys.Len(); n > start[p] {
			added[p] = set.keys.members[start[p]:n:n]
		}
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
			tuples += s.full[p].keys.Len()
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
// at its close. An invocation has one clause in its scope's plan and, where
// a window reads it through a frame, a second in the frame's plan
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1). The frame's clause
// stands for the scope's, and an evaluation at either continues under the
// scope's.
type ruleContinuations struct {
	byClause  map[*query.RuleInvocation]*ruleEvaluation
	standsFor map[*query.RuleInvocation]*query.RuleInvocation
}

// newRuleContinuations is a form execution's continuations, holding none.
func newRuleContinuations() *ruleContinuations {
	return &ruleContinuations{
		byClause:  map[*query.RuleInvocation]*ruleEvaluation{},
		standsFor: map[*query.RuleInvocation]*query.RuleInvocation{},
	}
}

// standFor records that framed, the invocation clause of the frame planned
// for inv, stands for inv.
func (c *ruleContinuations) standFor(framed, inv *query.RuleInvocation) {
	c.standsFor[framed] = inv
}

// clauseOf is the clause an evaluation at inv continues under: the clause
// inv stands for when inv is a frame's invocation clause, and inv itself
// otherwise.
func (c *ruleContinuations) clauseOf(inv *query.RuleInvocation) *query.RuleInvocation {
	if stood, ok := c.standsFor[inv]; ok {
		return stood
	}
	return inv
}

// complete completes every evaluation held.
func (c *ruleContinuations) complete() {
	for _, ev := range c.byClause {
		ev.complete()
	}
}
