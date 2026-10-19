package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// OrFallbackRelation's mechanisms, each pinned at the unit level: how a
// branch's run size is read from its clauses, how a run is framed, built and
// probed, and how the fallback and the union iterate.

func branchPattern(entity, value query.Symbol, attr datalog.Keyword) query.Clause {
	return &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
	}}
}

func branchGround(value query.Symbol, v interface{}) query.Clause {
	return &query.Expression{Function: &query.GroundFunction{Value: v}, Binding: value}
}

// branchSubquery is an uncorrelated subquery binding value from every entity
// carrying attr.
func branchSubquery(value query.Symbol, attr datalog.Keyword) *query.SubqueryPattern {
	inner := datalog.NewSymbol("?inner")
	return &query.SubqueryPattern{
		Query: &query.Query{
			Find:  []query.FindElement{query.FindVariable{Symbol: value}},
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: []query.Clause{branchPattern(inner, value, attr)},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{value}},
	}
}

// holdsInvocation finds an invocation at the top of a branch, inside a
// nested form and inside a subquery's inner query, and nothing else.
func TestHoldsInvocationDescendsIntoSubqueryBodies(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	invocation := &query.RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []query.PatternElement{
		query.Variable{Name: entity}, query.Variable{Name: value},
	}}
	require.True(t, holdsInvocation([]query.Clause{invocation}))
	require.True(t, holdsInvocation([]query.Clause{&query.NotJoinClause{RequiredVars: []query.Symbol{entity}, Clauses: []query.Clause{invocation}}}))
	require.True(t, holdsInvocation([]query.Clause{&query.SubqueryPattern{
		Query:   &query.Query{Where: []query.Clause{invocation}},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{value}},
	}}))
	require.False(t, holdsInvocation([]query.Clause{branchPattern(entity, value, datalog.NewKeyword(":t/a"))}))
	require.False(t, holdsInvocation(nil))
}

// A narrowable branch is data patterns alone.
func TestNarrowableBranchIsDataPatternsAlone(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	pattern := branchPattern(entity, value, datalog.NewKeyword(":t/a"))
	ground := branchGround(value, int64(0))
	require.True(t, isDataPatternOnlyBranch([]query.Clause{pattern, pattern}))
	require.False(t, isDataPatternOnlyBranch([]query.Clause{pattern, ground}))
	require.False(t, isDataPatternOnlyBranch(nil))
}

// Whether every branch answers at most one tuple per subject tuple is
// judged against the header symbols the subject carries: an expression
// binding one value and a subquery binding one tuple do, a relation binding
// does when every find variable's binding is carried, and a data pattern
// does when the matcher serves it by one lookup, which a matcher without
// the capability never does.
func TestBranchesEmitAtMostOneJudgedAgainstTheCarriedHeader(t *testing.T) {
	entity, value, task := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?task")
	ground := branchGround(value, int64(0))
	tupleBound := &query.SubqueryPattern{Query: &query.Query{}, Binding: query.TupleBinding{Variables: []query.Symbol{value}}}
	grouped := &query.SubqueryPattern{
		Query: &query.Query{Find: []query.FindElement{
			query.FindVariable{Symbol: entity},
			query.FindAggregate{Function: datalog.SymCount, Arg: task},
		}},
		Binding: query.RelationBinding{Variables: []query.Symbol{entity, value}},
	}
	required := []query.Symbol{entity}
	carried := []query.Symbol{entity}
	matcher := NewMemoryPatternMatcher(nil)
	require.True(t, orBranchesEmitAtMostOne([][]query.Clause{{ground}, {tupleBound}}, required, carried, matcher))
	require.True(t, orBranchesEmitAtMostOne([][]query.Clause{{grouped}, {ground}}, required, carried, matcher))
	require.False(t, orBranchesEmitAtMostOne([][]query.Clause{{grouped}, {ground}}, required, nil, matcher), "a group key the subject does not carry")
	require.False(t, orBranchesEmitAtMostOne([][]query.Clause{{branchPattern(entity, value, datalog.NewKeyword(":t/a"))}}, required, carried, matcher), "a matcher without the capability")
	require.False(t, orBranchesEmitAtMostOne([][]query.Clause{{}}, required, carried, matcher))
	require.False(t, orBranchesEmitAtMostOne(nil, required, carried, matcher))

	carriedSet := map[query.Symbol]bool{entity: true}
	require.True(t, relationBindingGroupsAreOuterBound(grouped.Query, []query.Symbol{entity, value}, carriedSet))
	require.False(t, relationBindingGroupsAreOuterBound(grouped.Query, []query.Symbol{value, entity}, carriedSet), "the binding's first variable is not carried")
	require.False(t, relationBindingGroupsAreOuterBound(grouped.Query, []query.Symbol{entity}, carriedSet), "an arity disagreement")
}

// branchProvides reads what each clause provides under the rule set: a
// pattern's variables, an expression's binding, a subquery's binding and a
// nested form's outputs, each once.
func TestBranchProvidesReadsEachClausesProvides(t *testing.T) {
	entity, value, other := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?other")
	attr := datalog.NewKeyword(":t/a")
	provided := branchProvides([]query.Clause{
		branchPattern(entity, value, attr),
		branchGround(other, int64(1)),
		branchSubquery(value, attr),
		&query.OrJoinClause{RequiredVars: []query.Symbol{entity}, OutputVars: []query.Symbol{other}, Branches: [][]query.Clause{{branchPattern(entity, other, attr)}}},
	}, nil)
	require.Equal(t, []query.Symbol{entity, value, other}, provided)
}

// symbolsMatch is order-sensitive.
func TestSymbolsMatchIsOrderSensitive(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	require.True(t, symbolsMatch([]query.Symbol{a, b}, []query.Symbol{a, b}))
	require.False(t, symbolsMatch([]query.Symbol{a, b}, []query.Symbol{b, a}))
	require.False(t, symbolsMatch([]query.Symbol{a}, []query.Symbol{a, b}))
	require.True(t, symbolsMatch(nil, nil))
}

// agreeing keeps the matches agreeing with the subject tuple on the carried
// outputs, read from the subject tuple's position or the environment's
// value, and returns the matches themselves when every one agrees.
func TestAgreeingKeepsTheMatchesAgreeingOnTheCarriedOutputs(t *testing.T) {
	entity, value, flag := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?flag")
	e1 := datalog.NewIdentity("agree1")
	it := &matchEmit{carried: []carriedOutput{
		{symbol: value, outerPos: 1},
		{symbol: flag, outerPos: -1, envValue: "env"},
	}}
	branchSyms := []query.Symbol{entity, value, flag}
	outerTuple := Tuple{e1, int64(5)}

	agreeing := []Tuple{{e1, int64(5), "env"}, {e1, int64(5), "env"}}
	kept := it.agreeing(agreeing, branchSyms, outerTuple)
	require.Len(t, kept, 2)
	require.True(t, &kept[0] == &agreeing[0], "matches that all agree are returned as they are")

	mixed := []Tuple{{e1, int64(5), "env"}, {e1, int64(6), "env"}, {e1, int64(5), "other"}, {e1, int64(5), "env"}}
	kept = it.agreeing(mixed, branchSyms, outerTuple)
	require.Equal(t, []Tuple{{e1, int64(5), "env"}, {e1, int64(5), "env"}}, kept)

	require.True(t, it.agrees(Tuple{e1}, []query.Symbol{entity}, outerTuple), "a match carrying no carried output agrees")
	none := &matchEmit{}
	require.True(t, &none.agreeing(mixed, branchSyms, outerTuple)[0] == &mixed[0], "with no carried output every match agrees")
}

// planBranches frames each branch under the header and plans it: a branch
// holding an invocation is framed over the required run alone, whatever it
// reads, its find the outputs; a branch reading nothing of what it is given
// is framed over the required symbols the environment alone binds, its find
// the outputs, so the uncorrelated subquery and the lone ground alike are
// framed over no required run when the subject binds the run, and over the
// environment's required symbols when the environment does; every other
// branch is framed over the run symbols.
func TestPlanBranchesFramesEachBranchUnderTheHeader(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":t/a")
	outer := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{datalog.NewIdentity("plan1")}})
	r := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{branchPattern(entity, value, attr)}, {branchSubquery(value, attr)}, {branchGround(value, int64(0))}},
		outer,
		ExecutorOptions{},
		true,
	)
	plans, err := r.planBranches()
	require.NoError(t, err)
	require.Len(t, plans, 3)
	require.False(t, plans[0].readsNothing, "a pattern on the required entity reads it")
	require.Equal(t, []query.Symbol{entity}, plans[0].frame.required)
	require.Equal(t, []query.Symbol{value}, plans[0].frame.outputs())
	require.True(t, plans[1].readsNothing, "an uncorrelated subquery reads nothing")
	require.Empty(t, plans[1].frame.required)
	require.Equal(t, []query.Symbol{value}, plans[1].frame.outputs())
	require.True(t, plans[2].readsNothing, "a lone ground reads nothing")
	require.Empty(t, plans[2].frame.required)
	require.Equal(t, []query.Symbol{value}, plans[2].frame.outputs())
	for i, plan := range plans {
		require.NotNil(t, plan.plan, "branch %d is planned", i+1)
	}

	// A branch holding an invocation, under a union whose subject carries the
	// required entity and the output: framed over the required run alone, the
	// carried output joined through the branch cache's key after.
	reach := datalog.NewSymbol("reach")
	rules, err := query.NewRuleSet([]*query.Rule{{
		Name: reach,
		Head: []query.Symbol{entity, value},
		Body: []query.Clause{branchPattern(entity, value, attr)},
	}})
	require.NoError(t, err)
	invocation := &query.RuleInvocation{Name: reach, Args: []query.PatternElement{
		query.Variable{Name: entity}, query.Variable{Name: value},
	}}
	union := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext().WithRules(rules),
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{invocation}},
		NewMaterializedRelation([]query.Symbol{entity, value}, []Tuple{{datalog.NewIdentity("plan2"), int64(1)}}),
		ExecutorOptions{},
		false,
	)
	plans, err = union.planBranches()
	require.NoError(t, err)
	require.False(t, plans[0].readsNothing)
	require.Equal(t, []query.Symbol{entity}, plans[0].frame.required, "an invocation branch is framed over the required run alone")
	require.Equal(t, []query.Symbol{value}, plans[0].frame.outputs())

	// A branch reading nothing, under a fallback whose required variable the
	// environment alone binds: framed over that variable, so the run carries
	// the input's value.
	z := datalog.NewSymbol("?z")
	given := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext().WithEnvironment(NewMaterializedRelation([]query.Symbol{z}, []Tuple{{int64(1)}})),
		[]query.Symbol{z},
		[]query.Symbol{value},
		[][]query.Clause{{branchGround(value, int64(0))}},
		NewUnitRelation(ExecutorOptions{}),
		ExecutorOptions{},
		true,
	)
	plans, err = given.planBranches()
	require.NoError(t, err)
	require.True(t, plans[0].readsNothing)
	require.Equal(t, []query.Symbol{z}, plans[0].frame.required, "a branch reading nothing is framed over the required symbols the environment alone binds")
	require.Equal(t, []query.Symbol{value}, plans[0].frame.outputs())
}

// frameBranches frames a not-join's body as the fallback frames a branch,
// under the all-required header: a body reading the header's entity is
// framed over it with no output, and a body providing a header symbol it
// never reads is framed over the required symbols the environment alone
// binds, none here, with that symbol as its output, so its one run over the
// unit relation carries the symbol for the index to key on.
func TestFrameBranchesFramesANotJoinBodyOverItsHeader(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":t/a")
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	header := []query.Symbol{entity}
	subject := NewMaterializedRelation([]query.Symbol{entity, value}, nil)
	runSyms, _ := runSymbols(header, subject.Symbols())
	plans, err := frameBranches(exec, NewContext(), [][]query.Clause{{branchPattern(entity, value, attr)}, {branchGround(entity, int64(1))}}, framing{
		required: header, given: header, runSyms: runSyms, subject: subject,
	})
	require.NoError(t, err)
	require.Len(t, plans, 2)
	require.False(t, plans[0].readsNothing, "a body reading the header's entity reads it")
	require.Equal(t, []query.Symbol{entity}, plans[0].frame.required, "and is framed over it")
	require.Empty(t, plans[0].frame.outputs(), "an all-required header has no output")
	require.True(t, plans[1].readsNothing, "a ground providing the entity reads nothing of it")
	require.Empty(t, plans[1].frame.required, "no required symbol is the environment's alone")
	require.Equal(t, []query.Symbol{entity}, plans[1].frame.outputs(), "the symbol the body provides is its find, so the index keys on it")
	for i, plan := range plans {
		require.NotNil(t, plan.plan, "body %d is planned", i+1)
	}
}

// frameBranches frames a subquery's inner query over its inputs with its
// find shape: the frame's find is the inner :find as written, a variable and
// an aggregate by its display form, after the given symbols the body
// provides beyond its frame, and the frame plans.
func TestFrameBranchesFramesASubqueryOverItsInputsWithItsFindShape(t *testing.T) {
	x, v, s := datalog.NewSymbol("?x"), datalog.NewSymbol("?v"), datalog.NewSymbol("?s")
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	count := query.FindAggregate{Function: datalog.SymCount, Arg: s}
	body := []query.Clause{branchPattern(x, v, datalog.NewKeyword(":t/v")), branchPattern(x, s, datalog.NewKeyword(":t/s"))}
	plans, err := frameBranches(exec, NewContext(), [][]query.Clause{body}, framing{
		required: []query.Symbol{x}, find: []query.FindElement{query.FindVariable{Symbol: v}, count}, given: []query.Symbol{x}, runSyms: []query.Symbol{x}, subject: NewMaterializedRelation([]query.Symbol{x}, nil),
	})
	require.NoError(t, err)
	require.Len(t, plans, 1)
	require.False(t, plans[0].readsNothing, "the body reads its input")
	require.Equal(t, []query.Symbol{x}, plans[0].frame.required)
	require.Equal(t, []query.FindElement{query.FindVariable{Symbol: v}, count}, plans[0].frame.find, "the inner :find as written")
	require.Equal(t, []query.Symbol{v, datalog.NewSymbol("(count ?s)")}, plans[0].frame.outputs(), "the variable's symbol and the aggregate's display form")
	require.NotNil(t, plans[0].plan, "the frame plans")
}

// runSymbols orders what a form's branches are given against the subject:
// the given symbols the subject carries lead, in the subject's order, and
// are the probe key, and the symbols the subject lacks follow.
func TestRunSymbolsLeadWithTheSubjectsSymbolsInItsOrder(t *testing.T) {
	a, b, x, z := datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?x"), datalog.NewSymbol("?z")
	runSyms, joinSyms := runSymbols([]query.Symbol{b, a, z}, []query.Symbol{x, a, b})
	require.Equal(t, []query.Symbol{a, b, z}, runSyms, "the given symbols the subject carries lead in the subject's order, and the rest follow")
	require.Equal(t, []query.Symbol{a, b}, joinSyms, "the probe key is the leading symbols")
}

// environmentRelation is the environment's tuple over the run symbols the
// environment alone binds: a run symbol the subject carries and an
// environment symbol the run lacks are left out, and with nothing left or no
// environment there is no relation.
func TestEnvironmentRelationIsTheEnvironmentsTupleOverTheRunSymbolsItAloneBinds(t *testing.T) {
	a, z, q := datalog.NewSymbol("?a"), datalog.NewSymbol("?z"), datalog.NewSymbol("?q")
	env := NewMaterializedRelation([]query.Symbol{a, z, q}, []Tuple{{int64(1), int64(2), int64(3)}})
	rel, err := environmentRelation(NewContext().WithEnvironment(env), []query.Symbol{a, z}, []query.Symbol{a})
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{z}, rel.Symbols(), "?a the subject carries and ?q the run lacks are left out")
	require.Equal(t, Tuple{int64(2)}, rel.Get(0))

	none, err := environmentRelation(NewContext().WithEnvironment(env), []query.Symbol{a}, []query.Symbol{a})
	require.NoError(t, err)
	require.Nil(t, none, "the subject carries every run symbol")

	bare, err := environmentRelation(NewContext(), []query.Symbol{a, z}, []query.Symbol{a})
	require.NoError(t, err)
	require.Nil(t, bare, "no environment")
}

// continuationsFor gives an iterator under a context carrying no
// continuations new ones it owns, in the context it runs under, and an
// iterator under a context carrying them shares them and owns nothing.
func TestContinuationsForSharesTheContextsAndOwnsNewOnes(t *testing.T) {
	ctx, continued, owns := continuationsFor(NewContext())
	require.True(t, owns, "a context carrying no continuations gets new ones the iterator owns")
	require.NotNil(t, continued)
	require.Same(t, continued, ctx.continued, "the context the iterator runs under carries them")

	nested, shared, ownsNested := continuationsFor(ctx)
	require.False(t, ownsNested, "a nested form finds the context's continuations and shares them")
	require.Same(t, continued, shared)
	require.Same(t, ctx, nested)
}

// newWindowedIterator composes the modules over the subject: the reader over
// the subject's iterator for the form, keyed on the required symbols the
// subject carries; one frame per distinct required list among the plans,
// resolved against the frames' environment relation its caller gives, the
// environment's tuple over the run symbols it alone binds for a branch
// form; and the continuations the iterator owns, in the context its
// branches run under.
func TestNewWindowedIteratorComposesTheModulesOverTheSubject(t *testing.T) {
	entity, value, z := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?z")
	attr := datalog.NewKeyword(":t/a")
	exec := newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{})
	ctx := NewContext().WithEnvironment(NewMaterializedRelation([]query.Symbol{z}, []Tuple{{int64(7)}}))
	subject := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{datalog.NewIdentity("compose1")}})
	required := []query.Symbol{entity, z}
	runSyms, joinSyms := runSymbols(required, subject.Symbols())
	envRel, err := environmentRelation(ctx, runSyms, subject.Symbols())
	require.NoError(t, err)
	f := framing{
		form: "or-default-join", subject: subject, required: required, find: []query.FindElement{query.FindVariable{Symbol: value}},
		given: required, runSyms: runSyms, key: joinSyms, environment: envRel,
	}
	plans, err := frameBranches(exec, ctx, [][]query.Clause{{branchPattern(entity, value, attr)}, {branchGround(value, int64(0))}}, f)
	require.NoError(t, err)
	it := newWindowedIterator(exec, ctx, f, plans, ExecutorOptions{})
	require.NoError(t, it.err)
	require.NotNil(t, it.reader, "the reader is opened over the subject")
	require.Equal(t, "or-default-join", it.reader.form)
	require.Equal(t, []int{0}, it.reader.keyPos, "the window's distinct keys are counted over the required symbols the subject carries")
	require.Len(t, it.frames, 2, "one frame per distinct required list: the pattern's over the entity and ?z, the ground's over ?z alone")
	require.Equal(t, []int{0, 1}, it.frameOf)
	require.True(t, it.ownsContinued, "an iterator under a context carrying no continuations owns new ones")
	require.Same(t, it.continued, it.ctx.continued, "the context the branches run under carries them")
	require.Equal(t, f, it.framing, "the iterator reads the framing it was composed over")
}

// buildBranchTuples runs a branch's plan over the run it is given, and over
// the unit relation when given none, and reads the result whole with its
// symbols, the frame's header; a failure of the branch's scan is the
// build's error.
func TestBuildBranchTuplesRunsThePlanOverTheRunAndReadsItWhole(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":t/a")
	e1 := datalog.NewIdentity("build1")
	datoms := []datalog.Datom{{E: e1, A: attr, V: int64(5), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}}
	plansOf := func(matcher PatternMatcher, required []query.Symbol, branch query.Clause, subject Relation) (*OrFallbackRelation, []branchRun) {
		r := NewOrFallbackRelation(newQueryExecutor(matcher, nil, ExecutorOptions{}), NewContext(), required, []query.Symbol{value}, [][]query.Clause{{branch}}, subject, ExecutorOptions{}, true)
		plans, err := r.planBranches()
		require.NoError(t, err)
		return r, plans
	}

	r, plans := plansOf(NewMemoryPatternMatcher(nil), nil, branchGround(value, int64(3)), NewUnitRelation(ExecutorOptions{}))
	tuples, syms, err := buildBranchTuples(r.executor, r.ctx, r.framing(nil), plans[0], nil)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{value}, syms, "the result's symbols are the frame's header")
	require.Equal(t, []Tuple{{int64(3)}}, tuples, "a nil run is the unit relation, over which the ground yields its one tuple")

	subject := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{e1}})
	run := newMaterializedRelationFromSet([]query.Symbol{entity}, []Tuple{{e1}}, ExecutorOptions{}, deduplicatedProperties([]query.Symbol{entity}))
	r, plans = plansOf(NewMemoryPatternMatcher(datoms), []query.Symbol{entity}, branchPattern(entity, value, attr), subject)
	tuples, syms, err = buildBranchTuples(r.executor, r.ctx, r.framing(nil), plans[0], run)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{entity, value}, syms)
	require.Equal(t, []Tuple{{e1, int64(5)}}, tuples, "the pattern over the run's entity yields its value")

	failing := datalog.NewKeyword(":t/failing")
	matcher := &failingScanMatcher{failAttr: failing, dataRel: NewMaterializedRelation([]query.Symbol{entity, value}, nil)}
	r, plans = plansOf(matcher, []query.Symbol{entity}, branchPattern(entity, value, failing), subject)
	_, _, err = buildBranchTuples(r.executor, r.ctx, r.framing(nil), plans[0], run)
	require.ErrorIs(t, err, errInjectedIterator, "the branch's scan failure is the build's error")
}

// buildBranchTuples presents a branch's answer under the enclosing names
// when its frame carries a substitution: each answer symbol the
// substitution names is renamed to the symbol standing for it, two answer
// symbols standing for one name keep the first, and an answer symbol the
// substitution does not name, a constant argument's, is dropped. The inner
// query reads ?x and finds ?v, called with ?e at two places and a constant
// at the third, bound to ?c.
func TestBuildBranchTuplesPresentsTheAnswerUnderTheFramesSubstitution(t *testing.T) {
	x, y, k, v := datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?k"), datalog.NewSymbol("?v")
	e, c := datalog.NewSymbol("?e"), datalog.NewSymbol("?c")
	attr := datalog.NewKeyword(":t/a")
	e1 := datalog.NewIdentity("present1")
	datoms := []datalog.Datom{{E: e1, A: attr, V: int64(5), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}}
	exec := newQueryExecutor(NewMemoryPatternMatcher(datoms), nil, ExecutorOptions{})
	subst, err := query.SubstitutionOf([]query.Symbol{x, y, v}, []query.Symbol{e, e, c})
	require.NoError(t, err)
	f := framing{
		required: []query.Symbol{x, y, k}, find: []query.FindElement{query.FindVariable{Symbol: v}}, given: []query.Symbol{x, y, k}, runSyms: []query.Symbol{x, y, k},
		subject: NewMaterializedRelation([]query.Symbol{e}, nil), subst: subst,
	}
	plans, err := frameBranches(exec, NewContext(), [][]query.Clause{{branchPattern(x, v, attr)}}, f)
	require.NoError(t, err)
	run := newMaterializedRelationFromSet([]query.Symbol{x, y, k}, []Tuple{{e1, e1, int64(9)}}, ExecutorOptions{}, deduplicatedProperties([]query.Symbol{x, y, k}))
	tuples, syms, err := buildBranchTuples(exec, NewContext(), f, plans[0], run)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{e, c}, syms, "?x renamed to ?e, ?y standing for ?e too dropped, the constant's ?k dropped, ?v renamed to ?c")
	require.Equal(t, []Tuple{{e1, int64(5)}}, tuples)
}

// buildFromCache is the fallback's build policy: a branch holding a clause
// other than a data pattern answers with no symbols and reports nothing; a
// branch of data patterns alone reports the narrowing decision with the
// run's key count and, on a cold cache, answers with no symbols, so the
// composition builds it from the plan; and on a warm cache it answers with
// the cache's tuples over the entity and value symbols.
func TestBuildFromCacheIsTheFallbacksBuildOfAPatternBranch(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":item/priority")
	e1 := datalog.NewIdentity("fromcache:e1")
	matcher := &bundleLookupMatcher{values: map[bundleLookupKey]interface{}{{entity: e1, attr: attr}: int64(4)}}
	var events []annotations.Event
	opts := ExecutorOptions{Handler: func(e annotations.Event) { events = append(events, e) }}
	it := &OrFallbackIterator{
		windowedIterator: windowedIterator{executor: newQueryExecutor(matcher, nil, opts), framing: framing{key: []query.Symbol{entity}}, options: opts},
		branches:         [][]query.Clause{{branchGround(value, int64(0))}, {branchPattern(entity, value, attr)}},
	}
	run := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{e1}})

	tuples, syms, err := it.buildFromCache(0, run)
	require.NoError(t, err)
	require.Nil(t, syms, "a ground branch is not the fallback's to build")
	require.Nil(t, tuples)
	require.Empty(t, events, "and reports no narrowing")

	tuples, syms, err = it.buildFromCache(1, run)
	require.NoError(t, err)
	require.Nil(t, syms, "on a cold cache the pattern branch is built from the plan")
	require.Nil(t, tuples)
	require.Len(t, events, 1)
	require.Equal(t, annotations.OrFallbackBranchNarrowed, events[0].Name)
	require.Equal(t, 1, events[0].Data["branch"])
	require.Equal(t, true, events[0].Data["narrowed"])
	require.Equal(t, 1, events[0].Data["join_keys"], "the run's one key")

	it.prefetched = true
	tuples, syms, err = it.buildFromCache(1, run)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{entity, value}, syms, "on a warm cache the branch is built from the cache")
	require.Equal(t, []Tuple{{e1, int64(4)}}, tuples)
}

// The module events carry their payloads under the form: the build names
// the branch, its symbols, the subject's and the size; the subject tuple
// event renders the tuple; the exhausted event carries the form alone; and
// with no handler nothing is reported.
func TestModuleEventsCarryTheirPayloadsUnderTheForm(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	var events []annotations.Event
	options := ExecutorOptions{Handler: func(e annotations.Event) { events = append(events, e) }}
	it := &OrFallbackIterator{windowedIterator: windowedIterator{framing: framing{form: "or-default-join", subject: NewMaterializedRelation([]query.Symbol{entity}, nil)}, options: options}}
	w := &windowReader{form: "or-default-join", options: options}
	it.reportCacheBuild(2, []query.Symbol{entity, value}, 7)
	w.reportTuple(Tuple{int64(1)})
	w.reportExhausted()
	require.Len(t, events, 3)
	require.Equal(t, annotations.ScopeBranchBuild, events[0].Name)
	require.Equal(t, "or-default-join", events[0].Data[annotations.KeyForm])
	require.Equal(t, 2, events[0].Data["branch"])
	require.Equal(t, []query.Symbol{entity, value}, events[0].Data["branch_syms"])
	require.Equal(t, []query.Symbol{entity}, events[0].Data["outer_syms"])
	require.Equal(t, 7, events[0].Data["branch_size"])
	require.Equal(t, annotations.ScopeSubjectTuple, events[1].Name)
	require.Equal(t, "or-default-join", events[1].Data[annotations.KeyForm])
	require.Equal(t, "[1]", events[1].Data["tuple"])
	require.Equal(t, annotations.ScopeSubjectExhausted, events[2].Name)
	require.Equal(t, map[string]interface{}{annotations.KeyForm: "or-default-join"}, events[2].Data)

	silent := &OrFallbackIterator{windowedIterator: windowedIterator{framing: framing{subject: NewMaterializedRelation([]query.Symbol{entity}, nil)}}}
	silent.reportCacheBuild(0, nil, 0)
	quiet := &windowReader{}
	quiet.reportTuple(Tuple{})
	quiet.reportExhausted()
}

// emitCachedMatches sets the first match as the current tuple, projected
// onto the output symbols with the gaps filled from the subject tuple, and
// records the rest for the drain, which next reads out projected the same
// way and ends with the drain cleared; with no match it emits nothing.
func TestEmitCachedMatchesSetsTheFirstTupleAndTheDrainState(t *testing.T) {
	entity, name, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?name"), datalog.NewSymbol("?value")
	e1 := datalog.NewIdentity("emit1")
	it := &OrFallbackIterator{
		matchEmit: matchEmit{outerSyms: []query.Symbol{entity, name}, outputSyms: []query.Symbol{entity, name, value}},
	}
	cb := &cachedBranch{branchSyms: []query.Symbol{entity, value}}
	outerTuple := Tuple{e1, "n"}
	matches := []Tuple{{e1, int64(1)}, {e1, int64(2)}}
	require.True(t, it.emitCachedMatches(0, cb, matches, outerTuple))
	require.Equal(t, Tuple{e1, "n", int64(1)}, it.currentTuple)
	require.Equal(t, matches, it.matches)
	require.Equal(t, 1, it.matchPos)
	require.Equal(t, outerTuple, it.outer)
	require.False(t, it.plan.identity)

	second, ok := it.next()
	require.True(t, ok)
	require.Equal(t, Tuple{e1, "n", int64(2)}, second, "the drain's next match, projected with the gap filled from the subject tuple")
	_, ok = it.next()
	require.False(t, ok, "the drain ends after its last match")
	require.Nil(t, it.matches, "an ended drain is cleared")

	require.False(t, it.emitCachedMatches(0, cb, nil, outerTuple))
}

// The fallback answers a subject tuple from the first branch with a result
// for it, reading the result before the carried-output filter: a tuple
// whose carried output disagrees with branch 1's value is dropped and never
// passed to branch 2, and a tuple branch 1 has no result for takes branch
// 2.
func TestNextShortCircuitDecidesBeforeTheCarriedOutputFilter(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":t/a")
	e1, e2 := datalog.NewIdentity("decide1"), datalog.NewIdentity("decide2")
	datoms := []datalog.Datom{{E: e1, A: attr, V: int64(5), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}}
	var successes []int
	options := ExecutorOptions{Handler: func(e annotations.Event) {
		if e.Name == annotations.OrFallbackBranchSuccess {
			successes = append(successes, e.Data["branch_index"].(int))
		}
	}}
	outer := NewMaterializedRelation([]query.Symbol{entity, value}, []Tuple{{e1, int64(0)}, {e2, int64(0)}})
	relation := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(datoms), nil, options),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{branchPattern(entity, value, attr)}, {branchGround(value, int64(0))}},
		outer,
		options,
		true,
	)
	it := relation.Iterator()
	require.True(t, it.Next(), "the second subject tuple is answered")
	require.Equal(t, Tuple{e2, int64(0)}, it.Tuple())
	require.False(t, it.Next())
	require.NoError(t, it.Error())
	require.Equal(t, []int{1}, successes, "branch 1 answers e1 and the filter drops it; only e2 reaches branch 2")
}

// The union takes every branch's result per subject tuple, in branch order,
// and advances to the next subject tuple when the branches are exhausted.
func TestNextCorrelatedUnionTakesEveryBranchPerSubjectTuple(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemA, itemB := datalog.NewKeyword(":t/a"), datalog.NewKeyword(":t/b")
	e1, e2 := datalog.NewIdentity("union1"), datalog.NewIdentity("union2")
	datoms := []datalog.Datom{
		{E: e1, A: itemA, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: e1, A: itemB, V: int64(2), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: e2, A: itemB, V: int64(3), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
	}
	outer := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{e1}, {e2}})
	relation := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(datoms), nil, ExecutorOptions{}),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{branchPattern(entity, value, itemA)}, {branchPattern(entity, value, itemB)}},
		outer,
		ExecutorOptions{},
		false,
	)
	it := relation.Iterator()
	var got []Tuple
	for it.Next() {
		got = append(got, copyTuple(it.Tuple()))
	}
	require.NoError(t, it.Error())
	require.Equal(t, []Tuple{{e1, int64(1)}, {e1, int64(2)}, {e2, int64(3)}}, got)
}

// A union branch reading nothing of the subject is built at the first
// window and indexed on the header symbols its result shares with the
// subject, the required run and the carried outputs the subject carries,
// and probed per subject tuple: the probe's matches are the branch tuples
// agreeing with the subject tuple on those symbols, and the union's tuples
// are the subject joined with the branch on them. An output the environment
// alone binds is joined by the carried-output filter after the probe, which
// TestUnionBranchProvidingAnEnvironmentBoundOutputIsJoinedWithTheEnvironmentsTuple
// pins.
func TestUnionBranchIndexIsKeyedOnTheRequiredRunAndTheCarriedOutputs(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":t/a")
	x1, x2, x3 := datalog.NewIdentity("keyed1"), datalog.NewIdentity("keyed2"), datalog.NewIdentity("keyed3")
	datoms := []datalog.Datom{
		{E: x1, A: attr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: x2, A: attr, V: int64(3), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	subject := func() Relation {
		return NewMaterializedRelation([]query.Symbol{entity, value}, []Tuple{{x1, int64(1)}, {x1, int64(2)}, {x2, int64(3)}, {x3, int64(3)}})
	}
	unionOf := func(branch query.Clause) *OrFallbackIterator {
		relation := NewOrFallbackRelation(
			newQueryExecutor(NewMemoryPatternMatcher(datoms), nil, ExecutorOptions{}),
			NewContext(),
			[]query.Symbol{entity},
			[]query.Symbol{value},
			[][]query.Clause{{branch}},
			subject(),
			ExecutorOptions{},
			false,
		)
		it := relation.Iterator().(*OrFallbackIterator)
		require.NoError(t, it.err)
		require.True(t, it.nextWindow())
		require.NotNil(t, it.branchCache[0], "a branch reading nothing is built at the first window")
		return it
	}
	collect := func(it *OrFallbackIterator) []Tuple {
		var got []Tuple
		for it.Next() {
			got = append(got, copyTuple(it.Tuple()))
		}
		require.NoError(t, it.Error())
		return got
	}

	// A branch providing the carried output alone is keyed on it.
	it := unionOf(branchSubquery(value, attr))
	cb := it.branchCache[0]
	require.Equal(t, []Tuple{{int64(1)}}, cb.Probe(Tuple{x1, int64(1)}))
	require.Empty(t, cb.Probe(Tuple{x1, int64(2)}))
	require.Equal(t, []Tuple{{int64(3)}}, cb.Probe(Tuple{x3, int64(3)}))
	require.Equal(t, []Tuple{{x1, int64(1)}, {x2, int64(3)}, {x3, int64(3)}}, collect(it))

	// A branch providing the required run and the carried output is keyed on
	// both.
	inner, innerValue := datalog.NewSymbol("?inner"), datalog.NewSymbol("?innerValue")
	it = unionOf(&query.SubqueryPattern{
		Query: &query.Query{
			Find:  []query.FindElement{query.FindVariable{Symbol: inner}, query.FindVariable{Symbol: innerValue}},
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: []query.Clause{branchPattern(inner, innerValue, attr)},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{entity, value}},
	})
	cb = it.branchCache[0]
	require.Equal(t, []Tuple{{x1, int64(1)}}, cb.Probe(Tuple{x1, int64(1)}))
	require.Empty(t, cb.Probe(Tuple{x1, int64(2)}))
	require.Empty(t, cb.Probe(Tuple{x3, int64(3)}))
	require.Equal(t, []Tuple{{x1, int64(1)}, {x2, int64(3)}}, collect(it))
}

// A union branch providing an output the environment alone binds is joined
// with the environment's tuple: the branch's tuples disagreeing with the
// environment's value are dropped, and each subject tuple is paired with
// the agreeing value alone.
func TestUnionBranchProvidingAnEnvironmentBoundOutputIsJoinedWithTheEnvironmentsTuple(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":t/a")
	x1, x2, x3 := datalog.NewIdentity("envout1"), datalog.NewIdentity("envout2"), datalog.NewIdentity("envout3")
	datoms := []datalog.Datom{
		{E: x1, A: attr, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: x2, A: attr, V: int64(3), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	subject := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{x1}, {x3}})
	ctx := NewContext().WithEnvironment(NewMaterializedRelation([]query.Symbol{value}, []Tuple{{int64(1)}}))
	relation := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(datoms), nil, ExecutorOptions{}),
		ctx,
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{branchSubquery(value, attr)}},
		subject,
		ExecutorOptions{},
		false,
	)
	it := relation.Iterator().(*OrFallbackIterator)
	require.NoError(t, it.err)
	require.True(t, it.nextWindow())
	require.NotNil(t, it.branchCache[0], "a branch reading nothing is built at the first window")
	var got []Tuple
	for it.Next() {
		got = append(got, copyTuple(it.Tuple()))
	}
	require.NoError(t, it.Error())
	require.Equal(t, []Tuple{{x1, int64(1)}, {x3, int64(1)}}, got, "the value 3 disagrees with the environment and is dropped")
}

// Whether the result deduplicates follows the form and the subject's key: a
// union of more than one branch deduplicates on the composite key, since two
// branches can answer one subject tuple; a union of one branch deduplicates
// nothing; and a fallback deduplicates only over a subject carrying no key,
// whatever its branches' arity, since one branch answers each subject tuple
// and a branch's answer is a set. A fallback over a keyed subject whose
// branches answer more than one tuple carries the composite key and not the
// subject's.
func TestDeduplicationFollowsTheFormAndTheSubjectsKey(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	e1 := datalog.NewIdentity("dedup1")
	keyed := newMaterializedRelationFromSet([]query.Symbol{entity}, []Tuple{{e1}}, ExecutorOptions{}, deduplicatedProperties([]query.Symbol{entity}))
	keyless := newMaterializedRelationFromSet([]query.Symbol{entity}, []Tuple{{e1}}, ExecutorOptions{}, RelationProperties{})
	multi := [][]query.Clause{
		{branchPattern(entity, value, datalog.NewKeyword(":t/a"))},
		{branchPattern(entity, value, datalog.NewKeyword(":t/b"))},
	}
	one := [][]query.Clause{{branchGround(value, int64(0))}}
	form := func(subject Relation, branches [][]query.Clause, shortCircuit bool) *OrFallbackRelation {
		return NewOrFallbackRelation(
			newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
			NewContext(),
			[]query.Symbol{entity},
			[]query.Symbol{value},
			branches,
			subject,
			ExecutorOptions{},
			shortCircuit,
		)
	}

	require.True(t, form(keyed, multi, false).deduplicate, "a union of two branches deduplicates")
	require.False(t, form(keyed, one, false).deduplicate, "a union of one branch deduplicates nothing")

	fallback := form(keyed, multi, true)
	require.False(t, fallback.deduplicate, "a fallback over a keyed subject deduplicates nothing")
	require.Contains(t, fallback.properties.Keys, []query.Symbol{entity, value}, "the composite key")
	require.NotContains(t, fallback.properties.Keys, []query.Symbol{entity}, "the subject's key does not survive branches answering more than one tuple")
	require.True(t, form(keyless, multi, true).deduplicate, "a fallback over a keyless subject deduplicates")
	require.True(t, form(keyless, one, true).deduplicate, "a fallback over a keyless subject deduplicates whatever its branches' arity")
}

// fusionOracle is a matcher answering CanFuseAttributeFetch from a function
// of the source and the attribute.
type fusionOracle struct {
	answer func(source query.Symbol, attr datalog.Keyword) bool
}

func (fusionOracle) Match(*query.Query, Relations) (Relation, error) { return nil, nil }

func (o fusionOracle) CanFuseAttributeFetch(source query.Symbol, attr datalog.Keyword) bool {
	return o.answer(source, attr)
}

// A fallback keeps its subject's key through a branch of one data pattern
// when the pattern answers at most one tuple per subject tuple: its entity
// position is a required symbol the subject carries, its attribute is a
// constant, and the matcher serves the attribute on the pattern's source by
// one lookup. A matcher answering every version, as under History, one
// without the capability, an entity position the header does not require,
// and a default-source answer for a pattern on a named source each leave the
// result with the composite key alone.
func TestFallbackKeepsTheSubjectsKeyThroughAPatternBranchServedByOneLookup(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	attr := datalog.NewKeyword(":t/a")
	e1 := datalog.NewIdentity("lookup1")
	keyed := newMaterializedRelationFromSet([]query.Symbol{entity}, []Tuple{{e1}}, ExecutorOptions{}, deduplicatedProperties([]query.Symbol{entity}))
	pattern := branchPattern(entity, value, attr)
	ground := branchGround(value, int64(0))
	keysOf := func(matcher PatternMatcher, required, outputs []query.Symbol, branch query.Clause) [][]query.Symbol {
		return NewOrFallbackRelation(
			newQueryExecutor(matcher, nil, ExecutorOptions{}),
			NewContext(),
			required,
			outputs,
			[][]query.Clause{{branch}, {ground}},
			keyed,
			ExecutorOptions{},
			true,
		).properties.Keys
	}
	subjectKey := []query.Symbol{entity}
	cardinalityOne := fusionOracle{answer: func(source query.Symbol, asked datalog.Keyword) bool {
		return source == datalog.SymDollar && asked == attr
	}}
	everyVersion := fusionOracle{answer: func(query.Symbol, datalog.Keyword) bool { return false }}

	require.Contains(t, keysOf(cardinalityOne, []query.Symbol{entity}, []query.Symbol{value}, pattern), subjectKey, "a cardinality-one pattern keyed by the subject's required entity")
	require.NotContains(t, keysOf(everyVersion, []query.Symbol{entity}, []query.Symbol{value}, pattern), subjectKey, "the matcher answers every version, as under History")
	require.NotContains(t, keysOf(NewMemoryPatternMatcher(nil), []query.Symbol{entity}, []query.Symbol{value}, pattern), subjectKey, "a matcher without the capability")
	require.NotContains(t, keysOf(cardinalityOne, nil, []query.Symbol{entity, value}, pattern), subjectKey, "an entity the header does not require")

	named := datalog.NewSymbol("$named")
	onNamed := &query.DataPattern{Source: named, Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
	}}
	bySource := fusionOracle{answer: func(source query.Symbol, _ datalog.Keyword) bool { return source == named }}
	require.Contains(t, keysOf(bySource, []query.Symbol{entity}, []query.Symbol{value}, onNamed), subjectKey, "a pattern on a named source is answered by that source")
	require.NotContains(t, keysOf(cardinalityOne, []query.Symbol{entity}, []query.Symbol{value}, onNamed), subjectKey, "the default source does not answer for a named one")
}

// A data pattern answers at most one tuple per subject tuple when its
// entity position is a required symbol the subject carries, its attribute
// is a constant, and the matcher serves the attribute on the pattern's
// source by one lookup. A matcher answering every version, one without the
// capability, an entity the header does not require or the subject does
// not carry, an attribute position holding a variable, and a default-source
// answer for a pattern on a named source each deny it.
func TestPatternAnswersAtMostOneAsksTheMatcherForThePatternsSource(t *testing.T) {
	entity, value, task := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?task")
	attr := datalog.NewKeyword(":t/a")
	pattern := branchPattern(entity, value, attr).(*query.DataPattern)
	required := []query.Symbol{entity}
	carried := map[query.Symbol]bool{entity: true}
	cardinalityOne := fusionOracle{answer: func(source query.Symbol, asked datalog.Keyword) bool {
		return source == datalog.SymDollar && asked == attr
	}}
	everyVersion := fusionOracle{answer: func(query.Symbol, datalog.Keyword) bool { return false }}
	require.True(t, patternAnswersAtMostOne(pattern, required, carried, cardinalityOne), "a cardinality-one pattern keyed by the subject's required entity")
	require.False(t, patternAnswersAtMostOne(pattern, required, carried, everyVersion), "the matcher answers every version, as under History")
	require.False(t, patternAnswersAtMostOne(pattern, required, carried, NewMemoryPatternMatcher(nil)), "a matcher without the capability")
	require.False(t, patternAnswersAtMostOne(pattern, nil, carried, cardinalityOne), "an entity the header does not require")
	require.False(t, patternAnswersAtMostOne(pattern, required, nil, cardinalityOne), "an entity the subject does not carry")
	require.False(t, patternAnswersAtMostOne(branchPattern(value, entity, attr).(*query.DataPattern), required, carried, cardinalityOne), "an entity position holding an output")
	variableAttr := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Variable{Name: task}, query.Variable{Name: value},
	}}
	require.False(t, patternAnswersAtMostOne(variableAttr, required, carried, cardinalityOne), "an attribute position holding a variable")
	named := datalog.NewSymbol("$named")
	onNamed := &query.DataPattern{Source: named, Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
	}}
	bySource := fusionOracle{answer: func(source query.Symbol, _ datalog.Keyword) bool { return source == named }}
	require.True(t, patternAnswersAtMostOne(onNamed, required, carried, bySource), "a pattern on a named source is answered by that source")
	require.False(t, patternAnswersAtMostOne(onNamed, required, carried, cardinalityOne), "the default source does not answer for a named one")
}

// What a branch is given is the required run under the fallback, and the
// required run with the carried outputs under the union, each symbol once.
func TestGivenSymbolsAreTheRequiredRunWithTheCarriedOutputsUnderTheUnion(t *testing.T) {
	entity, value, flag := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?flag")
	carried := []carriedOutput{{symbol: value, outerPos: 1}, {symbol: flag, outerPos: -1, envValue: "env"}, {symbol: value, outerPos: 1}}
	require.Equal(t, []query.Symbol{entity}, givenSymbols([]query.Symbol{entity}, carried, true), "the fallback gives the required run alone")
	require.Equal(t, []query.Symbol{entity, value, flag}, givenSymbols([]query.Symbol{entity}, carried, false), "the union gives the carried outputs after the required run, each once")
	require.Empty(t, givenSymbols(nil, nil, false))
}

// A branch reads a given symbol when a clause consumes it that no clause
// of the branch provides, or a data pattern mentions it, the reads in the
// given order; a symbol the branch provides, one it provides and then
// consumes, one an invocation mentions, and one no clause names are not
// read.
func TestBranchReadsAConsumedUnprovidedSymbolOrAMentionedOne(t *testing.T) {
	entity, value, sum, other := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?sum"), datalog.NewSymbol("?other")
	attr := datalog.NewKeyword(":t/a")
	given := []query.Symbol{entity, value}
	sumOfBoth := &query.Expression{
		Function: query.ArithmeticFunction{Op: datalog.SymAdd, Args: []query.Term{
			query.VariableTerm{Symbol: entity}, query.VariableTerm{Symbol: value},
		}},
		Binding: sum,
	}
	require.Equal(t, []query.Symbol{entity}, branchReads([]query.Clause{branchSubquery(value, attr), sumOfBoth}, given, nil), "a consumed symbol no clause provides is read, and one the subquery provides is not")
	require.Equal(t, []query.Symbol{entity, value}, branchReads([]query.Clause{branchPattern(entity, value, attr)}, given, nil), "a data pattern's mention is a read")
	require.Equal(t, []query.Symbol{entity, value}, branchReads([]query.Clause{branchPattern(value, entity, attr)}, given, nil), "the reads keep the given order whatever the pattern's")
	invocation := &query.RuleInvocation{Name: datalog.NewSymbol("reach"), Args: []query.PatternElement{
		query.Variable{Name: entity}, query.Variable{Name: other},
	}}
	require.Empty(t, branchReads([]query.Clause{invocation}, given, nil), "an invocation's mention is not a read: it provides its variables and consumes nothing")
	nested := &query.OrJoinClause{RequiredVars: []query.Symbol{value}, OutputVars: []query.Symbol{other}, Branches: [][]query.Clause{{branchPattern(value, other, attr)}}}
	require.Equal(t, []query.Symbol{value}, branchReads([]query.Clause{nested}, given, nil), "a nested form's required run is consumed")
	require.Empty(t, branchReads([]query.Clause{branchGround(value, int64(0))}, given, nil), "a provided symbol is not read")
	doubled := &query.Expression{
		Function: query.ArithmeticFunction{Op: datalog.SymAdd, Args: []query.Term{
			query.VariableTerm{Symbol: entity}, query.VariableTerm{Symbol: entity},
		}},
		Binding: sum,
	}
	require.Empty(t, branchReads([]query.Clause{branchGround(entity, int64(1)), doubled}, given, nil), "a symbol the branch provides and then consumes is not read")
	require.Empty(t, branchReads([]query.Clause{branchSubquery(other, attr)}, given, nil), "a symbol no clause names is not read")
}
