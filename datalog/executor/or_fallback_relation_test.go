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
	it := &OrFallbackIterator{carried: []carriedOutput{
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
	none := &OrFallbackIterator{}
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
	require.Equal(t, []query.Symbol{value}, plans[0].frame.outputs)
	require.True(t, plans[1].readsNothing, "an uncorrelated subquery reads nothing")
	require.Empty(t, plans[1].frame.required)
	require.Equal(t, []query.Symbol{value}, plans[1].frame.outputs)
	require.True(t, plans[2].readsNothing, "a lone ground reads nothing")
	require.Empty(t, plans[2].frame.required)
	require.Equal(t, []query.Symbol{value}, plans[2].frame.outputs)
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
	require.Equal(t, []query.Symbol{value}, plans[0].frame.outputs)

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
	require.Equal(t, []query.Symbol{value}, plans[0].frame.outputs)
}

// runBranch runs a branch's plan over the run it is given, and over the
// unit relation when given none.
func TestRunBranchRunsOverTheUnitRelationWithNoInput(t *testing.T) {
	value := datalog.NewSymbol("?value")
	r := NewOrFallbackRelation(
		newQueryExecutor(NewMemoryPatternMatcher(nil), nil, ExecutorOptions{}),
		NewContext(),
		nil,
		[]query.Symbol{value},
		[][]query.Clause{{branchGround(value, int64(3))}},
		NewUnitRelation(ExecutorOptions{}),
		ExecutorOptions{},
		true,
	)
	plans, err := r.planBranches()
	require.NoError(t, err)
	it := &OrFallbackIterator{executor: r.executor, ctx: r.ctx, plans: plans, options: r.options}
	result, err := it.runBranch(0, nil)
	require.NoError(t, err)
	tuples, err := CollectTuples(result, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, tuples)
}

// OrFallbackIterator's events carry their payloads: the cache build names
// the branch, its symbols, the subject's and the size; the outer tuple
// event renders the tuple; the exhausted event carries nothing; and with no
// handler nothing is reported.
func TestOrFallbackEventsCarryTheirPayloads(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	var events []annotations.Event
	it := &OrFallbackIterator{
		outerSyms: []query.Symbol{entity},
		options:   ExecutorOptions{Handler: func(e annotations.Event) { events = append(events, e) }},
	}
	it.reportCacheBuild(2, []query.Symbol{entity, value}, 7)
	it.reportOuterTuple(Tuple{int64(1)})
	it.reportOuterExhausted()
	require.Len(t, events, 3)
	require.Equal(t, annotations.OrFallbackCacheBuild, events[0].Name)
	require.Equal(t, 2, events[0].Data["branch"])
	require.Equal(t, []query.Symbol{entity, value}, events[0].Data["branch_syms"])
	require.Equal(t, []query.Symbol{entity}, events[0].Data["outer_syms"])
	require.Equal(t, 7, events[0].Data["branch_size"])
	require.Equal(t, annotations.OrFallbackOuterTuple, events[1].Name)
	require.Equal(t, "[1]", events[1].Data["tuple"])
	require.Equal(t, annotations.OrFallbackOuterExhausted, events[2].Name)
	require.Empty(t, events[2].Data)

	silent := &OrFallbackIterator{outerSyms: []query.Symbol{entity}}
	silent.reportCacheBuild(0, nil, 0)
	silent.reportOuterTuple(Tuple{})
	silent.reportOuterExhausted()
}

// emitCachedMatches sets the first match as the current tuple, projected
// onto the output symbols with the gaps filled from the subject tuple, and
// records the rest for the drain; with no match it emits nothing.
func TestEmitCachedMatchesSetsTheFirstTupleAndTheDrainState(t *testing.T) {
	entity, name, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?name"), datalog.NewSymbol("?value")
	e1 := datalog.NewIdentity("emit1")
	it := &OrFallbackIterator{
		outputSyms: []query.Symbol{entity, name, value},
		outerSyms:  []query.Symbol{entity, name},
	}
	cb := &cachedBranch{branchSyms: []query.Symbol{entity, value}}
	outerTuple := Tuple{e1, "n"}
	matches := []Tuple{{e1, int64(1)}, {e1, int64(2)}}
	require.True(t, it.emitCachedMatches(0, cb, matches, outerTuple))
	require.Equal(t, Tuple{e1, "n", int64(1)}, it.currentTuple)
	require.Equal(t, matches, it.cachedMatches)
	require.Equal(t, 1, it.cachedMatchPos)
	require.Equal(t, outerTuple, it.cachedOuter)
	require.False(t, it.cachedPlan.identity)

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
		require.True(t, it.fillWindow())
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
	require.True(t, it.fillWindow())
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
