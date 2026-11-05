package executor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestBuildBranchFromEACacheEnvironmentNarrowsV pins the EA-cache arm's
// environment narrowing: when the branch pattern's V variable is bound by
// the environment relation, only tuples carrying the environment's value
// survive — the SemiJoin analog of the scan path's environment-narrowed
// input. The test constructs a run lacking V beside an environment that
// binds it, an input the windowed route never builds.
func TestBuildBranchFromEACacheEnvironmentNarrowsV(t *testing.T) {
	symE := datalog.NewSymbol("?e")
	symV := datalog.NewSymbol("?v")
	attr := datalog.NewKeyword(":item/priority")
	e1 := datalog.NewIdentity("eaenv:e1")
	e2 := datalog.NewIdentity("eaenv:e2")

	matcher := factsDatabase(t, []datalog.Datom{
		{E: e1, A: attr, V: int64(1)},
		{E: e2, A: attr, V: int64(2)},
	}).Matcher()
	exec := NewQueryExecutor(matcher, nil, ExecutorOptions{})

	run := NewMaterializedRelation([]query.Symbol{symE}, []Tuple{{e1}, {e2}})
	it := NewOrFallbackIterator(exec, []query.Symbol{symE}, nil, ExecutorOptions{})
	it.SetEnvironment(NewMaterializedRelation([]query.Symbol{symV}, []Tuple{{int64(2)}}))
	branch := []query.Clause{
		&query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: symE},
				query.Constant{Value: attr},
				query.Variable{Name: symV},
			},
		},
	}

	tuples, branchSyms := it.BuildBranchFromEACache(branch, run)
	require.NoError(t, it.Err())
	require.Equal(t, []query.Symbol{symE, symV}, branchSyms)
	assert.Equal(t, []Tuple{{e2, int64(2)}}, tuples, "e1's value 1 does not match the environment's 2")
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
	plansOf := func(matcher PatternMatcher, required []query.Symbol, branch query.Clause, subject Relation) (*OrFallbackRelation, []BranchRun) {
		r, err := NewOrFallbackRelation(NewQueryExecutor(matcher, nil, ExecutorOptions{}), NewContext(), required, []query.Symbol{value}, [][]query.Clause{{branch}}, subject, ExecutorOptions{}, true)
		require.NoError(t, err)
		plans, err := r.PlanBranches()
		require.NoError(t, err)
		return r, plans
	}

	r, plans := plansOf(factsDatabase(t, nil), nil, BranchGround(value, int64(3)), NewUnitRelation(ExecutorOptions{}))
	tuples, syms, err := BuildBranchTuples(r.Executor(), r.Ctx(), r.Framing(nil), plans[0], nil)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{value}, syms, "the result's symbols are the frame's header")
	require.Equal(t, []Tuple{{int64(3)}}, tuples, "a nil run is the unit relation, over which the ground yields its one tuple")

	subject := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{e1}})
	run := NewMaterializedRelationFromSetWithProperties([]query.Symbol{entity}, []Tuple{{e1}}, ExecutorOptions{}, DeduplicatedProperties([]query.Symbol{entity}))
	r, plans = plansOf(factsDatabase(t, datoms), []query.Symbol{entity}, BranchPattern(entity, value, attr), subject)
	tuples, syms, err = BuildBranchTuples(r.Executor(), r.Ctx(), r.Framing(nil), plans[0], run)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{entity, value}, syms)
	require.Equal(t, []Tuple{{e1, int64(5)}}, tuples, "the pattern over the run's entity yields its value")

	failing := datalog.NewKeyword(":t/failing")
	matcher := NewFailingScanMatcher(failing, NewMaterializedRelation([]query.Symbol{entity, value}, nil))
	r, plans = plansOf(matcher, []query.Symbol{entity}, BranchPattern(entity, value, failing), subject)
	_, _, err = BuildBranchTuples(r.Executor(), r.Ctx(), r.Framing(nil), plans[0], run)
	require.ErrorIs(t, err, ErrInjectedIterator, "the branch's scan failure is the build's error")
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
	exec := NewQueryExecutor(factsDatabase(t, datoms), nil, ExecutorOptions{})
	subst, err := query.SubstitutionOf([]query.Symbol{x, y, v}, []query.Symbol{e, e, c})
	require.NoError(t, err)
	f := NewSubqueryFraming(
		[]query.Symbol{x, y, k}, []query.FindElement{query.FindVariable{Symbol: v}}, []query.Symbol{x, y, k}, []query.Symbol{e, e, nil},
		NewMaterializedRelation([]query.Symbol{e}, nil), subst)

	plans, err := FrameBranches(exec, NewContext(), [][]query.Clause{{BranchPattern(x, v, attr)}}, f)
	require.NoError(t, err)
	run := NewMaterializedRelationFromSetWithProperties([]query.Symbol{x, y, k}, []Tuple{{e1, e1, int64(9)}}, ExecutorOptions{}, DeduplicatedProperties([]query.Symbol{x, y, k}))
	tuples, syms, err := BuildBranchTuples(exec, NewContext(), f, plans[0], run)
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
	matcher := factsDatabase(t, []datalog.Datom{{E: e1, A: attr, V: int64(4)}}).Matcher()
	var events []annotations.Event
	opts := ExecutorOptions{Handler: func(e annotations.Event) { events = append(events, e) }}
	it := NewOrFallbackIterator(NewQueryExecutor(matcher, nil, opts), []query.Symbol{entity}, [][]query.Clause{{BranchGround(value, int64(0))}, {BranchPattern(entity, value, attr)}}, opts)
	run := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{e1}})

	tuples, syms, err := it.BuildFromCache(0, run)
	require.NoError(t, err)
	require.Nil(t, syms, "a ground branch is not the fallback's to build")
	require.Nil(t, tuples)
	require.Empty(t, events, "and reports no narrowing")

	tuples, syms, err = it.BuildFromCache(1, run)
	require.NoError(t, err)
	require.Nil(t, syms, "on a cold cache the pattern branch is built from the plan")
	require.Nil(t, tuples)
	require.Len(t, events, 1)
	require.Equal(t, annotations.OrFallbackBranchNarrowed, events[0].Name)
	require.Equal(t, 1, events[0].Data["branch"])
	require.Equal(t, true, events[0].Data["narrowed"])
	require.Equal(t, 1, events[0].Data["join_keys"], "the run's one key")

	it.SetPrefetched()
	tuples, syms, err = it.BuildFromCache(1, run)
	require.NoError(t, err)
	require.Equal(t, []query.Symbol{entity, value}, syms, "on a warm cache the branch is built from the cache")
	require.Equal(t, []Tuple{{e1, int64(4)}}, tuples)
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
	relation, err := NewOrFallbackRelation(
		NewQueryExecutor(factsDatabase(t, datoms), nil, options),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{BranchPattern(entity, value, attr)}, {BranchGround(value, int64(0))}},
		outer,
		options,
		true,
	)
	require.NoError(t, err)
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
	relation, err := NewOrFallbackRelation(
		NewQueryExecutor(factsDatabase(t, datoms), nil, ExecutorOptions{}),
		NewContext(),
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{BranchPattern(entity, value, itemA)}, {BranchPattern(entity, value, itemB)}},
		outer,
		ExecutorOptions{},
		false,
	)
	require.NoError(t, err)
	it := relation.Iterator()
	var got []Tuple
	for it.Next() {
		got = append(got, CopyTuple(it.Tuple()))
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
		relation, err := NewOrFallbackRelation(
			NewQueryExecutor(factsDatabase(t, datoms), nil, ExecutorOptions{}),
			NewContext(),
			[]query.Symbol{entity},
			[]query.Symbol{value},
			[][]query.Clause{{branch}},
			subject(),
			ExecutorOptions{},
			false,
		)
		require.NoError(t, err)
		it := relation.Iterator().(*OrFallbackIterator)
		require.NoError(t, it.Err())
		require.True(t, it.NextWindow())
		require.NotNil(t, it.BranchCache(0), "a branch reading nothing is built at the first window")
		return it
	}
	collect := func(it *OrFallbackIterator) []Tuple {
		var got []Tuple
		for it.Next() {
			got = append(got, CopyTuple(it.Tuple()))
		}
		require.NoError(t, it.Error())
		return got
	}

	// A branch providing the carried output alone is keyed on it.
	it := unionOf(BranchSubquery(value, attr))
	cb := it.BranchCache(0)
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
			Where: []query.Clause{BranchPattern(inner, innerValue, attr)},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
		Binding: query.RelationBinding{Variables: []query.Symbol{entity, value}},
	})
	cb = it.BranchCache(0)
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
	relation, err := NewOrFallbackRelation(
		NewQueryExecutor(factsDatabase(t, datoms), nil, ExecutorOptions{}),
		ctx,
		[]query.Symbol{entity},
		[]query.Symbol{value},
		[][]query.Clause{{BranchSubquery(value, attr)}},
		subject,
		ExecutorOptions{},
		false,
	)
	require.NoError(t, err)
	it := relation.Iterator().(*OrFallbackIterator)
	require.NoError(t, it.Err())
	require.True(t, it.NextWindow())
	require.NotNil(t, it.BranchCache(0), "a branch reading nothing is built at the first window")
	var got []Tuple
	for it.Next() {
		got = append(got, CopyTuple(it.Tuple()))
	}
	require.NoError(t, it.Error())
	require.Equal(t, []Tuple{{x1, int64(1)}, {x3, int64(1)}}, got, "the value 3 disagrees with the environment and is dropped")
}
