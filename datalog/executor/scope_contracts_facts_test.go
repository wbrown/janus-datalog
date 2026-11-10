package executor_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// The contracts of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7 that an
// executor over a memory database pins, each the subject of one test.

// A streaming subject of a form whose every branch runs per tuple yields its
// first result tuple after one pull of the subject, reports no size, and is
// read once, to its end.
func TestStreamingOuterIsReadOnce(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemValue, itemOther := datalog.NewKeyword(":item/value"), datalog.NewKeyword(":item/other")
	const subjects = 5
	var datoms []datalog.Datom
	var tuples []Tuple
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		tuples = append(tuples, Tuple{e})
	}
	pattern := func(attr datalog.Keyword) query.Clause {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
		}}
	}
	q := &query.Query{Where: []query.Clause{&query.OrJoinClause{
		RequiredVars: []query.Symbol{entity},
		OutputVars:   []query.Symbol{value},
		Branches:     [][]query.Clause{{pattern(itemValue)}, {pattern(itemOther)}},
	}}}
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, mode.PlannerOptions()).QueryExecutor(nil)
			groups, err := exec.Execute(NewContext(), q, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the form's result stands in the subject's place")
			result := groups[0]
			require.Equal(t, -1, result.Size(), "the result over a streaming subject reports no size")
			it := result.Iterator()
			require.True(t, it.Next(), "the form answers a tuple")
			require.Equal(t, 1, pulls.Count(), "the first result tuple follows one pull of the subject")
			for it.Next() {
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, subjects, pulls.Count(), "the subject is read once, to its end")
		})
	}
}

// A find consuming nothing reads no group: every group is a verdict, and a
// plan whose fragment finds nothing yields the unit relation, one tuple
// over no symbol, when each group has a tuple, and nothing when one has
// none. A scope whose header is empty is such a plan, so a branch
// mentioning no header symbol answers as a verdict.
func TestAFindConsumingNothingIsAVerdictOverEveryGroup(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemValue, itemOther := datalog.NewKeyword(":item/value"), datalog.NewKeyword(":item/other")
	datoms := []datalog.Datom{{E: datalog.NewIdentity("verdict1"), A: itemValue, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}}}
	finding := func(attr datalog.Keyword) *query.Query {
		return &query.Query{
			In: []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
			Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
			}}},
		}
	}
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, mode.PlannerOptions())
			plan, err := exec.GetPlanner().PlanQuery(finding(itemValue), query.Bindings{}, nil)
			require.NoError(t, err)
			result, err := exec.ExecuteRealized(NewContext(), plan, nil)
			require.NoError(t, err)
			require.Empty(t, result.Symbols(), "the unit relation carries no symbol")
			tuples, err := CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 1, "the unit relation carries one tuple")

			plan, err = exec.GetPlanner().PlanQuery(finding(itemOther), query.Bindings{}, nil)
			require.NoError(t, err)
			result, err = exec.ExecuteRealized(NewContext(), plan, nil)
			require.NoError(t, err)
			tuples, err = CollectTuples(result, nil)
			require.NoError(t, err)
			require.Empty(t, tuples, "a body with no match yields nothing")
		})
	}
}

// An or-join reports its execution through the or-clause events. Over a
// subject it reports a begin and a completion whose semantics is the
// correlated union, the completion firing at construction with no size. Over
// the unit relation it reports a begin, the union event, one branch
// completion per branch, and a completion whose semantics is the union and
// whose size is the union's. An or-default-join reports through its
// OrFallbackRelation's events and no or-clause event.
func TestFormReportsItsExecution(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemA, itemB := datalog.NewKeyword(":item/a"), datalog.NewKeyword(":item/b")
	e1, e2 := datalog.NewIdentity("report1"), datalog.NewIdentity("report2")
	datoms := []datalog.Datom{
		{E: e1, A: itemA, V: int64(1), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: e2, A: itemB, V: int64(2), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	pattern := func(attr datalog.Keyword) query.Clause {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
		}}
	}
	branches := [][]query.Clause{{pattern(itemA)}, {pattern(itemB)}}
	subject := func() []Relation {
		return []Relation{NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{e1}, {e2}})}
	}
	forms := []struct {
		name      string
		clause    query.Clause
		inputs    []Relation
		semantics string
	}{
		{
			name:      "or-join over a subject",
			clause:    &query.OrJoinClause{RequiredVars: []query.Symbol{entity}, OutputVars: []query.Symbol{value}, Branches: branches},
			inputs:    subject(),
			semantics: "correlated-union",
		},
		{
			name:      "or-join over the unit relation",
			clause:    &query.OrJoinClause{OutputVars: []query.Symbol{entity, value}, Branches: branches},
			semantics: "union",
		},
		{
			name:   "or-default-join",
			clause: &query.OrDefaultJoinClause{RequiredVars: []query.Symbol{entity}, OutputVars: []query.Symbol{value}, Branches: branches},
			inputs: subject(),
		},
	}
	for _, mode := range LegsFor(t, FusionOption) {
		for _, form := range forms {
			t.Run(mode.Name()+"/"+form.name, func(t *testing.T) {
				var events []annotations.Event
				opts := mode.PlannerOptions()
				opts.Handler = mode.Recording(func(e annotations.Event) { events = append(events, e) })
				exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, opts).QueryExecutor(nil)
				groups, err := exec.Execute(NewContext(), &query.Query{Where: []query.Clause{form.clause}}, form.inputs)
				require.NoError(t, err)
				var tuples [][]interface{}
				for _, group := range groups {
					collected, err := CollectTuples(group, nil)
					require.NoError(t, err)
					tuples = append(tuples, collected...)
				}
				require.ElementsMatch(t, [][]interface{}{{e1, int64(1)}, {e2, int64(2)}}, tuples)

				reported := OrClauseEvents(events)
				if form.semantics == "" {
					require.Empty(t, reported, "the fallback handler reports through its OrFallbackRelation's events and no or-clause event")
					return
				}
				require.NotEmpty(t, reported, "the or-join reports its execution")
				begin, complete := reported[0], reported[len(reported)-1]
				require.Equal(t, annotations.OrClauseBegin, begin.Name)
				require.Equal(t, 2, begin.Data["branch_count"])
				require.Equal(t, annotations.OrClauseComplete, complete.Name)
				require.Equal(t, form.semantics, complete.Data["semantics"])
				require.Equal(t, true, complete.Data[annotations.KeySuccess])
				require.False(t, complete.End.IsZero(), "the completion is timed from the begin")
				between := reported[1 : len(reported)-1]
				if form.semantics == "union" {
					require.Equal(t, 2, complete.Data["result_size"], "the union's size is the completion's")
					require.Len(t, between, 3, "the union event and one completion per branch; got %v", between)
					require.Equal(t, annotations.OrClauseUnion, between[0].Name)
					require.Equal(t, 2, between[0].Data["branch_count"])
					for i, branch := range between[1:] {
						require.Equal(t, annotations.OrClauseBranchComplete, branch.Name)
						require.Equal(t, i, branch.Data["branch_index"])
						require.Equal(t, "union", branch.Data["mode"])
						require.Equal(t, 1, branch.Data["result_size"])
					}
					return
				}
				require.Equal(t, -1, complete.Data["result_size"], "the completion of a streaming form fires at construction with no size")
				require.Empty(t, between, "the correlated union reports no union event and no branch completion")
			})
		}
	}
}

// A scope's run reports no phase: a query over a form reports the phases of
// its own plan, and the branches' runs per subject tuple add none, so the
// phase events number the plan's phases whatever the subject's size.
func TestScopeRunReportsNoPhase(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemN, itemA, itemB := datalog.NewKeyword(":item/n"), datalog.NewKeyword(":item/a"), datalog.NewKeyword(":item/b")
	const subjects = 6
	var datoms []datalog.Datom
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("phase%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: itemN, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(2 * i), ReplicaID: 1}})
		attr := itemA
		if i%2 == 0 {
			attr = itemB
		}
		datoms = append(datoms, datalog.Datom{E: e, A: attr, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(2*i + 1), ReplicaID: 1}})
	}
	pattern := func(attr datalog.Keyword) query.Clause {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
		}}
	}
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: entity}, query.FindVariable{Symbol: value}},
		In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
		Where: []query.Clause{
			&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: entity}, query.Constant{Value: itemN}, query.Blank{},
			}},
			&query.OrJoinClause{
				RequiredVars: []query.Symbol{entity},
				OutputVars:   []query.Symbol{value},
				Branches:     [][]query.Clause{{pattern(itemA)}, {pattern(itemB)}},
			},
		},
	}
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var begins, completes int
			opts := mode.PlannerOptions()
			opts.Handler = mode.Recording(func(e annotations.Event) {
				switch e.Name {
				case annotations.PhaseBegin:
					begins++
				case annotations.PhaseComplete:
					completes++
				}
			})
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, opts)
			prepared, err := query.Prepare(q, nil)
			require.NoError(t, err)
			plan, err := exec.GetPlanner().PlanQuery(prepared, query.Bindings{}, nil)
			require.NoError(t, err)
			tuples, err := CollectTuples(exec.Execute(q))
			require.NoError(t, err)
			require.Len(t, tuples, subjects, "every entity's :item/a or :item/b is answered")
			require.Equal(t, len(plan.Phases), begins, "the query reports its own phases and the scope's runs add none")
			require.Equal(t, len(plan.Phases), completes, "the query reports its own phases and the scope's runs add none")
		})
	}
}

// A subquery that reads a symbol a form provides reads the form's result
// once, over the form's streaming subject: the form's result stands in the
// subject's place, the subquery's binding enters the scope as a relation
// over its symbols joined with it, and the subject is pulled once to its
// end. An inner query holding no invocation materializes nothing of the
// subject.
func TestSubqueryOverAFormReadsTheSubjectOnce(t *testing.T) {
	entity, value, count := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?count")
	pointer, target := datalog.NewSymbol("?pointer"), datalog.NewSymbol("?target")
	itemValue, itemOther, pointsAt := datalog.NewKeyword(":item/value"), datalog.NewKeyword(":item/other"), datalog.NewKeyword(":item/points-at")
	const subjects = 5
	var datoms []datalog.Datom
	var tuples []Tuple
	var want [][]interface{}
	var lamport uint64
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item%d", i))
		lamport++
		datoms = append(datoms, datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: lamport, ReplicaID: 1}})
		for j := 0; j < i; j++ {
			lamport++
			datoms = append(datoms, datalog.Datom{E: datalog.NewIdentity(fmt.Sprintf("pointer%d-%d", i, j)), A: pointsAt, V: int64(i), Tx: datalog.ElementID{Lamport: lamport, ReplicaID: 1}})
		}
		tuples = append(tuples, Tuple{e})
		want = append(want, []interface{}{e, int64(i), int64(i)})
	}
	pattern := func(attr datalog.Keyword) query.Clause {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: attr}, query.Variable{Name: value},
		}}
	}
	q := &query.Query{Where: []query.Clause{
		&query.OrJoinClause{
			RequiredVars: []query.Symbol{entity},
			OutputVars:   []query.Symbol{value},
			Branches:     [][]query.Clause{{pattern(itemValue)}, {pattern(itemOther)}},
		},
		&query.SubqueryPattern{
			Query: &query.Query{
				Find: []query.FindElement{query.FindAggregate{Function: datalog.SymCount, Arg: pointer}},
				In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.ScalarInput{Symbol: target}},
				Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
					query.Variable{Name: pointer}, query.Constant{Value: pointsAt}, query.Variable{Name: target},
				}}},
			},
			Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Variable{Name: value}},
			Binding: query.RelationBinding{Variables: []query.Symbol{count}},
		},
	}}
	for _, mode := range LegsFor(t, FusionOption, AggregationOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			opts := mode.PlannerOptions()
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, opts).QueryExecutor(nil)
			groups, err := exec.Execute(NewContext(), q, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the subquery's result stands in the form's place")
			collected, err := CollectTuples(groups[0], nil)
			require.NoError(t, err)
			syms := groups[0].Symbols()
			ei, vi, ci := query.SymbolIndex(syms, entity), query.SymbolIndex(syms, value), query.SymbolIndex(syms, count)
			require.True(t, ei >= 0 && vi >= 0 && ci >= 0, "the result carries the subject's symbols and the binding; got %v", syms)
			var got [][]interface{}
			for _, tuple := range collected {
				got = append(got, []interface{}{tuple[ei], tuple[vi], tuple[ci]})
			}
			require.ElementsMatch(t, want, got)
			require.Equal(t, subjects, pulls.Count(), "the subject is read once, to its end")
		})
	}
}

// A not-join over a streaming subject is a form over the window: its result
// stands in the subject's place and reports no size, its first tuple follows
// one pull of the subject, and the subject is read once, to its end. The odd
// items carry no :item/other, so the first item is the first result and the
// odd items are the result.
func TestNotJoinOverAStreamingSubjectReadsItOnce(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemOther := datalog.NewKeyword(":item/other")
	const subjects = 5
	var datoms []datalog.Datom
	var tuples []Tuple
	var want [][]interface{}
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item%d", i))
		tuples = append(tuples, Tuple{e})
		if i%2 == 0 {
			datoms = append(datoms, datalog.Datom{E: e, A: itemOther, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
			continue
		}
		want = append(want, []interface{}{e})
	}
	q := &query.Query{Where: []query.Clause{&query.NotJoinClause{
		RequiredVars: []query.Symbol{entity},
		Clauses: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: itemOther}, query.Variable{Name: value},
		}}},
	}}}
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, mode.PlannerOptions()).QueryExecutor(nil)
			groups, err := exec.Execute(NewContext(), q, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the form's result stands in the subject's place")
			result := groups[0]
			require.Equal(t, -1, result.Size(), "the result over a streaming subject reports no size")
			it := result.Iterator()
			require.True(t, it.Next(), "the form answers a tuple")
			require.Equal(t, 1, pulls.Count(), "the first result tuple follows one pull of the subject")
			got := [][]interface{}{{it.Tuple()[0]}}
			for it.Next() {
				got = append(got, []interface{}{it.Tuple()[0]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, subjects, pulls.Count(), "the subject is read once, to its end")
			require.ElementsMatch(t, want, got)
		})
	}
}

// A not-join's body is built once per window over the window's keys no
// earlier window ran, each window tuple probes the index the builds keep,
// and the result is the subject's tuples with no match in the subject's
// order; the module events report under the form, and no event of the
// or-fallback family fires. Ten items, each twice, item by item and then
// again: under a cap of eight the fifth window carries no key the earlier
// windows did not, so the body is built four times, over item 1, items 2 and
// 3, items 4 to 7 and items 8 to 10, and the even items, which carry
// :item/other, are dropped in every window, the second pass by the index
// the first pass built.
func TestNotJoinBodyIsBuiltOncePerWindowOverTheKeysNotYetRun(t *testing.T) {
	entity, n, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?n"), datalog.NewSymbol("?value")
	itemOther := datalog.NewKeyword(":item/other")
	const items = 10
	var datoms []datalog.Datom
	var tuples []Tuple
	var want [][]interface{}
	for pass := int64(1); pass <= 2; pass++ {
		for i := 1; i <= items; i++ {
			e := datalog.NewIdentity(fmt.Sprintf("notjoin-item%d", i))
			tuples = append(tuples, Tuple{e, pass})
			if i%2 == 0 {
				if pass == 1 {
					datoms = append(datoms, datalog.Datom{E: e, A: itemOther, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
				}
				continue
			}
			want = append(want, []interface{}{e, pass})
		}
	}
	q := &query.Query{Where: []query.Clause{&query.NotJoinClause{
		RequiredVars: []query.Symbol{entity},
		Clauses: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: itemOther}, query.Variable{Name: value},
		}}},
	}}}
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			var events []annotations.Event
			opts := mode.PlannerOptions()
			opts.ScopeWindowCap = 8
			opts.Handler = mode.Recording(func(e annotations.Event) { events = append(events, e) })
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, opts).QueryExecutor(nil)
			subject := NewStreamingRelation([]query.Symbol{entity, n}, NewMaterializedRelation([]query.Symbol{entity, n}, tuples).Iterator())
			groups, err := exec.Execute(NewContext(), q, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the form's result stands in the subject's place")
			got, err := CollectTuples(groups[0], nil)
			require.NoError(t, err)
			require.Equal(t, want, got, "the subject's tuples with no match, in the subject's order")

			ordinals, tupleCounts, distinctKeys := windowEvents(events)
			require.Equal(t, []int{1, 2, 3, 4, 5}, ordinals, "twenty subject tuples under a cap of eight are read in five windows")
			require.Equal(t, []int{1, 2, 4, 8, 5}, tupleCounts)
			require.Equal(t, tupleCounts, distinctKeys, "no window carries one item twice")
			var builtSizes []int
			for _, e := range events {
				switch e.Name {
				case annotations.ScopeBranchBuild:
					require.Equal(t, "not-join", e.Data[annotations.KeyForm], "the build reports under the form")
					builtSizes = append(builtSizes, e.Data["branch_size"].(int))
				case annotations.ScopeWindow, annotations.ScopeSubjectTuple, annotations.ScopeSubjectExhausted:
					require.Equal(t, "not-join", e.Data[annotations.KeyForm], "the module events report under the form")
				case annotations.OrFallbackIteratorCreated, annotations.OrFallbackBranchNarrowed, annotations.OrFallbackBranchSuccess:
					t.Fatalf("a not-join fires no event of the or-fallback family; got %s", e.Name)
				}
			}
			require.Equal(t, []int{0, 1, 2, 2}, builtSizes, "the body is built over the keys each window carries that no earlier window ran, item 1, items 2 and 3, items 4 to 7 and items 8 to 10, and the fifth window carries none")
		})
	}
}

// A not-join body holding an invocation continues one evaluation across the
// windows: the strata begin and complete, and the rewrite runs, as many
// times under twenty windows as under five, and the items the predicate
// derives are dropped.
func TestNotJoinBodyHoldingAnInvocationIsContinuedAcrossTheWindows(t *testing.T) {
	entity := datalog.NewSymbol("?entity")
	itemOther := datalog.NewKeyword(":item/other")
	const items = 20
	var datoms []datalog.Datom
	var tuples []Tuple
	var want [][]interface{}
	for i := 1; i <= items; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("notjoin-rule-item%d", i))
		tuples = append(tuples, Tuple{e})
		if i%2 == 0 {
			datoms = append(datoms, datalog.Datom{E: e, A: itemOther, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
			continue
		}
		want = append(want, []interface{}{e})
	}
	rs := MustRules(t, `[[(other ?e) [?e :item/other _]]]`)
	form := MustParse(t, `[:find ?entity :in $ % :where (not-join [?entity] (other ?entity))]`).Where
	type counts struct{ begins, completes, rewrites, windows int }
	run := func(t *testing.T, mode OptimizerMode, opts planner.PlannerOptions) counts {
		var c counts
		opts.Handler = mode.Recording(func(e annotations.Event) {
			switch e.Name {
			case annotations.RulesStratumBegin:
				c.begins++
			case annotations.RulesStratumComplete:
				c.completes++
			case annotations.RulesMagicSets:
				c.rewrites++
			case annotations.ScopeWindow:
				c.windows++
			}
		})
		exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, opts).QueryExecutor(nil)
		subject := NewStreamingRelation([]query.Symbol{entity}, NewMaterializedRelation([]query.Symbol{entity}, tuples).Iterator())
		groups, err := exec.Execute(NewContext().WithRules(rs), &query.Query{Where: form}, []Relation{subject})
		require.NoError(t, err)
		require.Len(t, groups, 1, "the form's result stands in the subject's place")
		got, err := CollectTuples(groups[0], nil)
		require.NoError(t, err)
		require.Equal(t, want, got, "the items the predicate derives are dropped, the rest kept in the subject's order")
		return c
	}
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			single := mode.PlannerOptions()
			single.ScopeWindowCap = 1
			twenty := run(t, mode, single)
			require.Equal(t, items, twenty.windows, "under a cap of one each subject tuple is a window")
			require.GreaterOrEqual(t, twenty.begins, 1, "the body's invocation evaluates its predicate")

			windowed := mode.PlannerOptions()
			windowed.ScopeWindowCap = 8
			five := run(t, mode, windowed)
			require.Equal(t, 5, five.windows, "twenty subject tuples under a cap of eight span five windows")
			require.Equal(t, twenty.begins, five.begins, "each stratum begins once, whatever the windows")
			require.Equal(t, twenty.completes, five.completes, "each stratum completes once, at the last window")
			require.Equal(t, twenty.rewrites, five.rewrites, "the invocation is rewritten once per form execution")
		})
	}
}

// A subquery over a streaming subject is a form over the window: its result
// stands in the subject's place and reports no size, its first tuple follows
// one pull of the subject, and the subject is read once, to its end. Each
// item is answered beside its :item/value.
func TestSubqueryOverAStreamingSubjectReadsItOnce(t *testing.T) {
	entity, value, item := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?item")
	itemValue := datalog.NewKeyword(":item/value")
	const subjects = 5
	var datoms []datalog.Datom
	var tuples []Tuple
	var want [][]interface{}
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		tuples = append(tuples, Tuple{e})
		want = append(want, []interface{}{e, int64(i)})
	}
	q := &query.Query{Where: []query.Clause{&query.SubqueryPattern{
		Query: &query.Query{
			Find: []query.FindElement{query.FindVariable{Symbol: value}},
			In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.ScalarInput{Symbol: item}},
			Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: item}, query.Constant{Value: itemValue}, query.Variable{Name: value},
			}}},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Variable{Name: entity}},
		Binding: query.RelationBinding{Variables: []query.Symbol{value}},
	}}}
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, mode.PlannerOptions()).QueryExecutor(nil)
			groups, err := exec.Execute(NewContext(), q, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the subquery's result stands in the subject's place")
			result := groups[0]
			require.Equal(t, -1, result.Size(), "the result over a streaming subject reports no size")
			syms := result.Symbols()
			ei, vi := query.SymbolIndex(syms, entity), query.SymbolIndex(syms, value)
			require.True(t, ei >= 0 && vi >= 0, "the result carries the subject's symbol and the binding; got %v", syms)
			it := result.Iterator()
			require.True(t, it.Next(), "the subquery answers a tuple")
			require.Equal(t, 1, pulls.Count(), "the first result tuple follows one pull of the subject")
			got := [][]interface{}{{it.Tuple()[ei], it.Tuple()[vi]}}
			for it.Next() {
				got = append(got, []interface{}{it.Tuple()[ei], it.Tuple()[vi]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, subjects, pulls.Count(), "the subject is read once, to its end")
			require.ElementsMatch(t, want, got)
		})
	}
}

// pullCountingDatabase answers every match through its facts database, and
// reads the matches of one attribute through a counting iterator: the group
// the clause matching that attribute produces.
type pullCountingDatabase struct {
	*storage.Database
	attr  datalog.Keyword
	pulls *CountingIterator
}

// Match answers q through the database, counting the pulls of the matches
// of attr.
func (d *pullCountingDatabase) Match(q *query.Query, bindings Relations) (Relation, error) {
	rel, err := d.Database.Match(q, bindings)
	if err != nil {
		return nil, err
	}
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	if a, ok := pattern.GetA().(query.Constant); !ok || a.Value != d.attr {
		return rel, nil
	}
	d.pulls = NewCountingIterator(rel.Iterator())
	return NewStreamingRelationWithProperties(rel.Symbols(), d.pulls, rel.Options(), rel.Properties()), nil
}

// A data pattern sharing a variable with a group the scope produces is a form
// over the window: the plan reads the second pattern in windows of the first
// pattern's matches, the pattern is one body under the header of the
// variables it shares with the group, its result stands in the group's place
// and reports no size, its first tuple follows one pull of the group, and the
// group is read once, to its end. Each item is answered beside its
// :item/value.
func TestPatternOverAStreamingGroupReadsItOnce(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemKind, itemValue := datalog.NewKeyword(":item/kind"), datalog.NewKeyword(":item/value")
	const subjects = 5
	var datoms []datalog.Datom
	var want [][]interface{}
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("bound%d", i))
		datoms = append(datoms,
			datalog.Datom{E: e, A: itemKind, V: "thing", Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}},
			datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(subjects + i), ReplicaID: 1}})
		want = append(want, []interface{}{e, int64(i)})
	}
	q, err := query.Prepare(MustParse(t, `[:find ?entity ?value :where [?entity :item/kind "thing"] [?entity :item/value ?value]]`), nil)
	require.NoError(t, err)
	// On the legs without attribute-fetch fusion the second pattern runs over
	// the group through the window.
	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return !m.Fusion() }) {
		t.Run(mode.Name(), func(t *testing.T) {
			db := &pullCountingDatabase{Database: factsDatabase(t, datoms)}
			executor := NewExecutorWithOptions(db, nil, mode.PlannerOptions())
			plan, err := executor.GetPlanner().PlanQuery(q, query.Bindings{}, nil)
			require.NoError(t, err)
			require.Len(t, plan.Phases, 1, "the two patterns join in one phase")
			where := plan.Phases[0].Query.Where
			require.Len(t, where, 2, "the phase holds the two patterns")
			first, second := where[0].(*query.DataPattern), where[1]
			require.Contains(t, plan.Windows, second, "the plan reads the second pattern in windows of the group the first produces")
			db.attr = first.GetA().(query.Constant).Value.(datalog.Keyword)
			groups, err := executor.QueryExecutor(plan).Execute(NewContext(), plan.Phases[0].Query, nil)
			require.NoError(t, err)
			require.Len(t, groups, 1, "the pattern's result stands in the group's place")
			require.NotNil(t, db.pulls, "the first pattern's matches are read through the counting iterator")
			pulls := db.pulls
			result := groups[0]
			require.Equal(t, -1, result.Size(), "the result over a streaming group reports no size")
			syms := result.Symbols()
			ei, vi := query.SymbolIndex(syms, entity), query.SymbolIndex(syms, value)
			require.True(t, ei >= 0 && vi >= 0, "the result carries the group's symbol and the pattern's; got %v", syms)
			it := result.Iterator()
			require.True(t, it.Next(), "the pattern answers a tuple")
			require.Equal(t, 1, pulls.Count(), "the first result tuple follows one pull of the group")
			got := [][]interface{}{{it.Tuple()[ei], it.Tuple()[vi]}}
			for it.Next() {
				got = append(got, []interface{}{it.Tuple()[ei], it.Tuple()[vi]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, subjects, pulls.Count(), "the group is read once, to its end")
			require.ElementsMatch(t, want, got)
		})
	}
}

// A pattern after a join of two groups takes the window: the join result is
// a group the scope produces, so the plan windows the pattern, which reads
// the result once, in windows, under the form "pattern", and answers the
// join of the group with the pattern's matches
// (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md, T6 and L7). The input
// group over ?e and ?x is joined with the first pattern's matches at the
// collapse, and the second pattern runs over that join result. Every item
// carries :item/value, and the even items carry :item/other, so the even
// items are answered beside their payload and both values.
func TestAPatternOverAJoinResultTakesTheWindow(t *testing.T) {
	entity, x := datalog.NewSymbol("?e"), datalog.NewSymbol("?x")
	v, w := datalog.NewSymbol("?v"), datalog.NewSymbol("?w")
	itemValue, itemOther := datalog.NewKeyword(":item/value"), datalog.NewKeyword(":item/other")
	const items = 6
	var datoms []datalog.Datom
	var tuples []Tuple
	var want [][]interface{}
	for i := 1; i <= items; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("joined-item%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		tuples = append(tuples, Tuple{e, int64(100 + i)})
		if i%2 == 0 {
			datoms = append(datoms, datalog.Datom{E: e, A: itemOther, V: int64(10 * i), Tx: datalog.ElementID{Lamport: uint64(items + i), ReplicaID: 1}})
			want = append(want, []interface{}{e, int64(100 + i), int64(i), int64(10 * i)})
		}
	}
	q, err := query.Prepare(MustParse(t, `[:find ?e ?x ?v ?w :in $ [[?e ?x]] :where [?e :item/value ?v] [?e :item/other ?w]]`), nil)
	require.NoError(t, err)
	// On the legs without attribute-fetch fusion the second pattern runs over
	// the join result through the window.
	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return !m.Fusion() }) {
		t.Run(mode.Name(), func(t *testing.T) {
			var forms []string
			opts := mode.PlannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.ScopeWindow {
					forms = append(forms, e.Data[annotations.KeyForm].(string))
				}
			}
			executor := NewExecutorWithOptions(factsDatabase(t, datoms), nil, opts)
			plan, err := executor.GetPlanner().PlanQuery(q, query.Bindings{Symbols: map[query.Symbol]bool{entity: true, x: true}}, nil)
			require.NoError(t, err)
			require.Len(t, plan.Phases, 1, "the group and the two patterns join in one phase")
			group := NewMaterializedRelation([]query.Symbol{entity, x}, tuples)
			groups, err := executor.QueryExecutor(plan).Execute(NewContext(), plan.Phases[0].Query, []Relation{group})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the second pattern's result stands in the join result's place")
			got, err := CollectTuples(groups[0], nil)
			require.NoError(t, err)
			syms := groups[0].Symbols()
			ei, xi, vi, wi := query.SymbolIndex(syms, entity), query.SymbolIndex(syms, x), query.SymbolIndex(syms, v), query.SymbolIndex(syms, w)
			require.True(t, ei >= 0 && xi >= 0 && vi >= 0 && wi >= 0, "the result carries the group's symbols and both patterns'; got %v", syms)
			var answered [][]interface{}
			for _, tuple := range got {
				answered = append(answered, []interface{}{tuple[ei], tuple[xi], tuple[vi], tuple[wi]})
			}
			require.ElementsMatch(t, want, answered, "the join of the group with both patterns' matches")
			require.NotEmpty(t, forms, "the pattern over the join result reads it through the window")
			for _, form := range forms {
				require.Equal(t, "pattern", form, "the window reports under the pattern's form")
			}
		})
	}
}

// A bound invocation at the top level is a form over the window: the plan
// reads the invocation in windows of the group a pattern before it produces,
// the invocation is one body under the header of its bound variables as the
// required run and its free variables as the outputs, its fixpoint seeded by
// each window's keys and continued across the windows. Its result stands in
// the group's place and reports no size, its first tuple follows one pull of
// the group, the group is read once, to its end, and the rewrite and each
// stratum run as many times under six windows as under three. Over a chain
// of six pages each page is answered beside every page after it; with both
// arguments bound the invocation is a verdict per key, keeping the group's
// tuples that the closure holds and the group's keys.
func TestBoundInvocationOverAStreamingGroupReadsItOnce(t *testing.T) {
	a, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?c")
	const pages = 6
	pageCheck := datalog.NewKeyword(":page/check")
	datoms := ChainDatoms(pages)
	for i, pair := range [][2]int{{1, 3}, {2, 1}, {3, 5}, {4, 6}} {
		datoms = append(datoms, datalog.Datom{E: Page(pair[0]), A: pageCheck, V: Page(pair[1]), Tx: datalog.ElementID{Lamport: uint64(1000 + i), ReplicaID: 1}})
	}
	rs := MustRules(t, FollowsRulesText)
	type counts struct{ begins, rewrites, windows int }
	// run plans text under the rule set, its pattern producing the group of
	// produced's matches and its invocation read in windows of that group,
	// and runs the plan's phase, counting the pulls of the group.
	run := func(t *testing.T, mode OptimizerMode, opts planner.PlannerOptions, text string, produced datalog.Keyword) (Relation, *CountingIterator, *counts) {
		c := &counts{}
		opts.Handler = mode.Recording(func(e annotations.Event) {
			switch e.Name {
			case annotations.RulesStratumBegin:
				c.begins++
			case annotations.RulesMagicSets:
				c.rewrites++
			case annotations.ScopeWindow:
				c.windows++
			}
		})
		db := &pullCountingDatabase{Database: factsDatabase(t, datoms), attr: produced}
		executor := NewExecutorWithOptions(db, nil, opts)
		q, err := query.Prepare(MustParse(t, text), rs)
		require.NoError(t, err)
		plan, err := executor.GetPlanner().PlanQuery(q, query.Bindings{Rules: rs}, nil)
		require.NoError(t, err)
		require.Len(t, plan.Phases, 1, "the pattern and the invocation join in one phase")
		var invocation query.Clause
		for _, clause := range plan.Phases[0].Query.Where {
			if _, ok := clause.(*query.RuleInvocation); ok {
				invocation = clause
			}
		}
		require.Contains(t, plan.Windows, invocation, "the plan reads the invocation in windows of the group the pattern produces")
		groups, err := executor.QueryExecutor(plan).Execute(NewContext().WithRules(rs), plan.Phases[0].Query, nil)
		require.NoError(t, err)
		require.Len(t, groups, 1, "the invocation's result stands in the group's place")
		require.NotNil(t, db.pulls, "the pattern's matches are read through the counting iterator")
		return groups[0], db.pulls, c
	}
	const withAnOutput = `[:find ?a ?c :in $ % :where [?a :page/kind _] (follows ?a ?c)]`
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name()+"/with an output", func(t *testing.T) {
			opts := mode.PlannerOptions()
			opts.ScopeWindowCap = 8
			result, pulls, three := run(t, mode, opts, withAnOutput, PageKind)
			require.Equal(t, -1, result.Size(), "the result over a streaming group reports no size")
			syms := result.Symbols()
			ai, ci := query.SymbolIndex(syms, a), query.SymbolIndex(syms, c)
			require.True(t, ai >= 0 && ci >= 0, "the result carries the group's symbol and the free variable; got %v", syms)
			it := result.Iterator()
			require.True(t, it.Next(), "the invocation answers a tuple")
			require.Equal(t, 1, pulls.Count(), "the first result tuple follows one pull of the group")
			pairs := map[[2]datalog.Identity]bool{}
			record := func(tuple Tuple) {
				pairs[[2]datalog.Identity{tuple[ai].(datalog.Identity), tuple[ci].(datalog.Identity)}] = true
			}
			record(it.Tuple())
			for it.Next() {
				record(it.Tuple())
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, pages, pulls.Count(), "the group is read once, to its end")
			RequireSamePairs(t, pairs, ChainClosure(pages))
			require.Equal(t, 3, three.windows, "six subject tuples under a cap of eight are read in three windows")
			require.Equal(t, 1, three.rewrites, "the invocation is rewritten once")

			single := mode.PlannerOptions()
			single.ScopeWindowCap = 1
			result, _, six := run(t, mode, single, withAnOutput, PageKind)
			_, err := CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, pages, six.windows, "under a cap of one each subject tuple is a window")
			require.Equal(t, three.rewrites, six.rewrites, "one rewrite whatever the window count")
			require.GreaterOrEqual(t, three.begins, 1, "the invocation evaluates its predicate")
			require.Equal(t, three.begins, six.begins, "each stratum begins once, continued across the windows")
		})
		t.Run(mode.Name()+"/as a verdict per key", func(t *testing.T) {
			opts := mode.PlannerOptions()
			opts.ScopeWindowCap = 8
			result, pulls, _ := run(t, mode, opts, `[:find ?a ?c :in $ % :where [?a :page/check ?c] (follows ?a ?c)]`, pageCheck)
			require.Equal(t, -1, result.Size(), "the result over a streaming group reports no size")
			require.ElementsMatch(t, []query.Symbol{a, c}, result.Symbols(), "a verdict adds no symbol")
			require.NotEmpty(t, result.Properties().Keys, "a verdict per key keeps the subject's keys")
			got, err := CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, 4, pulls.Count(), "the group, the four checked pairs, is read once, to its end")
			syms := result.Symbols()
			ai, ci := query.SymbolIndex(syms, a), query.SymbolIndex(syms, c)
			var kept [][]interface{}
			for _, tuple := range got {
				kept = append(kept, []interface{}{tuple[ai], tuple[ci]})
			}
			require.ElementsMatch(t, [][]interface{}{{Page(1), Page(3)}, {Page(3), Page(5)}, {Page(4), Page(6)}}, kept, "the pairs the closure holds are kept and page 2 to page 1 is dropped")
		})
	}
}

// A form whose subject spans two streaming groups joins them with nothing
// materialized: one group's tuples are held in the join index and the other
// streams through it, so the result reports no size, its first tuple follows
// the held group read whole and one pull of the streamed group, and each
// group is read once, to its end. Each item is answered beside its own value
// and its tag's.
func TestFormOverTwoStreamingGroupsReadsEachOnce(t *testing.T) {
	entity, tag, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?tag"), datalog.NewSymbol("?value")
	itemValue, tagValue := datalog.NewKeyword(":item/value"), datalog.NewKeyword(":tag/value")
	const subjects = 5
	var datoms []datalog.Datom
	var items, tagged []Tuple
	var want [][]interface{}
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item%d", i))
		g := datalog.NewIdentity(fmt.Sprintf("tag%d", i))
		datoms = append(datoms,
			datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(2 * i), ReplicaID: 1}},
			datalog.Datom{E: g, A: tagValue, V: int64(10 * i), Tx: datalog.ElementID{Lamport: uint64(2*i + 1), ReplicaID: 1}})
		items = append(items, Tuple{e})
		tagged = append(tagged, Tuple{e, g})
		want = append(want, []interface{}{e, g, int64(i)}, []interface{}{e, g, int64(10 * i)})
	}
	q := &query.Query{Where: []query.Clause{&query.OrJoinClause{
		RequiredVars: []query.Symbol{entity, tag},
		OutputVars:   []query.Symbol{value},
		Branches: [][]query.Clause{
			{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: entity}, query.Constant{Value: itemValue}, query.Variable{Name: value}}}},
			{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: tag}, query.Constant{Value: tagValue}, query.Variable{Name: value}}}},
		},
	}}}
	// The symmetric hash join holds neither group whole and pulls both as the
	// result is consumed; the pulls asserted are the standard hash join's, so
	// the legs are those without the symmetric join.
	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return !m.Symmetric() }) {
		t.Run(mode.Name(), func(t *testing.T) {
			itemPulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{entity}, items).Iterator())
			taggedPulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{entity, tag}, tagged).Iterator())
			first := NewStreamingRelation([]query.Symbol{entity}, itemPulls)
			second := NewStreamingRelation([]query.Symbol{entity, tag}, taggedPulls)
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, mode.PlannerOptions()).QueryExecutor(nil)
			groups, err := exec.Execute(NewContext(), q, []Relation{first, second})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the form's result stands in the place of both groups")
			result := groups[0]
			require.Equal(t, -1, result.Size(), "the result over a streaming subject reports no size")
			syms := result.Symbols()
			ei, gi, vi := query.SymbolIndex(syms, entity), query.SymbolIndex(syms, tag), query.SymbolIndex(syms, value)
			require.True(t, ei >= 0 && gi >= 0 && vi >= 0, "the result carries both groups' symbols and the output; got %v", syms)
			it := result.Iterator()
			require.True(t, it.Next(), "the form answers a tuple")
			require.Equal(t, subjects+1, itemPulls.Count()+taggedPulls.Count(), "the first result tuple follows one group read whole into the join index and one pull of the other")
			got := [][]interface{}{{it.Tuple()[ei], it.Tuple()[gi], it.Tuple()[vi]}}
			for it.Next() {
				got = append(got, []interface{}{it.Tuple()[ei], it.Tuple()[gi], it.Tuple()[vi]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, subjects, itemPulls.Count(), "the first group is read once, to its end")
			require.Equal(t, subjects, taggedPulls.Count(), "the second group is read once, to its end")
			require.ElementsMatch(t, want, got)
		})
	}
}

// A form whose subject spans two streaming groups sharing no symbol has
// their product as its subject, joined under no key with nothing
// materialized: the later group's tuples are held under one key and every
// tuple of the first streams through them, so the result reports no size,
// its first tuple follows the later group read whole and one pull of the
// first, and each group is read once, to its end. Every item is answered
// beside every tag, with the item's value and the tag's.
func TestFormOverTwoStreamingGroupsSharingNoSymbolReadsEachOnce(t *testing.T) {
	entity, tag, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?tag"), datalog.NewSymbol("?value")
	itemValue, tagValue := datalog.NewKeyword(":item/value"), datalog.NewKeyword(":tag/value")
	const items, tags = 3, 2
	var datoms []datalog.Datom
	var itemTuples, tagTuples []Tuple
	var want [][]interface{}
	for i := 1; i <= items; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		itemTuples = append(itemTuples, Tuple{e})
	}
	for j := 1; j <= tags; j++ {
		g := datalog.NewIdentity(fmt.Sprintf("tag%d", j))
		datoms = append(datoms, datalog.Datom{E: g, A: tagValue, V: int64(10 * j), Tx: datalog.ElementID{Lamport: uint64(items + j), ReplicaID: 1}})
		tagTuples = append(tagTuples, Tuple{g})
	}
	for i := 1; i <= items; i++ {
		for j := 1; j <= tags; j++ {
			e, g := itemTuples[i-1][0], tagTuples[j-1][0]
			want = append(want, []interface{}{e, g, int64(i)}, []interface{}{e, g, int64(10 * j)})
		}
	}
	q := &query.Query{Where: []query.Clause{&query.OrJoinClause{
		RequiredVars: []query.Symbol{entity, tag},
		OutputVars:   []query.Symbol{value},
		Branches: [][]query.Clause{
			{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: entity}, query.Constant{Value: itemValue}, query.Variable{Name: value}}}},
			{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: tag}, query.Constant{Value: tagValue}, query.Variable{Name: value}}}},
		},
	}}}
	// The symmetric hash join holds neither group whole and pulls both as the
	// result is consumed; the pulls asserted are the standard hash join's, so
	// the legs are those without the symmetric join.
	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return !m.Symmetric() }) {
		t.Run(mode.Name(), func(t *testing.T) {
			itemPulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{entity}, itemTuples).Iterator())
			tagPulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{tag}, tagTuples).Iterator())
			first := NewStreamingRelation([]query.Symbol{entity}, itemPulls)
			second := NewStreamingRelation([]query.Symbol{tag}, tagPulls)
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, mode.PlannerOptions()).QueryExecutor(nil)
			groups, err := exec.Execute(NewContext(), q, []Relation{first, second})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the form's result stands in the place of both groups")
			result := groups[0]
			require.Equal(t, -1, result.Size(), "the result over a streaming subject reports no size")
			syms := result.Symbols()
			ei, gi, vi := query.SymbolIndex(syms, entity), query.SymbolIndex(syms, tag), query.SymbolIndex(syms, value)
			require.True(t, ei >= 0 && gi >= 0 && vi >= 0, "the result carries both groups' symbols and the output; got %v", syms)
			it := result.Iterator()
			require.True(t, it.Next(), "the form answers a tuple")
			require.Equal(t, tags, tagPulls.Count(), "the first result tuple follows the later group held whole under one key")
			require.Equal(t, 1, itemPulls.Count(), "and one pull of the first group, which streams")
			got := [][]interface{}{{it.Tuple()[ei], it.Tuple()[gi], it.Tuple()[vi]}}
			for it.Next() {
				got = append(got, []interface{}{it.Tuple()[ei], it.Tuple()[gi], it.Tuple()[vi]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, items, itemPulls.Count(), "the item group is read once, to its end")
			require.Equal(t, tags, tagPulls.Count(), "the tag group is read once, to its end")
			require.ElementsMatch(t, want, got)
		})
	}
}

// Over three streaming groups sharing no symbol the group held at each join
// is the one joined, never the join so far: the first group streams, pulled
// once before the result's first tuple, and each later group is read whole
// into its join's index, so the fold holds the later groups and never their
// product. Every item is answered beside every tag and every shelf, with
// each one's value.
func TestFormOverThreeStreamingGroupsHoldsEachLaterGroup(t *testing.T) {
	entity, tag, shelf, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?tag"), datalog.NewSymbol("?shelf"), datalog.NewSymbol("?value")
	itemValue, tagValue, shelfValue := datalog.NewKeyword(":item/value"), datalog.NewKeyword(":tag/value"), datalog.NewKeyword(":shelf/value")
	const items, tags, shelves = 2, 3, 4
	var datoms []datalog.Datom
	var itemTuples, tagTuples, shelfTuples []Tuple
	for i := 1; i <= items; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("item%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		itemTuples = append(itemTuples, Tuple{e})
	}
	for j := 1; j <= tags; j++ {
		g := datalog.NewIdentity(fmt.Sprintf("tag%d", j))
		datoms = append(datoms, datalog.Datom{E: g, A: tagValue, V: int64(10 * j), Tx: datalog.ElementID{Lamport: uint64(items + j), ReplicaID: 1}})
		tagTuples = append(tagTuples, Tuple{g})
	}
	for k := 1; k <= shelves; k++ {
		s := datalog.NewIdentity(fmt.Sprintf("shelf%d", k))
		datoms = append(datoms, datalog.Datom{E: s, A: shelfValue, V: int64(100 * k), Tx: datalog.ElementID{Lamport: uint64(items + tags + k), ReplicaID: 1}})
		shelfTuples = append(shelfTuples, Tuple{s})
	}
	var want [][]interface{}
	for i := 1; i <= items; i++ {
		for j := 1; j <= tags; j++ {
			for k := 1; k <= shelves; k++ {
				e, g, s := itemTuples[i-1][0], tagTuples[j-1][0], shelfTuples[k-1][0]
				want = append(want,
					[]interface{}{e, g, s, int64(i)},
					[]interface{}{e, g, s, int64(10 * j)},
					[]interface{}{e, g, s, int64(100 * k)})
			}
		}
	}
	q := &query.Query{Where: []query.Clause{&query.OrJoinClause{
		RequiredVars: []query.Symbol{entity, tag, shelf},
		OutputVars:   []query.Symbol{value},
		Branches: [][]query.Clause{
			{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: entity}, query.Constant{Value: itemValue}, query.Variable{Name: value}}}},
			{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: tag}, query.Constant{Value: tagValue}, query.Variable{Name: value}}}},
			{&query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: shelf}, query.Constant{Value: shelfValue}, query.Variable{Name: value}}}},
		},
	}}}
	// The symmetric hash join holds neither group whole and pulls both as the
	// result is consumed; the pulls asserted are the standard hash join's, so
	// the legs are those without the symmetric join.
	for _, mode := range OptimizerModesWhere(func(m OptimizerMode) bool { return !m.Symmetric() }) {
		t.Run(mode.Name(), func(t *testing.T) {
			itemPulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{entity}, itemTuples).Iterator())
			tagPulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{tag}, tagTuples).Iterator())
			shelfPulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{shelf}, shelfTuples).Iterator())
			first := NewStreamingRelation([]query.Symbol{entity}, itemPulls)
			second := NewStreamingRelation([]query.Symbol{tag}, tagPulls)
			third := NewStreamingRelation([]query.Symbol{shelf}, shelfPulls)
			exec := NewExecutorWithOptions(factsDatabase(t, datoms), nil, mode.PlannerOptions()).QueryExecutor(nil)
			groups, err := exec.Execute(NewContext(), q, []Relation{first, second, third})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the form's result stands in the place of the three groups")
			result := groups[0]
			require.Equal(t, -1, result.Size(), "the result over a streaming subject reports no size")
			syms := result.Symbols()
			ei, gi, si, vi := query.SymbolIndex(syms, entity), query.SymbolIndex(syms, tag), query.SymbolIndex(syms, shelf), query.SymbolIndex(syms, value)
			require.True(t, ei >= 0 && gi >= 0 && si >= 0 && vi >= 0, "the result carries the three groups' symbols and the output; got %v", syms)
			it := result.Iterator()
			require.True(t, it.Next(), "the form answers a tuple")
			require.Equal(t, 1, itemPulls.Count(), "the first group streams, pulled once before the first tuple")
			require.Equal(t, tags, tagPulls.Count(), "the second group is held whole in its join's index")
			require.Equal(t, shelves, shelfPulls.Count(), "the third group is held whole in its join's index")
			got := [][]interface{}{{it.Tuple()[ei], it.Tuple()[gi], it.Tuple()[si], it.Tuple()[vi]}}
			for it.Next() {
				got = append(got, []interface{}{it.Tuple()[ei], it.Tuple()[gi], it.Tuple()[si], it.Tuple()[vi]})
			}
			require.NoError(t, it.Error())
			require.NoError(t, it.Close())
			require.Equal(t, items, itemPulls.Count(), "the first group is read once, to its end")
			require.Equal(t, tags, tagPulls.Count(), "the second group is read once")
			require.Equal(t, shelves, shelfPulls.Count(), "the third group is read once")
			require.ElementsMatch(t, want, got)
		})
	}
}
