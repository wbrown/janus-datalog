package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The contracts of docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §7 that an
// executor over an in-memory matcher pins, each the subject of one test.

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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, mode.plannerOptions()).queryExecutor()
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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, mode.plannerOptions())
			plan, err := exec.planner.PlanQuery(finding(itemValue), query.Bindings{}, nil)
			require.NoError(t, err)
			result, err := exec.ExecuteRealized(NewContext(), plan, nil)
			require.NoError(t, err)
			require.Empty(t, result.Symbols(), "the unit relation carries no symbol")
			tuples, err := CollectTuples(result, nil)
			require.NoError(t, err)
			require.Len(t, tuples, 1, "the unit relation carries one tuple")

			plan, err = exec.planner.PlanQuery(finding(itemOther), query.Bindings{}, nil)
			require.NoError(t, err)
			result, err = exec.ExecuteRealized(NewContext(), plan, nil)
			require.NoError(t, err)
			tuples, err = CollectTuples(result, nil)
			require.NoError(t, err)
			require.Empty(t, tuples, "a body with no match yields nothing")
		})
	}
}

// A failure of a not-join body's scan is the query's error, never an
// anti-join over a body with no result that keeps every subject tuple. The
// anti-join reads the body whole and carries the failure on the result it
// returns, so the error surfaces when the result is driven.
func TestNotJoinBodyScanFailureIsTheQuerysError(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	failing := datalog.NewKeyword(":item/failing")
	q := &query.Query{Where: []query.Clause{&query.NotJoinClause{
		RequiredVars: []query.Symbol{entity},
		Clauses: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: failing}, query.Variable{Name: value},
		}}},
	}}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			matcher := &failingScanMatcher{
				failAttr: failing,
				dataRel:  NewMaterializedRelation([]query.Symbol{entity, value}, nil),
			}
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions()).queryExecutor()
			outer := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{datalog.NewIdentity("body-error")}})
			groups, err := exec.Execute(NewContext(), q, []Relation{outer})
			require.NoError(t, err, "the anti-join carries the body's failure on its result")
			require.Len(t, groups, 1, "the anti-join's result stands in the subject's place")
			require.ErrorIs(t, driveErr(groups[0]), errInjectedIterator)
		})
	}
}

// orClauseEvents returns, in order, the events of the or-clause family a
// handler saw: the begin, the union, the branch completions and the
// completion, which §4.9 has the or-join handler emit and the fallback
// handler never emit.
func orClauseEvents(events []annotations.Event) []annotations.Event {
	var result []annotations.Event
	for _, e := range events {
		switch e.Name {
		case annotations.OrClauseBegin, annotations.OrClauseUnion, annotations.OrClauseBranchComplete, annotations.OrClauseComplete:
			result = append(result, e)
		}
	}
	return result
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
	for _, mode := range optimizerModes {
		for _, form := range forms {
			t.Run(mode.name+"/"+form.name, func(t *testing.T) {
				var events []annotations.Event
				opts := mode.plannerOptions()
				opts.Handler = func(e annotations.Event) { events = append(events, e) }
				exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, opts).queryExecutor()
				groups, err := exec.Execute(NewContext(), &query.Query{Where: []query.Clause{form.clause}}, form.inputs)
				require.NoError(t, err)
				var tuples [][]interface{}
				for _, group := range groups {
					collected, err := CollectTuples(group, nil)
					require.NoError(t, err)
					tuples = append(tuples, collected...)
				}
				require.ElementsMatch(t, [][]interface{}{{e1, int64(1)}, {e2, int64(2)}}, tuples)

				reported := orClauseEvents(events)
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

// A branch failure on the union route completes the or-join with success
// false and the error, and the error is the query's: the union route reads
// each branch whole, so the failure is Execute's own and no result stands.
func TestFormFailureCompletesWithSuccessFalse(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	failing := datalog.NewKeyword(":item/failing")
	q := &query.Query{Where: []query.Clause{&query.OrJoinClause{
		OutputVars: []query.Symbol{entity, value},
		Branches: [][]query.Clause{{&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: entity}, query.Constant{Value: failing}, query.Variable{Name: value},
		}}}},
	}}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) { events = append(events, e) }
			matcher := &failingScanMatcher{
				failAttr: failing,
				dataRel:  NewMaterializedRelation([]query.Symbol{entity, value}, nil),
			}
			exec := NewExecutorWithOptions(matcher, nil, opts).queryExecutor()
			_, err := exec.Execute(NewContext(), q, nil)
			require.ErrorIs(t, err, errInjectedIterator, "the union route reads the branch whole, so its failure is Execute's")
			reported := orClauseEvents(events)
			require.NotEmpty(t, reported, "the or-join reports its execution")
			complete := reported[len(reported)-1]
			require.Equal(t, annotations.OrClauseComplete, complete.Name)
			require.Equal(t, false, complete.Data[annotations.KeySuccess])
			require.NotEmpty(t, complete.Data["error"], "the completion carries the branch's error")
		})
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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var begins, completes int
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				switch e.Name {
				case annotations.PhaseBegin:
					begins++
				case annotations.PhaseComplete:
					completes++
				}
			}
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, opts)
			prepared, err := query.Prepare(q, nil)
			require.NoError(t, err)
			plan, err := exec.planner.PlanQuery(prepared, query.Bindings{}, nil)
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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, opts).queryExecutor()
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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, mode.plannerOptions()).queryExecutor()
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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			var events []annotations.Event
			opts := mode.plannerOptions()
			opts.ScopeWindowCap = 8
			opts.Handler = func(e annotations.Event) { events = append(events, e) }
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, opts).queryExecutor()
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
	rs := mustRules(t, `[[(other ?e) [?e :item/other _]]]`)
	form := mustParse(t, `[:find ?entity :in $ % :where (not-join [?entity] (other ?entity))]`).Where
	type counts struct{ begins, completes, rewrites, windows int }
	run := func(t *testing.T, opts planner.PlannerOptions) counts {
		var c counts
		opts.Handler = func(e annotations.Event) {
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
		}
		exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, opts).queryExecutor()
		subject := NewStreamingRelation([]query.Symbol{entity}, NewMaterializedRelation([]query.Symbol{entity}, tuples).Iterator())
		groups, err := exec.Execute(NewContext().WithRules(rs), &query.Query{Where: form}, []Relation{subject})
		require.NoError(t, err)
		require.Len(t, groups, 1, "the form's result stands in the subject's place")
		got, err := CollectTuples(groups[0], nil)
		require.NoError(t, err)
		require.Equal(t, want, got, "the items the predicate derives are dropped, the rest kept in the subject's order")
		return c
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			single := mode.plannerOptions()
			single.ScopeWindowCap = 1
			twenty := run(t, single)
			require.Equal(t, items, twenty.windows, "under a cap of one each subject tuple is a window")
			require.GreaterOrEqual(t, twenty.begins, 1, "the body's invocation evaluates its predicate")

			windowed := mode.plannerOptions()
			windowed.ScopeWindowCap = 8
			five := run(t, windowed)
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
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, mode.plannerOptions()).queryExecutor()
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

// A data pattern over a streaming group is a form over the window: the
// pattern is one body under the header of the variables it shares with the
// group, its result stands in the group's place and reports no size, its
// first tuple follows one pull of the group, and the group is read once, to
// its end. Each item is answered beside its :item/value.
func TestPatternOverAStreamingGroupReadsItOnce(t *testing.T) {
	entity, value := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value")
	itemValue := datalog.NewKeyword(":item/value")
	const subjects = 5
	var datoms []datalog.Datom
	var tuples []Tuple
	var want [][]interface{}
	for i := 1; i <= subjects; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("bound%d", i))
		datoms = append(datoms, datalog.Datom{E: e, A: itemValue, V: int64(i), Tx: datalog.ElementID{Lamport: uint64(i), ReplicaID: 1}})
		tuples = append(tuples, Tuple{e})
		want = append(want, []interface{}{e, int64(i)})
	}
	q := &query.Query{Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: entity}, query.Constant{Value: itemValue}, query.Variable{Name: value},
	}}}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			base := NewMaterializedRelation([]query.Symbol{entity}, tuples)
			pulls := NewCountingIterator(base.Iterator())
			subject := NewStreamingRelation([]query.Symbol{entity}, pulls)
			exec := NewExecutorWithOptions(NewMemoryPatternMatcher(datoms), nil, mode.plannerOptions()).queryExecutor()
			groups, err := exec.Execute(NewContext(), q, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the pattern's result stands in the group's place")
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

// A failure of a subquery body's scan is the query's error, surfacing when
// the result is driven: the form's result stands in the subject's place and
// carries the failure, as a not-join's does.
func TestSubqueryBodyScanFailureSurfacesWhenTheResultIsDriven(t *testing.T) {
	entity, value, item := datalog.NewSymbol("?entity"), datalog.NewSymbol("?value"), datalog.NewSymbol("?item")
	failing := datalog.NewKeyword(":item/failing")
	q := &query.Query{Where: []query.Clause{&query.SubqueryPattern{
		Query: &query.Query{
			Find: []query.FindElement{query.FindVariable{Symbol: value}},
			In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.ScalarInput{Symbol: item}},
			Where: []query.Clause{&query.DataPattern{Elements: []query.PatternElement{
				query.Variable{Name: item}, query.Constant{Value: failing}, query.Variable{Name: value},
			}}},
		},
		Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Variable{Name: entity}},
		Binding: query.RelationBinding{Variables: []query.Symbol{value}},
	}}}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			matcher := &failingScanMatcher{
				failAttr: failing,
				dataRel:  NewMaterializedRelation([]query.Symbol{item, value}, nil),
			}
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions()).queryExecutor()
			outer := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{datalog.NewIdentity("body-error")}})
			groups, err := exec.Execute(NewContext(), q, []Relation{outer})
			require.NoError(t, err, "the form's result carries the body's failure")
			require.Len(t, groups, 1, "the subquery's result stands in the subject's place")
			require.ErrorIs(t, driveErr(groups[0]), errInjectedIterator)
		})
	}
}
