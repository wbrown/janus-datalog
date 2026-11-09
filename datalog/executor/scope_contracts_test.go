package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

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
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions()).queryExecutor(&planner.RealizedPlan{})
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
			exec := NewExecutorWithOptions(matcher, nil, opts).queryExecutor(&planner.RealizedPlan{})
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

// An invocation of a predicate the scope holds, a stratum's relation or its
// delta under the fixpoint, has no fixpoint to seed: it reads the held
// relation and joins it at the collapse, where the join holds the held
// relation and streams the group, so no window is read whatever binds its
// arguments, the group is read once, and the result is the join. The scope
// holds follows as pages 1 to 4 each followed by page 5, and the group binds
// ?a to pages 1, 2 and 6, so pages 1 and 2 are answered beside page 5 and
// page 6 is dropped.
func TestInvocationOfAHeldPredicateOverAStreamingGroupJoinsAtTheCollapse(t *testing.T) {
	a, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?c")
	follows := datalog.NewSymbol("follows")
	rs := mustRules(t, followsRulesText)
	head, ok := rs.Head(follows)
	require.True(t, ok, "the rule set declares follows")
	held := NewMaterializedRelation(head, []Tuple{{page(1), page(5)}, {page(2), page(5)}, {page(3), page(5)}, {page(4), page(5)}})
	clauses := mustParse(t, `[:find ?a ?c :in $ % :where (follows ?a ?c)]`).Where
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			windows := 0
			opts := mode.plannerOptions()
			opts.Handler = func(e annotations.Event) {
				if e.Name == annotations.ScopeWindow {
					windows++
				}
			}
			exec := NewExecutorWithOptions(nil, nil, opts).queryExecutor(&planner.RealizedPlan{})
			pulls := NewCountingIterator(NewMaterializedRelation([]query.Symbol{a}, []Tuple{{page(1)}, {page(2)}, {page(6)}}).Iterator())
			subject := NewStreamingRelationWithProperties([]query.Symbol{a}, pulls, exec.options, deduplicatedProperties([]query.Symbol{a}))
			ctx := NewContext().WithRules(rs).WithDerived(map[query.Symbol]Relation{follows: held})
			groups, err := exec.Execute(ctx, &query.Query{Where: clauses}, []Relation{subject})
			require.NoError(t, err)
			require.Len(t, groups, 1, "the held relation is joined with the group at the collapse")
			got, err := CollectTuples(groups[0], nil)
			require.NoError(t, err)
			syms := groups[0].Symbols()
			ai, ci := query.SymbolIndex(syms, a), query.SymbolIndex(syms, c)
			require.True(t, ai >= 0 && ci >= 0, "the result carries the group's symbol and the free variable; got %v", syms)
			var pairs [][]interface{}
			for _, tuple := range got {
				pairs = append(pairs, []interface{}{tuple[ai], tuple[ci]})
			}
			require.ElementsMatch(t, [][]interface{}{{page(1), page(5)}, {page(2), page(5)}}, pairs, "the group's pages the held relation follows are answered beside page 5")
			require.Equal(t, 3, pulls.Count(), "the group is read once, to its end")
			require.Equal(t, 0, windows, "an invocation of a held predicate reads no window")
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
			exec := NewExecutorWithOptions(matcher, nil, mode.plannerOptions()).queryExecutor(&planner.RealizedPlan{})
			outer := NewMaterializedRelation([]query.Symbol{entity}, []Tuple{{datalog.NewIdentity("body-error")}})
			groups, err := exec.Execute(NewContext(), q, []Relation{outer})
			require.NoError(t, err, "the form's result carries the body's failure")
			require.Len(t, groups, 1, "the subquery's result stands in the subject's place")
			require.ErrorIs(t, driveErr(groups[0]), errInjectedIterator)
		})
	}
}
