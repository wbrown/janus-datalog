package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestNotClauseWithExistentialBodyVariablePlans pins the Correlates
// scheduling rule: (not [?d :seen/val ?v]) correlates on {?d ?v}, but only
// ?v is bindable by the rest of the query — ?d is existential and must not
// block phasing. Before the fix, every body variable was a hard scheduling
// requirement and this query could not plan at all.
func TestNotClauseWithExistentialBodyVariablePlans(t *testing.T) {
	e := datalog.NewSymbol("?e")
	v := datalog.NewSymbol("?v")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":item/val")},
		query.Variable{Name: v},
	}}
	notClause := &query.NotClause{Clauses: []query.Clause{
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?d")},
			query.Constant{Value: datalog.NewKeyword(":seen/val")},
			query.Variable{Name: v},
		}},
	}}

	phases, err := createPhasesGreedy(
		[]query.Clause{notClause, pattern},
		[]query.Symbol{v},
		map[query.Symbol]bool{},
	)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}

	assertScheduledAfter(t, phases, notClause, pattern,
		"the NOT must schedule after the pattern that binds its bindable correlate ?v")
}

// TestNotOverOrJoinPlans pins the or-join composition: the or-join's header
// is its complete interface, so (not (or-join [?v] ...)) correlates on ?v
// alone — branch locals must neither leak nor block.
func TestNotOverOrJoinPlans(t *testing.T) {
	e := datalog.NewSymbol("?e")
	v := datalog.NewSymbol("?v")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":item/val")},
		query.Variable{Name: v},
	}}
	notClause := &query.NotClause{Clauses: []query.Clause{
		&query.OrJoinClause{
			JoinVars: []query.Symbol{v},
			Branches: [][]query.Clause{{
				&query.DataPattern{Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?d")},
					query.Constant{Value: datalog.NewKeyword(":seen/val")},
					query.Variable{Name: v},
				}},
			}},
		},
	}}

	phases, err := createPhasesGreedy(
		[]query.Clause{notClause, pattern},
		[]query.Symbol{v},
		map[query.Symbol]bool{},
	)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}

	assertScheduledAfter(t, phases, notClause, pattern,
		"the NOT must schedule after the pattern that binds the or-join header ?v")
}
