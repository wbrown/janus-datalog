package planner

import (
	"strings"
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

// TestFullyDisjointNotRejectedAtPlanning pins the disjoint-NOT ruling: a NOT
// body sharing no variable the query can bind (by clause or by input) has no
// anti-join keys, and its quantification would silently become global —
// rejected at planning with a message naming the clause. A body variable
// bindable via :in keeps the clause correlated and planning succeeds.
func TestFullyDisjointNotRejectedAtPlanning(t *testing.T) {
	e := datalog.NewSymbol("?e")
	v := datalog.NewSymbol("?v")
	w := datalog.NewSymbol("?w")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":item/val")},
		query.Variable{Name: v},
	}}
	notClause := &query.NotClause{Clauses: []query.Clause{
		&query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?d")},
			query.Constant{Value: datalog.NewKeyword(":seen/val")},
			query.Variable{Name: w},
		}},
	}}

	_, err := createPhasesGreedy(
		[]query.Clause{pattern, notClause},
		[]query.Symbol{v},
		map[query.Symbol]bool{},
	)
	if err == nil {
		t.Fatal("expected planning to reject a NOT sharing no bindable variable with the enclosing query")
	}
	if !containsAll(err.Error(), "(not ", "?w", "unify") {
		t.Fatalf("rejection must name the clause and state the unification rule, got: %v", err)
	}

	// The same body is correlated when an input can bind ?w.
	_, err = createPhasesGreedy(
		[]query.Clause{pattern, notClause},
		[]query.Symbol{v},
		map[query.Symbol]bool{w: true},
	)
	if err != nil {
		t.Fatalf("input-bound body variable must keep the NOT plannable: %v", err)
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
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
