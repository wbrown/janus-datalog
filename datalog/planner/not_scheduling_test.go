package planner

import (
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestNotClauseWithExistentialBodyVariablePlans pins the Correlates
// scheduling rule: (not-join [?v] [?d :seen/val ?v]), the header form the
// inference writes for a not reading ?v from its scope, correlates on ?v
// alone — ?d is the body's own and must not block phasing.
func TestNotClauseWithExistentialBodyVariablePlans(t *testing.T) {
	e := datalog.NewSymbol("?e")
	v := datalog.NewSymbol("?v")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":item/val")},
		query.Variable{Name: v},
	}}
	notClause := &query.NotJoinClause{RequiredVars: []query.Symbol{v}, Clauses: []query.Clause{
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
		query.Bindings{},
	)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}

	assertScheduledAfter(t, phases, notClause, pattern,
		"the NOT must schedule after the pattern that binds its bindable correlate ?v")
}

// TestFullyDisjointNotRefusedAtPreparation pins the disjoint-NOT ruling: a
// NOT body sharing no variable with the enclosing scope has no anti-join
// keys, and its quantification would silently become global — refused at
// preparation with a message naming the clause. A body variable the query's
// inputs bind keeps the clause correlated, and the prepared query plans.
func TestFullyDisjointNotRefusedAtPreparation(t *testing.T) {
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

	_, err := query.Prepare(&query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: v}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
		Where: []query.Clause{pattern, notClause},
	}, nil)
	if err == nil {
		t.Fatal("expected preparation to refuse a NOT sharing no variable with the enclosing scope")
	}
	if !containsAll(err.Error(), "(not ", "?w", "shares no variable") {
		t.Fatalf("refusal must name the clause and state the sharing rule, got: %v", err)
	}

	// The same body is correlated when an input binds ?w.
	prepared, err := query.Prepare(&query.Query{
		Find:  []query.FindElement{query.FindVariable{Symbol: v}},
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.ScalarInput{Symbol: w}},
		Where: []query.Clause{pattern, notClause},
	}, nil)
	if err != nil {
		t.Fatalf("input-bound body variable must keep the NOT preparable: %v", err)
	}
	if _, err := NewClauseBasedPlanner(nil, PlannerOptions{}).PlanQuery(prepared, query.Bindings{}, nil); err != nil {
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
// is its complete interface, so (not-join [?v] (or-join [?v] ...)), the
// header form the inference writes for a not over it, correlates on ?v
// alone — branch locals must neither leak nor block.
func TestNotOverOrJoinPlans(t *testing.T) {
	e := datalog.NewSymbol("?e")
	v := datalog.NewSymbol("?v")

	pattern := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: e},
		query.Constant{Value: datalog.NewKeyword(":item/val")},
		query.Variable{Name: v},
	}}
	notClause := &query.NotJoinClause{RequiredVars: []query.Symbol{v}, Clauses: []query.Clause{
		&query.OrJoinClause{
			OutputVars: []query.Symbol{v},
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
		query.Bindings{},
	)
	if err != nil {
		t.Fatalf("phasing failed: %v", err)
	}

	assertScheduledAfter(t, phases, notClause, pattern,
		"the NOT must schedule after the pattern that binds the or-join header ?v")
}
