package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An inference form, or, or-default or not, unifies with the enclosing scope
// on every free variable its branches share with it, so a phase runs the
// clause binding such a variable before the form, as it runs an expression
// before a pattern reading the variable it binds: the form's output does not
// carry a variable one branch alone reads, and a binding that arrives after
// the form has nothing left to unify with.
func TestInferenceFormSchedulesAfterTheClauseBindingItsBranchVariable(t *testing.T) {
	e, a, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?a"), datalog.NewSymbol("?v")
	pattern := func(elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elements}
	}
	ground := &query.Expression{Function: &query.GroundFunction{Value: datalog.NewKeyword(":t/tag")}, Binding: a}
	anchor := pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/n")}, query.Blank{})
	reading := []query.Clause{pattern(query.Variable{Name: e}, query.Variable{Name: a}, query.Variable{Name: v})}
	other := []query.Clause{pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/tag")}, query.Constant{Value: datalog.NewKeyword(":t/none")})}
	forms := []struct {
		name string
		form query.Clause
	}{
		{"or", &query.OrClause{Branches: [][]query.Clause{reading, other}}},
		{"or-default", &query.OrDefaultClause{Branches: [][]query.Clause{reading, other}}},
		{"not", &query.NotClause{Clauses: reading}},
	}
	paths := []struct {
		name    string
		options PlannerOptions
	}{
		{"clause-based", PlannerOptions{}},
		{"algebra", PlannerOptions{EnableAlgebraOptimizer: true}},
	}
	for _, path := range paths {
		for _, form := range forms {
			t.Run(path.name+"/"+form.name, func(t *testing.T) {
				q := &query.Query{
					Find:  []query.FindElement{query.FindVariable{Symbol: e}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
					Where: []query.Clause{ground, anchor, form.form},
				}
				plan, err := NewClauseBasedPlanner(nil, path.options).PlanQuery(q, Bindings{}, nil)
				if err != nil {
					t.Fatalf("PlanQuery: %v", err)
				}
				groundPhase, groundIndex, formPhase, formIndex := -1, -1, -1, -1
				for i, phase := range plan.Phases {
					for j, clause := range phase.Query.Where {
						switch c := clause.(type) {
						case *query.Expression:
							if c.String() == ground.String() {
								groundPhase, groundIndex = i, j
							}
						case *query.OrClause, *query.OrJoinClause, *query.OrDefaultClause, *query.OrDefaultJoinClause, *query.NotClause, *query.NotJoinClause:
							formPhase, formIndex = i, j
						}
					}
				}
				if groundPhase < 0 || formPhase < 0 {
					t.Fatalf("the plan carries the ground at phase %d and the form at phase %d:\n%s", groundPhase, formPhase, plan)
				}
				if groundPhase > formPhase || (groundPhase == formPhase && groundIndex > formIndex) {
					t.Errorf("the ground binding ?a runs at phase %d clause %d, after the %s reading ?a at phase %d clause %d:\n%s",
						groundPhase, groundIndex, form.name, formPhase, formIndex, plan)
				}
			})
		}
	}
}
