package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// clausePosition returns the phase and index of the first clause of a plan
// that renders as clause, or -1 and -1 when the plan carries none.
func clausePosition(plan *RealizedPlan, clause query.Clause) (int, int) {
	for i, phase := range plan.Phases {
		for j, c := range phase.Query.Where {
			if c.String() == clause.String() {
				return i, j
			}
		}
	}
	return -1, -1
}

// formBranches returns the branches of a nested form, the one body of a not
// or not-join among them, and nil for any other clause.
func formBranches(clause query.Clause) [][]query.Clause {
	switch c := clause.(type) {
	case *query.OrClause:
		return c.Branches
	case *query.OrJoinClause:
		return c.Branches
	case *query.OrDefaultClause:
		return c.Branches
	case *query.OrDefaultJoinClause:
		return c.Branches
	case *query.NotClause:
		return [][]query.Clause{c.Clauses}
	case *query.NotJoinClause:
		return [][]query.Clause{c.Clauses}
	}
	return nil
}

// formPosition returns the phase and index of the first nested form of a
// plan one of whose branches carries a clause rendering as pattern, or -1
// and -1 when the plan carries none. The algebra path rewrites an or into an
// or-join and a not into a not-join, so the form under test is found by the
// pattern its branch reads.
func formPosition(plan *RealizedPlan, pattern query.Clause) (int, int) {
	for i, phase := range plan.Phases {
		for j, c := range phase.Query.Where {
			for _, branch := range formBranches(c) {
				for _, clause := range branch {
					if clause.String() == pattern.String() {
						return i, j
					}
				}
			}
		}
	}
	return -1, -1
}

// An inference form, or, or-default or not, unifies with the enclosing scope
// on every free variable its branches share with it, so a phase runs the
// clauses binding such a variable before the form, as it runs an expression
// before a pattern reading the variable it binds: the form's output does not
// carry a variable one branch alone reads, and a binding that arrives after
// the form has nothing left to unify with. The binding may be one expression,
// a chain of them with the later ones not ready until the earlier have run,
// a subquery, or an explicit-header form.
func TestInferenceFormSchedulesAfterTheClausesBindingItsBranchVariable(t *testing.T) {
	e, a, b, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?v")
	pattern := func(elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elements}
	}
	anchor := pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/n")}, query.Blank{})
	read := pattern(query.Variable{Name: e}, query.Variable{Name: a}, query.Variable{Name: v})
	reading := []query.Clause{read}
	other := []query.Clause{pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/tag")}, query.Constant{Value: datalog.NewKeyword(":t/none")})}
	bindings := []struct {
		name    string
		clauses []query.Clause
	}{
		{"one expression", []query.Clause{
			&query.Expression{Function: &query.GroundFunction{Value: datalog.NewKeyword(":t/tag")}, Binding: a},
		}},
		{"two expressions", []query.Clause{
			&query.Expression{Function: &query.GroundFunction{Value: datalog.NewKeyword(":t/tag")}, Binding: b},
			&query.Expression{Function: &query.IdentityFunction{Arg: query.VariableTerm{Symbol: b}}, Binding: a},
		}},
		{"a subquery", []query.Clause{
			&query.SubqueryPattern{
				Query: &query.Query{
					Find:  []query.FindElement{query.FindVariable{Symbol: a}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
					Where: []query.Clause{&query.Expression{Function: &query.GroundFunction{Value: datalog.NewKeyword(":t/tag")}, Binding: a}},
				},
				Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}},
				Binding: query.TupleBinding{Variables: []query.Symbol{a}},
			},
		}},
		{"an or-join", []query.Clause{
			&query.OrJoinClause{OutputVars: []query.Symbol{a}, Branches: [][]query.Clause{
				{&query.Expression{Function: &query.GroundFunction{Value: datalog.NewKeyword(":t/tag")}, Binding: a}},
				{&query.Expression{Function: &query.GroundFunction{Value: datalog.NewKeyword(":t/other")}, Binding: a}},
			}},
		}},
	}
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
		for _, binding := range bindings {
			for _, form := range forms {
				t.Run(path.name+"/"+binding.name+"/"+form.name, func(t *testing.T) {
					where := append(append([]query.Clause{}, binding.clauses...), anchor, form.form)
					q := &query.Query{
						Find:  []query.FindElement{query.FindVariable{Symbol: e}},
						In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
						Where: where,
					}
					prepared, err := query.Prepare(q, nil)
					if err != nil {
						t.Fatalf("Prepare: %v", err)
					}
					plan, err := NewClauseBasedPlanner(nil, path.options).PlanQuery(prepared, query.Bindings{}, nil)
					if err != nil {
						t.Fatalf("PlanQuery: %v", err)
					}
					formPhase, formIndex := formPosition(plan, read)
					if formPhase < 0 {
						t.Fatalf("the plan carries no form reading %s:\n%s", read, plan)
					}
					for _, clause := range binding.clauses {
						phase, index := clausePosition(plan, clause)
						if phase < 0 {
							t.Fatalf("the plan carries no %s:\n%s", clause, plan)
						}
						if phase > formPhase || (phase == formPhase && index > formIndex) {
							t.Errorf("%s runs at phase %d clause %d, after the %s reading ?a at phase %d clause %d:\n%s",
								clause, phase, index, form.name, formPhase, formIndex, plan)
						}
					}
				})
			}
		}
	}
}

// A clause that binds a variable a branch alone reads, and that itself needs
// a variable only the form provides, leaves no order binding the variable
// before the form, and the plan is refused naming the form and the variable.
func TestInferenceFormWhoseRequiredVariableNoOrderBindsIsRefused(t *testing.T) {
	e, a, x := datalog.NewSymbol("?e"), datalog.NewSymbol("?a"), datalog.NewSymbol("?x")
	pattern := func(elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elements}
	}
	anchor := pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/n")}, query.Blank{})
	read := pattern(query.Variable{Name: e}, query.Variable{Name: a}, query.Variable{Name: x})
	form := &query.OrClause{Branches: [][]query.Clause{
		{read},
		{pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/tag")}, query.Variable{Name: x})},
	}}
	needing := &query.Expression{Function: &query.IdentityFunction{Arg: query.VariableTerm{Symbol: x}}, Binding: a}
	paths := []struct {
		name    string
		options PlannerOptions
	}{
		{"clause-based", PlannerOptions{}},
		{"algebra", PlannerOptions{EnableAlgebraOptimizer: true}},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			q := &query.Query{
				Find:  []query.FindElement{query.FindVariable{Symbol: e}},
				In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
				Where: []query.Clause{anchor, form, needing},
			}
			prepared, err := query.Prepare(q, nil)
			if err != nil {
				t.Fatalf("Prepare: %v", err)
			}
			plan, err := NewClauseBasedPlanner(nil, path.options).PlanQuery(prepared, query.Bindings{}, nil)
			if err == nil {
				t.Fatalf("the plan carries the or whose required ?a no order binds:\n%s", plan)
			}
			if !containsAll(err.Error(), "(or-join [[?a] ?e ?x]", "?a") {
				t.Errorf("the refusal names the form and ?a, got: %v", err)
			}
		})
	}
}
