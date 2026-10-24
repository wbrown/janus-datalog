package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The planner plans the query with every or, not and or-default rewritten as
// the header form that states its interface, so no phase of either path
// carries an inference form: the phase text is the whole interface.
func TestPlan_PhasesCarryHeaderFormsOnly(t *testing.T) {
	e, a, v, k := datalog.NewSymbol("?e"), datalog.NewSymbol("?a"), datalog.NewSymbol("?v"), datalog.NewSymbol("?k")
	pattern := func(elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elements}
	}
	anchor := pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/n")}, query.Blank{})
	binder := &query.Expression{Function: &query.GroundFunction{Value: datalog.NewKeyword(":t/tag")}, Binding: a}
	reading := []query.Clause{pattern(query.Variable{Name: e}, query.Variable{Name: a}, query.Variable{Name: v})}
	other := []query.Clause{pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/tag")}, query.Constant{Value: datalog.NewKeyword(":t/none")})}
	forms := []struct {
		name string
		find []query.FindElement
		form query.Clause
	}{
		{"or", []query.FindElement{query.FindVariable{Symbol: e}}, &query.OrClause{Branches: [][]query.Clause{reading, other}}},
		{"or holding a not", []query.FindElement{query.FindVariable{Symbol: e}}, &query.OrClause{Branches: [][]query.Clause{
			{pattern(query.Variable{Name: e}, query.Variable{Name: a}, query.Variable{Name: v}), &query.NotClause{Clauses: []query.Clause{pattern(query.Variable{Name: v}, query.Constant{Value: datalog.NewKeyword(":t/n")}, query.Blank{})}}},
			other,
		}}},
		{"or-default", []query.FindElement{query.FindVariable{Symbol: e}, query.FindVariable{Symbol: k}}, &query.OrDefaultClause{Branches: [][]query.Clause{
			{pattern(query.Variable{Name: e}, query.Variable{Name: a}, query.Variable{Name: v}), &query.Expression{Function: &query.GroundFunction{Value: int64(1)}, Binding: k}},
			{&query.Expression{Function: &query.GroundFunction{Value: int64(0)}, Binding: k}},
		}}},
		{"not", []query.FindElement{query.FindVariable{Symbol: e}}, &query.NotClause{Clauses: reading}},
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
					Find:  form.find,
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
					Where: []query.Clause{anchor, binder, form.form},
				}
				prepared, err := query.Prepare(q, nil)
				if err != nil {
					t.Fatalf("Prepare: %v", err)
				}
				plan, err := NewClauseBasedPlanner(nil, path.options).PlanQuery(prepared, query.Bindings{}, nil)
				if err != nil {
					t.Fatalf("PlanQuery: %v", err)
				}
				for i, phase := range plan.Phases {
					query.WalkClauses(phase.Query.Where, func(c query.Clause) bool {
						switch c.(type) {
						case *query.OrClause, *query.NotClause, *query.OrDefaultClause:
							t.Errorf("phase %d carries the inference form %s; every form in a phase states its header:\n%s", i, c, plan)
						}
						return true
					})
				}
			})
		}
	}
}

// The planner takes a prepared query and computes no interface: a query
// holding a plain or reaches ScopeOf, which refuses a headerless form, on
// both paths.
func TestPlanQuery_RequiresAPreparedQuery(t *testing.T) {
	e, v := datalog.NewSymbol("?e"), datalog.NewSymbol("?v")
	pattern := func(elements ...query.PatternElement) *query.DataPattern {
		return &query.DataPattern{Elements: elements}
	}
	q := &query.Query{
		Find: []query.FindElement{query.FindVariable{Symbol: e}},
		In:   []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}},
		Where: []query.Clause{
			pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/n")}, query.Blank{}),
			&query.OrClause{Branches: [][]query.Clause{
				{pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/tag")}, query.Variable{Name: v})},
				{pattern(query.Variable{Name: e}, query.Constant{Value: datalog.NewKeyword(":t/n")}, query.Variable{Name: v})},
			}},
		},
	}
	paths := []struct {
		name    string
		options PlannerOptions
	}{
		{"clause-based", PlannerOptions{}},
		{"algebra", PlannerOptions{EnableAlgebraOptimizer: true}},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("PlanQuery returned on a query holding a plain or; the planner takes a prepared query")
				}
			}()
			_, _ = NewClauseBasedPlanner(nil, path.options).PlanQuery(q, query.Bindings{}, nil)
		})
	}
}
