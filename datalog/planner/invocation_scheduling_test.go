package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An invocation with a variable argument that a pending clause binds is
// placed after that clause on both planner paths, whatever the written order
// and however the scorer ranks the two, so the invocation derives only what
// the binding reaches.
func TestInvocationSchedulesAfterTheClauseBindingItsArgument(t *testing.T) {
	rules, err := parser.ParseRules(`[[(reach ?a ?b) [?a :edge ?b]]
	                                  [(reach ?a ?c) [?a :edge ?b] (reach ?b ?c)]]`)
	if err != nil {
		t.Fatalf("ParseRules: %v", err)
	}
	read := &query.DataPattern{Elements: []query.PatternElement{
		query.Variable{Name: datalog.NewSymbol("?x")},
		query.Constant{Value: datalog.NewKeyword(":edge")},
		query.Variable{Name: datalog.NewSymbol("?z")},
	}}
	queries := []struct {
		name string
		text string
	}{
		{"or written first", `[:find ?x ?y :in $ % :where (or [?x :edge ?z] [?z :edge ?x]) (reach ?x ?y)]`},
		{"or written after", `[:find ?x ?y :in $ % :where (reach ?x ?y) (or [?x :edge ?z] [?z :edge ?x])]`},
	}
	paths := []struct {
		name    string
		options PlannerOptions
	}{
		{"clause-based", PlannerOptions{}},
		{"algebra", PlannerOptions{EnableAlgebraOptimizer: true}},
	}
	for _, path := range paths {
		for _, q := range queries {
			t.Run(path.name+"/"+q.name, func(t *testing.T) {
				parsed, err := parser.ParseQuery(q.text)
				if err != nil {
					t.Fatalf("ParseQuery: %v", err)
				}
				prepared, err := query.Prepare(parsed, rules)
				if err != nil {
					t.Fatalf("Prepare: %v", err)
				}
				plan, err := NewClauseBasedPlanner(nil, path.options).PlanQuery(prepared, query.Bindings{Rules: rules}, nil)
				if err != nil {
					t.Fatalf("PlanQuery: %v", err)
				}
				formPhase, formIndex := formPosition(plan, read)
				if formPhase < 0 {
					t.Fatalf("the plan carries no form reading %s:\n%s", read, plan)
				}
				invPhase, invIndex := -1, -1
				for i, phase := range plan.Phases {
					for j, c := range phase.Query.Where {
						if _, ok := c.(*query.RuleInvocation); ok && invPhase < 0 {
							invPhase, invIndex = i, j
						}
					}
				}
				if invPhase < 0 {
					t.Fatalf("the plan carries no invocation:\n%s", plan)
				}
				if invPhase < formPhase || (invPhase == formPhase && invIndex < formIndex) {
					t.Errorf("the invocation runs at phase %d clause %d, before the form binding ?x at phase %d clause %d:\n%s",
						invPhase, invIndex, formPhase, formIndex, plan)
				}
			})
		}
	}
}
