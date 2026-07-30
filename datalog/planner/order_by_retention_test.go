package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// These tests pin the planner half of the order-by fix from
// BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md:
// effective :order-by symbols not already projected by :find must survive
// phasing — threaded through Keep and appended to the last phase's find
// clause — so the executor can sort the assembled result before stripping
// it back to the declared :find shape.

// lastPhaseFindSymbols collects the variable symbols of a plan's final
// phase find clause.
func lastPhaseFindSymbols(plan *RealizedPlan) []query.Symbol {
	last := plan.Phases[len(plan.Phases)-1]
	var syms []query.Symbol
	for _, elem := range last.Query.Find {
		if v, ok := elem.(query.FindVariable); ok {
			syms = append(syms, v.Symbol)
		}
	}
	return syms
}

func TestPlanRetainsNonProjectedOrderBySymbols(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?name
	                              :where [?e :user/name ?name]
	                                     [?e :user/age ?age]
	                              :order-by [[?age :asc]]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	p := NewClauseBasedPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	age := query.Symbol(nil)
	for _, sym := range lastPhaseFindSymbols(plan) {
		if sym.String() == "?age" {
			age = sym
		}
	}
	if age == nil {
		t.Errorf("last phase find must retain sort symbol ?age, got %v",
			plan.Phases[len(plan.Phases)-1].Query.Find)
	}

	// The retained symbol must be threaded through every earlier phase's
	// Keep that provides it, or the last phase has nothing to project.
	for i, phase := range plan.Phases[:len(plan.Phases)-1] {
		provides := false
		for _, sym := range phase.Provides {
			if sym.String() == "?age" {
				provides = true
			}
		}
		if !provides {
			continue
		}
		kept := false
		for _, sym := range phase.Keep {
			if sym.String() == "?age" {
				kept = true
			}
		}
		if !kept {
			t.Errorf("phase %d provides ?age but does not Keep it", i)
		}
	}

	// The plan's top-level query is the executor's record of the declared
	// :find shape to strip back to — it must stay unaugmented.
	if len(plan.Query.Find) != 1 {
		t.Errorf("plan.Query.Find must remain the original :find, got %v", plan.Query.Find)
	}
}

func TestPlanRetainsOnlyNonProjectedSortKeys(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?dept ?name
	                              :where [?e :user/name ?name]
	                                     [?e :user/dept ?dept]
	                                     [?e :user/age ?age]
	                              :order-by [[?dept :asc] [?age :desc]]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	p := NewClauseBasedPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	syms := lastPhaseFindSymbols(plan)
	if len(syms) != 3 {
		t.Fatalf("expected last phase find [?dept ?name ?age], got %v", syms)
	}
	// Original find order preserved, retained symbol appended.
	want := []string{"?dept", "?name", "?age"}
	for i, sym := range syms {
		if sym.String() != want[i] {
			t.Errorf("find symbol %d: expected %s, got %s", i, want[i], sym)
		}
	}
}

func TestPlanDoesNotRetainForAggregates(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?city (count ?p)
	                              :where [?p :person/city ?city]
	                              :order-by [[?city :asc]]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	p := NewClauseBasedPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	last := plan.Phases[len(plan.Phases)-1]
	if len(last.Query.Find) != 2 {
		t.Errorf("aggregate find clause must not be augmented (grouping would change), got %v", last.Query.Find)
	}
}

func TestPlanDoesNotRetainConstantInputKeys(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?name
	                              :in $ ?status
	                              :where [?e :user/status ?status]
	                                     [?e :user/name ?name]
	                              :order-by [[?status :asc]]]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	p := NewClauseBasedPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	last := plan.Phases[len(plan.Phases)-1]
	if len(last.Query.Find) != 1 {
		t.Errorf("constant-input sort key must not be retained (identity sort), got %v", last.Query.Find)
	}
}
