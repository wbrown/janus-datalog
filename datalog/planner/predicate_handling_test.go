package planner

import (
	"github.com/wbrown/janus-datalog/datalog/parser"
	"testing"
)

func TestPlannerHandlesNewPredicates(t *testing.T) {
	queryStr := `
	[:find ?x ?y
	 :where
	 [?e :foo/x ?x]
	 [?e :foo/y ?y]
	 [(> ?x 5)]
	 [(< ?y 10)]
	 [(= ?x ?y)]]
	`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	p := NewPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatal(err)
	}

	// Check that predicates made it into the plan
	totalPredicates := 0
	for i, phase := range plan.Phases {
		t.Logf("Phase %d has %d predicates", i, len(phase.Predicates))
		totalPredicates += len(phase.Predicates)

		for j, pred := range phase.Predicates {
			t.Logf("  Predicate %d: %T -> %v", j, pred.Predicate, pred.Predicate)
		}
	}

	if totalPredicates == 0 {
		t.Error("No predicates found in plan - new predicate types are being ignored!")
	}

	// We expect 3 predicates total
	if totalPredicates != 3 {
		t.Errorf("Expected 3 predicates in plan, got %d", totalPredicates)
	}
}
