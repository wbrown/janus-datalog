package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPredicateAssignmentToPhases(t *testing.T) {
	queryStr := `[:find ?name
	               :where [?person :person/name ?name]
	                      [(= ?name "Alice")]]`

	parsedQuery, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Verify parsed query has the predicate
	if len(parsedQuery.Where) != 2 {
		t.Fatalf("Expected 2 where clauses, got %d", len(parsedQuery.Where))
	}

	if _, ok := parsedQuery.Where[1].(query.Predicate); !ok {
		t.Fatalf("Second where clause should be a Predicate, got %T", parsedQuery.Where[1])
	}

	// Create planner
	opts := PlannerOptions{
		EnableDynamicReordering: true,
		EnablePredicatePushdown: true,
		EnableFineGrainedPhases: true,
	}
	planr := NewPlanner(nil, opts)

	// Plan the query
	plan, err := planr.Plan(parsedQuery)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Realize the plan and check the Query has the predicate
	// NOTE: Predicates may be pushed to storage and removed from phase.Predicates,
	// but they should be reconstructed in the realized query.
	realized := plan.Realize()

	for i, phase := range realized.Phases {
		t.Logf("Realized Phase %d: %d where clauses", i, len(phase.Query.Where))
		hasPredicateClause := false
		for j, clause := range phase.Query.Where {
			t.Logf("  Clause %d: %T", j, clause)
			if _, ok := clause.(query.Predicate); ok {
				hasPredicateClause = true
			}
		}

		if !hasPredicateClause {
			t.Errorf("Phase %d Query is missing the predicate clause!", i)
			t.Logf("Original query had predicate: %v", parsedQuery.Where[1])
		}
	}
}
