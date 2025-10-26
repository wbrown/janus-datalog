package planner

import (
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestEqualityPredicatePlanning(t *testing.T) {
	queryStr := `[:find ?year ?month 
				  :where
				  [?p :price/time ?time]
				  [(year ?time) ?year]
				  [(month ?time) ?month]
				  [?p2 :price/time ?time2]
				  [(year ?time2) ?year2]
				  [(month ?time2) ?month2]
				  [(= ?year ?year2)]
				  [(= ?month ?month2)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	p := NewPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}

	t.Logf("Query plan:\n%s", plan.String())

	// Check that equality predicates are assigned to phases
	foundEqualityPredicate := false
	for i, phase := range plan.Phases {
		t.Logf("Phase %d - Available: %v, Provides: %v", i+1, phase.Available, phase.Provides)

		for _, pred := range phase.Predicates {
			if comp, ok := pred.Predicate.(*query.Comparison); ok && comp.Op == query.OpEQ {
				foundEqualityPredicate = true
				t.Logf("Found equality predicate in phase %d: %s", i+1, pred.Predicate.String())
			}
		}

		for _, joinPred := range phase.JoinPredicates {
			if comp, ok := joinPred.Predicate.(*query.Comparison); ok && comp.Op == query.OpEQ {
				t.Logf("Found JOIN predicate in phase %d: %s (left=%s, right=%s)",
					i+1, joinPred.Predicate.String(), joinPred.LeftSymbol, joinPred.RightSymbol)
			}
		}

		for _, expr := range phase.Expressions {
			if expr.IsEquality {
				t.Logf("Found equality expression in phase %d: %s", i+1, expr.Expression.String())
			}
		}
	}

	if !foundEqualityPredicate && len(plan.Phases) > 0 {
		// Check if they're join predicates instead
		for _, phase := range plan.Phases {
			if len(phase.JoinPredicates) > 0 {
				t.Error("Equality predicates were incorrectly classified as join predicates!")
			}
		}
		if !foundEqualityPredicate {
			t.Error("No equality predicates found in any phase")
		}
	}
}
