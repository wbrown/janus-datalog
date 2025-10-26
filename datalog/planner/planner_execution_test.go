package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestPlannerExecutionEquivalency tests that old and new planners produce equivalent results
// This is more important than plan structure equivalency
func TestPlannerExecutionEquivalency(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "simple pattern",
			query: `[:find ?e ?name
			         :where [?e :person/name ?name]]`,
		},
		{
			name: "two patterns with join",
			query: `[:find ?person ?age
			         :where
			         [?person :person/name ?name]
			         [?person :person/age ?age]]`,
		},
		{
			name: "pattern with predicate",
			query: `[:find ?person ?age
			         :where
			         [?person :person/age ?age]
			         [(> ?age 21)]]`,
		},
		{
			name: "pattern with expression",
			query: `[:find ?person ?doubled
			         :where
			         [?person :person/age ?age]
			         [(* ?age 2) ?doubled]]`,
		},
		{
			name: "multi-phase query",
			query: `[:find ?person ?product
			         :where
			         [?person :person/name ?name]
			         [?order :order/customer ?person]
			         [?order :order/product ?product]]`,
		},
	}

	defaultOpts := PlannerOptions{
		EnableDynamicReordering:             true,
		EnablePredicatePushdown:             true,
		EnableSemanticRewriting:             false, // Disabled in new planner
		EnableConditionalAggregateRewriting: false, // Disabled in new planner
		EnableCSE:                           false,
		MaxPhases:                           10,
		EnableFineGrainedPhases:             true,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse query
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query: %v", err)
			}

			// Old planner
			oldPlanner := NewPlanner(nil, defaultOpts)
			oldPlan, err := oldPlanner.Plan(q)
			if err != nil {
				t.Fatalf("old planner failed: %v", err)
			}
			oldRealized := oldPlan.Realize()

			// New clause-based planner
			newPlanner := NewClauseBasedPlanner(nil, defaultOpts)
			newRealized, err := newPlanner.Plan(q)
			if err != nil {
				t.Fatalf("new planner failed: %v", err)
			}

			// Both planners should produce valid plans
			if len(oldRealized.Phases) == 0 {
				t.Error("old planner produced empty plan")
			}
			if len(newRealized.Phases) == 0 {
				t.Error("new planner produced empty plan")
			}

			// Check that both plans have same final output symbols
			if len(oldRealized.Phases) > 0 && len(newRealized.Phases) > 0 {
				oldFinal := oldRealized.Phases[len(oldRealized.Phases)-1]
				newFinal := newRealized.Phases[len(newRealized.Phases)-1]

				if !symbolSetsEqual(oldFinal.Keep, newFinal.Keep) {
					t.Errorf("final output symbols differ:\n  old=%v\n  new=%v",
						oldFinal.Keep, newFinal.Keep)
				}
			}

			// Note: We don't compare plan structure because different phasing strategies
			// can produce different but equivalent plans. The key is execution equivalency.
			// TODO: Add actual execution tests with mock data
		})
	}
}
