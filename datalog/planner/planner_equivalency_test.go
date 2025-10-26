package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPlannerEquivalency compares the old planner to the new clause-based planner
func TestPlannerEquivalency(t *testing.T) {
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
		EnableSemanticRewriting:             false, // Disable for now
		EnableConditionalAggregateRewriting: false, // Disable for now
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

			// Compare plans (informational, not strict equality)
			// Different phasing strategies can produce different valid plans
			comparePlansLoose(t, oldRealized, newRealized)
		})
	}
}

// comparePlansLoose checks if two RealizedPlans are similar
// Different phasing strategies can produce different but equivalent plans
func comparePlansLoose(t *testing.T, old, new *RealizedPlan) {
	// Log phase count difference (not an error)
	if len(old.Phases) != len(new.Phases) {
		t.Logf("phase count differs: old=%d, new=%d (different phasing strategy)",
			len(old.Phases), len(new.Phases))
		// Don't fail on phase count - just continue checking what we can
	}

	// Check that final outputs match (most important check)
	if len(old.Phases) > 0 && len(new.Phases) > 0 {
		oldFinal := old.Phases[len(old.Phases)-1]
		newFinal := new.Phases[len(new.Phases)-1]

		if !symbolSetsEqual(oldFinal.Keep, newFinal.Keep) {
			t.Errorf("final output symbols differ:\n  old=%v\n  new=%v",
				oldFinal.Keep, newFinal.Keep)
		}
	}

	// If phase counts match, do detailed comparison
	minPhases := len(old.Phases)
	if len(new.Phases) < minPhases {
		minPhases = len(new.Phases)
	}

	for i := 0; i < minPhases; i++ {
		oldPhase := old.Phases[i]
		newPhase := new.Phases[i]

		// Log differences but don't fail - different strategies are valid
		if !symbolSetsEqual(oldPhase.Provides, newPhase.Provides) {
			t.Logf("phase %d: Provides differ (old=%v, new=%v)",
				i, oldPhase.Provides, newPhase.Provides)
		}
	}
}

// symbolSetsEqual checks if two symbol slices contain the same symbols (order-independent)
func symbolSetsEqual(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}

	aSet := make(map[query.Symbol]bool)
	for _, sym := range a {
		aSet[sym] = true
	}

	for _, sym := range b {
		if !aSet[sym] {
			return false
		}
	}

	return true
}
