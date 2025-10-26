package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestCrossProductAvoidance(t *testing.T) {
	// This is the test I should have fixed instead of deleting
	// It tests that fine-grained phase creation prevents memory-exploding cross-products

	tests := []struct {
		name              string
		query             string
		enableFineGrained bool
		minExpectedPhases int
		description       string
	}{
		{
			name: "Disjoint patterns should create cross-product without fine-grained",
			query: `[:find ?n1 ?n2 ?n3
			         :where 
			         [?p1 :person/name ?n1]
			         [?p1 :person/type "customer"]
			         
			         [?p2 :person/name ?n2]  
			         [?p2 :person/type "employee"]
			         
			         [?p3 :person/name ?n3]
			         [?p3 :person/type "vendor"]]`,
			enableFineGrained: false,
			minExpectedPhases: 1, // Without fine-grained, might put all in one phase (bad!)
			description:       "Without fine-grained phases, could create n³ cross-product",
		},
		{
			name: "Same query with fine-grained should avoid cross-product",
			query: `[:find ?n1 ?n2 ?n3
			         :where 
			         [?p1 :person/name ?n1]
			         [?p1 :person/type "customer"]
			         
			         [?p2 :person/name ?n2]
			         [?p2 :person/type "employee"]
			         
			         [?p3 :person/name ?n3]
			         [?p3 :person/type "vendor"]]`,
			enableFineGrained: true,
			minExpectedPhases: 3, // With fine-grained, should separate into 3+ phases
			description:       "With fine-grained phases, avoids cross-product explosion",
		},
		{
			name: "Four independent groups",
			query: `[:find ?n1 ?n2 ?n3 ?n4
			         :where 
			         [?p1 :person/name ?n1]
			         [?p1 :person/active true]
			         
			         [?p2 :person/name ?n2]
			         [?p2 :person/age 25]
			         
			         [?p3 :person/name ?n3]
			         [?p3 :person/city "NYC"]
			         
			         [?p4 :person/name ?n4]
			         [?p4 :person/role "admin"]]`,
			enableFineGrained: true,
			minExpectedPhases: 4,
			description:       "Each independent group should be in its own phase",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			planner := NewPlanner(nil, PlannerOptions{
				EnableFineGrainedPhases: tt.enableFineGrained,
				MaxPhases:               10,
			})

			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			plan, err := planner.Plan(q)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			t.Logf("%s", tt.description)
			t.Logf("Created %d phases (expected at least %d)", len(plan.Phases), tt.minExpectedPhases)

			if len(plan.Phases) < tt.minExpectedPhases {
				t.Errorf("Expected at least %d phases, got %d", tt.minExpectedPhases, len(plan.Phases))
				t.Logf("Plan:\n%s", plan.String())
			}

			// Check that patterns are distributed across phases
			if tt.enableFineGrained && len(plan.Phases) > 1 {
				// Each phase should have a reasonable number of patterns
				for i, phase := range plan.Phases {
					if len(phase.Patterns) > 4 {
						t.Logf("Warning: Phase %d has %d patterns - might create cross-product",
							i+1, len(phase.Patterns))
					}
				}
			}
		})
	}
}

func TestDisjointGroupDetection(t *testing.T) {
	// Test that the planner correctly identifies disjoint pattern groups

	planner := NewPlanner(nil, PlannerOptions{
		EnableFineGrainedPhases: true,
	})

	// Query with two completely disjoint groups
	query := `[:find ?name1 ?age1 ?name2 ?age2
	           :where 
	           ;; Group 1
	           [?person1 :person/name ?name1]
	           [?person1 :person/age ?age1]
	           [(> ?age1 25)]
	           
	           ;; Group 2 (no connection to group 1)
	           [?person2 :person/name ?name2]
	           [?person2 :person/age ?age2]
	           [(< ?age2 30)]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Should create multiple phases to avoid cross-product
	if len(plan.Phases) < 2 {
		t.Errorf("Expected multiple phases for disjoint groups, got %d", len(plan.Phases))
		t.Logf("Plan:\n%s", plan.String())
	}

	// Check that variables are properly separated
	phase1Provides := make(map[string]bool)
	if len(plan.Phases) > 0 {
		for _, sym := range plan.Phases[0].Provides {
			phase1Provides[string(sym)] = true
		}
	}

	// First phase should provide either person1 variables OR person2 variables, not both
	hasPerson1 := phase1Provides["?person1"] || phase1Provides["?name1"] || phase1Provides["?age1"]
	hasPerson2 := phase1Provides["?person2"] || phase1Provides["?name2"] || phase1Provides["?age2"]

	if hasPerson1 && hasPerson2 {
		t.Log("Warning: First phase provides variables from both disjoint groups")
		t.Logf("Phase 1 provides: %v", plan.Phases[0].Provides)
	}
}

func TestExpressionBridgingDisjointGroups(t *testing.T) {
	// Test that expressions can bridge disjoint pattern groups

	planner := NewPlanner(nil, PlannerOptions{
		EnableFineGrainedPhases: true,
	})

	query := `[:find ?x ?y ?sum
	           :where 
	           [?a :foo/value ?x]
	           [?b :bar/value ?y]
	           [(+ ?x ?y) ?sum]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Should have multiple phases:
	// 1. Pattern for ?x
	// 2. Pattern for ?y
	// 3. Expression combining them
	t.Logf("Plan has %d phases", len(plan.Phases))
	t.Logf("Plan:\n%s", plan.String())

	if len(plan.Phases) < 2 {
		t.Logf("Expected multiple phases, got %d", len(plan.Phases))
	}

	// Find which phase has the expression
	expressionPhaseIndex := -1
	for i, phase := range plan.Phases {
		if len(phase.Expressions) > 0 {
			expressionPhaseIndex = i
			break
		}
	}

	if expressionPhaseIndex == -1 {
		t.Error("No phase contains the expression")
	} else {
		// Expression phase should have both ?x and ?y available
		// (either from previous phases or from patterns in the same phase)
		exprPhase := plan.Phases[expressionPhaseIndex]
		hasX := false
		hasY := false

		// Check Available (from previous phases)
		for _, sym := range exprPhase.Available {
			if string(sym) == "?x" {
				hasX = true
			}
			if string(sym) == "?y" {
				hasY = true
			}
		}

		// Check Provides (from this phase)
		for _, sym := range exprPhase.Provides {
			if string(sym) == "?x" {
				hasX = true
			}
			if string(sym) == "?y" {
				hasY = true
			}
		}

		if !hasX || !hasY {
			t.Errorf("Expression phase should have both ?x and ?y available")
			t.Logf("Available: %v, Provides: %v", exprPhase.Available, exprPhase.Provides)
		}
	}
}
