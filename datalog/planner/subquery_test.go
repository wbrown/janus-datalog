package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestPlanSubquery(t *testing.T) {
	// Simple subquery that computes max price
	query := `[:find ?symbol ?max-price
	           :where 
	           [?s :symbol/ticker ?symbol]
	           [(q [:find (max ?price)
	                :in $ ?sym
	                :where [?p :price/symbol ?sym]
	                       [?p :price/value ?price]]
	               ?s) [[?max-price]]]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Verify we have phases
	if len(plan.Phases) == 0 {
		t.Error("Expected at least one phase")
	}

	// Find the phase with the subquery
	foundSubquery := false
	for _, phase := range plan.Phases {
		if len(phase.Subqueries) > 0 {
			foundSubquery = true
			subqPlan := phase.Subqueries[0]

			// Verify inputs
			if len(subqPlan.Inputs) != 1 || subqPlan.Inputs[0] != "?s" {
				t.Errorf("Expected subquery to have input [?s], got %v", subqPlan.Inputs)
			}

			// Verify nested plan exists
			if subqPlan.NestedPlan == nil {
				t.Error("Expected nested plan to be created")
			} else {
				// Verify nested plan has phases
				if len(subqPlan.NestedPlan.Phases) == 0 {
					t.Error("Expected nested plan to have phases")
				}
			}

			// Verify phase provides the output symbol
			hasMaxPrice := false
			for _, sym := range phase.Provides {
				if sym == "?max-price" {
					hasMaxPrice = true
					break
				}
			}
			if !hasMaxPrice {
				t.Errorf("Expected phase to provide ?max-price, got %v", phase.Provides)
			}
		}
	}

	if !foundSubquery {
		t.Error("Expected to find a subquery in the plan")
	}
}

func TestPlanSubqueryWithMultipleInputs(t *testing.T) {
	// Subquery with multiple inputs
	query := `[:find ?symbol ?date ?high
	           :where 
	           [?s :symbol/ticker ?symbol]
	           [(ground "2025-06-02") ?date]
	           [(q [:find (max ?h)
	                :in $ ?sym ?d
	                :where [?p :price/symbol ?sym]
	                       [?p :price/time ?t]
	                       [(same-date? ?t ?d)]
	                       [?p :price/high ?h]]
	               ?s ?date) [[?high]]]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Find the phase with the subquery
	foundSubquery := false
	for _, phase := range plan.Phases {
		if len(phase.Subqueries) > 0 {
			foundSubquery = true
			subqPlan := phase.Subqueries[0]

			// Verify inputs
			if len(subqPlan.Inputs) != 2 {
				t.Errorf("Expected subquery to have 2 inputs, got %d", len(subqPlan.Inputs))
			}

			// Verify both inputs are available before this phase
			for _, input := range subqPlan.Inputs {
				found := false
				// Check if input is in Available (from previous phases)
				for _, sym := range phase.Available {
					if sym == input {
						found = true
						break
					}
				}
				// Or in Provides (from this phase's patterns executed before subquery)
				if !found {
					for _, sym := range phase.Provides {
						if sym == input {
							found = true
							break
						}
					}
				}
				if !found {
					t.Errorf("Subquery input %s not available in phase", input)
				}
			}
		}
	}

	if !foundSubquery {
		t.Error("Expected to find a subquery in the plan")
	}
}

func TestPlanNestedSubqueries(t *testing.T) {
	// Query with nested subqueries
	query := `[:find ?dept ?avg-age
	           :where 
	           [?d :department/name ?dept]
	           [(q [:find (avg ?age)
	                :in $ ?d
	                :where 
	                  [(q [:find ?person ?a
	                       :in $ ?department
	                       :where [?person :person/department ?department]
	                              [?person :person/age ?a]]
	                      ?d) [[?person ?age]]]]
	               ?d) [[?avg-age]]]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Verify we can plan nested subqueries
	if len(plan.Phases) == 0 {
		t.Error("Expected at least one phase")
	}

	// The outer query should have a subquery
	foundSubquery := false
	for _, phase := range plan.Phases {
		if len(phase.Subqueries) > 0 {
			foundSubquery = true
			subqPlan := phase.Subqueries[0]

			if subqPlan.NestedPlan == nil {
				t.Error("Expected nested plan for outer subquery")
			} else {
				// The nested plan should also have a subquery
				foundNestedSubquery := false
				for _, nestedPhase := range subqPlan.NestedPlan.Phases {
					if len(nestedPhase.Subqueries) > 0 {
						foundNestedSubquery = true
						if nestedPhase.Subqueries[0].NestedPlan == nil {
							t.Error("Expected nested plan for inner subquery")
						}
					}
				}
				if !foundNestedSubquery {
					t.Error("Expected to find nested subquery in inner query plan")
				}
			}
		}
	}

	if !foundSubquery {
		t.Error("Expected to find a subquery in the plan")
	}
}
