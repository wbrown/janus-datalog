package tests

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestDebugRewriting - minimal test to debug why pattern isn't detected
func TestDebugRewriting(t *testing.T) {
	// Use the EXACT query from the unit test that works
	queryStr := `[:find ?name ?day ?max-value
	             :where
	             [?p :person/name ?name]
	             [?e :event/person ?p]
	             [?e :event/time ?time]
	             [(day ?time) ?day]

	             [(q [:find (max ?v)
	                  :in $ ?person ?d
	                  :where
	                  [?ev :event/person ?person]
	                  [?ev :event/time ?t]
	                  [(day ?t) ?pd]
	                  [(= ?pd ?d)]
	                  [?ev :event/value ?v]]
	               $ ?p ?day) [[?max-value]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	// Plan WITHOUT rewriting
	stats := &planner.Statistics{}
	p1 := planner.NewPlanner(stats, planner.PlannerOptions{
		EnableConditionalAggregateRewriting: false,
	})
	plan1, err := p1.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed (no rewriting): %v", err)
	}

	t.Logf("Plan without rewriting: %d phases", len(plan1.Phases))
	for i, phase := range plan1.Phases {
		t.Logf("  Phase %d: %d patterns, %d subqueries", i, len(phase.Patterns), len(phase.Subqueries))
		if len(phase.Subqueries) > 0 {
			subq := phase.Subqueries[0]
			t.Logf("    Subquery 0: decorrelated=%v, inputs=%d",
				subq.Decorrelated, len(subq.Inputs))
		}
	}

	// Plan WITH rewriting
	p2 := planner.NewPlanner(stats, planner.PlannerOptions{
		EnableConditionalAggregateRewriting: true,
	})
	plan2, err := p2.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed (with rewriting): %v", err)
	}

	t.Logf("Plan with rewriting: %d phases", len(plan2.Phases))

	// Check metadata in ALL phases
	foundRewriting := false
	for i, phase := range plan2.Phases {
		t.Logf("  Phase %d: %d patterns, %d subqueries", i, len(phase.Patterns), len(phase.Subqueries))
		if phase.Metadata != nil {
			if condAggs, ok := phase.Metadata["conditional_aggregates"]; ok {
				t.Logf("    ✓ Conditional aggregates found in phase %d metadata", i)
				foundRewriting = true
				_ = condAggs
			}
		}
	}

	if !foundRewriting {
		t.Error("❌ No conditional aggregates found in any phase metadata")
	}
}
