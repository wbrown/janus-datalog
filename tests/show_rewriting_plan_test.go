package tests

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

func TestShowRewritingPlan(t *testing.T) {
	queryStr := `[:find ?name ?day ?max-value
	             :where
	             [?p :person/name ?name]
	             [?e :event/person ?p]
	             [?e :event/time ?time]
	             [(day ?time) ?day]

	             ; Subquery: max value for this person and day
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
		t.Fatalf("Failed to parse query: %v", err)
	}

	t.Log("=== WITHOUT REWRITING ===")
	opts1 := planner.PlannerOptions{
		EnableDynamicReordering:             true,
		EnableConditionalAggregateRewriting: false,
	}
	p1 := planner.NewPlanner(nil, opts1)
	plan1, err := p1.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed: %v", err)
	}
	t.Logf("\n%s", plan1.String())

	t.Log("\n=== WITH REWRITING ===")
	opts2 := planner.PlannerOptions{
		EnableDynamicReordering:             true,
		EnableConditionalAggregateRewriting: true,
	}
	p2 := planner.NewPlanner(nil, opts2)
	plan2, err := p2.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed: %v", err)
	}
	t.Logf("\n%s", plan2.String())
}
