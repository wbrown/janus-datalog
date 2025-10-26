package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestExtractCorrelationSignature(t *testing.T) {
	// Parse a simple subquery
	queryStr := `[:find (max ?h)
	              :in $ ?year ?month ?day ?hour
	              :where
	                [?b :price/symbol ?sym]
	                [?b :price/time ?time]
	                [(year ?time) ?y]
	                [(= ?y ?year)]
	                [(month ?time) ?m]
	                [(= ?m ?month)]
	                [(day ?time) ?d]
	                [(= ?d ?day)]
	                [(hour ?time) ?hr]
	                [(= ?hr ?hour)]
	                [?b :price/high ?h]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Plan the query
	planner := NewPlanner(nil, PlannerOptions{})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Create a subquery plan
	subqPlan := SubqueryPlan{
		Inputs:     []query.Symbol{"?year", "?month", "?day", "?hour"},
		NestedPlan: plan,
	}

	// Extract signature
	sig := extractCorrelationSignature(&subqPlan)

	// Verify signature - pure aggregations should NOT be marked for decorrelation
	// This query is [:find (max ?h)] - a pure aggregation with no grouping variables
	// The fix ensures IsAggregate=false because decorrelating pure aggregations breaks them
	if sig.IsAggregate {
		t.Error("Expected pure aggregation to NOT be marked for decorrelation (IsAggregate should be false)")
	}

	if len(sig.CorrelationVars) != 4 {
		t.Errorf("Expected 4 correlation vars, got %d", len(sig.CorrelationVars))
	}

	// Check correlation vars
	expectedVars := map[query.Symbol]bool{
		"?year":  true,
		"?month": true,
		"?day":   true,
		"?hour":  true,
	}

	for _, v := range sig.CorrelationVars {
		if !expectedVars[v] {
			t.Errorf("Unexpected correlation var: %v", v)
		}
	}
}

func TestDetectDecorrelationOpportunities(t *testing.T) {
	// Create two subqueries with same correlation signature
	subq1 := `[:find (max ?h) (min ?l)
	           :in $ ?year ?month ?day ?hour
	           :where
	             [?b :price/symbol ?sym]
	             [?b :price/time ?time]
	             [(year ?time) ?y] [(= ?y ?year)]
	             [(month ?time) ?m] [(= ?m ?month)]
	             [(day ?time) ?d] [(= ?d ?day)]
	             [(hour ?time) ?hr] [(= ?hr ?hour)]
	             [?b :price/high ?h]
	             [?b :price/low ?l]]`

	subq2 := `[:find (sum ?v)
	           :in $ ?year ?month ?day ?hour
	           :where
	             [?b :price/symbol ?sym]
	             [?b :price/time ?time]
	             [(year ?time) ?y] [(= ?y ?year)]
	             [(month ?time) ?m] [(= ?m ?month)]
	             [(day ?time) ?d] [(= ?d ?day)]
	             [(hour ?time) ?hr] [(= ?hr ?hour)]
	             [?b :price/volume ?v]]`

	// Parse and plan both queries
	planner := NewPlanner(nil, PlannerOptions{})

	q1, err := parser.ParseQuery(subq1)
	if err != nil {
		t.Fatalf("Failed to parse subq1: %v", err)
	}
	plan1, err := planner.Plan(q1)
	if err != nil {
		t.Fatalf("Failed to plan subq1: %v", err)
	}

	q2, err := parser.ParseQuery(subq2)
	if err != nil {
		t.Fatalf("Failed to parse subq2: %v", err)
	}
	plan2, err := planner.Plan(q2)
	if err != nil {
		t.Fatalf("Failed to plan subq2: %v", err)
	}

	// Create phase with both subqueries
	phase := Phase{
		Subqueries: []SubqueryPlan{
			{
				Inputs:     []query.Symbol{"?year", "?month", "?day", "?hour"},
				NestedPlan: plan1,
			},
			{
				Inputs:     []query.Symbol{"?year", "?month", "?day", "?hour"},
				NestedPlan: plan2,
			},
		},
	}

	// Debug: Check signatures
	sig1 := extractCorrelationSignature(&phase.Subqueries[0])
	sig2 := extractCorrelationSignature(&phase.Subqueries[1])

	t.Logf("Sig1: IsAgg=%v, Vars=%v, Hash=%v", sig1.IsAggregate, sig1.CorrelationVars, sig1.Hash())
	t.Logf("Sig2: IsAgg=%v, Vars=%v, Hash=%v", sig2.IsAggregate, sig2.CorrelationVars, sig2.Hash())

	// Detect opportunities
	opportunities := detectDecorrelationOpportunities(&phase)

	t.Logf("Found %d opportunities", len(opportunities))

	// These are PURE aggregations ([:find (max ?h) (min ?l)] and [:find (sum ?v)])
	// After the fix, pure aggregations should NOT be decorrelated
	// Therefore we expect 0 opportunities (not 1)
	if len(opportunities) != 0 {
		t.Fatalf("Expected 0 opportunities (pure aggregations should not be decorrelated), got %d", len(opportunities))
	}
}

func TestDetectDecorrelationOpportunities_GroupedAggregates(t *testing.T) {
	// Create two GROUPED aggregate subqueries (have both aggregates AND grouping variables)
	// These SHOULD be decorrelated because they're grouped, not pure
	subq1 := `[:find ?sym (max ?h)
	           :in $ ?year ?month
	           :where
	             [?b :price/symbol ?sym]
	             [?b :price/time ?time]
	             [(year ?time) ?y] [(= ?y ?year)]
	             [(month ?time) ?m] [(= ?m ?month)]
	             [?b :price/high ?h]]`

	subq2 := `[:find ?sym (sum ?v)
	           :in $ ?year ?month
	           :where
	             [?b :price/symbol ?sym]
	             [?b :price/time ?time]
	             [(year ?time) ?y] [(= ?y ?year)]
	             [(month ?time) ?m] [(= ?m ?month)]
	             [?b :price/volume ?v]]`

	// Parse and plan
	planner := NewPlanner(nil, PlannerOptions{})

	q1, _ := parser.ParseQuery(subq1)
	plan1, _ := planner.Plan(q1)

	q2, _ := parser.ParseQuery(subq2)
	plan2, _ := planner.Plan(q2)

	// Create phase
	phase := Phase{
		Subqueries: []SubqueryPlan{
			{
				Inputs:     []query.Symbol{"?year", "?month"},
				NestedPlan: plan1,
			},
			{
				Inputs:     []query.Symbol{"?year", "?month"},
				NestedPlan: plan2,
			},
		},
	}

	// Detect - should find ONE opportunity (grouped aggregates CAN be decorrelated)
	opportunities := detectDecorrelationOpportunities(&phase)

	if len(opportunities) != 1 {
		t.Errorf("Expected 1 opportunity (grouped aggregates can be decorrelated), got %d", len(opportunities))
	}

	if len(opportunities) > 0 && len(opportunities[0].Subqueries) != 2 {
		t.Errorf("Expected 2 subqueries in opportunity, got %d", len(opportunities[0].Subqueries))
	}
}

func TestDetectDecorrelationOpportunities_DifferentKeys(t *testing.T) {
	// Create two subqueries with DIFFERENT correlation keys
	subq1 := `[:find (max ?h)
	           :in $ ?year ?month
	           :where
	             [?b :price/time ?time]
	             [(year ?time) ?y] [(= ?y ?year)]
	             [(month ?time) ?m] [(= ?m ?month)]
	             [?b :price/high ?h]]`

	subq2 := `[:find (sum ?v)
	           :in $ ?year ?month ?day
	           :where
	             [?b :price/time ?time]
	             [(year ?time) ?y] [(= ?y ?year)]
	             [(month ?time) ?m] [(= ?m ?month)]
	             [(day ?time) ?d] [(= ?d ?day)]
	             [?b :price/volume ?v]]`

	// Parse and plan
	planner := NewPlanner(nil, PlannerOptions{})

	q1, _ := parser.ParseQuery(subq1)
	plan1, _ := planner.Plan(q1)

	q2, _ := parser.ParseQuery(subq2)
	plan2, _ := planner.Plan(q2)

	// Create phase
	phase := Phase{
		Subqueries: []SubqueryPlan{
			{
				Inputs:     []query.Symbol{"?year", "?month"},
				NestedPlan: plan1,
			},
			{
				Inputs:     []query.Symbol{"?year", "?month", "?day"},
				NestedPlan: plan2,
			},
		},
	}

	// Detect - should find NO opportunities (different keys)
	opportunities := detectDecorrelationOpportunities(&phase)

	if len(opportunities) != 0 {
		t.Errorf("Expected 0 opportunities (different keys), got %d", len(opportunities))
	}
}

func TestDetectDecorrelationOpportunities_SingleSubquery(t *testing.T) {
	// Single subquery should not be decorrelated
	subq1 := `[:find (max ?h)
	           :in $ ?year ?month
	           :where
	             [?b :price/time ?time]
	             [(year ?time) ?y] [(= ?y ?year)]
	             [(month ?time) ?m] [(= ?m ?month)]
	             [?b :price/high ?h]]`

	planner := NewPlanner(nil, PlannerOptions{})
	q1, _ := parser.ParseQuery(subq1)
	plan1, _ := planner.Plan(q1)

	phase := Phase{
		Subqueries: []SubqueryPlan{
			{
				Inputs:     []query.Symbol{"?year", "?month"},
				NestedPlan: plan1,
			},
		},
	}

	// Should find NO opportunities (only 1 subquery)
	opportunities := detectDecorrelationOpportunities(&phase)

	if len(opportunities) != 0 {
		t.Errorf("Expected 0 opportunities (single subquery), got %d", len(opportunities))
	}
}
