package planner

import (
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestPlannerWithNewPredicateTypes(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		options  PlannerOptions
		validate func(*testing.T, *QueryPlan)
	}{
		{
			name: "simple predicates",
			query: `[:find ?e ?n 
			         :where 
			         [?e :person/name ?n]
			         [(< ?e 100)]
			         [(= ?n "Alice")]]`,
			options: PlannerOptions{},
			validate: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Phases) != 1 {
					t.Errorf("Expected 1 phase, got %d", len(plan.Phases))
				}

				phase := plan.Phases[0]
				if len(phase.Predicates) != 2 {
					t.Errorf("Expected 2 predicates, got %d", len(phase.Predicates))
				}

				// Check first predicate is comparison
				pred1 := phase.Predicates[0]
				if pred1.Type != PredicateComparison {
					t.Errorf("Expected first predicate type 'comparison', got %s", pred1.Type)
				}
				if _, ok := pred1.Predicate.(*query.Comparison); !ok {
					t.Errorf("Expected Comparison type, got %T", pred1.Predicate)
				}

				// Check second predicate is equality
				pred2 := phase.Predicates[1]
				if pred2.Type != PredicateEquality {
					t.Errorf("Expected second predicate type 'equality', got %s", pred2.Type)
				}
				if comp, ok := pred2.Predicate.(*query.Comparison); !ok {
					t.Errorf("Expected Comparison type, got %T", pred2.Predicate)
				} else if comp.Op != query.OpEQ {
					t.Errorf("Expected OpEQ, got %v", comp.Op)
				}
			},
		},
		{
			name: "time extraction with pushdown",
			query: `[:find ?s ?t ?o
			         :where 
			         [?b :price/symbol ?s]
			         [?b :price/time ?t]
			         [?b :price/open ?o]
			         [(day ?t) ?d]
			         [(month ?t) ?m]
			         [(= ?d 20)]
			         [(= ?m 6)]]`,
			options: PlannerOptions{
				EnablePredicatePushdown: true,
			},
			validate: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Phases) != 1 {
					t.Errorf("Expected 1 phase, got %d", len(plan.Phases))
				}

				phase := plan.Phases[0]

				// Check expressions are TimeExtractionFunction
				if len(phase.Expressions) != 2 {
					t.Errorf("Expected 2 expressions, got %d", len(phase.Expressions))
				}
				for i, expr := range phase.Expressions {
					if _, ok := expr.Expression.Function.(*query.TimeExtractionFunction); !ok {
						t.Errorf("Expression %d: expected TimeExtractionFunction, got %T", i, expr.Expression.Function)
					}
				}

				// Check that predicates were pushed to storage
				// When pushdown is enabled, time extraction predicates should be combined
				// and pushed to storage constraints
				hasTimeConstraints := false
				for _, pattern := range phase.Patterns {
					if pattern.Metadata != nil {
						if constraints, ok := pattern.Metadata["storage_constraints"]; ok {
							hasTimeConstraints = true
							// Verify constraints are properly typed
							if constraintList, ok := constraints.([]StorageConstraint); ok {
								for _, c := range constraintList {
									if c.Type == ConstraintTimeExtraction {
										t.Logf("Found time extraction constraint: field=%s, value=%v", c.TimeField, c.Value)
									}
								}
							}
						}
					}
				}

				if !hasTimeConstraints {
					t.Error("Expected time extraction constraints to be pushed to storage")
				}

				// After pushdown, there should be no predicates left (they were pushed to storage)
				if len(phase.Predicates) != 0 {
					t.Errorf("Expected 0 predicates after pushdown, got %d", len(phase.Predicates))
				}
			},
		},
		{
			name: "chained comparison",
			query: `[:find ?x
			         :where 
			         [?e :value ?x]
			         [(< 0 ?x 100)]]`,
			options: PlannerOptions{},
			validate: func(t *testing.T, plan *QueryPlan) {
				phase := plan.Phases[0]
				if len(phase.Predicates) != 1 {
					t.Errorf("Expected 1 predicate, got %d", len(phase.Predicates))
				}

				pred := phase.Predicates[0]
				if pred.Type != PredicateChainedComparison {
					t.Errorf("Expected 'chained_comparison', got %s", pred.Type)
				}
				if _, ok := pred.Predicate.(*query.ChainedComparison); !ok {
					t.Errorf("Expected ChainedComparison type, got %T", pred.Predicate)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.query)
			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			p := NewPlanner(nil, tt.options)
			plan, err := p.Plan(q)
			if err != nil {
				t.Fatalf("Plan error: %v", err)
			}

			tt.validate(t, plan)
		})
	}
}

func TestPredicateInterfaceEval(t *testing.T) {
	// Test that the predicates can be evaluated using the interface
	ednQuery := `[:find ?x ?y
	              :where 
	              [?e :foo ?x]
	              [?e :bar ?y]
	              [(< ?x 10)]
	              [(= ?y "test")]]`

	q, err := parser.ParseQuery(ednQuery)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	p := NewPlanner(nil, PlannerOptions{})
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatalf("Plan error: %v", err)
	}

	phase := plan.Phases[0]

	// Test that predicates implement the interface correctly
	bindings := map[query.Symbol]interface{}{
		"?x": 5,
		"?y": "test",
	}

	for _, predPlan := range phase.Predicates {
		pred := predPlan.Predicate

		// Verify it has the required methods
		result, err := pred.Eval(bindings)
		if err != nil {
			t.Errorf("Eval error: %v", err)
		}

		// Check specific results
		if comp, ok := pred.(*query.Comparison); ok {
			if comp.Op == query.OpLT {
				// (< ?x 10) with ?x=5 should be true
				if !result {
					t.Error("Expected (< 5 10) to be true")
				}
			} else if comp.Op == query.OpEQ {
				// (= ?y "test") with ?y="test" should be true
				if !result {
					t.Error("Expected (= 'test' 'test') to be true")
				}
			}
		}

		// Check other interface methods
		_ = pred.RequiredSymbols()
		_ = pred.Selectivity()
		_ = pred.CanPushToStorage()
	}
}
