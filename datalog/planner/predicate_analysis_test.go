package planner

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

// TestPredicatePlanCreation tests that predicates are properly analyzed and metadata populated
func TestPredicatePlanCreation(t *testing.T) {
	tests := []struct {
		name          string
		queryStr      string
		expectedPreds []struct {
			Type      PredicatePlanType
			Variable  query.Symbol
			Value     interface{}
			Operator  query.CompareOp
			TimeField string
		}
	}{
		{
			name: "EqualityPredicate",
			queryStr: `[:find ?x
			            :where
			            [?x :foo/bar ?y]
			            [(= ?y 42)]]`,
			expectedPreds: []struct {
				Type      PredicatePlanType
				Variable  query.Symbol
				Value     interface{}
				Operator  query.CompareOp
				TimeField string
			}{
				{
					Type:     PredicateEquality,
					Variable: "?y",
					Value:    int64(42),
					Operator: query.OpEQ,
				},
			},
		},
		{
			name: "ComparisonPredicates",
			queryStr: `[:find ?x
			            :where
			            [?x :foo/value ?v]
			            [(> ?v 100)]
			            [(< ?v 200)]]`,
			expectedPreds: []struct {
				Type      PredicatePlanType
				Variable  query.Symbol
				Value     interface{}
				Operator  query.CompareOp
				TimeField string
			}{
				{
					Type:     PredicateComparison,
					Variable: "?v",
					Value:    int64(100),
					Operator: query.OpGT,
				},
				{
					Type:     PredicateComparison,
					Variable: "?v",
					Value:    int64(200),
					Operator: query.OpLT,
				},
			},
		},
		{
			name: "ReversedComparison",
			queryStr: `[:find ?x
			            :where
			            [?x :foo/value ?v]
			            [(< 100 ?v)]]`,
			expectedPreds: []struct {
				Type      PredicatePlanType
				Variable  query.Symbol
				Value     interface{}
				Operator  query.CompareOp
				TimeField string
			}{
				{
					Type:     PredicateComparison,
					Variable: "?v",
					Value:    int64(100),
					Operator: query.OpGT, // Flipped from < to >
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := parser.ParseQuery(tt.queryStr)
			if err != nil {
				t.Fatalf("Failed to parse query: %v", err)
			}

			planner := NewPlanner(nil, PlannerOptions{})
			plan, err := planner.Plan(q)
			if err != nil {
				t.Fatalf("Failed to plan query: %v", err)
			}

			// Collect all predicates from all phases
			var foundPreds []PredicatePlan
			for _, phase := range plan.Phases {
				foundPreds = append(foundPreds, phase.Predicates...)
			}

			if len(foundPreds) != len(tt.expectedPreds) {
				t.Errorf("Expected %d predicates, got %d", len(tt.expectedPreds), len(foundPreds))
			}

			for i, expected := range tt.expectedPreds {
				if i >= len(foundPreds) {
					break
				}
				actual := foundPreds[i]

				if actual.Type != expected.Type {
					t.Errorf("Predicate %d: expected type %s, got %s", i, expected.Type, actual.Type)
				}
				if actual.Variable != expected.Variable {
					t.Errorf("Predicate %d: expected variable %s, got %s", i, expected.Variable, actual.Variable)
				}
				if actual.Value != expected.Value {
					t.Errorf("Predicate %d: expected value %v, got %v", i, expected.Value, actual.Value)
				}
				if actual.Operator != expected.Operator {
					t.Errorf("Predicate %d: expected operator %s, got %s", i, expected.Operator, actual.Operator)
				}
				if actual.TimeField != expected.TimeField {
					t.Errorf("Predicate %d: expected time field %s, got %s", i, expected.TimeField, actual.TimeField)
				}
			}
		})
	}
}

// TestTimeExtractionPredicates tests time extraction function analysis
func TestTimeExtractionPredicates(t *testing.T) {
	// Note: [(day ?t) ?d] is parsed as an ExpressionPattern, not a FunctionPattern
	// This test documents the current behavior
	queryStr := `[:find ?b
	              :where
	              [?b :price/time ?t]
	              [(day ?t) ?d]
	              [(= ?d 20)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Check what pattern types we got
	var dataPatterns, predicates, expressions int
	for _, pattern := range q.Where {
		switch pattern.(type) {
		case *query.DataPattern:
			dataPatterns++
		case query.Predicate:
			predicates++
		case *query.Expression:
			expressions++
		}
	}

	if dataPatterns != 1 {
		t.Errorf("Expected 1 data pattern, got %d", dataPatterns)
	}
	// We expect either FunctionPattern or Predicate
	if predicates != 1 {
		t.Errorf("Expected 1 predicate (equality), got %d", predicates)
	}
	// We expect either ExpressionPattern or Expression
	if expressions != 1 {
		t.Errorf("Expected 1 expression (day extraction), got %d", expressions)
	}

	// Plan the query
	planner := NewPlanner(nil, PlannerOptions{
		EnablePredicatePushdown: true,
	})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Check that the expression + equality created a time extraction constraint
	var foundTimeConstraint bool
	for _, phase := range plan.Phases {
		for _, pattern := range phase.Patterns {
			if pattern.Metadata != nil {
				if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
					for _, c := range constraints {
						if c.Type == ConstraintTimeExtraction && c.TimeField == "day" && c.Value == int64(20) {
							foundTimeConstraint = true
							t.Log("Found time extraction constraint: day = 20")
						}
					}
				}
			}
		}
	}

	if !foundTimeConstraint {
		t.Error("Expected [(day ?t) ?d] + [(= ?d 20)] to create a time extraction constraint")
	}
}

// TestPredicatePropagationWithConstraints tests that constraints are actually created
func TestPredicatePropagationWithConstraints(t *testing.T) {
	queryStr := `[:find ?b ?v
	              :where
	              [?b :price/symbol ?s]
	              [?b :price/volume ?v]
	              [(> ?v 1000000)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	t.Run("WithoutPushdown", func(t *testing.T) {
		planner := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: false,
		})
		plan, err := planner.Plan(q)
		if err != nil {
			t.Fatal(err)
		}

		// Without pushdown, no storage constraints
		for _, phase := range plan.Phases {
			for _, pattern := range phase.Patterns {
				if pattern.Metadata != nil {
					if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok && len(constraints) > 0 {
						t.Errorf("Expected no storage constraints without pushdown, got %d", len(constraints))
					}
				}
			}
		}

		// Predicates should remain at phase level
		hasPhasePredicates := false
		for _, phase := range plan.Phases {
			if len(phase.Predicates) > 0 {
				hasPhasePredicates = true
			}
		}
		if !hasPhasePredicates {
			t.Error("Expected predicates at phase level without pushdown")
		}
	})

	t.Run("WithPushdown", func(t *testing.T) {
		planner := NewPlanner(nil, PlannerOptions{
			EnablePredicatePushdown: true,
		})
		plan, err := planner.Plan(q)
		if err != nil {
			t.Fatal(err)
		}

		// With pushdown, should have storage constraints
		foundConstraint := false
		for _, phase := range plan.Phases {
			for _, pattern := range phase.Patterns {
				if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
					// Check if this is the volume pattern
					if len(dp.Elements) > 1 {
						if attr, ok := dp.Elements[1].(query.Constant); ok {
							if kw, ok := attr.Value.(datalog.Keyword); ok && kw.String() == ":price/volume" {
								// This pattern should have a constraint
								if pattern.Metadata != nil {
									if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok && len(constraints) > 0 {
										foundConstraint = true
										// Verify the constraint
										c := constraints[0]
										if c.Type != ConstraintRange {
											t.Errorf("Expected range constraint, got %s", c.Type)
										}
										if c.Operator != query.OpGT {
											t.Errorf("Expected > operator, got %s", c.Operator)
										}
										if c.Value != int64(1000000) {
											t.Errorf("Expected value 1000000, got %v", c.Value)
										}
									}
								}
							}
						}
					}
				}
			}
		}

		if !foundConstraint {
			t.Error("Expected storage constraint on volume pattern with pushdown")
		}
	})
}

// TestCrossPatternPropagation tests that constraints propagate across patterns with shared entities
func TestCrossPatternPropagation(t *testing.T) {
	queryStr := `[:find ?b ?h ?v
	              :where
	              [?b :price/high ?h]
	              [?b :price/volume ?v]
	              [(> ?h 150)]
	              [(> ?v 1000000)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnablePredicatePushdown: true,
	})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatal(err)
	}

	// Both patterns should have constraints since they share entity ?b
	constraintsByAttribute := make(map[string][]StorageConstraint)

	for _, phase := range plan.Phases {
		for _, pattern := range phase.Patterns {
			if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
				if len(dp.Elements) > 1 {
					if attr, ok := dp.Elements[1].(query.Constant); ok {
						if kw, ok := attr.Value.(datalog.Keyword); ok {
							attrStr := kw.String()
							if pattern.Metadata != nil {
								if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
									constraintsByAttribute[attrStr] = constraints
								}
							}
						}
					}
				}
			}
		}
	}

	// Check high pattern
	if highConstraints, found := constraintsByAttribute[":price/high"]; found {
		if len(highConstraints) != 1 {
			t.Errorf("Expected 1 constraint on :price/high, got %d", len(highConstraints))
		} else {
			c := highConstraints[0]
			if c.Type != ConstraintRange || c.Operator != query.OpGT || c.Value != int64(150) {
				t.Errorf("Wrong constraint on :price/high: %+v", c)
			}
		}
	} else {
		t.Error("Expected constraint on :price/high pattern")
	}

	// Check volume pattern
	if volConstraints, found := constraintsByAttribute[":price/volume"]; found {
		if len(volConstraints) != 1 {
			t.Errorf("Expected 1 constraint on :price/volume, got %d", len(volConstraints))
		} else {
			c := volConstraints[0]
			if c.Type != ConstraintRange || c.Operator != query.OpGT || c.Value != int64(1000000) {
				t.Errorf("Wrong constraint on :price/volume: %+v", c)
			}
		}
	} else {
		t.Error("Expected constraint on :price/volume pattern")
	}
}

// TestPredicateRemovalAfterPushdown verifies pushed predicates are removed from phase
func TestPredicateRemovalAfterPushdown(t *testing.T) {
	queryStr := `[:find ?b ?v
	              :where
	              [?b :price/volume ?v]
	              [(> ?v 1000000)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatal(err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnablePredicatePushdown: true,
	})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatal(err)
	}

	// After pushdown, the > predicate should be removed from phase predicates
	for _, phase := range plan.Phases {
		for _, pred := range phase.Predicates {
			if pred.Type == PredicateComparison && pred.Variable == "?v" {
				t.Error("Comparison predicate should have been removed after pushdown")
			}
		}
	}

	// But it should be in pattern metadata as a constraint
	foundInMetadata := false
	for _, phase := range plan.Phases {
		for _, pattern := range phase.Patterns {
			if pattern.Metadata != nil {
				if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
					for _, c := range constraints {
						if c.Type == ConstraintRange && c.Value == int64(1000000) {
							foundInMetadata = true
						}
					}
				}
			}
		}
	}

	if !foundInMetadata {
		t.Error("Predicate should have been converted to storage constraint")
	}
}
