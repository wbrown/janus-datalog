package planner

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

func TestPredicatePropagation(t *testing.T) {
	t.Run("BasicEntityPropagation", func(t *testing.T) {
		// Setup: Two patterns sharing entity ?b with time predicate
		// [?b :price/symbol ?s]
		// [?b :price/time ?t]
		// [(day ?t) ?d]
		// [(= ?d 20)]

		phase := Phase{
			Patterns: []PatternPlan{
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/symbol")},
							query.Variable{Name: "?s"},
						},
					},
				},
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/time")},
							query.Variable{Name: "?t"},
						},
					},
				},
			},
			Predicates: []PredicatePlan{
				{
					Type:         PredicateTimeExtraction,
					Variable:     "?t",
					TimeField:    "day",
					Value:        20,
					Operator:     query.OpEQ,
					RequiredVars: []query.Symbol{"?t"},
				},
			},
		}

		phase.PushPredicates()

		// Check that the time predicate was propagated to the time pattern
		timePattern := phase.Patterns[1]
		if timePattern.Metadata == nil {
			t.Fatal("Expected metadata on time pattern")
		}

		constraints, ok := timePattern.Metadata["storage_constraints"].([]StorageConstraint)
		if !ok || len(constraints) == 0 {
			t.Fatal("Expected storage constraints on time pattern")
		}

		// Verify the constraint
		constraint := constraints[0]
		if constraint.Type != ConstraintTimeExtraction {
			t.Errorf("Expected time_extraction constraint, got %s", constraint.Type)
		}
		if constraint.TimeField != "day" {
			t.Errorf("Expected day field, got %s", constraint.TimeField)
		}
		if constraint.Value != 20 {
			t.Errorf("Expected value 20, got %v", constraint.Value)
		}
	})

	t.Run("CrossPatternPropagation", func(t *testing.T) {
		// Test that predicates from one pattern can be pushed to another via shared entity
		// [?b :price/symbol AAPL]
		// [?b :price/high ?h]
		// [(> ?h 150)]

		phase := Phase{
			Patterns: []PatternPlan{
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/symbol")},
							query.Constant{Value: datalog.NewIdentity("symbol:AAPL")},
						},
					},
				},
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/high")},
							query.Variable{Name: "?h"},
						},
					},
				},
			},
			Predicates: []PredicatePlan{
				{
					Type:         PredicateComparison,
					Variable:     "?h",
					Value:        150.0,
					Operator:     query.OpGT,
					RequiredVars: []query.Symbol{"?h"},
				},
			},
		}

		phase.PushPredicates()

		// The high > 150 predicate should be pushed to the high pattern
		highPattern := phase.Patterns[1]
		constraints, ok := highPattern.Metadata["storage_constraints"].([]StorageConstraint)
		if !ok || len(constraints) == 0 {
			t.Fatal("Expected storage constraints on high pattern")
		}

		constraint := constraints[0]
		if constraint.Type != ConstraintRange {
			t.Errorf("Expected range constraint, got %s", constraint.Type)
		}
		if constraint.Operator != query.OpGT {
			t.Errorf("Expected > operator, got %s", constraint.Operator)
		}
		if constraint.Value != 150.0 {
			t.Errorf("Expected value 150.0, got %v", constraint.Value)
		}
	})

	t.Run("MultiplePredicatePropagation", func(t *testing.T) {
		// Test multiple predicates on same entity
		// [?b :price/symbol ?s]
		// [?b :price/time ?t]
		// [?b :price/volume ?v]
		// [(day ?t) ?d]
		// [(= ?d 20)]
		// [(> ?v 1000000)]

		phase := Phase{
			Patterns: []PatternPlan{
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/symbol")},
							query.Variable{Name: "?s"},
						},
					},
				},
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/time")},
							query.Variable{Name: "?t"},
						},
					},
				},
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/volume")},
							query.Variable{Name: "?v"},
						},
					},
				},
			},
			Predicates: []PredicatePlan{
				{
					Type:         PredicateTimeExtraction,
					Variable:     "?t",
					TimeField:    "day",
					Value:        20,
					Operator:     query.OpEQ,
					RequiredVars: []query.Symbol{"?t"},
				},
				{
					Type:         PredicateComparison,
					Variable:     "?v",
					Value:        int64(1000000),
					Operator:     query.OpGT,
					RequiredVars: []query.Symbol{"?v"},
				},
			},
		}

		phase.PushPredicates()

		// Check time pattern has time constraint
		timePattern := phase.Patterns[1]
		timeConstraints, _ := timePattern.Metadata["storage_constraints"].([]StorageConstraint)
		if len(timeConstraints) != 1 {
			t.Fatalf("Expected 1 constraint on time pattern, got %d", len(timeConstraints))
		}

		// Check volume pattern has volume constraint
		volumePattern := phase.Patterns[2]
		volumeConstraints, _ := volumePattern.Metadata["storage_constraints"].([]StorageConstraint)
		if len(volumeConstraints) != 1 {
			t.Fatalf("Expected 1 constraint on volume pattern, got %d", len(volumeConstraints))
		}

		if volumeConstraints[0].Operator != query.OpGT {
			t.Errorf("Expected > operator on volume, got %s", volumeConstraints[0].Operator)
		}
	})

	t.Run("NoSharedEntityNoPropagation", func(t *testing.T) {
		// Patterns with different entities shouldn't share predicates
		// [?a :user/name ?name]
		// [?b :price/high ?h]
		// [(> ?h 150)]

		phase := Phase{
			Patterns: []PatternPlan{
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?a"},
							query.Constant{Value: datalog.NewKeyword(":user/name")},
							query.Variable{Name: "?name"},
						},
					},
				},
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/high")},
							query.Variable{Name: "?h"},
						},
					},
				},
			},
			Predicates: []PredicatePlan{
				{
					Type:         PredicateComparison,
					Variable:     "?h",
					Value:        150.0,
					Operator:     query.OpGT,
					RequiredVars: []query.Symbol{"?h"},
				},
			},
		}

		phase.PushPredicates()

		// Only the price pattern should have constraints
		userPattern := phase.Patterns[0]
		if userPattern.Metadata != nil {
			if constraints, ok := userPattern.Metadata["storage_constraints"].([]StorageConstraint); ok && len(constraints) > 0 {
				t.Error("User pattern shouldn't have constraints from price predicate")
			}
		}

		pricePattern := phase.Patterns[1]
		constraints, _ := pricePattern.Metadata["storage_constraints"].([]StorageConstraint)
		if len(constraints) != 1 {
			t.Fatalf("Expected 1 constraint on price pattern, got %d", len(constraints))
		}
	})

	t.Run("PredicateRemoval", func(t *testing.T) {
		// Test that pushed predicates are removed from phase predicates
		phase := Phase{
			Patterns: []PatternPlan{
				{
					Pattern: &query.DataPattern{
						Elements: []query.PatternElement{
							query.Variable{Name: "?b"},
							query.Constant{Value: datalog.NewKeyword(":price/time")},
							query.Variable{Name: "?t"},
						},
					},
				},
			},
			Predicates: []PredicatePlan{
				{
					Type:         PredicateTimeExtraction,
					Variable:     "?t",
					TimeField:    "day",
					Value:        20,
					Operator:     query.OpEQ,
					RequiredVars: []query.Symbol{"?t"},
				},
			},
		}

		phase.PushPredicates()

		// The predicate should be removed from phase predicates after being pushed
		if len(phase.Predicates) != 0 {
			t.Errorf("Expected predicates to be removed after pushing, got %d", len(phase.Predicates))
		}
	})
}

func TestSelectivityEstimation(t *testing.T) {
	tests := []struct {
		name        string
		constraints []StorageConstraint
		expected    float64
	}{
		{
			name:        "NoConstraints",
			constraints: []StorageConstraint{},
			expected:    1.0,
		},
		{
			name: "EqualityConstraint",
			constraints: []StorageConstraint{
				{Type: ConstraintEquality, Operator: query.OpEQ},
			},
			expected: 0.01,
		},
		{
			name: "RangeConstraint",
			constraints: []StorageConstraint{
				{Type: ConstraintRange, Operator: query.OpGT},
			},
			expected: 0.2,
		},
		{
			name: "DayExtraction",
			constraints: []StorageConstraint{
				{Type: ConstraintTimeExtraction, TimeField: "day"},
			},
			expected: 1.0 / 30,
		},
		{
			name: "MultipleConstraints",
			constraints: []StorageConstraint{
				{Type: ConstraintEquality, Operator: query.OpEQ},   // 0.01
				{Type: ConstraintTimeExtraction, TimeField: "day"}, // * 1/30
			},
			expected: 0.01 / 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pattern := PatternPlan{
				Metadata: map[string]interface{}{
					"storage_constraints": tt.constraints,
				},
			}

			selectivity := analyzeSelectivity(pattern)

			// Allow small floating point differences
			if diff := selectivity - tt.expected; diff < -0.0001 || diff > 0.0001 {
				t.Errorf("Expected selectivity %f, got %f", tt.expected, selectivity)
			}
		})
	}
}

func TestGroupByEntityVariable(t *testing.T) {
	patterns := []PatternPlan{
		{
			Pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?a"},
					query.Constant{Value: datalog.NewKeyword(":user/name")},
					query.Variable{Name: "?name"},
				},
			},
		},
		{
			Pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?b"},
					query.Constant{Value: datalog.NewKeyword(":price/symbol")},
					query.Variable{Name: "?s"},
				},
			},
		},
		{
			Pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?b"},
					query.Constant{Value: datalog.NewKeyword(":price/time")},
					query.Variable{Name: "?t"},
				},
			},
		},
	}

	groups := groupPatternsByEntity(patterns)

	// Should have 2 groups: ?a with 1 pattern, ?b with 2 patterns
	if len(groups) != 2 {
		t.Fatalf("Expected 2 groups, got %d", len(groups))
	}

	if len(groups["?a"]) != 1 {
		t.Errorf("Expected 1 pattern for ?a, got %d", len(groups["?a"]))
	}

	if len(groups["?b"]) != 2 {
		t.Errorf("Expected 2 patterns for ?b, got %d", len(groups["?b"]))
	}
}
