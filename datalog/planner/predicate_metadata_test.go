package planner

import (
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
)

// TestPredicatePlanMetadataGeneration verifies that predicates get proper metadata
// This documents the expected metadata for different predicate patterns
func TestPredicatePlanMetadataGeneration(t *testing.T) {
	tests := []struct {
		name          string
		queryStr      string
		checkPhase    int // Which phase to check
		expectedPreds []struct {
			predicateStr string
			hasType      bool
			typeVal      string
			hasVariable  bool
			varVal       query.Symbol
			hasValue     bool
			value        interface{}
		}
	}{
		{
			name: "SimpleEqualityPredicate",
			queryStr: `[:find ?d
			            :where
			            [?x :foo/bar ?d]
			            [(= ?d 20)]]`,
			checkPhase: 0,
			expectedPreds: []struct {
				predicateStr string
				hasType      bool
				typeVal      string
				hasVariable  bool
				varVal       query.Symbol
				hasValue     bool
				value        interface{}
			}{
				{
					predicateStr: "[(= ?d 20)]",
					hasType:      true,
					typeVal:      "equality",
					hasVariable:  true,
					varVal:       "?d",
					hasValue:     true,
					value:        int64(20),
				},
			},
		},
		{
			name: "ComparisonPredicate",
			queryStr: `[:find ?v
			            :where
			            [?x :foo/value ?v]
			            [(> ?v 100)]]`,
			checkPhase: 0,
			expectedPreds: []struct {
				predicateStr string
				hasType      bool
				typeVal      string
				hasVariable  bool
				varVal       query.Symbol
				hasValue     bool
				value        interface{}
			}{
				{
					predicateStr: "[(> ?v 100)]",
					hasType:      true,
					typeVal:      "comparison",
					hasVariable:  true,
					varVal:       "?v",
					hasValue:     true,
					value:        int64(100),
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

			if tt.checkPhase >= len(plan.Phases) {
				t.Fatalf("Phase %d doesn't exist (only %d phases)", tt.checkPhase, len(plan.Phases))
			}

			phase := plan.Phases[tt.checkPhase]
			if len(phase.Predicates) != len(tt.expectedPreds) {
				t.Errorf("Expected %d predicates in phase %d, got %d",
					len(tt.expectedPreds), tt.checkPhase, len(phase.Predicates))
			}

			for i, expected := range tt.expectedPreds {
				if i >= len(phase.Predicates) {
					break
				}
				pred := phase.Predicates[i]

				// Check predicate string representation
				if pred.Predicate != nil && pred.Predicate.String() != expected.predicateStr {
					t.Errorf("Predicate %d: expected %s, got %s",
						i, expected.predicateStr, pred.Predicate.String())
				}

				// Check Type field
				if expected.hasType {
					if pred.Type.String() != expected.typeVal {
						t.Errorf("Predicate %d: expected Type=%s, got %s",
							i, expected.typeVal, pred.Type.String())
					}
				} else if pred.Type != PredicateUnknown {
					t.Errorf("Predicate %d: expected no Type, got %s", i, pred.Type.String())
				}

				// Check Variable field
				if expected.hasVariable {
					if pred.Variable != expected.varVal {
						t.Errorf("Predicate %d: expected Variable=%s, got %s",
							i, expected.varVal, pred.Variable)
					}
				} else if pred.Variable != "" {
					t.Errorf("Predicate %d: expected no Variable, got %s", i, pred.Variable)
				}

				// Check Value field
				if expected.hasValue {
					if pred.Value != expected.value {
						t.Errorf("Predicate %d: expected Value=%v, got %v",
							i, expected.value, pred.Value)
					}
				} else if pred.Value != nil {
					t.Errorf("Predicate %d: expected no Value, got %v", i, pred.Value)
				}
			}
		})
	}
}

// TestExpressionPatternRecognition verifies that time extraction patterns are recognized as expressions
func TestExpressionPatternRecognition(t *testing.T) {
	queryStr := `[:find ?b ?d
	              :where
	              [?b :price/time ?t]
	              [(day ?t) ?d]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Count pattern types
	var dataPatterns, expressions, functions int
	for _, pattern := range q.Where {
		t.Logf("Pattern type: %T", pattern)
		switch pattern.(type) {
		case *query.DataPattern:
			dataPatterns++
		case query.Predicate:
			functions++
		case *query.Expression:
			t.Logf("Found new Expression type")
			expressions++
		}
	}

	if dataPatterns != 1 {
		t.Errorf("Expected 1 DataPattern, got %d", dataPatterns)
	}
	// Parser still uses ExpressionPattern, so this test is still valid
	if expressions != 1 {
		t.Errorf("Expected 1 ExpressionPattern for [(day ?t) ?d], got %d", expressions)
	}
	if functions != 0 {
		t.Errorf("Expected 0 FunctionPatterns, got %d", functions)
	}

	// Verify the expression details
	for _, pattern := range q.Where {
		if expr, ok := pattern.(*query.Expression); ok {
			if fn, ok := expr.Function.(*query.TimeExtractionFunction); ok {
				if fn.Field != "day" {
					t.Errorf("Expected expression function 'day', got %s", fn.Field)
				}
			}
			if expr.Binding != "?d" {
				t.Errorf("Expected expression binding '?d', got %s", expr.Binding)
			}
		}
	}
}

// TestOHLCQueryPhaseStructure documents exactly how the OHLC query is structured in phases
func TestOHLCQueryPhaseStructure(t *testing.T) {
	queryStr := `[:find ?b ?t ?h ?l ?c ?v
	              :where 
	              [?s :symbol/ticker "CRWV"]
	              [?b :price/symbol ?s]
	              [?b :price/time ?t]
	              [?b :price/high ?h]
	              [?b :price/low ?l]
	              [?b :price/close ?c]
	              [?b :price/volume ?v]
	              [(day ?t) ?d]
	              [(= ?d 20)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	planner := NewPlanner(nil, PlannerOptions{
		EnablePredicatePushdown: true,
	})
	plan, err := planner.Plan(q)
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	// Document the phase structure
	t.Logf("OHLC Query produces %d phases", len(plan.Phases))

	for i, phase := range plan.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		t.Logf("  Expressions: %d", len(phase.Expressions))
		t.Logf("  Predicates: %d", len(phase.Predicates))

		// Log expressions
		for j, expr := range phase.Expressions {
			t.Logf("    Expression %d: Function=%s, Output=%s",
				j, expr.Expression.Function, expr.Output)
		}

		// Log predicates with metadata
		for j, pred := range phase.Predicates {
			t.Logf("    Predicate %d: %v", j, pred.Predicate)
			t.Logf("      Type=%s, Variable=%s, Value=%v, Operator=%s, TimeField=%s",
				pred.Type, pred.Variable, pred.Value, pred.Operator, pred.TimeField)
		}

		// Check for storage constraints
		constraintCount := 0
		for _, pattern := range phase.Patterns {
			if pattern.Metadata != nil {
				if constraints, ok := pattern.Metadata["storage_constraints"].([]StorageConstraint); ok {
					constraintCount += len(constraints)
				}
			}
		}
		t.Logf("  Storage constraints pushed: %d", constraintCount)
	}

	// Verify expected structure
	if len(plan.Phases) != 2 {
		t.Errorf("Expected 2 phases, got %d", len(plan.Phases))
	}

	// Phase 0: ticker pattern
	if len(plan.Phases) > 0 {
		phase0 := plan.Phases[0]
		if len(phase0.Patterns) != 1 {
			t.Errorf("Phase 0: expected 1 pattern, got %d", len(phase0.Patterns))
		}
	}

	// Phase 1: all price patterns
	if len(plan.Phases) > 1 {
		phase1 := plan.Phases[1]
		if len(phase1.Patterns) != 6 {
			t.Errorf("Phase 1: expected 6 patterns, got %d", len(phase1.Patterns))
		}
		if len(phase1.Expressions) != 1 {
			t.Errorf("Phase 1: expected 1 expression [(day ?t) ?d], got %d", len(phase1.Expressions))
		}
		// The predicate is consumed when combined with expression to create storage constraint
		if len(phase1.Predicates) != 0 {
			t.Errorf("Phase 1: expected 0 predicates (consumed by storage constraint), got %d", len(phase1.Predicates))
		}
	}
}
