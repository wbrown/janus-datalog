package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"testing"
	"time"
)

// TestConvertPlannerConstraints tests conversion from planner to executor constraints
func TestConvertPlannerConstraints(t *testing.T) {
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?b"},
			query.Constant{Value: datalog.NewKeyword(":price/volume")},
			query.Variable{Name: "?v"},
		},
	}

	tests := []struct {
		name               string
		plannerConstraints []planner.StorageConstraint
		expectedCount      int
		validate           func(t *testing.T, constraints []StorageConstraint)
	}{
		{
			name: "EqualityConstraint",
			plannerConstraints: []planner.StorageConstraint{
				{
					Type:     planner.ConstraintEquality,
					Value:    int64(42),
					Operator: query.OpEQ,
				},
			},
			expectedCount: 1,
			validate: func(t *testing.T, constraints []StorageConstraint) {
				ec, ok := constraints[0].(*equalityConstraint)
				if !ok {
					t.Fatal("Expected equalityConstraint")
				}
				if ec.position != 2 {
					t.Errorf("Expected position 2 (value), got %d", ec.position)
				}
				if ec.value != int64(42) {
					t.Errorf("Expected value 42, got %v", ec.value)
				}
			},
		},
		{
			name: "RangeConstraintGreaterThan",
			plannerConstraints: []planner.StorageConstraint{
				{
					Type:     planner.ConstraintRange,
					Value:    int64(1000000),
					Operator: query.OpGT,
				},
			},
			expectedCount: 1,
			validate: func(t *testing.T, constraints []StorageConstraint) {
				rc, ok := constraints[0].(*rangeConstraint)
				if !ok {
					t.Fatal("Expected rangeConstraint")
				}
				if rc.position != 2 {
					t.Errorf("Expected position 2 (value), got %d", rc.position)
				}
				if rc.min != int64(1000000) {
					t.Errorf("Expected min 1000000, got %v", rc.min)
				}
				if rc.includeMin != false {
					t.Error("Expected includeMin to be false for >")
				}
				if rc.max != nil {
					t.Errorf("Expected no max for >, got %v", rc.max)
				}
			},
		},
		{
			name: "RangeConstraintLessThanOrEqual",
			plannerConstraints: []planner.StorageConstraint{
				{
					Type:     planner.ConstraintRange,
					Value:    int64(500),
					Operator: "<=",
				},
			},
			expectedCount: 1,
			validate: func(t *testing.T, constraints []StorageConstraint) {
				rc, ok := constraints[0].(*rangeConstraint)
				if !ok {
					t.Fatal("Expected rangeConstraint")
				}
				if rc.max != int64(500) {
					t.Errorf("Expected max 500, got %v", rc.max)
				}
				if rc.includeMax != true {
					t.Error("Expected includeMax to be true for <=")
				}
				if rc.min != nil {
					t.Errorf("Expected no min for <=, got %v", rc.min)
				}
			},
		},
		{
			name: "TimeExtractionConstraint",
			plannerConstraints: []planner.StorageConstraint{
				{
					Type:      planner.ConstraintTimeExtraction,
					TimeField: "day",
					Value:     int64(20),
					Operator:  "=",
				},
			},
			expectedCount: 1,
			validate: func(t *testing.T, constraints []StorageConstraint) {
				tc, ok := constraints[0].(*timeExtractionConstraint)
				if !ok {
					t.Fatal("Expected timeExtractionConstraint")
				}
				if tc.position != 2 {
					t.Errorf("Expected position 2 (value), got %d", tc.position)
				}
				if tc.extractFn != "day" {
					t.Errorf("Expected extractFn 'day', got %s", tc.extractFn)
				}
				if tc.expected != int64(20) {
					t.Errorf("Expected value 20, got %v", tc.expected)
				}
			},
		},
		{
			name: "MultipleConstraints",
			plannerConstraints: []planner.StorageConstraint{
				{
					Type:     planner.ConstraintRange,
					Value:    int64(100),
					Operator: query.OpGT,
				},
				{
					Type:     planner.ConstraintRange,
					Value:    int64(200),
					Operator: query.OpLT,
				},
			},
			expectedCount: 2,
			validate: func(t *testing.T, constraints []StorageConstraint) {
				// First constraint: > 100
				rc1, ok := constraints[0].(*rangeConstraint)
				if !ok {
					t.Fatal("Expected first constraint to be rangeConstraint")
				}
				if rc1.min != int64(100) || rc1.includeMin != false {
					t.Errorf("First constraint wrong: min=%v, includeMin=%v", rc1.min, rc1.includeMin)
				}

				// Second constraint: < 200
				rc2, ok := constraints[1].(*rangeConstraint)
				if !ok {
					t.Fatal("Expected second constraint to be rangeConstraint")
				}
				if rc2.max != int64(200) || rc2.includeMax != false {
					t.Errorf("Second constraint wrong: max=%v, includeMax=%v", rc2.max, rc2.includeMax)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			constraints := convertPlannerConstraints(pattern, tt.plannerConstraints)

			if len(constraints) != tt.expectedCount {
				t.Fatalf("Expected %d constraints, got %d", tt.expectedCount, len(constraints))
			}

			if tt.validate != nil {
				tt.validate(t, constraints)
			}
		})
	}
}

// TestConstraintEvaluation tests that constraints correctly filter datoms
func TestConstraintEvaluation(t *testing.T) {
	tests := []struct {
		name       string
		constraint StorageConstraint
		datom      datalog.Datom
		expected   bool
	}{
		{
			name: "EqualityMatch",
			constraint: &equalityConstraint{
				position: 2,
				value:    int64(42),
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/value"),
				V: int64(42),
			},
			expected: true,
		},
		{
			name: "EqualityNoMatch",
			constraint: &equalityConstraint{
				position: 2,
				value:    int64(42),
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/value"),
				V: int64(100),
			},
			expected: false,
		},
		{
			name: "RangeGreaterThan",
			constraint: &rangeConstraint{
				position:   2,
				min:        int64(100),
				includeMin: false,
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/value"),
				V: int64(150),
			},
			expected: true,
		},
		{
			name: "RangeGreaterThanBoundary",
			constraint: &rangeConstraint{
				position:   2,
				min:        int64(100),
				includeMin: false,
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/value"),
				V: int64(100),
			},
			expected: false, // Not included because it's > not >=
		},
		{
			name: "RangeGreaterThanOrEqual",
			constraint: &rangeConstraint{
				position:   2,
				min:        int64(100),
				includeMin: true,
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/value"),
				V: int64(100),
			},
			expected: true, // Included because it's >=
		},
		{
			name: "RangeBetween",
			constraint: &rangeConstraint{
				position:   2,
				min:        int64(100),
				max:        int64(200),
				includeMin: true,
				includeMax: false,
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/value"),
				V: int64(150),
			},
			expected: true,
		},
		{
			name: "TimeExtractionDay",
			constraint: &timeExtractionConstraint{
				position:  2,
				extractFn: "day",
				expected:  int64(20),
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/time"),
				V: time.Date(2025, 6, 20, 10, 30, 0, 0, time.UTC),
			},
			expected: true,
		},
		{
			name: "TimeExtractionDayNoMatch",
			constraint: &timeExtractionConstraint{
				position:  2,
				extractFn: "day",
				expected:  int64(20),
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/time"),
				V: time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC),
			},
			expected: false,
		},
		{
			name: "TimeExtractionMonth",
			constraint: &timeExtractionConstraint{
				position:  2,
				extractFn: "month",
				expected:  int64(6),
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/time"),
				V: time.Date(2025, 6, 20, 10, 30, 0, 0, time.UTC),
			},
			expected: true,
		},
		{
			name: "TimeExtractionHour",
			constraint: &timeExtractionConstraint{
				position:  2,
				extractFn: "hour",
				expected:  int64(10),
			},
			datom: datalog.Datom{
				E: datalog.NewIdentity("test"),
				A: datalog.NewKeyword(":test/time"),
				V: time.Date(2025, 6, 20, 10, 30, 0, 0, time.UTC),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.constraint.Evaluate(&tt.datom)
			if result != tt.expected {
				t.Errorf("Expected Evaluate() to return %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestExecutorWithCustomPlannerOptions tests NewExecutorWithOptions
func TestExecutorWithCustomPlannerOptions(t *testing.T) {
	// Create a mock matcher
	matcher := &mockMatcher{}

	t.Run("DefaultExecutor", func(t *testing.T) {
		exec := NewExecutor(matcher)
		if exec == nil {
			t.Fatal("Expected executor, got nil")
		}
		// Default has predicate pushdown enabled
		if exec.planner == nil {
			t.Fatal("Expected planner to be initialized")
		}
	})

	t.Run("CustomOptions", func(t *testing.T) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnablePredicatePushdown: false,
			EnableFineGrainedPhases: true,
			MaxPhases:               5,
		})
		if exec == nil {
			t.Fatal("Expected executor, got nil")
		}
		if exec.planner == nil {
			t.Fatal("Expected planner to be initialized")
		}
	})
}

// mockMatcher implements PatternMatcher for testing
type mockMatcher struct{}

func (m *mockMatcher) Match(pattern *query.DataPattern, bindings Relations) (Relation, error) {
	// Return empty relation for testing
	return NewMaterializedRelation([]query.Symbol{}, []Tuple{}), nil
}
