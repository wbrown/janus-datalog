package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestGroundPredicate(t *testing.T) {
	tests := []struct {
		name     string
		pred     Predicate
		bindings map[Symbol]interface{}
		expected bool
	}{
		{
			name: "All variables bound",
			pred: &GroundPredicate{
				Variables: []Symbol{"?x", "?y"},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
				"?y": "hello",
			},
			expected: true,
		},
		{
			name: "Some variables missing",
			pred: &GroundPredicate{
				Variables: []Symbol{"?x", "?y"},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
			},
			expected: false,
		},
		{
			name: "Empty bindings",
			pred: &GroundPredicate{
				Variables: []Symbol{"?x"},
			},
			bindings: map[Symbol]interface{}{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.pred.Eval(tt.bindings)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMissingPredicate(t *testing.T) {
	tests := []struct {
		name     string
		pred     Predicate
		bindings map[Symbol]interface{}
		expected bool
	}{
		{
			name: "Variable is missing",
			pred: &MissingPredicate{
				Variables: []Symbol{"?z"},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
				"?y": "hello",
			},
			expected: true,
		},
		{
			name: "Variable is present",
			pred: &MissingPredicate{
				Variables: []Symbol{"?x"},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
			},
			expected: false,
		},
		{
			name: "Multiple variables all missing",
			pred: &MissingPredicate{
				Variables: []Symbol{"?a", "?b"},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
			},
			expected: true,
		},
		{
			name: "One variable present",
			pred: &MissingPredicate{
				Variables: []Symbol{"?a", "?x"},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.pred.Eval(tt.bindings)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNotEqualPredicate(t *testing.T) {
	tests := []struct {
		name     string
		pred     Predicate
		bindings map[Symbol]interface{}
		expected bool
	}{
		{
			name: "Variables are not equal",
			pred: &NotEqualPredicate{
				Comparison: Comparison{
					Op:    OpEQ,
					Left:  VariableTerm{Symbol: "?x"},
					Right: VariableTerm{Symbol: "?y"},
				},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
				"?y": int64(10),
			},
			expected: true,
		},
		{
			name: "Variables are equal",
			pred: &NotEqualPredicate{
				Comparison: Comparison{
					Op:    OpEQ,
					Left:  VariableTerm{Symbol: "?x"},
					Right: VariableTerm{Symbol: "?y"},
				},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
				"?y": int64(5),
			},
			expected: false,
		},
		{
			name: "Variable not equal to constant",
			pred: &NotEqualPredicate{
				Comparison: Comparison{
					Op:    OpEQ,
					Left:  VariableTerm{Symbol: "?x"},
					Right: ConstantTerm{Value: int64(10)},
				},
			},
			bindings: map[Symbol]interface{}{
				"?x": int64(5),
			},
			expected: true,
		},
		{
			name: "Keyword not equal to different keyword",
			pred: &NotEqualPredicate{
				Comparison: Comparison{
					Op:    OpEQ,
					Left:  VariableTerm{Symbol: "?attr"},
					Right: ConstantTerm{Value: datalog.NewKeyword(":symbol/ticker")},
				},
			},
			bindings: map[Symbol]interface{}{
				"?attr": datalog.NewKeyword(":symbol/name"),
			},
			expected: true,
		},
		{
			name: "Keyword not equal to same keyword",
			pred: &NotEqualPredicate{
				Comparison: Comparison{
					Op:    OpEQ,
					Left:  VariableTerm{Symbol: "?attr"},
					Right: ConstantTerm{Value: datalog.NewKeyword(":symbol/ticker")},
				},
			},
			bindings: map[Symbol]interface{}{
				"?attr": datalog.NewKeyword(":symbol/ticker"),
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.pred.Eval(tt.bindings)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
			t.Logf("Predicate %s evaluated to %v", tt.pred.String(), result)
		})
	}
}
