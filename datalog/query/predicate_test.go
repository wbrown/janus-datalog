package query

import (
	"testing"
	"time"
)

func TestComparison(t *testing.T) {
	tests := []struct {
		name     string
		pred     Predicate
		bindings map[Symbol]interface{}
		expected bool
	}{
		{
			name: "Variable equals constant",
			pred: &Comparison{
				Op:    OpEQ,
				Left:  VariableTerm{Symbol: "?x"},
				Right: ConstantTerm{Value: int64(5)},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5)},
			expected: true,
		},
		{
			name: "Variable not equals constant",
			pred: &Comparison{
				Op:    OpEQ,
				Left:  VariableTerm{Symbol: "?x"},
				Right: ConstantTerm{Value: int64(5)},
			},
			bindings: map[Symbol]interface{}{"?x": int64(10)},
			expected: false,
		},
		{
			name: "Variable less than constant",
			pred: &Comparison{
				Op:    OpLT,
				Left:  VariableTerm{Symbol: "?x"},
				Right: ConstantTerm{Value: int64(10)},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5)},
			expected: true,
		},
		{
			name: "Variable equals variable",
			pred: &Comparison{
				Op:    OpEQ,
				Left:  VariableTerm{Symbol: "?x"},
				Right: VariableTerm{Symbol: "?y"},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5), "?y": int64(5)},
			expected: true,
		},
		{
			name: "Variable not equals variable",
			pred: &Comparison{
				Op:    OpEQ,
				Left:  VariableTerm{Symbol: "?x"},
				Right: VariableTerm{Symbol: "?y"},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5), "?y": int64(10)},
			expected: false,
		},
		{
			name: "Constant less than variable",
			pred: &Comparison{
				Op:    OpLT,
				Left:  ConstantTerm{Value: int64(5)},
				Right: VariableTerm{Symbol: "?x"},
			},
			bindings: map[Symbol]interface{}{"?x": int64(10)},
			expected: true,
		},
		{
			name: "String comparison",
			pred: &Comparison{
				Op:    OpLT,
				Left:  VariableTerm{Symbol: "?s"},
				Right: ConstantTerm{Value: "zebra"},
			},
			bindings: map[Symbol]interface{}{"?s": "apple"},
			expected: true,
		},
		{
			name: "Time comparison",
			pred: &Comparison{
				Op:    OpGT,
				Left:  VariableTerm{Symbol: "?t"},
				Right: ConstantTerm{Value: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
			},
			bindings: map[Symbol]interface{}{"?t": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
			expected: true,
		},
		{
			name: "Mixed numeric types",
			pred: &Comparison{
				Op:    OpEQ,
				Left:  VariableTerm{Symbol: "?x"},
				Right: ConstantTerm{Value: float64(5.0)},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5)},
			expected: true,
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

func TestChainedComparison(t *testing.T) {
	tests := []struct {
		name     string
		pred     Predicate
		bindings map[Symbol]interface{}
		expected bool
	}{
		{
			name: "Range check: 0 < x < 10",
			pred: &ChainedComparison{
				Op: OpLT,
				Terms: []Term{
					ConstantTerm{Value: int64(0)},
					VariableTerm{Symbol: "?x"},
					ConstantTerm{Value: int64(10)},
				},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5)},
			expected: true,
		},
		{
			name: "Range check: 0 < x < 10 (out of range)",
			pred: &ChainedComparison{
				Op: OpLT,
				Terms: []Term{
					ConstantTerm{Value: int64(0)},
					VariableTerm{Symbol: "?x"},
					ConstantTerm{Value: int64(10)},
				},
			},
			bindings: map[Symbol]interface{}{"?x": int64(15)},
			expected: false,
		},
		{
			name: "Chained variables: x < y < z",
			pred: &ChainedComparison{
				Op: OpLT,
				Terms: []Term{
					VariableTerm{Symbol: "?x"},
					VariableTerm{Symbol: "?y"},
					VariableTerm{Symbol: "?z"},
				},
			},
			bindings: map[Symbol]interface{}{"?x": int64(1), "?y": int64(5), "?z": int64(10)},
			expected: true,
		},
		{
			name: "Chained variables: x < y < z (not satisfied)",
			pred: &ChainedComparison{
				Op: OpLT,
				Terms: []Term{
					VariableTerm{Symbol: "?x"},
					VariableTerm{Symbol: "?y"},
					VariableTerm{Symbol: "?z"},
				},
			},
			bindings: map[Symbol]interface{}{"?x": int64(1), "?y": int64(10), "?z": int64(5)},
			expected: false,
		},
		{
			name: "All equal",
			pred: &ChainedComparison{
				Op: OpEQ,
				Terms: []Term{
					VariableTerm{Symbol: "?x"},
					VariableTerm{Symbol: "?y"},
					VariableTerm{Symbol: "?z"},
				},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5), "?y": int64(5), "?z": int64(5)},
			expected: true,
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

func TestRequiredSymbols(t *testing.T) {
	tests := []struct {
		name     string
		pred     Predicate
		expected []Symbol
	}{
		{
			name: "Variable comparison",
			pred: &Comparison{
				Op:    OpEQ,
				Left:  VariableTerm{Symbol: "?x"},
				Right: ConstantTerm{Value: int64(5)},
			},
			expected: []Symbol{"?x"},
		},
		{
			name: "Two variables",
			pred: &Comparison{
				Op:    OpEQ,
				Left:  VariableTerm{Symbol: "?x"},
				Right: VariableTerm{Symbol: "?y"},
			},
			expected: []Symbol{"?x", "?y"},
		},
		{
			name: "Chained comparison",
			pred: &ChainedComparison{
				Op: OpLT,
				Terms: []Term{
					ConstantTerm{Value: int64(0)},
					VariableTerm{Symbol: "?x"},
					VariableTerm{Symbol: "?y"},
					ConstantTerm{Value: int64(100)},
				},
			},
			expected: []Symbol{"?x", "?y"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			symbols := tt.pred.RequiredSymbols()
			if len(symbols) != len(tt.expected) {
				t.Errorf("Expected %d symbols, got %d", len(tt.expected), len(symbols))
			}
			// Check each expected symbol is present
			for _, exp := range tt.expected {
				found := false
				for _, sym := range symbols {
					if sym == exp {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected symbol %s not found", exp)
				}
			}
		})
	}
}
