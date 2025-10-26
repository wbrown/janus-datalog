package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestVariadicFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   VariadicFilter
		tuple    Tuple
		columns  []query.Symbol
		expected bool
	}{
		{
			name: "chained less than - all variables true",
			filter: VariadicFilter{
				Function: "<",
				Args: []query.PatternElement{
					query.Variable{Name: "?a"},
					query.Variable{Name: "?b"},
					query.Variable{Name: "?c"},
				},
			},
			tuple:    Tuple{1, 5, 10},
			columns:  []query.Symbol{"?a", "?b", "?c"},
			expected: true,
		},
		{
			name: "chained less than - all variables false",
			filter: VariadicFilter{
				Function: "<",
				Args: []query.PatternElement{
					query.Variable{Name: "?a"},
					query.Variable{Name: "?b"},
					query.Variable{Name: "?c"},
				},
			},
			tuple:    Tuple{10, 5, 1},
			columns:  []query.Symbol{"?a", "?b", "?c"},
			expected: false,
		},
		{
			name: "range check with constants",
			filter: VariadicFilter{
				Function: "<=",
				Args: []query.PatternElement{
					query.Constant{Value: int64(0)},
					query.Variable{Name: "?x"},
					query.Constant{Value: int64(100)},
				},
			},
			tuple:    Tuple{50},
			columns:  []query.Symbol{"?x"},
			expected: true,
		},
		{
			name: "range check out of bounds",
			filter: VariadicFilter{
				Function: "<=",
				Args: []query.PatternElement{
					query.Constant{Value: int64(0)},
					query.Variable{Name: "?x"},
					query.Constant{Value: int64(100)},
				},
			},
			tuple:    Tuple{150},
			columns:  []query.Symbol{"?x"},
			expected: false,
		},
		{
			name: "multiple equality true",
			filter: VariadicFilter{
				Function: "=",
				Args: []query.PatternElement{
					query.Variable{Name: "?x"},
					query.Variable{Name: "?y"},
					query.Variable{Name: "?z"},
				},
			},
			tuple:    Tuple{42, 42, 42},
			columns:  []query.Symbol{"?x", "?y", "?z"},
			expected: true,
		},
		{
			name: "multiple equality false",
			filter: VariadicFilter{
				Function: "=",
				Args: []query.PatternElement{
					query.Variable{Name: "?x"},
					query.Variable{Name: "?y"},
					query.Variable{Name: "?z"},
				},
			},
			tuple:    Tuple{42, 42, 43},
			columns:  []query.Symbol{"?x", "?y", "?z"},
			expected: false,
		},
		{
			name: "mixed variables and constants",
			filter: VariadicFilter{
				Function: "<",
				Args: []query.PatternElement{
					query.Constant{Value: int64(0)},
					query.Variable{Name: "?x"},
					query.Variable{Name: "?y"},
					query.Constant{Value: int64(100)},
				},
			},
			tuple:    Tuple{10, 20},
			columns:  []query.Symbol{"?x", "?y"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.Evaluate(tt.tuple, tt.columns)
			if got != tt.expected {
				t.Errorf("Evaluate() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestVariadicFilterRequiredSymbols(t *testing.T) {
	filter := VariadicFilter{
		Function: "<",
		Args: []query.PatternElement{
			query.Constant{Value: int64(0)},
			query.Variable{Name: "?x"},
			query.Variable{Name: "?y"},
			query.Constant{Value: int64(100)},
		},
	}

	symbols := filter.RequiredSymbols()
	expected := []query.Symbol{"?x", "?y"}

	if len(symbols) != len(expected) {
		t.Fatalf("RequiredSymbols() returned %d symbols, want %d", len(symbols), len(expected))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("RequiredSymbols()[%d] = %v, want %v", i, sym, expected[i])
		}
	}
}
