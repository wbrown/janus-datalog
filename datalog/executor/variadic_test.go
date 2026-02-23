package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestVariadicFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   VariadicFilter
		tuple    Tuple
		symbols  []query.Symbol
		expected bool
	}{
		{
			name: "chained less than - all variables true",
			filter: VariadicFilter{
				Function: "<",
				Args: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?b")},
					query.Variable{Name: datalog.NewSymbol("?c")},
				},
			},
			tuple:    Tuple{1, 5, 10},
			symbols:  []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")},
			expected: true,
		},
		{
			name: "chained less than - all variables false",
			filter: VariadicFilter{
				Function: "<",
				Args: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?b")},
					query.Variable{Name: datalog.NewSymbol("?c")},
				},
			},
			tuple:    Tuple{10, 5, 1},
			symbols:  []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b"), datalog.NewSymbol("?c")},
			expected: false,
		},
		{
			name: "range check with constants",
			filter: VariadicFilter{
				Function: "<=",
				Args: []query.PatternElement{
					query.Constant{Value: int64(0)},
					query.Variable{Name: datalog.NewSymbol("?x")},
					query.Constant{Value: int64(100)},
				},
			},
			tuple:    Tuple{50},
			symbols:  []query.Symbol{datalog.NewSymbol("?x")},
			expected: true,
		},
		{
			name: "range check out of bounds",
			filter: VariadicFilter{
				Function: "<=",
				Args: []query.PatternElement{
					query.Constant{Value: int64(0)},
					query.Variable{Name: datalog.NewSymbol("?x")},
					query.Constant{Value: int64(100)},
				},
			},
			tuple:    Tuple{150},
			symbols:  []query.Symbol{datalog.NewSymbol("?x")},
			expected: false,
		},
		{
			name: "multiple equality true",
			filter: VariadicFilter{
				Function: "=",
				Args: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?x")},
					query.Variable{Name: datalog.NewSymbol("?y")},
					query.Variable{Name: datalog.NewSymbol("?z")},
				},
			},
			tuple:    Tuple{42, 42, 42},
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")},
			expected: true,
		},
		{
			name: "multiple equality false",
			filter: VariadicFilter{
				Function: "=",
				Args: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?x")},
					query.Variable{Name: datalog.NewSymbol("?y")},
					query.Variable{Name: datalog.NewSymbol("?z")},
				},
			},
			tuple:    Tuple{42, 42, 43},
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")},
			expected: false,
		},
		{
			name: "mixed variables and constants",
			filter: VariadicFilter{
				Function: "<",
				Args: []query.PatternElement{
					query.Constant{Value: int64(0)},
					query.Variable{Name: datalog.NewSymbol("?x")},
					query.Variable{Name: datalog.NewSymbol("?y")},
					query.Constant{Value: int64(100)},
				},
			},
			tuple:    Tuple{10, 20},
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.Evaluate(tt.tuple, tt.symbols)
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
			query.Variable{Name: datalog.NewSymbol("?x")},
			query.Variable{Name: datalog.NewSymbol("?y")},
			query.Constant{Value: int64(100)},
		},
	}

	symbols := filter.RequiredSymbols()
	expected := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	if len(symbols) != len(expected) {
		t.Fatalf("RequiredSymbols() returned %d symbols, want %d", len(symbols), len(expected))
	}

	for i, sym := range symbols {
		if sym != expected[i] {
			t.Errorf("RequiredSymbols()[%d] = %v, want %v", i, sym, expected[i])
		}
	}
}
