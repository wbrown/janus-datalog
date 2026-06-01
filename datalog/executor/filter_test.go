package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestComparisonFilter(t *testing.T) {
	tests := []struct {
		name     string
		filter   ComparisonFilter
		tuple    Tuple
		symbols  []query.Symbol
		expected bool
	}{
		{
			name: "less than integer - true",
			filter: ComparisonFilter{
				Function: "<",
				Symbol:   datalog.NewSymbol("?age"),
				Value:    int64(30),
			},
			tuple:    Tuple{int64(25)},
			symbols:  []query.Symbol{datalog.NewSymbol("?age")},
			expected: true,
		},
		{
			name: "less than integer - false",
			filter: ComparisonFilter{
				Function: "<",
				Symbol:   datalog.NewSymbol("?age"),
				Value:    int64(30),
			},
			tuple:    Tuple{int64(35)},
			symbols:  []query.Symbol{datalog.NewSymbol("?age")},
			expected: false,
		},
		{
			name: "equals string",
			filter: ComparisonFilter{
				Function: "=",
				Symbol:   datalog.NewSymbol("?name"),
				Value:    "Alice",
			},
			tuple:    Tuple{"Alice"},
			symbols:  []query.Symbol{datalog.NewSymbol("?name")},
			expected: true,
		},
		{
			name: "not equals string",
			filter: ComparisonFilter{
				Function: "!=",
				Symbol:   datalog.NewSymbol("?name"),
				Value:    "Alice",
			},
			tuple:    Tuple{"Bob"},
			symbols:  []query.Symbol{datalog.NewSymbol("?name")},
			expected: true,
		},
		{
			name: "symbol not in symbols",
			filter: ComparisonFilter{
				Function: "<",
				Symbol:   datalog.NewSymbol("?missing"),
				Value:    int64(30),
			},
			tuple:    Tuple{int64(25)},
			symbols:  []query.Symbol{datalog.NewSymbol("?age")},
			expected: false,
		},
		{
			name: "multiple symbols",
			filter: ComparisonFilter{
				Function: ">=",
				Symbol:   datalog.NewSymbol("?score"),
				Value:    3.5,
			},
			tuple:    Tuple{"Alice", 4.0},
			symbols:  []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?score")},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Evaluate(tt.tuple, tt.symbols)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestBinaryFilterEvaluate(t *testing.T) {
	tests := []struct {
		name     string
		filter   BinaryFilter
		tuple    Tuple
		symbols  []query.Symbol
		expected bool
	}{
		{
			name: "less than - true",
			filter: BinaryFilter{
				Function: "<",
				Left:     datalog.NewSymbol("?x"),
				Right:    datalog.NewSymbol("?y"),
			},
			tuple:    Tuple{int64(10), int64(20)},
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			expected: true,
		},
		{
			name: "greater than - false",
			filter: BinaryFilter{
				Function: ">",
				Left:     datalog.NewSymbol("?x"),
				Right:    datalog.NewSymbol("?y"),
			},
			tuple:    Tuple{int64(10), int64(20)},
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			expected: false,
		},
		{
			name: "equals - true",
			filter: BinaryFilter{
				Function: "=",
				Left:     datalog.NewSymbol("?a"),
				Right:    datalog.NewSymbol("?b"),
			},
			tuple:    Tuple{"test", "test"},
			symbols:  []query.Symbol{datalog.NewSymbol("?a"), datalog.NewSymbol("?b")},
			expected: true,
		},
		{
			name: "mixed types",
			filter: BinaryFilter{
				Function: "<",
				Left:     datalog.NewSymbol("?x"),
				Right:    datalog.NewSymbol("?y"),
			},
			tuple:    Tuple{int64(10), 20.5},
			symbols:  []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")},
			expected: true,
		},
		{
			name: "missing left symbol",
			filter: BinaryFilter{
				Function: "<",
				Left:     datalog.NewSymbol("?missing"),
				Right:    datalog.NewSymbol("?y"),
			},
			tuple:    Tuple{int64(10)},
			symbols:  []query.Symbol{datalog.NewSymbol("?x")},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Evaluate(tt.tuple, tt.symbols)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFilterRelation(t *testing.T) {
	// Create a test relation
	rel := NewMaterializedRelation(
		[]query.Symbol{datalog.NewSymbol("?person"), datalog.NewSymbol("?age"), datalog.NewSymbol("?city")},
		[]Tuple{
			{"Alice", int64(30), "NYC"},
			{"Bob", int64(25), "SF"},
			{"Charlie", int64(35), "NYC"},
			{"David", int64(28), "SF"},
			{"Eve", int64(32), "LA"},
		},
	)

	// Test with comparison filter
	ageFilter := ComparisonFilter{
		Function: "<",
		Symbol:   datalog.NewSymbol("?age"),
		Value:    int64(30),
	}

	filtered := rel.Filter(ageFilter)

	// Should have Bob and David
	if filtered.Size() != 2 {
		t.Errorf("expected 2 results, got %d", filtered.Size())
	}

	// Test with missing symbol
	missingFilter := ComparisonFilter{
		Function: "=",
		Symbol:   datalog.NewSymbol("?missing"),
		Value:    "test",
	}

	empty := rel.Filter(missingFilter)
	if !empty.IsEmpty() {
		t.Error("expected empty relation for missing symbol")
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		name     string
		left     interface{}
		right    interface{}
		expected int
	}{
		// Integer comparisons
		{"int64 less", int64(10), int64(20), -1},
		{"int64 equal", int64(20), int64(20), 0},
		{"int64 greater", int64(30), int64(20), 1},

		// Float comparisons
		{"float less", 10.5, 20.5, -1},
		{"float equal", 20.5, 20.5, 0},
		{"float greater", 30.5, 20.5, 1},

		// String comparisons
		{"string less", "Alice", "Bob", -1},
		{"string equal", "Bob", "Bob", 0},
		{"string greater", "Charlie", "Bob", 1},

		// Boolean comparisons
		{"bool false < true", false, true, -1},
		{"bool equal", true, true, 0},
		{"bool true > false", true, false, 1},

		// Mixed numeric types
		{"int to int64", int(10), int64(20), -1},
		{"int64 to float", int64(10), 20.5, -1},
		{"float to int", 10.5, int(10), 1},

		// Mixed types order by type rank (numeric=1 < bool=2 < string=4).
		{"string vs int", "test", 123, 1},  // string rank > numeric rank
		{"bool vs string", true, "test", -1}, // bool rank < string rank
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := datalog.CompareValues(tt.left, tt.right)
			if result != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestRequiredSymbols(t *testing.T) {
	// Test ComparisonFilter
	cf := ComparisonFilter{
		Function: "<",
		Symbol:   datalog.NewSymbol("?age"),
		Value:    30,
	}

	cfSyms := cf.RequiredSymbols()
	if len(cfSyms) != 1 || cfSyms[0] != datalog.NewSymbol("?age") {
		t.Errorf("ComparisonFilter.RequiredSymbols() = %v, want [?age]", cfSyms)
	}

	// Test BinaryFilter
	bf := BinaryFilter{
		Function: ">",
		Left:     datalog.NewSymbol("?x"),
		Right:    datalog.NewSymbol("?y"),
	}

	bfSyms := bf.RequiredSymbols()
	if len(bfSyms) != 2 || bfSyms[0] != datalog.NewSymbol("?x") || bfSyms[1] != datalog.NewSymbol("?y") {
		t.Errorf("BinaryFilter.RequiredSymbols() = %v, want [?x ?y]", bfSyms)
	}
}
