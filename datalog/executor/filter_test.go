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
		columns  []query.Symbol
		expected bool
	}{
		{
			name: "less than integer - true",
			filter: ComparisonFilter{
				Function: "<",
				Symbol:   "?age",
				Value:    int64(30),
			},
			tuple:    Tuple{int64(25)},
			columns:  []query.Symbol{"?age"},
			expected: true,
		},
		{
			name: "less than integer - false",
			filter: ComparisonFilter{
				Function: "<",
				Symbol:   "?age",
				Value:    int64(30),
			},
			tuple:    Tuple{int64(35)},
			columns:  []query.Symbol{"?age"},
			expected: false,
		},
		{
			name: "equals string",
			filter: ComparisonFilter{
				Function: "=",
				Symbol:   "?name",
				Value:    "Alice",
			},
			tuple:    Tuple{"Alice"},
			columns:  []query.Symbol{"?name"},
			expected: true,
		},
		{
			name: "not equals string",
			filter: ComparisonFilter{
				Function: "!=",
				Symbol:   "?name",
				Value:    "Alice",
			},
			tuple:    Tuple{"Bob"},
			columns:  []query.Symbol{"?name"},
			expected: true,
		},
		{
			name: "symbol not in columns",
			filter: ComparisonFilter{
				Function: "<",
				Symbol:   "?missing",
				Value:    int64(30),
			},
			tuple:    Tuple{int64(25)},
			columns:  []query.Symbol{"?age"},
			expected: false,
		},
		{
			name: "multiple columns",
			filter: ComparisonFilter{
				Function: ">=",
				Symbol:   "?score",
				Value:    3.5,
			},
			tuple:    Tuple{"Alice", 4.0},
			columns:  []query.Symbol{"?name", "?score"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Evaluate(tt.tuple, tt.columns)
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
		columns  []query.Symbol
		expected bool
	}{
		{
			name: "less than - true",
			filter: BinaryFilter{
				Function: "<",
				Left:     "?x",
				Right:    "?y",
			},
			tuple:    Tuple{int64(10), int64(20)},
			columns:  []query.Symbol{"?x", "?y"},
			expected: true,
		},
		{
			name: "greater than - false",
			filter: BinaryFilter{
				Function: ">",
				Left:     "?x",
				Right:    "?y",
			},
			tuple:    Tuple{int64(10), int64(20)},
			columns:  []query.Symbol{"?x", "?y"},
			expected: false,
		},
		{
			name: "equals - true",
			filter: BinaryFilter{
				Function: "=",
				Left:     "?a",
				Right:    "?b",
			},
			tuple:    Tuple{"test", "test"},
			columns:  []query.Symbol{"?a", "?b"},
			expected: true,
		},
		{
			name: "mixed types",
			filter: BinaryFilter{
				Function: "<",
				Left:     "?x",
				Right:    "?y",
			},
			tuple:    Tuple{int64(10), 20.5},
			columns:  []query.Symbol{"?x", "?y"},
			expected: true,
		},
		{
			name: "missing left symbol",
			filter: BinaryFilter{
				Function: "<",
				Left:     "?missing",
				Right:    "?y",
			},
			tuple:    Tuple{int64(10)},
			columns:  []query.Symbol{"?x"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.filter.Evaluate(tt.tuple, tt.columns)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestFilterRelation(t *testing.T) {
	// Create a test relation
	rel := NewMaterializedRelation(
		[]query.Symbol{"?person", "?age", "?city"},
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
		Symbol:   "?age",
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
		Symbol:   "?missing",
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

		// Type mismatch
		{"string vs int", "test", 123, -1},
		{"bool vs string", true, "test", -1},
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
		Symbol:   "?age",
		Value:    30,
	}

	cfSyms := cf.RequiredSymbols()
	if len(cfSyms) != 1 || cfSyms[0] != "?age" {
		t.Errorf("ComparisonFilter.RequiredSymbols() = %v, want [?age]", cfSyms)
	}

	// Test BinaryFilter
	bf := BinaryFilter{
		Function: ">",
		Left:     "?x",
		Right:    "?y",
	}

	bfSyms := bf.RequiredSymbols()
	if len(bfSyms) != 2 || bfSyms[0] != "?x" || bfSyms[1] != "?y" {
		t.Errorf("BinaryFilter.RequiredSymbols() = %v, want [?x ?y]", bfSyms)
	}
}
