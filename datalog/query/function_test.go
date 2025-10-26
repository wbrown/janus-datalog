package query

import (
	"testing"
	"time"
)

func TestArithmeticFunction(t *testing.T) {
	tests := []struct {
		name     string
		fn       Function
		bindings map[Symbol]interface{}
		expected interface{}
	}{
		{
			name: "Addition int",
			fn: ArithmeticFunction{
				Op:    OpAdd,
				Left:  VariableTerm{Symbol: "?x"},
				Right: ConstantTerm{Value: int64(10)},
			},
			bindings: map[Symbol]interface{}{"?x": int64(5)},
			expected: int64(15),
		},
		{
			name: "Subtraction float",
			fn: ArithmeticFunction{
				Op:    OpSubtract,
				Left:  VariableTerm{Symbol: "?x"},
				Right: VariableTerm{Symbol: "?y"},
			},
			bindings: map[Symbol]interface{}{"?x": 10.5, "?y": 3.5},
			expected: 7.0,
		},
		{
			name: "Multiplication mixed",
			fn: ArithmeticFunction{
				Op:    OpMultiply,
				Left:  VariableTerm{Symbol: "?x"},
				Right: ConstantTerm{Value: 2.5},
			},
			bindings: map[Symbol]interface{}{"?x": int64(4)},
			expected: 10.0,
		},
		{
			name: "Division",
			fn: ArithmeticFunction{
				Op:    OpDivide,
				Left:  ConstantTerm{Value: int64(10)},
				Right: VariableTerm{Symbol: "?x"},
			},
			bindings: map[Symbol]interface{}{"?x": int64(2)},
			expected: 5.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.fn.Eval(tt.bindings)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v (%T), got %v (%T)",
					tt.expected, tt.expected, result, result)
			}
		})
	}
}

func TestStringConcatFunction(t *testing.T) {
	fn := StringConcatFunction{
		Terms: []Term{
			ConstantTerm{Value: "Hello "},
			VariableTerm{Symbol: "?name"},
			ConstantTerm{Value: "!"},
		},
	}

	bindings := map[Symbol]interface{}{
		"?name": "World",
	}

	result, err := fn.Eval(bindings)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expected := "Hello World!"
	if result != expected {
		t.Errorf("Expected %s, got %v", expected, result)
	}
}

func TestTimeExtractionFunction(t *testing.T) {
	testTime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		field    string
		expected int64
	}{
		{"year", 2024},
		{"month", 6},
		{"day", 15},
		{"hour", 14},
		{"minute", 30},
		{"second", 45},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			fn := TimeExtractionFunction{
				Field:    tt.field,
				TimeTerm: VariableTerm{Symbol: "?t"},
			}

			bindings := map[Symbol]interface{}{
				"?t": testTime,
			}

			result, err := fn.Eval(bindings)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if result != tt.expected {
				t.Errorf("Expected %d, got %v", tt.expected, result)
			}
		})
	}
}

func TestAggregates(t *testing.T) {
	values := []interface{}{int64(10), int64(20), int64(30), int64(40)}

	tests := []struct {
		name     string
		agg      AggregateFunction
		expected interface{}
	}{
		{
			name:     "Count",
			agg:      CountAggregate{Var: "?x"},
			expected: int64(4),
		},
		{
			name:     "Sum",
			agg:      SumAggregate{Var: "?x"},
			expected: int64(100),
		},
		{
			name:     "Avg",
			agg:      AvgAggregate{Var: "?x"},
			expected: 25.0,
		},
		{
			name:     "Min",
			agg:      MinAggregate{Var: "?x"},
			expected: int64(10),
		},
		{
			name:     "Max",
			agg:      MaxAggregate{Var: "?x"},
			expected: int64(40),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.agg.Aggregate(values)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v (%T), got %v (%T)",
					tt.expected, tt.expected, result, result)
			}
		})
	}
}
