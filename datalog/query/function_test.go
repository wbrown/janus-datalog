package query

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
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
				Op: datalog.SymAdd,
				Args: []Term{
					VariableTerm{Symbol: datalog.NewSymbol("?x")},
					ConstantTerm{Value: int64(10)},
				},
			},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?x"): int64(5)},
			expected: int64(15),
		},
		{
			name: "Subtraction float",
			fn: ArithmeticFunction{
				Op: datalog.SymSubtract,
				Args: []Term{
					VariableTerm{Symbol: datalog.NewSymbol("?x")},
					VariableTerm{Symbol: datalog.NewSymbol("?y")},
				},
			},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?x"): 10.5, datalog.NewSymbol("?y"): 3.5},
			expected: 7.0,
		},
		{
			name: "Multiplication mixed",
			fn: ArithmeticFunction{
				Op: datalog.SymMultiply,
				Args: []Term{
					VariableTerm{Symbol: datalog.NewSymbol("?x")},
					ConstantTerm{Value: 2.5},
				},
			},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?x"): int64(4)},
			expected: 10.0,
		},
		{
			name: "Division",
			fn: ArithmeticFunction{
				Op: datalog.SymDivide,
				Args: []Term{
					ConstantTerm{Value: int64(10)},
					VariableTerm{Symbol: datalog.NewSymbol("?x")},
				},
			},
			bindings: map[Symbol]interface{}{datalog.NewSymbol("?x"): int64(2)},
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

func TestArithmeticFunctionsClojureArities(t *testing.T) {
	testCases := []struct {
		name string
		fn   ArithmeticFunction
		want interface{}
	}{
		{
			name: "variadic addition",
			fn: ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{
				ConstantTerm{Value: int64(1)},
				ConstantTerm{Value: int64(2)},
				ConstantTerm{Value: int64(3)},
			}},
			want: int64(6),
		},
		{
			name: "unary subtraction",
			fn: ArithmeticFunction{Op: datalog.SymSubtract, Args: []Term{
				ConstantTerm{Value: int64(5)},
			}},
			want: int64(-5),
		},
		{
			name: "variadic subtraction",
			fn: ArithmeticFunction{Op: datalog.SymSubtract, Args: []Term{
				ConstantTerm{Value: int64(10)},
				ConstantTerm{Value: int64(3)},
				ConstantTerm{Value: int64(2)},
			}},
			want: int64(5),
		},
		{
			name: "unary division",
			fn: ArithmeticFunction{Op: datalog.SymDivide, Args: []Term{
				ConstantTerm{Value: int64(4)},
			}},
			want: float64(0.25),
		},
		{
			name: "variadic division",
			fn: ArithmeticFunction{Op: datalog.SymDivide, Args: []Term{
				ConstantTerm{Value: int64(24)},
				ConstantTerm{Value: int64(3)},
				ConstantTerm{Value: int64(2)},
			}},
			want: float64(4),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := testCase.fn.Eval(nil)
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.want {
				t.Fatalf("got %v (%T), want %v (%T)", got, got, testCase.want, testCase.want)
			}
		})
	}
}

func TestArithmeticFunctionsRejectInvalidZeroArity(t *testing.T) {
	for _, operator := range []Symbol{datalog.SymAdd, datalog.SymSubtract, datalog.SymMultiply, datalog.SymDivide} {
		_, err := (ArithmeticFunction{Op: operator}).Eval(nil)
		if err == nil {
			t.Fatalf("%s must reject zero arguments", operator)
		}
	}
}

func TestStringConcatFunction(t *testing.T) {
	fn := StringConcatFunction{
		Terms: []Term{
			ConstantTerm{Value: "Hello "},
			VariableTerm{Symbol: datalog.NewSymbol("?name")},
			ConstantTerm{Value: "!"},
		},
	}

	bindings := map[Symbol]interface{}{
		datalog.NewSymbol("?name"): "World",
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
		field    Symbol
		expected int64
	}{
		{datalog.SymYear, 2024},
		{datalog.SymMonth, 6},
		{datalog.SymDay, 15},
		{datalog.SymHour, 14},
		{datalog.SymMinute, 30},
		{datalog.SymSecond, 45},
	}

	for _, tt := range tests {
		t.Run(tt.field.String(), func(t *testing.T) {
			fn := TimeExtractionFunction{
				Field:    tt.field,
				TimeTerm: VariableTerm{Symbol: datalog.NewSymbol("?t")},
			}

			bindings := map[Symbol]interface{}{
				datalog.NewSymbol("?t"): testTime,
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

func TestGroundFunctionString(t *testing.T) {
	t.Run("scalar", func(t *testing.T) {
		g := GroundFunction{Value: 42}
		if g.String() != "(ground 42)" {
			t.Errorf("got %q", g.String())
		}
	})
	t.Run("vector", func(t *testing.T) {
		g := GroundFunction{Value: []interface{}{1, 2, 3}}
		if g.String() != "(ground [1 2 3])" {
			t.Errorf("got %q", g.String())
		}
	})
	t.Run("empty_vector", func(t *testing.T) {
		g := GroundFunction{Value: []interface{}{}}
		if g.String() != "(ground [])" {
			t.Errorf("got %q", g.String())
		}
	})
}
