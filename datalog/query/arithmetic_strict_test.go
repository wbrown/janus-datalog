package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Arithmetic operands are numbers: int64 or float64, with Go integer and
// float widths normalized. Everything else is a loud error — including
// numeric strings, which are not numbers: strings become values of other
// types only by boundary construction, never by evaluation-time parsing.
// Previously non-numeric operands silently became 0 and numeric strings were
// parsed, so (+ ?x 1) with ?x = "abc" evaluated to 1.

func TestArithmeticRejectsNonNumericOperands(t *testing.T) {
	x := datalog.NewSymbol("?x")
	for _, val := range []interface{}{"abc", "42", true, datalog.NewKeyword(":k/w")} {
		fn := ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{
			VariableTerm{Symbol: x},
			ConstantTerm{Value: int64(1)},
		}}
		if _, err := fn.Eval(map[Symbol]interface{}{x: val}); err == nil {
			t.Errorf("(+ %v 1) must error: %T is not a number", val, val)
		}
	}
}

func TestArithmeticNumericOperandsUnchanged(t *testing.T) {
	x := datalog.NewSymbol("?x")
	sum := ArithmeticFunction{Op: datalog.SymAdd, Args: []Term{
		VariableTerm{Symbol: x},
		ConstantTerm{Value: int64(2)},
	}}

	got, err := sum.Eval(map[Symbol]interface{}{x: int64(3)})
	if err != nil || got != int64(5) {
		t.Errorf("(+ 3 2) = %v, %v; want int64 5", got, err)
	}

	got, err = sum.Eval(map[Symbol]interface{}{x: 3.5})
	if err != nil || got != 5.5 {
		t.Errorf("(+ 3.5 2) = %v, %v; want float64 5.5", got, err)
	}
}

// Vector indices are integers. A string or float index previously coerced
// silently (strings and other types to 0, floats by truncation), so
// (nth ?vec "x") returned element 0 instead of erroring.
func TestVectorIndexRequiresInteger(t *testing.T) {
	v := datalog.NewSymbol("?vec")
	bindings := map[Symbol]interface{}{v: []interface{}{"a", "b", "c"}}

	nth := NthFunction{VecTerm: VariableTerm{Symbol: v}, IndexTerm: ConstantTerm{Value: int64(1)}}
	got, err := nth.Eval(bindings)
	if err != nil || got != "b" {
		t.Fatalf("(nth ?vec 1) = %v, %v; want \"b\"", got, err)
	}

	for _, idx := range []interface{}{"1", 1.5, true} {
		bad := NthFunction{VecTerm: VariableTerm{Symbol: v}, IndexTerm: ConstantTerm{Value: idx}}
		if _, err := bad.Eval(bindings); err == nil {
			t.Errorf("(nth ?vec %v) must error: %T is not an integer index", idx, idx)
		}
	}

	subvec := SubvecFunction{
		VecTerm:   VariableTerm{Symbol: v},
		StartTerm: ConstantTerm{Value: "0"},
		EndTerm:   ConstantTerm{Value: int64(2)},
	}
	if _, err := subvec.Eval(bindings); err == nil {
		t.Error("(subvec ?vec \"0\" 2) must error: a string is not an integer index")
	}
}
