package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// = and != are value equality (datalog.ValuesEqual): type-strict, matching
// join semantics and Clojure's =. The ordering operators remain the total
// order (CompareValues), where int and float compare by magnitude — so
// (>= 3 3.0) is true while (= 3 3.0) is false, exactly as in Clojure.

func TestComparisonEqualityIsStrict(t *testing.T) {
	x := datalog.NewSymbol("?x")
	bindings := map[Symbol]interface{}{x: int64(3)}

	eval := func(op Symbol, right interface{}) bool {
		c := Comparison{Op: op, Left: VariableTerm{Symbol: x}, Right: ConstantTerm{Value: right}}
		ok, err := c.Eval(bindings)
		if err != nil {
			t.Fatalf("Eval failed: %v", err)
		}
		return ok
	}

	if eval(datalog.SymEQ, float64(3)) {
		t.Error("(= 3 3.0) must be false: equality is type-strict like join keys")
	}
	if !eval(datalog.SymNE, float64(3)) {
		t.Error("(!= 3 3.0) must be true")
	}
	if !eval(datalog.SymEQ, int64(3)) {
		t.Error("(= 3 3) must be true")
	}
	if !eval(datalog.SymGTE, float64(3)) {
		t.Error("(>= 3 3.0) must be true: ordering compares by magnitude")
	}
	if !eval(datalog.SymLT, float64(3.5)) {
		t.Error("(< 3 3.5) must be true")
	}
	if eval(datalog.SymEQ, "3") {
		t.Error("(= 3 \"3\") must be false")
	}
}

func TestChainedComparisonEqualityIsStrict(t *testing.T) {
	x := datalog.NewSymbol("?x")
	bindings := map[Symbol]interface{}{x: int64(3)}

	strictEq := ChainedComparison{Op: datalog.SymEQ, Terms: []Term{
		VariableTerm{Symbol: x},
		ConstantTerm{Value: float64(3)},
	}}
	ok, err := strictEq.Eval(bindings)
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	if ok {
		t.Error("chained (= ?x 3.0) with int64 3 must be false")
	}

	ordering := ChainedComparison{Op: datalog.SymLT, Terms: []Term{
		ConstantTerm{Value: float64(2.5)},
		VariableTerm{Symbol: x},
		ConstantTerm{Value: int64(4)},
	}}
	ok, err = ordering.Eval(bindings)
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}
	if !ok {
		t.Error("chained (< 2.5 ?x 4) with int64 3 must be true: ordering is by magnitude")
	}
}
