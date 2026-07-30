package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// str/starts-with? is a concrete predicate type constructed at parse — not a
// name dispatched through a string switch at evaluation. A non-string value
// or prefix is the typed non-match; an unbound variable is an error.

func TestStrStartsWithPredicate(t *testing.T) {
	name := datalog.NewSymbol("?name")
	pred := StrStartsWithPredicate{
		Value:  VariableTerm{Symbol: name},
		Prefix: ConstantTerm{Value: "Dr."},
	}

	ok, err := pred.Eval(map[Symbol]interface{}{name: "Dr. Watson"})
	if err != nil || !ok {
		t.Errorf("(str/starts-with? \"Dr. Watson\" \"Dr.\") = %v, %v; want true", ok, err)
	}

	ok, err = pred.Eval(map[Symbol]interface{}{name: "Watson"})
	if err != nil || ok {
		t.Errorf("(str/starts-with? \"Watson\" \"Dr.\") = %v, %v; want false", ok, err)
	}

	ok, err = pred.Eval(map[Symbol]interface{}{name: int64(7)})
	if err != nil || ok {
		t.Errorf("non-string value is the typed non-match: got %v, %v", ok, err)
	}

	if _, err := pred.Eval(map[Symbol]interface{}{}); err == nil {
		t.Error("unbound variable must error")
	}
}

// FunctionPredicate is the invocation form for user-defined predicate
// functions; an unregistered name errors loudly at evaluation instead of
// silently filtering.
func TestFunctionPredicateUnknownNameErrors(t *testing.T) {
	pred := FunctionPredicate{Fn: "my/custom?"}
	if _, err := pred.Eval(nil); err == nil {
		t.Error("an unwired predicate function name must error at evaluation")
	}
}
