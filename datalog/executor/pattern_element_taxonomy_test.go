package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// PatternElement is a closed taxonomy: Variable, Blank, Constant,
// VectorConstant. matchesElement matches a vector literal against a vector
// value by value equality, and a value that is no vector matches no vector
// literal.

func TestMatchesElementVectorConstant(t *testing.T) {
	vec := []interface{}{"a", "b"}
	if !matchesElement(vec, query.VectorConstant{Values: []interface{}{"a", "b"}}) {
		t.Error("a vector value must match an equal vector literal")
	}
	if matchesElement(vec, query.VectorConstant{Values: []interface{}{"a"}}) {
		t.Error("unequal vectors must not match")
	}
	if matchesElement("scalar", query.VectorConstant{Values: []interface{}{"a"}}) {
		t.Error("a non-vector value must not match a vector literal")
	}
}
