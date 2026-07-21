package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// PatternElement is a closed taxonomy: Variable, Blank, Constant,
// VectorConstant. The memory-matcher element handling previously absorbed
// VectorConstant in silent defaults — matchesElement returned false (a
// vector literal matched nothing) and extractPatternValue returned nil (a
// vector literal was treated as unbound).

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

func TestExtractPatternValueVectorConstant(t *testing.T) {
	got := extractPatternValue(query.VectorConstant{Values: []interface{}{"a"}})
	require.Equal(t, []interface{}{"a"}, got)
}
