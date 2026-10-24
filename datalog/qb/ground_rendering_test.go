package qb

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A ground over a Go string slice renders as a vector of quoted strings, the
// form the parser reads, so the built query's text parses back to a ground
// over the same strings (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §6).
func TestBuild_AStringVectorGroundRendersAsQuotedStrings(t *testing.T) {
	e, x := NewVar("e"), NewVar("x")
	q, err := Query().Find(x).Where(Pat(e, Kw(":t/a"), Blank()), Ground([]string{"1", "2"}).As(x)).Build()
	require.NoError(t, err)

	rendered := q.String()
	require.Contains(t, rendered, `(ground ["1" "2"])`)

	parsed, err := parser.ParseQuery(rendered)
	require.NoError(t, err, "the rendering parses:\n%s", rendered)
	ground, ok := parsed.Where[1].(*query.Expression)
	require.True(t, ok, "the second clause parses as an expression, got %T", parsed.Where[1])
	fn, ok := ground.Function.(*query.GroundFunction)
	require.True(t, ok, "the expression parses as a ground, got %T", ground.Function)
	require.True(t, reflect.DeepEqual([]interface{}{"1", "2"}, fn.Value), "the ground holds the two strings, got %#v", fn.Value)
}
