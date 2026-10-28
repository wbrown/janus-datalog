package qb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A query built with a shape the parser refuses is refused at Build with
// the parser's message: an or with no branch, a branch with no clause, a
// not or a not-join with no clause, an or-default with one branch, and a
// data pattern of five elements. A refusal inside a branch names the form
// and the branch by its position counted from one.
func TestBuild_RefusesTheShapesTheParserRefuses(t *testing.T) {
	e, v, w := NewVar("e"), NewVar("v"), NewVar("w")
	first, second := Kw(":t/a"), Kw(":t/b")
	shapes := []struct {
		name  string
		shape interface{}
		names string
	}{
		{"an or with no branch", Or(), "or clause must have at least one branch"},
		{"an or with a branch of no clause", Or().Branch().Branch(Pat(e, second, w)), "or branch 1 must have at least one clause"},
		{"a not with no clause", Not(), "not clause must have at least one inner clause"},
		{"a not-join with no clause", NotJoin([]*Var{e}), "not-join clause must have at least one inner clause"},
		{"an or-default with one branch", OrDefault().Branch(Pat(e, second, w)), "or-default clause must have at least two branches"},
		{"a data pattern of five elements", Pat(e, first, v, w, w), "data pattern must have 1 to 4 elements, got 5"},
		{"a not with no clause in an or's second branch", Or().Branch(Pat(e, second, w)).Branch(Not()), "or branch 2: not clause must have at least one inner clause"},
	}
	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			_, err := Query().Find(e).Where(Pat(e, first, v), s.shape).Build()
			require.ErrorContains(t, err, s.names)
		})
	}
}
