package qb

import (
	"reflect"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
)

// The expression builders produce what the parser produces: an expression
// clause built from Add, Str or Ground is the same value, compared by
// reflect.DeepEqual, as the clause parsed from its text.
func TestExpressionBuildersEqualTheirParsedForm(t *testing.T) {
	n, m := NewVar("n"), NewVar("m")
	a, b, c := NewVar("a"), NewVar("b"), NewVar("c")
	x := NewVar("x")
	cases := []struct {
		name  string
		built *Expression
		text  string
	}{
		{"add", Add(n, int64(1)).As(m), `[:find ?m :where [(+ ?n 1) ?m]]`},
		{"str", Str(a, " ", b).As(c), `[:find ?c :where [(str ?a " " ?b) ?c]]`},
		{"ground", Ground(int64(42)).As(x), `[:find ?x :where [(ground 42) ?x]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(tc.text)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tc.built.toClause(), parsed.Where[0]) {
				t.Errorf("built expression %#v differs from parsed %#v", tc.built.toClause(), parsed.Where[0])
			}
		})
	}
}
