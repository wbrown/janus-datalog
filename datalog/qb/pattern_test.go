package qb

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// A data pattern built by Pat with five arguments, the fifth whether the
// datom asserts, is the same value, compared by reflect.DeepEqual, as the
// clause parsed from its text, whether the fifth is a variable or the
// constant true or false.
func TestPatOfFiveElementsEqualsItsParsedForm(t *testing.T) {
	e, v, tx, added := NewVar("e"), NewVar("v"), NewVar("tx"), NewVar("added")
	one := Kw(":w/one")
	cases := []struct {
		name  string
		built *Pattern
		text  string
	}{
		{"a variable", Pat(e, one, v, tx, added), `[:find ?e :where [?e :w/one ?v ?tx ?added]]`},
		{"the constant true", Pat(e, one, v, tx, true), `[:find ?e :where [?e :w/one ?v ?tx true]]`},
		{"the constant false", Pat(e, one, v, tx, false), `[:find ?e :where [?e :w/one ?v ?tx false]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parser.ParseQuery(tc.text)
			require.NoError(t, err)
			if !reflect.DeepEqual(tc.built.toClause(), parsed.Where[0]) {
				t.Errorf("built pattern %#v differs from parsed %#v", tc.built.toClause(), parsed.Where[0])
			}
		})
	}
}
