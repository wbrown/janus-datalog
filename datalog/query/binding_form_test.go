package query

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestBindingFormBoundVariables pins the symbol extraction for every binding
// form. BindingForm is sealed (unexported marker method), so these four cases
// are the complete taxonomy; every consumer relies on exactly these results.
func TestBindingFormBoundVariables(t *testing.T) {
	a := datalog.NewSymbol("?a")
	b := datalog.NewSymbol("?b")

	cases := []struct {
		name    string
		binding BindingForm
		want    []Symbol
	}{
		{"tuple", TupleBinding{Variables: []Symbol{a, b}}, []Symbol{a, b}},
		{"relation", RelationBinding{Variables: []Symbol{a, b}}, []Symbol{a, b}},
		{"collection", CollectionBinding{Variable: a}, []Symbol{a}},
		{"scalar", ScalarBinding{Variable: a}, []Symbol{a}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.binding.BoundVariables()
			if len(got) != len(tc.want) {
				t.Fatalf("BoundVariables() = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Errorf("BoundVariables()[%d] = %v, want %v", i, got[i], tc.want[i])
				}
			}
		})
	}
}
