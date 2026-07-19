package parser

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestExtractVariablesCoversEveryBindingForm pins that a subquery's bound
// variables are extracted for all four binding forms. The hand-written switch
// this replaces was missing ScalarBinding, so a scalar-bound subquery variable
// was silently invisible to variable extraction.
func TestExtractVariablesCoversEveryBindingForm(t *testing.T) {
	out := datalog.NewSymbol("?out")

	cases := []struct {
		name    string
		binding query.BindingForm
	}{
		{"scalar", query.ScalarBinding{Variable: out}},
		{"collection", query.CollectionBinding{Variable: out}},
		{"tuple", query.TupleBinding{Variables: []query.Symbol{out}}},
		{"relation", query.RelationBinding{Variables: []query.Symbol{out}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clauses := []query.Clause{
				&query.SubqueryPattern{
					Query:   &query.Query{},
					Binding: tc.binding,
				},
			}
			vars := ExtractVariables(clauses)
			if !query.ContainsSymbol(vars, out) {
				t.Errorf("ExtractVariables must include the %s-bound subquery variable; got %v", tc.name, vars)
			}
		})
	}
}
