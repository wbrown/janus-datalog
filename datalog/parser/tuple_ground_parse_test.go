package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestParseTupleGroundBinding verifies that (ground [...]) with [[...]] tuple
// binding syntax parses correctly. This is the format produced by the query
// builder's TupleGround().As() method, so the parser must round-trip it.
//
// Bug: the parser handled [(ground [0 0 0]) [?a ?b ?c]] (single-bracket
// tuple binding) but not [(ground [0 0 0]) [[?a ?b ?c]]] (double-bracket
// Datomic-style tuple binding). The double-bracket form is what the query
// builder produces and what Datomic uses.
func TestParseTupleGroundBinding(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantVars  []string // expected tuple binding variables
		wantError bool
	}{
		{
			name: "single-bracket tuple binding (existing support)",
			input: `[:find ?a ?b
			         :where [?x :type :foo]
			                [(ground [0 0]) [?a ?b]]]`,
			wantVars: []string{"?a", "?b"},
		},
		{
			name: "double-bracket tuple binding (Datomic style)",
			input: `[:find ?a ?b ?c
			         :where [?x :type :foo]
			                [(ground [0 0 0]) [[?a ?b ?c]]]]`,
			wantVars: []string{"?a", "?b", "?c"},
		},
		{
			name: "tuple ground in OR fallback branch",
			input: `[:find ?e ?count ?total
			         :where [?e :entity/type :entity.type/scenario]
			                (or [(q [:find (count ?t) (sum ?v)
			                         :in $ ?s
			                         :where [?t :task/root ?s]
			                                [?t :task/value ?v]]
			                        $ ?e) [[?count ?total]]]
			                    [(ground [0 0]) [[?count ?total]]])]`,
			wantVars: []string{"?count", "?total"},
		},
		{
			name: "five-element tuple ground (production pattern)",
			input: `[:find ?e ?a ?b ?c ?d ?f
			         :where [?e :type :foo]
			                [(ground [0 0 0 0 0]) [[?a ?b ?c ?d ?f]]]]`,
			wantVars: []string{"?a", "?b", "?c", "?d", "?f"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			if tt.wantError {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err, "query should parse without error")

			// Find the expression clause with tuple binding
			var foundTupleBinding bool
			for _, clause := range q.Where {
				switch c := clause.(type) {
				case *query.Expression:
					if tb, ok := c.Binding.(query.TupleBinding); ok {
						foundTupleBinding = true
						require.Len(t, tb.Variables, len(tt.wantVars))
						for i, wantVar := range tt.wantVars {
							assert.Equal(t, datalog.NewSymbol(wantVar), tb.Variables[i])
						}
					}
				case *query.OrClause:
					// Check branches for tuple bindings
					for _, branch := range c.Branches {
						for _, bc := range branch {
							if expr, ok := bc.(*query.Expression); ok {
								if tb, ok := expr.Binding.(query.TupleBinding); ok {
									foundTupleBinding = true
									require.Len(t, tb.Variables, len(tt.wantVars))
									for i, wantVar := range tt.wantVars {
										assert.Equal(t, datalog.NewSymbol(wantVar), tb.Variables[i])
									}
								}
							}
						}
					}
				}
			}
			assert.True(t, foundTupleBinding, "should find a tuple binding in the parsed query")
		})
	}
}
