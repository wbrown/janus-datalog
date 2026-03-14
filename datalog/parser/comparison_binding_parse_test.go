package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestParseComparisonBinding verifies that [[(> ?x 0)] ?result] syntax parses
// correctly. This is a comparison expression that binds its boolean result to
// a variable — the format produced by the query builder's Gt().As() method.
//
// Bug: the parser only checked for node.Nodes[0].Type == edn.NodeList to
// detect expression patterns. But [[(> ?x 0)] ?result] has a vector wrapping
// a list as the first element, so it fell through to data pattern parsing
// which requires 3-4 elements.
func TestParseComparisonBinding(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantBind string
	}{
		{
			name: "greater-than binding",
			input: `[:find ?x ?positive
			         :where [?x :val ?v]
			                [[(> ?v 0)] ?positive]]`,
			wantBind: "?positive",
		},
		{
			name: "less-than binding",
			input: `[:find ?x ?cheap
			         :where [?x :price ?p]
			                [[(< ?p 100)] ?cheap]]`,
			wantBind: "?cheap",
		},
		{
			name: "equality binding",
			input: `[:find ?x ?match
			         :where [?x :status ?s]
			                [[(= ?s :active)] ?match]]`,
			wantBind: "?match",
		},
		{
			name: "comparison binding in OR branch",
			input: `[:find ?e ?count ?complete
			         :where [?e :type :scenario]
			                (or [(q [:find (count ?t)
			                         :in $ ?s
			                         :where [?t :task/root ?s]]
			                        $ ?e) [[?count]]]
			                    [(ground 0) ?count])
			                [[(> ?count 0)] ?complete]]`,
			wantBind: "?complete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := ParseQuery(tt.input)
			require.NoError(t, err, "query should parse without error")

			// Find the expression with the comparison binding
			var found bool
			for _, clause := range q.Where {
				if expr, ok := clause.(*query.Expression); ok {
					if sym, ok := expr.Binding.(query.Symbol); ok {
						if sym == datalog.NewSymbol(tt.wantBind) {
							found = true
						}
					}
				}
			}
			assert.True(t, found, "should find expression with binding %s", tt.wantBind)
		})
	}
}
