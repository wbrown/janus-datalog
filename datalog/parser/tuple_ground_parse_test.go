package parser

import (
	"testing"
	"time"

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

// TestVectorConstantTaggedLiterals verifies that tagged literals (#inst, #db/id)
// are accepted inside vector constants. The parser currently rejects them with
// "unsupported type in vector constant" because the vector element switch
// doesn't handle edn.NodeTagged.
//
// This blocks round-trippability: FormatValueEDN correctly emits #inst for
// time.Time values, but the parser can't parse it back inside (ground [...]).
func TestVectorConstantTaggedLiterals(t *testing.T) {
	t.Run("inst in ground vector", func(t *testing.T) {
		q, err := ParseQuery(`[:find ?tag ?ts
		                       :where [?e :item/tag ?tag]
		                              [(ground [:none #inst "2024-01-01T00:00:00Z"]) [?tag ?ts]]]`)
		require.NoError(t, err, "#inst must be valid inside ground vector")

		// Find the ground expression
		var found bool
		for _, clause := range q.Where {
			expr, ok := clause.(*query.Expression)
			if !ok {
				continue
			}
			gf, ok := expr.Function.(*query.GroundFunction)
			if !ok {
				continue
			}
			values, ok := gf.Value.([]interface{})
			if !ok {
				continue
			}
			found = true
			require.Len(t, values, 2)
			assert.Equal(t, datalog.NewKeyword(":none"), values[0])
			ts, ok := values[1].(time.Time)
			require.True(t, ok, "expected time.Time, got %T", values[1])
			assert.Equal(t, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), ts)
		}
		assert.True(t, found, "should find ground expression with vector")
	})

	t.Run("inst in or-default ground vector", func(t *testing.T) {
		q, err := ParseQuery(`[:find ?e ?tag ?ts
		                       :where [?e :item/status :status/done]
		                              (or-default [?e :item/tag ?tag]
		                                          [?e :item/updated-at ?ts]
		                                          [(ground [:none #inst "0001-01-01T00:00:00Z"]) [[?tag ?ts]]])]`)
		require.NoError(t, err, "#inst must be valid inside or-default ground vector")
		_ = q
	})

	t.Run("identity in ground vector", func(t *testing.T) {
		q, err := ParseQuery(`[:find ?ref ?label
		                       :where [(ground [#identity "0$&1Jt:M;j(7P!6s0BvD4k!,!" "N/A"]) [?ref ?label]]]`)
		require.NoError(t, err, "#identity must be valid inside ground vector")
		_ = q
	})
}
