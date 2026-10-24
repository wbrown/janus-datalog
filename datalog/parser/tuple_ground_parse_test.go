package parser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An expression's binding takes Datomic's forms (NESTED_SCOPES_AS_QUERIES.md
// goal 12): the tuple form [?a ?b] destructures the function's one tuple,
// whatever the function, and the relation form [[?a ?b]] binds the relation
// over its collection of tuples. Each renders as written and parses back to
// the same binding, and a ground whose constant the form cannot bind is
// refused naming the repair: a flat ground under the relation form names the
// tuple form, and a ground of the wrong length or a ground inner vector of
// the wrong arity names both counts.
func TestParseExpressionBindings(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	idx, item := datalog.NewSymbol("?idx"), datalog.NewSymbol("?item")
	bindings := []struct {
		name    string
		query   string
		clause  int
		binding interface{}
	}{
		{"a flat ground under the tuple form",
			`[:find ?a ?b :where [(ground [0 0]) [?a ?b]]]`, 0,
			query.TupleBinding{Variables: []query.Symbol{a, b}}},
		{"a ground of vectors under the tuple form, each a value",
			`[:find ?a ?b :where [(ground [[1 2] [3 4]]) [?a ?b]]]`, 0,
			query.TupleBinding{Variables: []query.Symbol{a, b}}},
		{"a ground of vectors under the relation form",
			`[:find ?a ?b :where [(ground [[1 2] [3 4]]) [[?a ?b]]]]`, 0,
			query.RelationBinding{Variables: []query.Symbol{a, b}}},
		{"an empty ground under the relation form",
			`[:find ?a ?b :where [(ground []) [[?a ?b]]]]`, 0,
			query.RelationBinding{Variables: []query.Symbol{a, b}}},
		{"enumerate under the relation form",
			`[:find ?idx ?item :where [?x :t/vec ?vec] [(enumerate ?vec) [[?idx ?item]]]]`, 1,
			query.RelationBinding{Variables: []query.Symbol{idx, item}}},
		{"enumerate under the tuple form, each variable one pair",
			`[:find ?idx ?item :where [?x :t/vec ?vec] [(enumerate ?vec) [?idx ?item]]]`, 1,
			query.TupleBinding{Variables: []query.Symbol{idx, item}}},
		{"a ground under the collection form, one tuple per element",
			`[:find ?v :where [(ground [:a :e :i]) [?v ...]]]`, 0,
			query.CollectionBinding{Variable: datalog.NewSymbol("?v")}},
		{"enumerate under the collection form, one tuple per pair",
			`[:find ?p :where [?x :t/vec ?vec] [(enumerate ?vec) [?p ...]]]`, 1,
			query.CollectionBinding{Variable: datalog.NewSymbol("?p")}},
		{"a tuple ground in a fallback branch",
			`[:find ?e ?count ?total :where [?e :t/n _] (or-default [?e :t/pair ?count] [(ground [0 0]) [?count ?total]])]`, 1,
			query.TupleBinding{Variables: []query.Symbol{datalog.NewSymbol("?count"), datalog.NewSymbol("?total")}}},
	}
	for _, c := range bindings {
		t.Run(c.name, func(t *testing.T) {
			q, err := ParseQuery(c.query)
			require.NoError(t, err)
			expr := expressionAt(t, q.Where[c.clause])
			assert.Equal(t, c.binding, expr.Binding)

			formatted := q.String()
			assert.Contains(t, formatted, expr.String(), "the clause renders as written")
			reparsed, err := ParseQuery(formatted)
			require.NoError(t, err, "the rendering parses:\n%s", formatted)
			assert.Equal(t, c.binding, expressionAt(t, reparsed.Where[c.clause]).Binding)
		})
	}

	refusals := []struct {
		name   string
		query  string
		repair string
	}{
		{"a flat ground under the relation form names the tuple form",
			`[:find ?a ?b :where [(ground [0 0]) [[?a ?b]]]]`, "[?a ?b]"},
		{"a scalar ground under the collection form names the scalar form",
			`[:find ?v :where [(ground 1) [?v ...]]]`, "scalar form ?v"},
		{"a ground vector of another length than the tuple form",
			`[:find ?a ?b ?c :where [(ground [1 2]) [?a ?b ?c]]]`, "2 values, 3 binding variables"},
		{"a ground inner vector of another arity than the relation form",
			`[:find ?a ?b :where [(ground [[1 2] [3]]) [[?a ?b]]]]`, "tuple 2"},
	}
	for _, c := range refusals {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseQuery(c.query)
			require.Error(t, err, "the parser accepts %s", c.query)
			assert.Contains(t, err.Error(), c.repair)
		})
	}
}

// expressionAt is the expression a clause is, or the one its fallback form's
// last branch holds.
func expressionAt(t *testing.T, clause query.Clause) *query.Expression {
	t.Helper()
	switch c := clause.(type) {
	case *query.Expression:
		return c
	case *query.OrDefaultClause:
		last := c.Branches[len(c.Branches)-1]
		return expressionAt(t, last[len(last)-1])
	}
	t.Fatalf("clause %s is no expression", clause)
	return nil
}

// TestVectorConstantTaggedLiterals verifies that tagged literals (#inst, #db/id)
// are accepted inside vector constants: the vector element switch handles
// edn.NodeTagged.
//
// Round-trippability depends on it. FormatValueEDN emits #inst for time.Time
// values, and the parser has to read that back inside (ground [...]).
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
		                                          [(ground [:none #inst "0001-01-01T00:00:00Z"]) [?tag ?ts]])]`)
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
