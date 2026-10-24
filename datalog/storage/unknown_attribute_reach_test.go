package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A keyword that arrives at a pattern's attribute position on the default
// source through a variable is a value, under every shape of arrival and
// both planner paths: a refusing database refuses nothing of it, and the
// pattern evaluates over the store, as a keyword at a value position does
// (docs/reference/SCHEMA.md, "Unknown attributes are refused on request").

// refusingStringDB opens a refusing database on mode declaring :t/string and
// :t/keyword, holding one entity whose :t/keyword is the typo the tests name,
// so a keyword at a value position has a datom to match.
func refusingStringDB(t *testing.T, mode optimizerMode) *Database {
	t.Helper()
	s, err := schema.NewBuilder().Attribute(":t/string").Type(schema.TypeString).Add().
		Attribute(":t/keyword").Type(schema.TypeKeyword).Add().Build()
	require.NoError(t, err)
	d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s, RefuseUnknownAttributes: true})
	tx := d.NewTransaction()
	require.NoError(t, tx.Add(datalog.NewIdentity("h"), kw(":t/string"), "x"))
	require.NoError(t, tx.Add(datalog.NewIdentity("h"), kw(":t/keyword"), kw(":t/strnig")))
	_, err = tx.Commit()
	require.NoError(t, err)
	return d
}

func TestQueryRunsAKeywordThroughEveryShapeOfReach(t *testing.T) {
	typo := kw(":t/strnig")
	cases := []struct {
		name, text, rules string
		inputs            []interface{}
	}{
		{"an input through a subquery into an invocation", `[:find ?e :in $ % ?a :where [(q [:find ?e :in $ % ?x :where (has ?e ?x)] $ % ?a) [[?e]]]]`, `[[(has ?e ?a) [?e ?a _]]]`, []interface{}{typo}},
		{"a constant through a rule body's subquery", `[:find ?e :in $ % :where (has :t/strnig ?e)]`, `[[(has [?a] ?e) [?h :t/string _] [(q [:find ?x :in $ ?attr :where [?x ?attr _]] $ ?a) [[?e]]]]]`, nil},
		{"an input through a rule body's subquery", `[:find ?e :in $ % ?b :where (has ?b ?e)]`, `[[(has [?a] ?e) [?h :t/string _] [(q [:find ?x :in $ ?attr :where [?x ?attr _]] $ ?a) [[?e]]]]]`, []interface{}{typo}},
		{"a constant through a rule's or-join header", `[:find ?e :in $ % :where (has :t/strnig ?e)]`, `[[(has [?a] ?e) [?e :t/string _] (or-join [[?a] ?e] [?e ?a _] [?e :t/string _])]]`, nil},
		{"a constant through a rule's not-join header", `[:find ?e :in $ % :where (has :t/strnig ?e)]`, `[[(has [?a] ?e) [?e :t/string _] (not-join [?e ?a] [?e ?a _])]]`, nil},
		{"a constant through two levels of rules", `[:find ?e :in $ % :where (has ?e :t/strnig)]`, `[[(has ?e ?a) (has2 ?e ?a)] [(has2 ?e ?b) [?e ?b _]]]`, nil},
		{"a constant through a rule's identity alias", `[:find ?e :in $ % :where (has :t/strnig ?e)]`, `[[(has [?a] ?e) [(identity ?a) ?b] [?e ?b _]]]`, nil},
		{"a constant invocation inside a not-join", `[:find ?e :in $ % :where [?e :t/string _] (not-join [?e] (has ?e :t/strnig))]`, `[[(has ?e ?a) [?e ?a _]]]`, nil},
		{"a constant invocation inside an or-join branch", `[:find ?e :in $ % :where (or-join [?e] (has ?e :t/strnig) [?e :t/string _])]`, `[[(has ?e ?a) [?e ?a _]]]`, nil},
		{"a ground into an invocation", `[:find ?e :in $ % :where [(ground :t/strnig) ?a] (has ?e ?a)]`, `[[(has ?e ?a) [?e ?a _]]]`, nil},
		{"a ground collection", `[:find ?e :where [(ground [:t/string :t/strnig]) [?a ...]] [?e ?a _]]`, "", nil},
		{"a ground relation", `[:find ?e :where [(ground [[:t/strnig]]) [[?a]]] [?e ?a _]]`, "", nil},
		{"a ground tuple", `[:find ?e :where [(ground [:t/strnig 1]) [?a ?n]] [?e ?a _]]`, "", nil},
		{"a tuple input", `[:find ?e :in $ [?a ?n] :where [?e ?a _]]`, "", []interface{}{[]interface{}{typo, int64(1)}}},
		{"a relation input", `[:find ?e :in $ [[?a ?n]] :where [?e ?a _]]`, "", []interface{}{[][]interface{}{{typo, int64(1)}}}},
		{"a relation input in its other spelling", `[:find ?e :in $ [[?a ?n] ...] :where [?e ?a _]]`, "", []interface{}{[][]interface{}{{typo, int64(1)}}}},
		{"a subquery's tuple input", `[:find ?e :in $ ?b :where [(q [:find ?x :in $ [?attr ?n] :where [?x ?attr _]] $ ?b 1) [[?e]]]]`, "", []interface{}{typo}},
		{"a relation input into an invocation in a subquery body", `[:find ?e :in $ % [[?a]] :where [?h :t/string _] [(q [:find ?x :in $ % ?y :where (has ?x ?y)] $ % ?a) [[?e]]]]`, `[[(has ?e ?a) [?e ?a _]]]`, []interface{}{[][]interface{}{{typo}}}},
		{"a constant through a rule's or-default-join header", `[:find ?e ?x :in $ % :where (has :t/strnig ?e ?x)]`, `[[(has [?a] ?e ?x) [?e :t/string _] (or-default-join [[?e ?a] ?x] [?e ?a ?x] [(ground "none") ?x])]]`, nil},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := refusingStringDB(t, mode)
				var inputs []interface{}
				if c.rules != "" {
					inputs = append(inputs, mustRuleSet(t, c.rules))
				}
				inputs = append(inputs, c.inputs...)
				_, err := executor.CollectTuples(d.Query(c.text, inputs...))
				require.NoError(t, err, "a keyword reaching the attribute position through a variable is a value")
			})
		}
	}
}

// A keyword at a value position names no attribute: a rule position its body
// reads as a value, a subquery's inner :in at a value position, a rule head
// variable a branch local outside the header shadows, an input a subquery's
// own scope shadows, and a rule whose bodies read the position as a value in
// one and as an attribute in another each run.
func TestQueryRunsAKeywordAtAValuePosition(t *testing.T) {
	typo := kw(":t/strnig")
	cases := []struct {
		name, text, rules string
		inputs            []interface{}
	}{
		{"a constant at a rule's value position", `[:find ?e :in $ % :where (has ?e :t/strnig)]`, `[[(has ?e ?v) [?e :t/keyword ?v]]]`, nil},
		{"two bodies, one reading the position as an attribute", `[:find ?e :in $ % :where (has :t/strnig ?e)]`, `[[(has [?v] ?e) [?e :t/keyword ?v]] [(has [?v] ?e) [?e ?v _]]]`, nil},
		{"a subquery's inner :in at a value position", `[:find ?e :in $ ?a :where [(q [:find ?x :in $ ?k :where [?x :t/keyword ?k]] $ ?a) [[?e]]]]`, "", []interface{}{typo}},
		{"a rule head variable a branch local shadows", `[:find ?e :in $ % :where (has :t/strnig ?e)]`, `[[(has [?a] ?e) [?e :t/keyword ?a] (or-join [?e] [?e ?a _])]]`, nil},
		{"an input a subquery's own scope shadows", `[:find ?e :in $ ?a :where [?h :t/keyword ?a] [(q [:find ?x :where [?x ?a _]] $) [[?e]]]]`, "", []interface{}{typo}},
	}
	for _, mode := range optimizerModes {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d := refusingStringDB(t, mode)
				var inputs []interface{}
				if c.rules != "" {
					inputs = append(inputs, mustRuleSet(t, c.rules))
				}
				inputs = append(inputs, c.inputs...)
				_, err := executor.CollectTuples(d.Query(c.text, inputs...))
				require.NoError(t, err, "a keyword at a value position is data")
			})
		}
	}
}
