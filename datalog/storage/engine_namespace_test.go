package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A variable a user writes in a namespace the engine mints variables into,
// unify, scope, magic, adorned or delta, is refused at Query naming the
// variable and the namespace, wherever it stands, at any depth and in a rule
// set's text, so it never joins the engine's own: the later occurrence of a
// variable a pattern names twice, which UnifyPatterns writes as
// ?unify/<name>.<position>, and the holder of a collection input's whole
// value, ?scope/input.<position>. A variable outside those namespaces answers
// as any variable does
// (docs/proposals/RULES_WITNESSES_AND_SPECULATIVE_VALUES.md §1.1).
func TestEngineNamespace_AVariableInAMintedNamespaceIsRefusedAtQuery(t *testing.T) {
	x, y1, y2 := named("x"), named("y1"), named("y2")
	facts := []fact{
		{x, ":p/self", x},
		{x, ":p/n", int64(5)},
		{y1, ":r", int64(1)}, {y1, ":s", "a"},
		{y2, ":r", int64(2)}, {y2, ":s", "b"},
	}
	cases := []struct {
		name     string
		text     string
		rules    string // the rule set given as %, empty for none
		variable string // the variable the refusal names, empty for an accepted case
		tuples   []string
	}{
		{"the unify namespace beside a pattern naming a variable twice", `[:find ?n :where [?e :p/self ?e] [?e :p/n ?unify/e.2] [(identity ?unify/e.2) ?n]]`, "", "?unify/e.2", nil},
		{"the unify namespace in an or-join branch", `[:find ?n :where [?e :p/n _] (or-join [?e ?n] (and [?e :p/self ?e] [?e :p/n ?unify/e.2] [(identity ?unify/e.2) ?n]) (and [?e :p/none ?n]))]`, "", "?unify/e.2", nil},
		{"the unify namespace in a subquery's inner query", `[:find ?n :where [(q [:find ?m :where [?e :p/self ?e] [?e :p/n ?unify/e.2] [(identity ?unify/e.2) ?m]] $) [[?n]]]]`, "", "?unify/e.2", nil},
		{"the unify namespace in a rule body", `[:find ?n :in $ % :where (r ?n)]`, `[[(r ?n) [?e :p/self ?e] [?e :p/n ?unify/e.2] [(identity ?unify/e.2) ?n]]]`, "?unify/e.2", nil},
		{"the scope namespace in an inner find beside a collection argument", `[:find ?m ?w :where [(ground [1 2]) ?ns] [(q [:find ?n ?scope/input.1 :in $ [?n ...] :where [?y :r ?n] [?y :s ?scope/input.1]] $ ?ns) [[?m ?w]]]]`, "", "?scope/input.1", nil},
		{"the scope namespace at the top level", `[:find ?n :where [?e :p/n ?scope/input.1] [(identity ?scope/input.1) ?n]]`, "", "?scope/input.1", nil},
		{"the magic namespace in a pattern", `[:find ?n :where [?e :p/n ?magic.bf/x.0] [(identity ?magic.bf/x.0) ?n]]`, "", "?magic.bf/x.0", nil},
		{"the adorned namespace in a pattern", `[:find ?n :where [?e :p/n ?adorned.bf/x] [(identity ?adorned.bf/x) ?n]]`, "", "?adorned.bf/x", nil},
		{"the delta namespace in a pattern", `[:find ?n :where [?e :p/n ?delta/x] [(identity ?delta/x) ?n]]`, "", "?delta/x", nil},
		{"a variable with no namespace beside a pattern naming a variable twice", `[:find ?n :where [?e :p/self ?e] [?e :p/n ?w] [(identity ?w) ?n]]`, "", "", []string{"5"}},
		{"unify with no namespace", `[:find ?n :where [?e :p/n ?unify] [(identity ?unify) ?n]]`, "", "", []string{"5"}},
		{"a hyphenated name with no namespace", `[:find ?n :where [?e :p/n ?unify-me] [(identity ?unify-me) ?n]]`, "", "", []string{"5"}},
		{"a namespace that begins like an engine's", `[:find ?n :where [?e :p/n ?scoped/x] [(identity ?scoped/x) ?n]]`, "", "", []string{"5"}},
		{"an engine's kind after the first dot", `[:find ?n :where [?e :p/n ?x.unify/y] [(identity ?x.unify/y) ?n]]`, "", "", []string{"5"}},
		{"an engine's kind as the name", `[:find ?n :where [?e :p/n ?mine/unify] [(identity ?mine/unify) ?n]]`, "", "", []string{"5"}},
		{"a variable of the user's own in an inner find beside a collection argument", `[:find ?m ?w :where [(ground [1 2]) ?ns] [(q [:find ?n ?v :in $ [?n ...] :where [?y :r ?n] [?y :s ?v]] $ ?ns) [[?m ?w]]]]`, "", "", []string{"1 a", "2 b"}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					var inputs []interface{}
					if c.rules != "" {
						inputs = append(inputs, c.rules)
					}
					rel, err := d.Query(c.text, inputs...)
					if c.variable != "" {
						require.Error(t, err, "Query refuses the variable")
						require.ErrorContains(t, err, c.variable, "the refusal names the variable")
						require.ErrorContains(t, err, "namespace", "the refusal names the namespace")
						return
					}
					require.NoError(t, err)
					require.Equal(t, c.tuples, renderedTuples(t, rel, names))
				})
			}
		})
	}
}
