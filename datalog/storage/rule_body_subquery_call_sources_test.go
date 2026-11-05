package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// A bound invocation's body holds a subquery over a constant input that
// invokes a rule and reads a get-else, and the call passes sources at the
// subquery's inner names. The entity held the name "uno" and then "one", and
// :t/a "old" and then "new". The query's $ is the latest view and $h the
// history handle, whose scan of :t/a answers both values where its lookup
// answers "new". On both optimizer paths each subquery reads the source its
// call passes at each inner name:
//   - With two sources swapped, the pattern reads $ through the inner $h and
//     the get-else reads $h through the inner $. The name "one" answers the
//     lookup's value. The name "uno", which the latest view no longer holds,
//     answers no tuple.
//   - With one source passed at two inner names, both read the history handle.
func TestARuleBodySubqueryReadsEachSourceItsCallPassesOnBothOptimizerPaths(t *testing.T) {
	name := datalog.NewKeyword(":t/name")
	a := datalog.NewKeyword(":t/a")
	one := datalog.NewIdentity("rule-body-call-sources:one")
	text := `[:find ?e ?v :in $ $h % :where [?e :t/name _] (r ?e ?v)]`
	cases := []struct {
		name  string
		rules string
		want  [][]interface{}
	}{
		{"two sources swapped, a name the latest view holds",
			`[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ $h % ?n :where [$h ?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $h $ % "one") [[?v]]]] [(s ?y) [?y :t/name _]]]`,
			[][]interface{}{{one, "new"}}},
		{"two sources swapped, a name only the history holds",
			`[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $ $h % ?n :where [$h ?x :t/name ?n] (s ?x) [(get-else $ ?x :t/a "none") ?w]] $h $ % "uno") [[?v]]]] [(s ?y) [?y :t/name _]]]`,
			nil},
		{"one source passed at two inner names",
			`[[(r ?e ?v) [?e :t/name _] [(q [:find ?w :in $a $b % ?n :where [$b ?x :t/name ?n] (s ?x) [(get-else $a ?x :t/a "none") ?w]] $h $h % "one") [[?v]]]] [(s ?y) [?y :t/name _]]]`,
			[][]interface{}{{one, "new"}}},
	}
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{Ident: name, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	s.Add(&schema.AttributeDefinition{Ident: a, ValueType: schema.TypeString, Cardinality: schema.CardinalityOne})
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})
			tx := d.NewTransaction()
			require.NoError(t, tx.Add(one, name, "uno"))
			require.NoError(t, tx.Add(one, a, "old"))
			_, err := tx.Commit()
			require.NoError(t, err)
			tx = d.NewTransaction()
			require.NoError(t, tx.Add(one, name, "one"))
			require.NoError(t, tx.Add(one, a, "new"))
			_, err = tx.Commit()
			require.NoError(t, err)
			sources := WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$h"): d.History()})
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					rules, err := parser.ParseRules(c.rules)
					require.NoError(t, err)
					got, err := executor.CollectTuples(d.Query(text, rules, sources))
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, got, "the subquery reads the source its call passes at each inner name")
				})
			}
		})
	}
}
