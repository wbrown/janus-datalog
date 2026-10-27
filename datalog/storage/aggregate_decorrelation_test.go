package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// An aggregating subquery's inner query reads the sources its call passes,
// under its own :in names, wherever the call stands: at the top level, under
// a fallback, inside a union branch, grouped by a find variable, nested in a
// filtering call, and inside a not-join. The database $ holds p1 with :p/n 1
// and :p/name "home", and $other holds p1 with :p/n 10 and :p/name "away".
func TestAnAggregatingSubqueryReadsTheSourcesItsCallPasses(t *testing.T) {
	p1 := datalog.NewIdentity("p1")
	cases := []struct {
		name, text string
		want       [][]interface{}
	}{
		{"an inner :in $t given $other",
			`[:find ?e ?s :in $ $other :where [?e :p/n _] [(q [:find (sum ?v) :in $t ?x :where [$t ?x :p/n ?v]] $other ?e) [[?s]]]]`,
			[][]interface{}{{p1, int64(10)}}},
		{"an inner :in $t given $",
			`[:find ?e ?s :in $ $other :where [?e :p/n _] [(q [:find (sum ?v) :in $t ?x :where [$t ?x :p/n ?v]] $ ?e) [[?s]]]]`,
			[][]interface{}{{p1, int64(1)}}},
		{"an inner :in $ given $other",
			`[:find ?e ?s :in $ $other :where [?e :p/n _] [(q [:find (sum ?v) :in $ ?x :where [?x :p/n ?v]] $other ?e) [[?s]]]]`,
			[][]interface{}{{p1, int64(10)}}},
		{"under a fallback with a zero default",
			`[:find ?e ?s :in $ $other :where [?e :p/n _] (or-default-join [[?e] ?s] [(q [:find (sum ?v) :in $ ?x :where [?x :p/n ?v]] $other ?e) [[?s]]] [(ground 0) ?s])]`,
			[][]interface{}{{p1, int64(10)}}},
		{"inside a union branch beside a pattern",
			`[:find ?e ?s :in $ $other :where [?e :p/n _] (or-join [[?e] ?s] (and [?e :p/name _] [(q [:find (sum ?v) :in $ ?x :where [?x :p/n ?v]] $other ?e) [[?s]]]))]`,
			[][]interface{}{{p1, int64(10)}}},
		{"grouped by a find variable",
			`[:find ?e ?n ?s :in $ $other :where [?e :p/n _] [(q [:find ?n (sum ?v) :in $ ?x :where [?x :p/n ?v] [?x :p/name ?n]] $other ?e) [[?n ?s]]]]`,
			[][]interface{}{{p1, "away", int64(10)}}},
		{"nested in a filtering call that passes its source on",
			`[:find ?e ?s :in $ $other :where [?e :p/n _] [(q [:find ?s :in $t ?x :where [(q [:find (sum ?v) :in $u ?y :where [$u ?y :p/n ?v]] $t ?x) [[?s]]] [(> ?s 0)]] $other ?e) [[?s]]]]`,
			[][]interface{}{{p1, int64(10)}}},
		{"inside a not-join keyed on the sum",
			`[:find ?e :in $ $other :where [?e :p/n _] (not-join [?e] [(q [:find (sum ?v) :in $ ?x :where [?x :p/n ?v]] $other ?e) [[?s]]] [(< ?s 5)])]`,
			[][]interface{}{{p1}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			home := createOptimizerModeDB(t, mode, DatabaseOptions{})
			away := createOptimizerModeDB(t, mode, DatabaseOptions{})
			for d, held := range map[*Database]struct {
				name string
				n    int64
			}{home: {"home", 1}, away: {"away", 10}} {
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(p1, kw(":p/n"), held.n))
				require.NoError(t, tx.Add(p1, kw(":p/name"), held.name))
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(home.Query(c.text, WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$other"): away})))
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, tuples)
				})
			}
		})
	}
}

// An aggregating subquery answers under every shape of its :in, each entry
// bound by the call's argument at its place: a scalar beside a collection
// entry over another symbol or over the scalar's own, beside a tuple entry
// and beside a relation entry, and one outer variable passed at two scalar
// positions, whether the inner names them once or twice. Each entity is
// answered the sum of :p/w over the entities its arguments select.
func TestAnAggregatingSubqueryAnswersUnderEveryShapeOfItsIn(t *testing.T) {
	e1, e2, e3, e4 := named("e1"), named("e2"), named("e3"), named("e4")
	facts := []fact{
		{e1, ":p/n", int64(1)}, {e1, ":p/m", int64(2)}, {e1, ":p/k", int64(3)}, {e1, ":p/w", int64(100)},
		{e2, ":p/n", int64(1)}, {e2, ":p/m", int64(1)}, {e2, ":p/k", int64(4)}, {e2, ":p/w", int64(10)},
		{e3, ":p/n", int64(2)}, {e3, ":p/m", int64(2)}, {e3, ":p/k", int64(3)}, {e3, ":p/w", int64(1)},
		{e4, ":p/n", int64(1)}, {e4, ":p/m", int64(2)}, {e4, ":p/k", int64(3)}, {e4, ":p/w", int64(5)},
	}
	byN := [][]interface{}{{e1.id, int64(115)}, {e2.id, int64(115)}, {e3.id, int64(1)}, {e4.id, int64(115)}}
	byNK := [][]interface{}{{e1.id, int64(105)}, {e2.id, int64(10)}, {e3.id, int64(1)}, {e4.id, int64(105)}}
	cases := []struct {
		name, text string
		want       [][]interface{}
	}{
		{"a scalar beside a collection entry over another symbol",
			`[:find ?e ?s :where [?e :p/n ?a] [(ground [3 4]) ?b] [(q [:find (sum ?v) :in $ ?n [?m ...] :where [?x :p/n ?n] [?x :p/k ?m] [?x :p/w ?v]] $ ?a ?b) [[?s]]]]`,
			byN},
		{"a scalar beside a collection entry over its own symbol",
			`[:find ?e ?s :where [?e :p/n ?a] [(ground [1 2]) ?b] [(q [:find (sum ?v) :in $ ?n [?n ...] :where [?x :p/n ?n] [?x :p/w ?v]] $ ?a ?b) [[?s]]]]`,
			byN},
		{"a scalar beside a tuple entry",
			`[:find ?e ?s :where [?e :p/n ?a] [?e :p/m ?b] [?e :p/k ?c] [(q [:find (sum ?v) :in $ ?n [?m ?k] :where [?x :p/n ?n] [?x :p/m ?m] [?x :p/k ?k] [?x :p/w ?v]] $ ?a ?b ?c) [[?s]]]]`,
			byNK},
		{"a scalar beside a relation entry",
			`[:find ?e ?s :where [?e :p/n ?a] [?e :p/k ?c] [(q [:find (sum ?v) :in $ ?n [[?k]] :where [?x :p/n ?n] [?x :p/k ?k] [?x :p/w ?v]] $ ?a ?c) [[?s]]]]`,
			byNK},
		{"one outer variable at two scalar positions the inner names apart",
			`[:find ?e ?s :where [?e :p/m ?a] [(q [:find (sum ?v) :in $ ?n ?m :where [?x :p/n ?n] [?x :p/m ?m] [?x :p/w ?v]] $ ?a ?a) [[?s]]]]`,
			[][]interface{}{{e1.id, int64(1)}, {e2.id, int64(10)}, {e3.id, int64(1)}, {e4.id, int64(1)}}},
		{"one outer variable at two scalar positions the inner names alike",
			`[:find ?e ?s :where [?e :p/n ?a] [(q [:find (sum ?v) :in $ ?n ?n :where [?x :p/n ?n] [?x :p/w ?v]] $ ?a ?a) [[?s]]]]`,
			byN},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(d.Query(c.text))
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, tuples)
				})
			}
		})
	}
}
