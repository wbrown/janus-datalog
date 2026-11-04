package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A correlated subquery answers what its run for each combination of its
// inputs answers, on both optimizer modes: where its inner :find names its
// input, where its binding names the correlation variable at the input's
// place or at another, where its binding names one variable twice, and where
// an equality binds its input and its :find names the equality's other side.
// Each entity is answered from the entities holding its :p/k.
func TestACorrelatedSubqueryAnswersAlikeWhereItsFindNamesItsInputOrItsBindingRepeatsAName(t *testing.T) {
	e1, e2, e3, e4, e5 := named("e1"), named("e2"), named("e3"), named("e4"), named("e5")
	facts := []fact{
		{e1, ":p/k", int64(1)}, {e1, ":p/v", int64(1)},
		{e2, ":p/k", int64(1)}, {e2, ":p/v", int64(1)},
		{e3, ":p/k", int64(1)}, {e3, ":p/v", int64(2)},
		{e4, ":p/k", int64(2)}, {e4, ":p/v", int64(2)},
		{e5, ":p/k", int64(3)}, {e5, ":p/v", int64(1)},
	}
	countsByK := [][]interface{}{{e1.id, int64(3)}, {e2.id, int64(3)}, {e3.id, int64(3)}, {e4.id, int64(1)}, {e5.id, int64(1)}}
	cases := []struct {
		name, text string
		want       [][]interface{}
	}{
		{"an aggregate whose find names its input, its binding naming the correlation variable there",
			`[:find ?q ?n :where [?q :p/k ?b] [(q [:find ?b (count ?z) :in $ ?b :where [?z :p/k ?b]] $ ?b) [[?b ?n]]]]`,
			countsByK},
		{"an aggregate whose find names its input, its binding naming a fresh variable there",
			`[:find ?q ?c ?n :where [?q :p/k ?b] [(q [:find ?b (count ?z) :in $ ?b :where [?z :p/k ?b]] $ ?b) [[?c ?n]]]]`,
			[][]interface{}{{e1.id, int64(1), int64(3)}, {e2.id, int64(1), int64(3)}, {e3.id, int64(1), int64(3)}, {e4.id, int64(2), int64(1)}, {e5.id, int64(3), int64(1)}}},
		{"an aggregate whose binding names the correlation variable at another find position",
			`[:find ?q ?n :where [?q :p/k ?b] [(q [:find ?v (count ?z) :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?b) [[?b ?n]]]]`,
			[][]interface{}{{e1.id, int64(2)}, {e2.id, int64(2)}, {e3.id, int64(2)}, {e4.id, int64(1)}}},
		{"an aggregate whose binding names one variable twice",
			`[:find ?q ?m :where [?q :p/k ?b] [(q [:find ?v (count ?z) :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v]] $ ?b) [[?m ?m]]]]`,
			[][]interface{}{{e5.id, int64(1)}}},
		{"a subquery whose find names its input, its binding naming the correlation variable there",
			`[:find ?q ?z :where [?q :p/k ?b] [(q [:find ?b ?z :in $ ?b :where [?z :p/k ?b] [?z :p/v ?v] [(> ?v 1)]] $ ?b) [[?b ?z]]]]`,
			[][]interface{}{{e1.id, e3.id}, {e2.id, e3.id}, {e3.id, e3.id}, {e4.id, e4.id}}},
		{"an aggregate whose input an equality binds, its find naming the equality's other side",
			`[:find ?q ?n :where [?q :p/k ?b] [(q [:find ?y (count ?z) :in $ ?b :where [?z :p/k ?y] [(= ?y ?b)]] $ ?b) [[?b ?n]]]]`,
			countsByK},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(d.Query(c.text))
					require.NoError(t, err, "the query answers what each combination's run answers")
					require.ElementsMatch(t, c.want, tuples, "the query answers what each combination's run answers")
				})
			}
		})
	}
}

// A correlated aggregating subquery answers no tuple for an outer tuple whose
// correlation value S holds no tuple at, since an aggregate over no tuples is
// no tuple. Without defaults Rule 1's rewrite joins each outer tuple to the
// group of its value, and a value with no group answers no tuple there either
// (docs/ALGEBRA.md, Rule 1). Under an or-default-join both paths answer the
// default for it. g1 has two members, and g2 has none.
func TestACorrelatedAggregateAnswersAValueWithNoTuplesAlikeOnBothOptimizerPaths(t *testing.T) {
	g1, g2, m1, m2 := named("g1"), named("g2"), named("m1"), named("m2")
	facts := []fact{
		{g1, ":grp/name", "g1"}, {g2, ":grp/name", "g2"},
		{m1, ":mem/grp", g1.id}, {m2, ":mem/grp", g1.id},
	}
	subquery := `[(q [:find (count ?m) :in $ ?g :where [?m :mem/grp ?g]] $ ?g) [[?c]]]`
	cases := []struct {
		name, text string
		want       [][]interface{}
	}{
		{"without defaults",
			`[:find ?n ?c :where [?g :grp/name ?n] ` + subquery + `]`,
			[][]interface{}{{"g1", int64(2)}}},
		{"under an or-default-join",
			`[:find ?n ?c :where [?g :grp/name ?n] (or-default-join [[?g] ?c] ` + subquery + ` [(ground 0) ?c])]`,
			[][]interface{}{{"g1", int64(2)}, {"g2", int64(0)}}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					tuples, err := executor.CollectTuples(d.Query(c.text))
					require.NoError(t, err)
					require.ElementsMatch(t, c.want, tuples, "g2, whose group holds no member, answers no tuple without defaults and the default under the fallback")
				})
			}
		})
	}
}
