package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A call whose inner :in omits the source answers what the same call with $
// written answers, under both optimizer modes: an aggregating inner query, a
// filtering inner query that does not aggregate, and an aggregating inner
// query whose :in names the rule set and no source. Each entity is answered
// over the entities sharing its :p/n.
func TestACallWhoseInnerOmitsTheSourceAnswersAsTheExplicitFormDoes(t *testing.T) {
	e1, e2, e3 := named("e1"), named("e2"), named("e3")
	facts := []fact{
		{e1, ":p/n", int64(1)}, {e1, ":p/w", int64(100)},
		{e2, ":p/n", int64(1)}, {e2, ":p/w", int64(10)},
		{e3, ":p/n", int64(2)}, {e3, ":p/w", int64(1)},
	}
	rules := `[[(weighs ?x ?n ?v) [?x :p/n ?n] [?x :p/w ?v]]]`
	cases := []struct {
		name, omitted, explicit string
		inputs                  []interface{}
	}{
		{"an aggregating inner query",
			`[:find ?e ?s :where [?e :p/n ?a] [(q [:find (sum ?v) :in ?n :where [?x :p/n ?n] [?x :p/w ?v]] ?a) [[?s]]]]`,
			`[:find ?e ?s :where [?e :p/n ?a] [(q [:find (sum ?v) :in $ ?n :where [?x :p/n ?n] [?x :p/w ?v]] $ ?a) [[?s]]]]`,
			nil},
		{"a filtering inner query that does not aggregate",
			`[:find ?e ?x :where [?e :p/n ?a] [(q [:find ?x :in ?n :where [?x :p/n ?n] [?x :p/w ?v] [(> ?v 5)]] ?a) [[?x] ...]]]`,
			`[:find ?e ?x :where [?e :p/n ?a] [(q [:find ?x :in $ ?n :where [?x :p/n ?n] [?x :p/w ?v] [(> ?v 5)]] $ ?a) [[?x] ...]]]`,
			nil},
		{"an aggregating inner query whose :in names the rule set and no source",
			`[:find ?e ?s :in $ % :where [?e :p/n ?a] [(q [:find (sum ?v) :in % ?n :where (weighs ?x ?n ?v)] % ?a) [[?s]]]]`,
			`[:find ?e ?s :in $ % :where [?e :p/n ?a] [(q [:find (sum ?v) :in $ % ?n :where (weighs ?x ?n ?v)] $ % ?a) [[?s]]]]`,
			[]interface{}{rules}},
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, facts)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					want, err := executor.CollectTuples(d.Query(c.explicit, c.inputs...))
					require.NoError(t, err, c.explicit)
					require.NotEmpty(t, want, "the explicit form answers tuples, so the comparison compares something")
					var got [][]interface{}
					require.NotPanics(t, func() { got, err = executor.CollectTuples(d.Query(c.omitted, c.inputs...)) }, c.omitted)
					require.NoError(t, err, c.omitted)
					require.ElementsMatch(t, want, got, "the omitted form answers what the explicit form answers; names %v", names)
				})
			}
		})
	}
}
