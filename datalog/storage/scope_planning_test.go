package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// formFacts: g carries :t/k :p, :t/q to h and :t/r 2; h carries :t/k :p and
// :t/n 1.
func formFacts() []fact {
	g, h := named("g"), named("h")
	return []fact{
		{g, ":t/k", datalog.NewKeyword(":p")}, {g, ":t/q", h}, {g, ":t/r", int64(2)},
		{h, ":t/k", datalog.NewKeyword(":p")}, {h, ":t/n", int64(1)},
	}
}

// Every scope of a query is planned at PlanQuery, and the executor's framing
// reads the plan the planner made
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1): once Explain has
// planned a query holding an or-join, an or-default-join, a not-join and a
// subquery, executing it through Query adds no miss and no plan to the
// database's plan cache, on both planner paths and every backend.
func TestQuery_PlansEveryScopeAtPlanQuery(t *testing.T) {
	const text = `[:find ?e ?m ?k ?c
	 :where [?e :t/k ?a]
	        (or-join [[?e] ?m] [?e :t/q ?m] [(identity ?e) ?m])
	        (or-default-join [[?e] ?k] [?e :t/r ?k] [(ground 0) ?k])
	        (not-join [?e] [?e :t/n _])
	        [(q [:find (count ?s) :in $ ?a :where [?s :t/k ?a]] $ ?a) [[?c]]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			names := commitFacts(t, d, formFacts())
			d.ClearPlanCache()
			_, err := d.Explain(text)
			require.NoError(t, err)
			_, plannedMisses, plannedSize := d.PlanCache().Stats()
			rel, err := d.Query(text)
			require.NoError(t, err)
			require.Equal(t, []string{"g g 2 2", "g h 2 2"}, renderedTuples(t, rel, names))
			_, misses, size := d.PlanCache().Stats()
			require.Equal(t, plannedMisses, misses, "executing a planned query plans nothing the planner did not")
			require.Equal(t, plannedSize, size, "executing a planned query adds no plan to the cache")
		})
	}
}
