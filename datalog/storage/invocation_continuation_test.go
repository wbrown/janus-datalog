package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An invocation inside a subquery inside a fallback branch continues one
// evaluation across the form's windows whichever route each window takes
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.2 and §4.4): a window
// whose body holds an empty group before the invocation seeds the derivation
// through it, and a window whose body streams reads the invocation through
// its own window. The subject is a relation input with the sink first.
func TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			counts := &ruleEventCounts{}
			d := createOptimizerModeDB(t, mode, DatabaseOptions{AnnotationHandler: counts.handle})
			g := writeGraph(t, d, chain(10))
			rules := mustRuleSet(t, closureRules)

			rel, err := d.Query(`[:find ?x :in $ % ?s :where [?x :type :node] (reach ?x ?s)]`, rules, nodeID("n10"))
			require.NoError(t, err)
			require.ElementsMatch(t, chainReaching(10), g.tuples(t, rel))
			atBaseline := counts.snapshot()
			require.Equal(t, 1, atBaseline[0], "the top-level invocation rewrites once")

			subject := make([]interface{}, 0, 10)
			for i := 10; i >= 1; i-- {
				subject = append(subject, []interface{}{nodeID(nodeName(i))})
			}
			counts.reset()
			rel, err = d.Query(`[:find ?x ?k :in $ % ?s [[?x] ...] :where (or-default [(q [:find (count ?y) :in $ % ?x ?s :where (reach ?x ?s) [?x :edge ?y]] $ % ?x ?s) [[?k]]] [(ground 0) ?k])]`, rules, nodeID("n10"), subject)
			require.NoError(t, err)
			require.ElementsMatch(t, append(chainReachingWith(10, ">1"), "n10>0"), g.tuples(t, rel))
			require.Equal(t, atBaseline, counts.snapshot(), "rewrites, strata and the tuples entering a delta with the sink's window first, against the top-level invocation's; the events:\n%s", counts.events())
			require.True(t, counts.numberedPerWindow(), "each window's iterations number from zero; the events:\n%s", counts.events())
		})
	}
}
