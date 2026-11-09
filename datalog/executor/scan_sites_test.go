package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The site walk makes, for each pattern the plan reads in windows, the site of
// its existence check: a copy of the pattern, a site of its own. A pattern the
// plan does not read in windows has none.
func TestTheSiteWalkMakesAnExistenceSiteForEachWindowedPattern(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?x :where [?e :t/kind ?k] [?x :t/label ?k]]`)
	require.NoError(t, err)
	prepared, err := query.Prepare(q, nil)
	require.NoError(t, err)
	for _, algebra := range []bool{false, true} {
		t.Run(fmt.Sprintf("algebra=%v", algebra), func(t *testing.T) {
			plan, err := planner.CreatePlanner(nil, planner.PlannerOptions{EnableAlgebraOptimizer: algebra}).PlanQuery(prepared, query.Bindings{}, nil)
			require.NoError(t, err)
			require.Len(t, plan.Phases, 1, "the two patterns join in one phase")
			var windowed, first *query.DataPattern
			for _, clause := range plan.Phases[0].Query.Where {
				pattern := clause.(*query.DataPattern)
				if plan.Windows[clause] {
					windowed = pattern
				} else {
					first = pattern
				}
			}
			require.NotNil(t, windowed, "the plan reads one pattern in windows")
			require.NotNil(t, first, "the plan reads the other pattern first")

			sites := sharedScanSites(plan)
			site, made := sites.existence[windowed]
			require.True(t, made, "the windowed pattern has an existence site")
			require.NotSame(t, windowed, site, "the existence check is a site of its own")
			require.Equal(t, windowed.Elements, site.Elements, "the existence check matches the pattern")
			require.NotContains(t, sites.existence, first, "a pattern the plan does not read in windows has no existence check")

			registry := NewScanRegistry()
			registry.expect(sites)
			require.Same(t, site, registry.existenceSite(windowed), "the registry answers the existence site the walk made")
			require.Same(t, windowed, NewScanRegistry().existenceSite(windowed), "a registry given no listing, as with scan sharing off, answers the pattern itself")
		})
	}
}
