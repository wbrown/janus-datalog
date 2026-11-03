package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A query's result carries no derived set. A :find that is exactly an
// invocation's arguments answers the invocation's relation, the set's tuples
// the evaluation derived; a join with the result as its build side drains
// the result's tuples and probes no index the evaluation keeps, under each
// planner profile (docs/proposals/CONCURRENCY_PROOF.md, P2).
func TestExecuteWithRelations_AResultCarriesNoDerivedSet(t *testing.T) {
	x, n := datalog.NewSymbol("?x"), datalog.NewSymbol("?n")
	rs := MustRules(t, FollowsRulesText)
	q := MustParse(t, `[:find ?x ?y :in $ % :where (follows ?x ?y)]`)
	for _, mode := range OptimizerModes {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(factsDatabase(t, ChainDatoms(6)), nil, mode.PlannerOptions())
			result, err := exec.ExecuteWithRelations(NewContext().WithRules(rs), q, nil)
			require.NoError(t, err)
			require.NotZero(t, result.Size(), "the fixture's closure holds pairs")

			// The group holds one tuple per tuple of the result, its ?x and
			// an ordinal, so the two sides tie in size and the result, on the
			// right, is the build side.
			position := query.SymbolIndex(result.Symbols(), x)
			require.GreaterOrEqual(t, position, 0, "the result binds ?x")
			var groupTuples []Tuple
			perKey := map[interface{}]int{}
			require.NoError(t, ForEach(result, func(tuple Tuple) error {
				groupTuples = append(groupTuples, Tuple{tuple[position], int64(len(groupTuples))})
				perKey[tuple[position]]++
				return nil
			}))
			expected := 0
			for _, c := range perKey {
				expected += c * c
			}
			var events []annotations.Event
			opts := ExecutorOptionsFromPlanner(mode.PlannerOptions())
			opts.Handler = func(event annotations.Event) { events = append(events, event) }
			group := NewMaterializedRelationWithOptions([]query.Symbol{x, n}, groupTuples, opts)
			require.Equal(t, result.Size(), group.Size())
			count := 0
			require.NoError(t, ForEach(HashJoinWithOptions(group, result, []query.Symbol{x}, opts), func(Tuple) error {
				count++
				return nil
			}))
			require.Equal(t, expected, count, "the join answers the nested loop's pairs")

			var strategy, build []annotations.Event
			for _, event := range events {
				switch event.Name {
				case annotations.JoinStrategy:
					strategy = append(strategy, event)
				case annotations.JoinBuild:
					build = append(build, event)
				}
			}
			require.Len(t, strategy, 1)
			require.Equal(t, "right", strategy[0].Data["build_side"], "the result is the build side")
			require.Equal(t, false, strategy[0].Data["build_kept"], "the join probed an index the evaluation keeps")
			require.Len(t, build, 1)
			require.Equal(t, false, build[0].Data["build_kept"])
			require.Equal(t, result.Size(), build[0].Data["tuple_count"], "the join drained the result's tuples")
		})
	}
}
