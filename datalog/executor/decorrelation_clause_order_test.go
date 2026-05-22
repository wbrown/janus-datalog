package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

func TestLegacyDecorrelationPreservesInterleavedClauseDependencies(t *testing.T) {
	alice := datalog.NewIdentity("employee:alice")
	dept := datalog.NewIdentity("dept:engineering")
	project1 := datalog.NewIdentity("project:one")
	project2 := datalog.NewIdentity("project:two")

	datoms := []datalog.Datom{
		{E: alice, A: datalog.NewKeyword(":employee/name"), V: "Alice", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: alice, A: datalog.NewKeyword(":employee/dept"), V: dept, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: dept, A: datalog.NewKeyword(":dept/name"), V: "Engineering", Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
		{E: project1, A: datalog.NewKeyword(":project/dept"), V: dept, Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
		{E: project2, A: datalog.NewKeyword(":project/dept"), V: dept, Tx: datalog.ElementID{Lamport: 5, ReplicaID: 1}},
	}

	q, err := parser.ParseQuery(`
		[:find ?dept-copy ?count
		 :where
		 [?e :employee/name "Alice"]
		 [(q [:find ?dept
		      :in $ ?emp
		      :where [?emp :employee/dept ?dept]]
		     $ ?e) [[?dept]]]
		 [(identity ?dept) ?dept-copy]
		 [(q [:find (count ?p)
		      :in $ ?dept
		      :where [?p :project/dept ?dept]]
		     $ ?dept) [[?count]]]]`)
	require.NoError(t, err)

	matcher := NewMemoryPatternMatcher(datoms)

	baseline := NewExecutorWithOptions(matcher, nil, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})
	baselineResult, err := baseline.Execute(q)
	require.NoError(t, err)
	baselineTuples, err := CollectTuples(baselineResult, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{dept, int64(2)}}, baselineTuples)

	withLegacyDecor := NewExecutorWithOptions(matcher, nil, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})
	decorResult, err := withLegacyDecor.Execute(q)
	require.NoError(t, err)
	decorTuples, err := CollectTuples(decorResult, nil)
	require.NoError(t, err)
	require.Equal(t, baselineTuples, decorTuples)
}
