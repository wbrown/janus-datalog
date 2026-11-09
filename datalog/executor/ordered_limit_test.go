package executor_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestOrderedLimitWithNonProjectedKeyRetainsFullSortSemantics(t *testing.T) {
	nameAttr := datalog.NewKeyword(":person/name")
	scoreAttr := datalog.NewKeyword(":person/score")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: nameAttr, V: "A", Tx: tx},
		{E: datalog.NewIdentity("p1"), A: scoreAttr, V: int64(100), Tx: tx},
		{E: datalog.NewIdentity("p2"), A: nameAttr, V: "A", Tx: tx},
		{E: datalog.NewIdentity("p2"), A: scoreAttr, V: int64(90), Tx: tx},
		{E: datalog.NewIdentity("p3"), A: nameAttr, V: "B", Tx: tx},
		{E: datalog.NewIdentity("p3"), A: scoreAttr, V: int64(80), Tx: tx},
	}

	q, err := parser.ParseQuery(`[:find ?name
	                              :where [?e :person/name ?name]
	                                     [?e :person/score ?score]
	                              :order-by [[?score :desc]]
	                              :limit 2]`)
	require.NoError(t, err)

	matcher := factsDatabase(t, datoms)
	for _, mode := range LegsFor(t, FusionOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			result, err := exec.Execute(q)
			require.NoError(t, err)
			got, err := CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"A"}, {"B"}}, got)
		})
	}
}

func TestOrderedLimitAfterAggregationUsesGlobalTopN(t *testing.T) {
	city := datalog.NewKeyword(":person/city")
	tx := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("p1"), A: city, V: "BOS", Tx: tx},
		{E: datalog.NewIdentity("p2"), A: city, V: "LA", Tx: tx},
		{E: datalog.NewIdentity("p3"), A: city, V: "NYC", Tx: tx},
		{E: datalog.NewIdentity("p4"), A: city, V: "SF", Tx: tx},
	}

	q, err := parser.ParseQuery(`[:find ?city (count ?p)
	                              :where [?p :person/city ?city]
	                              :order-by [[?city :asc]]
	                              :limit 2]`)
	require.NoError(t, err)

	matcher := factsDatabase(t, datoms)
	for _, mode := range LegsFor(t) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())
			result, err := exec.Execute(q)
			require.NoError(t, err)
			got, err := CollectTuples(result, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"BOS", int64(1)}, {"LA", int64(1)}}, got)
		})
	}
}
