package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// The qb route to TestReviewRoute_MemorySourceGetElseAnswersOneValueOnEveryRoute:
// GetElseFrom on a memory source answers the latest transaction's value on
// both optimizer paths.
func TestReviewRoute_MemorySourceGetElseFromAnswersOneValue(t *testing.T) {
	one := datalog.NewIdentity("review-route-qb:one")
	two := datalog.NewIdentity("review-route-qb:two")
	facts := []datalog.Datom{
		{E: one, A: kw(":item/name"), V: "one", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: one, A: kw(":item/tag"), V: "red", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
		{E: one, A: kw(":item/tag"), V: "blue", Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
		{E: two, A: kw(":item/name"), V: "two", Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
	}
	m := qb.Source("$m")
	e, n, v := qb.NewVar("e"), qb.NewVar("n"), qb.NewVar("v")
	built := qb.Query().Find(n, v).In(qb.DB, m).Where(
		qb.PatFrom(m, e, qb.Kw(":item/name"), n),
		qb.GetElseFrom(m, e, qb.Kw(":item/tag"), "none").As(v),
	).MustBuild()
	for _, mode := range legsFor(optimizerModes, algebraOption) {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tuples, err := executor.CollectTuples(d.Query(built, sourceNamed("$m", logDatabase(t, facts))))
			require.NoError(t, err)
			sort.Slice(tuples, func(i, j int) bool { return fmt.Sprint(tuples[i]) < fmt.Sprint(tuples[j]) })
			require.Equal(t, [][]interface{}{{"one", "blue"}, {"two", "none"}}, tuples, "query: %s", built)
		})
	}
}
