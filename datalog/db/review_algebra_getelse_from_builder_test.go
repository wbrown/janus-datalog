package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
)

// qb.GetElseFrom on a named memory database answers its resolved value, the
// datom with the latest transaction, under both optimizer paths
// (optimizer_modes_test.go).
func TestReviewAlgebra_GetElseFromOnAMemorySourceAnswersAlikeUnderBothOptimizerPaths(t *testing.T) {
	one := datalog.NewIdentity("review-algebra:qb:one")
	name, a := datalog.NewKeyword(":t/name"), datalog.NewKeyword(":t/a")
	held := databaseOfLog(t, []datalog.Datom{
		{E: one, A: name, V: "one", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: one, A: a, V: "old", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: one, A: a, V: "new", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	})
	m := qb.Source("$m")
	e, v := qb.NewVar("e"), qb.NewVar("v")
	q := qb.Query().Find(e, v).In(qb.DB, m).Where(
		qb.PatFrom(m, e, qb.Kw(":t/name"), qb.Blank()),
		qb.GetElseFrom(m, e, qb.Kw(":t/a"), "none").As(v),
	).MustBuild()
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, err := db.OpenMemory(db.WithPlannerOptions(mode.plannerOptions()))
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			got, err := executor.CollectTuples(d.Query(q,
				db.WithSources(map[datalog.Symbol]db.PatternMatcher{datalog.NewSymbol("$m"): held})))
			require.NoError(t, err)
			require.ElementsMatch(t, [][]interface{}{{one, "new"}}, got)
		})
	}
}
