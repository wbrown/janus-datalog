package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Through the public open API, a get-else on a named memory database or slice
// source answers what the same database answers with the algebra optimizer
// off (optimizer_modes_test.go): the memory database's resolved value, the
// datom with the latest transaction, and the slice source's fact, since a
// slice source is a database holding the slice's facts.
func TestReviewAlgebra_PublicGetElseOnANamedSourceAnswersAlikeUnderBothOptimizerPaths(t *testing.T) {
	one := datalog.NewIdentity("review-algebra:db:one")
	name, a := datalog.NewKeyword(":t/name"), datalog.NewKeyword(":t/a")
	facts := []datalog.Datom{
		{E: one, A: name, V: "one", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: one, A: a, V: "old", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		{E: one, A: a, V: "new", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
	}
	type item struct{ Name, A string }
	slice, err := db.NewSliceSource([]item{{"one", "a-one"}}, db.AttributeSchema[item]{
		name: func(i item) any { return i.Name },
		a:    func(i item) any { return i.A },
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, slice.Close()) }()
	cases := []struct {
		name    string
		source  db.PatternMatcher
		want    [][]interface{}
		wantErr string
	}{
		{"memory source", databaseOfLog(t, facts), [][]interface{}{{one, "new"}}, ""},
		{"slice source", slice, [][]interface{}{{datalog.NewIdentity("slice:0"), "a-one"}}, ""},
	}
	for _, mode := range legsFor(t, optimizerModes, algebraOption) {
		for _, c := range cases {
			t.Run(mode.name+"/"+c.name, func(t *testing.T) {
				d, err := db.OpenMemory(db.WithPlannerOptions(mode.plannerOptions()))
				require.NoError(t, err)
				defer func() { require.NoError(t, d.Close()) }()
				got, err := executor.CollectTuples(d.Query(
					`[:find ?e ?v :in $ $m :where [$m ?e :t/name _] [(get-else $m ?e :t/a "none") ?v]]`,
					db.WithSources(map[datalog.Symbol]db.PatternMatcher{datalog.NewSymbol("$m"): c.source})))
				if c.wantErr != "" {
					require.Error(t, err, "answered %v", got)
					require.Contains(t, err.Error(), c.wantErr)
					return
				}
				require.NoError(t, err)
				require.ElementsMatch(t, c.want, got)
			})
		}
	}
}
