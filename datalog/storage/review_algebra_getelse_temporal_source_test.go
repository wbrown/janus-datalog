package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A get-else on a temporal handle passed as a named source answers the same
// with the algebra optimizer on and off (optimizer_modes_test.go): a query
// that fails on one path fails on the other, and one that answers answers
// alike.
func TestReviewAlgebra_GetElseOnATemporalSourceAnswersAlikeUnderBothOptimizerPaths(t *testing.T) {
	type outcome struct {
		tuples [][]interface{}
		failed bool
	}
	asOf := func(d *Database, first datalog.ElementID) *Database { return d.AsOf(first) }
	history := func(d *Database, _ datalog.ElementID) *Database { return d.History() }
	const onNamed = `[:find ?e ?v :in $ $src :where [$src ?e :t/name _] [(get-else $src ?e :t/a "none") ?v]]`
	const onDefault = `[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`
	cases := []struct {
		name   string
		handle func(d *Database, first datalog.ElementID) *Database
		named  bool
	}{
		{"as-of the first transaction, named $src", asOf, true},
		{"history, named $src", history, true},
		{"as-of the first transaction, queried as $", asOf, false},
		{"history, queried as $", history, false},
	}
	one, two := datalog.NewIdentity("review-algebra:one"), datalog.NewIdentity("review-algebra:two")
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			outcomes := map[bool]outcome{}
			for _, mode := range optimizerModes {
				d := createOptimizerModeDB(t, mode, DatabaseOptions{})
				tx := d.NewTransaction()
				require.NoError(t, tx.Add(one, kw(":t/name"), "one"))
				require.NoError(t, tx.Add(one, kw(":t/a"), "old"))
				require.NoError(t, tx.Add(two, kw(":t/name"), "two"))
				first, err := tx.Commit()
				require.NoError(t, err)
				tx = d.NewTransaction()
				require.NoError(t, tx.Add(one, kw(":t/a"), "new"))
				require.NoError(t, tx.Add(two, kw(":t/a"), "later"))
				_, err = tx.Commit()
				require.NoError(t, err)

				handle := c.handle(d, first)
				var got [][]interface{}
				if c.named {
					got, err = executor.CollectTuples(d.Query(onNamed,
						WithSources(map[query.Symbol]executor.PatternMatcher{datalog.NewSymbol("$src"): handle})))
				} else {
					got, err = executor.CollectTuples(handle.Query(onDefault))
				}
				outcomes[mode.algebra] = outcome{tuples: got, failed: err != nil}
			}
			require.Equal(t, outcomes[false].failed, outcomes[true].failed, "off %+v, on %+v", outcomes[false], outcomes[true])
			require.ElementsMatch(t, outcomes[false].tuples, outcomes[true].tuples)
		})
	}
}
