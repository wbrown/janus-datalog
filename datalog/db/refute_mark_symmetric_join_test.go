package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Through the public db API, a join of a group whose expression fails on one
// tuple with a group holding no tuple answers alike under the default hash
// join and the symmetric hash join WithPlannerOptions enables.
func TestRefuteMark_EveryJoinStrategyAnswersAFailureBesideAnEmptySideAlike(t *testing.T) {
	v := datalog.NewKeyword(":item/v")
	queries := []string{
		`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x] [(identity ?x) ?b]]`,
		`[:find ?q :where [?p :item/missing ?x] [(identity ?x) ?b] [?q :item/v ?a] [(+ ?a 1) ?b]]`,
	}
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			type outcome struct {
				tuples [][]interface{}
				err    error
			}
			answers := map[bool][]outcome{}
			for _, symmetric := range []bool{false, true} {
				opts := mode.plannerOptions()
				opts.EnableSymmetricHashJoin = symmetric
				d, err := db.OpenMemory(db.WithPlannerOptions(opts))
				require.NoError(t, err)
				tx := d.NewTransaction()
				for i := 0; i < 16; i++ {
					entity := datalog.NewIdentity(fmt.Sprintf("refute-mark-dbsym:n%d", i))
					require.NoError(t, tx.Add(entity, v, int64(i)))
				}
				stringy := datalog.NewIdentity("refute-mark-dbsym:stringy")
				require.NoError(t, tx.Add(stringy, v, "x"))
				_, err = tx.Commit()
				require.NoError(t, err)
				for _, q := range queries {
					got, err := executor.CollectTuples(d.Query(q))
					answers[symmetric] = append(answers[symmetric], outcome{got, err})
				}
				require.NoError(t, d.Close())
			}
			for i, q := range queries {
				hash, sym := answers[false][i], answers[true][i]
				require.Equal(t, hash.err == nil, sym.err == nil, "query %s: hash join error %v, symmetric hash join error %v", q, hash.err, sym.err)
				require.ElementsMatch(t, hash.tuples, sym.tuples, "query %s", q)
			}
		})
	}
}
