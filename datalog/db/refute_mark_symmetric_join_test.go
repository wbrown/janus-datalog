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
// tuple with a group holding no tuple answers on every leg what the reference
// leg answers; four legs run the symmetric hash join in place of the hash
// join.
func TestRefuteMark_EveryJoinStrategyAnswersAFailureBesideAnEmptySideAlike(t *testing.T) {
	v := datalog.NewKeyword(":item/v")
	queries := []string{
		`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x] [(identity ?x) ?b]]`,
		`[:find ?q :where [?p :item/missing ?x] [(identity ?x) ?b] [?q :item/v ?a] [(+ ?a 1) ?b]]`,
	}
	type outcome struct {
		tuples [][]interface{}
		err    error
	}
	// answers opens a database on the leg, holding sixteen numbered items and
	// one whose value is a string, and answers both queries on it.
	answers := func(t *testing.T, mode optimizerMode) []outcome {
		d, err := db.OpenMemory(db.WithPlannerOptions(mode.plannerOptions()))
		require.NoError(t, err)
		defer func() { require.NoError(t, d.Close()) }()
		tx := d.NewTransaction()
		for i := 0; i < 16; i++ {
			entity := datalog.NewIdentity(fmt.Sprintf("refute-mark-dbsym:n%d", i))
			require.NoError(t, tx.Add(entity, v, int64(i)))
		}
		stringy := datalog.NewIdentity("refute-mark-dbsym:stringy")
		require.NoError(t, tx.Add(stringy, v, "x"))
		_, err = tx.Commit()
		require.NoError(t, err)
		var out []outcome
		for _, q := range queries {
			got, err := executor.CollectTuples(d.Query(q))
			out = append(out, outcome{got, err})
		}
		return out
	}
	want := answers(t, referenceMode)
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			got := answers(t, mode)
			for i, q := range queries {
				require.Equal(t, want[i].err == nil, got[i].err == nil, "query %s: reference error %v, leg error %v", q, want[i].err, got[i].err)
				require.ElementsMatch(t, want[i].tuples, got[i].tuples, "query %s", q)
			}
		})
	}
}
