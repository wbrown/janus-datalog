package db_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/db"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// Through the public db API, a :limit 1 over a join of a group whose
// expression fails on one tuple with a group holding a tuple at every key
// answers the failure on every leg; four legs run the symmetric hash join,
// and each group holds more tuples than it reads in one batch. The failing
// tuple's other clauses hold, so the failure is the query's error, as the
// same text without :limit answers.
func TestRefute_ALimitOverAJoinOfAnEvaluatingGroupAnswersItsFailureUnderEveryJoinStrategy(t *testing.T) {
	v, w := datalog.NewKeyword(":item/v"), datalog.NewKeyword(":item/w")
	queries := []string{
		`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/w ?x] [(identity ?x) ?b] :limit 1]`,
		`[:find ?q :where [?p :item/w ?x] [(identity ?x) ?b] [?q :item/v ?a] [(+ ?a 1) ?b] :limit 1]`,
	}
	const n = 400
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d, err := db.OpenMemory(db.WithPlannerOptions(mode.plannerOptions()))
			require.NoError(t, err)
			defer func() { require.NoError(t, d.Close()) }()
			tx := d.NewTransaction()
			for i := 0; i < n; i++ {
				require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("refute-limit-sym:n%d", i)), v, int64(i)))
				require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("refute-limit-sym:w%d", i)), w, int64(i+1)))
			}
			require.NoError(t, tx.Add(datalog.NewIdentity("refute-limit-sym:stringy"), v, "x"))
			_, err = tx.Commit()
			require.NoError(t, err)
			for _, q := range queries {
				got, err := executor.CollectTuples(d.Query(q))
				require.ErrorContains(t, err, "is not a number", "query %s answered %v with no error", q, got)
			}
		})
	}
}
