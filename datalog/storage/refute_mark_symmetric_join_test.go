package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A failure beside a clause holding at no assignment fails no query under
// each join the legs select: the hash join, and the symmetric hash join on
// the legs that turn it on. Each query joins a group whose expression fails
// on one tuple with an empty group on a shared symbol, so the join holds no
// tuple.
func TestReviewMark_AFailureBesideAnEmptyJoinSideFailsNoQueryUnderEveryJoin(t *testing.T) {
	v := datalog.NewKeyword(":item/v")
	w := datalog.NewKeyword(":item/w")
	cases := []struct {
		name  string
		query string
	}{
		{"the failing group first, joined on the failing expression's output",
			`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?p :item/missing ?x] [(identity ?x) ?b]]`},
		{"the empty group first, joined on the failing expression's output",
			`[:find ?q :where [?p :item/missing ?x] [(identity ?x) ?b] [?q :item/v ?a] [(+ ?a 1) ?b]]`},
		{"the failing group joined with an empty group on a pattern's entity",
			`[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b] [?r :item/w ?c] [(+ ?c 1) ?d] [?r :item/missing ?q]]`},
	}
	for _, mode := range legsFor(optimizerModes, symmetricOption, fusionOption) {
		t.Run(mode.name, func(t *testing.T) {
			opts := mode.plannerOptions()
			d := openBackendDB(t, mode.backend, DatabaseOptions{PlannerOptions: &opts})
			tx := d.NewTransaction()
			for i := 0; i < 16; i++ {
				entity := datalog.NewIdentity(fmt.Sprintf("review-mark-sym:n%d", i))
				require.NoError(t, tx.Add(entity, v, int64(i)))
			}
			stringy := datalog.NewIdentity("review-mark-sym:stringy")
			require.NoError(t, tx.Add(stringy, v, "x"))
			for i := 0; i < 4; i++ {
				entity := datalog.NewIdentity(fmt.Sprintf("review-mark-sym:w%d", i))
				require.NoError(t, tx.Add(entity, w, int64(i)))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					got, err := executor.CollectTuples(d.Query(c.query))
					require.NoError(t, err)
					require.Empty(t, got)
				})
			}
		})
	}
}
