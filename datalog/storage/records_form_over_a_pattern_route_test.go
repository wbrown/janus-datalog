package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// A subquery over the result of a pattern that reads a subquery's output
// decides a failed tuple at its key. The failing tuple is the one whose
// :item/v is a string: the expression fails there, the first subquery and
// the pattern hold there, and the last subquery holds at no assignment
// extending it, since that item carries no :item/tag. So the failure is not
// raised and the query answers the sixteen numeric items, on both optimizer
// paths (docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md §1, the failure
// contract).
func TestRecords_ASubqueryOverAPatternsResultDecidesAFailedTupleAtItsKey(t *testing.T) {
	v := datalog.NewKeyword(":item/v")
	kind := datalog.NewKeyword(":item/kind")
	tag := datalog.NewKeyword(":item/tag")
	text := `[:find ?q :where [?q :item/v ?a] [(+ ?a 1) ?b]
		[(q [:find ?k :in $ ?q :where [?q :item/kind ?k]] $ ?q) [[?k]]]
		[?x :item/kind ?k]
		[(q [:find ?t :in $ ?q :where [?q :item/tag ?t]] $ ?q) [[?t]]]]`
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			d := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := d.NewTransaction()
			for i := 0; i < 16; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("records-pattern-route:n%d", i))
				require.NoError(t, tx.Add(e, v, int64(i)))
				require.NoError(t, tx.Add(e, kind, "num"))
				require.NoError(t, tx.Add(e, tag, "t"))
			}
			stringy := datalog.NewIdentity("records-pattern-route:stringy")
			require.NoError(t, tx.Add(stringy, v, "x"))
			require.NoError(t, tx.Add(stringy, kind, "str"))
			_, err := tx.Commit()
			require.NoError(t, err)

			got, err := executor.CollectTuples(d.Query(text))
			require.NoError(t, err, "the last subquery holds at no assignment extending the failing tuple")
			require.Len(t, got, 16, "each numeric item is answered")
		})
	}
}
