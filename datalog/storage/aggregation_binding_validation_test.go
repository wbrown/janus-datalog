package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestQueryRejectsUnboundGroupedFindVariable(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode)

			total := datalog.NewKeyword(":order/total")
			tx := db.NewTransaction()
			for i := 0; i <= 100; i++ {
				require.NoError(t, tx.Add(
					datalog.NewIdentity(fmt.Sprintf("order-%d", i)),
					total,
					int64(i),
				))
			}
			_, err := tx.Commit()
			require.NoError(t, err)

			_, err = db.Query(
				`[:find ?missing (count ?order)
		  :where [?order :order/total ?total]]`,
			)
			require.Error(t, err)
			require.Contains(t, err.Error(), "group-by symbol ?missing")
		})
	}
}
