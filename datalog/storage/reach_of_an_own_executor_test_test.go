package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestStorageBackedPureAggregation builds its executor and matcher from
// mode.plannerOptions() under a handler of its own, and asserts the streaming
// path on the legs with streaming aggregation and the batch path on the
// others. Its aggregation selects the streaming strategy on every leg with
// streaming aggregation on. CLAUDE.md item 6: a test with a handler of its own
// has "its reach ... recorded as every other test's". Under -reach the record
// under the test's name holds streaming aggregation, so legsFor runs it on a
// leg with the option off, where its batch assertion runs.
func TestAnOwnHandlerTestInStorageHasItsReachRecorded(t *testing.T) {
	saved := *reachReport
	*reachReport = filepath.Join(t.TempDir(), "reach.txt")
	t.Cleanup(func() { *reachReport = saved })

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			symbol := datalog.NewIdentity("symbol:CRWV")
			require.NoError(t, tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), "CRWV"))
			for i := 0; i < 500; i++ {
				bar := datalog.NewIdentity(fmt.Sprintf("bar:%d", i))
				require.NoError(t, tx.Add(bar, datalog.NewKeyword(":price/symbol"), symbol))
				require.NoError(t, tx.Add(bar, datalog.NewKeyword(":price/high"), float64(100+i)))
			}
			_, err := tx.Commit()
			require.NoError(t, err)
			testStorageBackedPureAggregation(t, db, mode)
		})
	}
	reachRecorded.Lock()
	recorded := reachRecorded.byTest[t.Name()]
	reachRecorded.Unlock()
	require.True(t, recorded[aggregationOption], "the record holds streaming aggregation, which the aggregation selects: %v", recorded)
}
