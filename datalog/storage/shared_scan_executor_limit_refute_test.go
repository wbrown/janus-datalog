//go:build !(js && wasm)

package storage

import (
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// A query run through a database's executor (d.NewExecutor().Execute, the
// CLI's path for a query with no inputs, and ExecuteWithRelations under a
// caller's context) closes every storage iterator it opened once its result
// is read to its end and closed, as d.Query does, on every leg. On Badger, a
// WAL file surviving db.Close is an iterator left open
// (iterator_leak_test.go); on a counting store, opened equals closed. Badger
// is the instrument of the first family of subtests, so the backend is fixed
// there and the mode supplies the options alone.
func TestRefuteExecutorPathClosesEveryScanItOpened(t *testing.T) {
	texts := []struct {
		name string
		text string
	}{
		{"two sites joined on the value, limited", `[:find ?e ?f :where [?e :item/score ?v] [?f :item/score ?v] :limit 1]`},
		{"two sites joined on the value, limited to three", `[:find ?e ?f :where [?e :item/score ?v] [?f :item/score ?v] :limit 3]`},
	}
	entries := []struct {
		name string
		run  func(d *Database, text string) ([][]interface{}, error)
	}{
		{"NewExecutor().Execute", func(d *Database, text string) ([][]interface{}, error) {
			q, err := parser.ParseQuery(text)
			if err != nil {
				return nil, err
			}
			return executor.CollectTuples(d.NewExecutor().Execute(q))
		}},
		{"NewExecutor().ExecuteWithRelations under a caller's context", func(d *Database, text string) ([][]interface{}, error) {
			q, err := parser.ParseQuery(text)
			if err != nil {
				return nil, err
			}
			return executor.CollectTuples(d.NewExecutor().ExecuteWithRelations(executor.NewContext(), q, nil))
		}},
		{"Query", func(d *Database, text string) ([][]interface{}, error) {
			return executor.CollectTuples(d.Query(text))
		}},
	}
	score := datalog.NewKeyword(":item/score")
	fill := func(t *testing.T, d *Database, n int) {
		tx := d.NewTransaction()
		for i := 0; i < n; i++ {
			require.NoError(t, tx.Add(datalog.NewIdentity("executor-limit:"+strconv.Itoa(i)), score, int64(i)))
		}
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	for _, c := range texts {
		for _, mode := range legsFor(optimizerModes, sharingOption) {
			for _, entry := range entries {
				t.Run("badger/"+c.name+"/"+mode.name+"/"+entry.name, func(t *testing.T) {
					dir := t.TempDir()
					hits := 0
					opts := mode.plannerOptions()
					opts.Handler = recordingReach(t, func(e annotations.Event) {
						if e.Name == annotations.ScanSharingCacheHit {
							hits++
						}
					})
					d, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir, PlannerOptions: &opts})
					require.NoError(t, err)
					fill(t, d, 3000)
					tuples, err := entry.run(d, c.text)
					require.NoError(t, err)
					require.NotEmpty(t, tuples)
					t.Logf("%d tuples, shared-scan hits %d", len(tuples), hits)
					require.NoError(t, d.Close())
					found, err := os.ReadDir(dir)
					require.NoError(t, err)
					for _, file := range found {
						require.NotEqual(t, ".mem", filepath.Ext(file.Name()), "WAL file %s survives Close: a storage iterator the query opened was never closed", file.Name())
					}
				})
				t.Run("counting/"+c.name+"/"+mode.name+"/"+entry.name, func(t *testing.T) {
					var opened, closed atomic.Int32
					hits := 0
					opts := mode.plannerOptions()
					opts.Handler = recordingReach(t, func(e annotations.Event) {
						if e.Name == annotations.ScanSharingCacheHit {
							hits++
						}
					})
					d, err := NewDatabaseWithOptions(DatabaseOptions{
						Store:          scanTallyStore{Store: NewBadgerEmulatorStore(nil), opened: &opened, closed: &closed},
						PlannerOptions: &opts,
					})
					require.NoError(t, err)
					defer func() { require.NoError(t, d.Close()) }()
					fill(t, d, 3000)
					opened.Store(0)
					closed.Store(0)
					tuples, err := entry.run(d, c.text)
					require.NoError(t, err)
					require.NotEmpty(t, tuples)
					t.Logf("%d tuples, scans opened %d, closed %d, shared-scan hits %d", len(tuples), opened.Load(), closed.Load(), hits)
					require.Equal(t, opened.Load(), closed.Load(), "every scan the query opened is closed at its result's end")
				})
			}
		}
	}
}
