package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A scan the query's plan reads at two sites, whose readers both stop early,
// reports its storage completion by the time the query's result is closed:
// the query's end closes the shared scan's source. Before the close it has
// reported none, since no reader read it to its end. A leg with scan sharing
// off holds no shared scan, so the legs are those with sharing.
func TestASharedScanReportsItsCompletionWhenTheQueryEnds(t *testing.T) {
	kind := datalog.NewKeyword(":item/kind")
	const queryText = `[:find ?e :where [?e :item/kind _] [?z :item/kind _]]`

	for _, mode := range legsWhere(legsFor(backendModes, sharingOption), func(m optimizerMode) bool { return m.sharing }) {
		for _, cache := range cacheTestModes {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				var kindScans atomic.Int64
				db := createOptimizerModeDB(t, mode, DatabaseOptions{
					DisableCache: cache.disableCache,
					AnnotationHandler: func(event annotations.Event) {
						if event.Name != annotations.StorageScanComplete {
							return
						}
						pattern, ok := event.Data[annotations.KeyPattern].(*query.DataPattern)
						if !ok {
							return
						}
						if attr, ok := pattern.GetA().(query.Constant); ok && attr.Value == kind {
							kindScans.Add(1)
						}
					},
				})
				tx := db.NewTransaction()
				for i := 0; i < 50; i++ {
					require.NoError(t, tx.Add(datalog.NewIdentity(fmt.Sprintf("item:%d", i)), kind, "widget"))
				}
				_, err := tx.Commit()
				require.NoError(t, err)
				kindScans.Store(0)

				result, err := db.Query(queryText)
				require.NoError(t, err)
				it := result.Iterator()
				require.True(t, it.Next(), "the query answers an item")
				require.Zero(t, kindScans.Load(), "no reader has read the shared scan to its end")
				require.NoError(t, it.Close())
				require.Equal(t, int64(1), kindScans.Load(),
					"the query's end closes the shared scan, which reports its completion")
			})
		}
	}
}

// A query that fails at the call after a shared scan was created, and before
// anything read the scan, reports the scan's storage completion by the time
// the call returns: the query's end closes the shared scan's source on the
// failed path as it does when the result is closed. The two kind patterns
// carry a constant value, so the planner orders them first and they share one
// scan that no clause reads before the query fails. The fused fetch of
// :item/n reads the :item/m scan whole at its clause, and that scan faults
// under the read, so the legs are those with sharing and fusion.
func TestASharedScanReportsItsCompletionWhenTheQueryFailsAtTheCall(t *testing.T) {
	kind, m, n := datalog.NewKeyword(":item/kind"), datalog.NewKeyword(":item/m"), datalog.NewKeyword(":item/n")
	const queryText = `[:find ?x ?n :where [?e :item/kind "widget"] [?z :item/kind "widget"] [?x :item/m "widget"] [?x :item/n ?n]]`

	for _, mode := range legsWhere(legsFor(backendModes, sharingOption, fusionOption), func(m optimizerMode) bool { return m.sharing && m.fusion }) {
		for _, cache := range cacheTestModes {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				var kindScans atomic.Int64
				base, err := mode.backend.Open(t.TempDir(), &BinaryKeyEncoder{})
				require.NoError(t, err)
				store := &scanFaultStore{Store: base, failAfter: 20}
				opts := mode.plannerOptions()
				db, err := NewDatabaseWithOptions(DatabaseOptions{
					Store:          store,
					PlannerOptions: &opts,
					DisableCache:   cache.disableCache,
					AnnotationHandler: func(event annotations.Event) {
						if event.Name != annotations.StorageScanComplete {
							return
						}
						pattern, ok := event.Data[annotations.KeyPattern].(*query.DataPattern)
						if !ok {
							return
						}
						if attr, ok := pattern.GetA().(query.Constant); ok && attr.Value == kind {
							kindScans.Add(1)
						}
					},
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close()) })
				tx := db.NewTransaction()
				for i := 0; i < 50; i++ {
					e := datalog.NewIdentity(fmt.Sprintf("item:%d", i))
					require.NoError(t, tx.Add(e, kind, "widget"))
					require.NoError(t, tx.Add(e, m, "widget"))
					require.NoError(t, tx.Add(e, n, int64(i)))
				}
				_, err = tx.Commit()
				require.NoError(t, err)
				store.arm()
				kindScans.Store(0)

				_, err = db.Query(queryText)
				require.ErrorContains(t, err, errInjectedScanFault.Error(), "the fused fetch's read of the :item/m scan fails the query at the call")
				require.Equal(t, int64(1), kindScans.Load(),
					"the failed query's end closes the shared scan, which reports its completion")
			})
		}
	}
}
