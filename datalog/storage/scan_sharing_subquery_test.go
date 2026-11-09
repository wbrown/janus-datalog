package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// optimizationMatrixReferenceAnswer is what queryText answers over the
// optimization matrix's scenarios on the reference leg with the cache off. A
// test of the matrix computes it before its legs and compares each leg's
// answer with it inside that leg, so a leg run alone compares against the
// reference.
func optimizationMatrixReferenceAnswer(t *testing.T, queryText string, scenarios, tasksPerScenario int) [][]interface{} {
	t.Helper()
	options := referenceMode.plannerOptions()
	db := openBackendDB(t, referenceMode.backend, DatabaseOptions{
		Schema:         optimizationMatrixSchema(),
		DisableCache:   true,
		PlannerOptions: &options,
	})
	populateOptimizationMatrix(t, db, scenarios, tasksPerScenario)
	result, err := db.Query(queryText)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, scenarios, "the reference leg answers one tuple per scenario")
	return tuples
}

// The checkpoint's subquery bodies each scan [?t :task/status
// :status/complete], each built over its window of the scenarios. No group a
// body is built over binds ?t, so a body whose status pattern comes first
// reads the pattern's extent, and under scan sharing storage reads the extent
// once for the query. A body whose key pattern binds ?t before its status
// pattern reads the status of those tasks, through the fused fetch, the cache
// or a binding-driven scan, which is no read of the extent. Every leg of the
// matrix answers what the reference leg answers
// (optimizationMatrixReferenceAnswer).
func TestSubqueryBodiesScanningOnePatternReadItOnceUnderScanSharing(t *testing.T) {
	const scenarios, tasksPerScenario = 6, 10
	status := datalog.NewKeyword(":task/status")
	queryText := optimizationMatrixQuery(0)
	want := optimizationMatrixReferenceAnswer(t, queryText, scenarios, tasksPerScenario)

	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		for _, cache := range cacheTestModes {
			leg := mode.name + "/" + cache.name
			t.Run(leg, func(t *testing.T) {
				var statusScans, served, created, windows, formIterators atomic.Int64
				var createdFor, statusStrategies []string
				statusSites := map[*query.DataPattern]int{}
				options := mode.plannerOptions()
				db := openBackendDB(t, mode.backend, DatabaseOptions{
					Schema:         optimizationMatrixSchema(),
					DisableCache:   cache.disableCache,
					PlannerOptions: &options,
					AnnotationHandler: func(event annotations.Event) {
						switch event.Name {
						case annotations.ScanSharingCacheHit:
							served.Add(1)
						case annotations.ScanSharingCacheMiss:
							created.Add(1)
							createdFor = append(createdFor, event.Data["fingerprint"].(string))
						case annotations.ScopeWindow:
							windows.Add(1)
						case annotations.OrFallbackIteratorCreated:
							formIterators.Add(1)
						case annotations.StorageScanComplete:
							pattern, ok := event.Data[annotations.KeyPattern].(*query.DataPattern)
							if !ok {
								return
							}
							attr, ok := pattern.GetA().(query.Constant)
							if !ok || attr.Value != status {
								return
							}
							// A scan a binding of ?t drives reads the status of
							// given tasks; the extent's reads are the scans no
							// binding drives.
							strategy, ok := event.Data[annotations.KeyStrategy].(annotations.ScanStrategy)
							if !ok || (strategy != annotations.ScanDirect && strategy != annotations.ScanVValidation) {
								return
							}
							statusScans.Add(1)
							statusStrategies = append(statusStrategies, fmt.Sprint(event.Data))
							statusSites[pattern]++
						}
					},
				})
				populateOptimizationMatrix(t, db, scenarios, tasksPerScenario)
				statusScans.Store(0)
				served.Store(0)
				created.Store(0)
				windows.Store(0)
				formIterators.Store(0)
				createdFor = nil
				statusStrategies = nil
				statusSites = map[*query.DataPattern]int{}

				result, err := db.Query(queryText)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(result, nil)
				require.NoError(t, err)
				require.Equal(t, want, tuples, "%s answers what the reference leg answers", leg)

				if mode.sharing {
					require.Equal(t, int64(1), statusScans.Load(),
						"under scan sharing the subquery bodies read the status pattern's extent from storage once "+
							"(the registry served %d matches and created %d scans, for %v, over %d windows "+
							"and %d form iterators; the status scans %v, and their scans per site %v)",
						served.Load(), created.Load(), createdFor, windows.Load(), formIterators.Load(), statusStrategies, statusSites)
				}
			})
		}
	}
}

// A subquery body reading [?t :task/status :status/complete] at its one site
// reads it in every window of the scenarios when the algebra optimizer is off,
// since the body runs once per window. Under scan sharing the site's windows
// read one shared scan, so storage reads the pattern once for the query. Every
// leg answers what the reference leg answers
// (optimizationMatrixReferenceAnswer).
func TestASiteReadInEveryWindowIsSharedAcrossItsWindows(t *testing.T) {
	const scenarios, tasksPerScenario = 6, 10
	status := datalog.NewKeyword(":task/status")
	const queryText = `[:find ?scenario ?taskCount
	  :where [?scenario :entity/type :entity.type/scenario]
	         [(q [:find (count ?t)
	              :in $ ?s
	              :where [?t :task/root ?s]
	                     [?t :task/status :status/complete]]
	             $ ?scenario) [[?taskCount]]]]`
	want := optimizationMatrixReferenceAnswer(t, queryText, scenarios, tasksPerScenario)

	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		for _, cache := range cacheTestModes {
			leg := mode.name + "/" + cache.name
			t.Run(leg, func(t *testing.T) {
				var statusScans, served, created, windows atomic.Int64
				options := mode.plannerOptions()
				db := openBackendDB(t, mode.backend, DatabaseOptions{
					Schema:         optimizationMatrixSchema(),
					DisableCache:   cache.disableCache,
					PlannerOptions: &options,
					AnnotationHandler: func(event annotations.Event) {
						switch event.Name {
						case annotations.ScanSharingCacheHit:
							served.Add(1)
						case annotations.ScanSharingCacheMiss:
							created.Add(1)
						case annotations.ScopeWindow:
							windows.Add(1)
						case annotations.StorageScanComplete:
							pattern, ok := event.Data[annotations.KeyPattern].(*query.DataPattern)
							if !ok {
								return
							}
							if attr, ok := pattern.GetA().(query.Constant); ok && attr.Value == status {
								statusScans.Add(1)
							}
						}
					},
				})
				populateOptimizationMatrix(t, db, scenarios, tasksPerScenario)
				statusScans.Store(0)
				served.Store(0)
				created.Store(0)
				windows.Store(0)

				result, err := db.Query(queryText)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(result, nil)
				require.NoError(t, err)
				require.ElementsMatch(t, want, tuples, "%s answers what the reference leg answers", leg)

				if !mode.algebra {
					require.Greater(t, windows.Load(), int64(1),
						"with the algebra optimizer off the body runs over more than one window")
				}
				if mode.sharing {
					require.Equal(t, int64(1), statusScans.Load(),
						"under scan sharing the body's windows read the status pattern from storage once "+
							"(the registry served %d matches and created %d scans over %d windows)",
						served.Load(), created.Load(), windows.Load())
				}
			})
		}
	}
}

// A query that reads one pattern at two sites, where one site's reader stops
// early, answers every item on every leg of the matrix. The group the query
// does not read is checked for a match, which reads one tuple and stops, and
// the scan the other site reads stays whole.
func TestAConsumerThatStopsEarlyLeavesTheSharedScanWhole(t *testing.T) {
	kind := datalog.NewKeyword(":item/kind")
	const queryText = `[:find ?e :where [?e :item/kind _] [?z :item/kind _]]`
	var items []datalog.Identity
	var want [][]interface{}
	for _, name := range []string{"one", "two", "three"} {
		item := datalog.NewIdentity("item:" + name)
		items = append(items, item)
		want = append(want, []interface{}{item})
	}

	for _, mode := range legsFor(backendModes) {
		for _, cache := range cacheTestModes {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				db := createCacheTestDB(t, mode, cache.disableCache, nil)
				tx := db.NewTransaction()
				for i, item := range items {
					require.NoError(t, tx.Add(item, kind, []string{"one", "two", "three"}[i]))
				}
				_, err := tx.Commit()
				require.NoError(t, err)

				result, err := db.Query(queryText)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(result, nil)
				require.NoError(t, err)
				require.ElementsMatch(t, want, tuples, "the query answers every item")
			})
		}
	}
}

// A scan the query reads at one site is not shared: no sharing event names
// it, and its one reader's early close releases it, so a scan read only for
// a match reports its storage completion at that check's close, before the
// result is closed. The control reads the kind pattern at two sites: under
// scan sharing its scan is created once and served once, and storage reads
// it once. The legs with scan sharing off report no sharing event on either
// query. Every leg answers every item on both.
func TestAScanReadAtOneSiteIsNotShared(t *testing.T) {
	kind, color := datalog.NewKeyword(":item/kind"), datalog.NewKeyword(":item/color")
	const twoSites = `[:find ?e :where [?e :item/kind _] [?z :item/kind _]]`
	const oneSite = `[:find ?e :where [?e :item/kind _] [?z :item/color _]]`
	var items []datalog.Identity
	var want [][]interface{}
	for _, name := range []string{"one", "two", "three"} {
		item := datalog.NewIdentity("item:" + name)
		items = append(items, item)
		want = append(want, []interface{}{item})
	}

	for _, mode := range legsFor(backendModes) {
		for _, cache := range cacheTestModes {
			t.Run(mode.name+"/"+cache.name, func(t *testing.T) {
				var kindScans, colorScans, served, created atomic.Int64
				db := createOptimizerModeDB(t, mode, DatabaseOptions{
					DisableCache: cache.disableCache,
					AnnotationHandler: func(event annotations.Event) {
						switch event.Name {
						case annotations.ScanSharingCacheHit:
							served.Add(1)
						case annotations.ScanSharingCacheMiss:
							created.Add(1)
						case annotations.StorageScanComplete:
							pattern, ok := event.Data[annotations.KeyPattern].(*query.DataPattern)
							if !ok {
								return
							}
							attr, ok := pattern.GetA().(query.Constant)
							if !ok {
								return
							}
							switch attr.Value {
							case kind:
								kindScans.Add(1)
							case color:
								colorScans.Add(1)
							}
						}
					},
				})
				tx := db.NewTransaction()
				for i, item := range items {
					require.NoError(t, tx.Add(item, kind, []string{"one", "two", "three"}[i]))
					require.NoError(t, tx.Add(item, color, []string{"red", "green", "blue"}[i]))
				}
				_, err := tx.Commit()
				require.NoError(t, err)
				reset := func() {
					kindScans.Store(0)
					colorScans.Store(0)
					served.Store(0)
					created.Store(0)
				}

				reset()
				result, err := db.Query(twoSites)
				require.NoError(t, err)
				tuples, err := executor.CollectTuples(result, nil)
				require.NoError(t, err)
				require.ElementsMatch(t, want, tuples, "the two-site query answers every item")
				if mode.sharing {
					require.Equal(t, int64(1), created.Load(), "the kind scan, read at two sites, is created once")
					require.Equal(t, int64(1), served.Load(), "the kind scan's second site is served the shared scan")
					require.Equal(t, int64(1), kindScans.Load(), "storage reads the kind pattern once for the query")
				} else {
					require.Zero(t, created.Load()+served.Load(), "with scan sharing off no scan is shared")
				}

				reset()
				result, err = db.Query(oneSite)
				require.NoError(t, err)
				it := result.Iterator()
				require.True(t, it.Next(), "the one-site query answers an item")
				require.Zero(t, created.Load()+served.Load(), "no sharing event names a pattern read at one site")
				require.Equal(t, int64(1), colorScans.Load(),
					"the color scan, read once for a match, is closed by that check's early stop and reports its completion before the result is closed")
				got := [][]interface{}{{it.Tuple()[0]}}
				for it.Next() {
					got = append(got, []interface{}{it.Tuple()[0]})
				}
				require.NoError(t, it.Error())
				require.NoError(t, it.Close())
				require.ElementsMatch(t, want, got, "the one-site query answers every item")
				require.Equal(t, int64(1), colorScans.Load(), "the result's close closes the color scan no second time")
			})
		}
	}
}
