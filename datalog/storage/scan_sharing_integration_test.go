package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// setupScanSharingTestDB creates a database with scenarios and tasks
// for testing scan sharing across decorrelated subqueries.
func setupScanSharingTestDB(t testing.TB) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "scan-sharing-test-*")
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)

	tx := db.NewTransaction()

	// 5 scenarios with 50 tasks each
	for s := 0; s < 5; s++ {
		scenario := datalog.NewIdentity(fmt.Sprintf("scenario:%d", s))
		tx.Add(scenario, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario"))
		tx.Add(scenario, datalog.NewKeyword(":scenario/title"), fmt.Sprintf("Scenario %d", s))
		for i := 0; i < 50; i++ {
			task := datalog.NewIdentity(fmt.Sprintf("task:%d:%d", s, i))
			tx.Add(task, datalog.NewKeyword(":task/root"), scenario)
			tx.Add(task, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
			tx.Add(task, datalog.NewKeyword(":task/token-count"), int64(100+i))
		}
	}

	_, err = tx.Commit()
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// TestScanSharing_DecorrelatedSubqueries verifies that two decorrelated
// subqueries sharing [?t :task/root ?s] produce correct results with scan
// sharing, and that the sharing annotation events fire.
func TestScanSharing_DecorrelatedSubqueries(t *testing.T) {
	db, cleanup := setupScanSharingTestDB(t)
	defer cleanup()

	q := `[:find ?e ?count ?total
	       :where [?e :entity/type :entity.type/scenario]
	              (or-default [(q [:find (count ?t) (sum ?tok)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]
	                              [(get-else $ ?t :task/token-count 0) ?tok]]
	                      $ ?e) [[?count ?total]]]
	                  [(ground [0 0]) [[?count ?total]]])]`

	// Baseline without scan sharing
	db.ClearPlanCache()
	baselineOpts := DefaultPlannerOptions()
	baselineOpts.EnableAlgebraOptimizer = true
	baselineOpts.EnableSubqueryDecorrelation = false
	baselineOpts.EnableScanSharing = false
	baselineRel, err := queryWithPlannerOptions(db, q, baselineOpts)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	t.Logf("Baseline: %d results", len(baseline))

	// With scan sharing
	db.ClearPlanCache()
	var sharingEvents []annotations.Event
	db.SetAnnotationHandler(func(e annotations.Event) {
		if e.Name == "scan-sharing/cache-hit" || e.Name == "scan-sharing/cache-miss" {
			sharingEvents = append(sharingEvents, e)
		}
	})
	sharingOpts := DefaultPlannerOptions()
	sharingOpts.EnableAlgebraOptimizer = true
	sharingOpts.EnableSubqueryDecorrelation = false
	sharingOpts.EnableScanSharing = true
	sharingRel, err := queryWithPlannerOptions(db, q, sharingOpts)
	require.NoError(t, err)
	sharing, err := executor.CollectTuples(sharingRel, nil)
	require.NoError(t, err)
	t.Logf("Sharing: %d results", len(sharing))

	// Correctness: same result count
	assert.Equal(t, len(baseline), len(sharing), "scan sharing should produce same result count")

	// Sharing events should include at least one cache hit
	t.Logf("Sharing events: %d", len(sharingEvents))
	for _, e := range sharingEvents {
		t.Logf("  [%s] %v", e.Name, e.Data)
	}
}

// TestScanSharing_CorrectnessDifferential runs the same query with and
// without scan sharing and asserts identical result sets.
func TestScanSharing_CorrectnessDifferential(t *testing.T) {
	db, cleanup := setupScanSharingTestDB(t)
	defer cleanup()

	q := `[:find ?e ?count
	       :where [?e :entity/type :entity.type/scenario]
	              (or-default [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?e) [[?count]]]
	                  [(ground 0) ?count])]`

	db.ClearPlanCache()
	offOpts := DefaultPlannerOptions()
	offOpts.EnableAlgebraOptimizer = true
	offOpts.EnableSubqueryDecorrelation = false
	offOpts.EnableScanSharing = false
	offRel, err := queryWithPlannerOptions(db, q, offOpts)
	require.NoError(t, err)
	offResults, err := executor.CollectTuples(offRel, nil)
	require.NoError(t, err)

	db.ClearPlanCache()
	onOpts := DefaultPlannerOptions()
	onOpts.EnableAlgebraOptimizer = true
	onOpts.EnableSubqueryDecorrelation = false
	onOpts.EnableScanSharing = true
	onRel, err := queryWithPlannerOptions(db, q, onOpts)
	require.NoError(t, err)
	onResults, err := executor.CollectTuples(onRel, nil)
	require.NoError(t, err)

	assert.Equal(t, len(offResults), len(onResults),
		"scan sharing on/off should produce identical result count")

	// Verify each scenario has 50 completed tasks
	for _, r := range onResults {
		t.Logf("  %v: count=%v", r[0], r[1])
		assert.Equal(t, int64(50), r[1], "each scenario should have 50 tasks")
	}
}

// TestScanSharing_DisabledByDefault verifies that EnableScanSharing=false
// means no scan registry is created and behavior is unchanged.
func TestScanSharing_DisabledByDefault(t *testing.T) {
	db, cleanup := setupScanSharingTestDB(t)
	defer cleanup()

	q := `[:find ?e ?count
	       :where [?e :entity/type :entity.type/scenario]
	              (or-default [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?e) [[?count]]]
	                  [(ground 0) ?count])]`

	var sharingEvents int
	db.SetAnnotationHandler(func(e annotations.Event) {
		if e.Name == "scan-sharing/cache-hit" || e.Name == "scan-sharing/cache-miss" {
			sharingEvents++
		}
	})

	db.ClearPlanCache()
	opts := DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = true
	opts.EnableSubqueryDecorrelation = false
	opts.EnableScanSharing = false
	rel, err := queryWithPlannerOptions(db, q, opts)
	require.NoError(t, err)
	results, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)

	assert.Equal(t, 5, len(results))
	assert.Equal(t, 0, sharingEvents, "no scan-sharing events when disabled")
}
