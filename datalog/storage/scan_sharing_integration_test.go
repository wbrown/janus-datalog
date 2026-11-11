package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// setupScanSharingTestDB creates a database with scenarios and tasks
// for testing scan sharing across decorrelated subqueries.
func setupScanSharingTestDB(t testing.TB, mode optimizerMode) *Database {
	t.Helper()
	db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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

	_, err := tx.Commit()
	require.NoError(t, err)

	return db
}

// scanSharingReferenceAnswer is what text answers over the scan-sharing
// fixture on the reference leg. A test of the matrix computes it before its
// legs and compares each leg's answer with it inside that leg, so a leg run
// alone compares against the reference.
func scanSharingReferenceAnswer(t *testing.T, text string) [][]interface{} {
	t.Helper()
	db := setupScanSharingTestDB(t, referenceMode)
	rel, err := db.Query(text)
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(rel, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 5, "the reference leg answers one tuple per scenario")
	return tuples
}

// A fallback whose subquery counts and sums each scenario's complete tasks
// answers on every leg what the reference leg answers. That the status
// pattern's scan is shared across the subquery's bodies is
// TestSubqueryBodiesScanningOnePatternReadItOnceUnderScanSharing's.
func TestScanSharing_DecorrelatedSubqueries(t *testing.T) {
	q := `[:find ?e ?count ?total
	       :where [?e :entity/type :entity.type/scenario]
	              (or-default [(q [:find (count ?t) (sum ?tok)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]
	                              [(get-else $ ?t :task/token-count 0) ?tok]]
	                      $ ?e) [[?count ?total]]]
	                  [(ground [0 0]) [?count ?total]])]`
	want := scanSharingReferenceAnswer(t, q)
	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, prefetchOption, fusionOption, aggregationOption) {
		t.Run(mode.name, func(t *testing.T) {
			db := setupScanSharingTestDB(t, mode)
			rel, err := db.Query(q)
			require.NoError(t, err)
			results, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, want, results, "%s answers what the reference leg answers", mode.name)
		})
	}
}

// The scenario count query answers on every leg what the reference leg
// answers: each scenario's 50 complete tasks.
func TestScanSharing_CorrectnessDifferential(t *testing.T) {
	q := `[:find ?e ?count
	       :where [?e :entity/type :entity.type/scenario]
	              (or-default [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?e) [[?count]]]
	                  [(ground 0) ?count])]`
	want := scanSharingReferenceAnswer(t, q)
	for _, r := range want {
		require.Equal(t, int64(50), r[1], "each scenario holds 50 complete tasks")
	}
	for _, mode := range legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption) {
		t.Run(mode.name, func(t *testing.T) {
			db := setupScanSharingTestDB(t, mode)
			rel, err := db.Query(q)
			require.NoError(t, err)
			results, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, want, results, "%s answers what the reference leg answers", mode.name)
		})
	}
}

// With scan sharing off no sharing event fires, and the query answers what
// the reference leg answers.
func TestScanSharing_OffRaisesNoSharingEvent(t *testing.T) {
	q := `[:find ?e ?count
	       :where [?e :entity/type :entity.type/scenario]
	              (or-default [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?e) [[?count]]]
	                  [(ground 0) ?count])]`
	want := scanSharingReferenceAnswer(t, q)
	for _, mode := range legsWhere(legsFor(optimizerModes, algebraOption, sharingOption, fusionOption, aggregationOption), func(m optimizerMode) bool { return !m.sharing }) {
		t.Run(mode.name, func(t *testing.T) {
			db := setupScanSharingTestDB(t, mode)
			var sharingEvents int
			opts := mode.plannerOptions()
			opts.Handler = recordingReach(t, func(e annotations.Event) {
				if e.Name == annotations.ScanSharingCacheHit || e.Name == annotations.ScanSharingCacheMiss {
					sharingEvents++
				}
			})
			_, rel, err := db.queryUnderPlannerOptions(opts, q)
			require.NoError(t, err)
			results, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			require.ElementsMatch(t, want, results, "%s answers what the reference leg answers", mode.name)
			require.Zero(t, sharingEvents, "no sharing event fires with scan sharing off")
		})
	}
}
