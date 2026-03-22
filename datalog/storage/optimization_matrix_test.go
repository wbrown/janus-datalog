package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// TestOptimizationMatrix runs the production-structure query with all
// combinations of optimizer flags to measure their individual and combined
// impact. Uses 75 scenarios × 100 tasks (7,500 tasks total).
func TestOptimizationMatrix(t *testing.T) {
	dir, err := os.MkdirTemp("", "opt-matrix-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const (
		numScenarios     = 75
		tasksPerScenario = 100
	)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	defer db.Close()

	// Populate data
	baseTime := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	statusComplete := datalog.NewKeyword(":status/complete")
	kwOpening := datalog.NewKeyword(":task/opening")

	for s := 0; s < numScenarios; s++ {
		tx := db.NewTransaction()
		scenario := datalog.NewIdentity(fmt.Sprintf("scenario:%d", s))
		require.NoError(t, tx.Add(scenario, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario")))
		require.NoError(t, tx.Add(scenario, datalog.NewKeyword(":scenario/title"), fmt.Sprintf("Scenario %d", s)))
		require.NoError(t, tx.Add(scenario, datalog.NewKeyword(":scenario/created-at"), baseTime.Add(time.Duration(s)*time.Hour)))

		for j := 0; j < tasksPerScenario; j++ {
			task := datalog.NewIdentity(fmt.Sprintf("task:%d:%d", s, j))
			require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/root"), scenario))
			require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/status"), statusComplete))
			require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/completed-at"), baseTime.Add(time.Duration(s)*time.Hour+time.Duration(j)*time.Minute)))
			require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/token-count"), int64(500+j*10)))
			require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/duration"), int64(time.Duration(30+j)*time.Second)))
			if j == 0 {
				require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/key"), kwOpening))
			} else {
				require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/key"), datalog.NewKeyword(fmt.Sprintf(":scenario/task-%d", j))))
			}
		}
		_, err = tx.Commit()
		require.NoError(t, err)
	}
	t.Logf("Populated %d scenarios × %d tasks = %d tasks", numScenarios, tasksPerScenario, numScenarios*tasksPerScenario)

	queryStr := `[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete ?lastKey ?lastUpdatedAt
	  :where
	  [?scenario :entity/type :entity.type/scenario]
	  [?scenario :scenario/title ?title]
	  [?scenario :scenario/created-at ?createdAt]
	  (or-default [(q [:find (count ?t) (sum ?tok) (sum ?dur)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]
	                  [(get-else $ ?t :task/token-count 0) ?tok]
	                  [(get-else $ ?t :task/duration 0) ?dur]]
	          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
	      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/key :task/opening]
	                  [?t :task/status :status/complete]]
	          $ ?scenario) [[?openingCount]]]
	      [(ground 0) ?openingCount])
	  [[(> ?openingCount 0)] ?complete]
	  (or-default [(q [:find ?key ?ca
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]
	                  [?t :task/completed-at ?ca]
	                  [?t :task/key ?key]
	                  [(q [:find (max ?ca)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]
	                              [?t :task/completed-at ?ca]]
	                      $ ?s) [[?maxCa]]]
	                  [(= ?ca ?maxCa)]]
	          $ ?scenario) [[?lastKey ?lastUpdatedAt]]]
	      [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
	  :order-by [[?lastUpdatedAt :desc]]]`

	type config struct {
		name        string
		algebra     bool
		scanSharing bool
		prefetch    bool
		decorrelate bool // Selinger decorrelation (only when algebra=false)
	}

	configs := []config{
		{"baseline (no opts)", false, false, false, false},
		{"decorrelation only (Selinger)", false, false, false, true},
		{"algebra only", true, false, false, false},
		{"algebra + sharing", true, true, false, false},
		{"algebra + prefetch", true, false, true, false},
		{"algebra + sharing + prefetch", true, true, true, false},
		{"all on", true, true, true, true},
	}

	// Run each config, collecting timing
	t.Logf("\n%-40s %10s %10s", "Configuration", "Run 1", "Run 2")
	t.Logf("%-40s %10s %10s", "-------------", "-----", "-----")

	for _, cfg := range configs {
		var times [2]time.Duration
		for run := 0; run < 2; run++ {
			db.ClearPlanCache()
			opts := DefaultPlannerOptions()
			opts.EnableAlgebraOptimizer = cfg.algebra
			opts.EnableScanSharing = cfg.scanSharing
			opts.EnableEntityPrefetch = cfg.prefetch
			if cfg.algebra {
				opts.EnableSubqueryDecorrelation = false
			} else {
				opts.EnableSubqueryDecorrelation = cfg.decorrelate
			}

			start := time.Now()
			rel, err := queryWithPlannerOptions(db, queryStr, opts)
			require.NoError(t, err, "config %s failed", cfg.name)
			results, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			times[run] = time.Since(start)

			require.Equal(t, numScenarios, len(results), "config %s: wrong result count", cfg.name)
		}
		t.Logf("%-40s %10s %10s", cfg.name, times[0].Round(time.Millisecond), times[1].Round(time.Millisecond))
	}
}
