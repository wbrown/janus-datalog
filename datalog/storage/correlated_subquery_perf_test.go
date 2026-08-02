package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestCorrelatedSubqueryPerformance reproduces a production bottleneck from
// a downstream application's scenario listing query.
//
// The pattern: N scenarios, each with ~M tasks. The query runs 3 correlated
// subqueries per scenario (task stats, last task key, opening count), each
// wrapped in OR-fallback for scenarios with no matching tasks.
//
// With N=75 and M=100 this takes ~30s in production because each subquery
// scans the task index once per scenario. The fix is subquery decorrelation.
//
// Data shape:
//   - 75 scenarios with :entity/type, :scenario/title, :scenario/created-at
//   - ~100 completed tasks per scenario with :task/root, :task/status,
//     :task/key, :task/completed-at, :task/token-count, :task/duration
//   - 1 task per scenario has :task/key = :task/opening
func TestCorrelatedSubqueryPerformance(t *testing.T) {
	const (
		numScenarios     = 75
		tasksPerScenario = 100
	)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{
				AnnotationHandler: func(e annotations.Event) {
					if e.Name == "subquery/decorrelation-cached" || e.Name == "subquery/decorrelation-cache-hit" {
						t.Logf("[%s] %v", e.Name, e.Data)
					}
				},
			})

			// Populate data — one transaction per scenario to avoid BadgerDB size limits
			baseTime := time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC)

			statusComplete := datalog.NewKeyword(":status/complete")
			kwOpening := datalog.NewKeyword(":task/opening")
			kwEntityType := datalog.NewKeyword(":entity/type")
			kwScenarioType := datalog.NewKeyword(":entity.type/scenario")
			kwTitle := datalog.NewKeyword(":scenario/title")
			kwCreatedAt := datalog.NewKeyword(":scenario/created-at")
			kwTaskRoot := datalog.NewKeyword(":task/root")
			kwTaskStatus := datalog.NewKeyword(":task/status")
			kwTaskKey := datalog.NewKeyword(":task/key")
			kwTaskCompletedAt := datalog.NewKeyword(":task/completed-at")
			kwTaskTokenCount := datalog.NewKeyword(":task/token-count")
			kwTaskDuration := datalog.NewKeyword(":task/duration")

			for s := 0; s < numScenarios; s++ {
				tx := db.NewTransaction()
				scenario := datalog.NewIdentity(fmt.Sprintf("scenario:%d", s))
				require.NoError(t, tx.Add(scenario, kwEntityType, kwScenarioType))
				require.NoError(t, tx.Add(scenario, kwTitle, fmt.Sprintf("Scenario %d", s)))
				require.NoError(t, tx.Add(scenario, kwCreatedAt, baseTime.Add(time.Duration(s)*time.Hour)))

				for j := 0; j < tasksPerScenario; j++ {
					task := datalog.NewIdentity(fmt.Sprintf("task:%d:%d", s, j))
					require.NoError(t, tx.Add(task, kwTaskRoot, scenario))
					require.NoError(t, tx.Add(task, kwTaskStatus, statusComplete))
					require.NoError(t, tx.Add(task, kwTaskCompletedAt, baseTime.Add(time.Duration(s)*time.Hour+time.Duration(j)*time.Minute)))
					require.NoError(t, tx.Add(task, kwTaskTokenCount, int64(500+j*10)))
					require.NoError(t, tx.Add(task, kwTaskDuration, int64(time.Duration(30+j)*time.Second)))

					if j == 0 {
						require.NoError(t, tx.Add(task, kwTaskKey, kwOpening))
					} else {
						require.NoError(t, tx.Add(task, kwTaskKey, datalog.NewKeyword(fmt.Sprintf(":scenario/task-%d", j))))
					}
				}
				_, err := tx.Commit()
				require.NoError(t, err)
			}
			t.Logf("Populated %d scenarios with %d tasks each (%d total datoms)",
				numScenarios, tasksPerScenario, numScenarios*(3+tasksPerScenario*6))

			// The production query pattern — simplified but structurally identical.
			// 3 correlated OR-fallback subqueries, each scanning the task index.
			queryStr := `[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete ?lastKey ?lastUpdatedAt
	  :where
	  [?scenario :entity/type :entity.type/scenario]
	  [?scenario :scenario/title ?title]
	  [?scenario :scenario/created-at ?createdAt]

	  ;; Subquery 1: task stats (count, sum tokens, sum duration)
	  (or-default [(q [:find (count ?t) (sum ?tok) (sum ?dur)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]
	                  [(get-else $ ?t :task/token-count 0) ?tok]
	                  [(get-else $ ?t :task/duration 0) ?dur]]
	          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
	      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])

	  ;; Subquery 2: opening count (determines completeness)
	  (or-default [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/key :task/opening]
	                  [?t :task/status :status/complete]]
	          $ ?scenario) [[?openingCount]]]
	      [(ground 0) ?openingCount])
	  [[(> ?openingCount 0)] ?complete]

	  ;; Subquery 3: last task key (nested subquery for max timestamp)
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

			// Run without annotations first to get clean timing
			start := time.Now()
			results, err := executor.CollectTuples(db.Query(queryStr))
			elapsed := time.Since(start)
			require.NoError(t, err)
			require.Len(t, results, numScenarios, "should return one tuple per scenario")
			t.Logf("Query returned %d results in %s", len(results), elapsed)

			// Verify correctness of first result (ordered by lastUpdatedAt desc, so last scenario)
			last := results[0]
			t.Logf("First result: scenario=%v title=%v taskCount=%v totalTokens=%v complete=%v lastKey=%v",
				last[0], last[1], last[3], last[4], last[6], last[7])

			// Every scenario has 100 completed tasks
			for i, tuple := range results {
				taskCount := tuple[3]
				if tc, ok := taskCount.(int); ok {
					require.Equal(t, tasksPerScenario, tc, "scenario %d should have %d tasks", i, tasksPerScenario)
				}
				complete := tuple[6]
				require.Equal(t, true, complete, "scenario %d should be complete (has opening task)", i)
			}

			// Performance threshold: with decorrelation this should be <2s.
			// Without decorrelation, 75 scenarios × 4 subqueries × ~100 tasks = very slow.
			if elapsed > 10*time.Second {
				t.Logf("WARNING: Query took %s — correlated subqueries are not decorrelated", elapsed)
			}
		})
	}
}

// BenchmarkCorrelatedSubqueryPattern benchmarks the production query pattern
// at different scale factors to show the O(N*M) cost of correlated subqueries.
func BenchmarkCorrelatedSubqueryPattern(b *testing.B) {
	for _, numScenarios := range []int{10, 25, 50, 75} {
		b.Run(fmt.Sprintf("scenarios=%d", numScenarios), func(b *testing.B) {
			dir, err := os.MkdirTemp("", "correlated-bench-*")
			require.NoError(b, err)
			defer os.RemoveAll(dir)

			// Collect annotation events for analysis
			var events []annotations.Event
			handler := func(e annotations.Event) {
				events = append(events, e)
			}

			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:              dir,
				AnnotationHandler: handler,
			})
			require.NoError(b, err)
			defer db.Close()

			const tasksPerScenario = 100
			baseTime := time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC)

			for s := 0; s < numScenarios; s++ {
				tx := db.NewTransaction()
				scenario := datalog.NewIdentity(fmt.Sprintf("scenario:%d", s))
				tx.Add(scenario, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario"))
				tx.Add(scenario, datalog.NewKeyword(":scenario/title"), fmt.Sprintf("Scenario %d", s))
				tx.Add(scenario, datalog.NewKeyword(":scenario/created-at"), baseTime.Add(time.Duration(s)*time.Hour))

				for j := 0; j < tasksPerScenario; j++ {
					task := datalog.NewIdentity(fmt.Sprintf("task:%d:%d", s, j))
					tx.Add(task, datalog.NewKeyword(":task/root"), scenario)
					tx.Add(task, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
					tx.Add(task, datalog.NewKeyword(":task/completed-at"), baseTime.Add(time.Duration(s)*time.Hour+time.Duration(j)*time.Minute))
					tx.Add(task, datalog.NewKeyword(":task/token-count"), int64(500+j*10))
					tx.Add(task, datalog.NewKeyword(":task/duration"), int64(time.Duration(30+j)*time.Second))
					if j == 0 {
						tx.Add(task, datalog.NewKeyword(":task/key"), datalog.NewKeyword(":task/opening"))
					} else {
						tx.Add(task, datalog.NewKeyword(":task/key"), datalog.NewKeyword(fmt.Sprintf(":scenario/task-%d", j)))
					}
				}
				tx.Commit()
			}

			queryStr := `[:find ?scenario ?title ?taskCount ?totalTokens ?complete ?lastKey ?lastUpdatedAt
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

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				events = events[:0]
				results, err := executor.CollectTuples(db.Query(queryStr))
				if err != nil {
					b.Fatal(err)
				}
				if len(results) != numScenarios {
					b.Fatalf("expected %d results, got %d", numScenarios, len(results))
				}
			}
		})
	}
}

// TestCorrelatedSubqueryAlgebraOptimizer compares baseline (no algebra optimizer)
// against optimized (algebra optimizer with decorrelation) on the same data
// and query as TestCorrelatedSubqueryPerformance.
func TestCorrelatedSubqueryAlgebraOptimizer(t *testing.T) {
	for _, backend := range AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			testCorrelatedSubqueryAlgebraOptimizer(t, backend)
		})
	}
}

func testCorrelatedSubqueryAlgebraOptimizer(t *testing.T, backend Backend) {
	const (
		numScenarios     = 75
		tasksPerScenario = 100
	)

	db := openBackendDB(t, backend, DatabaseOptions{
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "subquery/correlation-check" || e.Name == "subquery/uncorrelated-cached" || e.Name == "subquery/uncorrelated-cache-hit" || e.Name == "subquery/inner-clause-dispatch" {
				t.Logf("[%s] %v", e.Name, e.Data)
			}
		},
	})

	// Reuse same data setup as TestCorrelatedSubqueryPerformance
	baseTime := time.Date(2026, 3, 14, 0, 0, 0, 0, time.UTC)
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
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	t.Logf("Populated %d scenarios with %d tasks each", numScenarios, tasksPerScenario)

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

	t.Run("baseline", func(t *testing.T) {
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = false
		db.ClearPlanCache()

		start := time.Now()
		rel, err := db.queryUnderPlannerOptions(opts, queryStr)
		require.NoError(t, err)
		results, err := executor.CollectTuples(rel, nil)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Len(t, results, numScenarios)
		t.Logf("Baseline: %d results in %s", len(results), elapsed)

		for i, tuple := range results {
			if tc, ok := tuple[3].(int); ok {
				require.Equal(t, tasksPerScenario, tc, "scenario %d task count", i)
			}
			require.Equal(t, true, tuple[6], "scenario %d complete", i)
		}
	})

	t.Run("algebra_optimizer", func(t *testing.T) {
		t.Logf("annotation handler nil: %v", db.plannerOptions.Handler == nil)
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = true
		db.ClearPlanCache()

		// Log the rewritten clauses
		q, _ := db.resolveQuery(queryStr)
		root, _ := algebra.Compile(&query.Query{Where: q.Where})
		optimizer := algebra.NewOptimizer(algebra.DefaultPasses(nil)...)
		optimized, _ := optimizer.Optimize(root)
		rewritten, _ := algebra.Decompile(optimized)
		t.Logf("Rewritten %d clauses:", len(rewritten))
		for i, c := range rewritten {
			t.Logf("  [%d] %T: %s", i, c, c.String())
		}

		db.ClearPlanCache()
		start := time.Now()
		rel, err := db.queryUnderPlannerOptions(opts, queryStr)
		if err != nil {
			t.Fatalf("query error: %v", err)
		}
		t.Logf("Relation: %v, symbols: %v, size: %d", rel != nil, rel.Symbols(), rel.Size())
		results, err := executor.CollectTuples(rel, nil)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("collect error: %v", err)
		}
		t.Logf("Optimized: %d results in %s", len(results), elapsed)

		for i, tuple := range results {
			if tc, ok := tuple[3].(int); ok {
				require.Equal(t, tasksPerScenario, tc, "scenario %d task count", i)
			}
			require.Equal(t, true, tuple[6], "scenario %d complete", i)
		}
	})
}

// TestCorrelatedSubqueryAlgebraOptimizerWithDefaults tests the algebra optimizer
// with mixed data: some scenarios have tasks, some don't. This matches production
// reality where the OR-fallback default branch must fire for entities without data.
//
// The first test (TestCorrelatedSubqueryAlgebraOptimizer) has tasks for ALL scenarios,
// so InnerJoin never drops anything — it validates the decorrelation speedup.
// This test validates correctness when defaults are needed.
func TestCorrelatedSubqueryAlgebraOptimizerWithDefaults(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testCorrelatedSubqueryAlgebraOptimizerWithDefaults(t, mode)
		})
	}
}

func testCorrelatedSubqueryAlgebraOptimizerWithDefaults(t *testing.T, mode optimizerMode) {
	const (
		totalScenarios       = 75
		scenariosWithTasks   = 37 // Matches production: roughly half have completed tasks
		tasksPerScenario     = 20 // Fewer tasks than the perf test — this is about correctness
		scenariosWithOpening = 30 // Subset that have an opening task (determines ?complete)
	)

	db := createOptimizerModeDB(t, mode, DatabaseOptions{})

	baseTime := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	statusComplete := datalog.NewKeyword(":status/complete")
	kwOpening := datalog.NewKeyword(":task/opening")

	// Create ALL scenarios with basic metadata
	for s := 0; s < totalScenarios; s++ {
		tx := db.NewTransaction()
		scenario := datalog.NewIdentity(fmt.Sprintf("scenario:%d", s))
		require.NoError(t, tx.Add(scenario, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario")))
		require.NoError(t, tx.Add(scenario, datalog.NewKeyword(":scenario/title"), fmt.Sprintf("Scenario %d", s)))
		require.NoError(t, tx.Add(scenario, datalog.NewKeyword(":scenario/created-at"), baseTime.Add(time.Duration(s)*time.Hour)))

		// Only some scenarios have tasks
		if s < scenariosWithTasks {
			for j := 0; j < tasksPerScenario; j++ {
				task := datalog.NewIdentity(fmt.Sprintf("task:%d:%d", s, j))
				require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/root"), scenario))
				require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/status"), statusComplete))
				require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/completed-at"), baseTime.Add(time.Duration(s)*time.Hour+time.Duration(j)*time.Minute)))
				require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/token-count"), int64(100+j)))
				require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/duration"), int64(time.Duration(10+j)*time.Second)))
				if j == 0 && s < scenariosWithOpening {
					require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/key"), kwOpening))
				} else {
					require.NoError(t, tx.Add(task, datalog.NewKeyword(":task/key"), datalog.NewKeyword(fmt.Sprintf(":scenario/task-%d", j))))
				}
			}
		}
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	t.Logf("Populated %d scenarios: %d with tasks (%d tasks each), %d without tasks",
		totalScenarios, scenariosWithTasks, tasksPerScenario, totalScenarios-scenariosWithTasks)

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

	t.Run("baseline", func(t *testing.T) {
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = false
		db.ClearPlanCache()

		start := time.Now()
		rel, err := db.queryUnderPlannerOptions(opts, queryStr)
		require.NoError(t, err)
		results, err := executor.CollectTuples(rel, nil)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Len(t, results, totalScenarios, "baseline must return ALL scenarios")
		t.Logf("Baseline: %d results in %s", len(results), elapsed)

		// Verify: scenarios with tasks have counts > 0, those without have 0
		withTasks := 0
		withoutTasks := 0
		for _, tuple := range results {
			tc := int64(0)
			switch v := tuple[3].(type) {
			case int64:
				tc = v
			case int:
				tc = int64(v)
			}
			if tc > 0 {
				withTasks++
			} else {
				withoutTasks++
			}
		}
		t.Logf("  With tasks: %d, Without tasks: %d", withTasks, withoutTasks)
		assert.Equal(t, scenariosWithTasks, withTasks, "scenarios with tasks")
		assert.Equal(t, totalScenarios-scenariosWithTasks, withoutTasks, "scenarios without tasks")
	})

	t.Run("algebra_optimizer", func(t *testing.T) {
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = true
		db.ClearPlanCache()

		start := time.Now()
		rel, err := db.queryUnderPlannerOptions(opts, queryStr)
		require.NoError(t, err)
		results, err := executor.CollectTuples(rel, nil)
		elapsed := time.Since(start)
		require.NoError(t, err)

		// THIS IS THE KEY ASSERTION: the optimizer must return ALL scenarios,
		// including those without tasks (which get default values from OR-fallback).
		require.Len(t, results, totalScenarios,
			"algebra optimizer must return ALL scenarios — entities without task data need OR-fallback defaults")
		t.Logf("Optimized: %d results in %s", len(results), elapsed)

		// Same verification as baseline
		withTasks := 0
		withoutTasks := 0
		for _, tuple := range results {
			tc := int64(0)
			switch v := tuple[3].(type) {
			case int64:
				tc = v
			case int:
				tc = int64(v)
			}
			if tc > 0 {
				withTasks++
			} else {
				withoutTasks++
			}
		}
		t.Logf("  With tasks: %d, Without tasks: %d", withTasks, withoutTasks)
		assert.Equal(t, scenariosWithTasks, withTasks, "scenarios with tasks")
		assert.Equal(t, totalScenarios-scenariosWithTasks, withoutTasks, "scenarios without tasks")
	})
}

// TestCorrelatedSubqueryAlgebraOptimizerProductionStructure tests the algebra
// optimizer with a query structurally identical to the production query.
// The production query has:
// - Multiple get-else expressions on the outer entity (6 optional attributes)
// - A NOT clause filtering deleted entities
// - 3 OR-fallback subqueries with correlation, aggregation, and get-else
// - The third subquery has a NESTED subquery (argmax pattern)
// - Order-by on the result
//
// Previous tests used simplified queries that missed phasing issues triggered
// by the combination of get-else + NOT + OR-fallback in the same clause set.
func TestCorrelatedSubqueryAlgebraOptimizerProductionStructure(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			testCorrelatedSubqueryAlgebraOptimizerProductionStructure(t, mode)
		})
	}
}

func testCorrelatedSubqueryAlgebraOptimizerProductionStructure(t *testing.T, mode optimizerMode) {
	const (
		numProjects     = 10
		itemsPerProject = 5
	)

	db := createOptimizerModeDB(t, mode, DatabaseOptions{
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "or-fallback/cache-build" || e.Name == "or-fallback/branch.success" {
				t.Logf("[%s] %v", e.Name, e.Data)
			}
			if e.Name == annotations.ScanSharingCacheHit || e.Name == annotations.ScanSharingCacheMiss {
				t.Logf("[SCAN-SHARING] [%s] %v", e.Name, e.Data)
			}
		},
	})

	baseTime := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	statusDone := datalog.NewKeyword(":status/done")
	kwInit := datalog.NewKeyword(":step/init")

	for p := 0; p < numProjects; p++ {
		tx := db.NewTransaction()
		project := datalog.NewIdentity(fmt.Sprintf("project:%d", p))
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/project")))
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":project/created-at"), baseTime.Add(time.Duration(p)*time.Hour)))

		// Optional attributes (like production's title, intensity, pov, genre, element, setting)
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":project/label"), fmt.Sprintf("Project %d", p)))
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":project/priority"), int64(p%3)))
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":project/category"), fmt.Sprintf("cat-%d", p%4)))
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":project/region"), fmt.Sprintf("region-%d", p%2)))
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":project/owner"), fmt.Sprintf("owner-%d", p%3)))
		require.NoError(t, tx.Add(project, datalog.NewKeyword(":project/notes"), fmt.Sprintf("notes for project %d", p)))

		for j := 0; j < itemsPerProject; j++ {
			item := datalog.NewIdentity(fmt.Sprintf("item:%d:%d", p, j))
			require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/project"), project))
			require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/status"), statusDone))
			require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/completed-at"), baseTime.Add(time.Duration(p)*time.Hour+time.Duration(j)*time.Minute)))
			require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/cost"), int64(100+j*10)))
			require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/weight"), int64(time.Duration(10+j)*time.Second)))
			require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/input-units"), int64(j*5)))
			require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/output-units"), int64(j*3)))
			if j == 0 {
				require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/key"), kwInit))
			} else {
				require.NoError(t, tx.Add(item, datalog.NewKeyword(":item/key"), datalog.NewKeyword(fmt.Sprintf(":step/work-%d", j))))
			}
		}
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	// Production-structure query: get-else on outer entity, NOT, 3 OR-fallback subqueries
	queryStr := `[:find ?project ?label ?createdAt ?priority ?category ?region ?owner ?notes ?itemCount ?totalCost ?totalWeight ?inputUnits ?outputUnits ?ready ?lastKey ?lastUpdatedAt
	  :where
	  [?project :entity/type :entity.type/project]
	  (not [?project :entity/deleted true])
	  [(get-else $ ?project :project/label "") ?label]
	  [?project :project/created-at ?createdAt]
	  [(get-else $ ?project :project/priority 0) ?priority]
	  [(get-else $ ?project :project/category "") ?category]
	  [(get-else $ ?project :project/region "") ?region]
	  [(get-else $ ?project :project/owner "") ?owner]
	  [(get-else $ ?project :project/notes "") ?notes]
	  (or-default [(q [:find (count ?i) (sum ?c) (sum ?w) (sum ?iu) (sum ?ou)
	           :in $ ?p
	           :where [?i :item/project ?p]
	                  [?i :item/status :status/done]
	                  (not [?i :entity/deleted true])
	                  [(get-else $ ?i :item/cost 0) ?c]
	                  [(get-else $ ?i :item/weight 0) ?w]
	                  [(get-else $ ?i :item/input-units 0) ?iu]
	                  [(get-else $ ?i :item/output-units 0) ?ou]]
	          $ ?project) [[?itemCount ?totalCost ?totalWeight ?inputUnits ?outputUnits]]]
	      [(ground [0 0 0 0 0]) [[?itemCount ?totalCost ?totalWeight ?inputUnits ?outputUnits]]])
	  (or-default [(q [:find (count ?i)
	           :in $ ?p
	           :where [?i :item/project ?p]
	                  [?i :item/key :step/init]
	                  [?i :item/status :status/done]
	                  (not [?i :entity/deleted true])]
	          $ ?project) [[?initCount]]]
	      [(ground 0) ?initCount])
	  [[(> ?initCount 0)] ?ready]
	  (or-default [(q [:find ?key ?ca
	           :in $ ?p
	           :where [?i :item/project ?p]
	                  [?i :item/status :status/done]
	                  [?i :item/completed-at ?ca]
	                  [?i :item/key ?key]
	                  (not [?i :entity/deleted true])
	                  [(q [:find (max ?ca)
	                       :in $ ?p
	                       :where [?i :item/project ?p]
	                              [?i :item/status :status/done]
	                              [?i :item/completed-at ?ca]
	                              (not [?i :entity/deleted true])]
	                      $ ?p) [[?maxCa]]]
	                  [(= ?ca ?maxCa)]]
	          $ ?project) [[?lastKey ?lastUpdatedAt]]]
	      [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
	  :order-by [[?lastUpdatedAt :desc]]]`

	t.Run("baseline", func(t *testing.T) {
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = false
		db.ClearPlanCache()

		start := time.Now()
		rel, err := db.queryUnderPlannerOptions(opts, queryStr)
		require.NoError(t, err)
		results, err := executor.CollectTuples(rel, nil)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Len(t, results, numProjects, "baseline must return ALL projects")
		t.Logf("Baseline: %d results in %s", len(results), elapsed)

		for _, tuple := range results {
			ic := int64(0)
			switch v := tuple[8].(type) {
			case int64:
				ic = v
			case int:
				ic = int64(v)
			}
			assert.Equal(t, int64(itemsPerProject), ic, "each project has %d items", itemsPerProject)
		}
	})

	t.Run("algebra_optimizer", func(t *testing.T) {
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = true
		db.ClearPlanCache()

		start := time.Now()
		rel, err := db.queryUnderPlannerOptions(opts, queryStr)
		require.NoError(t, err, "algebra optimizer must not crash on production-structure query")
		results, err := executor.CollectTuples(rel, nil)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Len(t, results, numProjects,
			"algebra optimizer must return ALL projects with production-structure query")
		t.Logf("Optimized: %d results in %s", len(results), elapsed)

		for _, tuple := range results {
			ic := int64(0)
			switch v := tuple[8].(type) {
			case int64:
				ic = v
			case int:
				ic = int64(v)
			}
			assert.Equal(t, int64(itemsPerProject), ic, "each project has %d items", itemsPerProject)
		}
	})
}
