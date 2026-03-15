package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/planner"
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
	dir, err := os.MkdirTemp("", "correlated-subquery-perf-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const (
		numScenarios    = 75
		tasksPerScenario = 100
	)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir,
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "subquery/decorrelation-cached" || e.Name == "subquery/decorrelation-cache-hit" {
				t.Logf("[%s] %v", e.Name, e.Data)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

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
		_, err = tx.Commit()
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
	  (or [(q [:find (count ?t) (sum ?tok) (sum ?dur)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]
	                  [(get-else $ ?t :task/token-count 0) ?tok]
	                  [(get-else $ ?t :task/duration 0) ?dur]]
	          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
	      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])

	  ;; Subquery 2: opening count (determines completeness)
	  (or [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/key :task/opening]
	                  [?t :task/status :status/complete]]
	          $ ?scenario) [[?openingCount]]]
	      [(ground 0) ?openingCount])
	  [[(> ?openingCount 0)] ?complete]

	  ;; Subquery 3: last task key (nested subquery for max timestamp)
	  (or [(q [:find ?key ?ca
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
	require.Len(t, results, numScenarios, "should return one row per scenario")
	t.Logf("Query returned %d results in %s", len(results), elapsed)

	// Verify correctness of first result (ordered by lastUpdatedAt desc, so last scenario)
	last := results[0]
	t.Logf("First result: scenario=%v title=%v taskCount=%v totalTokens=%v complete=%v lastKey=%v",
		last[0], last[1], last[3], last[4], last[6], last[7])

	// Every scenario has 100 completed tasks
	for i, row := range results {
		taskCount := row[3]
		if tc, ok := taskCount.(int); ok {
			require.Equal(t, tasksPerScenario, tc, "scenario %d should have %d tasks", i, tasksPerScenario)
		}
		complete := row[6]
		require.Equal(t, true, complete, "scenario %d should be complete (has opening task)", i)
	}

	// Performance threshold: with decorrelation this should be <2s.
	// Without decorrelation, 75 scenarios × 4 subqueries × ~100 tasks = very slow.
	if elapsed > 10*time.Second {
		t.Logf("WARNING: Query took %s — correlated subqueries are not decorrelated", elapsed)
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
			  (or [(q [:find (count ?t) (sum ?tok) (sum ?dur)
			           :in $ ?s
			           :where [?t :task/root ?s]
			                  [?t :task/status :status/complete]
			                  [(get-else $ ?t :task/token-count 0) ?tok]
			                  [(get-else $ ?t :task/duration 0) ?dur]]
			          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
			      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])
			  (or [(q [:find (count ?t)
			           :in $ ?s
			           :where [?t :task/root ?s]
			                  [?t :task/key :task/opening]
			                  [?t :task/status :status/complete]]
			          $ ?scenario) [[?openingCount]]]
			      [(ground 0) ?openingCount])
			  [[(> ?openingCount 0)] ?complete]
			  (or [(q [:find ?key ?ca
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

// queryWithPlannerOptions runs a query with custom planner options.
// Used to test with/without the algebra optimizer.
func queryWithPlannerOptions(db *Database, queryStr string, opts planner.PlannerOptions) (executor.Relation, error) {
	q, err := db.resolveQuery(queryStr)
	if err != nil {
		return nil, err
	}
	router := executor.NewSourceRouter(buildSourceMap(nil, db.Matcher()))
	inputs, err := db.convertInputsToRelations(q, nil)
	if err != nil {
		return nil, err
	}
	opts.Cache = db.planCache
	exec := executor.NewExecutorWithOptions(router, db, opts)
	return exec.ExecuteWithRelations(executor.NewContext(db.AnnotationHandler()), q, inputs)
}

// TestCorrelatedSubqueryAlgebraOptimizer compares baseline (no algebra optimizer)
// against optimized (algebra optimizer with decorrelation) on the same data
// and query as TestCorrelatedSubqueryPerformance.
func TestCorrelatedSubqueryAlgebraOptimizer(t *testing.T) {
	dir, err := os.MkdirTemp("", "correlated-subquery-algebra-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	const (
		numScenarios     = 75
		tasksPerScenario = 100
	)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir,
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "subquery/correlation-check" || e.Name == "subquery/uncorrelated-cached" || e.Name == "subquery/uncorrelated-cache-hit" || e.Name == "subquery/inner-clause-dispatch" {
				t.Logf("[%s] %v", e.Name, e.Data)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

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
		_, err = tx.Commit()
		require.NoError(t, err)
	}
	t.Logf("Populated %d scenarios with %d tasks each", numScenarios, tasksPerScenario)

	queryStr := `[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete ?lastKey ?lastUpdatedAt
	  :where
	  [?scenario :entity/type :entity.type/scenario]
	  [?scenario :scenario/title ?title]
	  [?scenario :scenario/created-at ?createdAt]
	  (or [(q [:find (count ?t) (sum ?tok) (sum ?dur)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/status :status/complete]
	                  [(get-else $ ?t :task/token-count 0) ?tok]
	                  [(get-else $ ?t :task/duration 0) ?dur]]
	          $ ?scenario) [[?taskCount ?totalTokens ?totalDuration]]]
	      [(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])
	  (or [(q [:find (count ?t)
	           :in $ ?s
	           :where [?t :task/root ?s]
	                  [?t :task/key :task/opening]
	                  [?t :task/status :status/complete]]
	          $ ?scenario) [[?openingCount]]]
	      [(ground 0) ?openingCount])
	  [[(> ?openingCount 0)] ?complete]
	  (or [(q [:find ?key ?ca
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
		rel, err := queryWithPlannerOptions(db, queryStr, opts)
		require.NoError(t, err)
		results, err := executor.CollectTuples(rel, nil)
		elapsed := time.Since(start)
		require.NoError(t, err)
		require.Len(t, results, numScenarios)
		t.Logf("Baseline: %d results in %s", len(results), elapsed)

		for i, row := range results {
			if tc, ok := row[3].(int); ok {
				require.Equal(t, tasksPerScenario, tc, "scenario %d task count", i)
			}
			require.Equal(t, true, row[6], "scenario %d complete", i)
		}
	})

	t.Run("algebra_optimizer", func(t *testing.T) {
		t.Logf("annotation handler nil: %v", db.AnnotationHandler() == nil)
		opts := DefaultPlannerOptions()
		opts.EnableAlgebraOptimizer = true
		opts.EnableSubqueryDecorrelation = false // Algebra optimizer handles decorrelation
		db.ClearPlanCache()

		// Log the rewritten clauses
		q, _ := db.resolveQuery(queryStr)
		root, _ := algebra.Compile(&query.Query{Where: q.Where})
		optimizer := algebra.NewOptimizer(algebra.DefaultPasses()...)
		optimized, _ := optimizer.Optimize(root)
		rewritten, _ := algebra.Decompile(optimized)
		t.Logf("Rewritten %d clauses:", len(rewritten))
		for i, c := range rewritten {
			t.Logf("  [%d] %T: %s", i, c, c.String())
		}

		db.ClearPlanCache()
		start := time.Now()
		rel, err := queryWithPlannerOptions(db, queryStr, opts)
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

		for i, row := range results {
			if tc, ok := row[3].(int); ok {
				require.Equal(t, tasksPerScenario, tc, "scenario %d task count", i)
			}
			require.Equal(t, true, row[6], "scenario %d complete", i)
		}
	})
}
