package storage

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func optimizationMatrixSchema() *schema.Schema {
	s := schema.NewSchema()
	for _, definition := range []struct {
		attr      string
		valueType datalog.Keyword
	}{
		{":entity/type", schema.TypeKeyword},
		{":scenario/title", schema.TypeString},
		{":scenario/created-at", schema.TypeInstant},
		{":task/root", schema.TypeRef},
		{":task/status", schema.TypeKeyword},
		{":task/completed-at", schema.TypeInstant},
		{":task/token-count", schema.TypeLong},
		{":task/duration", schema.TypeLong},
		{":task/key", schema.TypeKeyword},
	} {
		s.Add(&schema.AttributeDefinition{
			Ident:       datalog.NewKeyword(definition.attr),
			ValueType:   definition.valueType,
			Cardinality: schema.CardinalityOne,
		})
	}
	return s
}

const optimizationMatrixQueryBody = `[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete ?lastKey ?lastUpdatedAt
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
  :order-by [[?lastUpdatedAt :desc]]`

func optimizationMatrixQuery(limit int) string {
	if limit > 0 {
		return fmt.Sprintf("%s :limit %d]", optimizationMatrixQueryBody, limit)
	}
	return optimizationMatrixQueryBody + "]"
}

func populateOptimizationMatrix(tb testing.TB, db *Database, numScenarios, tasksPerScenario int) {
	tb.Helper()
	baseTime := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	statusComplete := datalog.NewKeyword(":status/complete")
	kwOpening := datalog.NewKeyword(":task/opening")

	for scenarioIndex := 0; scenarioIndex < numScenarios; scenarioIndex++ {
		tx := db.NewTransaction()
		scenario := datalog.NewIdentity(fmt.Sprintf("scenario:%d", scenarioIndex))
		require.NoError(tb, tx.Add(scenario, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario")))
		require.NoError(tb, tx.Add(scenario, datalog.NewKeyword(":scenario/title"), fmt.Sprintf("Scenario %d", scenarioIndex)))
		require.NoError(tb, tx.Add(scenario, datalog.NewKeyword(":scenario/created-at"), baseTime.Add(time.Duration(scenarioIndex)*time.Hour)))

		for taskIndex := 0; taskIndex < tasksPerScenario; taskIndex++ {
			task := datalog.NewIdentity(fmt.Sprintf("task:%d:%d", scenarioIndex, taskIndex))
			require.NoError(tb, tx.Add(task, datalog.NewKeyword(":task/root"), scenario))
			require.NoError(tb, tx.Add(task, datalog.NewKeyword(":task/status"), statusComplete))
			require.NoError(tb, tx.Add(task, datalog.NewKeyword(":task/completed-at"), baseTime.Add(time.Duration(scenarioIndex)*time.Hour+time.Duration(taskIndex)*time.Minute)))
			require.NoError(tb, tx.Add(task, datalog.NewKeyword(":task/token-count"), int64(500+taskIndex*10)))
			require.NoError(tb, tx.Add(task, datalog.NewKeyword(":task/duration"), int64(time.Duration(30+taskIndex)*time.Second)))
			if taskIndex == 0 {
				require.NoError(tb, tx.Add(task, datalog.NewKeyword(":task/key"), kwOpening))
			} else {
				require.NoError(tb, tx.Add(task, datalog.NewKeyword(":task/key"), datalog.NewKeyword(fmt.Sprintf(":scenario/task-%d", taskIndex))))
			}
		}
		_, err := tx.Commit()
		require.NoError(tb, err)
	}
}

// TestOptimizationMatrix runs the production-structure query with all
// combinations of optimizer flags to measure their individual and combined
// impact. Uses 75 scenarios × 100 tasks (7,500 tasks total).
func TestOptimizationMatrix(t *testing.T) {
	const (
		numScenarios     = 75
		tasksPerScenario = 100
	)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:   t.TempDir(),
		Schema: optimizationMatrixSchema(),
	})
	require.NoError(t, err)
	defer db.Close()

	populateOptimizationMatrix(t, db, numScenarios, tasksPerScenario)
	t.Logf("Populated %d scenarios × %d tasks = %d tasks", numScenarios, tasksPerScenario, numScenarios*tasksPerScenario)

	queryStr := optimizationMatrixQuery(0)

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

			start := time.Now()
			rel, err := db.queryUnderPlannerOptions(opts, queryStr)
			require.NoError(t, err, "config %s failed", cfg.name)
			results, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			times[run] = time.Since(start)

			require.Equal(t, numScenarios, len(results), "config %s: wrong result count", cfg.name)
		}
		t.Logf("%-40s %10s %10s", cfg.name, times[0].Round(time.Millisecond), times[1].Round(time.Millisecond))
	}
}

// TestComplexQueryRetainsScenarioKeyThroughFallbacks pins the algebra
// path's plan structure: the relation keys asserted below come from the
// optimizer's or-fallback property derivation (or/properties.derived).
// The baseline path derives no such keys and compensates with conservative
// deduplication — correct tuples, no key metadata — so the assertions cannot
// hold there; per docs/wip/OPTIMIZER_MODE_MATRIX.md this test declares its
// mode explicitly. Cross-mode result equivalence for the same query body
// is covered by TestOptimizationMatrix.
func TestComplexQueryRetainsScenarioKeyThroughFallbacks(t *testing.T) {
	popts := DefaultPlannerOptions()
	popts.EnableAlgebraOptimizer = true
	var propertyEvents []annotations.Event
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		Schema:         optimizationMatrixSchema(),
		PlannerOptions: &popts,
		AnnotationHandler: func(event annotations.Event) {
			if event.Name == annotations.OrPropertiesDerived {
				propertyEvents = append(propertyEvents, event)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

	populateOptimizationMatrix(t, db, 3, 2)
	base, err := db.Query(`[:find ?scenario ?title ?createdAt
		:where
		[?scenario :entity/type :entity.type/scenario]
		[?scenario :scenario/title ?title]
		[?scenario :scenario/created-at ?createdAt]]`)
	require.NoError(t, err)
	scenario := datalog.NewSymbol("?scenario")
	baseHasScenarioKey := false
	for _, key := range base.Properties().Keys {
		if len(key) == 1 && key[0] == scenario {
			baseHasScenarioKey = true
			break
		}
	}
	require.True(t, baseHasScenarioKey, "the scenario key must exist before fallback clauses")

	fallbackStages := []struct {
		name  string
		query string
	}{
		{
			name: "aggregate tuple fallback",
			query: `[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration
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
					[(ground [0 0 0]) [[?taskCount ?totalTokens ?totalDuration]]])]`,
		},
		{
			name: "scalar fallback and expression",
			query: `[:find ?scenario ?title ?createdAt ?taskCount ?totalTokens ?totalDuration ?complete
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
				[[(> ?openingCount 0)] ?complete]]`,
		},
	}
	for _, stage := range fallbackStages {
		stageResult, err := db.Query(stage.query)
		require.NoError(t, err, stage.name)
		found := false
		for _, key := range stageResult.Properties().Keys {
			if len(key) == 1 && key[0] == scenario {
				found = true
				break
			}
		}
		require.True(t, found, "%s must preserve the scenario key; keys=%v events=%v",
			stage.name, stageResult.Properties().Keys, propertyEvents)
	}

	result, err := db.Query(optimizationMatrixQuery(0))
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 3)

	lastKey := datalog.NewSymbol("?lastKey")
	lastUpdatedAt := datalog.NewSymbol("?lastUpdatedAt")
	foundComposite := false
	for _, key := range result.Properties().Keys {
		if len(key) != 3 {
			continue
		}
		members := map[query.Symbol]bool{}
		for _, symbol := range key {
			members[symbol] = true
		}
		if members[scenario] && members[lastKey] && members[lastUpdatedAt] {
			foundComposite = true
			break
		}
	}
	require.True(t, foundComposite,
		"the multi-tuple argmax fallback must retain its proven composite key; keys=%v",
		result.Properties().Keys)
}

// BenchmarkComplexQueryCheckpoint measures the default production path through
// a query that exercises phase planning, joins, same-entity attribute bundles,
// correlated and nested subqueries, conditional aggregation, get-else,
// or-default, expressions, ordering, and bounded Top-N finalization.
func BenchmarkComplexQueryCheckpoint(b *testing.B) {
	benchmarkComplexQueryCheckpoint(b, nil)
}

// TestComplexQuerySubqueryExecutionCounts pins the algebra path's plan
// structure. Every counter asserted below is an optimizer artifact: one
// global execution per subquery body (4 total), or-fallback cache builds,
// fused constraints, replaced outer groups, narrowed materializations. The
// baseline path executes correlated subqueries once per outer combination
// by definition (10 scenarios × 4 subquery bodies = 40 executions), so the
// counts cannot hold there; per docs/wip/OPTIMIZER_MODE_MATRIX.md this test
// declares its mode explicitly. Cross-mode result equivalence for the same
// query body is covered by TestOptimizationMatrix.
func TestComplexQuerySubqueryExecutionCounts(t *testing.T) {
	popts := DefaultPlannerOptions()
	popts.EnableAlgebraOptimizer = true
	var subqueryExecutions atomic.Int64
	var fallbackCacheBuilds atomic.Int64
	var fusedConstraints atomic.Int64
	var uniqueJoinBuilds atomic.Int64
	var replacedOuterGroups atomic.Int64
	var narrowedOuterMaterializations atomic.Int64
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		Schema:         optimizationMatrixSchema(),
		PlannerOptions: &popts,
		AnnotationHandler: func(event annotations.Event) {
			switch event.Name {
			case "subquery/executor-path":
				subqueryExecutions.Add(1)
			case "or-fallback/cache-build":
				fallbackCacheBuilds.Add(1)
			case annotations.PatternFusedConstraint:
				fusedConstraints.Add(1)
			case annotations.JoinStrategy:
				if unique, _ := event.Data["build_key_unique"].(bool); unique {
					uniqueJoinBuilds.Add(1)
				}
			case "or/outer-replaced":
				replacedOuterGroups.Add(1)
			case "or-fallback/outer.materialized":
				narrowedOuterMaterializations.Add(1)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

	populateOptimizationMatrix(t, db, 10, 20)
	result, err := db.Query(optimizationMatrixQuery(10))
	require.NoError(t, err)
	tuples, err := executor.CollectTuples(result, nil)
	require.NoError(t, err)
	require.Len(t, tuples, 10)
	require.Equal(t, int64(4), subqueryExecutions.Load())
	require.Equal(t, int64(5), fallbackCacheBuilds.Load())
	require.Equal(t, int64(5), fusedConstraints.Load())
	require.Zero(t, uniqueJoinBuilds.Load())
	require.Equal(t, int64(5), replacedOuterGroups.Load())
	require.Equal(t, int64(1), narrowedOuterMaterializations.Load())
}

// TestComplexQueryCheckpointCacheSteadyState observes the EA cache's rebuild
// traffic on the complex checkpoint after warmup, via cache/rebuild events.
// A warm read-only steady state is expected to rebuild nothing; this reports
// any rebuild population by attribute and reason.
func TestComplexQueryCheckpointCacheSteadyState(t *testing.T) {
	const (
		numScenarios     = 75
		tasksPerScenario = 100
		resultLimit      = 25
	)
	var recording atomic.Bool
	// The attribute arrives interned rather than rendered, so grouping by it
	// compares pointers.
	type rebuildKey struct {
		attribute datalog.Keyword
		reason    string
	}
	counts := map[rebuildKey]int{}
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:   t.TempDir(),
		Schema: optimizationMatrixSchema(),
		AnnotationHandler: func(event annotations.Event) {
			if event.Name != annotations.CacheRebuild || !recording.Load() {
				return
			}
			counts[rebuildKey{
				attribute: event.Data[annotations.KeyAttribute].(datalog.Keyword),
				reason:    event.Data["reason"].(string),
			}]++
		},
	})
	require.NoError(t, err)
	defer db.Close()

	populateOptimizationMatrix(t, db, numScenarios, tasksPerScenario)
	queryText := optimizationMatrixQuery(resultLimit)

	// Warm parse, plan, storage, and entity-attribute caches.
	warm, err := db.Query(queryText)
	require.NoError(t, err)
	_, err = executor.CollectTuples(warm, nil)
	require.NoError(t, err)

	recording.Store(true)
	result, err := db.Query(queryText)
	require.NoError(t, err)
	_, err = executor.CollectTuples(result, nil)
	require.NoError(t, err)
	recording.Store(false)

	total := 0
	for key, n := range counts {
		total += n
		t.Logf("cache/rebuild attribute=%s reason=%s count=%d", key.attribute, key.reason, n)
	}
	require.Zero(t, total,
		"a warm read-only checkpoint must not rebuild any cache entry: every rebuild is a storage scan plus a transaction, and the freshness model says a warmed key stays fresh until a write touches it")
}

func BenchmarkComplexQueryJoinMaterialization(b *testing.B) {
	b.Run("materialized", func(b *testing.B) {
		options := DefaultPlannerOptions()
		options.EnableStreamingJoins = false
		benchmarkComplexQueryCheckpoint(b, &options)
	})
	b.Run("streaming", func(b *testing.B) {
		options := DefaultPlannerOptions()
		options.EnableStreamingJoins = true
		benchmarkComplexQueryCheckpoint(b, &options)
	})
}

// BenchmarkComplexQueryBackends runs the checkpoint query on every backend.
// Separate from BenchmarkComplexQueryCheckpoint, whose name and shape are the
// baseline several analyses in docs/perf compare against.
func BenchmarkComplexQueryBackends(b *testing.B) {
	for _, backend := range storeContractCases() {
		b.Run(backend.name, func(b *testing.B) {
			runComplexQueryCheckpoint(b, openContractDatabase(b, backend, DatabaseOptions{
				Schema: optimizationMatrixSchema(),
			}))
		})
	}
}

func benchmarkComplexQueryCheckpoint(b *testing.B, options *planner.PlannerOptions) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           b.TempDir(),
		Schema:         optimizationMatrixSchema(),
		PlannerOptions: options,
	})
	require.NoError(b, err)
	defer db.Close()

	runComplexQueryCheckpoint(b, db)
}

func runComplexQueryCheckpoint(b *testing.B, db *Database) {
	const (
		numScenarios     = 75
		tasksPerScenario = 100
		resultLimit      = 25
	)

	populateOptimizationMatrix(b, db, numScenarios, tasksPerScenario)
	queryText := optimizationMatrixQuery(resultLimit)

	// Warm parse, plan, storage, and entity-attribute caches. This checkpoint is
	// steady-state query execution through the public API.
	warm, err := db.Query(queryText)
	require.NoError(b, err)
	warmTuples, err := executor.CollectTuples(warm, nil)
	require.NoError(b, err)
	require.Len(b, warmTuples, resultLimit)
	for i := 1; i < len(warmTuples); i++ {
		previous := warmTuples[i-1][8].(time.Time)
		current := warmTuples[i][8].(time.Time)
		require.False(b, previous.Before(current), "results must be ordered by lastUpdatedAt descending")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := db.Query(queryText)
		if err != nil {
			b.Fatal(err)
		}
		tuples, err := executor.CollectTuples(result, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(tuples) != resultLimit {
			b.Fatalf("got %d tuples, want %d", len(tuples), resultLimit)
		}
	}
}
