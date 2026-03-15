package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/algebra"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// setupAlgebraTestDB creates a database with representative data for
// testing all clause types through the full pipeline.
func setupAlgebraTestDB(t testing.TB) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "algebra-integration-*")
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir,
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "subquery/uncorrelated-cached" || e.Name == "subquery/uncorrelated-cache-hit" || e.Name == "subquery/correlation-check" || e.Name == "or/begin" || e.Name == "or/fallback" || e.Name == "or/union" {
				t.Logf("[%s] %v", e.Name, e.Data)
			}
		},
	})
	require.NoError(t, err)

	tx := db.NewTransaction()

	// Scenarios
	s1 := datalog.NewIdentity("scenario:1")
	s2 := datalog.NewIdentity("scenario:2")
	s3 := datalog.NewIdentity("scenario:3")
	tx.Add(s1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario"))
	tx.Add(s2, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario"))
	tx.Add(s3, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":entity.type/scenario"))
	tx.Add(s1, datalog.NewKeyword(":scenario/title"), "First")
	tx.Add(s2, datalog.NewKeyword(":scenario/title"), "Second")
	tx.Add(s3, datalog.NewKeyword(":scenario/title"), "Third")
	tx.Add(s1, datalog.NewKeyword(":entity/tag"), "alpha")
	tx.Add(s2, datalog.NewKeyword(":entity/tag"), "beta")
	tx.Add(s3, datalog.NewKeyword(":entity/tag"), "gamma")
	// s3 is soft-deleted
	tx.Add(s3, datalog.NewKeyword(":entity/deleted"), true)

	// Tasks for s1 and s2 (s3 has none)
	for i := 0; i < 3; i++ {
		t := datalog.NewIdentity("task:1:" + string(rune('a'+i)))
		tx.Add(t, datalog.NewKeyword(":task/root"), s1)
		tx.Add(t, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
		tx.Add(t, datalog.NewKeyword(":task/token-count"), int64(100+i*10))
	}
	t1 := datalog.NewIdentity("task:2:a")
	tx.Add(t1, datalog.NewKeyword(":task/root"), s2)
	tx.Add(t1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete"))
	tx.Add(t1, datalog.NewKeyword(":task/token-count"), int64(50))

	_, err = tx.Commit()
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// queryWithAlgebra runs a query with the algebra optimizer enabled.
func queryWithAlgebra(db *Database, queryStr string) (executor.Relation, error) {
	opts := DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = true
	return queryWithPlannerOptions(db, queryStr, opts)
}

// queryWithoutAlgebra runs a query with the algebra optimizer disabled.
func queryWithoutAlgebra(db *Database, queryStr string) (executor.Relation, error) {
	opts := DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = false
	return queryWithPlannerOptions(db, queryStr, opts)
}

// TestAlgebraIntegration_SimplePatterns tests that simple data patterns
// produce identical results with and without the algebra optimizer.
func TestAlgebraIntegration_SimplePatterns(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

	q := `[:find ?e ?title :where [?e :entity/type :entity.type/scenario] [?e :scenario/title ?title]]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)

	db.ClearPlanCache()
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err)
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)

	assert.Equal(t, len(baseline), len(optimized), "same result count")
	t.Logf("Baseline: %v", baseline)
	t.Logf("Optimized: %v", optimized)
}

// TestAlgebraIntegration_NotClause tests NOT clauses through the full pipeline.
// This reproduces the production crash on the tags query.
func TestAlgebraIntegration_NotClause(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

	q := `[:find ?e ?tag
	       :where [?e :entity/type :entity.type/scenario]
	              [?e :entity/tag ?tag]
	              (not [?e :entity/deleted true])]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	t.Logf("Baseline: %d results: %v", len(baseline), baseline)

	db.ClearPlanCache()
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err, "algebra optimizer should not crash on NOT clause")
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)
	t.Logf("Optimized: %d results: %v", len(optimized), optimized)

	assert.Equal(t, len(baseline), len(optimized), "same result count")
}

// TestAlgebraIntegration_GetElse tests get-else through the full pipeline.
func TestAlgebraIntegration_GetElse(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

	q := `[:find ?e ?title
	       :where [?e :entity/type :entity.type/scenario]
	              (not [?e :entity/deleted true])
	              [(get-else $ ?e :scenario/title "") ?title]]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	t.Logf("Baseline: %d results: %v", len(baseline), baseline)

	db.ClearPlanCache()
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err, "algebra optimizer should not crash on get-else")
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)
	t.Logf("Optimized: %d results: %v", len(optimized), optimized)

	assert.Equal(t, len(baseline), len(optimized), "same result count")
}

// TestAlgebraIntegration_OrFallbackWithSubquery tests OR-fallback with
// correlated subquery through the full pipeline.
func TestAlgebraIntegration_OrFallbackWithSubquery(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

	q := `[:find ?e ?count
	       :where [?e :entity/type :entity.type/scenario]
	              (not [?e :entity/deleted true])
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?e) [[?count]]]
	                  [(ground 0) ?count])]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	t.Logf("Baseline: %d results: %v", len(baseline), baseline)

	db.ClearPlanCache()
	// Debug: show rewritten clauses
	{
		parsed, _ := db.resolveQuery(q)
		root, _ := algebra.Compile(&query.Query{Where: parsed.Where})
		opt := algebra.NewOptimizer(algebra.DefaultPasses()...)
		optimized, _ := opt.Optimize(root)
		rewritten, _ := algebra.Decompile(optimized)
		t.Logf("Rewritten %d clauses:", len(rewritten))
		for i, c := range rewritten {
			t.Logf("  [%d] %T: %s", i, c, c.String())
		}
	}
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err, "algebra optimizer should handle OR-fallback with subquery")
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)
	t.Logf("Optimized: %d results: %v", len(optimized), optimized)

	assert.Equal(t, len(baseline), len(optimized), "same result count")
}

// TestAlgebraIntegration_SubqueryWithNot tests subqueries that internally
// contain NOT clauses (like the production pattern with entity/deleted).
func TestAlgebraIntegration_SubqueryWithNot(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

	q := `[:find ?e ?count
	       :where [?e :entity/type :entity.type/scenario]
	              (not [?e :entity/deleted true])
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]
	                              (not [?t :entity/deleted true])]
	                      $ ?e) [[?count]]]
	                  [(ground 0) ?count])]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	t.Logf("Baseline: %d results: %v", len(baseline), baseline)

	db.ClearPlanCache()
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err, "subquery with NOT should survive algebra optimizer")
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)
	t.Logf("Optimized: %d results: %v", len(optimized), optimized)

	assert.Equal(t, len(baseline), len(optimized), "same result count")
}

// TestAlgebraIntegration_SequentialQueries tests running multiple queries
// in sequence on the same database (like the production profiler).
func TestAlgebraIntegration_SequentialQueries(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

	queries := []struct {
		name string
		q    string
	}{
		{"entity_counts", "[:find ?type (count ?e) :where [?e :entity/type ?type]]"},
		{"with_not", "[:find ?e ?tag :where [?e :entity/type :entity.type/scenario] [?e :entity/tag ?tag] (not [?e :entity/deleted true])]"},
		{"with_subquery", `[:find ?e ?count
			:where [?e :entity/type :entity.type/scenario]
			       (not [?e :entity/deleted true])
			       (or [(q [:find (count ?t) :in $ ?s
			                :where [?t :task/root ?s] [?t :task/status :status/complete]
			                       (not [?t :entity/deleted true])]
			               $ ?e) [[?count]]]
			           [(ground 0) ?count])]`},
	}

	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			rel, err := db.Query(tc.q)
			require.NoError(t, err, "query %s should not crash", tc.name)
			results, err := executor.CollectTuples(rel, nil)
			require.NoError(t, err)
			t.Logf("%s: %d results", tc.name, len(results))
		})
	}
}

// TestAlgebraIntegration_MultipleOrFallbacks tests multiple OR-fallback
// clauses in one query (the production pattern).
func TestAlgebraIntegration_MultipleOrFallbacks(t *testing.T) {
	db, cleanup := setupAlgebraTestDB(t)
	defer cleanup()

	q := `[:find ?e ?count ?total
	       :where [?e :entity/type :entity.type/scenario]
	              (not [?e :entity/deleted true])
	              (or [(q [:find (count ?t) (sum ?tok)
	                       :in $ ?s
	                       :where [?t :task/root ?s]
	                              [?t :task/status :status/complete]
	                              [(get-else $ ?t :task/token-count 0) ?tok]]
	                      $ ?e) [[?count ?total]]]
	                  [(ground [0 0]) [[?count ?total]]])]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	t.Logf("Baseline: %d results: %v", len(baseline), baseline)

	db.ClearPlanCache()
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err, "algebra optimizer should handle multiple OR-fallbacks")
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)
	t.Logf("Optimized: %d results: %v", len(optimized), optimized)

	assert.Equal(t, len(baseline), len(optimized), "same result count")
}
