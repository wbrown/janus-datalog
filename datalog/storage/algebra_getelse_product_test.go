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
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// setupGetElseTestDB creates a database with projects that have optional
// attributes. Some attributes are present, some are missing (should default).
func setupGetElseTestDB(t testing.TB) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "getelse-product-*")
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)

	tx := db.NewTransaction()

	p1 := datalog.NewIdentity("proj:1")
	p2 := datalog.NewIdentity("proj:2")
	p3 := datalog.NewIdentity("proj:3")

	for _, p := range []datalog.Identity{p1, p2, p3} {
		require.NoError(t, tx.Add(p, datalog.NewKeyword(":project/type"), datalog.NewKeyword(":type/active")))
	}

	// p1 has all optional attributes
	require.NoError(t, tx.Add(p1, datalog.NewKeyword(":project/name"), "Alpha"))
	require.NoError(t, tx.Add(p1, datalog.NewKeyword(":project/opt-a"), "a1"))
	require.NoError(t, tx.Add(p1, datalog.NewKeyword(":project/opt-b"), "b1"))
	require.NoError(t, tx.Add(p1, datalog.NewKeyword(":project/opt-c"), "c1"))

	// p2 has some optional attributes
	require.NoError(t, tx.Add(p2, datalog.NewKeyword(":project/name"), "Beta"))
	require.NoError(t, tx.Add(p2, datalog.NewKeyword(":project/opt-a"), "a2"))

	// p3 has only name
	require.NoError(t, tx.Add(p3, datalog.NewKeyword(":project/name"), "Gamma"))

	_, err = tx.Commit()
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// setupGetElseWithItemsDB creates a database with projects and items for
// testing get-else + OR-with-subquery combinations.
func setupGetElseWithItemsDB(t testing.TB) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "getelse-items-*")
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)

	tx := db.NewTransaction()

	p1 := datalog.NewIdentity("proj:1")
	p2 := datalog.NewIdentity("proj:2")
	i1 := datalog.NewIdentity("item:1")
	i2 := datalog.NewIdentity("item:2")
	now := time.Now().Truncate(time.Second)

	// Two projects
	require.NoError(t, tx.Add(p1, datalog.NewKeyword(":project/type"), datalog.NewKeyword(":type/active")))
	require.NoError(t, tx.Add(p1, datalog.NewKeyword(":project/created-at"), now.Add(-24*time.Hour)))
	require.NoError(t, tx.Add(p2, datalog.NewKeyword(":project/type"), datalog.NewKeyword(":type/active")))
	require.NoError(t, tx.Add(p2, datalog.NewKeyword(":project/created-at"), now.Add(-48*time.Hour)))

	// Items for p1 only (p2 has none → should use fallback defaults)
	require.NoError(t, tx.Add(i1, datalog.NewKeyword(":item/project"), p1))
	require.NoError(t, tx.Add(i1, datalog.NewKeyword(":item/status"), datalog.NewKeyword(":status/done")))
	require.NoError(t, tx.Add(i1, datalog.NewKeyword(":item/tag"), datalog.NewKeyword(":tag/primary")))
	require.NoError(t, tx.Add(i1, datalog.NewKeyword(":item/cost"), int64(100)))
	require.NoError(t, tx.Add(i1, datalog.NewKeyword(":item/updated-at"), now.Add(-1*time.Hour)))
	require.NoError(t, tx.Add(i2, datalog.NewKeyword(":item/project"), p1))
	require.NoError(t, tx.Add(i2, datalog.NewKeyword(":item/status"), datalog.NewKeyword(":status/done")))
	require.NoError(t, tx.Add(i2, datalog.NewKeyword(":item/tag"), datalog.NewKeyword(":tag/secondary")))
	require.NoError(t, tx.Add(i2, datalog.NewKeyword(":item/cost"), int64(200)))
	require.NoError(t, tx.Add(i2, datalog.NewKeyword(":item/updated-at"), now))

	_, err = tx.Commit()
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// TestGetElseMultiple_NoCartesianProduct tests that multiple get-else
// expressions on the same entity produce one row per entity, not a
// Cartesian product.
func TestGetElseMultiple_NoCartesianProduct(t *testing.T) {
	db, cleanup := setupGetElseTestDB(t)
	defer cleanup()

	q := `[:find ?p ?name ?a ?b ?c
	       :where [?p :project/type :type/active]
	              [(get-else $ ?p :project/name "") ?name]
	              [(get-else $ ?p :project/opt-a "") ?a]
	              [(get-else $ ?p :project/opt-b "") ?b]
	              [(get-else $ ?p :project/opt-c "") ?c]]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	require.Len(t, baseline, 3, "baseline: one row per project")

	db.ClearPlanCache()
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err)
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)
	assert.Len(t, optimized, 3,
		"algebra bridge: must produce one row per project, not a Cartesian product")
}

// TestGetElseMultiple_CorrectValues verifies that get-else returns the
// stored value when present and the default when absent, through the
// algebra bridge path.
func TestGetElseMultiple_CorrectValues(t *testing.T) {
	db, cleanup := setupGetElseTestDB(t)
	defer cleanup()

	q := `[:find ?name ?a ?b ?c
	       :where [?p :project/type :type/active]
	              [(get-else $ ?p :project/name "") ?name]
	              [(get-else $ ?p :project/opt-a "default-a") ?a]
	              [(get-else $ ?p :project/opt-b "default-b") ?b]
	              [(get-else $ ?p :project/opt-c "default-c") ?c]]`

	db.ClearPlanCache()
	baselineRel, err := queryWithoutAlgebra(db, q)
	require.NoError(t, err)
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)

	baselineByName := make(map[string]executor.Tuple)
	for _, tuple := range baseline {
		baselineByName[tuple[0].(string)] = tuple
	}

	assert.Equal(t, "a1", baselineByName["Alpha"][1])
	assert.Equal(t, "b1", baselineByName["Alpha"][2])
	assert.Equal(t, "c1", baselineByName["Alpha"][3])
	assert.Equal(t, "a2", baselineByName["Beta"][1])
	assert.Equal(t, "default-b", baselineByName["Beta"][2])
	assert.Equal(t, "default-c", baselineByName["Beta"][3])
	assert.Equal(t, "default-a", baselineByName["Gamma"][1])
	assert.Equal(t, "default-b", baselineByName["Gamma"][2])
	assert.Equal(t, "default-c", baselineByName["Gamma"][3])

	db.ClearPlanCache()
	optimizedRel, err := queryWithAlgebra(db, q)
	require.NoError(t, err)
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)

	optimizedByName := make(map[string]executor.Tuple)
	for _, tuple := range optimized {
		optimizedByName[tuple[0].(string)] = tuple
	}

	assert.Len(t, optimized, len(baseline), "same row count")
	for name, expected := range baselineByName {
		actual, ok := optimizedByName[name]
		require.True(t, ok, "missing result for %s", name)
		assert.Equal(t, expected, actual, "values must match for %s", name)
	}
}

// TestGetElsePipelineInvariant verifies the single-relation pipeline invariant
// via annotations: after each clause, there should be exactly 1 relation group.
func TestGetElsePipelineInvariant(t *testing.T) {
	db, cleanup := setupGetElseTestDB(t)
	defer cleanup()

	q := `[:find ?p ?name ?a ?b
	       :where [?p :project/type :type/active]
	              [(get-else $ ?p :project/name "") ?name]
	              [(get-else $ ?p :project/opt-a "") ?a]
	              [(get-else $ ?p :project/opt-b "") ?b]]`

	maxGroups := 0
	replacements := 0
	db.SetAnnotationHandler(func(event annotations.Event) {
		switch event.Name {
		case "collapse/success":
			if after, ok := event.Data["relations.after"]; ok {
				if n, ok := after.(int); ok && n > maxGroups {
					maxGroups = n
				}
			}
		case "or/outer-replaced":
			replacements++
			if remaining, ok := event.Data["remaining_groups"].(int); ok && remaining > maxGroups {
				maxGroups = remaining
			}
		}
	})
	defer db.SetAnnotationHandler(nil)

	db.ClearPlanCache()
	rel, err := queryWithAlgebra(db, q)
	require.NoError(t, err)
	_, err = executor.CollectTuples(rel, nil)
	require.NoError(t, err)

	require.Equal(t, 3, replacements,
		"each get-else rewrite must replace its consumed outer relation")
	assert.Equal(t, 1, maxGroups,
		"pipeline invariant: should never have more than 1 relation group after collapse; "+
			"got %d (disjoint groups = Cartesian product risk)", maxGroups)
}

// =============================================================================
// 2×2 matrix: (or / or-default) × (parsed string / qb builder)
//
// Tests the full complex structure: pattern + 6 get-else + pattern +
// 3 OR-with-subquery + comparison binding + order-by.
// Each test expects 2 rows (one per project), not a Cartesian product.
// =============================================================================

// buildComplexQuery_OrClause builds the complex query using qb.Or()
// which produces *query.OrClause (union semantics).
func buildComplexQuery_OrClause(t *testing.T) *query.Query {
	t.Helper()
	return buildComplexQueryWithOrType(t, false)
}

// buildComplexQuery_OrDefaultClause builds the complex query using qb.OrDefault()
// which produces *query.OrDefaultClause (fallback semantics).
func buildComplexQuery_OrDefaultClause(t *testing.T) *query.Query {
	t.Helper()
	return buildComplexQueryWithOrType(t, true)
}

func buildComplexQueryWithOrType(t *testing.T, useOrDefault bool) *query.Query {
	t.Helper()
	kw := qb.Kw

	project := qb.NewVar("project")
	createdAt := qb.NewVar("createdAt")
	optA := qb.NewVar("optA")
	optB := qb.NewVar("optB")
	optC := qb.NewVar("optC")
	optD := qb.NewVar("optD")
	optE := qb.NewVar("optE")
	optF := qb.NewVar("optF")
	itemCount := qb.NewVar("itemCount")
	totalCost := qb.NewVar("totalCost")
	doneCount := qb.NewVar("doneCount")
	ready := qb.NewVar("ready")
	lastTag := qb.NewVar("lastTag")
	lastUpdatedAt := qb.NewVar("lastUpdatedAt")

	// Item count subquery
	i, p := qb.NewVar("i"), qb.NewVar("p")
	cost := qb.NewVar("cost")
	countSubq := qb.Query().
		Find(qb.Count(i), qb.Sum(cost)).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
			qb.GetElse(i, kw(":item/cost"), 0).As(cost),
		)

	// Done count subquery
	i, p = qb.NewVar("i"), qb.NewVar("p")
	doneSubq := qb.Query().
		Find(qb.Count(i)).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/tag"), kw(":tag/primary")),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
		)

	// Max updated-at subquery
	i, p = qb.NewVar("i"), qb.NewVar("p")
	ts := qb.NewVar("ts")
	maxTsSubq := qb.Query().
		Find(qb.Max(ts)).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
			qb.Pat(i, kw(":item/updated-at"), ts),
		)

	// Last tag subquery (nested)
	i, p = qb.NewVar("i"), qb.NewVar("p")
	ts = qb.NewVar("ts")
	tag, maxTs := qb.NewVar("tag"), qb.NewVar("maxTs")
	lastTagSubq := qb.Query().
		Find(tag, ts).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
			qb.Pat(i, kw(":item/updated-at"), ts),
			qb.Pat(i, kw(":item/tag"), tag),
			qb.Subquery(maxTsSubq, p).BindTuple(maxTs),
			qb.Eq(ts, maxTs),
		)

	// Build OR clauses using the selected type
	var or1, or2, or3 interface{}
	if useOrDefault {
		or1 = qb.OrDefault().
			Branch(qb.Subquery(countSubq, project).BindTuple(itemCount, totalCost)).
			Branch(qb.TupleGround(0, 0).As(itemCount, totalCost))
		or2 = qb.OrDefault().
			Branch(qb.Subquery(doneSubq, project).BindTuple(doneCount)).
			Branch(qb.Ground(0).As(doneCount))
		or3 = qb.OrDefault().
			Branch(qb.Subquery(lastTagSubq, project).BindTuple(lastTag, lastUpdatedAt)).
			Branch(qb.TupleGround(datalog.NewKeyword(":none"), 0).As(lastTag, lastUpdatedAt))
	} else {
		or1 = qb.Or().
			Branch(qb.Subquery(countSubq, project).BindTuple(itemCount, totalCost)).
			Branch(qb.TupleGround(0, 0).As(itemCount, totalCost))
		or2 = qb.Or().
			Branch(qb.Subquery(doneSubq, project).BindTuple(doneCount)).
			Branch(qb.Ground(0).As(doneCount))
		or3 = qb.Or().
			Branch(qb.Subquery(lastTagSubq, project).BindTuple(lastTag, lastUpdatedAt)).
			Branch(qb.TupleGround(datalog.NewKeyword(":none"), 0).As(lastTag, lastUpdatedAt))
	}

	return qb.Query().
		Find(project, createdAt, optA, optB, optC, optD, optE, optF,
			itemCount, totalCost, ready, lastTag, lastUpdatedAt).
		Where(
			qb.Pat(project, kw(":project/type"), kw(":type/active")),
			qb.GetElse(project, kw(":project/opt-a"), "").As(optA),
			qb.Pat(project, kw(":project/created-at"), createdAt),
			qb.GetElse(project, kw(":project/opt-b"), "").As(optB),
			qb.GetElse(project, kw(":project/opt-c"), "").As(optC),
			qb.GetElse(project, kw(":project/opt-d"), "").As(optD),
			qb.GetElse(project, kw(":project/opt-e"), "").As(optE),
			qb.GetElse(project, kw(":project/opt-f"), "").As(optF),
			or1, or2,
			qb.Gt(doneCount, 0).As(ready),
			or3,
		).OrderBy(qb.Desc(lastUpdatedAt)).MustBuild()
}

// parsedComplexQuery_Or returns the complex query as an EDN string using (or ...).
const parsedComplexQuery_Or = `
[:find ?project ?createdAt ?optA ?optB ?optC ?optD ?optE ?optF
       ?itemCount ?totalCost ?ready ?lastTag ?lastUpdatedAt
 :where
 [?project :project/type :type/active]
 [(get-else $ ?project :project/opt-a "") ?optA]
 [?project :project/created-at ?createdAt]
 [(get-else $ ?project :project/opt-b "") ?optB]
 [(get-else $ ?project :project/opt-c "") ?optC]
 [(get-else $ ?project :project/opt-d "") ?optD]
 [(get-else $ ?project :project/opt-e "") ?optE]
 [(get-else $ ?project :project/opt-f "") ?optF]
 (or [(q [:find (count ?i) (sum ?cost)
          :in $ ?p
          :where [?i :item/project ?p]
                 [?i :item/status :status/done]
                 [(get-else $ ?i :item/cost 0) ?cost]]
         $ ?project) [[?itemCount ?totalCost]]]
     [(ground [0 0]) [[?itemCount ?totalCost]]])
 (or [(q [:find (count ?i)
          :in $ ?p
          :where [?i :item/project ?p]
                 [?i :item/tag :tag/primary]
                 [?i :item/status :status/done]]
         $ ?project) [[?doneCount]]]
     [(ground 0) ?doneCount])
 [[(> ?doneCount 0)] ?ready]
 (or [(q [:find ?tag ?ts
          :in $ ?p
          :where [?i :item/project ?p]
                 [?i :item/status :status/done]
                 [?i :item/updated-at ?ts]
                 [?i :item/tag ?tag]
                 [(q [:find (max ?ts2)
                      :in $ ?p2
                      :where [?i2 :item/project ?p2]
                             [?i2 :item/status :status/done]
                             [?i2 :item/updated-at ?ts2]]
                    $ ?p) [[?maxTs]]]
                 [(= ?ts ?maxTs)]]
         $ ?project) [[?lastTag ?lastUpdatedAt]]]
     [(ground [:none #inst "0001-01-01T00:00:00Z"]) [[?lastTag ?lastUpdatedAt]]])
 :order-by [[?lastUpdatedAt :desc]]]`

// parsedComplexQuery_OrDefault returns the complex query using (or-default ...).
const parsedComplexQuery_OrDefault = `
[:find ?project ?createdAt ?optA ?optB ?optC ?optD ?optE ?optF
       ?itemCount ?totalCost ?ready ?lastTag ?lastUpdatedAt
 :where
 [?project :project/type :type/active]
 [(get-else $ ?project :project/opt-a "") ?optA]
 [?project :project/created-at ?createdAt]
 [(get-else $ ?project :project/opt-b "") ?optB]
 [(get-else $ ?project :project/opt-c "") ?optC]
 [(get-else $ ?project :project/opt-d "") ?optD]
 [(get-else $ ?project :project/opt-e "") ?optE]
 [(get-else $ ?project :project/opt-f "") ?optF]
 (or-default [(q [:find (count ?i) (sum ?cost)
          :in $ ?p
          :where [?i :item/project ?p]
                 [?i :item/status :status/done]
                 [(get-else $ ?i :item/cost 0) ?cost]]
         $ ?project) [[?itemCount ?totalCost]]]
     [(ground [0 0]) [[?itemCount ?totalCost]]])
 (or-default [(q [:find (count ?i)
          :in $ ?p
          :where [?i :item/project ?p]
                 [?i :item/tag :tag/primary]
                 [?i :item/status :status/done]]
         $ ?project) [[?doneCount]]]
     [(ground 0) ?doneCount])
 [[(> ?doneCount 0)] ?ready]
 (or-default [(q [:find ?tag ?ts
          :in $ ?p
          :where [?i :item/project ?p]
                 [?i :item/status :status/done]
                 [?i :item/updated-at ?ts]
                 [?i :item/tag ?tag]
                 [(q [:find (max ?ts2)
                      :in $ ?p2
                      :where [?i2 :item/project ?p2]
                             [?i2 :item/status :status/done]
                             [?i2 :item/updated-at ?ts2]]
                    $ ?p) [[?maxTs]]]
                 [(= ?ts ?maxTs)]]
         $ ?project) [[?lastTag ?lastUpdatedAt]]]
     [(ground [:none #inst "0001-01-01T00:00:00Z"]) [[?lastTag ?lastUpdatedAt]]])
 :order-by [[?lastUpdatedAt :desc]]]`

func runComplexQueryTest(t *testing.T, db *Database, q interface{}, label string, expectedRows int) {
	t.Helper()

	tuples, err := executor.CollectTuples(db.Query(q))
	require.NoError(t, err, "%s: query should not error", label)

	t.Logf("%s: got %d results", label, len(tuples))
	for _, tuple := range tuples {
		t.Logf("  %v", tuple)
	}

	assert.Len(t, tuples, expectedRows, "%s: expected %d rows", label, expectedRows)
}

// TestGetElseComplex_OrSemantics compares the (or ...) query result with and
// without the algebra bridge to verify they agree. Whatever the base executor
// produces is the correct semantics; the algebra bridge must match.
func TestGetElseComplex_OrSemantics(t *testing.T) {
	db, cleanup := setupGetElseWithItemsDB(t)
	defer cleanup()

	q := buildComplexQuery_OrClause(t)

	// Print original clauses
	t.Logf("BEFORE algebra bridge (%d clauses):", len(q.Where))
	for i, c := range q.Where {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Print algebra bridge output
	bridged, err := algebra.Compile(&query.Query{Where: q.Where})
	require.NoError(t, err)
	t.Logf("Compiled tree:\n%s", bridged.String())
	opt := algebra.NewOptimizer(algebra.DefaultPasses()...)
	optimizedTree, err := opt.Optimize(bridged)
	require.NoError(t, err)
	t.Logf("Optimized tree:\n%s", optimizedTree.String())
	decompiled, err := algebra.Decompile(optimizedTree)
	require.NoError(t, err)
	t.Logf("AFTER algebra bridge (%d clauses):", len(decompiled))
	for i, c := range decompiled {
		t.Logf("  [%d] %T: %s", i, c, c.String())
	}

	// Without algebra bridge
	db.ClearPlanCache()
	opts := DefaultPlannerOptions()
	opts.EnableAlgebraOptimizer = false
	router := executor.NewSourceRouter(buildSourceMap(nil, db.Matcher()))
	exec := executor.NewExecutorWithOptions(router, db, opts)
	baselineRel, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, nil)
	require.NoError(t, err, "baseline should not error")
	baseline, err := executor.CollectTuples(baselineRel, nil)
	require.NoError(t, err)
	t.Logf("Without algebra: %d results", len(baseline))
	for _, tuple := range baseline {
		t.Logf("  %v", tuple)
	}

	// With algebra bridge
	db.ClearPlanCache()
	opts2 := DefaultPlannerOptions()
	opts2.EnableAlgebraOptimizer = true
	exec2 := executor.NewExecutorWithOptions(router, db, opts2)
	optimizedRel, err := exec2.ExecuteWithRelations(executor.NewContext(nil), q, nil)
	require.NoError(t, err, "optimized should not error")
	optimized, err := executor.CollectTuples(optimizedRel, nil)
	require.NoError(t, err)
	t.Logf("With algebra: %d results", len(optimized))
	for _, tuple := range optimized {
		t.Logf("  %v", tuple)
	}

	assert.Equal(t, len(baseline), len(optimized),
		"algebra bridge must produce same row count as base executor")
}

// TestGetElseComplex_ParsedOr tests with parsed (or ...) — union semantics.
// Both branches contribute rows when both match. Verified empirically:
// base executor without algebra bridge also produces 9 rows
// (TestGetElseComplex_OrSemantics). This is correct union behavior, not a bug.
// Use (or-default ...) for fallback semantics (2 rows).
func TestGetElseComplex_ParsedOr(t *testing.T) {
	db, cleanup := setupGetElseWithItemsDB(t)
	defer cleanup()
	runComplexQueryTest(t, db, parsedComplexQuery_Or, "parsed (or)", 9)
}

// TestGetElseComplex_ParsedOrDefault tests with parsed (or-default ...).
// Fallback semantics: one row per project.
func TestGetElseComplex_ParsedOrDefault(t *testing.T) {
	db, cleanup := setupGetElseWithItemsDB(t)
	defer cleanup()
	runComplexQueryTest(t, db, parsedComplexQuery_OrDefault, "parsed (or-default)", 2)
}

// TestGetElseComplex_QBOr tests with qb.Or() → *query.OrClause.
// Same union semantics as parsed (or ...): 9 rows.
func TestGetElseComplex_QBOr(t *testing.T) {
	db, cleanup := setupGetElseWithItemsDB(t)
	defer cleanup()
	q := buildComplexQuery_OrClause(t)
	t.Logf("Query: %s", q.String())
	runComplexQueryTest(t, db, q, "qb.Or()", 9)
}

// TestGetElseComplex_QBOrDefault tests with qb.OrDefault() → *query.OrDefaultClause.
// Fallback semantics: 2 rows.
func TestGetElseComplex_QBOrDefault(t *testing.T) {
	db, cleanup := setupGetElseWithItemsDB(t)
	defer cleanup()
	q := buildComplexQuery_OrDefaultClause(t)
	t.Logf("Query: %s", q.String())
	runComplexQueryTest(t, db, q, "qb.OrDefault()", 2)
}

// TestGetElseComplex_StructuralComparison compares the clause types produced
// by qb.OrDefault() vs parsed (or-default ...) to find why one passes and
// the other fails.
func TestGetElseComplex_StructuralComparison(t *testing.T) {
	// QB path
	qbQuery := buildComplexQuery_OrDefaultClause(t)
	t.Logf("QB clause types:")
	for i, clause := range qbQuery.Where {
		t.Logf("  [%d] %T: %s", i, clause, clause.String())
	}

	// Parsed path
	parsed, err := parser.ParseQuery(parsedComplexQuery_OrDefault)
	_ = fmt.Sprintf // suppress unused import
	require.NoError(t, err)
	t.Logf("Parsed clause types:")
	for i, clause := range parsed.Where {
		t.Logf("  [%d] %T: %s", i, clause, clause.String())
	}

	// Compare clause counts
	assert.Equal(t, len(qbQuery.Where), len(parsed.Where),
		"clause count should match")

	// Compare each clause type
	for i := 0; i < len(qbQuery.Where) && i < len(parsed.Where); i++ {
		qbType := fmt.Sprintf("%T", qbQuery.Where[i])
		parsedType := fmt.Sprintf("%T", parsed.Where[i])
		if qbType != parsedType {
			t.Errorf("clause %d type mismatch: qb=%s parsed=%s", i, qbType, parsedType)
			t.Logf("  qb:     %s", qbQuery.Where[i].String())
			t.Logf("  parsed: %s", parsed.Where[i].String())
		}
	}
}
