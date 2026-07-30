package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// buildComplexNotQuery builds a complex query that exercises the interaction
// between NOT clauses, multiple get-else expressions, correlated OR-with-subquery
// branches, comparison bindings, and order-by. This is the minimal structure
// that triggers the NOT clause scheduling bug (GitHub #58).
//
// Data model: projects have items. Items may be deleted or completed.
// Query: find non-deleted projects with aggregated item stats, optional
// attributes with defaults, and a "latest item" lookup via nested subquery.
func buildComplexNotQuery() *query.Query {
	kw := qb.Kw

	project := qb.NewVar("project")
	name := qb.NewVar("name")
	createdAt := qb.NewVar("createdAt")
	optA := qb.NewVar("optA")
	optB := qb.NewVar("optB")
	optC := qb.NewVar("optC")
	optD := qb.NewVar("optD")
	optE := qb.NewVar("optE")
	optF := qb.NewVar("optF")
	itemCount := qb.NewVar("itemCount")
	totalCost := qb.NewVar("totalCost")
	totalWeight := qb.NewVar("totalWeight")
	totalVolume := qb.NewVar("totalVolume")
	totalUnits := qb.NewVar("totalUnits")
	ready := qb.NewVar("ready")
	lastTag := qb.NewVar("lastTag")
	lastUpdatedAt := qb.NewVar("lastUpdatedAt")
	doneCount := qb.NewVar("doneCount")

	// Item stats subquery: count completed non-deleted items, sum metrics
	i, p := qb.NewVar("i"), qb.NewVar("p")
	cost, weight, vol, units := qb.NewVar("cost"), qb.NewVar("weight"), qb.NewVar("vol"), qb.NewVar("units")
	itemStatsSubquery := qb.Query().
		Find(qb.Count(i), qb.Sum(cost), qb.Sum(weight), qb.Sum(vol), qb.Sum(units)).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
			qb.Not(qb.Pat(i, kw(":item/deleted"), true)),
			qb.GetElse(i, kw(":item/cost"), 0).As(cost),
			qb.GetElse(i, kw(":item/weight"), 0).As(weight),
			qb.GetElse(i, kw(":item/volume"), 0).As(vol),
			qb.GetElse(i, kw(":item/units"), 0).As(units),
		)

	// Done count subquery
	i, p = qb.NewVar("i"), qb.NewVar("p")
	doneCountSubquery := qb.Query().
		Find(qb.Count(i)).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/tag"), kw(":tag/primary")),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
			qb.Not(qb.Pat(i, kw(":item/deleted"), true)),
		)

	// Max updated-at inner subquery
	i, p = qb.NewVar("i"), qb.NewVar("p")
	ts := qb.NewVar("ts")
	maxTsSubquery := qb.Query().
		Find(qb.Max(ts)).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
			qb.Pat(i, kw(":item/updated-at"), ts),
			qb.Not(qb.Pat(i, kw(":item/deleted"), true)),
		)

	// Last item tag subquery (nested subquery for max timestamp)
	i, p = qb.NewVar("i"), qb.NewVar("p")
	ts = qb.NewVar("ts")
	tag, maxTs := qb.NewVar("tag"), qb.NewVar("maxTs")
	lastTagSubquery := qb.Query().
		Find(tag, ts).
		In(qb.DB, qb.Scalar(p)).
		Where(
			qb.Pat(i, kw(":item/project"), p),
			qb.Pat(i, kw(":item/status"), kw(":status/done")),
			qb.Pat(i, kw(":item/updated-at"), ts),
			qb.Pat(i, kw(":item/tag"), tag),
			qb.Not(qb.Pat(i, kw(":item/deleted"), true)),
			qb.Subquery(maxTsSubquery, p).BindTuple(maxTs),
			qb.Eq(ts, maxTs),
		)

	// Main query: pattern + NOT + 6 get-else + 3 OR-with-subquery + comparison + order-by
	return qb.Query().
		Find(project, name, createdAt, optA, optB, optC,
			optD, optE, optF, itemCount, totalCost, totalWeight,
			totalVolume, totalUnits, ready, lastTag, lastUpdatedAt).
		Where(
			qb.Pat(project, kw(":project/type"), kw(":type/active")),
			qb.Not(qb.Pat(project, kw(":project/deleted"), true)),

			qb.GetElse(project, kw(":project/name"), "").As(name),
			qb.Pat(project, kw(":project/created-at"), createdAt),
			qb.GetElse(project, kw(":project/opt-a"), "").As(optA),
			qb.GetElse(project, kw(":project/opt-b"), "").As(optB),
			qb.GetElse(project, kw(":project/opt-c"), "").As(optC),
			qb.GetElse(project, kw(":project/opt-d"), "").As(optD),
			qb.GetElse(project, kw(":project/opt-e"), "").As(optE),
			qb.GetElse(project, kw(":project/opt-f"), "").As(optF),

			qb.OrDefault().
				Branch(qb.Subquery(itemStatsSubquery, project).BindTuple(
					itemCount, totalCost, totalWeight, totalVolume, totalUnits)).
				Branch(qb.TupleGround(0, 0, 0, 0, 0).As(
					itemCount, totalCost, totalWeight, totalVolume, totalUnits)),

			qb.OrDefault().
				Branch(qb.Subquery(doneCountSubquery, project).BindTuple(doneCount)).
				Branch(qb.Ground(0).As(doneCount)),

			qb.Gt(doneCount, 0).As(ready),

			qb.OrDefault().
				Branch(qb.Subquery(lastTagSubquery, project).BindTuple(lastTag, lastUpdatedAt)).
				Branch(qb.TupleGround(datalog.NewKeyword(":none"), time.Time{}).As(lastTag, lastUpdatedAt)),
		).OrderBy(qb.Desc(lastUpdatedAt)).MustBuild()
}

// TestNotClauseComplexQuery_E2E reproduces GitHub issue #58: NOT clause fails
// with "NOT clause variables not found in input relation" in a complex query
// with multiple get-else, OR-with-subquery, comparison, and order-by clauses.
func TestNotClauseComplexQuery_E2E(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			// Tracing only; registered at open because everything the database
			// builds is constructed with it.
			db := createOptimizerModeDB(t, mode, func(event annotations.Event) {
				t.Logf("ANNOTATION: %s %v", event.Name, event.Data)
			})

			tx := db.NewTransaction()

			proj1 := datalog.NewIdentity("proj:1")
			proj2 := datalog.NewIdentity("proj:2")
			proj3 := datalog.NewIdentity("proj:3") // deleted
			item1 := datalog.NewIdentity("item:1")
			item2 := datalog.NewIdentity("item:2")
			item3 := datalog.NewIdentity("item:3")
			now := time.Now().Truncate(time.Second)

			// Project 1: active, has completed items
			require.NoError(t, tx.Add(proj1, datalog.NewKeyword(":project/type"), datalog.NewKeyword(":type/active")))
			require.NoError(t, tx.Add(proj1, datalog.NewKeyword(":project/name"), "Alpha"))
			require.NoError(t, tx.Add(proj1, datalog.NewKeyword(":project/created-at"), now.Add(-24*time.Hour)))

			// Project 2: active, no items
			require.NoError(t, tx.Add(proj2, datalog.NewKeyword(":project/type"), datalog.NewKeyword(":type/active")))
			require.NoError(t, tx.Add(proj2, datalog.NewKeyword(":project/name"), "Beta"))
			require.NoError(t, tx.Add(proj2, datalog.NewKeyword(":project/created-at"), now.Add(-48*time.Hour)))

			// Project 3: deleted
			require.NoError(t, tx.Add(proj3, datalog.NewKeyword(":project/type"), datalog.NewKeyword(":type/active")))
			require.NoError(t, tx.Add(proj3, datalog.NewKeyword(":project/name"), "Gamma"))
			require.NoError(t, tx.Add(proj3, datalog.NewKeyword(":project/created-at"), now.Add(-72*time.Hour)))
			require.NoError(t, tx.Add(proj3, datalog.NewKeyword(":project/deleted"), true))

			// Items for proj1
			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/project"), proj1))
			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/status"), datalog.NewKeyword(":status/done")))
			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/tag"), datalog.NewKeyword(":tag/primary")))
			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/cost"), int64(100)))
			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/weight"), int64(5000)))
			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/updated-at"), now.Add(-1*time.Hour)))

			require.NoError(t, tx.Add(item2, datalog.NewKeyword(":item/project"), proj1))
			require.NoError(t, tx.Add(item2, datalog.NewKeyword(":item/status"), datalog.NewKeyword(":status/done")))
			require.NoError(t, tx.Add(item2, datalog.NewKeyword(":item/tag"), datalog.NewKeyword(":tag/secondary")))
			require.NoError(t, tx.Add(item2, datalog.NewKeyword(":item/cost"), int64(200)))
			require.NoError(t, tx.Add(item2, datalog.NewKeyword(":item/weight"), int64(3000)))
			require.NoError(t, tx.Add(item2, datalog.NewKeyword(":item/updated-at"), now))

			// Deleted item for proj1 — should be excluded by NOT in subqueries
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/project"), proj1))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/status"), datalog.NewKeyword(":status/done")))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/tag"), datalog.NewKeyword(":tag/tertiary")))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/cost"), int64(999)))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/weight"), int64(9999)))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/updated-at"), now.Add(1*time.Hour)))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/deleted"), true))

			_, err := tx.Commit()
			require.NoError(t, err)

			q := buildComplexNotQuery()
			t.Logf("Query: %s", q.String())

			tuples, err := executor.CollectTuples(db.Query(q))
			require.NoError(t, err, "Complex query with NOT clauses should not fail")

			t.Logf("Got %d results", len(tuples))
			for _, tuple := range tuples {
				t.Logf("Result: %v", tuple)
			}

			// Should have 2 projects (proj3 is deleted)
			require.Len(t, tuples, 2, "Deleted project should be excluded")

			// Verify proj3 (deleted) is not in results
			for _, tuple := range tuples {
				projID := tuple[0].(datalog.Identity)
				assert.NotEqual(t, "proj:3", projID.String(),
					"Deleted project should not appear in results")
			}

			// Pin the aggregate positions exactly, as int64 — nil here means an
			// aggregate silently skipped its inputs, and a bare Go int means a
			// builder constant bypassed boundary normalization
			// (BUG_BASELINE_ORDEFAULT_SUBQUERY_NIL_AGGREGATE.md).
			// Find positions: 9 itemCount, 10 totalCost, 11 totalWeight,
			// 12 totalVolume, 13 totalUnits, 14 ready.
			byName := make(map[string]executor.Tuple, len(tuples))
			for _, tuple := range tuples {
				byName[tuple[1].(string)] = tuple
			}
			alpha := byName["Alpha"]
			require.NotNil(t, alpha)
			require.Equal(t, int64(2), alpha[9], "Alpha item count")
			require.Equal(t, int64(300), alpha[10], "Alpha total cost")
			require.Equal(t, int64(8000), alpha[11], "Alpha total weight")
			require.Equal(t, int64(0), alpha[12], "Alpha total volume (get-else default, no :item/volume datoms)")
			require.Equal(t, int64(0), alpha[13], "Alpha total units (get-else default, no :item/units datoms)")
			require.Equal(t, true, alpha[14], "Alpha ready")

			beta := byName["Beta"]
			require.NotNil(t, beta)
			for pos := 9; pos <= 13; pos++ {
				require.Equal(t, int64(0), beta[pos], "Beta fallback tuple position %d", pos)
			}
			require.Equal(t, false, beta[14], "Beta ready")
		})
	}
}

// TestNotClauseWithUnboundInnerVar_E2E tests NOT where the inner pattern
// introduces a new variable not in the outer scope.
// This is the pattern most affected by extractNotClauseSymbols treating
// inner Provides as Requires.
func TestNotClauseWithUnboundInnerVar_E2E(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, nil)

			tx := db.NewTransaction()

			item1 := datalog.NewIdentity("item:1")
			item2 := datalog.NewIdentity("item:2")
			item3 := datalog.NewIdentity("item:3")

			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/name"), "Item 1"))
			require.NoError(t, tx.Add(item2, datalog.NewKeyword(":item/name"), "Item 2"))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/name"), "Item 3"))

			// item1 and item3 have errors (with various error messages)
			require.NoError(t, tx.Add(item1, datalog.NewKeyword(":item/error"), "timeout"))
			require.NoError(t, tx.Add(item3, datalog.NewKeyword(":item/error"), "connection refused"))

			_, err := tx.Commit()
			require.NoError(t, err)

			// Query: find items that have NO errors
			// The NOT inner pattern introduces ?err which doesn't exist in outer scope
			queryStr := `[:find ?name
	              :where [?e :item/name ?name]
	                     (not [?e :item/error ?err])]`

			tuples, err := executor.CollectTuples(db.Query(queryStr))
			require.NoError(t, err, "NOT with inner-only variable should not fail")

			names := make(map[string]bool)
			for _, tuple := range tuples {
				names[tuple[0].(string)] = true
			}

			assert.True(t, names["Item 2"], "Item 2 has no errors, should be included")
			assert.False(t, names["Item 1"], "Item 1 has an error, should be excluded")
			assert.False(t, names["Item 3"], "Item 3 has an error, should be excluded")
			assert.Len(t, names, 1)
		})
	}
}
