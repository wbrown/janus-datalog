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

// TestOrUnionWithExpressionBranches verifies that OR-union semantics work
// correctly when branches contain expressions (ground, subqueries, NOT).
//
// Currently, BranchHasExpressions forces fallback semantics whenever any
// branch contains an expression. This prevents using OR-union for the
// left-outer-join pattern:
//
//	(or-join [?key ?value]
//	  [?key :attr ?value]                            ;; branch 1: has value
//	  (and (not [?key :attr _]) [(ground 0) ?value]) ;; branch 2: default
//	)
//
// Both branches should run independently and merge (union). Fallback mode
// tries branch 1 per-tuple and short-circuits, never reaching branch 2.
func TestOrUnionWithExpressionBranches(t *testing.T) {
	dir, err := os.MkdirTemp("", "or-union-expr-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir,
		AnnotationHandler: func(e annotations.Event) {
			if e.Name == "or/union" || e.Name == "or/branch.complete" || e.Name == "or/complete" {
				fmt.Printf("  [ANNOTATION] %s: %v\n", e.Name, e.Data)
			}
		},
	})
	require.NoError(t, err)
	defer db.Close()

	// Setup: 3 entities, only 2 have :score attribute
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")

	tx.Add(e1, datalog.NewKeyword(":entity/name"), "Alice")
	tx.Add(e2, datalog.NewKeyword(":entity/name"), "Bob")
	tx.Add(e3, datalog.NewKeyword(":entity/name"), "Carol")
	tx.Add(e1, datalog.NewKeyword(":entity/score"), int64(100))
	tx.Add(e2, datalog.NewKeyword(":entity/score"), int64(200))
	// e3 has no :entity/score

	_, err = tx.Commit()
	require.NoError(t, err)

	// Test: or-join with expression branch should use union semantics
	// Branch 1: entities WITH score → get the score
	// Branch 2: entities WITHOUT score → default 0
	// Union = all entities with scores
	t.Run("or_join_union_with_ground_default", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?score
			 :where [?e :entity/name ?name]
			        (or-join [?e ?score]
			          [?e :entity/score ?score]
			          (and (not [?e :entity/score _])
			               [(ground 0) ?score]))]`))
		require.NoError(t, err, "OR-join with expression branches should work with union semantics")
		t.Logf("Results: %v", results)
		require.Len(t, results, 3, "should return all 3 entities")

		byName := make(map[string]int64)
		for _, row := range results {
			byName[row[0].(string)] = row[1].(int64)
		}
		assert.Equal(t, int64(100), byName["Alice"])
		assert.Equal(t, int64(200), byName["Bob"])
		assert.Equal(t, int64(0), byName["Carol"])
	})

	// Same test with plain or (not or-join)
	t.Run("or_union_with_ground_default", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?score
			 :where [?e :entity/name ?name]
			        (or [?e :entity/score ?score]
			            (and (not [?e :entity/score _])
			                 [(ground 0) ?score]))]`))
		require.NoError(t, err, "OR with expression branches should work with union semantics")
		t.Logf("Results: %v", results)
		require.Len(t, results, 3, "should return all 3 entities")

		byName := make(map[string]int64)
		for _, row := range results {
			byName[row[0].(string)] = row[1].(int64)
		}
		assert.Equal(t, int64(100), byName["Alice"])
		assert.Equal(t, int64(200), byName["Bob"])
		assert.Equal(t, int64(0), byName["Carol"])
	})

	// Setup children data before these tests
	{
		tx := db.NewTransaction()
		for i := 0; i < 3; i++ {
			c := datalog.NewIdentity(fmt.Sprintf("child:1:%d", i))
			tx.Add(c, datalog.NewKeyword(":child/parent"), e1)
		}
		c := datalog.NewIdentity("child:2:0")
		tx.Add(c, datalog.NewKeyword(":child/parent"), e2)
		// e3 has no children
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	// Verify NOT branch independently: entities without children
	t.Run("not_branch_standalone", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name
			 :where [?e :entity/name ?name]
			        (not [_ :child/parent ?e])]`))
		require.NoError(t, err)
		t.Logf("Entities without children: %v", results)
		require.Len(t, results, 1)
		assert.Equal(t, "Carol", results[0][0])
	})

	// Test: decorrelated subquery in or-join union branch
	t.Run("or_join_decorrelated_subquery_with_default", func(t *testing.T) {

		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?count
			 :where [?e :entity/name ?name]
			        (or-join [?e ?count]
			          [(q [:find ?e (count ?c)
			               :in $
			               :where [?c :child/parent ?e]]
			              $) [[?e ?count] ...]]
			          (and (not [_ :child/parent ?e])
			               [(ground 0) ?count]))]`))
		require.NoError(t, err, "decorrelated subquery in or-join union should work")
		t.Logf("Results: %v", results)
		require.Len(t, results, 3, "should return all 3 entities")

		byName := make(map[string]int64)
		for _, row := range results {
			byName[row[0].(string)] = row[1].(int64)
		}
		assert.Equal(t, int64(3), byName["Alice"])
		assert.Equal(t, int64(1), byName["Bob"])
		assert.Equal(t, int64(0), byName["Carol"])
	})
}
