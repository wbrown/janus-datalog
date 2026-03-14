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

// TestOrUnionAsLeftJoin tests whether OR with union semantics can express
// left outer join semantics: "get value if exists, else default."
//
// The idea: instead of OR-fallback (per-tuple, correlated), use OR-union
// with two branches:
//   Branch 1: entities WITH matching data → get the value
//   Branch 2: entities WITHOUT matching data → get the default
//   Union = all entities with values
//
// This avoids correlated execution entirely — both branches run independently.
func TestOrUnionAsLeftJoin(t *testing.T) {
	dir, err := os.MkdirTemp("", "or-union-left-join-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              dir,
		AnnotationHandler: func(e annotations.Event) {},
	})
	require.NoError(t, err)
	defer db.Close()

	// Setup: 3 parents, some with children, some without
	tx := db.NewTransaction()
	p1 := datalog.NewIdentity("parent:1")
	p2 := datalog.NewIdentity("parent:2")
	p3 := datalog.NewIdentity("parent:3")

	tx.Add(p1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/parent"))
	tx.Add(p2, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/parent"))
	tx.Add(p3, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/parent"))
	tx.Add(p1, datalog.NewKeyword(":parent/name"), "Alice")
	tx.Add(p2, datalog.NewKeyword(":parent/name"), "Bob")
	tx.Add(p3, datalog.NewKeyword(":parent/name"), "Carol")

	// Alice has 3 children, Bob has 1, Carol has none
	for i := 0; i < 3; i++ {
		c := datalog.NewIdentity(fmt.Sprintf("child:1:%d", i))
		tx.Add(c, datalog.NewKeyword(":child/parent"), p1)
		tx.Add(c, datalog.NewKeyword(":child/value"), int64(10+i))
	}
	c := datalog.NewIdentity("child:2:0")
	tx.Add(c, datalog.NewKeyword(":child/parent"), p2)
	tx.Add(c, datalog.NewKeyword(":child/value"), int64(99))

	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify data
	verifyResults, err := executor.CollectTuples(db.Query(
		`[:find ?name :where [?p :parent/name ?name]]`))
	require.NoError(t, err)
	t.Logf("Parents: %v", verifyResults)

	childResults, err := executor.CollectTuples(db.Query(
		`[:find ?p ?c :where [?c :child/parent ?p]]`))
	require.NoError(t, err)
	t.Logf("Children: %d", len(childResults))

	// Test 0: bare subquery without OR wrapping
	// Disable algebra optimizer to test baseline behavior
	t.Run("bare_subquery", func(t *testing.T) {
		t.Logf("EnableAlgebraOptimizer default: %v", DefaultPlannerOptions().EnableAlgebraOptimizer)
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?count
			 :where [?p :entity/type :type/parent]
			        [?p :parent/name ?name]
			        [(q [:find (count ?c)
			             :in $ ?p
			             :where [?c :child/parent ?p]]
			            $ ?p) [[?count]]]]`))
		if err != nil {
			t.Logf("Bare subquery error: %v", err)
		}
		require.NoError(t, err)
		t.Logf("Bare subquery results: %v", results)
	})

	// Test 1: Simple OR-fallback (correlated, current approach)
	t.Run("or_fallback_correlated", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?count
			 :where [?p :entity/type :type/parent]
			        [?p :parent/name ?name]
			        (or [(q [:find (count ?c)
			                 :in $ ?p
			                 :where [?c :child/parent ?p]]
			                $ ?p) [[?count]]]
			            [(ground 0) ?count])]`))
		require.NoError(t, err)
		t.Logf("OR-fallback results: %v", results)
		require.Len(t, results, 3, "should return all 3 parents")

		byName := make(map[string]int)
		for _, row := range results {
			byName[row[0].(string)] = int(row[1].(int64))
		}
		assert.Equal(t, 3, byName["Alice"])
		assert.Equal(t, 1, byName["Bob"])
		assert.Equal(t, 0, byName["Carol"])
	})

	// Test 2: OR-union with NOT branch for left join semantics
	// Uses RelationBinding [[?p ?count] ...] for multi-row subquery
	t.Run("or_union_with_not", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?count
			 :where [?p :entity/type :type/parent]
			        [?p :parent/name ?name]
			        (or-join [?p ?count]
			          [(q [:find ?p (count ?c)
			               :in $
			               :where [?c :child/parent ?p]]
			              $) [[?p ?count] ...]]
			          (and (not [_ :child/parent ?p])
			               [(ground 0) ?count]))]`))
		if err != nil {
			t.Logf("OR-union error: %v", err)
		}
		require.NoError(t, err)
		t.Logf("OR-union results: %v", results)
		require.Len(t, results, 3, "should return all 3 parents")

		byName := make(map[string]int)
		for _, row := range results {
			byName[row[0].(string)] = int(row[1].(int64))
		}
		assert.Equal(t, 3, byName["Alice"])
		assert.Equal(t, 1, byName["Bob"])
		assert.Equal(t, 0, byName["Carol"])
	})

	// Test 3: Decorrelated subquery with RelationBinding (inner join only, no defaults)
	t.Run("decorrelated_relation_binding", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?count
			 :where [?p :entity/type :type/parent]
			        [?p :parent/name ?name]
			        [(q [:find ?p (count ?c)
			             :in $
			             :where [?c :child/parent ?p]]
			            $) [[?p ?count] ...]]]`))
		if err != nil {
			t.Logf("Decorrelated error: %v", err)
		}
		require.NoError(t, err)
		t.Logf("Decorrelated results: %v", results)
		// Only parents WITH children should appear (inner join)
		require.Len(t, results, 2, "inner join should return only parents with children")

		byName := make(map[string]int)
		for _, row := range results {
			byName[row[0].(string)] = int(row[1].(int64))
		}
		assert.Equal(t, 3, byName["Alice"])
		assert.Equal(t, 1, byName["Bob"])
	})

	// Test 4: Decorrelated subquery with TupleBinding.
	// TupleBinding [[?a ?b]] binds a single tuple. A decorrelated subquery
	// returns multiple rows, so this should use RelationBinding [[?a ?b] ...]
	// instead. This test documents the current behavior.
	t.Run("decorrelated_tuple_binding", func(t *testing.T) {
		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?count
			 :where [?p :entity/type :type/parent]
			        [?p :parent/name ?name]
			        [(q [:find ?p (count ?c)
			             :in $
			             :where [?c :child/parent ?p]]
			            $) [[?p ?count]]]]`))
		if err != nil {
			t.Logf("TupleBinding with multi-row result: %v", err)
			t.Log("Use RelationBinding [[?p ?count] ...] for multi-row subquery results")
			t.Skip("TupleBinding only handles single-row results per Datomic spec")
		}
		t.Logf("Tuple binding results: %v", results)
	})
}
