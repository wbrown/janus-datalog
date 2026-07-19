//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// setupBaseEntities creates a fresh database with 3 named entities.
// Alice has score 100, Bob has score 200, Carol has no score.
func setupBaseEntities(t *testing.T) (*Database, datalog.Identity, datalog.Identity, datalog.Identity) {
	t.Helper()
	dir, err := os.MkdirTemp("", "or-union-expr-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")

	tx := db.NewTransaction()
	tx.Add(e1, datalog.NewKeyword(":entity/name"), "Alice")
	tx.Add(e2, datalog.NewKeyword(":entity/name"), "Bob")
	tx.Add(e3, datalog.NewKeyword(":entity/name"), "Carol")
	tx.Add(e1, datalog.NewKeyword(":entity/score"), int64(100))
	tx.Add(e2, datalog.NewKeyword(":entity/score"), int64(200))
	_, err = tx.Commit()
	require.NoError(t, err)

	return db, e1, e2, e3
}

// addChildrenTypesAndFriends adds children, types, and friend relationships.
func addChildrenTypesAndFriends(t *testing.T, db *Database, e1, e2, e3 datalog.Identity) {
	t.Helper()
	tx := db.NewTransaction()
	for i := 0; i < 3; i++ {
		c := datalog.NewIdentity(fmt.Sprintf("child:1:%d", i))
		tx.Add(c, datalog.NewKeyword(":child/parent"), e1)
	}
	c := datalog.NewIdentity("child:2:0")
	tx.Add(c, datalog.NewKeyword(":child/parent"), e2)

	tx.Add(e1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/parent"))
	tx.Add(e2, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/parent"))
	tx.Add(e3, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/child"))

	tx.Add(e1, datalog.NewKeyword(":entity/friend"), e2)
	tx.Add(e2, datalog.NewKeyword(":entity/friend"), e1)
	tx.Add(e3, datalog.NewKeyword(":entity/friend"), e1)

	_, err := tx.Commit()
	require.NoError(t, err)
}

func TestOrUnionWithExpressionBranches(t *testing.T) {
	t.Run("or_join_union_with_ground_default", func(t *testing.T) {
		db, _, _, _ := setupBaseEntities(t)

		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?score
			 :where [?e :entity/name ?name]
			        (or-default-join [[?e] ?score]
			          [?e :entity/score ?score]
			          (and (not [?e :entity/score _])
			               [(ground 0) ?score]))]`))
		require.NoError(t, err)
		t.Logf("Results: %v", results)
		require.Len(t, results, 3)

		byName := make(map[string]int64)
		for _, row := range results {
			byName[row[0].(string)] = row[1].(int64)
		}
		assert.Equal(t, int64(100), byName["Alice"])
		assert.Equal(t, int64(200), byName["Bob"])
		assert.Equal(t, int64(0), byName["Carol"])
	})

	t.Run("or_union_with_ground_default", func(t *testing.T) {
		db, _, _, _ := setupBaseEntities(t)

		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?score
			 :where [?e :entity/name ?name]
			        (or-default [?e :entity/score ?score]
			            (and (not [?e :entity/score _])
			                 [(ground 0) ?score]))]`))
		require.NoError(t, err)
		t.Logf("Results: %v", results)
		require.Len(t, results, 3)

		byName := make(map[string]int64)
		for _, row := range results {
			byName[row[0].(string)] = row[1].(int64)
		}
		assert.Equal(t, int64(100), byName["Alice"])
		assert.Equal(t, int64(200), byName["Bob"])
		assert.Equal(t, int64(0), byName["Carol"])
	})

	t.Run("not_branch_standalone", func(t *testing.T) {
		db, e1, e2, e3 := setupBaseEntities(t)
		addChildrenTypesAndFriends(t, db, e1, e2, e3)

		results, err := executor.CollectTuples(db.Query(`
			[:find ?name
			 :where [?e :entity/name ?name]
			        (not [_ :child/parent ?e])]`))
		require.NoError(t, err)
		t.Logf("Entities without children: %v", results)
		require.Len(t, results, 1)
		assert.Equal(t, "Carol", results[0][0])
	})

	t.Run("nested_or_join_in_and_branches", func(t *testing.T) {
		db, e1, e2, e3 := setupBaseEntities(t)
		addChildrenTypesAndFriends(t, db, e1, e2, e3)

		results, err := executor.CollectTuples(db.Query(`
			[:find ?self-name ?related-name
			 :where [?self :entity/name ?self-name]
			        [?self :entity/type ?stype]
			        (or-default-join [[?self ?stype] ?related]
			          (and [(ground :type/parent) ?stype]
			               (or-join [?related ?self]
			                 [?related :child/parent ?self]
			                 [?self :entity/friend ?related]))
			          (and [(ground :type/child) ?stype]
			               [?self :entity/friend ?related]))
			        [?related :entity/name ?related-name]]`))
		require.NoError(t, err)
		t.Logf("Results: %v", results)
		require.Greater(t, len(results), 0)

		byPair := make(map[string]bool)
		for _, row := range results {
			byPair[row[0].(string)+"→"+row[1].(string)] = true
		}
		assert.True(t, byPair["Alice→Bob"], "Alice should find friend Bob")
		assert.True(t, byPair["Bob→Alice"], "Bob should find friend Alice")
		assert.True(t, byPair["Carol→Alice"], "Carol should find friend Alice")
	})

	// Generic version of the 4-branch nested or-join pattern.
	// Same structure as the downstream query but with abstract names.
	// 4 entity types, each with type-dependent relationship rules:
	//   alpha: related by containment OR shared region
	//   beta: related by co-location (or location itself) OR provider
	//   gamma: related by co-location (or location itself)
	//   delta: related by link OR parent link chain
	t.Run("deeply_nested_or_join_4_branches_generic", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "deep-nested-generic-*")
		require.NoError(t, err)
		t.Cleanup(func() { os.RemoveAll(dir) })
		db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })

		// Entities:
		//   A1 (alpha) — located at A2, in region R1
		//   A2 (alpha) — in region R1
		//   R1 (region, no type)
		//   B1 (beta) — at A2 location, provider for A1
		//   G1 (gamma) — at A2 location
		//   D1 (delta) — linked to D2 (parent)
		//   D2 (delta) — parent

		a1 := datalog.NewIdentity("a1")
		a2 := datalog.NewIdentity("a2")
		r1 := datalog.NewIdentity("r1")
		b1 := datalog.NewIdentity("b1")
		g1 := datalog.NewIdentity("g1")
		d1 := datalog.NewIdentity("d1")
		d2 := datalog.NewIdentity("d2")

		tx := db.NewTransaction()

		tx.Add(a1, datalog.NewKeyword(":entity/name"), "A1")
		tx.Add(a2, datalog.NewKeyword(":entity/name"), "A2")
		tx.Add(r1, datalog.NewKeyword(":entity/name"), "R1")
		tx.Add(b1, datalog.NewKeyword(":entity/name"), "B1")
		tx.Add(g1, datalog.NewKeyword(":entity/name"), "G1")
		tx.Add(d1, datalog.NewKeyword(":entity/name"), "D1")
		tx.Add(d2, datalog.NewKeyword(":entity/name"), "D2")

		tx.Add(a1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/alpha"))
		tx.Add(a2, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/alpha"))
		tx.Add(b1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/beta"))
		tx.Add(g1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/gamma"))
		tx.Add(d1, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/delta"))
		tx.Add(d2, datalog.NewKeyword(":entity/type"), datalog.NewKeyword(":type/delta"))

		tx.Add(a1, datalog.NewKeyword(":rel/location"), a2)
		tx.Add(a1, datalog.NewKeyword(":rel/region"), r1)
		tx.Add(a2, datalog.NewKeyword(":rel/region"), r1)

		tx.Add(b1, datalog.NewKeyword(":rel/location"), a2)
		tx.Add(b1, datalog.NewKeyword(":rel/provider"), a1)

		tx.Add(g1, datalog.NewKeyword(":rel/location"), a2)

		tx.Add(d1, datalog.NewKeyword(":rel/link"), d2)

		_, err = tx.Commit()
		require.NoError(t, err)

		// Same 4-branch structure with generic attributes
		results, err := executor.CollectTuples(db.Query(`
			[:find ?rname ?rtype
			 :where
			 [?self :entity/name "A1"]
			 [?self :entity/type ?stype]
			 (or-default-join [[?self ?stype] ?related]
			   (and [(ground :type/alpha) ?stype]
			        (or-join [?related ?self]
			          [?related :rel/location ?self]
			          (and [?self :rel/region ?rgn]
			               [?related :rel/region ?rgn])))
			   (and [(ground :type/beta) ?stype]
			        (or-join [?related ?self]
			          (and [?self :rel/location ?loc]
			               (or [?related :rel/location ?loc]
			                   [(identity ?loc) ?related]))
			          [?related :rel/provider ?self]))
			   (and [(ground :type/gamma) ?stype]
			        [?self :rel/location ?loc2]
			        (or [?related :rel/location ?loc2]
			            [(identity ?loc2) ?related]))
			   (and [(ground :type/delta) ?stype]
			        (or-join [?related ?self]
			          [?related :rel/link ?self]
			          (and [?self :rel/link ?parent]
			               (or [(identity ?parent) ?related]
			                   [?related :rel/link ?parent])))))
			 [?related :entity/name ?rname]
			 [?related :entity/type ?rtype]]`))
		require.NoError(t, err)
		t.Logf("Results (%d):", len(results))
		for _, row := range results {
			t.Logf("  %s (%s)", row[0], row[1])
		}

		// A1 is type alpha. Branch 1 applies:
		//   [?related :rel/location ?self] → nothing located AT A1
		//   [?self :rel/region ?rgn] [?related :rel/region ?rgn]
		//     → A1 in R1, A2 also in R1 → A2 is related via shared region
		byName := make(map[string]bool)
		for _, row := range results {
			byName[row[0].(string)] = true
		}
		require.Greater(t, len(results), 0, "A1 should find related entities")
		assert.True(t, byName["A2"], "should find A2 via shared region")
	})

	t.Run("ground_rebinding_existing_symbol", func(t *testing.T) {
		db, e1, e2, e3 := setupBaseEntities(t)
		addChildrenTypesAndFriends(t, db, e1, e2, e3)

		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?stype
			 :where [?e :entity/name ?name]
			        [?e :entity/type ?stype]
			        (or-join [?e ?stype]
			          (and [(ground :type/parent) ?stype]
			               [?e :entity/friend _])
			          (and [(ground :type/child) ?stype]
			               [?e :entity/friend _]))]`))
		require.NoError(t, err)
		t.Logf("Results: %v", results)
		require.Greater(t, len(results), 0, "ground rebinding existing symbol should produce results")
	})

	t.Run("or_join_decorrelated_subquery_with_default", func(t *testing.T) {
		db, e1, e2, e3 := setupBaseEntities(t)
		addChildrenTypesAndFriends(t, db, e1, e2, e3)

		results, err := executor.CollectTuples(db.Query(`
			[:find ?name ?count
			 :where [?e :entity/name ?name]
			        (or-default-join [[?e] ?count]
			          [(q [:find ?e (count ?c)
			               :in $
			               :where [?c :child/parent ?e]]
			              $) [[?e ?count] ...]]
			          (and (not [_ :child/parent ?e])
			               [(ground 0) ?count]))]`))
		require.NoError(t, err)
		t.Logf("Results: %v", results)
		require.Len(t, results, 3)

		byName := make(map[string]int64)
		for _, row := range results {
			byName[row[0].(string)] = row[1].(int64)
		}
		assert.Equal(t, int64(3), byName["Alice"])
		assert.Equal(t, int64(1), byName["Bob"])
		assert.Equal(t, int64(0), byName["Carol"])
	})
}

// TestOrCorrelatedUnionWithNestedOrExpression_E2E verifies that OR with nested
// OR+expression branches correctly routes to correlated execution when using
// variable attributes from collection inputs.
//
// Without recursive branchesNeedCorrelatedExecution, the outer OR uses
// uncorrelated union, ?fwd gets dropped from the output (intersection of
// branch symbols), and ?target picks up values from ALL attributes (not just
// the collection), causing identity to return wrong types (string, bool
// instead of Identity).
func TestOrCorrelatedUnionWithNestedOrExpression_E2E(t *testing.T) {
	dir, err := os.MkdirTemp("", "or-nested-expr-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	area1 := datalog.NewIdentity("area:caves")
	room1 := datalog.NewIdentity("room:guard")
	room2 := datalog.NewIdentity("room:shrine")
	npc1 := datalog.NewIdentity("npc:merchant")

	tx := db.NewTransaction()
	tx.Add(area1, datalog.NewKeyword(":entity/name"), "Coastal Caves")
	tx.Add(room1, datalog.NewKeyword(":entity/name"), "Guard Chamber")
	tx.Add(room2, datalog.NewKeyword(":entity/name"), "Shrine Hall")
	tx.Add(npc1, datalog.NewKeyword(":entity/name"), "Merchant")
	// room1 and room2 share area1
	tx.Add(room1, datalog.NewKeyword(":entity/area"), area1)
	tx.Add(room2, datalog.NewKeyword(":entity/area"), area1)
	// npc1 is located in room1
	tx.Add(npc1, datalog.NewKeyword(":entity/location"), room1)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Exact production pattern: variable attributes from collection inputs
	results, err := executor.CollectTuples(db.Query(`
		[:find ?related
		 :in $ ?self [?fwd ...] [?rev ...]
		 :where
		 (or (and [?self ?fwd ?target]
		          (or [?related ?fwd ?target]
		              [(identity ?target) ?related]))
		     [?related ?rev ?self])]`,
		room1,
		[]datalog.Keyword{datalog.NewKeyword(":entity/area")},
		[]datalog.Keyword{datalog.NewKeyword(":entity/location")},
	))
	require.NoError(t, err)

	t.Logf("Results (%d):", len(results))
	identities := make(map[datalog.Identity]bool)
	for i, row := range results {
		val := row[0]
		id, ok := val.(datalog.Identity)
		if !ok {
			t.Errorf("result[%d]: expected Identity, got %T (%v)", i, val, val)
			continue
		}
		identities[id] = true
		t.Logf("  [%d] %v", i, id)
	}

	// room2 shares :entity/area with room1
	assert.True(t, identities[room2], "missing room2 (shares area via forward ref)")
	// area1 via (identity ?target)
	assert.True(t, identities[area1], "missing area1 (area entity via identity)")
	// npc1 has :entity/location = room1 (reverse ref)
	assert.True(t, identities[npc1], "missing npc1 (located in room1 via reverse ref)")
}
