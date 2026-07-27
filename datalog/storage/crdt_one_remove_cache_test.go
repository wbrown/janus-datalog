package storage

import (
	"crypto/sha1"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// CardinalityOne Remove Tests — CACHE PATH
// =============================================================================
//
// These tests mirror crdt_one_remove_test.go but exercise the CACHE resolution
// path (ResolveLWW) instead of the streaming path (CRDTResolvingIterator).
//
// The streaming tests all bind E via :in parameters, which bypasses the cache.
// These tests use:
//   1. PullInto / Pull — always resolves through the EA cache
//   2. Multi-clause queries — E bound by a prior join clause, not :in
//
// Bug: ResolveLWW does not check datom.Op. After Remove(), the OpCRDTRemove
// tombstone has the highest Tx and is the first EATV entry, but ResolveLWW
// returns its V without checking Op. The attribute appears to still exist.
//
// See: docs/bugs/BUG_CACHE_CARDINALIY_ONE_TOMBSTONE.md
// =============================================================================

// struct for PullInto tests — uses pointer so nil means "attribute absent"
type PersonOptionalName struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name *string          `datalog:"person/name"`
}

// struct with two attributes for multi-clause join tests
type PersonWithCity struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name *string          `datalog:"person/name"`
	City *string          `datalog:"person/city"`
}

// createRemoveCacheTestDB creates a DB with :person/name (one) and :person/city (one).
// popts sets the database's default planner options (nil = defaults).
func createCacheRemoveTestDB(t *testing.T, popts *planner.PlannerOptions) (*Database, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           dir,
		PlannerOptions: popts,
	})
	require.NoError(t, err)

	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/city").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	return db, func() { db.Close() }
}

func entityFromIdentity(identity datalog.Identity) Entity {
	var entity Entity
	copy(entity[:], identity.Bytes())
	return entity
}

func TestEntityFromIdentityIgnoresInternedDisplayString(t *testing.T) {
	seed := "cache-remove-storage-identity"
	hash := sha1.Sum([]byte(seed))
	datalog.NewIdentityFromHash(hash)
	identity := datalog.NewIdentity(seed)

	require.Equal(t, Entity(hash), entityFromIdentity(identity))
}

// =============================================================================
// PullInto Tests
// =============================================================================

// Cache Test 1: Add, Remove, PullInto → attribute absent
func TestCacheRemove_PullInto_RoundTrip(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Add
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Verify value exists via PullInto
			var before PersonOptionalName
			require.NoError(t, db.PullInto(e, &before))
			require.NotNil(t, before.Name, "precondition: name should exist")
			assert.Equal(t, "Alice", *before.Name)

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// PullInto → attribute absent
			var after PersonOptionalName
			require.NoError(t, db.PullInto(e, &after))
			assert.Nil(t, after.Name,
				"BUG: PullInto returns old value after Remove(). "+
					"Expected nil, got %v. ResolveLWW doesn't check datom.Op.", after.Name)
		})
	}
}

// Cache Test 2: Add, overwrite, Remove → attribute absent via PullInto
func TestCacheRemove_PullInto_AfterOverwrite(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Add "Alice"
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Overwrite with "Bob"
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(e, a, "Bob"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Remove
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Remove(e, a, "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// PullInto → absent
			var person PersonOptionalName
			require.NoError(t, db.PullInto(e, &person))
			assert.Nil(t, person.Name, "attribute should not exist after Remove via PullInto")
		})
	}
}

// Cache Test 3: Add, Remove, Add again → latest Add wins via PullInto
func TestCacheRemove_PullInto_ThenReAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Add
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Re-add
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Add(e, a, "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Latest Add wins
			var person PersonOptionalName
			require.NoError(t, db.PullInto(e, &person))
			require.NotNil(t, person.Name, "attribute should exist after re-Add")
			assert.Equal(t, "Bob", *person.Name)
		})
	}
}

// Cache Test 4: Remove before any Add, then Add → value exists via PullInto
func TestCacheRemove_PullInto_BeforeAnyAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Remove first
			tx := db.NewTransaction()
			require.NoError(t, tx.Remove(e, a, "phantom"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Then Add
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Add wins
			var person PersonOptionalName
			require.NoError(t, db.PullInto(e, &person))
			require.NotNil(t, person.Name, "Add should win over earlier Remove")
			assert.Equal(t, "Alice", *person.Name)
		})
	}
}

// Cache Test 5: V is irrelevant for CardinalityOne remove via PullInto
func TestCacheRemove_PullInto_VIsIrrelevant(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Add "Alice"
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove with different V
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Bob"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Attribute absent
			var person PersonOptionalName
			require.NoError(t, db.PullInto(e, &person))
			assert.Nil(t, person.Name,
				"attribute should not exist — V is irrelevant for CardinalityOne Remove")
		})
	}
}

// Cache Test 6: Multiple entities, remove one, PullInto both
func TestCacheRemove_PullInto_MultipleEntities(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e1 := datalog.NewIdentity("alice")
			e2 := datalog.NewIdentity("bob")
			a := datalog.NewKeyword(":person/name")

			// Add both
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e1, a, "Alice"))
			require.NoError(t, tx.Add(e2, a, "Bob"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove only entity1
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e1, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// entity1: absent
			var p1 PersonOptionalName
			require.NoError(t, db.PullInto(e1, &p1))
			assert.Nil(t, p1.Name, "entity1 attribute should not exist after Remove")

			// entity2: unaffected
			var p2 PersonOptionalName
			require.NoError(t, db.PullInto(e2, &p2))
			require.NotNil(t, p2.Name, "entity2 attribute should still exist")
			assert.Equal(t, "Bob", *p2.Name)
		})
	}
}

// =============================================================================
// Pull (wildcard) Tests
// =============================================================================

// Cache Test 7: Add, Remove, Pull("*") → attribute absent from result map
func TestCacheRemove_Pull_RoundTrip(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Add
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Verify exists
			result, err := db.Pull(e, "[*]")
			require.NoError(t, err)
			assert.Equal(t, "Alice", result["person/name"], "precondition: name should exist")

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Pull → absent
			result, err = db.Pull(e, "[*]")
			require.NoError(t, err)
			_, exists := result["person/name"]
			assert.False(t, exists,
				"BUG: Pull returns old value after Remove(). "+
					"Expected key absent, got %v", result["person/name"])
		})
	}
}

// =============================================================================
// Multi-Clause Join-Bound E Tests
// =============================================================================
//
// These queries bind ?e via a prior clause (not :in), which causes the
// second clause to resolve through the EA cache instead of streaming.
// Pattern: [?e :person/city ?city] binds ?e, then [?e :person/name ?name]
// resolves through cache for that bound ?e.
// =============================================================================

// Cache Test 8: Add name+city, Remove name, multi-clause query → name absent
func TestCacheRemove_JoinBoundE_RoundTrip(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")

			// Add both attributes
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/city"), "Portland"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Verify both exist via multi-clause query
			// :person/city binds ?e, then :person/name resolves with bound E → cache path
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			require.Len(t, results, 1, "precondition: should find 1 result")

			// Remove the name
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, datalog.NewKeyword(":person/name"), "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Multi-clause query → name clause should fail to match (tombstoned)
			results, err = executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			assert.Len(t, results, 0,
				"BUG: join-bound query returns stale data after Remove(). "+
					"Expected 0 results, got %d", len(results))
		})
	}
}

// Cache Test 9: Add, Remove, re-Add → latest wins via join-bound query
func TestCacheRemove_JoinBoundE_ThenReAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")

			// Add both attributes
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/city"), "Portland"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove name
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, datalog.NewKeyword(":person/name"), "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Re-add with different value
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Add(e, datalog.NewKeyword(":person/name"), "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Latest Add wins
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			require.Len(t, results, 1, "should find 1 result after re-Add")
			assert.Equal(t, "Bob", results[0][0])
		})
	}
}

// Cache Test 10: Multiple entities, remove one, join-bound query
func TestCacheRemove_JoinBoundE_MultipleEntities(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e1 := datalog.NewIdentity("alice")
			e2 := datalog.NewIdentity("bob")

			// Add both entities with both attributes
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Add(e1, datalog.NewKeyword(":person/city"), "Portland"))
			require.NoError(t, tx.Add(e2, datalog.NewKeyword(":person/name"), "Bob"))
			require.NoError(t, tx.Add(e2, datalog.NewKeyword(":person/city"), "Seattle"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove only entity1's name
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e1, datalog.NewKeyword(":person/name"), "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Query: entity1 should not appear, entity2 should
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			assert.Len(t, results, 1, "only entity2 should appear")
			if len(results) == 1 {
				assert.Equal(t, "Bob", results[0][0])
				assert.Equal(t, "Seattle", results[0][1])
			}
		})
	}
}

// Cache Test 11: V-irrelevant Remove via join-bound query
func TestCacheRemove_JoinBoundE_VIsIrrelevant(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")

			// Add both attributes
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/city"), "Portland"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove name with different V (doesn't matter for CardinalityOne)
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, datalog.NewKeyword(":person/name"), "NotAlice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Should return 0 results — name is tombstoned regardless of V
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			assert.Len(t, results, 0,
				"attribute should not exist — V is irrelevant for CardinalityOne Remove")
		})
	}
}

// =============================================================================
// ResolveLWW Direct Tests
// =============================================================================
//
// These test ResolveLWW directly through the CacheResolver interface,
// verifying it returns nil for tombstoned attributes.
// =============================================================================

// Cache Test 12: ResolveLWW returns nil after Remove
func TestCacheRemove_ResolveLWW_Direct(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Verify ResolveLWW returns value
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(db.Schema())
	eStorage := entityFromIdentity(e)
	var aStorage Attribute
	copy(aStorage[:], a.String())

	val, _, _, err := matcher.ResolveLWW(eStorage, aStorage)
	require.NoError(t, err)
	assert.Equal(t, "Alice", val, "precondition: ResolveLWW should return value")

	// Remove
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// ResolveLWW should return nil after Remove
	val, _, _, err = matcher.ResolveLWW(eStorage, aStorage)
	require.NoError(t, err)
	assert.Nil(t, val,
		"BUG: ResolveLWW returns value after Remove(). "+
			"Expected nil, got %v. Does not check datom.Op.", val)
}

// Cache Test 13: Cache rebuild after Remove returns nil
func TestCacheRemove_CacheRebuild(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Clear cache to force rebuild
	db.Cache().Clear()

	// Cache rebuild should reflect tombstone
	eStorage := entityFromIdentity(e)
	var aStorage Attribute
	copy(aStorage[:], a.String())
	key := CacheKey{E: eStorage, A: aStorage}

	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(db.Schema())
	entry, _, err := db.Cache().GetOrResolve(key, matcher, nil, nil)
	require.NoError(t, err)

	// Entry should either be nil or have nil OneValue
	if entry != nil {
		assert.Nil(t, entry.OneValue(),
			"BUG: Cache rebuild returns value after Remove(). "+
				"Expected nil OneValue, got %v", entry.OneValue())
	}
}

// =============================================================================
// Stale Cache Invalidation Tests (P9)
// =============================================================================
//
// The production path: cache is warm (query populated it), then Remove()
// happens, Commit() invalidates the cache entry, next query triggers rebuild.
// This is different from cold-cache tests where no entry exists before Remove.
// =============================================================================

// Cache Test 14: Warm cache → Remove → query again → absent
func TestCacheRemove_StaleInvalidation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")

			// Add both attributes
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/city"), "Portland"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query to warm the cache (PullInto populates EA cache)
			var before PersonOptionalName
			require.NoError(t, db.PullInto(e, &before))
			require.NotNil(t, before.Name, "precondition: cache should be warm with value")
			assert.Equal(t, "Alice", *before.Name)

			// Remove — Commit() should invalidate the cached entry
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, datalog.NewKeyword(":person/name"), "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Query again — cache was warm, now stale, should rebuild and see tombstone
			var after PersonOptionalName
			require.NoError(t, db.PullInto(e, &after))
			assert.Nil(t, after.Name,
				"BUG: PullInto returns stale cached value after Remove(). "+
					"Cache invalidation or rebuild doesn't handle tombstone.")

			// Also verify via multi-clause join query
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			assert.Len(t, results, 0,
				"BUG: Join query returns stale cached data after Remove()")
		})
	}
}

// =============================================================================
// Set() + Remove Tests (S7)
// =============================================================================
//
// All existing tests use tx.Add(). The the application reproducer uses
// tx.Set(). These tests verify the cache path with Set() then Remove().
// =============================================================================

// Cache Test 15: Set() then Remove() via PullInto
func TestCacheRemove_PullInto_SetThenRemove(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Set (not Add)
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Verify
			var before PersonOptionalName
			require.NoError(t, db.PullInto(e, &before))
			require.NotNil(t, before.Name)
			assert.Equal(t, "Alice", *before.Name)

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// PullInto → absent
			var after PersonOptionalName
			require.NoError(t, db.PullInto(e, &after))
			assert.Nil(t, after.Name,
				"BUG: PullInto returns value after Set() then Remove()")
		})
	}
}

// Cache Test 16: Set() then Remove() via join-bound query
func TestCacheRemove_JoinBoundE_SetThenRemove(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")

			// Set both attributes
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Set(e, datalog.NewKeyword(":person/city"), "Portland"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove name
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, datalog.NewKeyword(":person/name"), "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Join query → absent
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			assert.Len(t, results, 0,
				"BUG: Join query returns value after Set() then Remove()")
		})
	}
}

// Cache Test 17: Set() then Remove() via ResolveLWW direct
func TestCacheRemove_ResolveLWW_SetThenRemove(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Set
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// ResolveLWW → nil
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(db.Schema())
	eStorage := entityFromIdentity(e)
	var aStorage Attribute
	copy(aStorage[:], a.String())

	val, _, _, err := matcher.ResolveLWW(eStorage, aStorage)
	require.NoError(t, err)
	assert.Nil(t, val,
		"BUG: ResolveLWW returns value after Set() then Remove()")
}

// =============================================================================
// Join-Bound Completeness (S2, S4 via join path)
// =============================================================================

// Cache Test 18: Add → Add → Remove via join-bound query
func TestCacheRemove_JoinBoundE_AfterOverwrite(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")

			// Add name + city
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/name"), "Alice"))
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/city"), "Portland"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Overwrite name
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(e, datalog.NewKeyword(":person/name"), "Bob"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Remove
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Remove(e, datalog.NewKeyword(":person/name"), "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Join query → absent
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			assert.Len(t, results, 0,
				"attribute should not exist after overwrite then Remove")
		})
	}
}

// Cache Test 19: Remove → Add via join-bound query
func TestCacheRemove_JoinBoundE_BeforeAnyAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")

			// Add city first (needed for join)
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, datalog.NewKeyword(":person/city"), "Portland"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove name (before any Add of name)
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, datalog.NewKeyword(":person/name"), "phantom"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Add name
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Add(e, datalog.NewKeyword(":person/name"), "Alice"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Add has higher Tx, wins
			results, err := executor.CollectTuples(db.Query(
				`[:find ?name ?city :where [?e :person/city ?city] [?e :person/name ?name]]`))
			require.NoError(t, err)
			require.Len(t, results, 1, "Add should win over earlier Remove")
			assert.Equal(t, "Alice", results[0][0])
		})
	}
}

// =============================================================================
// ResolveLWW Return Contract
// =============================================================================

// Cache Test 20: After Remove, ResolveLWW returns (nil, non-zero ElementID, nil)
func TestCacheRemove_ResolveLWW_ReturnsElementID(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Add
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// ResolveLWW should return nil value but non-zero ElementID
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(db.Schema())
	eStorage := entityFromIdentity(e)
	var aStorage Attribute
	copy(aStorage[:], a.String())

	val, elemID, _, err := matcher.ResolveLWW(eStorage, aStorage)
	require.NoError(t, err, "ResolveLWW should not error after Remove")
	assert.Nil(t, val, "value should be nil after Remove")
	assert.NotEqual(t, datalog.ElementID{}, elemID,
		"ElementID should be non-zero after Remove — needed for cache freshness tracking")
}

// =============================================================================
// P5×S2-S7: Pull wildcard — full scenario coverage
// =============================================================================

func TestCacheRemove_Pull_AfterOverwrite(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(e, a, "Bob"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Remove(e, a, "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			result, err := db.Pull(e, "[*]")
			require.NoError(t, err)
			_, exists := result["person/name"]
			assert.False(t, exists, "Pull: name should be absent after overwrite then Remove")
		})
	}
}

func TestCacheRemove_Pull_ThenReAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Add(e, a, "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			result, err := db.Pull(e, "[*]")
			require.NoError(t, err)
			assert.Equal(t, "Bob", result["person/name"], "Pull: re-Add should win")
		})
	}
}

func TestCacheRemove_Pull_BeforeAnyAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Remove(e, a, "phantom"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			result, err := db.Pull(e, "[*]")
			require.NoError(t, err)
			assert.Equal(t, "Alice", result["person/name"], "Pull: Add should win over earlier Remove")
		})
	}
}

func TestCacheRemove_Pull_VIsIrrelevant(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Bob"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			result, err := db.Pull(e, "[*]")
			require.NoError(t, err)
			_, exists := result["person/name"]
			assert.False(t, exists, "Pull: V is irrelevant for CardinalityOne Remove")
		})
	}
}

func TestCacheRemove_Pull_MultipleEntities(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e1 := datalog.NewIdentity("alice")
			e2 := datalog.NewIdentity("bob")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e1, a, "Alice"))
			require.NoError(t, tx.Add(e2, a, "Bob"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e1, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			r1, err := db.Pull(e1, "[*]")
			require.NoError(t, err)
			_, exists := r1["person/name"]
			assert.False(t, exists, "Pull: entity1 name should be absent after Remove")

			r2, err := db.Pull(e2, "[*]")
			require.NoError(t, err)
			assert.Equal(t, "Bob", r2["person/name"], "Pull: entity2 name should still exist")
		})
	}
}

func TestCacheRemove_Pull_SetThenRemove(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			result, err := db.Pull(e, "[*]")
			require.NoError(t, err)
			_, exists := result["person/name"]
			assert.False(t, exists, "Pull: should be absent after Set then Remove")
		})
	}
}

// =============================================================================
// P7×S2-S6: ResolveLWW direct — full scenario coverage
// =============================================================================

// resolveLWWValue calls ResolveLWW and returns (value, elementID)
func resolveLWW(t *testing.T, db *Database, e datalog.Identity, a datalog.Keyword) (any, datalog.ElementID) {
	t.Helper()
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(db.Schema())
	eStorage := entityFromIdentity(e)
	var aStorage Attribute
	copy(aStorage[:], a.String())
	val, eid, _, err := matcher.ResolveLWW(eStorage, aStorage)
	require.NoError(t, err)
	return val, eid
}

func TestCacheRemove_ResolveLWW_AfterOverwrite(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Remove(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	val, _ := resolveLWW(t, db, e, a)
	assert.Nil(t, val, "ResolveLWW: should return nil after overwrite then Remove")
}

func TestCacheRemove_ResolveLWW_ThenReAdd(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Add(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	val, _ := resolveLWW(t, db, e, a)
	assert.Equal(t, "Bob", val, "ResolveLWW: re-Add should win")
}

func TestCacheRemove_ResolveLWW_BeforeAnyAdd(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Remove(e, a, "phantom"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	val, _ := resolveLWW(t, db, e, a)
	assert.Equal(t, "Alice", val, "ResolveLWW: Add should win over earlier Remove")
}

func TestCacheRemove_ResolveLWW_VIsIrrelevant(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	val, _ := resolveLWW(t, db, e, a)
	assert.Nil(t, val, "ResolveLWW: V is irrelevant for CardinalityOne Remove")
}

func TestCacheRemove_ResolveLWW_MultipleEntities(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e1 := datalog.NewIdentity("alice")
	e2 := datalog.NewIdentity("bob")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e1, a, "Alice"))
	require.NoError(t, tx.Add(e2, a, "Bob"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e1, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	val1, _ := resolveLWW(t, db, e1, a)
	assert.Nil(t, val1, "ResolveLWW: entity1 should be nil after Remove")

	val2, _ := resolveLWW(t, db, e2, a)
	assert.Equal(t, "Bob", val2, "ResolveLWW: entity2 should still have value")
}

// =============================================================================
// P8×S2-S7: Cache rebuild — full scenario coverage
// =============================================================================

// rebuildAndGetOneValue clears cache, rebuilds, returns OneValue
func cacheRebuildOneValue(t *testing.T, db *Database, e datalog.Identity, a datalog.Keyword) any {
	t.Helper()
	db.Cache().Clear()
	eStorage := entityFromIdentity(e)
	var aStorage Attribute
	copy(aStorage[:], a.String())
	key := CacheKey{E: eStorage, A: aStorage}
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(db.Schema())
	entry, _, err := db.Cache().GetOrResolve(key, matcher, nil, nil)
	require.NoError(t, err)
	if entry == nil {
		return nil
	}
	return entry.OneValue()
}

func TestCacheRemove_CacheRebuild_AfterOverwrite(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Remove(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	val := cacheRebuildOneValue(t, db, e, a)
	assert.Nil(t, val, "Cache rebuild: should be nil after overwrite then Remove")
}

func TestCacheRemove_CacheRebuild_ThenReAdd(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Add(e, a, "Bob"))
	_, err = tx3.Commit()
	require.NoError(t, err)

	val := cacheRebuildOneValue(t, db, e, a)
	assert.Equal(t, "Bob", val, "Cache rebuild: re-Add should win")
}

func TestCacheRemove_CacheRebuild_BeforeAnyAdd(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Remove(e, a, "phantom"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	val := cacheRebuildOneValue(t, db, e, a)
	assert.Equal(t, "Alice", val, "Cache rebuild: Add should win over earlier Remove")
}

func TestCacheRemove_CacheRebuild_VIsIrrelevant(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	val := cacheRebuildOneValue(t, db, e, a)
	assert.Nil(t, val, "Cache rebuild: V is irrelevant for CardinalityOne Remove")
}

func TestCacheRemove_CacheRebuild_MultipleEntities(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e1 := datalog.NewIdentity("alice")
	e2 := datalog.NewIdentity("bob")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e1, a, "Alice"))
	require.NoError(t, tx.Add(e2, a, "Bob"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e1, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	val1 := cacheRebuildOneValue(t, db, e1, a)
	assert.Nil(t, val1, "Cache rebuild: entity1 should be nil after Remove")

	val2 := cacheRebuildOneValue(t, db, e2, a)
	assert.Equal(t, "Bob", val2, "Cache rebuild: entity2 should still have value")
}

func TestCacheRemove_CacheRebuild_SetThenRemove(t *testing.T) {
	db, cleanup := createCacheRemoveTestDB(t, nil)
	defer cleanup()

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(e, a, "Alice"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(e, a, "Alice"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	val := cacheRebuildOneValue(t, db, e, a)
	assert.Nil(t, val, "Cache rebuild: should be nil after Set then Remove")
}

// =============================================================================
// P9×S2-S7: Stale cache invalidation — full scenario coverage
// =============================================================================

// pullName warms PullInto cache and returns the name value (or nil)
func warmCachePullIntoName(t *testing.T, db *Database, e datalog.Identity) *string {
	t.Helper()
	var p PersonOptionalName
	require.NoError(t, db.PullInto(e, &p))
	return p.Name
}

func TestCacheRemove_StaleInvalidation_AfterOverwrite(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(e, a, "Bob"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Warm cache
			name := warmCachePullIntoName(t, db, e)
			require.NotNil(t, name)

			// Remove
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Remove(e, a, "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Stale cache should be invalidated
			name = warmCachePullIntoName(t, db, e)
			assert.Nil(t, name, "Stale: should be nil after overwrite then Remove")
		})
	}
}

func TestCacheRemove_StaleInvalidation_ThenReAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Warm cache
			name := warmCachePullIntoName(t, db, e)
			require.NotNil(t, name)
			assert.Equal(t, "Alice", *name)

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Re-add
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Add(e, a, "Bob"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			name = warmCachePullIntoName(t, db, e)
			require.NotNil(t, name, "Stale: re-Add should win")
			assert.Equal(t, "Bob", *name)
		})
	}
}

func TestCacheRemove_StaleInvalidation_BeforeAnyAdd(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			// Remove first (cache is empty — no warm-up possible for this entity's name)
			tx := db.NewTransaction()
			require.NoError(t, tx.Remove(e, a, "phantom"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Warm cache (should see nothing or tombstone)
			name := warmCachePullIntoName(t, db, e)
			// Name might be nil or might show tombstone value depending on bug state

			// Add
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			name = warmCachePullIntoName(t, db, e)
			require.NotNil(t, name, "Stale: Add should win over earlier Remove")
			assert.Equal(t, "Alice", *name)
		})
	}
}

func TestCacheRemove_StaleInvalidation_VIsIrrelevant(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Warm cache
			name := warmCachePullIntoName(t, db, e)
			require.NotNil(t, name)
			assert.Equal(t, "Alice", *name)

			// Remove with different V
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Bob"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			name = warmCachePullIntoName(t, db, e)
			assert.Nil(t, name, "Stale: V is irrelevant for CardinalityOne Remove")
		})
	}
}

func TestCacheRemove_StaleInvalidation_MultipleEntities(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e1 := datalog.NewIdentity("alice")
			e2 := datalog.NewIdentity("bob")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(e1, a, "Alice"))
			require.NoError(t, tx.Add(e2, a, "Bob"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Warm cache for both
			n1 := warmCachePullIntoName(t, db, e1)
			require.NotNil(t, n1)
			n2 := warmCachePullIntoName(t, db, e2)
			require.NotNil(t, n2)

			// Remove only entity1
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e1, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			n1 = warmCachePullIntoName(t, db, e1)
			assert.Nil(t, n1, "Stale: entity1 should be nil after Remove")

			n2 = warmCachePullIntoName(t, db, e2)
			require.NotNil(t, n2, "Stale: entity2 should still have value")
			assert.Equal(t, "Bob", *n2)
		})
	}
}

func TestCacheRemove_StaleInvalidation_SetThenRemove(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := createCacheRemoveTestDB(t, &popts)
			defer cleanup()

			e := datalog.NewIdentity("alice")
			a := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e, a, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Warm cache
			name := warmCachePullIntoName(t, db, e)
			require.NotNil(t, name)
			assert.Equal(t, "Alice", *name)

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(e, a, "Alice"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			name = warmCachePullIntoName(t, db, e)
			assert.Nil(t, name, "Stale: should be nil after Set then Remove")
		})
	}
}
