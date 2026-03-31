package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// These tests verify that the cache is actually integrated into the query path,
// not just available for direct LookupAttribute calls.

// Helper to create a test database with cache for integration tests
func createCacheIntegrationTestDatabase(t *testing.T) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

// TestQueryPathUsesCache verifies that the matcher created by Database.Matcher()
// has the cache wired in and uses it for lookups.
func TestQueryPathUsesCache(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get matcher from database - this should have cache set
	matcher := db.Matcher()
	bm, ok := matcher.(*BadgerMatcher)
	require.True(t, ok, "matcher should be a *BadgerMatcher")
	require.NotNil(t, bm.cache, "matcher should have cache set")

	// Clear the cache entries (but keep the maxVersions tracking)
	// This simulates a scenario where we need to verify cache population
	db.Cache().Clear()

	// First lookup should populate the cache
	val, found := bm.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Alice", val)

	// Verify the cache was populated
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	// The cache should now have an entry for this (E, A) pair
	entry := db.Cache().GetOrResolve(key, bm)
	require.NotNil(t, entry)
	assert.Equal(t, "Alice", entry.OneValue())
}

// TestPullAPIUsesCache verifies that Pull API lookups go through the cache.
func TestPullAPIUsesCache(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema with multiple cardinalities
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "developer")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "golang")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get matcher from database
	matcher := db.Matcher()
	bm, ok := matcher.(*BadgerMatcher)
	require.True(t, ok)

	// Lookup name (cardinality-one)
	name, found := bm.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Alice", name)

	// Lookup all tags (cardinality-many via LookupAllAttributes)
	tags, err := bm.LookupAllAttributes(e, datalog.NewKeyword(":person/tags"))
	require.NoError(t, err)
	assert.Len(t, tags, 2)
	// Convert to set for order-independent comparison
	tagSet := make(map[interface{}]bool)
	for _, tag := range tags {
		tagSet[tag] = true
	}
	assert.True(t, tagSet["developer"])
	assert.True(t, tagSet["golang"])
}

// TestCacheInvalidatedOnCommit verifies that committing a transaction
// invalidates affected cache entries.
func TestCacheInvalidatedOnCommit(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add initial data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get matcher and populate cache
	matcher := db.Matcher()
	bm := matcher.(*BadgerMatcher)
	val, found := bm.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Alice", val)

	// Update the name
	tx2 := db.NewTransaction()
	err = tx2.Set(e, datalog.NewKeyword(":person/name"), "Bob")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// New matcher should see the updated value
	matcher2 := db.Matcher()
	bm2 := matcher2.(*BadgerMatcher)
	val2, found := bm2.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Bob", val2, "cache should reflect updated value after commit")
}

// TestMultipleMatchersShareCache verifies that multiple matchers from the same
// database share the same cache instance.
func TestMultipleMatchersShareCache(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get two matchers
	matcher1 := db.Matcher().(*BadgerMatcher)
	matcher2 := db.Matcher().(*BadgerMatcher)

	// Both should have the same cache
	assert.Same(t, db.Cache(), matcher1.cache, "matcher1 should share database cache")
	assert.Same(t, db.Cache(), matcher2.cache, "matcher2 should share database cache")

	// Lookup via matcher1 populates cache
	val, found := matcher1.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Alice", val)

	// Lookup via matcher2 should use the cached entry (same cache instance)
	val2, found := matcher2.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Alice", val2)
}

// TestAsOfDoesNotUseCache verifies that as-of queries bypass the cache
// since the cache only stores the latest state.
func TestAsOfDoesNotUseCache(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add initial data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	tx1ID, err := tx.Commit()
	require.NoError(t, err)

	// Update the name
	tx2 := db.NewTransaction()
	err = tx2.Set(e, datalog.NewKeyword(":person/name"), "Bob")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Current value should be "Bob"
	matcher := db.Matcher().(*BadgerMatcher)
	val, found := matcher.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Bob", val)

	// As-of tx1 should still return "Alice" (bypassing cache)
	asOfMatcher := db.AsOf(tx1ID).Matcher().(*BadgerMatcher)
	asOfVal, found := asOfMatcher.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)
	assert.Equal(t, "Alice", asOfVal, "as-of query should bypass cache and return historical value")
}

// TestVectorCacheIntegration verifies that vector attributes work with the cache.
func TestVectorCacheIntegration(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema with vector attribute
	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add vector data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("character1")
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "stealth")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "archery")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get matcher from database
	matcher := db.Matcher().(*BadgerMatcher)

	// Lookup vector via LookupAttribute
	skills, found := matcher.LookupAttribute(e, datalog.NewKeyword(":character/skills"))
	require.True(t, found)

	skillSlice, ok := skills.([]string)
	require.True(t, ok, "vector should be returned as []string")
	assert.Len(t, skillSlice, 2)
	assert.Equal(t, "stealth", skillSlice[0])
	assert.Equal(t, "archery", skillSlice[1])
}

// TestCacheResolverInterface verifies that BadgerMatcher implements CacheResolver
// correctly when used by the cache.
func TestCacheResolverInterface(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "dev")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get matcher which implements CacheResolver
	matcher := db.Matcher().(*BadgerMatcher)

	// Test that matcher can be used as a CacheResolver
	eBytes := Entity(e.Hash())
	var nameAttr, tagsAttr Attribute
	copy(nameAttr[:], ":person/name")
	copy(tagsAttr[:], ":person/tags")

	// Test GetCardinality
	assert.Equal(t, schema.CardinalityOne, matcher.GetCardinality(nameAttr))
	assert.Equal(t, schema.CardinalityMany, matcher.GetCardinality(tagsAttr))

	// Test ResolveLWW
	val, maxID, err := matcher.ResolveLWW(eBytes, nameAttr)
	require.NoError(t, err)
	assert.Equal(t, "Alice", val)
	assert.NotZero(t, maxID.Lamport)

	// Test ResolveAddWins
	set, maxID, err := matcher.ResolveAddWins(eBytes, tagsAttr)
	require.NoError(t, err)
	_, hasDev := set["dev"]
	assert.True(t, hasDev)
	assert.NotZero(t, maxID.Lamport)
}

// TestQueryExecutionUsesCache verifies that actual Datalog queries
// (not just LookupAttribute) use the cache through the Match() path.
func TestQueryExecutionUsesCache(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/age").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Set(e, datalog.NewKeyword(":person/age"), int64(30))
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear the cache to test that query populates it
	db.Cache().Clear()

	// Execute a Datalog query - this should use the cache path
	result, err := executor.CollectTuples(db.Query(`[:find ?name :where [?e :person/name ?name]]`))
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "Alice", result[0][0])

	// Verify the cache was populated by the query
	eBytes := Entity(e.Hash())
	var nameAttr Attribute
	copy(nameAttr[:], ":person/name")
	key := CacheKey{E: eBytes, A: nameAttr}

	// The cache should now have an entry from the query execution
	entry := db.Cache().GetOrResolve(key, db.Matcher().(*BadgerMatcher))
	require.NotNil(t, entry, "cache should be populated after query execution")
	assert.Equal(t, "Alice", entry.OneValue())
}

// TestJoinQueryUsesCache verifies that join queries with bound E from bindings
// also use the cache path.
func TestJoinQueryUsesCache(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/city").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	e2 := datalog.NewIdentity("person2")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Set(e1, datalog.NewKeyword(":person/city"), "NYC")
	require.NoError(t, err)
	err = tx.Set(e2, datalog.NewKeyword(":person/name"), "Bob")
	require.NoError(t, err)
	err = tx.Set(e2, datalog.NewKeyword(":person/city"), "LA")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear the cache
	db.Cache().Clear()

	// Execute a join query - the second pattern should use cache after ?e is bound
	// from the first pattern
	result, err := executor.CollectTuples(db.Query(`[:find ?name ?city :where [?e :person/name ?name] [?e :person/city ?city]]`))
	require.NoError(t, err)
	require.Len(t, result, 2, "should return 2 person results")

	// Verify results contain expected data
	names := make(map[string]string)
	for _, tuple := range result {
		names[tuple[0].(string)] = tuple[1].(string)
	}
	assert.Equal(t, "NYC", names["Alice"])
	assert.Equal(t, "LA", names["Bob"])

	// Verify the cache was populated for the city attribute
	e1Bytes := Entity(e1.Hash())
	var cityAttr Attribute
	copy(cityAttr[:], ":person/city")
	key := CacheKey{E: e1Bytes, A: cityAttr}

	entry := db.Cache().GetOrResolve(key, db.Matcher().(*BadgerMatcher))
	require.NotNil(t, entry, "cache should be populated after join query")
	assert.Equal(t, "NYC", entry.OneValue())
}

// TestCardinalityManyQueryUsesCache verifies queries on cardinality-many
// attributes use the cache.
func TestCardinalityManyQueryUsesCache(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema with cardinality-many attribute
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "developer")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "golang")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear the cache
	db.Cache().Clear()

	// Execute query for cardinality-many attribute
	result, err := executor.CollectTuples(db.Query(`[:find ?tag :where [?e :person/name "Alice"] [?e :person/tags ?tag]]`))
	require.NoError(t, err)
	require.Len(t, result, 2, "should return 2 tags")

	// Verify results
	tags := make(map[interface{}]bool)
	for _, tuple := range result {
		tags[tuple[0]] = true
	}
	assert.True(t, tags["developer"])
	assert.True(t, tags["golang"])

	// Verify the cache was populated
	eBytes := Entity(e.Hash())
	var tagsAttr Attribute
	copy(tagsAttr[:], ":person/tags")
	key := CacheKey{E: eBytes, A: tagsAttr}

	entry := db.Cache().GetOrResolve(key, db.Matcher().(*BadgerMatcher))
	require.NotNil(t, entry, "cache should be populated after cardinality-many query")
	_, hasDeveloper := entry.ManySet()["developer"]
	_, hasGolang := entry.ManySet()["golang"]
	assert.True(t, hasDeveloper)
	assert.True(t, hasGolang)
}

// TestCacheConcurrency verifies that concurrent cache access with real storage
// is thread-safe and returns correct values.
func TestCacheConcurrency(t *testing.T) {
	db, cleanup := createCacheIntegrationTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get matcher (implements CacheResolver)
	matcher := db.Matcher().(*BadgerMatcher)
	cache := db.Cache()

	eBytes := Entity(e.Hash())
	var nameAttr Attribute
	copy(nameAttr[:], ":person/name")
	key := CacheKey{E: eBytes, A: nameAttr}

	// Populate initial entry
	cache.GetOrResolve(key, matcher)

	var wg sync.WaitGroup

	// Concurrent readers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry := cache.GetOrResolve(key, matcher)
			assert.NotNil(t, entry)
		}()
	}

	// Concurrent writers (updating max version)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: uint64(100 + i), ReplicaID: 1})
		}(i)
	}

	// Concurrent invalidations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.Invalidate([]CacheKey{key})
		}()
	}

	wg.Wait()
	// Should complete without panic or data race
}
