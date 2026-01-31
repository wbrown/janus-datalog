package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Helper to create a temp database for matcher-cache tests
func createMatcherCacheTestDatabase(t *testing.T) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

func TestMatcherCacheCardinalityOne(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").
		Type(schema.TypeString).
		One().
		Add().
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

	// Use matcher with cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	// Verify cache returns correct LWW value
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityOne, entry.Cardinality())
	assert.Equal(t, "Alice", entry.OneValue())
}

func TestMatcherCacheCardinalityMany(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/tags").
		Type(schema.TypeString).
		Many().
		Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "veteran")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use matcher with cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	// Verify cache returns correct set
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/tags")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityMany, entry.Cardinality())
	assert.True(t, entry.ManySet()["warrior"])
	assert.True(t, entry.ManySet()["veteran"])
}

func TestMatcherCacheCardinalityVector(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":character/skills").
		Type(schema.TypeString).
		Vector().
		Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("character1")
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "stealth")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "archery")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use matcher with cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	// Verify cache returns correct vector
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":character/skills")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityVector, entry.Cardinality())
	assert.Equal(t, []any{"stealth", "archery"}, entry.VectorList())
}

func TestMatcherCacheConsistentWithDirectScan(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
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

	// Get value via cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	cachedValue := entry.OneValue()

	// Get value via direct lookup (LookupAttribute)
	directValue, found := matcher.LookupAttribute(e, datalog.NewKeyword(":person/name"))
	require.True(t, found)

	// Both should return the same value
	assert.Equal(t, cachedValue, directValue)
}

func TestMatcherCacheInvalidationOnWrite(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
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

	// Verify initial value via cache
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry1 := db.Cache().GetOrResolve(key, matcher)
	assert.Equal(t, "Alice", entry1.OneValue())

	// Update data
	tx2 := db.NewTransaction()
	err = tx2.Set(e, datalog.NewKeyword(":person/name"), "Bob")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Cache should be invalidated, next resolve should see new value
	entry2 := db.Cache().GetOrResolve(key, matcher)
	assert.Equal(t, "Bob", entry2.OneValue())
}

func TestMatcherCacheWithSchemalessAttribute(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// No schema set - should default to cardinality-one

	// Add data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("entity1")
	// Set works without schema (defaults to cardinality-one)
	err := tx.Set(e, datalog.NewKeyword(":unknown/attr"), "value")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use matcher without schema
	matcher := NewBadgerMatcher(db.Store())

	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":unknown/attr")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	// Should default to cardinality-one
	assert.Equal(t, schema.CardinalityOne, entry.Cardinality())
	assert.Equal(t, "value", entry.OneValue())
}

func TestMatcherCacheLWWResolution(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Write multiple values - highest ElementID should win
	tx1 := db.NewTransaction()
	e := datalog.NewIdentity("person1")
	err = tx1.Set(e, datalog.NewKeyword(":person/name"), "First")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	err = tx2.Set(e, datalog.NewKeyword(":person/name"), "Second")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	tx3 := db.NewTransaction()
	err = tx3.Set(e, datalog.NewKeyword(":person/name"), "Third")
	require.NoError(t, err)
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Cache should resolve to "Third" (highest ElementID)
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/name")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, "Third", entry.OneValue())
}

func TestMatcherCacheAddWinsResolution(t *testing.T) {
	db, cleanup := createMatcherCacheTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	e := datalog.NewIdentity("person1")

	// Add a tag
	tx1 := db.NewTransaction()
	err = tx1.Add(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Remove the tag
	tx2 := db.NewTransaction()
	err = tx2.Remove(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Add it back
	tx3 := db.NewTransaction()
	err = tx3.Add(e, datalog.NewKeyword(":person/tags"), "warrior")
	require.NoError(t, err)
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Cache should resolve to "warrior" being in set (add-wins with latest add)
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	eBytes := Entity(e.Hash())
	var aBytes Attribute
	copy(aBytes[:], ":person/tags")
	key := CacheKey{E: eBytes, A: aBytes}

	entry := db.Cache().GetOrResolve(key, matcher)
	require.NotNil(t, entry)
	assert.True(t, entry.ManySet()["warrior"], "warrior should be in set after add-remove-add")
}
