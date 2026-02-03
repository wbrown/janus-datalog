package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Helper to create a temp database for warmup tests
func createWarmupTestDatabase(t *testing.T) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

func TestWarmCacheSingleAttribute(t *testing.T) {
	db, cleanup := createWarmupTestDatabase(t)
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

	// Add some data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	e2 := datalog.NewIdentity("person2")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Set(e2, datalog.NewKeyword(":person/name"), "Bob")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache to simulate restart
	db.Cache().Clear()

	// Warm cache
	err = db.WarmCache([]datalog.Keyword{datalog.NewKeyword(":person/name")})
	require.NoError(t, err)

	// Verify entries are cached
	e1Bytes := Entity(e1.Hash())
	var a Attribute
	copy(a[:], ":person/name")
	key1 := CacheKey{E: e1Bytes, A: a}

	e2Bytes := Entity(e2.Hash())
	key2 := CacheKey{E: e2Bytes, A: a}

	// The entries should be populated
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	entry1 := db.Cache().GetOrResolve(key1, matcher)
	entry2 := db.Cache().GetOrResolve(key2, matcher)

	require.NotNil(t, entry1)
	require.NotNil(t, entry2)
	assert.Equal(t, "Alice", entry1.OneValue())
	assert.Equal(t, "Bob", entry2.OneValue())
}

func TestWarmCacheMultipleAttributes(t *testing.T) {
	db, cleanup := createWarmupTestDatabase(t)
	defer cleanup()

	// Create schema with multiple attributes
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/age").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Set(e1, datalog.NewKeyword(":person/age"), int64(30))
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Warm both attributes
	err = db.WarmCache([]datalog.Keyword{
		datalog.NewKeyword(":person/name"),
		datalog.NewKeyword(":person/age"),
	})
	require.NoError(t, err)

	// Verify both are cached
	eBytes := Entity(e1.Hash())

	var nameAttr Attribute
	copy(nameAttr[:], ":person/name")
	var ageAttr Attribute
	copy(ageAttr[:], ":person/age")

	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	nameEntry := db.Cache().GetOrResolve(CacheKey{E: eBytes, A: nameAttr}, matcher)
	ageEntry := db.Cache().GetOrResolve(CacheKey{E: eBytes, A: ageAttr}, matcher)

	require.NotNil(t, nameEntry)
	require.NotNil(t, ageEntry)
	assert.Equal(t, "Alice", nameEntry.OneValue())
	assert.Equal(t, int64(30), ageEntry.OneValue())
}

func TestWarmCacheEmptyAttribute(t *testing.T) {
	db, cleanup := createWarmupTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Don't add any data - attribute is empty

	// Warm cache should not error on empty attribute
	err = db.WarmCache([]datalog.Keyword{datalog.NewKeyword(":person/name")})
	require.NoError(t, err)
}

func TestWarmCacheUpdatesAttrVersion(t *testing.T) {
	db, cleanup := createWarmupTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Warm cache
	err = db.WarmCache([]datalog.Keyword{datalog.NewKeyword(":person/name")})
	require.NoError(t, err)

	// Attribute freshness should now be tracked
	var a Attribute
	copy(a[:], ":person/name")
	fresh := db.Cache().IsAttributeFresh(a, db.Store())
	assert.True(t, fresh, "attribute should be fresh after warmup")
}

func TestWarmCacheIdempotent(t *testing.T) {
	db, cleanup := createWarmupTestDatabase(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Warm cache twice - should be safe
	err = db.WarmCache([]datalog.Keyword{datalog.NewKeyword(":person/name")})
	require.NoError(t, err)

	err = db.WarmCache([]datalog.Keyword{datalog.NewKeyword(":person/name")})
	require.NoError(t, err)

	// Verify data is still correct
	eBytes := Entity(e1.Hash())
	var a Attribute
	copy(a[:], ":person/name")

	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	entry := db.Cache().GetOrResolve(CacheKey{E: eBytes, A: a}, matcher)
	require.NotNil(t, entry)
	assert.Equal(t, "Alice", entry.OneValue())
}
