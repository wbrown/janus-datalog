package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// createWarmupTestDB creates a temp database for warmup tests
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
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	entry1, err1 := db.Cache().GetOrResolve(key1, matcher, nil, nil, DiscardIntake)
	require.NoError(t, err1)
	entry2, err2 := db.Cache().GetOrResolve(key2, matcher, nil, nil, DiscardIntake)
	require.NoError(t, err2)

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

	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)

	nameEntry, nameErr := db.Cache().GetOrResolve(CacheKey{E: eBytes, A: nameAttr}, matcher, nil, nil, DiscardIntake)
	require.NoError(t, nameErr)
	ageEntry, ageErr := db.Cache().GetOrResolve(CacheKey{E: eBytes, A: ageAttr}, matcher, nil, nil, DiscardIntake)
	require.NoError(t, ageErr)

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

// TestWarmCacheOnCacheDisabledDatabase pins the one caller in database.go that
// reaches d.cache without asking whether there is one.
//
// DisableCache leaves d.cache nil, and six siblings in the same file test for
// that before touching it — the cache is an optimization, so each resolves from
// storage instead. WarmCache had no such test and called GetOrResolve straight
// through, which dereferences c.slots on the nil receiver.
//
// Warming has no storage fallback to take: with no cache there is nothing to
// warm, so the call succeeds having done nothing. That is the honest answer for
// an optimization the caller has switched off, and it keeps WarmCache callable
// from setup code that does not know how the database was opened.
func TestWarmCacheOnCacheDisabledDatabase(t *testing.T) {
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         t.TempDir(),
		DisableCache: true,
	})
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("person:alice")
	name := datalog.NewKeyword(":person/name")
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, name, "Alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, db.WarmCache([]datalog.Keyword{name}),
		"warming a database that has no cache is a no-op, not a failure")

	// The data is still readable — warming was skipped, not the read path.
	resolved, err := db.ResolveEntityAttributes(alice, []datalog.Keyword{name})
	require.NoError(t, err)
	assert.Equal(t, "Alice", resolved[name])
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

	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)

	entry, err := db.Cache().GetOrResolve(CacheKey{E: eBytes, A: a}, matcher, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "Alice", entry.OneValue())
}
