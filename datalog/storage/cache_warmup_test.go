package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// createWarmupTestDatabase creates a database on the mode's backend for
// warmup tests.
func createWarmupTestDatabase(t *testing.T, mode optimizerMode) *Database {
	return createOptimizerModeDB(t, mode, DatabaseOptions{})
}

func TestWarmCacheSingleAttribute(t *testing.T) {
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := createWarmupTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").
				Type(schema.TypeString).
				One().
				Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add some data
			name := datalog.NewKeyword(":person/name")
			tx := db.NewTransaction()
			e1 := datalog.NewIdentity("person1")
			e2 := datalog.NewIdentity("person2")
			err = tx.Set(e1, name, "Alice")
			require.NoError(t, err)
			err = tx.Set(e2, name, "Bob")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Clear cache to simulate restart
			db.Cache().Clear()

			// Warm cache
			err = db.WarmCache([]datalog.Keyword{name})
			require.NoError(t, err)

			// Verify entries are cached
			key1 := CacheKey{E: e1, A: name}
			key2 := CacheKey{E: e2, A: name}

			// The entries should be populated
			matcher := NewPatternMatcher(db.Store())
			matcher.SetSchema(s)
			entry1, err1 := db.Cache().GetOrResolve(key1, s.GetAttribute(name), matcher, db.Cache().ReadPoint(), nil, DiscardIntake)
			require.NoError(t, err1)
			entry2, err2 := db.Cache().GetOrResolve(key2, s.GetAttribute(name), matcher, db.Cache().ReadPoint(), nil, DiscardIntake)
			require.NoError(t, err2)

			require.NotNil(t, entry1)
			require.NotNil(t, entry2)
			assert.Equal(t, "Alice", entry1.OneValue())
			assert.Equal(t, "Bob", entry2.OneValue())
		})
	}
}

func TestWarmCacheMultipleAttributes(t *testing.T) {
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := createWarmupTestDatabase(t, mode)

			// Create schema with multiple attributes
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/age").Type(schema.TypeLong).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			name, age := datalog.NewKeyword(":person/name"), datalog.NewKeyword(":person/age")
			tx := db.NewTransaction()
			e1 := datalog.NewIdentity("person1")
			err = tx.Set(e1, name, "Alice")
			require.NoError(t, err)
			err = tx.Set(e1, age, int64(30))
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Clear cache
			db.Cache().Clear()

			// Warm both attributes
			err = db.WarmCache([]datalog.Keyword{name, age})
			require.NoError(t, err)

			// Verify both are cached
			matcher := NewPatternMatcher(db.Store())
			matcher.SetSchema(s)

			nameEntry, nameErr := db.Cache().GetOrResolve(CacheKey{E: e1, A: name}, s.GetAttribute(name), matcher, db.Cache().ReadPoint(), nil, DiscardIntake)
			require.NoError(t, nameErr)
			ageEntry, ageErr := db.Cache().GetOrResolve(CacheKey{E: e1, A: age}, s.GetAttribute(age), matcher, db.Cache().ReadPoint(), nil, DiscardIntake)
			require.NoError(t, ageErr)

			require.NotNil(t, nameEntry)
			require.NotNil(t, ageEntry)
			assert.Equal(t, "Alice", nameEntry.OneValue())
			assert.Equal(t, int64(30), ageEntry.OneValue())
		})
	}
}

func TestWarmCacheEmptyAttribute(t *testing.T) {
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := createWarmupTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Don't add any data - attribute is empty

			// Warm cache should not error on empty attribute
			name := datalog.NewKeyword(":person/name")
			require.NoError(t, db.WarmCache([]datalog.Keyword{name}))

			// The entries warming resolved for the attribute: none while no
			// entity holds it, and one once an entity does.
			warmed := func() int {
				n := 0
				db.Cache().slots.Range(func(key CacheKey, slot cacheSlot) bool {
					if key.A == name && slot.entry != nil {
						n++
					}
					return true
				})
				return n
			}
			assert.Equal(t, 0, warmed(), "warming an attribute no entity holds caches nothing")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(datalog.NewIdentity("person1"), name, "Alice"))
			_, err = tx.Commit()
			require.NoError(t, err)
			require.NoError(t, db.WarmCache([]datalog.Keyword{name}))
			assert.Equal(t, 1, warmed(), "warming caches the one entity that holds the attribute")
		})
	}
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
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{DisableCache: true})

			alice := datalog.NewIdentity("person:alice")
			name := datalog.NewKeyword(":person/name")
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			require.NoError(t, db.WarmCache([]datalog.Keyword{name}),
				"warming a database that has no cache is a no-op, not a failure")

			// The data is still readable — warming was skipped, not the read path.
			resolved, err := db.ResolveEntityAttributes(alice, []datalog.Keyword{name})
			require.NoError(t, err)
			assert.Equal(t, "Alice", resolved[name])
		})
	}
}

func TestWarmCacheIdempotent(t *testing.T) {
	for _, mode := range legsFor(optimizerModes) {
		t.Run(mode.name, func(t *testing.T) {
			db := createWarmupTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			name := datalog.NewKeyword(":person/name")
			tx := db.NewTransaction()
			e1 := datalog.NewIdentity("person1")
			err = tx.Set(e1, name, "Alice")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Warm cache twice - should be safe
			err = db.WarmCache([]datalog.Keyword{name})
			require.NoError(t, err)

			err = db.WarmCache([]datalog.Keyword{name})
			require.NoError(t, err)

			// Verify data is still correct
			matcher := NewPatternMatcher(db.Store())
			matcher.SetSchema(s)

			entry, err := db.Cache().GetOrResolve(CacheKey{E: e1, A: name}, s.GetAttribute(name), matcher, db.Cache().ReadPoint(), nil, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, "Alice", entry.OneValue())
		})
	}
}
