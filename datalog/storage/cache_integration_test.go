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

// createCacheIntegrationTestDatabase creates a database on the mode's backend
// with cache for integration tests.
func createCacheIntegrationTestDatabase(t *testing.T, mode optimizerMode) *Database {
	return createOptimizerModeDB(t, mode, DatabaseOptions{})
}

// TestQueryPathUsesCache verifies that the matcher created by Database.Matcher()
// has the cache wired in and uses it for lookups.
func TestQueryPathUsesCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			name := datalog.NewKeyword(":person/name")
			tx := db.NewTransaction()
			e := datalog.NewIdentity("person1")
			err = tx.Set(e, name, "Alice")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Get matcher from database - this should have cache set
			matcher := db.Matcher()
			bm, ok := matcher.(*PatternMatcher)
			require.True(t, ok, "matcher should be a *PatternMatcher")
			require.NotNil(t, bm.cache, "matcher should have cache set")

			// Clear the cache entries (but keep the maxVersions tracking)
			// This simulates a scenario where we need to verify cache population
			db.Cache().Clear()

			// First lookup should populate the cache
			val, found := requireAttributeLookup(t, bm, e, name)
			require.True(t, found)
			assert.Equal(t, "Alice", val)

			// Verify the cache was populated
			eBytes := Entity(e.Hash())
			var aBytes Attribute
			copy(aBytes[:], name.String())
			key := CacheKey{E: eBytes, A: aBytes}

			// The cache should now have an entry for this (E, A) pair
			entry, err := db.Cache().GetOrResolve(key, bm.GetCardinality(name), bm, nil, nil, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, "Alice", entry.OneValue())
		})
	}
}

// TestPullAPIUsesCache verifies that Pull API lookups go through the cache.
func TestPullAPIUsesCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema with multiple cardinalities
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

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
			bm, ok := matcher.(*PatternMatcher)
			require.True(t, ok)

			// Lookup name (cardinality-one)
			name, found := requireAttributeLookup(t, bm, e, datalog.NewKeyword(":person/name"))
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
		})
	}
}

// TestCacheInvalidatedOnCommit verifies that committing a transaction
// invalidates affected cache entries.
func TestCacheInvalidatedOnCommit(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add initial data
			tx := db.NewTransaction()
			e := datalog.NewIdentity("person1")
			err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Get matcher and populate cache
			matcher := db.Matcher()
			bm := matcher.(*PatternMatcher)
			val, found := requireAttributeLookup(t, bm, e, datalog.NewKeyword(":person/name"))
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
			bm2 := matcher2.(*PatternMatcher)
			val2, found := requireAttributeLookup(t, bm2, e, datalog.NewKeyword(":person/name"))
			require.True(t, found)
			assert.Equal(t, "Bob", val2, "cache should reflect updated value after commit")
		})
	}
}

// TestMultipleMatchersShareCache verifies that multiple matchers from the same
// database share the same cache instance.
func TestMultipleMatchersShareCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			tx := db.NewTransaction()
			e := datalog.NewIdentity("person1")
			err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Get two matchers
			matcher1 := db.Matcher().(*PatternMatcher)
			matcher2 := db.Matcher().(*PatternMatcher)

			// Both should have the same cache
			assert.Same(t, db.Cache(), matcher1.cache, "matcher1 should share database cache")
			assert.Same(t, db.Cache(), matcher2.cache, "matcher2 should share database cache")

			// Lookup via matcher1 populates cache
			val, found := requireAttributeLookup(t, matcher1, e, datalog.NewKeyword(":person/name"))
			require.True(t, found)
			assert.Equal(t, "Alice", val)

			// Lookup via matcher2 should use the cached entry (same cache instance)
			val2, found := requireAttributeLookup(t, matcher2, e, datalog.NewKeyword(":person/name"))
			require.True(t, found)
			assert.Equal(t, "Alice", val2)
		})
	}
}

// TestAsOfDoesNotUseCache verifies that as-of queries bypass the cache
// since the cache only stores the latest state.
func TestAsOfDoesNotUseCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

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
			matcher := db.Matcher().(*PatternMatcher)
			val, found := requireAttributeLookup(t, matcher, e, datalog.NewKeyword(":person/name"))
			require.True(t, found)
			assert.Equal(t, "Bob", val)

			// As-of tx1 should still return "Alice" (bypassing cache)
			asOfMatcher := db.AsOf(tx1ID).Matcher().(*PatternMatcher)
			asOfVal, found := requireAttributeLookup(t, asOfMatcher, e, datalog.NewKeyword(":person/name"))
			require.True(t, found)
			assert.Equal(t, "Alice", asOfVal, "as-of query should bypass cache and return historical value")
		})
	}
}

// TestVectorCacheIntegration verifies that vector attributes work with the cache.
func TestVectorCacheIntegration(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema with vector attribute
			s, err := schema.NewBuilder().
				Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

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
			matcher := db.Matcher().(*PatternMatcher)

			// Lookup vector via LookupAttribute
			skills, found := requireAttributeLookup(t, matcher, e, datalog.NewKeyword(":character/skills"))
			require.True(t, found)

			skillSlice, ok := skills.([]string)
			require.True(t, ok, "vector should be returned as []string")
			assert.Len(t, skillSlice, 2)
			assert.Equal(t, "stealth", skillSlice[0])
			assert.Equal(t, "archery", skillSlice[1])
		})
	}
}

// TestCacheResolverInterface verifies that PatternMatcher implements CacheResolver
// correctly when used by the cache.
func TestCacheResolverInterface(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			name, tags := datalog.NewKeyword(":person/name"), datalog.NewKeyword(":person/tags")
			tx := db.NewTransaction()
			e := datalog.NewIdentity("person1")
			err = tx.Set(e, name, "Alice")
			require.NoError(t, err)
			err = tx.Add(e, tags, "dev")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Get matcher which implements CacheResolver
			matcher := db.Matcher().(*PatternMatcher)

			// Test that matcher can be used as a CacheResolver
			eBytes := Entity(e.Hash())
			var nameAttr, tagsAttr Attribute
			copy(nameAttr[:], name.String())
			copy(tagsAttr[:], tags.String())

			// Test GetCardinality
			assert.Equal(t, schema.CardinalityOne, matcher.GetCardinality(name))
			assert.Equal(t, schema.CardinalityMany, matcher.GetCardinality(tags))

			// Test ResolveLWW
			lww := &scanReport{}
			val, maxID, present, err := matcher.ResolveLWW(eBytes, nameAttr, lww)
			require.NoError(t, err)
			assert.True(t, present, "the (E, A) carries datoms, so it is present")
			assert.Equal(t, "Alice", val)
			assert.NotZero(t, maxID.Lamport)
			assert.Positive(t, lww.scanned, "resolution read the index and must report it")

			// Test ResolveAddWins
			addWins := &scanReport{}
			set, maxID, present, err := matcher.ResolveAddWins(eBytes, tagsAttr, addWins)
			require.NoError(t, err)
			assert.True(t, present, "the (E, A) carries datoms, so it is present")
			_, hasDev := set["dev"]
			assert.True(t, hasDev)
			assert.NotZero(t, maxID.Lamport)
			assert.Positive(t, addWins.scanned, "resolution read the index and must report it")
		})
	}
}

// TestQueryExecutionUsesCache verifies that actual Datalog queries
// (not just LookupAttribute) use the cache through the Match() path.
func TestQueryExecutionUsesCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/age").Type(schema.TypeLong).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			name := datalog.NewKeyword(":person/name")
			tx := db.NewTransaction()
			e := datalog.NewIdentity("person1")
			err = tx.Set(e, name, "Alice")
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
			copy(nameAttr[:], name.String())
			key := CacheKey{E: eBytes, A: nameAttr}

			// The cache should now have an entry from the query execution
			matcher := db.Matcher().(*PatternMatcher)
			entry, err := db.Cache().GetOrResolve(key, matcher.GetCardinality(name), matcher, nil, nil, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry, "cache should be populated after query execution")
			assert.Equal(t, "Alice", entry.OneValue())
		})
	}
}

// TestJoinQueryUsesCache verifies that join queries with bound E from bindings
// also use the cache path.
func TestJoinQueryUsesCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/city").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			name, city := datalog.NewKeyword(":person/name"), datalog.NewKeyword(":person/city")
			tx := db.NewTransaction()
			e1 := datalog.NewIdentity("person1")
			e2 := datalog.NewIdentity("person2")
			err = tx.Set(e1, name, "Alice")
			require.NoError(t, err)
			err = tx.Set(e1, city, "NYC")
			require.NoError(t, err)
			err = tx.Set(e2, name, "Bob")
			require.NoError(t, err)
			err = tx.Set(e2, city, "LA")
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
			copy(cityAttr[:], city.String())
			key := CacheKey{E: e1Bytes, A: cityAttr}

			matcher := db.Matcher().(*PatternMatcher)
			entry, err := db.Cache().GetOrResolve(key, matcher.GetCardinality(city), matcher, nil, nil, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry, "cache should be populated after join query")
			assert.Equal(t, "NYC", entry.OneValue())
		})
	}
}

// TestCardinalityManyQueryUsesCache verifies queries on cardinality-many
// attributes use the cache.
func TestCardinalityManyQueryUsesCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create schema with cardinality-many attribute
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			tags := datalog.NewKeyword(":person/tags")
			tx := db.NewTransaction()
			e := datalog.NewIdentity("person1")
			err = tx.Set(e, datalog.NewKeyword(":person/name"), "Alice")
			require.NoError(t, err)
			err = tx.Add(e, tags, "developer")
			require.NoError(t, err)
			err = tx.Add(e, tags, "golang")
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
			found := make(map[interface{}]bool)
			for _, tuple := range result {
				found[tuple[0]] = true
			}
			assert.True(t, found["developer"])
			assert.True(t, found["golang"])

			// Verify the cache was populated
			eBytes := Entity(e.Hash())
			var tagsAttr Attribute
			copy(tagsAttr[:], tags.String())
			key := CacheKey{E: eBytes, A: tagsAttr}

			matcher := db.Matcher().(*PatternMatcher)
			entry, err := db.Cache().GetOrResolve(key, matcher.GetCardinality(tags), matcher, nil, nil, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry, "cache should be populated after cardinality-many query")
			_, hasDeveloper := entry.ManySet()["developer"]
			_, hasGolang := entry.ManySet()["golang"]
			assert.True(t, hasDeveloper)
			assert.True(t, hasGolang)
		})
	}
}

// TestCardinalityManyUnboundScanUsesCache: a pattern over a cardinality-many
// attribute with the entity open answers every entity's current set, and
// resolves each entity's set through the cache: after the scan every entity's
// set is cached, so the next scan reads no set from storage. A member
// withdrawn and a member added after the scan are what the next scan answers.
func TestCardinalityManyUnboundScanUsesCache(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			s, err := schema.NewBuilder().
				Attribute(":person/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			tags := datalog.NewKeyword(":person/tags")
			alice := datalog.NewIdentity("person1")
			bob := datalog.NewIdentity("person2")
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(alice, tags, "developer"))
			require.NoError(t, tx.Add(alice, tags, "golang"))
			require.NoError(t, tx.Add(bob, tags, "designer"))
			_, err = tx.Commit()
			require.NoError(t, err)

			const scan = `[:find ?e ?tag :where [?e :person/tags ?tag]]`
			sets := func() map[datalog.Identity]map[interface{}]bool {
				tuples, err := executor.CollectTuples(db.Query(scan))
				require.NoError(t, err)
				answered := map[datalog.Identity]map[interface{}]bool{}
				for _, tuple := range tuples {
					e := tuple[0].(datalog.Identity)
					if answered[e] == nil {
						answered[e] = map[interface{}]bool{}
					}
					answered[e][tuple[1]] = true
				}
				return answered
			}

			require.Equal(t, map[datalog.Identity]map[interface{}]bool{
				alice: {"developer": true, "golang": true},
				bob:   {"designer": true},
			}, sets(), "the scan answers every entity's current set")

			var tagsAttr Attribute
			copy(tagsAttr[:], tags.String())
			for _, e := range []datalog.Identity{alice, bob} {
				assert.True(t, db.Cache().GetCachedAttrs(Entity(e.Hash()))[tagsAttr],
					"the scan resolves %s's set through the cache, so the set is cached after it", e)
			}

			tx = db.NewTransaction()
			require.NoError(t, tx.Remove(alice, tags, "developer"))
			require.NoError(t, tx.Add(bob, tags, "writer"))
			_, err = tx.Commit()
			require.NoError(t, err)

			require.Equal(t, map[datalog.Identity]map[interface{}]bool{
				alice: {"golang": true},
				bob:   {"designer": true, "writer": true},
			}, sets(), "the next scan answers the sets as the commit left them")
		})
	}
}

// TestCacheConcurrency verifies that concurrent cache access with real storage
// is thread-safe and returns correct values.
func TestCacheConcurrency(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createCacheIntegrationTestDatabase(t, mode)

			// Create schema
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, db.SetSchema(s))

			// Add data
			name := datalog.NewKeyword(":person/name")
			tx := db.NewTransaction()
			e := datalog.NewIdentity("person1")
			err = tx.Set(e, name, "Alice")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Get matcher (implements CacheResolver)
			matcher := db.Matcher().(*PatternMatcher)
			cache := db.Cache()

			eBytes := Entity(e.Hash())
			var nameAttr Attribute
			copy(nameAttr[:], name.String())
			key := CacheKey{E: eBytes, A: nameAttr}

			// Populate initial entry
			_, err = cache.GetOrResolve(key, matcher.GetCardinality(name), matcher, nil, nil, DiscardIntake)
			require.NoError(t, err)

			var wg sync.WaitGroup

			// Concurrent readers
			for i := 0; i < 50; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					entry, err := cache.GetOrResolve(key, matcher.GetCardinality(name), matcher, nil, nil, DiscardIntake)
					assert.NoError(t, err)
					assert.NotNil(t, entry)
					assert.Equal(t, "Alice", entry.OneValue(), "every reader is served the value the store holds")
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
		})
	}
}
