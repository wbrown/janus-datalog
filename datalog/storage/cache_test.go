package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The cache resolves an (E, A) through a CacheResolver, and the resolver is
// the pattern matcher over the database's store. Each test writes the datoms
// its case needs through the database, resolves them through the matcher into
// the cache under test, and reads the entry back.

// cacheFixture is a database on the mode's backend declaring :person/name
// (one), :person/tags (many) and :character/skills (vector), a matcher over
// its store, and the entity the tests write.
type cacheFixture struct {
	db      *Database
	matcher *PatternMatcher
	entity  datalog.Identity
	name    datalog.Keyword
	tags    datalog.Keyword
	skills  datalog.Keyword
}

func newCacheFixture(t *testing.T, mode optimizerMode) *cacheFixture {
	t.Helper()
	db := createOptimizerModeDB(t, mode, DatabaseOptions{})
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	require.NoError(t, db.SetSchema(s))
	return &cacheFixture{
		db:      db,
		matcher: db.Matcher().(*PatternMatcher),
		entity:  datalog.NewIdentity("person1"),
		name:    datalog.NewKeyword(":person/name"),
		tags:    datalog.NewKeyword(":person/tags"),
		skills:  datalog.NewKeyword(":character/skills"),
	}
}

// key is the cache key of the fixture's entity under attr.
func (f *cacheFixture) key(attr datalog.Keyword) CacheKey {
	var a Attribute
	copy(a[:], attr.String())
	return CacheKey{E: Entity(f.entity.Hash()), A: a}
}

// set writes value as the current value of attr on the entity.
func (f *cacheFixture) set(t *testing.T, attr datalog.Keyword, value string) {
	t.Helper()
	tx := f.db.NewTransaction()
	require.NoError(t, tx.Set(f.entity, attr, value))
	_, err := tx.Commit()
	require.NoError(t, err)
}

// add adds values to attr on the entity, in order, in one transaction.
func (f *cacheFixture) add(t *testing.T, attr datalog.Keyword, values ...string) {
	t.Helper()
	tx := f.db.NewTransaction()
	for _, value := range values {
		require.NoError(t, tx.Add(f.entity, attr, value))
	}
	_, err := tx.Commit()
	require.NoError(t, err)
}

// remove removes value from attr on the entity.
func (f *cacheFixture) remove(t *testing.T, attr datalog.Keyword, value string) {
	t.Helper()
	tx := f.db.NewTransaction()
	require.NoError(t, tx.Remove(f.entity, attr, value))
	_, err := tx.Commit()
	require.NoError(t, err)
}

// resolve resolves attr on the entity through the fixture's matcher into
// cache, accruing the intake into report.
func (f *cacheFixture) resolve(cache *Cache, attr datalog.Keyword, report *scanReport) (*CacheEntry, error) {
	return cache.GetOrResolve(f.key(attr), f.matcher.GetCardinality(attr), f.matcher, nil, nil, report)
}

// version is the ElementID of the datom the matcher resolves attr on the
// entity to under last-writer-wins, the version a writer reports to the cache
// through UpdateMaxVersion.
func (f *cacheFixture) version(t *testing.T, attr datalog.Keyword) datalog.ElementID {
	t.Helper()
	key := f.key(attr)
	_, maxID, present, err := f.matcher.ResolveLWW(key.E, key.A, DiscardIntake)
	require.NoError(t, err)
	require.True(t, present, "the entity carries %s", attr)
	return maxID
}

// TestCacheEntryCarriesResolutionIntake pins that the index intake a resolution
// spends reaches the entry it built, for all three cardinalities, and that the
// intake GetOrResolve returns is what *that call* read.
//
// The two are different numbers and both are needed. entry.scanned is what the
// entry cost to build and stays with it for the entry's life, so a later reader
// of the same entry can say what it cost. The returned intake is per-call: the
// build cost when this call built it, zero when it came from cache, because a
// hit reads no index. Reporting entry.scanned on a hit would make a trace of a
// thousand hits claim a thousand index reads that never happened.
func TestCacheEntryCarriesResolutionIntake(t *testing.T) {
	for _, mode := range optimizerModes {
		// want is the keys the resolution takes from the index: under
		// cardinality one the group's first key, the current value, and under
		// many and vector the whole group, one key per datom written.
		for _, tc := range []struct {
			name  string
			write func(t *testing.T, f *cacheFixture) datalog.Keyword
			want  int
		}{
			{"one", func(t *testing.T, f *cacheFixture) datalog.Keyword {
				f.set(t, f.name, "Alice")
				f.set(t, f.name, "Alicia")
				return f.name
			}, 1},
			{"many", func(t *testing.T, f *cacheFixture) datalog.Keyword {
				f.add(t, f.tags, "dev", "ops", "qa")
				return f.tags
			}, 3},
			{"vector", func(t *testing.T, f *cacheFixture) datalog.Keyword {
				f.add(t, f.skills, "a", "b", "c")
				return f.skills
			}, 3},
		} {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				f := newCacheFixture(t, mode)
				attr := tc.write(t, f)

				cache := NewCache()
				build := &scanReport{}
				entry, err := f.resolve(cache, attr, build)
				require.NoError(t, err)
				require.NotNil(t, entry)
				require.Equal(t, tc.want, build.scanned, "the call that built the entry spent the build cost")
				require.Equal(t, tc.want, entry.scanned,
					"the entry must carry what its resolution read")

				// A hit rebuilds nothing, so it reads no index: it accrues nothing,
				// while the entry keeps the cost of the build it came from.
				onHit := &scanReport{}
				hit, err := f.resolve(cache, attr, onHit)
				require.NoError(t, err)
				require.Same(t, entry, hit, "second call must be a hit, not a rebuild")
				require.Equal(t, 0, onHit.scanned, "a hit reads no index")
				require.Equal(t, build.scanned, hit.scanned,
					"the entry still carries what building it read")
			})
		}
	}
}

// An entry resolved under one cardinality is not a hit for a reader resolving
// under another: after a declaration changes from a set to cardinality one, a
// reader under cardinality one is served the latest member, never the set's
// view, whatever reader resolved the pair before.
func TestCacheEntryUnderAnotherCardinalityIsNotAHit(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.add(t, f.tags, "x")
			f.add(t, f.tags, "y")
			asSet := f.matcher
			asOneSchema, err := schema.NewBuilder().
				Attribute(":person/tags").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			require.NoError(t, f.db.SetSchema(asOneSchema))
			asOne := f.db.Matcher().(*PatternMatcher)
			key := f.key(f.tags)

			cache := NewCache()
			set, err := cache.GetOrResolve(key, asSet.GetCardinality(f.tags), asSet, nil, nil, DiscardIntake)
			require.NoError(t, err)
			require.Same(t, schema.CardinalityMany, set.Cardinality())
			require.Len(t, set.ManySet(), 2)

			one, err := cache.GetOrResolve(key, asOne.GetCardinality(f.tags), asOne, nil, nil, DiscardIntake)
			require.NoError(t, err)
			require.Same(t, schema.CardinalityOne, one.Cardinality(), "a reader under cardinality one is served no set's view")
			require.Equal(t, "y", one.OneValue(), "the reader resolved the pair under its own cardinality")
		})
	}
}

// TestCacheEntryValueCountsWhatResolutionProduced pins what a cache-resolved
// pattern reports as values.served: the values this entry hands the pattern,
// counted in the unit the pattern binds.
//
// The absent cardinality-one case is the one worth pinning: a never-set (E, A)
// and one whose highest-Tx entry is a tombstone both resolve to no value, and
// counting either as one would make a tombstoned attribute read in the stream
// exactly like a present one.
//
// The vector cases are the other. A cardinality-vector entry serves **one**
// value however many elements the vector holds — the pattern binds V to the
// whole vector and matches once — so a three-element vector reporting three
// served against one matched would render as two values filtered out by a
// pattern that filtered nothing. Elements are the vector's contents, not
// separate values; if a reader wants that count it is a different key, and
// nothing asks for it.
//
// An empty vectorList serves one value for the same reason: it is a *cleared*
// vector, and the empty vector is a value. There is no never-set entry for it
// to be mistaken for, because an absent (E, A) has no entry at all — which is
// what makes the count answerable here. It was 0 while the entry could stand
// for either state and the arm reading it guessed absence.
func TestCacheEntryValueCountsWhatResolutionProduced(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry *CacheEntry
		want  int
	}{
		{"one", &CacheEntry{cardinality: schema.CardinalityOne, oneValue: "Alice"}, 1},
		{"one absent", &CacheEntry{cardinality: schema.CardinalityOne}, 0},
		{"many", &CacheEntry{cardinality: schema.CardinalityMany,
			manySet: map[any]any{"dev": "dev", "ops": "ops"}}, 2},
		{"many empty", &CacheEntry{cardinality: schema.CardinalityMany,
			manySet: map[any]any{}}, 0},
		{"vector serves one value, not its element count",
			&CacheEntry{cardinality: schema.CardinalityVector,
				vectorList: []any{"x", "y", "z"}}, 1},
		{"vector cleared serves the empty vector", &CacheEntry{cardinality: schema.CardinalityVector,
			vectorList: []any{}}, 1},
		{"schemaless resolves last-write-wins", &CacheEntry{
			cardinality: schema.CardinalityUnknown, oneValue: int64(7)}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.entry.valueCount())
		})
	}
}

func TestCacheNewCache(t *testing.T) {
	cache := NewCache()
	require.NotNil(t, cache, "NewCache should return non-nil cache")
}

// A second resolve of a fresh entry is a hit: the same entry, with no index
// read.
func TestCacheFreshness(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.set(t, f.name, "Alice")
			cache := NewCache()

			entry1, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry1)
			assert.Equal(t, "Alice", entry1.OneValue())

			onHit := &scanReport{}
			entry2, err := f.resolve(cache, f.name, onHit)
			require.NoError(t, err)
			require.Same(t, entry1, entry2, "a fresh entry is served again")
			assert.Equal(t, 0, onHit.scanned, "a hit reads no index")
		})
	}
}

// An invalidated entry is rebuilt from the store once the key's version moves
// past it.
func TestCacheInvalidation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.set(t, f.name, "Alice")
			cache := NewCache()
			key := f.key(f.name)

			entry1, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			assert.Equal(t, "Alice", entry1.OneValue())

			cache.Invalidate([]CacheKey{key})
			f.set(t, f.name, "Bob")
			cache.UpdateMaxVersion(key, f.version(t, f.name))

			entry2, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry2)
			require.NotSame(t, entry1, entry2, "the entry is rebuilt after invalidation")
			assert.Equal(t, "Bob", entry2.OneValue())
		})
	}
}

// An entry behind the key's version is rebuilt from the store, and the rebuilt
// entry carries the version of the datom it resolved.
func TestCacheRebuildWhenStale(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.set(t, f.name, "Alice")
			cache := NewCache()
			key := f.key(f.name)

			entry1, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			assert.Equal(t, f.version(t, f.name), entry1.Version())

			f.set(t, f.name, "Carol")
			carol := f.version(t, f.name)
			cache.UpdateMaxVersion(key, carol)

			entry2, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			require.NotSame(t, entry1, entry2, "a stale entry is rebuilt")
			assert.Equal(t, "Carol", entry2.OneValue())
			assert.Equal(t, carol, entry2.Version())
		})
	}
}

func TestUpdateMaxVersion(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.set(t, f.name, "test")
			cache := NewCache()
			key := f.key(f.name)

			// Initial update
			cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 100, ReplicaID: 1})

			// Lower value should not update
			cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 50, ReplicaID: 1})

			// Higher value should update
			cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

			// Nothing is cached for the key yet, so the first resolve builds the
			// entry whatever version the key is at.
			entry, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			assert.NotNil(t, entry)
		})
	}
}

func TestUpdateMaxVersionConcurrency(t *testing.T) {
	cache := NewCache()

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// Run concurrent UpdateMaxVersion calls
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: uint64(i), ReplicaID: 1})
		}(i)
	}
	wg.Wait()

	// Should complete without panic or data race
}

func TestCacheRebuildCardinalityOne(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.set(t, f.name, "Alice")
			cache := NewCache()

			entry, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, schema.CardinalityOne, entry.Cardinality())
			assert.Equal(t, "Alice", entry.OneValue())
			assert.Nil(t, entry.ManySet())
			assert.Nil(t, entry.VectorList())
		})
	}
}

func TestCacheRebuildCardinalityMany(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.add(t, f.tags, "warrior", "veteran")
			cache := NewCache()

			entry, err := f.resolve(cache, f.tags, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, schema.CardinalityMany, entry.Cardinality())
			assert.Nil(t, entry.OneValue())
			_, hasWarrior := entry.ManySet()["warrior"]
			_, hasVeteran := entry.ManySet()["veteran"]
			assert.True(t, hasWarrior)
			assert.True(t, hasVeteran)
			assert.Nil(t, entry.VectorList())
		})
	}
}

func TestCacheRebuildCardinalityVector(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.add(t, f.skills, "stealth", "archery", "lockpicking")
			cache := NewCache()

			entry, err := f.resolve(cache, f.skills, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, schema.CardinalityVector, entry.Cardinality())
			assert.Nil(t, entry.OneValue())
			assert.Nil(t, entry.ManySet())
			assert.Equal(t, []any{"stealth", "archery", "lockpicking"}, entry.VectorList())
			assert.Len(t, entry.VectorIndex(), 3)
		})
	}
}

func TestCacheManySetMembership(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.add(t, f.tags, "a", "b", "c")
			cache := NewCache()

			entry, err := f.resolve(cache, f.tags, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)

			// O(1) membership check via map
			set := entry.ManySet()
			_, hasA := set["a"]
			_, hasB := set["b"]
			_, hasC := set["c"]
			_, hasD := set["d"]
			assert.True(t, hasA)
			assert.True(t, hasB)
			assert.True(t, hasC)
			assert.False(t, hasD)
		})
	}
}

func TestCacheManyEmptyAfterRemoves(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.add(t, f.tags, "a")
			f.remove(t, f.tags, "a")
			cache := NewCache()

			entry, err := f.resolve(cache, f.tags, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, 0, len(entry.ManySet()))
		})
	}
}

func TestCacheClear(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.set(t, f.name, "Alice")
			cache := NewCache()
			key := f.key(f.name)

			entry1, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			assert.Equal(t, "Alice", entry1.OneValue())

			cache.Clear()
			f.set(t, f.name, "Bob")
			cache.UpdateMaxVersion(key, f.version(t, f.name))

			entry2, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			require.NotSame(t, entry1, entry2, "the entry is rebuilt after a clear")
			assert.Equal(t, "Bob", entry2.OneValue())
		})
	}
}

// A cache with nothing in it, as after a restart, resolves the first access
// from the store and serves the second from the entry.
func TestCacheAfterRestart(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			f := newCacheFixture(t, mode)
			f.set(t, f.name, "Alice")
			cache := NewCache()

			entry, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			require.NotNil(t, entry)
			assert.Equal(t, "Alice", entry.OneValue())

			entry2, err := f.resolve(cache, f.name, DiscardIntake)
			require.NoError(t, err)
			require.Same(t, entry, entry2, "the second access is a hit")
		})
	}
}

// A resolver whose index read fails reaches the caller as the error, with no
// entry, for all three cardinalities, and the failing miss opens one scan:
// the resolver is invoked once and a failed rebuild is not retried.
func TestCacheRebuildStoreError(t *testing.T) {
	for _, mode := range optimizerModes {
		for _, tc := range []struct {
			name  string
			write func(t *testing.T, f *cacheFixture) datalog.Keyword
		}{
			{"one", func(t *testing.T, f *cacheFixture) datalog.Keyword {
				f.set(t, f.name, "Alice")
				return f.name
			}},
			{"many", func(t *testing.T, f *cacheFixture) datalog.Keyword {
				f.add(t, f.tags, "dev")
				return f.tags
			}},
			{"vector", func(t *testing.T, f *cacheFixture) datalog.Keyword {
				f.add(t, f.skills, "a")
				return f.skills
			}},
		} {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				f := newCacheFixture(t, mode)
				attr := tc.write(t, f)
				store := &scanFaultStore{Store: f.db.Store()}
				store.arm()
				faulting := NewPatternMatcher(store)
				faulting.SetSchema(f.db.Schema())
				cache := NewCache()

				entry, err := cache.GetOrResolve(f.key(attr), faulting.GetCardinality(attr), faulting, nil, nil, DiscardIntake)
				require.ErrorIs(t, err, errInjectedScanFault)
				assert.Nil(t, entry)
				assert.Equal(t, 1, store.scans, "the failing miss opens one scan and is not retried")
			})
		}
	}
}
