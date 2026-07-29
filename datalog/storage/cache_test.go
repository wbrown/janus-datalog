package storage

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// mockCacheResolver is a test mock for CacheResolver
type mockCacheResolver struct {
	cardinality     datalog.Keyword
	lwwValue        any
	lwwMaxID        datalog.ElementID
	lwwScanned      int
	lwwErr          error
	addWinsSet      map[any]any
	addWinsMaxID    datalog.ElementID
	addWinsScanned  int
	addWinsErr      error
	rgaElements     []any
	rgaPositions    []datalog.ElementID
	rgaMaxID        datalog.ElementID
	rgaScanned      int
	rgaErr          error
	resolveLWWCalls int
	resolveAddCalls int
	resolveRGACalls int

	// absent arranges an (E, A) carrying no datoms, so every resolve method
	// reports present=false and the cache stores no key for it. Zero-valued
	// because every fixture below arranges a resolvable value; a test that
	// wants absence says so.
	absent bool
}

func (m *mockCacheResolver) GetCardinality(a Attribute) datalog.Keyword {
	return m.cardinality
}

func (m *mockCacheResolver) ResolveLWW(e Entity, a Attribute, report *scanReport) (any, datalog.ElementID, bool, error) {
	m.resolveLWWCalls++
	if report != nil {
		report.scanned += m.lwwScanned
	}
	return m.lwwValue, m.lwwMaxID, !m.absent, m.lwwErr
}

func (m *mockCacheResolver) ResolveAddWins(e Entity, a Attribute, report *scanReport) (map[any]any, datalog.ElementID, bool, error) {
	m.resolveAddCalls++
	if report != nil {
		report.scanned += m.addWinsScanned
	}
	return m.addWinsSet, m.addWinsMaxID, !m.absent, m.addWinsErr
}

func (m *mockCacheResolver) ResolveRGA(e Entity, a Attribute, report *scanReport) ([]any, []datalog.ElementID, datalog.ElementID, bool, error) {
	m.resolveRGACalls++
	if report != nil {
		report.scanned += m.rgaScanned
	}
	return m.rgaElements, m.rgaPositions, m.rgaMaxID, !m.absent, m.rgaErr
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
	var e Entity
	copy(e[:], "entity1")

	for _, tc := range []struct {
		name     string
		resolver *mockCacheResolver
		attr     string
	}{
		{"one", &mockCacheResolver{
			cardinality: schema.CardinalityOne,
			lwwValue:    "Alice",
			lwwScanned:  7,
		}, ":person/name"},
		{"many", &mockCacheResolver{
			cardinality:    schema.CardinalityMany,
			addWinsSet:     map[any]any{"dev": "dev"},
			addWinsScanned: 11,
		}, ":person/tag"},
		{"vector", &mockCacheResolver{
			cardinality:  schema.CardinalityVector,
			rgaElements:  []any{"a"},
			rgaPositions: []datalog.ElementID{{Lamport: 1, ReplicaID: 1}},
			rgaScanned:   13,
		}, ":person/skill"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var a Attribute
			copy(a[:], tc.attr)
			key := CacheKey{E: e, A: a}

			cache := NewCache()
			build := &scanReport{}
			entry, err := cache.GetOrResolve(key, tc.resolver, nil, nil, build)
			require.NoError(t, err)
			require.NotNil(t, entry)

			want := tc.resolver.lwwScanned + tc.resolver.addWinsScanned + tc.resolver.rgaScanned
			require.Equal(t, want, entry.scanned,
				"the entry must carry what its resolution read")
			require.Equal(t, want, build.scanned,
				"the call that built the entry spent the build cost")

			// A hit rebuilds nothing, so it reads no index: it accrues nothing,
			// while the entry keeps the cost of the build it came from.
			onHit := &scanReport{}
			hit, err := cache.GetOrResolve(key, tc.resolver, nil, nil, onHit)
			require.NoError(t, err)
			require.Same(t, entry, hit, "second call must be a hit, not a rebuild")
			require.Equal(t, 0, onHit.scanned, "a hit reads no index")
			require.Equal(t, want, hit.scanned,
				"the entry still carries what building it read")
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

func TestCacheFreshness(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwValue:    "Alice",
		lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// First call should resolve from storage
	entry1, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry1)
	assert.Equal(t, "Alice", entry1.OneValue())
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// Second call should return cached entry (no additional resolve)
	entry2, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry2)
	assert.Equal(t, "Alice", entry2.OneValue())
	assert.Equal(t, 1, resolver.resolveLWWCalls, "should not call resolver again when fresh")
}

func TestCacheInvalidation(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwValue:    "Alice",
		lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// Populate cache
	_, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// Invalidate
	cache.Invalidate([]CacheKey{key})

	// Update resolver to return different value
	resolver.lwwValue = "Bob"
	resolver.lwwMaxID = datalog.ElementID{Lamport: 200, ReplicaID: 1}

	// Update maxVersions to trigger rebuild
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Next call should resolve again
	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "Bob", entry.OneValue())
	assert.Equal(t, 2, resolver.resolveLWWCalls, "should call resolver after invalidation")
}

func TestCacheRebuildWhenStale(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwValue:    "Alice",
		lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// Populate cache
	entry1, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	assert.Equal(t, datalog.ElementID{Lamport: 100, ReplicaID: 1}, entry1.Version())

	// Update maxVersions to make cache stale
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Update resolver
	resolver.lwwValue = "Carol"
	resolver.lwwMaxID = datalog.ElementID{Lamport: 200, ReplicaID: 1}

	// Next call should rebuild
	entry2, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	assert.Equal(t, "Carol", entry2.OneValue())
	assert.Equal(t, datalog.ElementID{Lamport: 200, ReplicaID: 1}, entry2.Version())
	assert.Equal(t, 2, resolver.resolveLWWCalls, "should rebuild when stale")
}

// TestCacheMockThreadSafety tests that Cache itself is thread-safe using a
// stateless mock. For real concurrency testing with storage, see
// TestCacheConcurrency in cache_integration_test.go.
func TestCacheMockThreadSafety(t *testing.T) {
	cache := NewCache()

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// Run concurrent GetOrResolve calls with per-goroutine resolvers
	// Each goroutine gets its own resolver to avoid shared mutable state
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine creates its own stateless resolver
			resolver := &mockCacheResolver{
				cardinality: schema.CardinalityOne,
				lwwValue:    "Alice",
				lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
			}
			entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
			assert.NoError(t, err)
			assert.NotNil(t, entry)
			assert.Equal(t, "Alice", entry.OneValue())
		}()
	}
	wg.Wait()

	// All should have completed without panic
}

func TestUpdateMaxVersion(t *testing.T) {
	cache := NewCache()

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// Initial update
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 100, ReplicaID: 1})

	// Lower value should not update
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 50, ReplicaID: 1})

	// Higher value should update
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Verify by checking if a cached entry at version 100 would be considered stale
	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwValue:    "test",
		lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}
	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	// Entry should be at version 100, but maxVersion is 200, so it should rebuild
	// Actually the entry is nil initially, so it will resolve
	assert.NotNil(t, entry)
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
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwValue:    "Alice",
		lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityOne, entry.Cardinality())
	assert.Equal(t, "Alice", entry.OneValue())
	assert.Nil(t, entry.ManySet())
	assert.Nil(t, entry.VectorList())
}

func TestCacheRebuildCardinalityMany(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality:  schema.CardinalityMany,
		addWinsSet:   map[any]any{"warrior": "warrior", "veteran": "veteran"},
		addWinsMaxID: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/tags")
	key := CacheKey{E: e, A: a}

	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityMany, entry.Cardinality())
	assert.Nil(t, entry.OneValue())
	_, hasWarrior := entry.ManySet()["warrior"]
	_, hasVeteran := entry.ManySet()["veteran"]
	assert.True(t, hasWarrior)
	assert.True(t, hasVeteran)
	assert.Nil(t, entry.VectorList())
}

func TestCacheRebuildCardinalityVector(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality:  schema.CardinalityVector,
		rgaElements:  []any{"stealth", "archery", "lockpicking"},
		rgaPositions: []datalog.ElementID{{Lamport: 1}, {Lamport: 2}, {Lamport: 3}},
		rgaMaxID:     datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":character/skills")
	key := CacheKey{E: e, A: a}

	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, schema.CardinalityVector, entry.Cardinality())
	assert.Nil(t, entry.OneValue())
	assert.Nil(t, entry.ManySet())
	assert.Equal(t, []any{"stealth", "archery", "lockpicking"}, entry.VectorList())
	assert.Len(t, entry.VectorIndex(), 3)
}

func TestCacheManySetMembership(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality:  schema.CardinalityMany,
		addWinsSet:   map[any]any{"a": "a", "b": "b", "c": "c"},
		addWinsMaxID: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":tags")
	key := CacheKey{E: e, A: a}

	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
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
}

func TestCacheManyEmptyAfterRemoves(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality:  schema.CardinalityMany,
		addWinsSet:   map[any]any{}, // Empty set after all removes
		addWinsMaxID: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":tags")
	key := CacheKey{E: e, A: a}

	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, 0, len(entry.ManySet()))
}

func TestCacheClear(t *testing.T) {
	cache := NewCache()
	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwValue:    "Alice",
		lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// Populate cache
	_, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// Clear cache
	cache.Clear()

	// Update maxVersions to trigger rebuild
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Next call should resolve again
	resolver.lwwValue = "Bob"
	resolver.lwwMaxID = datalog.ElementID{Lamport: 200, ReplicaID: 1}
	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	assert.Equal(t, "Bob", entry.OneValue())
	assert.Equal(t, 2, resolver.resolveLWWCalls)
}

func TestCacheAfterRestart(t *testing.T) {
	// Simulate cold start: maxVersions is empty, entries is empty
	cache := NewCache()

	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwValue:    "Alice",
		lwwMaxID:    datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// First access after restart should resolve
	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "Alice", entry.OneValue())
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// maxVersions should now be populated
	// Second access should use cache
	entry2, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.NoError(t, err)
	require.NotNil(t, entry2)
	assert.Equal(t, 1, resolver.resolveLWWCalls)
}

func TestCacheRebuildStoreError(t *testing.T) {
	cache := NewCache()

	// Resolver that returns errors
	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityOne,
		lwwErr:      assert.AnError,
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":person/name")
	key := CacheKey{E: e, A: a}

	// The resolver's error reaches the caller. A nil entry means the attribute
	// has no value, so a dropped error would make a failed read and an absent
	// attribute the same answer.
	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, entry)
	assert.Equal(t, 1, resolver.resolveLWWCalls)
}

func TestCacheRebuildAddWinsError(t *testing.T) {
	cache := NewCache()

	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityMany,
		addWinsErr:  assert.AnError,
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":tags")
	key := CacheKey{E: e, A: a}

	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, entry)
	assert.Equal(t, 1, resolver.resolveAddCalls)
}

func TestCacheRebuildRGAError(t *testing.T) {
	cache := NewCache()

	resolver := &mockCacheResolver{
		cardinality: schema.CardinalityVector,
		rgaErr:      assert.AnError,
	}

	var e Entity
	copy(e[:], "entity1")
	var a Attribute
	copy(a[:], ":skills")
	key := CacheKey{E: e, A: a}

	entry, err := cache.GetOrResolve(key, resolver, nil, nil, DiscardIntake)
	require.ErrorIs(t, err, assert.AnError)
	assert.Nil(t, entry)
	assert.Equal(t, 1, resolver.resolveRGACalls)
}
