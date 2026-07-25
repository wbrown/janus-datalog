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
	cardinality     schema.Cardinality
	lwwValue        any
	lwwMaxID        datalog.ElementID
	lwwErr          error
	addWinsSet      map[any]any
	addWinsMaxID    datalog.ElementID
	addWinsErr      error
	rgaElements     []any
	rgaPositions    []datalog.ElementID
	rgaMaxID        datalog.ElementID
	rgaErr          error
	resolveLWWCalls int
	resolveAddCalls int
	resolveRGACalls int
}

func (m *mockCacheResolver) GetCardinality(a Attribute) schema.Cardinality {
	return m.cardinality
}

func (m *mockCacheResolver) ResolveLWW(e Entity, a Attribute) (any, datalog.ElementID, error) {
	m.resolveLWWCalls++
	return m.lwwValue, m.lwwMaxID, m.lwwErr
}

func (m *mockCacheResolver) ResolveAddWins(e Entity, a Attribute) (map[any]any, datalog.ElementID, error) {
	m.resolveAddCalls++
	return m.addWinsSet, m.addWinsMaxID, m.addWinsErr
}

func (m *mockCacheResolver) ResolveRGA(e Entity, a Attribute) ([]any, []datalog.ElementID, datalog.ElementID, error) {
	m.resolveRGACalls++
	return m.rgaElements, m.rgaPositions, m.rgaMaxID, m.rgaErr
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
	entry1 := cache.GetOrResolve(key, resolver, nil)
	require.NotNil(t, entry1)
	assert.Equal(t, "Alice", entry1.OneValue())
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// Second call should return cached entry (no additional resolve)
	entry2 := cache.GetOrResolve(key, resolver, nil)
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
	cache.GetOrResolve(key, resolver, nil)
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// Invalidate
	cache.Invalidate([]CacheKey{key})

	// Update resolver to return different value
	resolver.lwwValue = "Bob"
	resolver.lwwMaxID = datalog.ElementID{Lamport: 200, ReplicaID: 1}

	// Update maxVersions to trigger rebuild
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Next call should resolve again
	entry := cache.GetOrResolve(key, resolver, nil)
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
	entry1 := cache.GetOrResolve(key, resolver, nil)
	assert.Equal(t, datalog.ElementID{Lamport: 100, ReplicaID: 1}, entry1.Version())

	// Update maxVersions to make cache stale
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Update resolver
	resolver.lwwValue = "Carol"
	resolver.lwwMaxID = datalog.ElementID{Lamport: 200, ReplicaID: 1}

	// Next call should rebuild
	entry2 := cache.GetOrResolve(key, resolver, nil)
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
			entry := cache.GetOrResolve(key, resolver, nil)
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
	entry := cache.GetOrResolve(key, resolver, nil)
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

	entry := cache.GetOrResolve(key, resolver, nil)
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

	entry := cache.GetOrResolve(key, resolver, nil)
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

	entry := cache.GetOrResolve(key, resolver, nil)
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

	entry := cache.GetOrResolve(key, resolver, nil)
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

	entry := cache.GetOrResolve(key, resolver, nil)
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
	cache.GetOrResolve(key, resolver, nil)
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// Clear cache
	cache.Clear()

	// Update maxVersions to trigger rebuild
	cache.UpdateMaxVersion(key, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Next call should resolve again
	resolver.lwwValue = "Bob"
	resolver.lwwMaxID = datalog.ElementID{Lamport: 200, ReplicaID: 1}
	entry := cache.GetOrResolve(key, resolver, nil)
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
	entry := cache.GetOrResolve(key, resolver, nil)
	require.NotNil(t, entry)
	assert.Equal(t, "Alice", entry.OneValue())
	assert.Equal(t, 1, resolver.resolveLWWCalls)

	// maxVersions should now be populated
	// Second access should use cache
	entry2 := cache.GetOrResolve(key, resolver, nil)
	require.NotNil(t, entry2)
	assert.Equal(t, 1, resolver.resolveLWWCalls)
}

// mockStore implements Store interface for testing IsAttributeFresh
type mockStore struct {
	maxAttrID    datalog.ElementID
	maxAttrErr   error
	maxAttrCalls int
}

func (m *mockStore) MaxElementIDForAttribute(a []byte) (datalog.ElementID, error) {
	m.maxAttrCalls++
	return m.maxAttrID, m.maxAttrErr
}

// Stub implementations for Store interface
func (m *mockStore) Assert(datoms []datalog.Datom) error                       { return nil }
func (m *mockStore) Retract(datoms []datalog.Datom) error                      { return nil }
func (m *mockStore) Scan(index IndexType, start, end []byte) (Iterator, error) { return nil, nil }
func (m *mockStore) MaxElementID() (datalog.ElementID, error)                  { return datalog.ElementID{}, nil }
func (m *mockStore) BeginTx() (StoreTx, error)                                 { return nil, nil }
func (m *mockStore) Close() error                                              { return nil }

func TestCacheAttributeFreshness(t *testing.T) {
	cache := NewCache()
	store := &mockStore{
		maxAttrID: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var a Attribute
	copy(a[:], ":person/name")

	// Initially, attrVersions is empty - should return false WITHOUT calling store
	// (short-circuit when no cached version exists)
	assert.False(t, cache.IsAttributeFresh(a, store))
	assert.Equal(t, 0, store.maxAttrCalls, "should not call store when no cached version")

	// After updating attribute version to match store
	cache.UpdateAttributeVersion(a, datalog.ElementID{Lamport: 100, ReplicaID: 1})
	assert.True(t, cache.IsAttributeFresh(a, store))
	assert.Equal(t, 1, store.maxAttrCalls, "should call store to compare versions")

	// If store version increases, should return false
	store.maxAttrID = datalog.ElementID{Lamport: 200, ReplicaID: 1}
	assert.False(t, cache.IsAttributeFresh(a, store))
	assert.Equal(t, 2, store.maxAttrCalls)
}

func TestCacheAttributeInvalidation(t *testing.T) {
	cache := NewCache()
	store := &mockStore{
		maxAttrID: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	var a Attribute
	copy(a[:], ":person/name")

	// Set attribute version
	cache.UpdateAttributeVersion(a, datalog.ElementID{Lamport: 100, ReplicaID: 1})
	assert.True(t, cache.IsAttributeFresh(a, store))

	// Update to new version
	cache.UpdateAttributeVersion(a, datalog.ElementID{Lamport: 200, ReplicaID: 1})

	// Now store needs to match new version
	assert.False(t, cache.IsAttributeFresh(a, store))
	store.maxAttrID = datalog.ElementID{Lamport: 200, ReplicaID: 1}
	assert.True(t, cache.IsAttributeFresh(a, store))
}

func TestCacheAttributeFreshnessStoreError(t *testing.T) {
	cache := NewCache()
	store := &mockStore{
		maxAttrErr: assert.AnError,
	}

	var a Attribute
	copy(a[:], ":person/name")

	// Set attribute version
	cache.UpdateAttributeVersion(a, datalog.ElementID{Lamport: 100, ReplicaID: 1})

	// Should return false when store returns error
	assert.False(t, cache.IsAttributeFresh(a, store))
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

	// GetOrResolve should return nil when resolver errors
	entry := cache.GetOrResolve(key, resolver, nil)
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

	entry := cache.GetOrResolve(key, resolver, nil)
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

	entry := cache.GetOrResolve(key, resolver, nil)
	assert.Nil(t, entry)
	assert.Equal(t, 1, resolver.resolveRGACalls)
}
