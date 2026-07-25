package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// createVectorCacheTestDB creates a temp database for vector cache tests
func createVectorCacheTestDatabase(t *testing.T) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

func TestVectorCacheHit(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
	defer cleanup()

	// Create schema with vector attribute
	s, err := schema.NewBuilder().
		Attribute(":character/skills").
		Type(schema.TypeString).
		Vector().
		Add().
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
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "lockpicking")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// First access should populate cache
	val0, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 0)
	require.NoError(t, err)
	assert.Equal(t, "stealth", val0)

	// Second access should be cached (O(1))
	val1, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 1)
	require.NoError(t, err)
	assert.Equal(t, "archery", val1)

	val2, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 2)
	require.NoError(t, err)
	assert.Equal(t, "lockpicking", val2)
}

func TestVectorCacheMiss(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache to simulate cache miss
	db.Cache().Clear()

	// Access should rebuild cache from storage
	val, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 0)
	require.NoError(t, err)
	assert.Equal(t, "stealth", val)
}

func TestVectorCacheInvalidation(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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

	// Add initial data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("character1")
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "stealth")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify initial length
	length, err := db.GetVectorLength(e, datalog.NewKeyword(":character/skills"))
	require.NoError(t, err)
	assert.Equal(t, int64(1), length)

	// Add more data (should invalidate cache)
	tx2 := db.NewTransaction()
	err = tx2.Add(e, datalog.NewKeyword(":character/skills"), "archery")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Cache should be invalidated, new length should reflect update
	length2, err := db.GetVectorLength(e, datalog.NewKeyword(":character/skills"))
	require.NoError(t, err)
	assert.Equal(t, int64(2), length2)
}

func TestVectorNthFromCache(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "first")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "second")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "third")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Test each position
	val0, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 0)
	require.NoError(t, err)
	assert.Equal(t, "first", val0)

	val1, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 1)
	require.NoError(t, err)
	assert.Equal(t, "second", val1)

	val2, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 2)
	require.NoError(t, err)
	assert.Equal(t, "third", val2)

	// Out of bounds should return nil
	val3, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 3)
	require.NoError(t, err)
	assert.Nil(t, val3)

	// Negative index should return nil
	valNeg, err := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), -1)
	require.NoError(t, err)
	assert.Nil(t, valNeg)
}

func TestVectorLengthFromCache(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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

	// Empty vector
	e := datalog.NewIdentity("character1")
	length, err := db.GetVectorLength(e, datalog.NewKeyword(":character/skills"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), length)

	// Add elements
	tx := db.NewTransaction()
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "a")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "b")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "c")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	length2, err := db.GetVectorLength(e, datalog.NewKeyword(":character/skills"))
	require.NoError(t, err)
	assert.Equal(t, int64(3), length2)
}

func TestVectorCachePreservesOrder(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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

	// Add data in specific order
	tx := db.NewTransaction()
	e := datalog.NewIdentity("character1")
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "alpha")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "beta")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "gamma")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify order is preserved
	val0, _ := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 0)
	val1, _ := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 1)
	val2, _ := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 2)

	assert.Equal(t, "alpha", val0)
	assert.Equal(t, "beta", val1)
	assert.Equal(t, "gamma", val2)
}

func TestVectorCacheAfterSet(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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

	// Add initial data
	tx := db.NewTransaction()
	e := datalog.NewIdentity("character1")
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "old1")
	require.NoError(t, err)
	err = tx.Add(e, datalog.NewKeyword(":character/skills"), "old2")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify initial state
	length, _ := db.GetVectorLength(e, datalog.NewKeyword(":character/skills"))
	assert.Equal(t, int64(2), length)

	// Replace with Set()
	tx2 := db.NewTransaction()
	err = tx2.Set(e, datalog.NewKeyword(":character/skills"), []any{"new1", "new2", "new3"})
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify new state after Set
	length2, _ := db.GetVectorLength(e, datalog.NewKeyword(":character/skills"))
	assert.Equal(t, int64(3), length2)

	val0, _ := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 0)
	val1, _ := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 1)
	val2, _ := db.GetVectorNth(e, datalog.NewKeyword(":character/skills"), 2)
	assert.Equal(t, "new1", val0)
	assert.Equal(t, "new2", val1)
	assert.Equal(t, "new3", val2)
}

func TestVectorNthNotVectorAttribute(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
	defer cleanup()

	// Create schema with cardinality-one attribute
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

	// GetVectorNth should error for non-vector attribute
	_, err = db.GetVectorNth(e, datalog.NewKeyword(":person/name"), 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a vector")
}

func TestVectorLengthNotVectorAttribute(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
	defer cleanup()

	// Create schema with cardinality-many attribute
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
	err = tx.Add(e, datalog.NewKeyword(":person/tags"), "tag1")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// GetVectorLength should error for non-vector attribute
	_, err = db.GetVectorLength(e, datalog.NewKeyword(":person/tags"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not a vector")
}

// TestVectorIndexContainsRealElementIDs verifies that the cache's vectorIndex
// contains actual ElementIDs (not zeros) that can be used for operations.
//
// This test exists because a previous implementation allocated the slice but
// filled it with zero values, which passed other tests but made the cache
// useless for its intended purpose.
func TestVectorIndexContainsRealElementIDs(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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

	// Add vector data
	e := datalog.NewIdentity("character1")
	a := datalog.NewKeyword(":character/skills")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "stealth"))
	require.NoError(t, tx.Add(e, a, "archery"))
	require.NoError(t, tx.Add(e, a, "lockpicking"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get the cache entry directly via the resolver interface
	matcher := NewPatternMatcher(db.store)
	matcher.SetSchema(s)

	var eBytes Entity
	eBytes = e.Hash()
	var aBytes Attribute
	copy(aBytes[:], a.String())

	values, positions, maxID, err := matcher.ResolveRGA(eBytes, aBytes)
	require.NoError(t, err)

	// Verify we got 3 elements
	require.Len(t, values, 3, "should have 3 values")
	require.Len(t, positions, 3, "should have 3 positions")

	// Verify values are correct
	assert.Equal(t, "stealth", values[0])
	assert.Equal(t, "archery", values[1])
	assert.Equal(t, "lockpicking", values[2])

	// CRITICAL: Verify positions contain REAL ElementIDs, not zeros
	for i, pos := range positions {
		assert.False(t, pos.IsZero(), "position %d should have non-zero ElementID, got %v", i, pos)
		assert.NotEqual(t, uint64(0), pos.Lamport, "position %d Lamport should be non-zero", i)
	}

	// Verify ElementIDs are in increasing order (added sequentially in same tx)
	assert.True(t, positions[0].Less(positions[1]),
		"position 0 (%v) should be less than position 1 (%v)", positions[0], positions[1])
	assert.True(t, positions[1].Less(positions[2]),
		"position 1 (%v) should be less than position 2 (%v)", positions[1], positions[2])

	// Verify maxID is at least as high as the last position
	assert.False(t, maxID.Less(positions[2]),
		"maxID (%v) should be >= last position (%v)", maxID, positions[2])

	t.Logf("ElementIDs: [%v, %v, %v], maxID: %v", positions[0], positions[1], positions[2], maxID)
}

// TestVectorIndexUsableForTombstone verifies that ElementIDs from vectorIndex
// can actually be used to tombstone specific elements.
func TestVectorIndexUsableForTombstone(t *testing.T) {
	db, cleanup := createVectorCacheTestDatabase(t)
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

	// Add vector data
	e := datalog.NewIdentity("character1")
	a := datalog.NewKeyword(":character/skills")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "stealth"))
	require.NoError(t, tx.Add(e, a, "archery"))
	require.NoError(t, tx.Add(e, a, "lockpicking"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Get the ElementID for "archery" (position 1) via cache
	matcher := NewPatternMatcher(db.store)
	matcher.SetSchema(s)

	var eBytes Entity
	eBytes = e.Hash()
	var aBytes Attribute
	copy(aBytes[:], a.String())

	_, positions, _, err := matcher.ResolveRGA(eBytes, aBytes)
	require.NoError(t, err)
	require.Len(t, positions, 3)

	archeryElementID := positions[1]
	require.False(t, archeryElementID.IsZero(), "archery ElementID should not be zero")

	// Now write a tombstone for that specific ElementID
	tx2 := db.NewTransaction()
	tombstoneID := db.clock.Next()
	tx2.datoms = append(tx2.datoms, datalog.Datom{
		E:        e,
		A:        a,
		V:        "archery", // Value for verification
		Tx:       tombstoneID,
		Op:       datalog.OpRGATombstone,
		AfterRef: archeryElementID, // The ElementID we got from cache
	})
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify "archery" is now gone
	matcher2 := NewPatternMatcher(db.store)
	matcher2.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher2, e, a)
	require.True(t, found)

	vec := result.([]string)
	require.Len(t, vec, 2, "should have 2 elements after tombstone")
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "lockpicking", vec[1])
	// "archery" should NOT be present
	for _, v := range vec {
		assert.NotEqual(t, "archery", v, "archery should have been tombstoned")
	}
}
