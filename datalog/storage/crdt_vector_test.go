package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestVectorBasicAdd verifies basic vector append via Add()
func TestVectorBasicAdd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-basic-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with vector attribute
	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Add skills in order
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, skills, "stealth"))
	require.NoError(t, tx.Add(alice, skills, "archery"))
	require.NoError(t, tx.Add(alice, skills, "lockpicking"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify the vector is resolved in order
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found, "should find skills")

	vec, ok := result.([]string)
	require.True(t, ok, "result should be []string for TypeString vector, got %T", result)
	require.Len(t, vec, 3, "should have 3 skills")

	// Order should match insertion order within same transaction
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
}

// TestVectorMultipleTransactions verifies ordering across transactions
func TestVectorMultipleTransactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-multi-tx-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// First transaction
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Second transaction
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(alice, skills, "lockpicking"))
	require.NoError(t, tx2.Add(alice, skills, "pickpocket"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify all elements in order
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)

	vec := result.([]string)
	require.Len(t, vec, 4)

	// All elements from tx1 should come before tx2
	// Within each tx, order is preserved
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
	assert.Equal(t, "pickpocket", vec[3])
}

// TestVectorEmpty verifies empty vector handling
func TestVectorEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-empty-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// No data added — never-set vector is not found, consistent with other cardinalities
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := requireAttributeLookup(t, matcher, alice, skills)
	assert.False(t, found, "never-set vector attribute should not be found")
	assert.Nil(t, result, "never-set vector should return nil")
}

// TestVectorWithDifferentTypes verifies vectors can hold different value types
func TestVectorWithDifferentTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-types-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":event/scores").Type(schema.TypeLong).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	game := datalog.NewIdentity("game1")
	scores := datalog.NewKeyword(":event/scores")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(game, scores, int64(100)))
	require.NoError(t, tx.Add(game, scores, int64(250)))
	require.NoError(t, tx.Add(game, scores, int64(175)))
	_, err = tx.Commit()
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := requireAttributeLookup(t, matcher, game, scores)
	require.True(t, found)

	vec := result.([]int64)
	require.Len(t, vec, 3)

	assert.Equal(t, int64(100), vec[0])
	assert.Equal(t, int64(250), vec[1])
	assert.Equal(t, int64(175), vec[2])
}

// TestRGAElementEncodeDecode verifies RGAElement round-trip encoding
func TestRGAElementEncodeDecode(t *testing.T) {
	elemID := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	afterRef := datalog.ElementID{Lamport: 50, ReplicaID: 1}

	// Test without tombstone
	elem := RGAElement{
		ID:       elemID,
		Value:    "test value",
		AfterRef: afterRef,
	}

	encoded := EncodeRGAElement(elem)
	decoded, err := DecodeRGAElement(elemID, encoded)
	require.NoError(t, err)

	assert.Equal(t, elemID, decoded.ID)
	assert.Equal(t, "test value", decoded.Value)
	assert.Equal(t, afterRef, decoded.AfterRef)
	assert.Nil(t, decoded.Tombstone)
}

// TestRGAElementWithTombstone verifies tombstoned element encoding
func TestRGAElementWithTombstone(t *testing.T) {
	elemID := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	afterRef := datalog.ElementID{Lamport: 50, ReplicaID: 1}
	tombstoneID := datalog.ElementID{Lamport: 150, ReplicaID: 1}

	elem := RGAElement{
		ID:        elemID,
		Value:     "deleted value",
		AfterRef:  afterRef,
		Tombstone: &tombstoneID,
	}

	encoded := EncodeRGAElement(elem)
	decoded, err := DecodeRGAElement(elemID, encoded)
	require.NoError(t, err)

	assert.Equal(t, elemID, decoded.ID)
	assert.Equal(t, "deleted value", decoded.Value)
	assert.Equal(t, afterRef, decoded.AfterRef)
	require.NotNil(t, decoded.Tombstone)
	assert.Equal(t, tombstoneID, *decoded.Tombstone)
}

// TestRGAReconstruction verifies basic RGA reconstruction
func TestRGAReconstruction(t *testing.T) {
	// Create elements with different afterRefs
	// Structure: HEAD -> elem1 -> elem2 -> elem3
	elem1 := RGAElement{
		ID:       datalog.ElementID{Lamport: 1, ReplicaID: 1},
		Value:    "first",
		AfterRef: HEAD, // After HEAD
	}
	elem2 := RGAElement{
		ID:       datalog.ElementID{Lamport: 2, ReplicaID: 1},
		Value:    "second",
		AfterRef: elem1.ID,
	}
	elem3 := RGAElement{
		ID:       datalog.ElementID{Lamport: 3, ReplicaID: 1},
		Value:    "third",
		AfterRef: elem2.ID,
	}

	// Reconstruct in any order - should produce same result
	elements := []RGAElement{elem3, elem1, elem2}
	result := ReconstructRGA(elements)

	require.Len(t, result, 3)
	assert.Equal(t, "first", result[0])
	assert.Equal(t, "second", result[1])
	assert.Equal(t, "third", result[2])
}

// TestRGAReconstructionWithConcurrentInserts verifies deterministic ordering
func TestRGAReconstructionWithConcurrentInserts(t *testing.T) {
	// Simulate concurrent inserts after HEAD from different replicas
	// Both elem1 and elem2 inserted after HEAD
	// Lower ElementID should come first
	elem1 := RGAElement{
		ID:       datalog.ElementID{Lamport: 1, ReplicaID: 100}, // Higher ReplicaID
		Value:    "from replica 100",
		AfterRef: HEAD,
	}
	elem2 := RGAElement{
		ID:       datalog.ElementID{Lamport: 1, ReplicaID: 50}, // Lower ReplicaID, same Lamport
		Value:    "from replica 50",
		AfterRef: HEAD,
	}

	elements := []RGAElement{elem1, elem2}
	result := ReconstructRGA(elements)

	require.Len(t, result, 2)
	// Lower ReplicaID should come first (deterministic tiebreaker)
	assert.Equal(t, "from replica 50", result[0])
	assert.Equal(t, "from replica 100", result[1])
}

// TestRGAReconstructionWithTombstones verifies deleted elements are filtered
func TestRGAReconstructionWithTombstones(t *testing.T) {
	tombstoneID := datalog.ElementID{Lamport: 10, ReplicaID: 1}

	elem1 := RGAElement{
		ID:       datalog.ElementID{Lamport: 1, ReplicaID: 1},
		Value:    "first",
		AfterRef: HEAD,
	}
	elem2 := RGAElement{
		ID:        datalog.ElementID{Lamport: 2, ReplicaID: 1},
		Value:     "deleted",
		AfterRef:  elem1.ID,
		Tombstone: &tombstoneID, // Deleted
	}
	elem3 := RGAElement{
		ID:       datalog.ElementID{Lamport: 3, ReplicaID: 1},
		Value:    "third",
		AfterRef: elem2.ID, // Still points to deleted element
	}

	elements := []RGAElement{elem1, elem2, elem3}
	result := ReconstructRGA(elements)

	// Tombstoned element should be filtered out
	require.Len(t, result, 2)
	assert.Equal(t, "first", result[0])
	assert.Equal(t, "third", result[1])
}

// TestVectorSchemaValidation verifies that Add() validates vector values
func TestVectorSchemaValidation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-validation-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	tx := db.NewTransaction()

	// String should work
	require.NoError(t, tx.Add(alice, skills, "stealth"))

	// Integer should fail (wrong type for string vector)
	err = tx.Add(alice, skills, int64(123))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema validation failed")
}

// TestVectorSetReplacesEntireVector verifies Set() replaces the entire vector
func TestVectorSetReplacesEntireVector(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// First, add some skills via Add()
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "lockpicking"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Verify initial state
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)

	// Now use Set() to replace entire vector
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"magic", "alchemy"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify the vector was replaced
	matcher2 := NewBadgerMatcher(db.store)
	matcher2.SetSchema(s)
	result2, found2 := requireAttributeLookup(t, matcher2, alice, skills)
	require.True(t, found2)
	vec2 := result2.([]string)
	require.Len(t, vec2, 2, "vector should have 2 elements after Set()")
	assert.Equal(t, "magic", vec2[0])
	assert.Equal(t, "alchemy", vec2[1])
}

// TestVectorSetToEmpty verifies Set() with empty slice clears vector
func TestVectorSetToEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-empty-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Add some skills
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Clear vector with empty Set()
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify vector is empty (but still "found" — empty is a value)
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	assert.True(t, found, "vector attribute always exists (empty is a value)")
	assert.Equal(t, []string{}, result, "cleared vector should return typed empty slice")
}

// TestVectorAddNoReadDatabase verifies Add() does NOT read from database
func TestVectorAddNoReadDatabase(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-no-read-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Pre-populate some skills
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Now Add() in a new transaction - it should NOT read the existing data
	// to determine afterRef, it should use HEAD (and chain within transaction)
	tx2 := db.NewTransaction()

	// These adds should chain: archery -> lockpicking (both after HEAD since
	// Add() doesn't read existing vector)
	require.NoError(t, tx2.Add(alice, skills, "archery"))
	require.NoError(t, tx2.Add(alice, skills, "lockpicking"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify all three skills exist (stealth from tx1, archery+lockpicking from tx2)
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3, "should have all 3 skills")

	// The order depends on ElementID sorting at HEAD level
	// stealth was added first (lower Lamport), so it should come first
	// archery and lockpicking are chained within tx2
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
}

// TestVectorQueryIntegration verifies vectors work through Datalog queries
func TestVectorQueryIntegration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-query-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/name").Type(schema.TypeString).Add().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	name := datalog.NewKeyword(":character/name")
	skills := datalog.NewKeyword(":character/skills")

	// Add data
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, skills, "stealth"))
	require.NoError(t, tx.Add(alice, skills, "archery"))
	require.NoError(t, tx.Add(bob, name, "Bob"))
	require.NoError(t, tx.Add(bob, skills, "magic"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query for entities with skills (should return both)
	results, err := executor.CollectTuples(db.Query(`[:find ?name :where [?e :character/name ?name] [?e :character/skills ?skills]]`))
	require.NoError(t, err)
	require.Len(t, results, 2, "should find both characters with skills")

	// Verify both names are present (order not guaranteed)
	names := make(map[string]bool)
	for _, tuple := range results {
		names[tuple[0].(string)] = true
	}
	assert.True(t, names["Alice"])
	assert.True(t, names["Bob"])
}

// TestVectorQueryWithBoundEntity verifies querying vector with E bound
func TestVectorQueryWithBoundEntity(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-query-bound-e-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/name").Type(schema.TypeString).Add().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":character/name")
	skills := datalog.NewKeyword(":character/skills")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, skills, "stealth"))
	require.NoError(t, tx.Add(alice, skills, "archery"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// First verify basic query works
	basicResults, err := executor.CollectTuples(db.Query(`[:find ?name :where [?e :character/name ?name]]`))
	require.NoError(t, err)
	t.Logf("Basic query results: %d tuples", len(basicResults))
	for i, tuple := range basicResults {
		t.Logf("  Basic tuple %d: %v", i, tuple)
	}

	// Check if LookupAttribute still works
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	lookupResult, found := requireAttributeLookup(t, matcher, alice, skills)
	t.Logf("LookupAttribute found=%v, result=%v (type: %T)", found, lookupResult, lookupResult)

	// Query with E bound via join - this binds ?e first, then queries skills
	// Pattern: [?e :name "Alice"] binds ?e, then [?e :skills ?skills] has E bound
	results, err := executor.CollectTuples(db.Query(`[:find ?skills :where [?e :character/name "Alice"] [?e :character/skills ?skills]]`))
	require.NoError(t, err, "query with bound E should not error")

	t.Logf("Vector query results count: %d", len(results))
	for i, tuple := range results {
		t.Logf("  Tuple %d: %v (type: %T)", i, tuple[0], tuple[0])
	}

	// With E bound via join, should return 1 tuple with the vector
	require.Len(t, results, 1, "should return 1 tuple with vector")

	// The value should be a slice, not raw bytes
	vec, ok := results[0][0].([]string)
	require.True(t, ok, "skills should be []string, got %T", results[0][0])
	require.Len(t, vec, 2)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
}

// TestVectorQueryProjectSkills verifies querying vector values directly
func TestVectorQueryProjectSkills(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-query-project-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/name").Type(schema.TypeString).Add().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":character/name")
	skills := datalog.NewKeyword(":character/skills")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, skills, "stealth"))
	require.NoError(t, tx.Add(alice, skills, "archery"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query that projects the skills variable directly
	// This tests whether vector values can be returned from queries
	results, err := executor.CollectTuples(db.Query(`[:find ?skills :where [?e :character/skills ?skills]]`))
	require.NoError(t, err, "query for vector values should not error")

	// Log what we actually get for debugging
	t.Logf("Results count: %d", len(results))
	for i, tuple := range results {
		t.Logf("  Tuple %d: %v (type: %T)", i, tuple[0], tuple[0])
	}

	// The behavior here depends on implementation:
	// Option A: Returns the whole vector as one result
	// Option B: Returns each element as separate tuples
	// Either is valid, but we need to verify SOMETHING works
	require.NotEmpty(t, results, "should return some results for vector query")
}

// TestVectorQueryNameAndSkills verifies joining vectors with other attributes
func TestVectorQueryNameAndSkills(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-query-join-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/name").Type(schema.TypeString).Add().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":character/name")
	skills := datalog.NewKeyword(":character/skills")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, skills, "stealth"))
	require.NoError(t, tx.Add(alice, skills, "archery"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query that joins name with skills and projects both
	results, err := executor.CollectTuples(db.Query(`[:find ?name ?skills :where [?e :character/name ?name] [?e :character/skills ?skills]]`))
	require.NoError(t, err, "join query should not error")

	t.Logf("Results count: %d", len(results))
	for i, tuple := range results {
		t.Logf("  Tuple %d: name=%v, skills=%v (type: %T)", i, tuple[0], tuple[1], tuple[1])
	}

	require.NotEmpty(t, results, "should return some results for join query")

	// Verify Alice is in the results
	foundAlice := false
	for _, tuple := range results {
		if tuple[0] == "Alice" {
			foundAlice = true
		}
	}
	assert.True(t, foundAlice, "should find Alice in results")
}

// TestVectorPullIntegration verifies Pull API resolves vector attributes
func TestVectorPullIntegration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-pull-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/name").Type(schema.TypeString).Add().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":character/name")
	skills := datalog.NewKeyword(":character/skills")

	// Add data with ordered skills
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, skills, "stealth"))
	require.NoError(t, tx.Add(alice, skills, "archery"))
	require.NoError(t, tx.Add(alice, skills, "lockpicking"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Use Pull API to get skills
	results, err := executor.CollectTuples(db.Query(`[:find (pull ?e [:character/name :character/skills]) :where [?e :character/name "Alice"]]`))
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Parse pull result
	pullResult, ok := results[0][0].(map[string]interface{})
	require.True(t, ok, "pull result should be a map, got %T: %v", results[0][0], results[0][0])

	// Check name (Pull API returns keys without leading colon)
	assert.Equal(t, "Alice", pullResult["character/name"])

	// Check skills is a slice in correct order
	skillsResult, ok := pullResult["character/skills"].([]string)
	require.True(t, ok, "skills should be a slice")
	require.Len(t, skillsResult, 3, "should have 3 skills")
	assert.Equal(t, "stealth", skillsResult[0])
	assert.Equal(t, "archery", skillsResult[1])
	assert.Equal(t, "lockpicking", skillsResult[2])
}

// TestVectorRemoveMostRecent verifies Remove() tombstones the most recently added
// element matching the value (LIFO semantics).
//
// Design decision: Remove(e, a, v) for cardinality-vector removes the element
// with highest ElementID matching value v. This provides:
// - O(k) performance where k = elements with matching value (no RGA reconstruction)
// - LIFO/stack semantics useful for undo operations
// - CRDT-friendly: "most recent" = "the one I most recently added"
//
// Users wanting to remove the FIRST occurrence (in order) must use
// read-modify-write via Set().
func TestVectorRemoveMostRecent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-remove-lifo-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Add skills with duplicates - "stealth" appears twice
	// Order: stealth(0), archery(1), stealth(2)
	// Position 0 and 2 both have value "stealth"
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth")) // First "stealth" - lower ElementID
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "stealth")) // Second "stealth" - higher ElementID
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Verify initial state: ["stealth", "archery", "stealth"]
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "stealth", vec[2])

	// Remove "stealth" - should remove the MOST RECENT one (position 2)
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(alice, skills, "stealth"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result: ["stealth", "archery"]
	// The first "stealth" remains, the second (most recent) is removed
	matcher2 := NewBadgerMatcher(db.store)
	matcher2.SetSchema(s)
	result2, found2 := requireAttributeLookup(t, matcher2, alice, skills)
	require.True(t, found2)
	vec2 := result2.([]string)
	require.Len(t, vec2, 2, "should have 2 elements after Remove()")
	assert.Equal(t, "stealth", vec2[0], "first stealth should remain")
	assert.Equal(t, "archery", vec2[1], "archery should remain")
}

// TestVectorRemoveNonExistent verifies Remove() is a no-op for values not in vector
func TestVectorRemoveNonExistent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-remove-nonexistent-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Add some skills
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Remove a value that doesn't exist - should be a no-op
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(alice, skills, "magic")) // "magic" not in vector
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify vector unchanged: ["stealth", "archery"]
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 2, "vector should be unchanged")
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
}

// TestVectorRemoveAllOccurrences verifies multiple Remove() calls remove all occurrences
func TestVectorRemoveAllOccurrences(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-remove-all-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Add skills with duplicates
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth")) // Position 0
	require.NoError(t, tx1.Add(alice, skills, "archery")) // Position 1
	require.NoError(t, tx1.Add(alice, skills, "stealth")) // Position 2
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Remove "stealth" twice - should remove both occurrences
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(alice, skills, "stealth")) // Removes position 2 (most recent)
	require.NoError(t, tx2.Remove(alice, skills, "stealth")) // Removes position 0 (now most recent)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result: ["archery"]
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 1, "should have 1 element after removing both stealths")
	assert.Equal(t, "archery", vec[0])
}

// TestAddSchemaAwareVector verifies Add() properly handles vector cardinality
func TestAddSchemaAwareVector(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "add-schema-vector-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with all three cardinalities
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).Add().            // One
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().     // Many
		Attribute(":person/skills").Type(schema.TypeString).Vector().Add(). // Vector
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")
	tags := datalog.NewKeyword(":person/tags")
	skills := datalog.NewKeyword(":person/skills")

	// Add values to all three types using same Add() method
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))     // One
	require.NoError(t, tx.Add(alice, tags, "developer")) // Many
	require.NoError(t, tx.Add(alice, tags, "lead"))      // Many
	require.NoError(t, tx.Add(alice, skills, "go"))      // Vector
	require.NoError(t, tx.Add(alice, skills, "python"))  // Vector
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify all cardinalities work correctly
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	// Cardinality-one: single value
	nameVal, found := requireAttributeLookup(t, matcher, alice, name)
	require.True(t, found)
	assert.Equal(t, "Alice", nameVal)

	// Cardinality-many: returns slice of all set members
	tagVal, found := requireAttributeLookup(t, matcher, alice, tags)
	require.True(t, found)
	tagSlice := tagVal.([]interface{})
	require.Len(t, tagSlice, 2)
	// Set members may be in any order
	tagSet := make(map[string]bool)
	for _, t := range tagSlice {
		tagSet[t.(string)] = true
	}
	assert.True(t, tagSet["developer"])
	assert.True(t, tagSet["lead"])

	// Cardinality-vector: returns ordered slice
	skillsVal, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := skillsVal.([]string)
	require.Len(t, vec, 2)
	assert.Equal(t, "go", vec[0])
	assert.Equal(t, "python", vec[1])
}

// ============================================================================
// Set() Backwards Diff Optimization Tests
// ============================================================================
//
// These tests verify Set() correctly handles partial updates via backwards diff.
// The optimization compares from the END first because appends are the dominant
// operation:
//
//   Old: ["a", "b", "c"]
//   New: ["a", "b", "c", "d"]  // Append
//
// Backwards diff finds common prefix = 3, common suffix = 0, so only inserts "d".
// The full-replace algorithm would do 3 tombstones + 4 inserts = 7 writes.
// The optimized algorithm does 1 insert.
//
// These tests verify CORRECTNESS. Performance is validated via benchmarks.

// TestVectorSetAppend verifies Set() efficiently handles appending elements.
// This is the most common operation and should only insert new elements.
func TestVectorSetAppend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-append-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Initial state: ["stealth", "archery", "lockpicking"]
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "lockpicking"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Append one element: ["stealth", "archery", "lockpicking", "magic"]
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"stealth", "archery", "lockpicking", "magic"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 4)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
	assert.Equal(t, "magic", vec[3])
}

// TestVectorSetAppendMultiple verifies Set() handles appending multiple elements.
func TestVectorSetAppendMultiple(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-append-multi-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Initial state: ["stealth"]
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Append multiple: ["stealth", "archery", "lockpicking"]
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"stealth", "archery", "lockpicking"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
}

// TestVectorSetChangeEnd verifies Set() handles changing the last element.
func TestVectorSetChangeEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-change-end-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Initial state: ["stealth", "archery", "lockpicking"]
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "lockpicking"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Change last element: ["stealth", "archery", "magic"]
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"stealth", "archery", "magic"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "magic", vec[2])
}

// TestVectorSetChangeMiddle verifies Set() handles changing a middle element.
func TestVectorSetChangeMiddle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-change-middle-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Initial state: ["stealth", "archery", "lockpicking"]
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "lockpicking"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Change middle element: ["stealth", "MAGIC", "lockpicking"]
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"stealth", "MAGIC", "lockpicking"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "MAGIC", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
}

// TestVectorSetPrepend verifies Set() handles prepending (worst case for optimization).
func TestVectorSetPrepend(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-prepend-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Initial state: ["archery", "lockpicking"]
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "lockpicking"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Prepend element: ["stealth", "archery", "lockpicking"]
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"stealth", "archery", "lockpicking"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
}

// TestVectorSetNoChange verifies Set() does nothing when vectors are identical.
func TestVectorSetNoChange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-nochange-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Initial state: ["stealth", "archery", "lockpicking"]
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "lockpicking"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Set to same values: ["stealth", "archery", "lockpicking"]
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"stealth", "archery", "lockpicking"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result unchanged
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
}

// TestVectorSetFromEmpty verifies Set() works when starting with empty vector.
func TestVectorSetFromEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-from-empty-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// No initial state - vector doesn't exist

	// Set from empty: ["stealth", "archery", "lockpicking"]
	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, skills, []interface{}{"stealth", "archery", "lockpicking"}))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify result
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
}

// TestVectorEnumerateQuery verifies that [(enumerate ?vec) [?idx ?val]] expands
// a vector into multiple result tuples in a Datalog query.
func TestVectorEnumerateQuery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-enumerate-query-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":product/label").Type(schema.TypeString).Add().
		Attribute(":product/tags").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	widget := datalog.NewIdentity("widget")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(widget, datalog.NewKeyword(":product/label"), "Widget"))
	require.NoError(t, tx.Add(widget, datalog.NewKeyword(":product/tags"), "waterproof"))
	require.NoError(t, tx.Add(widget, datalog.NewKeyword(":product/tags"), "lightweight"))
	require.NoError(t, tx.Add(widget, datalog.NewKeyword(":product/tags"), "durable"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// enumerate should expand the vector into 3 tuples, one per element
	results, err := executor.CollectTuples(db.Query(
		`[:find ?idx ?tag
		  :where
		  [?e :product/label "Widget"]
		  [?e :product/tags ?vec]
		  [(enumerate ?vec) [?idx ?tag]]]`))
	require.NoError(t, err)
	require.Len(t, results, 3, "enumerate should produce one tuple per vector element")

	// Verify index-value pairs
	type pair struct {
		idx int64
		tag string
	}
	var pairs []pair
	for _, tuple := range results {
		pairs = append(pairs, pair{
			idx: tuple[0].(int64),
			tag: tuple[1].(string),
		})
	}

	assert.Contains(t, pairs, pair{0, "waterproof"})
	assert.Contains(t, pairs, pair{1, "lightweight"})
	assert.Contains(t, pairs, pair{2, "durable"})
}

// TestVectorEnumerateMultipleEntities verifies enumerate correctly scopes to
// each entity's own vector, preventing cross-joins between entities.
func TestVectorEnumerateMultipleEntities(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-enumerate-multi-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":product/label").Type(schema.TypeString).Add().
		Attribute(":product/tags").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	widget := datalog.NewIdentity("widget")
	gadget := datalog.NewIdentity("gadget")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(widget, datalog.NewKeyword(":product/label"), "Widget"))
	require.NoError(t, tx.Add(widget, datalog.NewKeyword(":product/tags"), "waterproof"))
	require.NoError(t, tx.Add(widget, datalog.NewKeyword(":product/tags"), "lightweight"))
	require.NoError(t, tx.Add(gadget, datalog.NewKeyword(":product/label"), "Gadget"))
	require.NoError(t, tx.Add(gadget, datalog.NewKeyword(":product/tags"), "portable"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Each entity's vector should expand independently:
	// Widget: 2 tags, Gadget: 1 tag → 3 total tuples
	results, err := executor.CollectTuples(db.Query(
		`[:find ?label ?tag
		  :where
		  [?e :product/label ?label]
		  [?e :product/tags ?vec]
		  [(enumerate ?vec) [?idx ?tag]]]`))
	require.NoError(t, err)
	require.Len(t, results, 3, "should get Widget's 2 tags + Gadget's 1 tag = 3 tuples")

	// Count per product
	productTags := make(map[string][]string)
	for _, tuple := range results {
		label := tuple[0].(string)
		tag := tuple[1].(string)
		productTags[label] = append(productTags[label], tag)
	}

	require.Len(t, productTags["Widget"], 2, "Widget should have 2 tags")
	require.Len(t, productTags["Gadget"], 1, "Gadget should have 1 tag")
	assert.Contains(t, productTags["Widget"], "waterproof")
	assert.Contains(t, productTags["Widget"], "lightweight")
	assert.Contains(t, productTags["Gadget"], "portable")
}

// TestVectorEnumerateRefWithFilter verifies enumerate on ref vectors with
// subsequent attribute filtering on the referenced entities. Two parent entities
// each have a ref vector pointing to different child entities. Enumerate +
// filter by child attribute should correctly scope results per parent.
func TestVectorEnumerateRefWithFilter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-enumerate-ref-filter-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":folder/name").Type(schema.TypeString).Add().
		Attribute(":folder/items").Type(schema.TypeRef).Vector().Add().
		Attribute(":item/color").Type(schema.TypeKeyword).Add().
		Attribute(":item/label").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	// Create child entities (items) with different colors
	redItem := datalog.NewIdentity("red-item")
	blueItem := datalog.NewIdentity("blue-item")
	greenItem := datalog.NewIdentity("green-item")

	colorAttr := datalog.NewKeyword(":item/color")
	labelAttr := datalog.NewKeyword(":item/label")
	red := datalog.NewKeyword(":color/red")
	blue := datalog.NewKeyword(":color/blue")
	green := datalog.NewKeyword(":color/green")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(redItem, colorAttr, red))
	require.NoError(t, tx.Add(redItem, labelAttr, "Apple"))
	require.NoError(t, tx.Add(blueItem, colorAttr, blue))
	require.NoError(t, tx.Add(blueItem, labelAttr, "Sky"))
	require.NoError(t, tx.Add(greenItem, colorAttr, green))
	require.NoError(t, tx.Add(greenItem, labelAttr, "Leaf"))

	// Create parent entities (folders) with ref vectors to items
	folderA := datalog.NewIdentity("folder-a")
	folderB := datalog.NewIdentity("folder-b")
	nameAttr := datalog.NewKeyword(":folder/name")
	itemsAttr := datalog.NewKeyword(":folder/items")

	require.NoError(t, tx.Add(folderA, nameAttr, "Folder-A"))
	require.NoError(t, tx.Add(folderA, itemsAttr, redItem)) // Folder-A has red item
	require.NoError(t, tx.Add(folderB, nameAttr, "Folder-B"))
	require.NoError(t, tx.Add(folderB, itemsAttr, blueItem))  // Folder-B has blue item
	require.NoError(t, tx.Add(folderB, itemsAttr, greenItem)) // Folder-B also has green item

	_, err = tx.Commit()
	require.NoError(t, err)

	// Query: enumerate folder items, filter to red only
	// Should return only Folder-A's red item, NOT Folder-B
	results, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?itemLabel
		  :in $ ?color
		  :where
		  [?f :folder/name ?folderName]
		  [?f :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color ?color]
		  [?item :item/label ?itemLabel]]`,
		red))
	require.NoError(t, err)
	require.Len(t, results, 1, "only Folder-A has a red item")
	assert.Equal(t, "Folder-A", results[0][0].(string))
	assert.Equal(t, "Apple", results[0][1].(string))
}

// TestVectorEnumerateRefWithJoinsAndFilter adds an indirection layer: instance
// entities reference template entities via a ref, and the templates have ref
// vectors to child entities. The query joins through the indirection, enumerates,
// then filters by child attribute.
func TestVectorEnumerateRefWithJoinsAndFilter(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-enumerate-ref-join-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":instance/template").Type(schema.TypeRef).Add().
		Attribute(":instance/room").Type(schema.TypeRef).Add().
		Attribute(":instance/active").Type(schema.TypeBoolean).Add().
		Attribute(":folder/name").Type(schema.TypeString).Add().
		Attribute(":folder/items").Type(schema.TypeRef).Vector().Add().
		Attribute(":item/color").Type(schema.TypeKeyword).Add().
		Attribute(":item/label").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	// Child entities
	redItem := datalog.NewIdentity("red-item")
	blueItem := datalog.NewIdentity("blue-item")

	red := datalog.NewKeyword(":color/red")
	blue := datalog.NewKeyword(":color/blue")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(redItem, datalog.NewKeyword(":item/color"), red))
	require.NoError(t, tx.Add(redItem, datalog.NewKeyword(":item/label"), "Apple"))
	require.NoError(t, tx.Add(blueItem, datalog.NewKeyword(":item/color"), blue))
	require.NoError(t, tx.Add(blueItem, datalog.NewKeyword(":item/label"), "Sky"))

	// Template entities (folders) with ref vectors
	folderA := datalog.NewIdentity("folder-a")
	folderB := datalog.NewIdentity("folder-b")
	require.NoError(t, tx.Add(folderA, datalog.NewKeyword(":folder/name"), "Folder-A"))
	require.NoError(t, tx.Add(folderA, datalog.NewKeyword(":folder/items"), redItem))
	require.NoError(t, tx.Add(folderB, datalog.NewKeyword(":folder/name"), "Folder-B"))
	require.NoError(t, tx.Add(folderB, datalog.NewKeyword(":folder/items"), blueItem))

	// Instance entities referencing templates (like crawl actors referencing template actors)
	room := datalog.NewIdentity("room-1")
	instA := datalog.NewIdentity("inst-a")
	instB := datalog.NewIdentity("inst-b")
	require.NoError(t, tx.Add(instA, datalog.NewKeyword(":instance/template"), folderA))
	require.NoError(t, tx.Add(instA, datalog.NewKeyword(":instance/room"), room))
	require.NoError(t, tx.Add(instA, datalog.NewKeyword(":instance/active"), true))
	require.NoError(t, tx.Add(instB, datalog.NewKeyword(":instance/template"), folderB))
	require.NoError(t, tx.Add(instB, datalog.NewKeyword(":instance/room"), room))
	require.NoError(t, tx.Add(instB, datalog.NewKeyword(":instance/active"), true))

	_, err = tx.Commit()
	require.NoError(t, err)

	// Step 1: Without instance joins (should work — proven by TestVectorEnumerateRefWithFilter)
	r1, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?itemLabel
		  :in $ ?color
		  :where
		  [?folder :folder/name ?folderName]
		  [?folder :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color ?color]
		  [?item :item/label ?itemLabel]]`,
		red))
	require.NoError(t, err)
	t.Logf("step1 (no instance joins): %d tuples: %v", len(r1), r1)
	require.Len(t, r1, 1, "step1: only Folder-A has a red item")

	// Step 2: Add instance→template join but no :in room filter
	r2, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?itemLabel
		  :where
		  [?inst :instance/template ?folder]
		  [?folder :folder/name ?folderName]
		  [?folder :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color :color/red]
		  [?item :item/label ?itemLabel]]`))
	require.NoError(t, err)
	t.Logf("step2 (instance join, inline color): %d tuples: %v", len(r2), r2)
	require.Len(t, r2, 1, "step2: only Folder-A has a red item")

	// Step 2b: Instance join + :in color (no :in room)
	r2b, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?itemLabel
		  :in $ ?color
		  :where
		  [?inst :instance/template ?folder]
		  [?folder :folder/name ?folderName]
		  [?folder :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color ?color]
		  [?item :item/label ?itemLabel]]`,
		red))
	require.NoError(t, err)
	t.Logf("step2b (instance join, :in color): %d tuples: %v", len(r2b), r2b)
	require.Len(t, r2b, 1, "step2b: only Folder-A has a red item")

	// Step 2c: Instance join + :in room + inline color
	r2c, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?itemLabel
		  :in $ ?room
		  :where
		  [?inst :instance/room ?room]
		  [?inst :instance/template ?folder]
		  [?folder :folder/name ?folderName]
		  [?folder :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color :color/red]
		  [?item :item/label ?itemLabel]]`,
		room))
	require.NoError(t, err)
	t.Logf("step2c (instance join, :in room, inline color): %d tuples: %v", len(r2c), r2c)
	require.Len(t, r2c, 1, "step2c: only Folder-A has a red item")

	// Step 2d: Enumerate only (no color filter) with both :in params
	r2d, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?itemLabel ?idx
		  :in $ ?room ?color
		  :where
		  [?inst :instance/room ?room]
		  [?inst :instance/active true]
		  [?inst :instance/template ?folder]
		  [?folder :folder/name ?folderName]
		  [?folder :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/label ?itemLabel]]`,
		room, red))
	require.NoError(t, err)
	t.Logf("step2d (enumerate only, both :in params, no color filter): %d tuples: %v", len(r2d), r2d)

	// Step 2e: Without label join — just enumerate + color filter, find ?item entity
	// Enable annotation tracing for this query
	db.SetAnnotationHandler(func(event annotations.Event) {
		if event.Name == annotations.JoinHash {
			t.Logf("ANNOTATION [%s]: left.attrs=%v right.attrs=%v result.attrs=%v left.size=%v right.size=%v result.size=%v",
				event.Name,
				event.Data["left.attrs"], event.Data["right.attrs"], event.Data["result.attrs"],
				event.Data["left.size"], event.Data["right.size"], event.Data["result.size"])
		}
	})
	r2e, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?item ?color
		  :in $ ?room ?color
		  :where
		  [?inst :instance/room ?room]
		  [?inst :instance/active true]
		  [?inst :instance/template ?folder]
		  [?folder :folder/name ?folderName]
		  [?folder :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color ?color]]`,
		room, red))
	require.NoError(t, err)
	for i, tuple := range r2e {
		t.Logf("step2e tuple[%d]: folderName=%v item=%v color=%v", i, tuple[0], tuple[1], tuple[2])
	}
	t.Logf("step2e (enumerate + color filter, find ?item): %d tuples", len(r2e))
	db.SetAnnotationHandler(nil) // disable after step2e

	// Step 3: Full query with :in room and color
	r3, err := executor.CollectTuples(db.Query(
		`[:find ?folderName ?itemLabel
		  :in $ ?room ?color
		  :where
		  [?inst :instance/room ?room]
		  [?inst :instance/active true]
		  [?inst :instance/template ?folder]
		  [?folder :folder/name ?folderName]
		  [?folder :folder/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color ?color]
		  [?item :item/label ?itemLabel]]`,
		room, red))
	require.NoError(t, err)
	t.Logf("step3 (full query): %d tuples: %v", len(r3), r3)
	require.Len(t, r3, 1, "step3: only Folder-A has a red item")
	assert.Equal(t, "Folder-A", r3[0][0].(string))
	assert.Equal(t, "Apple", r3[0][1].(string))
}

// TestVectorSetTruncate verifies Set() handles removing elements from the end.
func TestVectorSetTruncate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-set-truncate-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	skills := datalog.NewKeyword(":character/skills")

	// Initial state: ["stealth", "archery", "lockpicking", "magic"]
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	require.NoError(t, tx1.Add(alice, skills, "lockpicking"))
	require.NoError(t, tx1.Add(alice, skills, "magic"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Truncate to first two: ["stealth", "archery"]
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"stealth", "archery"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify result
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(t, matcher, alice, skills)
	require.True(t, found)
	vec := result.([]string)
	require.Len(t, vec, 2)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
}

// TestPlannerReordersDataPatternBeforeEnumerate reproduces a bug where the
// greedy clause planner reorders a data pattern (e.g. [?item :item/color ?color])
// before the enumerate expression that provides ?item. Data patterns score ~210
// (100 base + 100 per constant + 10 per available var) while expressions score
// ~20, so the planner always picks patterns first. When the pattern runs without
// ?item from enumerate, it scans ALL items matching the color and joins on ?color
// alone — producing a cross-product instead of a per-container filtered join.
//
// Trigger condition: two :in parameters where one (?room) anchors early patterns
// and the other (?color) is used in a post-enumerate pattern. Both end up in the
// same input relation, so ?color is "available" when scoring the color pattern,
// boosting its score above enumerate's.
func TestPlannerReordersDataPatternBeforeEnumerate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "planner-reorder-enumerate-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":container/name").Type(schema.TypeString).Add().
		Attribute(":container/room").Type(schema.TypeRef).Add().
		Attribute(":container/items").Type(schema.TypeRef).Vector().Add().
		Attribute(":item/color").Type(schema.TypeKeyword).Add().
		Attribute(":item/label").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	red := datalog.NewKeyword(":color/red")
	blue := datalog.NewKeyword(":color/blue")
	room := datalog.NewIdentity("room-1")
	redItem := datalog.NewIdentity("red-item")
	blueItem := datalog.NewIdentity("blue-item")
	containerA := datalog.NewIdentity("container-a")
	containerB := datalog.NewIdentity("container-b")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(redItem, datalog.NewKeyword(":item/color"), red))
	require.NoError(t, tx.Add(redItem, datalog.NewKeyword(":item/label"), "Apple"))
	require.NoError(t, tx.Add(blueItem, datalog.NewKeyword(":item/color"), blue))
	require.NoError(t, tx.Add(blueItem, datalog.NewKeyword(":item/label"), "Sky"))
	require.NoError(t, tx.Add(containerA, datalog.NewKeyword(":container/name"), "A"))
	require.NoError(t, tx.Add(containerA, datalog.NewKeyword(":container/room"), room))
	require.NoError(t, tx.Add(containerA, datalog.NewKeyword(":container/items"), redItem))
	require.NoError(t, tx.Add(containerB, datalog.NewKeyword(":container/name"), "B"))
	require.NoError(t, tx.Add(containerB, datalog.NewKeyword(":container/room"), room))
	require.NoError(t, tx.Add(containerB, datalog.NewKeyword(":container/items"), blueItem))
	_, err = tx.Commit()
	require.NoError(t, err)

	// The query: find container names and item labels where the container is in
	// the given room and contains an item of the given color.
	//
	// Correct clause order: room pattern → name → items → enumerate → color → label
	// Buggy planner order: room → name → items → color (before enumerate!) → label → enumerate
	//
	// With the bug, [?item :item/color ?color] runs before enumerate provides ?item,
	// so it scans ALL items with color=red. The join with the accumulated relation
	// is on ?color only (not ?item), creating a cross-product: every container gets
	// ?item=redItem regardless of what's actually in its vector.
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?name ?label
		  :in $ ?room ?color
		  :where
		  [?c :container/room ?room]
		  [?c :container/name ?name]
		  [?c :container/items ?vec]
		  [(enumerate ?vec) [?idx ?item]]
		  [?item :item/color ?color]
		  [?item :item/label ?label]]`,
		room, red))
	require.NoError(t, err)
	t.Logf("tuples: %v", tuples)
	require.Len(t, tuples, 1, "only container A has a red item; container B has blue")
	assert.Equal(t, "A", tuples[0][0])
	assert.Equal(t, "Apple", tuples[0][1])
}

// TestTypedVector_AllBranches exercises every branch of typedVector.
func TestTypedVector_AllBranches(t *testing.T) {
	t.Run("TypeString", func(t *testing.T) {
		input := []any{"a", "b", "c"}
		result := typedVector(input, schema.TypeString)
		vec, ok := result.([]string)
		require.True(t, ok, "expected []string, got %T", result)
		assert.Equal(t, []string{"a", "b", "c"}, vec)
	})

	t.Run("TypeString/empty", func(t *testing.T) {
		result := typedVector([]any{}, schema.TypeString)
		vec, ok := result.([]string)
		require.True(t, ok, "expected []string, got %T", result)
		assert.Empty(t, vec)
	})

	t.Run("TypeLong", func(t *testing.T) {
		input := []any{int64(10), int64(20), int64(30)}
		result := typedVector(input, schema.TypeLong)
		vec, ok := result.([]int64)
		require.True(t, ok, "expected []int64, got %T", result)
		assert.Equal(t, []int64{10, 20, 30}, vec)
	})

	t.Run("TypeLong/empty", func(t *testing.T) {
		result := typedVector([]any{}, schema.TypeLong)
		vec, ok := result.([]int64)
		require.True(t, ok, "expected []int64, got %T", result)
		assert.Empty(t, vec)
	})

	t.Run("TypeDouble", func(t *testing.T) {
		input := []any{1.5, 2.7, 3.14}
		result := typedVector(input, schema.TypeDouble)
		vec, ok := result.([]float64)
		require.True(t, ok, "expected []float64, got %T", result)
		assert.Equal(t, []float64{1.5, 2.7, 3.14}, vec)
	})

	t.Run("TypeDouble/empty", func(t *testing.T) {
		result := typedVector([]any{}, schema.TypeDouble)
		vec, ok := result.([]float64)
		require.True(t, ok, "expected []float64, got %T", result)
		assert.Empty(t, vec)
	})

	t.Run("TypeBoolean", func(t *testing.T) {
		input := []any{true, false, true}
		result := typedVector(input, schema.TypeBoolean)
		vec, ok := result.([]bool)
		require.True(t, ok, "expected []bool, got %T", result)
		assert.Equal(t, []bool{true, false, true}, vec)
	})

	t.Run("TypeBoolean/empty", func(t *testing.T) {
		result := typedVector([]any{}, schema.TypeBoolean)
		vec, ok := result.([]bool)
		require.True(t, ok, "expected []bool, got %T", result)
		assert.Empty(t, vec)
	})

	t.Run("TypeRef/fallback", func(t *testing.T) {
		id1 := datalog.NewIdentity("a")
		id2 := datalog.NewIdentity("b")
		input := []any{id1, id2}
		result := typedVector(input, schema.TypeRef)
		vec, ok := result.([]any)
		require.True(t, ok, "TypeRef should fall through to []any, got %T", result)
		assert.Len(t, vec, 2)
	})

	t.Run("unknown/fallback", func(t *testing.T) {
		input := []any{"x", "y"}
		result := typedVector(input, "")
		vec, ok := result.([]any)
		require.True(t, ok, "empty ValueType should fall through to []any, got %T", result)
		assert.Len(t, vec, 2)
	})

	t.Run("TypeString/mixed", func(t *testing.T) {
		input := []any{"a", int64(42)}
		result := typedVector(input, schema.TypeString)
		// Mixed types: should return original []any
		vec, ok := result.([]any)
		require.True(t, ok, "mixed elements should return []any, got %T", result)
		assert.Len(t, vec, 2)
	})

	t.Run("TypeLong/mixed", func(t *testing.T) {
		input := []any{int64(1), "not a number"}
		result := typedVector(input, schema.TypeLong)
		vec, ok := result.([]any)
		require.True(t, ok, "mixed elements should return []any, got %T", result)
		assert.Len(t, vec, 2)
	})
}

// TestVectorTypeDouble verifies TypeDouble vectors through LookupAttribute and query.
func TestVectorTypeDouble(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-double-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":sensor/readings").Type(schema.TypeDouble).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	sensor := datalog.NewIdentity("sensor1")
	readings := datalog.NewKeyword(":sensor/readings")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(sensor, readings, 23.5))
	require.NoError(t, tx.Add(sensor, readings, 24.1))
	require.NoError(t, tx.Add(sensor, readings, 22.8))
	_, err = tx.Commit()
	require.NoError(t, err)

	// LookupAttribute path
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := requireAttributeLookup(t, matcher, sensor, readings)
	require.True(t, found)
	vec, ok := result.([]float64)
	require.True(t, ok, "expected []float64, got %T", result)
	require.Len(t, vec, 3)
	assert.Equal(t, 23.5, vec[0])
	assert.Equal(t, 24.1, vec[1])
	assert.Equal(t, 22.8, vec[2])

	// Query path
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where [?e :sensor/readings ?v]]`,
		sensor,
	))
	require.NoError(t, err)
	require.Len(t, tuples, 1)
	qvec, ok := tuples[0][0].([]float64)
	require.True(t, ok, "query should return []float64, got %T", tuples[0][0])
	assert.Equal(t, []float64{23.5, 24.1, 22.8}, qvec)
}

// TestVectorTypeBoolean verifies TypeBoolean vectors through LookupAttribute and query.
func TestVectorTypeBoolean(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-bool-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":config/flags").Type(schema.TypeBoolean).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	cfg := datalog.NewIdentity("cfg1")
	flags := datalog.NewKeyword(":config/flags")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(cfg, flags, true))
	require.NoError(t, tx.Add(cfg, flags, false))
	require.NoError(t, tx.Add(cfg, flags, true))
	_, err = tx.Commit()
	require.NoError(t, err)

	// LookupAttribute path
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := requireAttributeLookup(t, matcher, cfg, flags)
	require.True(t, found)
	vec, ok := result.([]bool)
	require.True(t, ok, "expected []bool, got %T", result)
	require.Len(t, vec, 3)
	assert.Equal(t, true, vec[0])
	assert.Equal(t, false, vec[1])
	assert.Equal(t, true, vec[2])

	// Query path
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where [?e :config/flags ?v]]`,
		cfg,
	))
	require.NoError(t, err)
	require.Len(t, tuples, 1)
	qvec, ok := tuples[0][0].([]bool)
	require.True(t, ok, "query should return []bool, got %T", tuples[0][0])
	assert.Equal(t, []bool{true, false, true}, qvec)
}

// TestVectorTypeLong_QueryPath verifies TypeLong vectors through the query executor.
func TestVectorTypeLong_QueryPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-long-query-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":event/scores").Type(schema.TypeLong).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	game := datalog.NewIdentity("game1")
	scores := datalog.NewKeyword(":event/scores")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(game, scores, int64(100)))
	require.NoError(t, tx.Add(game, scores, int64(250)))
	require.NoError(t, tx.Add(game, scores, int64(175)))
	_, err = tx.Commit()
	require.NoError(t, err)

	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?v :in $ ?e :where [?e :event/scores ?v]]`,
		game,
	))
	require.NoError(t, err)
	require.Len(t, tuples, 1)
	vec, ok := tuples[0][0].([]int64)
	require.True(t, ok, "query should return []int64, got %T", tuples[0][0])
	assert.Equal(t, []int64{100, 250, 175}, vec)
}

// TestSetWithTypedSlices verifies that tx.Set() accepts typed slices ([]string,
// []int64, etc.) directly, not just []interface{}. toAnySlice uses
// reflection to convert them.
func TestSetWithTypedSlices(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "set-typed-slices-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Attribute(":event/scores").Type(schema.TypeLong).Vector().Add().
		Attribute(":sensor/readings").Type(schema.TypeDouble).Vector().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	e := datalog.NewIdentity("entity1")

	t.Run("vector []string", func(t *testing.T) {
		tx := db.NewTransaction()
		require.NoError(t, tx.Set(e, datalog.NewKeyword(":character/skills"), []string{"stealth", "archery"}))
		_, err := tx.Commit()
		require.NoError(t, err)

		matcher := NewBadgerMatcher(db.store)
		matcher.SetSchema(s)
		result, found := requireAttributeLookup(t, matcher, e, datalog.NewKeyword(":character/skills"))
		require.True(t, found)
		vec, ok := result.([]string)
		require.True(t, ok, "expected []string, got %T", result)
		assert.Equal(t, []string{"stealth", "archery"}, vec)
	})

	t.Run("vector []int64", func(t *testing.T) {
		tx := db.NewTransaction()
		require.NoError(t, tx.Set(e, datalog.NewKeyword(":event/scores"), []int64{100, 250, 175}))
		_, err := tx.Commit()
		require.NoError(t, err)

		matcher := NewBadgerMatcher(db.store)
		matcher.SetSchema(s)
		result, found := requireAttributeLookup(t, matcher, e, datalog.NewKeyword(":event/scores"))
		require.True(t, found)
		vec, ok := result.([]int64)
		require.True(t, ok, "expected []int64, got %T", result)
		assert.Equal(t, []int64{100, 250, 175}, vec)
	})

	t.Run("vector []float64", func(t *testing.T) {
		tx := db.NewTransaction()
		require.NoError(t, tx.Set(e, datalog.NewKeyword(":sensor/readings"), []float64{23.5, 24.1}))
		_, err := tx.Commit()
		require.NoError(t, err)

		matcher := NewBadgerMatcher(db.store)
		matcher.SetSchema(s)
		result, found := requireAttributeLookup(t, matcher, e, datalog.NewKeyword(":sensor/readings"))
		require.True(t, found)
		vec, ok := result.([]float64)
		require.True(t, ok, "expected []float64, got %T", result)
		assert.Equal(t, []float64{23.5, 24.1}, vec)
	})

	t.Run("many []string", func(t *testing.T) {
		tx := db.NewTransaction()
		require.NoError(t, tx.Set(e, datalog.NewKeyword(":person/tags"), []string{"developer", "lead"}))
		_, err := tx.Commit()
		require.NoError(t, err)

		matcher := NewBadgerMatcher(db.store)
		matcher.SetSchema(s)
		result, found := requireAttributeLookup(t, matcher, e, datalog.NewKeyword(":person/tags"))
		require.True(t, found)
		// Cardinality-many returns []interface{} (unordered set)
		vals, ok := result.([]interface{})
		require.True(t, ok, "expected []interface{}, got %T", result)
		tagSet := make(map[string]bool)
		for _, v := range vals {
			tagSet[v.(string)] = true
		}
		assert.True(t, tagSet["developer"])
		assert.True(t, tagSet["lead"])
	})
}

// TestTypeDefault verifies BadgerMatcher.TypeDefault converts default values
// to match schema types for vector and many attributes.
func TestTypeDefault(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "type-default-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":entity/skills").Type(schema.TypeString).Vector().Add().
		Attribute(":entity/scores").Type(schema.TypeLong).Vector().Add().
		Attribute(":entity/tags").Type(schema.TypeString).Many().Add().
		Attribute(":entity/name").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	t.Run("vector string empty", func(t *testing.T) {
		result := matcher.TypeDefault(datalog.NewKeyword(":entity/skills"), []interface{}{})
		_, ok := result.([]string)
		assert.True(t, ok, "empty []interface{} for TypeString vector should become []string, got %T", result)
	})

	t.Run("vector string populated", func(t *testing.T) {
		result := matcher.TypeDefault(datalog.NewKeyword(":entity/skills"), []interface{}{"a", "b"})
		vec, ok := result.([]string)
		require.True(t, ok, "expected []string, got %T", result)
		assert.Equal(t, []string{"a", "b"}, vec)
	})

	t.Run("vector long populated", func(t *testing.T) {
		result := matcher.TypeDefault(datalog.NewKeyword(":entity/scores"), []interface{}{int64(1), int64(2)})
		vec, ok := result.([]int64)
		require.True(t, ok, "expected []int64, got %T", result)
		assert.Equal(t, []int64{1, 2}, vec)
	})

	t.Run("many string", func(t *testing.T) {
		result := matcher.TypeDefault(datalog.NewKeyword(":entity/tags"), []interface{}{"x"})
		// Many also gets typed via typedVector
		_, ok := result.([]string)
		assert.True(t, ok, "expected []string for many, got %T", result)
	})

	t.Run("cardinality-one passthrough", func(t *testing.T) {
		result := matcher.TypeDefault(datalog.NewKeyword(":entity/name"), "default")
		assert.Equal(t, "default", result, "cardinality-one should pass through unchanged")
	})

	t.Run("unknown attribute passthrough", func(t *testing.T) {
		result := matcher.TypeDefault(datalog.NewKeyword(":unknown/attr"), []interface{}{"x"})
		_, ok := result.([]interface{})
		assert.True(t, ok, "unknown attribute should pass through as []interface{}, got %T", result)
	})

	t.Run("no schema passthrough", func(t *testing.T) {
		noSchemaMatcher := NewBadgerMatcher(db.store)
		// No SetSchema call
		result := noSchemaMatcher.TypeDefault(datalog.NewKeyword(":entity/skills"), []interface{}{"x"})
		_, ok := result.([]interface{})
		assert.True(t, ok, "no schema should pass through, got %T", result)
	})
}

// TestVectorClearedVsNeverSet verifies the distinction between a vector that
// was explicitly cleared (Set to []) and one that was never written at all.
// Cleared vectors return ([]string{}, true); never-set return (nil, false).
func TestVectorClearedVsNeverSet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-cleared-vs-never-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	skills := datalog.NewKeyword(":character/skills")

	// Alice: add skills then clear them
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, skills, "stealth"))
	require.NoError(t, tx.Add(alice, skills, "archery"))
	_, err = tx.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Bob: never had skills written at all

	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	// Alice: explicitly cleared — tombstones exist, returns empty typed slice
	aliceResult, aliceFound := requireAttributeLookup(t, matcher, alice, skills)
	assert.True(t, aliceFound, "cleared vector should be found (tombstones exist)")
	assert.Equal(t, []string{}, aliceResult, "cleared vector should return typed empty slice")

	// Bob: never set — no datoms at all
	bobResult, bobFound := requireAttributeLookup(t, matcher, bob, skills)
	assert.False(t, bobFound, "never-set vector should not be found")
	assert.Nil(t, bobResult, "never-set vector should return nil")
}

// =============================================================================
// Vector Literal Matching in Data Patterns
// =============================================================================
//
// Bug: [?e :attr []] treated as wildcard instead of "match empty vector".
// See docs/bugs/BUG_EMPTY_VECTOR_LITERAL_MATCHES_NONEMPTY.md

// TestVectorLiteralMatch verifies that vector literals in data patterns match
// by exact equality, not as wildcards.
func TestVectorLiteralMatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-literal-match-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":entity/name").Type(schema.TypeString).Add().
		Attribute(":entity/lore").Type(schema.TypeString).Vector().Add().
		Attribute(":entity/scores").Type(schema.TypeLong).Vector().Add().
		Attribute(":entity/flags").Type(schema.TypeKeyword).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	name := datalog.NewKeyword(":entity/name")
	lore := datalog.NewKeyword(":entity/lore")
	scores := datalog.NewKeyword(":entity/scores")
	flags := datalog.NewKeyword(":entity/flags")

	hasLore := datalog.NewIdentity("has-lore")
	emptyLore := datalog.NewIdentity("empty-lore")
	noLore := datalog.NewIdentity("no-lore")
	twoLore := datalog.NewIdentity("two-lore")
	hasScores := datalog.NewIdentity("has-scores")
	twoScores := datalog.NewIdentity("two-scores")
	emptyScores := datalog.NewIdentity("empty-scores")
	hasFlags := datalog.NewIdentity("has-flags")
	twoFlags := datalog.NewIdentity("two-flags")
	emptyFlags := datalog.NewIdentity("empty-flags")

	// Populate data
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(hasLore, name, "HasLore"))
	require.NoError(t, tx.Add(hasLore, lore, "Deep in the mountains..."))

	require.NoError(t, tx.Add(emptyLore, name, "EmptyLore"))
	// emptyLore gets lore added then cleared below

	require.NoError(t, tx.Add(noLore, name, "NoLore"))
	// noLore: no lore attribute at all

	require.NoError(t, tx.Add(twoLore, name, "TwoLore"))
	require.NoError(t, tx.Add(twoLore, lore, "alpha"))
	require.NoError(t, tx.Add(twoLore, lore, "beta"))

	require.NoError(t, tx.Add(hasScores, name, "HasScores"))
	require.NoError(t, tx.Add(hasScores, scores, int64(100)))
	require.NoError(t, tx.Add(hasScores, scores, int64(250)))
	require.NoError(t, tx.Add(hasScores, scores, int64(175)))

	require.NoError(t, tx.Add(twoScores, name, "TwoScores"))
	require.NoError(t, tx.Add(twoScores, scores, int64(50)))
	require.NoError(t, tx.Add(twoScores, scores, int64(75)))

	require.NoError(t, tx.Add(emptyScores, name, "EmptyScores"))
	// emptyScores gets scores added then cleared below

	require.NoError(t, tx.Add(hasFlags, name, "HasFlags"))
	require.NoError(t, tx.Add(hasFlags, flags, datalog.NewKeyword(":flag/active")))
	require.NoError(t, tx.Add(hasFlags, flags, datalog.NewKeyword(":flag/visible")))

	require.NoError(t, tx.Add(twoFlags, name, "TwoFlags"))
	require.NoError(t, tx.Add(twoFlags, flags, datalog.NewKeyword(":flag/hidden")))
	require.NoError(t, tx.Add(twoFlags, flags, datalog.NewKeyword(":flag/locked")))

	require.NoError(t, tx.Add(emptyFlags, name, "EmptyFlags"))
	// emptyFlags gets flags added then cleared below

	_, err = tx.Commit()
	require.NoError(t, err)

	// Create then clear vectors to get "empty but set" state (tombstones)
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(emptyLore, lore, "placeholder"))
	_, err = tx2.Commit()
	require.NoError(t, err)
	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Set(emptyLore, lore, []interface{}{}))
	_, err = tx3.Commit()
	require.NoError(t, err)

	tx4 := db.NewTransaction()
	require.NoError(t, tx4.Add(emptyScores, scores, int64(999)))
	_, err = tx4.Commit()
	require.NoError(t, err)
	tx5 := db.NewTransaction()
	require.NoError(t, tx5.Set(emptyScores, scores, []interface{}{}))
	_, err = tx5.Commit()
	require.NoError(t, err)

	tx6 := db.NewTransaction()
	require.NoError(t, tx6.Add(emptyFlags, flags, datalog.NewKeyword(":flag/placeholder")))
	_, err = tx6.Commit()
	require.NoError(t, err)
	tx7 := db.NewTransaction()
	require.NoError(t, tx7.Set(emptyFlags, flags, []interface{}{}))
	_, err = tx7.Commit()
	require.NoError(t, err)

	// collectNames collects names from query results
	collectNames := func(t *testing.T, q string) []string {
		t.Helper()
		results, err := executor.CollectTuples(db.Query(q))
		require.NoError(t, err)
		names := make([]string, len(results))
		for i, tuple := range results {
			names[i] = tuple[0].(string)
		}
		return names
	}

	// --- String vector tests ---

	t.Run("string/empty literal matches only empty vector", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/lore []]]`)
		assert.Equal(t, []string{"EmptyLore"}, names)
	})

	t.Run("string/empty literal excludes non-empty", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/lore []]]`)
		assert.NotContains(t, names, "HasLore")
		assert.NotContains(t, names, "TwoLore")
	})

	t.Run("string/empty literal excludes missing", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/lore []]]`)
		assert.NotContains(t, names, "NoLore")
	})

	t.Run("string/populated literal matches exact", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/lore ["Deep in the mountains..."]]]`)
		assert.Equal(t, []string{"HasLore"}, names)
	})

	t.Run("string/populated literal no partial match", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/lore ["alpha"]]]`)
		assert.Empty(t, names, "subset should not match")
	})

	t.Run("string/multi-element literal matches exact", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/lore ["alpha" "beta"]]]`)
		assert.Equal(t, []string{"TwoLore"}, names)
	})

	t.Run("string/unbound V returns all vectors", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/lore ?lore]]`)
		// Empty vectors produce zero tuples (treated as "not found"),
		// so unbound V only returns entities with non-empty vectors.
		assert.Len(t, names, 2, "should find HasLore, TwoLore (empty vectors produce no tuples)")
		assert.Contains(t, names, "HasLore")
		assert.Contains(t, names, "TwoLore")
	})

	// --- Int64 vector tests ---

	t.Run("int64/empty literal", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/scores []]]`)
		assert.Equal(t, []string{"EmptyScores"}, names)
	})

	t.Run("int64/populated literal matches exact", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/scores [100 250 175]]]`)
		assert.Equal(t, []string{"HasScores"}, names)
	})

	t.Run("int64/populated literal excludes other non-empty", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/scores [100 250 175]]]`)
		assert.NotContains(t, names, "TwoScores")
	})

	t.Run("int64/multi-element literal matches exact", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/scores [50 75]]]`)
		assert.Equal(t, []string{"TwoScores"}, names)
	})

	t.Run("int64/wrong values no match", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/scores [100 250]]]`)
		assert.Empty(t, names)
	})

	t.Run("int64/unbound V returns all non-empty", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/scores ?s]]`)
		assert.Len(t, names, 2, "should find HasScores, TwoScores")
		assert.Contains(t, names, "HasScores")
		assert.Contains(t, names, "TwoScores")
	})

	// --- Keyword vector tests ---

	t.Run("keyword/empty literal", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/flags []]]`)
		assert.Equal(t, []string{"EmptyFlags"}, names)
	})

	t.Run("keyword/populated literal matches exact", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/flags [:flag/active :flag/visible]]]`)
		assert.Equal(t, []string{"HasFlags"}, names)
	})

	t.Run("keyword/populated literal excludes other non-empty", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/flags [:flag/active :flag/visible]]]`)
		assert.NotContains(t, names, "TwoFlags")
	})

	t.Run("keyword/multi-element literal matches exact", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/flags [:flag/hidden :flag/locked]]]`)
		assert.Equal(t, []string{"TwoFlags"}, names)
	})

	t.Run("keyword/wrong values no match", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/flags [:flag/active]]]`)
		assert.Empty(t, names)
	})

	t.Run("keyword/unbound V returns all non-empty", func(t *testing.T) {
		names := collectNames(t,
			`[:find ?name :where [?e :entity/name ?name] [?e :entity/flags ?f]]`)
		assert.Len(t, names, 2, "should find HasFlags, TwoFlags")
		assert.Contains(t, names, "HasFlags")
		assert.Contains(t, names, "TwoFlags")
	})
}

// TestVectorLiteralWithOr verifies the (or ...) scenario from the bug report:
// (or-default [(missing? $ ?e :attr)] [?e :attr []]) should return entities where the
// attribute is missing OR empty, but NOT entities with non-empty vectors.
func TestVectorLiteralWithOr(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vector-literal-or-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":entity/name").Type(schema.TypeString).Add().
		Attribute(":entity/lore").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	name := datalog.NewKeyword(":entity/name")
	lore := datalog.NewKeyword(":entity/lore")

	hasLore := datalog.NewIdentity("has-lore")
	emptyLore := datalog.NewIdentity("empty-lore")
	noLore := datalog.NewIdentity("no-lore")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(hasLore, name, "HasLore"))
	require.NoError(t, tx.Add(hasLore, lore, "Deep in the mountains..."))
	require.NoError(t, tx.Add(emptyLore, name, "EmptyLore"))
	require.NoError(t, tx.Add(noLore, name, "NoLore"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Create then clear emptyLore's vector
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(emptyLore, lore, "placeholder"))
	_, err = tx2.Commit()
	require.NoError(t, err)
	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Set(emptyLore, lore, []interface{}{}))
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Sanity check: missing? alone should find NoLore
	missingResults, err := executor.CollectTuples(db.Query(
		`[:find ?name :where
		  [?e :entity/name ?name]
		  [(missing? $ ?e :entity/lore)]]`))
	require.NoError(t, err)
	t.Logf("missing? alone: %v", missingResults)

	// Sanity check: [] alone should find EmptyLore
	emptyResults, err := executor.CollectTuples(db.Query(
		`[:find ?name :where
		  [?e :entity/name ?name]
		  [?e :entity/lore []]]`))
	require.NoError(t, err)
	t.Logf("[] alone: %v", emptyResults)

	results, err := executor.CollectTuples(db.Query(
		`[:find ?name :where
		  [?e :entity/name ?name]
		  (or-default [(missing? $ ?e :entity/lore)]
		      [?e :entity/lore []])]`))
	require.NoError(t, err)
	t.Logf("or combined: %v", results)

	names := make([]string, len(results))
	for i, tuple := range results {
		names[i] = tuple[0].(string)
	}

	assert.Contains(t, names, "NoLore", "missing attribute should match")
	assert.Contains(t, names, "EmptyLore", "empty vector should match")
	assert.NotContains(t, names, "HasLore", "non-empty vector should NOT match")
	assert.Len(t, names, 2)
}
