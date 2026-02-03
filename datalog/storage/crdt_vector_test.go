package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
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

	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found, "should find skills")

	vec, ok := result.([]any)
	require.True(t, ok, "result should be []any")
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

	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)

	vec := result.([]any)
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

	// No data added - should return not found
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := matcher.LookupAttribute(alice, skills)
	assert.False(t, found, "empty vector should return not found")
	assert.Nil(t, result)
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

	result, found := matcher.LookupAttribute(game, scores)
	require.True(t, found)

	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
	require.Len(t, vec, 3)

	// Now use Set() to replace entire vector
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"magic", "alchemy"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify the vector was replaced
	matcher2 := NewBadgerMatcher(db.store)
	matcher2.SetSchema(s)
	result2, found2 := matcher2.LookupAttribute(alice, skills)
	require.True(t, found2)
	vec2 := result2.([]any)
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

	// Verify vector is empty (not found)
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	result, found := matcher.LookupAttribute(alice, skills)
	assert.False(t, found, "empty vector should return not found")
	assert.Nil(t, result)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	results, err := db.ExecuteQuery(`[:find ?name :where [?e :character/name ?name] [?e :character/skills ?skills]]`)
	require.NoError(t, err)
	require.Len(t, results, 2, "should find both characters with skills")

	// Verify both names are present (order not guaranteed)
	names := make(map[string]bool)
	for _, row := range results {
		names[row[0].(string)] = true
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
	basicResults, err := db.ExecuteQuery(`[:find ?name :where [?e :character/name ?name]]`)
	require.NoError(t, err)
	t.Logf("Basic query results: %d rows", len(basicResults))
	for i, row := range basicResults {
		t.Logf("  Basic row %d: %v", i, row)
	}

	// Check if LookupAttribute still works
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)
	lookupResult, found := matcher.LookupAttribute(alice, skills)
	t.Logf("LookupAttribute found=%v, result=%v (type: %T)", found, lookupResult, lookupResult)

	// Query with E bound via join - this binds ?e first, then queries skills
	// Pattern: [?e :name "Alice"] binds ?e, then [?e :skills ?skills] has E bound
	results, err := db.ExecuteQuery(`[:find ?skills :where [?e :character/name "Alice"] [?e :character/skills ?skills]]`)
	require.NoError(t, err, "query with bound E should not error")

	t.Logf("Vector query results count: %d", len(results))
	for i, row := range results {
		t.Logf("  Row %d: %v (type: %T)", i, row[0], row[0])
	}

	// With E bound via join, should return 1 row with the vector
	require.Len(t, results, 1, "should return 1 row with vector")

	// The value should be a slice, not raw bytes
	vec, ok := results[0][0].([]interface{})
	require.True(t, ok, "skills should be []interface{}, got %T", results[0][0])
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
	results, err := db.ExecuteQuery(`[:find ?skills :where [?e :character/skills ?skills]]`)
	require.NoError(t, err, "query for vector values should not error")

	// Log what we actually get for debugging
	t.Logf("Results count: %d", len(results))
	for i, row := range results {
		t.Logf("  Row %d: %v (type: %T)", i, row[0], row[0])
	}

	// The behavior here depends on implementation:
	// Option A: Returns the whole vector as one result
	// Option B: Returns each element as separate rows
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
	results, err := db.ExecuteQuery(`[:find ?name ?skills :where [?e :character/name ?name] [?e :character/skills ?skills]]`)
	require.NoError(t, err, "join query should not error")

	t.Logf("Results count: %d", len(results))
	for i, row := range results {
		t.Logf("  Row %d: name=%v, skills=%v (type: %T)", i, row[0], row[1], row[1])
	}

	require.NotEmpty(t, results, "should return some results for join query")

	// Verify Alice is in the results
	foundAlice := false
	for _, row := range results {
		if row[0] == "Alice" {
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
	results, err := db.ExecuteQuery(`[:find (pull ?e [:character/name :character/skills]) :where [?e :character/name "Alice"]]`)
	require.NoError(t, err)
	require.Len(t, results, 1)

	// Parse pull result
	pullResult, ok := results[0][0].(map[string]interface{})
	require.True(t, ok, "pull result should be a map, got %T: %v", results[0][0], results[0][0])

	// Check name (Pull API returns keys without leading colon)
	assert.Equal(t, "Alice", pullResult["character/name"])

	// Check skills is a slice in correct order
	skillsResult, ok := pullResult["character/skills"].([]interface{})
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result2, found2 := matcher2.LookupAttribute(alice, skills)
	require.True(t, found2)
	vec2 := result2.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	nameVal, found := matcher.LookupAttribute(alice, name)
	require.True(t, found)
	assert.Equal(t, "Alice", nameVal)

	// Cardinality-many: returns slice of all set members
	tagVal, found := matcher.LookupAttribute(alice, tags)
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
	skillsVal, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := skillsVal.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
	require.Len(t, vec, 3)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
	assert.Equal(t, "lockpicking", vec[2])
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
	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]any)
	require.Len(t, vec, 2)
	assert.Equal(t, "stealth", vec[0])
	assert.Equal(t, "archery", vec[1])
}
