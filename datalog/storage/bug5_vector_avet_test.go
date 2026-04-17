package storage

// Bug #5 Test Cases
//
// These tests verify that AVET/VAET indices work correctly for vectors.
// Currently, RGAElement wrapper in V breaks these lookups.
//
// See: docs/proposals/CRDT_VECTOR_STORAGE_IMPLEMENTATION_PLAN.md (Bug #5 section)

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// encodeKeyword renders a keyword as its raw string bytes. Used only
// by the Bug #5 tests below, which build AVET index prefixes by hand
// to verify lookup behavior. This is NOT the production attribute
// encoding (which is a 32-byte hash via ToStorageDatom); these tests
// rely on the truncation behavior of `copy(prefix[1:33], aBytes[:])`
// when aBytes is shorter than 32 bytes.
func encodeKeyword(kw datalog.Keyword) []byte {
	return []byte(kw.String())
}

// TestVectorAVETLookup verifies that AVET index works for vector elements.
//
// Bug #5: Currently fails because V contains RGAElement bytes (TypeBytes),
// but AVET lookup searches for raw value (TypeString).
//
// Expected: Query [?e :skills "stealth"] should find entities with that skill.
func TestVectorAVETLookup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bug5-avet-lookup")
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
	bob := datalog.NewIdentity("bob")
	skills := datalog.NewKeyword(":character/skills")

	// Add skills to alice
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Add skills to bob (bob also has stealth)
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(bob, skills, "stealth"))
	require.NoError(t, tx2.Add(bob, skills, "swordplay"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Use AVET scan to find all entities with "stealth" skill
	// This mimics what a query like [?e :skills "stealth"] would do
	encoder := NewKeyEncoder(BinaryStrategy)

	// Encode the value with type prefix
	vType := byte(datalog.TypeString)
	vData := []byte("stealth")
	vBytes := append([]byte{vType}, vData...)

	// Encode attribute
	aBytes := encodeKeyword(skills)

	// Build AVET prefix: [prefix][A][V]
	prefix := make([]byte, 1+32+len(vBytes))
	prefix[0] = byte(AVET)
	copy(prefix[1:33], aBytes[:])
	copy(prefix[33:], vBytes)

	// Build end range
	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1]++ // Increment last byte to get exclusive end

	// Scan AVET index
	iter, err := db.store.Scan(AVET, prefix, end)
	require.NoError(t, err)
	defer iter.Close()

	// Collect found entities
	var foundEntities []string
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}
		if datom.E.Hash() == alice.Hash() {
			foundEntities = append(foundEntities, "alice")
		} else if datom.E.Hash() == bob.Hash() {
			foundEntities = append(foundEntities, "bob")
		}
	}

	// BUG #5: This currently fails - AVET lookup doesn't find vector elements
	// because V contains RGAElement bytes, not raw "stealth" string
	assert.Contains(t, foundEntities, "alice", "should find alice with stealth skill")
	assert.Contains(t, foundEntities, "bob", "should find bob with stealth skill")
	assert.Len(t, foundEntities, 2, "should find exactly 2 entities with stealth")

	// Also log what we found for debugging
	t.Logf("Found entities with stealth: %v (expected: [alice bob])", foundEntities)

	// Cross-check: verify the data IS there via LookupAttribute
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	aliceSkills, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found, "alice should have skills")
	t.Logf("Alice's skills via LookupAttribute: %v", aliceSkills)

	_ = encoder // Used for documentation, actual encoding done manually
}

// TestVectorAVETMultipleEntities verifies AVET returns all entities with same value.
func TestVectorAVETMultipleEntities(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bug5-avet-multi")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	tags := datalog.NewKeyword(":person/tags")

	// Create 5 entities, all with "important" tag
	entities := []datalog.Identity{
		datalog.NewIdentity("e1"),
		datalog.NewIdentity("e2"),
		datalog.NewIdentity("e3"),
		datalog.NewIdentity("e4"),
		datalog.NewIdentity("e5"),
	}

	for _, e := range entities {
		tx := db.NewTransaction()
		require.NoError(t, tx.Add(e, tags, "important"))
		require.NoError(t, tx.Add(e, tags, "unique-"+e.String()))
		_, err = tx.Commit()
		require.NoError(t, err)
	}

	// Build AVET prefix for "important"
	aBytes := encodeKeyword(tags)
	vType := byte(datalog.TypeString)
	vData := []byte("important")
	vBytes := append([]byte{vType}, vData...)

	prefix := make([]byte, 1+32+len(vBytes))
	prefix[0] = byte(AVET)
	copy(prefix[1:33], aBytes[:])
	copy(prefix[33:], vBytes)

	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1]++

	iter, err := db.store.Scan(AVET, prefix, end)
	require.NoError(t, err)
	defer iter.Close()

	foundCount := 0
	for iter.Next() {
		_, err := iter.Datom()
		if err == nil {
			foundCount++
		}
	}

	// BUG #5: This currently fails - foundCount will be 0
	assert.Equal(t, 5, foundCount, "should find all 5 entities with 'important' tag")
	t.Logf("Found %d entities with 'important' tag (expected: 5)", foundCount)
}

// TestVectorAVETAfterTombstone verifies tombstoned elements handling.
func TestVectorAVETAfterTombstone(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bug5-avet-tombstone")
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

	// Add skills
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, skills, "stealth"))
	require.NoError(t, tx1.Add(alice, skills, "archery"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Replace vector, removing "stealth"
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, skills, []interface{}{"archery", "lockpicking"}))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify via LookupAttribute that stealth is gone
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := matcher.LookupAttribute(alice, skills)
	require.True(t, found)
	vec := result.([]string)

	assert.NotContains(t, vec, "stealth", "stealth should be tombstoned")
	assert.Contains(t, vec, "archery", "archery should remain")
	assert.Contains(t, vec, "lockpicking", "lockpicking should be added")

	t.Logf("Skills after replacement: %v", vec)

	// Note: AVET tombstone semantics need clarification.
	// For now, this test documents that LookupAttribute correctly handles tombstones.
	// Future: AVET query should also respect tombstones.
}

// TestVectorVAETReverseLookup verifies VAET index works for entity refs in vectors.
func TestVectorVAETReverseLookup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bug5-vaet-lookup")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with vector of refs
	s, err := schema.NewBuilder().
		Attribute(":person/friends").Type(schema.TypeRef).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	charlie := datalog.NewIdentity("charlie")
	friends := datalog.NewKeyword(":person/friends")

	// Alice's friends are bob and charlie
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, friends, bob))
	require.NoError(t, tx.Add(alice, friends, charlie))
	_, err = tx.Commit()
	require.NoError(t, err)

	// VAET lookup: Who has bob as a friend?
	// Build VAET prefix: [prefix][V][A]
	bobRef := datalog.Reference(bob)
	vType := byte(datalog.TypeReference)
	vData := bobRef.Bytes()
	vBytes := append([]byte{vType}, vData...)

	aBytes := encodeKeyword(friends)

	prefix := make([]byte, 1+len(vBytes)+32)
	prefix[0] = byte(VAET)
	copy(prefix[1:1+len(vBytes)], vBytes)
	copy(prefix[1+len(vBytes):], aBytes[:])

	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1]++

	iter, err := db.store.Scan(VAET, prefix, end)
	require.NoError(t, err)
	defer iter.Close()

	foundAlice := false
	for iter.Next() {
		datom, err := iter.Datom()
		if err == nil && datom.E.Hash() == alice.Hash() {
			foundAlice = true
		}
	}

	// BUG #5: This currently fails - VAET lookup doesn't find vector refs
	assert.True(t, foundAlice, "VAET should find alice as having bob as friend")
	t.Logf("Found alice via VAET for bob: %v (expected: true)", foundAlice)
}

// TestVectorValueTypePreserved verifies V decodes as original type, not []byte.
func TestVectorValueTypePreserved(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bug5-type-preserved")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":test/strings").Type(schema.TypeString).Vector().Add().
		Attribute(":test/ints").Type(schema.TypeLong).Vector().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("test")
	strAttr := datalog.NewKeyword(":test/strings")
	intAttr := datalog.NewKeyword(":test/ints")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(entity, strAttr, "hello"))
	require.NoError(t, tx.Add(entity, intAttr, int64(42)))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Scan EAVT for this entity and check value types
	eBytes := entity.Hash()

	prefix := make([]byte, 1+20)
	prefix[0] = byte(EAVT)
	copy(prefix[1:21], eBytes[:])

	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1]++

	iter, err := db.store.Scan(EAVT, prefix, end)
	require.NoError(t, err)
	defer iter.Close()

	var foundTypes []string
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			continue
		}

		// Check the actual type of the value
		switch datom.V.(type) {
		case string:
			foundTypes = append(foundTypes, "string")
		case int64:
			foundTypes = append(foundTypes, "int64")
		case []byte:
			foundTypes = append(foundTypes, "[]byte")
		default:
			foundTypes = append(foundTypes, "other")
		}
	}

	t.Logf("Found value types: %v", foundTypes)

	// BUG #5: Currently values will be []byte (RGAElement wrapper)
	// After fix, they should be string and int64
	assert.Contains(t, foundTypes, "string", "string value should decode as string, not []byte")
	assert.Contains(t, foundTypes, "int64", "int value should decode as int64, not []byte")
	assert.NotContains(t, foundTypes, "[]byte", "values should not be []byte (RGAElement wrapper)")
}
