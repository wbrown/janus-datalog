package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestOrderedSet_UniqueElementsEnforcement verifies that UniqueElements prevents duplicates
func TestOrderedSet_UniqueElementsEnforcement(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "orderedset-unique-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create schema with OrderedSet (Vector + UniqueElements)
	s, err := schema.NewBuilder().
		Attribute(":character/prefs").Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	prefs := datalog.NewKeyword(":character/prefs")

	// Add values including duplicates
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, prefs, "dark-mode"))
	require.NoError(t, tx.Add(alice, prefs, "compact"))
	require.NoError(t, tx.Add(alice, prefs, "dark-mode")) // Duplicate - should be no-op
	require.NoError(t, tx.Add(alice, prefs, "notifications"))
	require.NoError(t, tx.Add(alice, prefs, "compact")) // Duplicate - should be no-op
	_, err = tx.Commit()
	require.NoError(t, err)

	// Verify only unique values stored
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := matcher.LookupAttribute(alice, prefs)
	require.True(t, found)

	vec, ok := result.([]any)
	require.True(t, ok, "result should be []any")

	t.Logf("OrderedSet contains: %v", vec)
	assert.Len(t, vec, 3, "should have 3 unique values")
	assert.Equal(t, "dark-mode", vec[0])
	assert.Equal(t, "compact", vec[1])
	assert.Equal(t, "notifications", vec[2])
}

// TestOrderedSet_UniqueAcrossTransactions verifies uniqueness across multiple transactions
func TestOrderedSet_UniqueAcrossTransactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "orderedset-multi-tx-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/prefs").Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	prefs := datalog.NewKeyword(":character/prefs")

	// First transaction: add initial values
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, prefs, "a"))
	require.NoError(t, tx1.Add(alice, prefs, "b"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Second transaction: try to add duplicates
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(alice, prefs, "b")) // Already exists - no-op
	require.NoError(t, tx2.Add(alice, prefs, "c")) // New value
	require.NoError(t, tx2.Add(alice, prefs, "a")) // Already exists - no-op
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := matcher.LookupAttribute(alice, prefs)
	require.True(t, found)

	vec, ok := result.([]any)
	require.True(t, ok)

	t.Logf("OrderedSet after two transactions: %v", vec)
	assert.Len(t, vec, 3, "should have 3 unique values: a, b, c")
}

// TestOrderedSet_SetReplacement verifies Set() correctly replaces with unique enforcement
func TestOrderedSet_SetReplacement(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "orderedset-set-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/prefs").Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	prefs := datalog.NewKeyword(":character/prefs")

	// Initial values
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Add(alice, prefs, "a"))
	require.NoError(t, tx1.Add(alice, prefs, "b"))
	require.NoError(t, tx1.Add(alice, prefs, "c"))
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Replace with new set (some overlap)
	tx2 := db.NewTransaction()
	newVals := []any{"b", "d", "e"} // Keep b, remove a & c, add d & e
	require.NoError(t, tx2.Set(alice, prefs, newVals))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Verify
	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := matcher.LookupAttribute(alice, prefs)
	require.True(t, found)

	vec, ok := result.([]any)
	require.True(t, ok)

	t.Logf("OrderedSet after Set(): %v", vec)
	assert.Len(t, vec, 3, "should have 3 values: b, d, e")
	assert.Equal(t, "b", vec[0])
	assert.Equal(t, "d", vec[1])
	assert.Equal(t, "e", vec[2])
}

// TestOrderedSet_QueryIntegration verifies OrderedSet works with queries
func TestOrderedSet_QueryIntegration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "orderedset-query-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":character/name").Type(schema.TypeString).Add().
		Attribute(":character/prefs").Type(schema.TypeString).OrderedSet().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":character/name")
	prefs := datalog.NewKeyword(":character/prefs")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, name, "Alice"))
	require.NoError(t, tx.Add(alice, prefs, "dark-mode"))
	require.NoError(t, tx.Add(alice, prefs, "compact"))
	require.NoError(t, tx.Add(alice, prefs, "dark-mode")) // Duplicate
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query returns vector as single array value
	tuples, err := executor.CollectTuples(db.Query(
		`[:find ?prefs :in $ ?e :where [?e :character/prefs ?prefs]]`,
		alice,
	))
	require.NoError(t, err)

	t.Logf("Query result: %v", tuples)
	require.Len(t, tuples, 1, "should return 1 tuple (vector as single value)")

	vec, ok := tuples[0][0].([]any)
	require.True(t, ok, "should be []any")
	assert.Len(t, vec, 2, "should have 2 unique values")
}

// TestOrderedSet_RefType verifies OrderedSet works with Identity references
func TestOrderedSet_RefType(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "orderedset-ref-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":person/follows").Type(schema.TypeRef).OrderedSet().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	carol := datalog.NewIdentity("carol")
	follows := datalog.NewKeyword(":person/follows")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, follows, bob))
	require.NoError(t, tx.Add(alice, follows, carol))
	require.NoError(t, tx.Add(alice, follows, bob)) // Duplicate ref - should be no-op
	_, err = tx.Commit()
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	result, found := matcher.LookupAttribute(alice, follows)
	require.True(t, found)

	vec, ok := result.([]any)
	require.True(t, ok)

	t.Logf("Follows (refs): %v", vec)
	assert.Len(t, vec, 2, "should have 2 unique refs")
}

// TestOrderedSet_VsRegularVector compares OrderedSet vs regular Vector behavior
func TestOrderedSet_VsRegularVector(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "orderedset-vs-vector-test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	s, err := schema.NewBuilder().
		Attribute(":entity/orderedset").Type(schema.TypeString).OrderedSet().Add().
		Attribute(":entity/vector").Type(schema.TypeString).Vector().Add(). // No UniqueElements
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(tmpDir, s)
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("entity1")
	orderedSetAttr := datalog.NewKeyword(":entity/orderedset")
	vectorAttr := datalog.NewKeyword(":entity/vector")

	tx := db.NewTransaction()
	// Add same values to both attributes
	for _, val := range []string{"a", "b", "a", "c", "b"} {
		require.NoError(t, tx.Add(entity, orderedSetAttr, val))
		require.NoError(t, tx.Add(entity, vectorAttr, val))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.store)
	matcher.SetSchema(s)

	// OrderedSet should have 3 unique values
	osResult, found := matcher.LookupAttribute(entity, orderedSetAttr)
	require.True(t, found)
	osVec, ok := osResult.([]any)
	require.True(t, ok)
	t.Logf("OrderedSet: %v (len=%d)", osVec, len(osVec))
	assert.Len(t, osVec, 3, "OrderedSet should have 3 unique values")

	// Regular Vector should have all 5 values (duplicates allowed)
	vecResult, found := matcher.LookupAttribute(entity, vectorAttr)
	require.True(t, found)
	vecVec, ok := vecResult.([]any)
	require.True(t, ok)
	t.Logf("Regular Vector: %v (len=%d)", vecVec, len(vecVec))
	assert.Len(t, vecVec, 5, "Regular Vector should have all 5 values including duplicates")
}

// TestOrderedSet_SchemaIsOrderedSet verifies the IsOrderedSet() derived getter
func TestOrderedSet_SchemaIsOrderedSet(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":test/orderedset").Type(schema.TypeString).OrderedSet().Add().
		Attribute(":test/vector").Type(schema.TypeString).Vector().Add().
		Attribute(":test/many").Type(schema.TypeString).Many().Add().
		Attribute(":test/one").Type(schema.TypeString).Add().
		Build()
	require.NoError(t, err)

	tests := []struct {
		attr     string
		expected bool
	}{
		{":test/orderedset", true},
		{":test/vector", false},
		{":test/many", false},
		{":test/one", false},
	}

	for _, tt := range tests {
		t.Run(tt.attr, func(t *testing.T) {
			def := s.GetAttribute(datalog.NewKeyword(tt.attr))
			require.NotNil(t, def)
			assert.Equal(t, tt.expected, def.IsOrderedSet(), "IsOrderedSet() for %s", tt.attr)
		})
	}
}
