package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Helper to create test database with schema
func createEntityResolveTestDB(t *testing.T) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

func TestResolveEntityAttributes_SingleAttribute(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache to ensure we resolve from storage
	db.Cache().Clear()

	// Resolve single attribute
	attrs := []datalog.Keyword{datalog.NewKeyword(":person/name")}
	result, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)

	assert.Equal(t, "Alice", result[datalog.NewKeyword(":person/name")])
}

func TestResolveEntityAttributes_MultipleAttributes(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/age").Type(schema.TypeLong).One().Add().
		Attribute(":person/email").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Set(e1, datalog.NewKeyword(":person/age"), int64(30))
	require.NoError(t, err)
	err = tx.Set(e1, datalog.NewKeyword(":person/email"), "alice@example.com")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Resolve all attributes
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":person/name"),
		datalog.NewKeyword(":person/age"),
		datalog.NewKeyword(":person/email"),
	}
	result, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)

	assert.Equal(t, "Alice", result[datalog.NewKeyword(":person/name")])
	assert.Equal(t, int64(30), result[datalog.NewKeyword(":person/age")])
	assert.Equal(t, "alice@example.com", result[datalog.NewKeyword(":person/email")])
}

func TestResolveEntityAttributes_CardinalityMany(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// Create schema with cardinality-many
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Add(e1, datalog.NewKeyword(":person/tags"), "developer")
	require.NoError(t, err)
	err = tx.Add(e1, datalog.NewKeyword(":person/tags"), "go")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Resolve
	attrs := []datalog.Keyword{datalog.NewKeyword(":person/tags")}
	result, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)

	// Should return both values
	tags := result[datalog.NewKeyword(":person/tags")].([]interface{})
	assert.Len(t, tags, 2)
	assert.Contains(t, tags, "developer")
	assert.Contains(t, tags, "go")
}

func TestResolveEntityAttributes_CardinalityVector(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// Create schema with cardinality-vector
	s, err := schema.NewBuilder().
		Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data (tx.Add appends to vector)
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Add(e1, datalog.NewKeyword(":person/skills"), "Go")
	require.NoError(t, err)
	err = tx.Add(e1, datalog.NewKeyword(":person/skills"), "Python")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Resolve
	attrs := []datalog.Keyword{datalog.NewKeyword(":person/skills")}
	result, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)

	// Should return a slice
	skills, ok := result[datalog.NewKeyword(":person/skills")].([]any)
	require.True(t, ok, "expected []any for cardinality-vector")
	assert.Equal(t, []any{"Go", "Python"}, skills)
}

func TestResolveEntityAttributes_LWWResolution(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data then update
	e1 := datalog.NewIdentity("person1")

	tx1 := db.NewTransaction()
	err = tx1.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	err = tx2.Set(e1, datalog.NewKeyword(":person/name"), "Alicia") // Update
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Resolve should return latest value
	attrs := []datalog.Keyword{datalog.NewKeyword(":person/name")}
	result, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)

	assert.Equal(t, "Alicia", result[datalog.NewKeyword(":person/name")])
}

func TestResolveEntityAttributes_MissingAttribute(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/age").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add only name, not age
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Resolve both - age should be missing
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":person/name"),
		datalog.NewKeyword(":person/age"),
	}
	result, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)

	assert.Equal(t, "Alice", result[datalog.NewKeyword(":person/name")])
	_, hasAge := result[datalog.NewKeyword(":person/age")]
	assert.False(t, hasAge, "missing attribute should not be in result")
}

func TestResolveEntityAttributes_UsesCachedEntries(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Add data
	tx := db.NewTransaction()
	e1 := datalog.NewIdentity("person1")
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	// First call should populate cache
	attrs := []datalog.Keyword{datalog.NewKeyword(":person/name")}
	result1, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result1[datalog.NewKeyword(":person/name")])

	// Verify entity attrs are tracked
	eBytes := Entity(e1.Hash())
	cachedAttrs := db.Cache().GetCachedAttrs(eBytes)
	assert.NotNil(t, cachedAttrs, "should have cached attrs tracked")

	// Second call should use cache
	result2, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result2[datalog.NewKeyword(":person/name")])
}

func TestResolveEntityAttributes_NonexistentEntity(t *testing.T) {
	db, cleanup := createEntityResolveTestDB(t)
	defer cleanup()

	// No data added
	e1 := datalog.NewIdentity("nonexistent")
	attrs := []datalog.Keyword{datalog.NewKeyword(":person/name")}

	result, err := db.ResolveEntityAttributes(e1, attrs)
	require.NoError(t, err)
	assert.Empty(t, result)
}
