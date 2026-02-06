package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Test struct for PullInto
type Person struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"person/name"`
	Age  int64            `datalog:"person/age"`
}

type PersonWithTags struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name string           `datalog:"person/name"`
	Tags []string         `datalog:"person/tags"`
}

type PersonWithSkills struct {
	ID     datalog.Identity `datalog:"-,id"`
	Name   string           `datalog:"person/name"`
	Skills []string         `datalog:"person/skills"`
}

func createPullIntoCRDTTestDB(t *testing.T) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

// TestPullInto_CardinalityOne_LWW verifies that PullInto returns the
// LWW-resolved value for cardinality-one attributes, not all historical values.
func TestPullInto_CardinalityOne_LWW(t *testing.T) {
	db, cleanup := createPullIntoCRDTTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/age").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Create entity and update multiple times
	e1 := datalog.NewIdentity("person1")

	tx1 := db.NewTransaction()
	err = tx1.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx1.Set(e1, datalog.NewKeyword(":person/age"), int64(25))
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Update name
	tx2 := db.NewTransaction()
	err = tx2.Set(e1, datalog.NewKeyword(":person/name"), "Alicia")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Update age
	tx3 := db.NewTransaction()
	err = tx3.Set(e1, datalog.NewKeyword(":person/age"), int64(30))
	require.NoError(t, err)
	_, err = tx3.Commit()
	require.NoError(t, err)

	// Clear cache to ensure we resolve from storage
	db.Cache().Clear()

	// PullInto should return the latest values, not historical
	var person Person
	err = db.PullInto(e1, &person)
	require.NoError(t, err)

	assert.Equal(t, "Alicia", person.Name, "should return LWW-resolved name")
	assert.Equal(t, int64(30), person.Age, "should return LWW-resolved age")
}

// TestPullInto_CardinalityMany_AddWins verifies that PullInto returns
// the add-wins resolved set for cardinality-many attributes.
func TestPullInto_CardinalityMany_AddWins(t *testing.T) {
	db, cleanup := createPullIntoCRDTTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Create entity with tags
	e1 := datalog.NewIdentity("person1")

	tx1 := db.NewTransaction()
	err = tx1.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/tags"), "developer")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/tags"), "go")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/tags"), "python")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Remove a tag
	tx2 := db.NewTransaction()
	err = tx2.Remove(e1, datalog.NewKeyword(":person/tags"), "python")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// PullInto should return only current set members
	var person PersonWithTags
	err = db.PullInto(e1, &person)
	require.NoError(t, err)

	assert.Equal(t, "Alice", person.Name)
	// Should have "developer" and "go", but NOT "python"
	assert.Contains(t, person.Tags, "developer")
	assert.Contains(t, person.Tags, "go")
	assert.NotContains(t, person.Tags, "python", "removed tag should not be in result")
	assert.Len(t, person.Tags, 2)
}

// TestPullInto_CardinalityVector_RGA verifies that PullInto returns
// the RGA-resolved ordered list for cardinality-vector attributes.
func TestPullInto_CardinalityVector_RGA(t *testing.T) {
	db, cleanup := createPullIntoCRDTTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Create entity with ordered skills
	e1 := datalog.NewIdentity("person1")

	tx1 := db.NewTransaction()
	err = tx1.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/skills"), "Go")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/skills"), "Python")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/skills"), "Rust")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// PullInto should return ordered skills
	var person PersonWithSkills
	err = db.PullInto(e1, &person)
	require.NoError(t, err)

	assert.Equal(t, "Alice", person.Name)
	assert.Equal(t, []string{"Go", "Python", "Rust"}, person.Skills)
}

// TestPull_Wildcard_CRDTResolution verifies that wildcard pulls (*)
// return CRDT-resolved values, not all historical values.
func TestPull_Wildcard_CRDTResolution(t *testing.T) {
	db, cleanup := createPullIntoCRDTTestDB(t)
	defer cleanup()

	// Create schema
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	e1 := datalog.NewIdentity("person1")

	// Add initial data
	tx1 := db.NewTransaction()
	err = tx1.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/tags"), "developer")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/tags"), "python")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Update name and remove a tag
	tx2 := db.NewTransaction()
	err = tx2.Set(e1, datalog.NewKeyword(":person/name"), "Alicia")
	require.NoError(t, err)
	err = tx2.Remove(e1, datalog.NewKeyword(":person/tags"), "python")
	require.NoError(t, err)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Wildcard pull should return CRDT-resolved values
	result, err := db.Pull(e1, "[*]")
	require.NoError(t, err)

	// Name should be the latest (LWW)
	assert.Equal(t, "Alicia", result["person/name"], "should return LWW-resolved name")

	// Tags should only contain non-removed values (add-wins)
	tags, ok := result["person/tags"].([]interface{})
	require.True(t, ok, "tags should be []interface{}")
	assert.Len(t, tags, 1, "should have 1 tag after removal")
	assert.Contains(t, tags, "developer")
}

// TestPull_Wildcard_CardinalityVector verifies that wildcard pulls
// return ordered values for cardinality-vector attributes.
func TestPull_Wildcard_CardinalityVector(t *testing.T) {
	db, cleanup := createPullIntoCRDTTestDB(t)
	defer cleanup()

	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/skills").Type(schema.TypeString).Vector().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	e1 := datalog.NewIdentity("person1")

	// Add ordered skills
	tx1 := db.NewTransaction()
	err = tx1.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/skills"), "Go")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/skills"), "Python")
	require.NoError(t, err)
	err = tx1.Add(e1, datalog.NewKeyword(":person/skills"), "Rust")
	require.NoError(t, err)
	_, err = tx1.Commit()
	require.NoError(t, err)

	// Clear cache
	db.Cache().Clear()

	// Wildcard pull should return ordered skills
	result, err := db.Pull(e1, "[*]")
	require.NoError(t, err)

	assert.Equal(t, "Alice", result["person/name"])

	skills, ok := result["person/skills"].([]interface{})
	require.True(t, ok, "skills should be []interface{}")
	require.Len(t, skills, 3)
	assert.Equal(t, "Go", skills[0])
	assert.Equal(t, "Python", skills[1])
	assert.Equal(t, "Rust", skills[2])
}

// TestPullInto_AfterCacheClear verifies that PullInto works correctly
// after the cache is cleared (simulating a restart).
func TestPullInto_AfterCacheClear(t *testing.T) {
	db, cleanup := createPullIntoCRDTTestDB(t)
	defer cleanup()

	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	e1 := datalog.NewIdentity("person1")

	// Multiple updates
	for i, name := range []string{"Alice", "Alicia", "Alice Updated"} {
		tx := db.NewTransaction()
		err = tx.Set(e1, datalog.NewKeyword(":person/name"), name)
		require.NoError(t, err, "update %d", i)
		_, err = tx.Commit()
		require.NoError(t, err, "commit %d", i)
	}

	// Clear cache to simulate restart
	db.Cache().Clear()

	// PullInto should still resolve correctly
	var person Person
	err = db.PullInto(e1, &person)
	require.NoError(t, err)

	assert.Equal(t, "Alice Updated", person.Name)
}

// =============================================================================
// Any/Interface{} Field Tests
// =============================================================================

// PersonWithAnyFields uses `any` for heterogeneous attribute inspection
type PersonWithAnyFields struct {
	ID   datalog.Identity `datalog:"-,id"`
	Name any              `datalog:"person/name"`
	Age  any              `datalog:"person/age"`
}

// TestPullInto_AnyField verifies that PullInto works with `any` typed fields
func TestPullInto_AnyField(t *testing.T) {
	db, cleanup := createPullIntoCRDTTestDB(t)
	defer cleanup()

	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Attribute(":person/age").Type(schema.TypeLong).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	e1 := datalog.NewIdentity("person1")

	tx := db.NewTransaction()
	err = tx.Set(e1, datalog.NewKeyword(":person/name"), "Alice")
	require.NoError(t, err)
	err = tx.Set(e1, datalog.NewKeyword(":person/age"), int64(30))
	require.NoError(t, err)
	_, err = tx.Commit()
	require.NoError(t, err)

	var person PersonWithAnyFields
	err = db.PullInto(e1, &person)
	require.NoError(t, err)

	// Verify the values are correct and have the right underlying types
	name, ok := person.Name.(string)
	assert.True(t, ok, "Name should be string, got %T", person.Name)
	assert.Equal(t, "Alice", name)

	age, ok := person.Age.(int64)
	assert.True(t, ok, "Age should be int64, got %T", person.Age)
	assert.Equal(t, int64(30), age)
}
