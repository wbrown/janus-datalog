package storage

// Collection Input Tests
//
// Tests for how collection inputs [?x ...] are processed by the executor.
// These tests verify that collection values are correctly unpacked into
// multiple tuples for pattern matching.

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// Single Collection Input Tests
// =============================================================================

// TestCollectionInput_SingleCollection verifies a single [?e ...] input works.
func TestCollectionInput_SingleCollection(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-single-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	// Create test data
	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
		datalog.NewIdentity("person-3"),
	}

	tx := db.NewTransaction()
	for i, e := range entities {
		tx.Add(e, datalog.NewKeyword(":person/name"), "Name"+string(rune('A'+i)))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with single collection input
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ [?e ...] :where [?e :person/name ?v]]`,
		entities)
	require.NoError(t, err)

	t.Logf("Results (%d):", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}

	assert.Len(t, results, 3, "should return one result per entity in collection")
}

// TestCollectionInput_SingleElement verifies collection with one element works.
func TestCollectionInput_SingleElement(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-one-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("person-1")

	tx := db.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":person/name"), "Alice")
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with single-element collection
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ [?e ...] :where [?e :person/name ?v]]`,
		[]datalog.Identity{entity})
	require.NoError(t, err)

	assert.Len(t, results, 1, "should return one result for single-element collection")
}

// TestCollectionInput_EmptyCollection verifies empty collection returns no results.
func TestCollectionInput_EmptyCollection(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-empty-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	// Add some data
	tx := db.NewTransaction()
	tx.Add(datalog.NewIdentity("person-1"), datalog.NewKeyword(":person/name"), "Alice")
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with empty collection
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ [?e ...] :where [?e :person/name ?v]]`,
		[]datalog.Identity{})
	require.NoError(t, err)

	assert.Len(t, results, 0, "empty collection should return no results")
}

// =============================================================================
// Multiple Collection Input Tests (Cross-Product)
// =============================================================================

// TestCollectionInput_TwoCollections verifies two [?x ...] inputs produce cross-product.
func TestCollectionInput_TwoCollections(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-two-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         dir,
		DisableCache: true,
	})
	require.NoError(t, err)
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/age"),
		ValueType:   schema.TypeLong,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
	}
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":person/name"),
		datalog.NewKeyword(":person/age"),
	}

	// Create data for all (entity, attribute) combinations
	tx := db.NewTransaction()
	for i, e := range entities {
		tx.Set(e, attrs[0], "Name"+string(rune('A'+i)))
		tx.Set(e, attrs[1], int64(20+i*10))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with two collection inputs - should get cross-product
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]`,
		entities, attrs)
	require.NoError(t, err)

	t.Logf("Results (%d):", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}

	// Expected: 2 entities × 2 attributes = 4 results
	assert.Len(t, results, 4, "should return cross-product: 2 entities × 2 attributes")
}

// TestCollectionInput_ThreeCollections verifies three collections produce full cross-product.
func TestCollectionInput_ThreeCollections(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-three-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	// Create entities with multiple attributes having multiple values
	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
	}

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tag"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany, // Allow multiple values
	})
	db.SetSchema(s)

	tags := []string{"alpha", "beta"}

	tx := db.NewTransaction()
	for _, e := range entities {
		for _, tag := range tags {
			tx.Add(e, datalog.NewKeyword(":person/tag"), tag)
		}
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query: find all (entity, tag) pairs where tag matches input collection
	// Using entity collection and value collection
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ [?e ...] [?v ...] :where [?e :person/tag ?v]]`,
		entities, tags)
	require.NoError(t, err)

	t.Logf("Results (%d):", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}

	// Expected: 2 entities × 2 tags = 4 results (only matching pairs)
	assert.Len(t, results, 4, "should return matching pairs from cross-product")
}

// =============================================================================
// Mixed Input Tests (Scalar + Collection)
// =============================================================================

// TestCollectionInput_ScalarPlusCollection verifies scalar and collection inputs work together.
func TestCollectionInput_ScalarPlusCollection(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-mixed-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
		datalog.NewIdentity("person-3"),
	}
	attr := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	for i, e := range entities {
		tx.Add(e, attr, "Name"+string(rune('A'+i)))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with scalar attribute and collection of entities
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ ?a [?e ...] :where [?e ?a ?v]]`,
		attr, entities)
	require.NoError(t, err)

	t.Logf("Results (%d):", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}

	assert.Len(t, results, 3, "should return one result per entity with scalar attribute")
}

// TestCollectionInput_CollectionPlusScalar verifies collection then scalar order works.
func TestCollectionInput_CollectionPlusScalar(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-mixed2-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	entities := []datalog.Identity{
		datalog.NewIdentity("person-1"),
		datalog.NewIdentity("person-2"),
	}
	attr := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	for i, e := range entities {
		tx.Add(e, attr, "Name"+string(rune('A'+i)))
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with collection first, then scalar
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ [?e ...] ?a :where [?e ?a ?v]]`,
		entities, attr)
	require.NoError(t, err)

	assert.Len(t, results, 2, "should return one result per entity")
}

// =============================================================================
// Different Value Type Tests
// =============================================================================

// TestCollectionInput_KeywordCollection verifies collection of keywords works.
func TestCollectionInput_KeywordCollection(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-keyword-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	entity := datalog.NewIdentity("person-1")
	attrs := []datalog.Keyword{
		datalog.NewKeyword(":person/name"),
		datalog.NewKeyword(":person/city"),
	}

	tx := db.NewTransaction()
	tx.Add(entity, attrs[0], "Alice")
	tx.Add(entity, attrs[1], "NYC")
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with collection of keywords (attributes)
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?a ?v :in $ ?e [?a ...] :where [?e ?a ?v]]`,
		entity, attrs)
	require.NoError(t, err)

	t.Logf("Results (%d):", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}

	assert.Len(t, results, 2, "should return one result per attribute in collection")
}

// TestCollectionInput_IntCollection verifies collection of integers works.
func TestCollectionInput_IntCollection(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-int-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":item/score"),
		ValueType:   schema.TypeLong,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entity := datalog.NewIdentity("item-1")
	scores := []int64{100, 200, 300}

	tx := db.NewTransaction()
	for _, score := range scores {
		tx.Add(entity, datalog.NewKeyword(":item/score"), score)
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with collection of integers
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ ?e [?v ...] :where [?e :item/score ?v]]`,
		entity, scores)
	require.NoError(t, err)

	assert.Len(t, results, 3, "should return one result per score in collection")
}

// TestCollectionInput_StringCollection verifies collection of strings works.
func TestCollectionInput_StringCollection(t *testing.T) {
	dir, err := os.MkdirTemp("", "collection-string-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tag"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entity := datalog.NewIdentity("person-1")
	tags := []string{"developer", "manager", "admin"}

	tx := db.NewTransaction()
	for _, tag := range tags {
		tx.Add(entity, datalog.NewKeyword(":person/tag"), tag)
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with collection of strings
	results, err := db.ExecuteQueryWithInputs(
		`[:find ?e ?v :in $ ?e [?v ...] :where [?e :person/tag ?v]]`,
		entity, tags)
	require.NoError(t, err)

	assert.Len(t, results, 3, "should return one result per tag in collection")
}
