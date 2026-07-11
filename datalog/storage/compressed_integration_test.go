package storage

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// newCompressedDB creates a test database with compression enabled.
func newCompressedDB(t *testing.T) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "compress-integ-*")
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// newCompressedDBWithSchema creates a test database with compression and schema.
func newCompressedDBWithSchema(t *testing.T, s schema.SchemaProvider) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "compress-integ-*")
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir,
		Schema:               s,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}
	return db, cleanup
}

// longString creates a compressible string above the threshold.
func longString(prefix string, size int) string {
	base := prefix + " " + strings.Repeat("The quick brown fox jumps over the lazy dog. ", size/45+2)
	if len(base) > size {
		return base[:size]
	}
	return base
}

// ---- Write and Read Back ----

func TestCompressedIntegration_WriteReadString(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":test/content")
	value := longString("Hello", 500)

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Read back via pattern match
	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next(), "expected at least one result")
	tuple := iter.Tuple()
	require.Len(t, tuple, 1)
	assert.Equal(t, value, tuple[0].(string), "read-back value should match written value")
	assert.False(t, iter.Next(), "expected exactly one result")
}

func TestCompressedIntegration_WriteReadBytes(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("bytes-entity")
	attr := datalog.NewKeyword(":test/data")
	value := []byte(longString("bytes", 500))

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next())
	got := iter.Tuple()[0].([]byte)
	assert.Equal(t, value, got, "bytes round-trip failed")
}

func TestCompressedIntegration_ShortStringUncompressed(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("short-entity")
	attr := datalog.NewKeyword(":test/name")
	value := "Alice" // well below 256 threshold

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next())
	assert.Equal(t, "Alice", iter.Tuple()[0].(string))
}

// ---- Value Equality via AVET ----

func TestCompressedIntegration_AVET_ExactMatch(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("avet-entity")
	attr := datalog.NewKeyword(":test/content")
	value := longString("AVET test", 500)

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Query with both A and V bound — goes through AVET index
	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: value}, // bound value triggers AVET
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next(), "AVET lookup should find the entity")
	gotEntity := iter.Tuple()[0]
	assert.Equal(t, entity, gotEntity,
		"AVET lookup returned wrong entity")
	assert.False(t, iter.Next(), "expected exactly one result")
}

func TestCompressedIntegration_AVET_NoFalsePositive(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("avet-entity")
	attr := datalog.NewKeyword(":test/content")
	value := longString("stored value", 500)
	differentValue := longString("different value", 500)

	tx := db.NewTransaction()
	tx.Add(entity, attr, value)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Search for a different value — should find nothing
	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: differentValue},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	assert.False(t, iter.Next(), "AVET search for non-existent value should return nothing")
}

// ---- CRDT Resolution ----

func TestCompressedIntegration_CRDT_CardinalityOne_LWW(t *testing.T) {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":test/content"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})

	db, cleanup := newCompressedDBWithSchema(t, s)
	defer cleanup()

	entity := datalog.NewIdentity("lww-entity")
	attr := datalog.NewKeyword(":test/content")

	// Write version 1
	tx1 := db.NewTransaction()
	tx1.Set(entity, attr, longString("version one", 500))
	_, err := tx1.Commit()
	require.NoError(t, err)

	// Write version 2
	tx2 := db.NewTransaction()
	tx2.Set(entity, attr, longString("version two", 500))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Query should return only the latest (version 2)
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	iter := results.Iterator()
	require.True(t, iter.Next())
	got := iter.Tuple()[0].(string)
	assert.True(t, strings.HasPrefix(got, "version two"),
		"LWW should return latest version, got: %s...", got[:40])
	assert.False(t, iter.Next(), "LWW should return exactly one result")
}

func TestCompressedIntegration_CRDT_CardinalityMany(t *testing.T) {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":test/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})

	db, cleanup := newCompressedDBWithSchema(t, s)
	defer cleanup()

	entity := datalog.NewIdentity("many-entity")
	attr := datalog.NewKeyword(":test/tags")
	tag1 := longString("first tag value", 500)
	tag2 := longString("second tag value", 500)

	tx := db.NewTransaction()
	tx.Add(entity, attr, tag1)
	tx.Add(entity, attr, tag2)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Both values should be present
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	var values []string
	iter := results.Iterator()
	for iter.Next() {
		values = append(values, iter.Tuple()[0].(string))
	}
	assert.Len(t, values, 2, "cardinality-many should return both values")

	// Verify both values are present
	found1, found2 := false, false
	for _, v := range values {
		if v == tag1 {
			found1 = true
		}
		if v == tag2 {
			found2 = true
		}
	}
	assert.True(t, found1, "first tag value not found")
	assert.True(t, found2, "second tag value not found")
}

func TestCompressedIntegration_CRDT_CardinalityMany_Remove(t *testing.T) {
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":test/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})

	db, cleanup := newCompressedDBWithSchema(t, s)
	defer cleanup()

	entity := datalog.NewIdentity("remove-entity")
	attr := datalog.NewKeyword(":test/tags")
	tag1 := longString("tag to keep", 500)
	tag2 := longString("tag to remove", 500)

	// Add both
	tx := db.NewTransaction()
	tx.Add(entity, attr, tag1)
	tx.Add(entity, attr, tag2)
	_, err := tx.Commit()
	require.NoError(t, err)

	// Remove tag2
	tx2 := db.NewTransaction()
	tx2.Retract(entity, attr, tag2)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Only tag1 should remain
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	var values []string
	iter := results.Iterator()
	for iter.Next() {
		values = append(values, iter.Tuple()[0].(string))
	}
	assert.Len(t, values, 1, "should have one remaining tag after retract")
	if len(values) == 1 {
		assert.Equal(t, tag1, values[0], "remaining tag should be the one not retracted")
	}
}

// ---- Mixed Compressed and Uncompressed ----

func TestCompressedIntegration_MixedValues(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("mixed-entity")

	// Write a mix of short (raw) and long (compressed) values
	tx := db.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":test/name"), "Alice")                       // Tier 1
	tx.Add(entity, datalog.NewKeyword(":test/content"), longString("content", 500)) // Tier 2
	tx.Add(entity, datalog.NewKeyword(":test/age"), int64(30))                      // non-string
	tx.Add(entity, datalog.NewKeyword(":test/active"), true)                        // non-string
	_, err := tx.Commit()
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.Store())

	// Read each back and verify
	for _, tc := range []struct {
		attr     string
		expected interface{}
	}{
		{":test/name", "Alice"},
		{":test/content", longString("content", 500)},
		{":test/age", int64(30)},
		{":test/active", true},
	} {
		t.Run(tc.attr, func(t *testing.T) {
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: entity},
					query.Constant{Value: datalog.NewKeyword(tc.attr)},
					query.Variable{Name: datalog.NewSymbol("?v")},
					query.Blank{},
				},
			}
			results, err := matcher.Match(query.PatternQuery(pattern), nil)
			require.NoError(t, err)

			iter := results.Iterator()
			require.True(t, iter.Next(), "expected result for %s", tc.attr)
			got := iter.Tuple()[0]
			assert.Equal(t, tc.expected, got, "value mismatch for %s", tc.attr)
		})
	}
}

// ---- Multiple Entities with Same Compressed Value ----

func TestCompressedIntegration_MultipleEntities(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	attr := datalog.NewKeyword(":test/content")
	content := longString("shared content", 500)

	// Write same value to 10 different entities
	for i := 0; i < 10; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx := db.NewTransaction()
		tx.Add(entity, attr, content)
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	// AVET lookup for the value should find all 10 entities
	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: content},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	count := 0
	iter := results.Iterator()
	for iter.Next() {
		count++
	}
	assert.Equal(t, 10, count, "AVET lookup should find all 10 entities with same compressed value")
}

// ---- Value Equality Across Compression Boundary ----

func TestCompressedIntegration_ValueEquality_CrossBoundary(t *testing.T) {
	// Create two databases: one with compression, one without
	// Write the same value to both, export, and verify equality

	dir1, err := os.MkdirTemp("", "compress-eq1-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir1)

	dir2, err := os.MkdirTemp("", "compress-eq2-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	// DB1: compression enabled
	db1, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:                 dir1,
		CompressionThreshold: 256,
	})
	require.NoError(t, err)
	defer db1.Close()

	// DB2: compression disabled
	db2, err := NewDatabaseWithOptions(DatabaseOptions{
		Path: dir2,
		// no compression threshold
	})
	require.NoError(t, err)
	defer db2.Close()

	entity := datalog.NewIdentity("eq-test")
	attr := datalog.NewKeyword(":test/content")
	value := longString("equality test", 500)

	// Write same value to both
	tx1 := db1.NewTransaction()
	tx1.Add(entity, attr, value)
	_, err = tx1.Commit()
	require.NoError(t, err)

	tx2 := db2.NewTransaction()
	tx2.Add(entity, attr, value)
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Read back from both — values should be equal
	readValue := func(db *Database) string {
		matcher := NewBadgerMatcher(db.Store())
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: entity},
				query.Constant{Value: attr},
				query.Variable{Name: datalog.NewSymbol("?v")},
				query.Blank{},
			},
		}
		results, err := matcher.Match(query.PatternQuery(pattern), nil)
		require.NoError(t, err)
		iter := results.Iterator()
		require.True(t, iter.Next())
		return iter.Tuple()[0].(string)
	}

	v1 := readValue(db1)
	v2 := readValue(db2)
	assert.Equal(t, v1, v2, "values should be equal regardless of compression")
	assert.Equal(t, value, v1, "compressed read should match original")
	assert.Equal(t, value, v2, "uncompressed read should match original")
}

// ---- Scan All Values for an Entity ----

func TestCompressedIntegration_ScanEntity(t *testing.T) {
	db, cleanup := newCompressedDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("scan-entity")

	// Write multiple attributes, mix of compressed and raw
	tx := db.NewTransaction()
	tx.Add(entity, datalog.NewKeyword(":test/name"), "Bob")
	tx.Add(entity, datalog.NewKeyword(":test/bio"), longString("biography", 800))
	tx.Add(entity, datalog.NewKeyword(":test/notes"), longString("notes", 600))
	tx.Add(entity, datalog.NewKeyword(":test/score"), int64(95))
	_, err := tx.Commit()
	require.NoError(t, err)

	// Scan all attributes for entity
	matcher := NewBadgerMatcher(db.Store())
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entity},
			query.Variable{Name: datalog.NewSymbol("?a")},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}
	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	seen := make(map[string]interface{})
	iter := results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		attr := tuple[0].(datalog.Keyword).String()
		seen[attr] = tuple[1]
	}

	// Should see all 4 attributes (plus :db/txInstant from each commit)
	assert.Contains(t, seen, ":test/name")
	assert.Contains(t, seen, ":test/bio")
	assert.Contains(t, seen, ":test/notes")
	assert.Contains(t, seen, ":test/score")

	assert.Equal(t, "Bob", seen[":test/name"])
	assert.Equal(t, longString("biography", 800), seen[":test/bio"])
	assert.Equal(t, longString("notes", 600), seen[":test/notes"])
	assert.Equal(t, int64(95), seen[":test/score"])
}

// ---- Determinism: Same Write Produces Same Keys ----

func TestCompressedIntegration_Determinism(t *testing.T) {
	// Write the same value twice to two different DBs — matcher should find it in both
	value := longString("determinism", 500)

	for i := 0; i < 2; i++ {
		dir, err := os.MkdirTemp("", fmt.Sprintf("compress-det%d-*", i))
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		db, err := NewDatabaseWithOptions(DatabaseOptions{
			Path:                 dir,
			CompressionThreshold: 256,
		})
		require.NoError(t, err)

		entity := datalog.NewIdentity("det-entity")
		attr := datalog.NewKeyword(":test/content")

		tx := db.NewTransaction()
		tx.Add(entity, attr, value)
		_, err = tx.Commit()
		require.NoError(t, err)

		// AVET lookup
		matcher := NewBadgerMatcher(db.Store())
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: attr},
				query.Constant{Value: value},
				query.Blank{},
			},
		}
		results, err := matcher.Match(query.PatternQuery(pattern), nil)
		require.NoError(t, err)

		iter := results.Iterator()
		require.True(t, iter.Next(), "DB %d: AVET lookup should find the entity", i)
		db.Close()
	}
}
