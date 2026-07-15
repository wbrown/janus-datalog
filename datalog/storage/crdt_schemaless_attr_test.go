//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// BUG: Schemaless Attributes Invisible to Bound Queries When Schema Present
// =============================================================================
//
// When a database has a schema defined, attributes NOT registered in the schema
// can be written successfully via tx.Add(), but bound queries via :in fail to
// find them. The root cause is that CRDTResolvingIterator treats unregistered
// attributes as CardinalityUnknown, which routes to processAddWins(). But
// schemaless datoms have Op=0 (zero value), and processAddWins only handles
// OpCRDTAdd (1) and OpCRDTRemove (2), silently dropping Op=0 datoms.
//
// See: docs/bugs/BUG_SCHEMALESS_ATTR_BOUND_QUERY.md
// =============================================================================

// TestSchemalessAttrBoundQuery_BugRepro reproduces the exact bug from the doc:
// schema exists, attribute not registered, bound query returns empty.
func TestSchemalessAttrBoundQuery_BugRepro(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			// Create schema with some attributes, but NOT :module/input
			s, err := schema.NewBuilder().
				Attribute(":module/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			db.SetSchema(s)

			entityID := datalog.NewIdentity("test-entity")

			// Write schemaless attribute — should succeed (additive schema)
			tx := db.NewTransaction()
			err = tx.Add(entityID, datalog.NewKeyword(":module/input"), "some text")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Bound query — should find the data
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
				entityID, datalog.NewKeyword(":module/input"),
			))
			require.NoError(t, err)

			assert.Len(t, results, 1,
				"[%s] Bound query for schemaless attribute should return 1 result", mode.name)
			if len(results) > 0 {
				assert.Equal(t, "some text", results[0][0],
					"[%s] Should find the written value", mode.name)
			}
		})
	}
}

// TestSchemalessAttrUnboundQuery_BugRepro tests that unbound queries also work
// for schemaless attributes when a schema is present.
func TestSchemalessAttrUnboundQuery_BugRepro(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			// Create schema with some attributes, but NOT :test/data
			s, err := schema.NewBuilder().
				Attribute(":test/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			db.SetSchema(s)

			entityID := datalog.NewIdentity("test-entity")

			// Write schemaless attribute
			tx := db.NewTransaction()
			err = tx.Add(entityID, datalog.NewKeyword(":test/data"), "hello")
			require.NoError(t, err)
			_, err = tx.Commit()
			require.NoError(t, err)

			// Unbound query — should find the data
			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :where [?e ?a ?v]]`,
			))
			require.NoError(t, err)

			// Should find at least the schemaless attribute
			found := false
			for _, tuple := range results {
				if len(tuple) >= 3 {
					if kw, ok := tuple[1].(datalog.Keyword); ok {
						if kw.String() == ":test/data" {
							found = true
							assert.Equal(t, "hello", tuple[2],
								"[%s] Should find correct value for schemaless attr", mode.name)
						}
					}
				}
			}
			assert.True(t, found,
				"[%s] Unbound query should find schemaless attribute :test/data", mode.name)
		})
	}
}

// TestSchemalessAttrMultipleWrites tests CRDT resolution for schemaless
// attributes when multiple writes occur. Schemaless default is CardinalityOne
// (LWW) — only the latest value should be returned.
func TestSchemalessAttrMultipleWrites(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			// Schema exists but :test/counter is not registered
			s, err := schema.NewBuilder().
				Attribute(":test/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			db.SetSchema(s)

			entityID := datalog.NewIdentity("test-entity")

			// Write multiple values — each overwrites the previous (LWW)
			for i := 0; i < 3; i++ {
				tx := db.NewTransaction()
				err = tx.Add(entityID, datalog.NewKeyword(":test/counter"), int64(i))
				require.NoError(t, err)
				_, err = tx.Commit()
				require.NoError(t, err)
			}

			// Bound query — schemaless = CardinalityOne (LWW), only latest value
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
				entityID, datalog.NewKeyword(":test/counter"),
			))
			require.NoError(t, err)

			require.Len(t, results, 1,
				"[%s] Schemaless = CardinalityOne (LWW): only latest value", mode.name)
			assert.Equal(t, int64(2), results[0][0],
				"[%s] Latest value should be 2 (third write)", mode.name)

			// Unbound query should also return only latest
			unboundResults, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :where [?e ?a ?v]]`,
			))
			require.NoError(t, err)

			var counterValues []any
			for _, tuple := range unboundResults {
				if len(tuple) >= 3 {
					if kw, ok := tuple[1].(datalog.Keyword); ok {
						if kw.String() == ":test/counter" {
							counterValues = append(counterValues, tuple[2])
						}
					}
				}
			}
			assert.Len(t, counterValues, 1,
				"[%s] Unbound query should also return only latest value", mode.name)
		})
	}
}

// =============================================================================
// Schemaless CardinalityOne (Default) Tests
// =============================================================================
//
// These tests verify that schemaless attributes default to CardinalityOne (LWW).
// Schemaless = no schema on the attribute (either no schema at all, or schema
// exists but the attribute is not registered).
//
// See: docs/bugs/BUG_SCHEMALESS_ATTR_BOUND_QUERY.md (tests 7-10)
// =============================================================================

// Test 8: Schemaless remove — tx.Add() then tx.Remove() → attribute doesn't exist
func TestSchemalessRemove_RoundTrip(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			// Schema exists but :test/data is not registered
			s, err := schema.NewBuilder().
				Attribute(":test/name").Type(schema.TypeString).One().Add().
				Build()
			require.NoError(t, err)
			db.SetSchema(s)

			entityID := datalog.NewIdentity("test-entity")
			attr := datalog.NewKeyword(":test/data")

			// Add
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(entityID, attr, "hello"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Verify exists
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
				entityID, attr,
			))
			require.NoError(t, err)
			require.Len(t, results, 1, "[%s] value should exist after Add", mode.name)

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(entityID, attr, "hello"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Bound query → attribute doesn't exist
			results, err = executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
				entityID, attr,
			))
			require.NoError(t, err)
			assert.Len(t, results, 0,
				"[%s] bound query: schemaless attribute should not exist after Remove", mode.name)

			// Unbound query → also doesn't exist
			unboundResults, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :where [?e ?a ?v]]`,
			))
			require.NoError(t, err)
			for _, tuple := range unboundResults {
				if len(tuple) >= 3 {
					if kw, ok := tuple[1].(datalog.Keyword); ok {
						assert.NotEqual(t, ":test/data", kw.String(),
							"[%s] unbound query should not find removed schemaless attribute", mode.name)
					}
				}
			}
		})
	}
}

// Test 9: Schemaless remove then re-add → latest Add wins
func TestSchemalessRemove_ThenReAdd(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			// No schema at all — fully schemaless
			entityID := datalog.NewIdentity("test-entity")
			attr := datalog.NewKeyword(":test/data")

			// Add
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(entityID, attr, "first"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Remove
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Remove(entityID, attr, "first"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Re-add with different value
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Add(entityID, attr, "second"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Latest Add wins
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
				entityID, attr,
			))
			require.NoError(t, err)
			require.Len(t, results, 1, "[%s] attribute should exist after re-Add", mode.name)
			assert.Equal(t, "second", results[0][0],
				"[%s] re-added value should be returned", mode.name)
		})
	}
}

// Test 10: Schema exists, attribute not registered → defaults to CardinalityOne
// Multiple writes return only latest. Remove works.
func TestSchemalessAttr_UnregisteredDefaultsToCardinalityOne(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			// Schema with many attributes, but NOT :unregistered/attr
			s, err := schema.NewBuilder().
				Attribute(":person/name").Type(schema.TypeString).One().Add().
				Attribute(":person/tags").Type(schema.TypeString).Many().Add().
				Build()
			require.NoError(t, err)
			db.SetSchema(s)

			entityID := datalog.NewIdentity("test-entity")
			attr := datalog.NewKeyword(":unregistered/attr")

			// Multiple writes — should be LWW (CardinalityOne default)
			tx := db.NewTransaction()
			require.NoError(t, tx.Add(entityID, attr, "v1"))
			_, err = tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Add(entityID, attr, "v2"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Add(entityID, attr, "v3"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Only latest value returned (LWW)
			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
				entityID, attr,
			))
			require.NoError(t, err)
			require.Len(t, results, 1,
				"[%s] unregistered attr defaults to CardinalityOne: only latest", mode.name)
			assert.Equal(t, "v3", results[0][0],
				"[%s] should return latest value (v3)", mode.name)

			// Remove works
			tx4 := db.NewTransaction()
			require.NoError(t, tx4.Remove(entityID, attr, "v3"))
			_, err = tx4.Commit()
			require.NoError(t, err)

			results, err = executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?attr :where [?e ?attr ?v]]`,
				entityID, attr,
			))
			require.NoError(t, err)
			assert.Len(t, results, 0,
				"[%s] unregistered attr Remove should work", mode.name)
		})
	}
}

// =============================================================================
// Nil-Schema Matcher Test
// =============================================================================
//
// Test 14: Data written with schema (tx.Set, OpNone), queried through a
// BadgerMatcher created without schema → CardinalityOne default works.
//
// This exercises the code path where CRDTResolvingIterator runs with nil schema
// and must default to CardinalityOne for all attributes.
//
// See: docs/bugs/BUG_SCHEMALESS_ATTR_BOUND_QUERY.md (test 14)
// =============================================================================

// Test 14: Nil-schema matcher can read data written with schema
func TestNilSchemaMatcher_ReadsSchemaData(t *testing.T) {
	db, cleanup := createCacheTestDB(t, false)
	defer cleanup()

	// Set up schema with CardinalityOne attribute
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	e := datalog.NewIdentity("alice")
	a := datalog.NewKeyword(":person/name")

	// Write via schema-aware tx.Add() — writes OpNone
	tx := db.NewTransaction()
	require.NoError(t, tx.Add(e, a, "Alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Overwrite — still OpNone, higher Tx
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Add(e, a, "Bob"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Create matcher WITHOUT schema
	matcher := NewBadgerMatcher(db.Store())
	// Deliberately NOT calling matcher.SetSchema()

	// Query through nil-schema matcher using a DataPattern
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: e},
			query.Constant{Value: a},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	require.NoError(t, err)

	// Should return data — CRDTResolvingIterator with nil schema defaults to
	// CardinalityOne. OpNone datoms are valid CardinalityOne assertions.
	// First entry (highest Tx) = "Bob". Should return exactly 1 result.
	count := 0
	var lastValue any
	iter := results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			lastValue = tuple[0]
		}
		count++
	}
	iter.Close()

	require.Equal(t, 1, count,
		"nil-schema matcher should return exactly 1 result (CardinalityOne default, LWW)")
	assert.Equal(t, "Bob", lastValue,
		"nil-schema matcher should return latest value (Bob)")
}
