package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// EA Cache Bypass Tests
// =============================================================================
//
// These tests verify that queries with A from input bindings produce correct
// results with cache enabled AND disabled, and that the cache path is used
// when available.
//
// See: docs/bugs/BUG_EA_CACHE_BYPASS_VARIABLE_ATTRIBUTE.md
// =============================================================================

// hasReuseStrategyEvent checks if any "storage/reuse-strategy" annotation was emitted.
// Presence of this event means the cache was bypassed and join strategies were used.
func hasReuseStrategyEvent(events []annotations.Event) bool {
	for _, e := range events {
		if e.Name == "storage/reuse-strategy" {
			return true
		}
	}
	return false
}

// createEACacheTestDB creates a database with schema for cache bypass testing.
func createEACacheTestDB(t *testing.T, disableCache bool) (*Database, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         dir,
		DisableCache: disableCache,
	})
	require.NoError(t, err)

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
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":doc/content"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityVector,
	})
	db.SetSchema(s)

	return db, func() { db.Close() }
}

// =============================================================================
// Test 1: Reproduction — prove the cache path is taken (cache-only test)
// =============================================================================

func TestEACacheBypass_Reproduction(t *testing.T) {
	db, cleanup := createEACacheTestDB(t, false)
	defer cleanup()

	personID := datalog.NewIdentity("person-1")
	nameAttr := datalog.NewKeyword(":person/name")

	for _, name := range []string{"Alice", "Bob", "Charlie"} {
		tx := db.NewTransaction()
		tx.Set(personID, nameAttr, name)
		_, err := tx.Commit()
		require.NoError(t, err)
	}

	t.Run("A_constant_uses_cache", func(t *testing.T) {
		var events []annotations.Event
		db.SetAnnotationHandler(func(e annotations.Event) {
			events = append(events, e)
		})
		defer db.SetAnnotationHandler(nil)

		results, err := db.ExecuteQueryWithInputs(
			`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
			personID)
		require.NoError(t, err)
		require.Len(t, results, 1)
		assert.Equal(t, "Charlie", results[0][0])

		assert.False(t, hasReuseStrategyEvent(events),
			"A as constant should use cache path, but storage/reuse-strategy was emitted")
	})

	t.Run("A_scalar_input_uses_cache", func(t *testing.T) {
		var events []annotations.Event
		db.SetAnnotationHandler(func(e annotations.Event) {
			events = append(events, e)
		})
		defer db.SetAnnotationHandler(nil)

		results, err := db.ExecuteQueryWithInputs(
			`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
			personID, nameAttr)
		require.NoError(t, err)
		require.Len(t, results, 1, "Should return 1 result (LWW winner)")
		assert.Equal(t, "Charlie", results[0][0])

		assert.False(t, hasReuseStrategyEvent(events),
			"A from scalar input should use cache path, not storage scans")
	})
}

// =============================================================================
// Tests 2-4: Correctness per cardinality (cache enabled AND disabled)
// =============================================================================

func TestEACacheBypass_CardinalityOne(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, nameAttr, name)
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			// A as constant
			constantResults, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
				personID)
			require.NoError(t, err)

			// A as scalar input
			inputResults, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr)
			require.NoError(t, err)

			require.Len(t, constantResults, 1, "[%s] A-as-constant: should return 1 result", mode.name)
			require.Len(t, inputResults, 1, "[%s] A-as-input: should return 1 result", mode.name)
			assert.Equal(t, constantResults[0][0], inputResults[0][0],
				"[%s] Results should be identical", mode.name)
			assert.Equal(t, "Charlie", inputResults[0][0])
		})
	}
}

func TestEACacheBypass_CardinalityMany(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			personID := datalog.NewIdentity("person-1")
			tagsAttr := datalog.NewKeyword(":person/tags")

			tx := db.NewTransaction()
			tx.Add(personID, tagsAttr, "warrior")
			tx.Add(personID, tagsAttr, "veteran")
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			tx2.Remove(personID, tagsAttr, "warrior")
			_, err = tx2.Commit()
			require.NoError(t, err)

			// A as constant
			constantResults, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e :where [?e :person/tags ?v]]`,
				personID)
			require.NoError(t, err)

			// A as scalar input
			inputResults, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, tagsAttr)
			require.NoError(t, err)

			require.Len(t, constantResults, 1, "[%s] A-as-constant: should return 1 result (add-wins)", mode.name)
			require.Len(t, inputResults, 1, "[%s] A-as-input: should return 1 result (add-wins)", mode.name)
			assert.Equal(t, "veteran", constantResults[0][0])
			assert.Equal(t, "veteran", inputResults[0][0])
		})
	}
}

func TestEACacheBypass_CardinalityVector(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			docID := datalog.NewIdentity("doc-1")
			contentAttr := datalog.NewKeyword(":doc/content")

			tx := db.NewTransaction()
			tx.Set(docID, contentAttr, []interface{}{"a", "b", "c"})
			_, err := tx.Commit()
			require.NoError(t, err)

			// A as constant
			constantResults, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e :where [?e :doc/content ?v]]`,
				docID)
			require.NoError(t, err)

			// A as scalar input
			inputResults, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				docID, contentAttr)
			require.NoError(t, err)

			require.Len(t, constantResults, 1, "[%s] A-as-constant: should return 1 result (resolved vector)", mode.name)
			require.Len(t, inputResults, 1, "[%s] A-as-input: should return 1 result (resolved vector)", mode.name)
			assert.Equal(t, constantResults[0][0], inputResults[0][0],
				"[%s] Vector results should be identical", mode.name)
		})
	}
}

// =============================================================================
// Tests 5-7: Input binding patterns (cache enabled AND disabled)
// =============================================================================

func TestEACacheBypass_TupleInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, nameAttr, name)
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ [[?e ?a]] :where [?e ?a ?v]]`,
				[]any{personID, nameAttr})
			require.NoError(t, err)
			require.Len(t, results, 1, "[%s] Tuple input should return 1 result", mode.name)
			assert.Equal(t, "Charlie", results[0][0])
		})
	}
}

func TestEACacheBypass_RelationInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			person1 := datalog.NewIdentity("person-1")
			person2 := datalog.NewIdentity("person-2")
			nameAttr := datalog.NewKeyword(":person/name")
			ageAttr := datalog.NewKeyword(":person/age")

			tx := db.NewTransaction()
			tx.Set(person1, nameAttr, "Alice")
			tx.Set(person2, ageAttr, int64(30))
			_, err := tx.Commit()
			require.NoError(t, err)

			results, err := db.ExecuteQueryWithInputs(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person2, ageAttr},
				})
			require.NoError(t, err)
			require.Len(t, results, 2, "[%s] Relation input should return 2 results", mode.name)
		})
	}
}

func TestEACacheBypass_CollectionEWithScalarA(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			nameAttr := datalog.NewKeyword(":person/name")

			entities := make([]datalog.Identity, 3)
			for i := 0; i < 3; i++ {
				entities[i] = datalog.NewIdentity(fmt.Sprintf("person-%d", i))
				for _, name := range []string{"First", "Second", fmt.Sprintf("Final-%d", i)} {
					tx := db.NewTransaction()
					tx.Set(entities[i], nameAttr, name)
					_, err := tx.Commit()
					require.NoError(t, err)
				}
			}

			results, err := db.ExecuteQueryWithInputs(
				`[:find ?e ?v :in $ [?e ...] ?a :where [?e ?a ?v]]`,
				[]datalog.Identity{entities[0], entities[1], entities[2]},
				nameAttr)
			require.NoError(t, err)
			require.Len(t, results, 3, "[%s] Should return 3 results (one LWW winner per entity)", mode.name)
		})
	}
}

// =============================================================================
// Tests 8-10: Edge cases (cache enabled AND disabled)
// =============================================================================

func TestEACacheBypass_NonexistentAttribute(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")
			ageAttr := datalog.NewKeyword(":person/age")

			tx := db.NewTransaction()
			tx.Set(personID, nameAttr, "Alice")
			_, err := tx.Commit()
			require.NoError(t, err)

			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, ageAttr)
			require.NoError(t, err)
			assert.Len(t, results, 0, "[%s] Nonexistent attribute should return 0 results", mode.name)
		})
	}
}

func TestEACacheBypass_CacheInvalidation(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			tx.Set(personID, nameAttr, "Alice")
			_, err := tx.Commit()
			require.NoError(t, err)

			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr)
			require.NoError(t, err)
			require.Len(t, results, 1)
			assert.Equal(t, "Alice", results[0][0])

			tx2 := db.NewTransaction()
			tx2.Set(personID, nameAttr, "Bob")
			_, err = tx2.Commit()
			require.NoError(t, err)

			results, err = db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr)
			require.NoError(t, err)
			require.Len(t, results, 1)
			assert.Equal(t, "Bob", results[0][0], "[%s] Should see new value after write", mode.name)
		})
	}
}

func TestEACacheBypass_MixedCardinalities_RelationInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			person1 := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")
			tagsAttr := datalog.NewKeyword(":person/tags")

			tx := db.NewTransaction()
			tx.Set(person1, nameAttr, "Alice")
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			tx2.Add(person1, tagsAttr, "warrior")
			tx2.Add(person1, tagsAttr, "veteran")
			_, err = tx2.Commit()
			require.NoError(t, err)

			results, err := db.ExecuteQueryWithInputs(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person1, tagsAttr},
				})
			require.NoError(t, err)
			assert.Len(t, results, 3,
				"[%s] Mixed cardinalities: 1 name + 2 tags = 3 results, got %d", mode.name, len(results))
		})
	}
}

// =============================================================================
// Benchmark (cache enabled vs cache disabled)
// =============================================================================

func BenchmarkEACacheBypass(b *testing.B) {
	for _, mode := range []struct {
		name         string
		disableCache bool
	}{
		{"cache_enabled", false},
		{"cache_disabled", true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:         dir,
				DisableCache: mode.disableCache,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			nameAttr := datalog.NewKeyword(":person/name")

			entities := make([]datalog.Identity, 100)
			for i := 0; i < 100; i++ {
				entities[i] = datalog.NewIdentity(fmt.Sprintf("person-%d", i))
				for _, name := range []string{"First", "Second", fmt.Sprintf("Final-%d", i)} {
					tx := db.NewTransaction()
					tx.Set(entities[i], nameAttr, name)
					if _, err := tx.Commit(); err != nil {
						b.Fatal(err)
					}
				}
			}

			b.Run("A_as_constant", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					e := entities[i%100]
					results, err := db.ExecuteQueryWithInputs(
						`[:find ?v :in $ ?e :where [?e :person/name ?v]]`, e)
					if err != nil {
						b.Fatal(err)
					}
					if len(results) != 1 {
						b.Fatalf("expected 1 result, got %d", len(results))
					}
				}
			})

			b.Run("A_as_scalar_input", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					e := entities[i%100]
					results, err := db.ExecuteQueryWithInputs(
						`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, e, nameAttr)
					if err != nil {
						b.Fatal(err)
					}
					if len(results) != 1 {
						b.Fatalf("expected 1 result, got %d", len(results))
					}
				}
			})

			b.Run("A_as_tuple_input", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					e := entities[i%100]
					results, err := db.ExecuteQueryWithInputs(
						`[:find ?v :in $ [[?e ?a]] :where [?e ?a ?v]]`,
						[]any{e, nameAttr})
					if err != nil {
						b.Fatal(err)
					}
					if len(results) != 1 {
						b.Fatalf("expected 1 result, got %d", len(results))
					}
				}
			})
		})
	}
}
