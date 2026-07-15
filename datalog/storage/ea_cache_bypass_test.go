//go:build !(js && wasm)

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
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

		results, err := executor.CollectTuples(db.Query(
			`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
			personID))
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

		results, err := executor.CollectTuples(db.Query(
			`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
			personID, nameAttr))
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
			constantResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
				personID))
			require.NoError(t, err)

			// A as scalar input
			inputResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr))
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
			constantResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e :where [?e :person/tags ?v]]`,
				personID))
			require.NoError(t, err)

			// A as scalar input
			inputResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, tagsAttr))
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
			constantResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e :where [?e :doc/content ?v]]`,
				docID))
			require.NoError(t, err)

			// A as scalar input
			inputResults, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				docID, contentAttr))
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

			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ [[?e ?a]] :where [?e ?a ?v]]`,
				[]any{personID, nameAttr}))
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

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person2, ageAttr},
				}))
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

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?v :in $ [?e ...] ?a :where [?e ?a ?v]]`,
				[]datalog.Identity{entities[0], entities[1], entities[2]},
				nameAttr))
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

			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, ageAttr))
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

			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr))
			require.NoError(t, err)
			require.Len(t, results, 1)
			assert.Equal(t, "Alice", results[0][0])

			tx2 := db.NewTransaction()
			tx2.Set(personID, nameAttr, "Bob")
			_, err = tx2.Commit()
			require.NoError(t, err)

			results, err = executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr))
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

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person1, tagsAttr},
				}))
			require.NoError(t, err)
			assert.Len(t, results, 3,
				"[%s] Mixed cardinalities: 1 name + 2 tags = 3 results, got %d", mode.name, len(results))
		})
	}
}

// =============================================================================
// Phase 2 Tests: Per-tuple A from relation/join bindings
// =============================================================================

// TestEACacheBypass_PerRowA_UsesCache tests that the cache is used when both E and A
// are symbols in the binding relation with multiple tuples (per-tuple A from join results).
// This is the Phase 2 optimization target.
//
// Setup: Each entity has a :config/attr that names its own value attribute.
// Pattern 1 resolves the attribute name. Pattern 2 looks up the value.
// After pattern 1, the binding for pattern 2 has multiple tuples with varying (E, A).
func TestEACacheBypass_PerRowA_UsesCache(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{Path: dir})
	require.NoError(t, err)
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":config/attr"),
		ValueType:   schema.TypeKeyword,
		Cardinality: schema.CardinalityOne,
	})
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

	configAttr := datalog.NewKeyword(":config/attr")
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")

	entity1 := datalog.NewIdentity("entity-1")
	entity2 := datalog.NewIdentity("entity-2")

	tx := db.NewTransaction()
	// entity-1: config/attr = :person/name, person/name = "Alice"
	tx.Set(entity1, configAttr, nameAttr)
	tx.Set(entity1, nameAttr, "Alice")
	// entity-2: config/attr = :person/age, person/age = 30
	tx.Set(entity2, configAttr, ageAttr)
	tx.Set(entity2, ageAttr, int64(30))
	_, err = tx.Commit()
	require.NoError(t, err)

	var events []annotations.Event
	db.SetAnnotationHandler(func(e annotations.Event) {
		events = append(events, e)
	})
	defer db.SetAnnotationHandler(nil)

	// Query: for each entity in the collection, look up its config/attr to get ?a,
	// then look up [?e ?a ?v]. After pattern 1, binding has 2 tuples with varying A.
	results, err := executor.CollectTuples(db.Query(
		`[:find ?e ?a ?v :in $ [?e ...] :where
		  [?e :config/attr ?a]
		  [?e ?a ?v]]`,
		[]interface{}{entity1, entity2}))
	require.NoError(t, err)
	require.Len(t, results, 2, "Should get 2 results (one per entity)")

	// Check that storage/reuse-strategy is NOT used for pattern 2
	// Before Phase 2 fix: FAILS — reuse-strategy IS present (cache bypassed)
	// After Phase 2 fix: passes — per-tuple A uses cache
	assert.False(t, hasReuseStrategyEvent(events),
		"Per-tuple A from join should use cache path, not storage/reuse-strategy scans")
}

func TestEACacheBypass_PerRowVector_RelationInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			person1 := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")
			contentAttr := datalog.NewKeyword(":doc/content")

			tx := db.NewTransaction()
			tx.Set(person1, nameAttr, "Alice")
			_, err := tx.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			tx2.Set(person1, contentAttr, []interface{}{"a", "b", "c"})
			_, err = tx2.Commit()
			require.NoError(t, err)

			var events []annotations.Event
			db.SetAnnotationHandler(func(e annotations.Event) {
				events = append(events, e)
			})

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person1, contentAttr},
				}))
			require.NoError(t, err)

			db.SetAnnotationHandler(nil)

			if len(results) != 2 {
				t.Logf("[%s] Got %d results: %v", mode.name, len(results), results)
				for _, e := range events {
					t.Logf("[%s] EVENT: %s %v", mode.name, e.Name, e.Data)
				}
			}
			assert.Len(t, results, 2,
				"[%s] Per-tuple vector: 1 name + 1 resolved vector = 2 results, got %d", mode.name, len(results))
		})
	}
}

func TestEACacheBypass_CacheMissFallback(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createEACacheTestDB(t, mode.disableCache)
			defer cleanup()

			person1 := datalog.NewIdentity("person-1")
			person2 := datalog.NewIdentity("person-nonexistent")
			nameAttr := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			tx.Set(person1, nameAttr, "Alice")
			_, err := tx.Commit()
			require.NoError(t, err)

			results, err := executor.CollectTuples(db.Query(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				[][]any{
					{person1, nameAttr},
					{person2, nameAttr},
				}))
			require.NoError(t, err)
			assert.Len(t, results, 1,
				"[%s] Should return 1 result (Alice only, nonexistent entity skipped)", mode.name)
		})
	}
}

func TestEACacheBypass_JoinBoundA(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := NewDatabaseWithOptions(DatabaseOptions{
				Path:         dir,
				DisableCache: mode.disableCache,
			})
			require.NoError(t, err)
			defer db.Close()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":meta/target-attr"),
				ValueType:   schema.TypeKeyword,
				Cardinality: schema.CardinalityOne,
			})
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			metaID := datalog.NewIdentity("meta-1")
			personID := datalog.NewIdentity("person-1")
			targetAttr := datalog.NewKeyword(":meta/target-attr")
			nameAttr := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			tx.Set(metaID, targetAttr, nameAttr)
			tx.Set(personID, nameAttr, "Alice")
			_, err = tx.Commit()
			require.NoError(t, err)

			results, err := executor.CollectTuples(db.Query(
				`[:find ?v :in $ ?meta :where
				  [?meta :meta/target-attr ?a]
				  [?person ?a ?v]]`,
				metaID))
			require.NoError(t, err)
			require.Len(t, results, 1,
				"[%s] Join-bound A should return 1 result", mode.name)
			assert.Equal(t, "Alice", results[0][0],
				"[%s] Join-bound A should find person's name", mode.name)
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
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?e :where [?e :person/name ?v]]`, e))
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
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`, e, nameAttr))
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
					results, err := executor.CollectTuples(db.Query(
						`[:find ?v :in $ [[?e ?a]] :where [?e ?a ?v]]`,
						[]any{e, nameAttr}))
					if err != nil {
						b.Fatal(err)
					}
					if len(results) != 1 {
						b.Fatalf("expected 1 result, got %d", len(results))
					}
				}
			})

			b.Run("A_as_relation_input", func(b *testing.B) {
				b.ReportAllocs()
				relInput := make([][]any, 100)
				for i := 0; i < 100; i++ {
					relInput[i] = []any{entities[i], nameAttr}
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					results, err := executor.CollectTuples(db.Query(
						`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
						relInput))
					if err != nil {
						b.Fatal(err)
					}
					if len(results) != 100 {
						b.Fatalf("expected 100 results, got %d", len(results))
					}
				}
			})
		})
	}
}
