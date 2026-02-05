package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// =============================================================================
// CRDT Cache Matrix Tests
// =============================================================================
//
// These tests verify that CRDT resolution works correctly with cache enabled
// AND disabled. This is critical because:
// - Cache is an optimization, not a correctness requirement
// - The code MUST work correctly with DisableCache: true
// - If a test passes only with cache enabled, the fix is incomplete
//
// See: docs/bugs/BUG_CRDT_QUERY_RESOLUTION_NOT_APPLIED.md
// =============================================================================

// cacheTestMode represents whether tests run with cache enabled or disabled
type cacheTestMode struct {
	name         string
	disableCache bool
}

var cacheTestModes = []cacheTestMode{
	{"cache_enabled", false},
	{"cache_disabled", true},
}

// createCacheTestDB creates a test database with the given cache mode
func createCacheTestDB(t *testing.T, disableCache bool) (*Database, func()) {
	dir := t.TempDir()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         dir,
		DisableCache: disableCache,
	})
	require.NoError(t, err)
	return db, func() { db.Close() }
}

// =============================================================================
// Test Matrix: All binding patterns
// =============================================================================

// TestCacheMatrix_AConstant tests when A is a constant in the pattern (baseline)
func TestCacheMatrix_AConstant(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")

			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, datalog.NewKeyword(":person/name"), name)
				tx.Commit()
			}

			// Pattern: A as constant (baseline - should work)
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e :where [?e :person/name ?v]]`,
				personID)
			require.NoError(t, err)
			assert.Len(t, results, 1, "[%s] A as constant should return 1 result", mode.name)
			if len(results) == 1 {
				assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
			}
		})
	}
}

// TestCacheMatrix_AFromScalarInput tests when A comes from a scalar input
func TestCacheMatrix_AFromScalarInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, nameAttr, name)
				tx.Commit()
			}

			// Pattern: A from scalar input
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, nameAttr)
			require.NoError(t, err)
			assert.Len(t, results, 1, "[%s] A from scalar input should return 1 result", mode.name)
			if len(results) == 1 {
				assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
			}
		})
	}
}

// TestCacheMatrix_AUnbound tests when A is completely unbound
func TestCacheMatrix_AUnbound(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

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

			personID := datalog.NewIdentity("person-1")

			// Write multiple values to name
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, datalog.NewKeyword(":person/name"), name)
				tx.Commit()
			}

			// Write multiple values to age
			for _, age := range []int64{20, 25, 30} {
				tx := db.NewTransaction()
				tx.Set(personID, datalog.NewKeyword(":person/age"), age)
				tx.Commit()
			}

			// Pattern: A completely unbound - should get 2 results (one per attribute)
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?a ?v :in $ ?e :where [?e ?a ?v]]`,
				personID)
			require.NoError(t, err)

			// Should return 2 results: one for name (Charlie), one for age (30)
			// NOT 6 results (all historical values)
			assert.Len(t, results, 2, "[%s] Unbound A should return 1 result per attribute (CRDT resolved)", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_EFromCollection_AFromScalar tests E from collection, A from scalar
func TestCacheMatrix_EFromCollection_AFromScalar(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			nameAttr := datalog.NewKeyword(":person/name")

			// Create 3 entities, each with multiple historical values
			entities := []datalog.Identity{
				datalog.NewIdentity("person-1"),
				datalog.NewIdentity("person-2"),
				datalog.NewIdentity("person-3"),
			}

			for i, entity := range entities {
				for _, suffix := range []string{"First", "Second", "Final"} {
					tx := db.NewTransaction()
					tx.Set(entity, nameAttr, fmt.Sprintf("Person%d-%s", i+1, suffix))
					tx.Commit()
				}
			}

			// Pattern: E from collection, A from scalar
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?e ?v :in $ [?e ...] ?a :where [?e ?a ?v]]`,
				entities, nameAttr)
			require.NoError(t, err)

			// Should return 3 results (one per entity, CRDT resolved)
			// NOT 9 results (all historical values)
			assert.Len(t, results, 3, "[%s] E from collection should return 1 result per entity", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_CardinalityMany tests CardinalityMany add-wins resolution
func TestCacheMatrix_CardinalityMany(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/tags"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityMany,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			tagsAttr := datalog.NewKeyword(":person/tags")

			// Set tags multiple times - each Set replaces the entire set
			tagSets := [][]string{
				{"red", "green", "blue"},
				{"alpha", "beta"},
				{"one", "two", "three"},
			}
			for _, tags := range tagSets {
				tx := db.NewTransaction()
				tagValues := make([]interface{}, len(tags))
				for i, tag := range tags {
					tagValues[i] = tag
				}
				tx.Set(personID, tagsAttr, tagValues)
				tx.Commit()
			}

			expectedCount := 3 // Last set has 3 tags

			// Query with A from scalar input
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				personID, tagsAttr)
			require.NoError(t, err)

			// Should return 3 results (current set members)
			// NOT 8 results (all historical tags)
			assert.Len(t, results, expectedCount, "[%s] CardinalityMany should return current set only", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// NOTE: [(history)] predicate removed from test matrix.
// History queries will use db.History() Datomic-style database view (future work).
// See: https://docs.datomic.com/client-tutorial/history.html

// TestCacheMatrix_PullIntoComparison verifies PullInto works in both modes
func TestCacheMatrix_PullIntoComparison(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")

			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, datalog.NewKeyword(":person/name"), name)
				tx.Commit()
			}

			// PullInto should always work
			type Person struct {
				ID   datalog.Identity `datalog:"-,id"`
				Name string           `datalog:"person/name"`
			}
			var person Person
			err := db.PullInto(personID, &person)
			require.NoError(t, err)
			assert.Equal(t, "Charlie", person.Name, "[%s] PullInto should return LWW winner", mode.name)
		})
	}
}

// =============================================================================
// Test Matrix: Additional binding patterns
// =============================================================================

// TestCacheMatrix_AFromCollection tests when A comes from a collection input
func TestCacheMatrix_AFromCollection(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

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

			personID := datalog.NewIdentity("person-1")

			// Write multiple values to name
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, datalog.NewKeyword(":person/name"), name)
				tx.Commit()
			}

			// Write multiple values to age
			for _, age := range []int64{20, 25, 30} {
				tx := db.NewTransaction()
				tx.Set(personID, datalog.NewKeyword(":person/age"), age)
				tx.Commit()
			}

			// Pattern: A from collection input - query both attributes
			attrs := []datalog.Keyword{
				datalog.NewKeyword(":person/name"),
				datalog.NewKeyword(":person/age"),
			}
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?a ?v :in $ ?e [?a ...] :where [?e ?a ?v]]`,
				personID, attrs)
			require.NoError(t, err)

			// Should return 2 results: one for name (Charlie), one for age (30)
			// NOT 6 results (all historical values)
			assert.Len(t, results, 2, "[%s] A from collection should return 1 result per attribute", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_AFromTupleInput tests when E and A come from a tuple input
func TestCacheMatrix_AFromTupleInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, nameAttr, name)
				tx.Commit()
			}

			// Pattern: E and A from tuple input [[?e ?a]]
			// Tuple input syntax requires double brackets
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ [[?e ?a]] :where [?e ?a ?v]]`,
				[]any{personID, nameAttr})
			require.NoError(t, err)

			assert.Len(t, results, 1, "[%s] Tuple input should return 1 result", mode.name)
			if len(results) == 1 {
				assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
			}

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_AFromRelationInput tests when E and A come from a relation input
func TestCacheMatrix_AFromRelationInput(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

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

			person1 := datalog.NewIdentity("person-1")
			person2 := datalog.NewIdentity("person-2")
			nameAttr := datalog.NewKeyword(":person/name")
			ageAttr := datalog.NewKeyword(":person/age")

			// Write multiple values for person1's name
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(person1, nameAttr, name)
				tx.Commit()
			}

			// Write multiple values for person2's age
			for _, age := range []int64{20, 25, 30} {
				tx := db.NewTransaction()
				tx.Set(person2, ageAttr, age)
				tx.Commit()
			}

			// Pattern: E and A from relation input [[?e ?a] ...]
			// Relation input uses [[vars] ...] syntax with slice of slices
			// Note: Keywords need to be converted to strings for the input
			relationInput := [][]any{
				{person1, nameAttr},
				{person2, ageAttr},
			}
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?e ?a ?v :in $ [[?e ?a] ...] :where [?e ?a ?v]]`,
				relationInput)
			require.NoError(t, err)

			// Should return 2 results: person1's name (Charlie), person2's age (30)
			// NOT 6 results (all historical values)
			assert.Len(t, results, 2, "[%s] Relation input should return 1 result per (E,A) pair", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_ABoundViaJoin tests when A is bound via join from another pattern
func TestCacheMatrix_ABoundViaJoin(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":config/tracked-attr"),
				ValueType:   schema.TypeKeyword,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			configID := datalog.NewIdentity("config-1")
			nameAttr := datalog.NewKeyword(":person/name")

			// Write multiple values for person's name
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, nameAttr, name)
				tx.Commit()
			}

			// Config entity stores which attribute to track
			tx := db.NewTransaction()
			tx.Set(configID, datalog.NewKeyword(":config/tracked-attr"), nameAttr)
			tx.Commit()

			// Pattern: A bound via join - get attribute from config, then use it
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?person ?config :where
				  [?config :config/tracked-attr ?a]
				  [?person ?a ?v]]`,
				personID, configID)
			require.NoError(t, err)

			// Should return 1 result (Charlie) - CRDT resolved
			assert.Len(t, results, 1, "[%s] A bound via join should return 1 result", mode.name)
			if len(results) == 1 {
				assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
			}

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_EAndABothFromCollections tests E and A both from collections
func TestCacheMatrix_EAndABothFromCollections(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

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

			// Write multiple values for each entity/attribute combination
			for i, entity := range entities {
				for _, name := range []string{"First", "Second", "Final"} {
					tx := db.NewTransaction()
					tx.Set(entity, attrs[0], fmt.Sprintf("Person%d-%s", i+1, name))
					tx.Commit()
				}
				for _, age := range []int64{20, 25, 30} {
					tx := db.NewTransaction()
					tx.Set(entity, attrs[1], age+int64(i*10))
					tx.Commit()
				}
			}

			// Pattern: Both E and A from collections
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?e ?a ?v :in $ [?e ...] [?a ...] :where [?e ?a ?v]]`,
				entities, attrs)
			require.NoError(t, err)

			// Should return 4 results: 2 entities × 2 attributes
			// NOT 12 results (all historical values)
			assert.Len(t, results, 4, "[%s] Both collections should return 1 result per (E,A) combination", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_WithNotClause tests CRDT resolution with NOT clause
func TestCacheMatrix_WithNotClause(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/active"),
				ValueType:   schema.TypeBoolean,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			person1 := datalog.NewIdentity("person-1")
			person2 := datalog.NewIdentity("person-2")
			nameAttr := datalog.NewKeyword(":person/name")
			activeAttr := datalog.NewKeyword(":person/active")

			// person1: name changes, ends up active
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(person1, nameAttr, name)
				tx.Commit()
			}
			tx := db.NewTransaction()
			tx.Set(person1, activeAttr, true)
			tx.Commit()

			// person2: name changes, ends up inactive
			for _, name := range []string{"Dave", "Eve", "Frank"} {
				tx := db.NewTransaction()
				tx.Set(person2, nameAttr, name)
				tx.Commit()
			}
			tx = db.NewTransaction()
			tx.Set(person2, activeAttr, false)
			tx.Commit()

			// Query: find names of active people (using NOT to exclude inactive)
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?name :where
				  [?e :person/name ?name]
				  [?e :person/active true]
				  (not [?e :person/active false])]`,
			)
			require.NoError(t, err)

			// Should return 1 result (Charlie) - only active person's current name
			assert.Len(t, results, 1, "[%s] NOT clause should work with CRDT resolved values", mode.name)
			if len(results) == 1 {
				assert.Equal(t, "Charlie", results[0][0], "[%s] Should return active person's LWW name", mode.name)
			}

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_WithOrClause tests CRDT resolution with OR clause
func TestCacheMatrix_WithOrClause(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/status"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			person1 := datalog.NewIdentity("person-1")
			person2 := datalog.NewIdentity("person-2")
			person3 := datalog.NewIdentity("person-3")
			nameAttr := datalog.NewKeyword(":person/name")
			statusAttr := datalog.NewKeyword(":person/status")

			// Setup: 3 people with changing names, different final statuses
			for i, person := range []datalog.Identity{person1, person2, person3} {
				for _, name := range []string{"First", "Second", "Final"} {
					tx := db.NewTransaction()
					tx.Set(person, nameAttr, fmt.Sprintf("Person%d-%s", i+1, name))
					tx.Commit()
				}
			}

			// Final statuses: person1=active, person2=pending, person3=inactive
			statuses := []string{"active", "pending", "inactive"}
			for i, person := range []datalog.Identity{person1, person2, person3} {
				tx := db.NewTransaction()
				tx.Set(person, statusAttr, statuses[i])
				tx.Commit()
			}

			// Query: find names where status is "active" OR "pending"
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?name :where
				  [?e :person/name ?name]
				  (or [?e :person/status "active"]
				      [?e :person/status "pending"])]`,
			)
			require.NoError(t, err)

			// Should return 2 results (Person1-Final, Person2-Final)
			// NOT 6 results (all historical names of those people)
			assert.Len(t, results, 2, "[%s] OR clause should return CRDT resolved names", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_WithAggregation tests CRDT resolution with aggregation
func TestCacheMatrix_WithAggregation(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/score"),
				ValueType:   schema.TypeLong,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			// Create 3 people, each with multiple historical scores
			for i := 1; i <= 3; i++ {
				personID := datalog.NewIdentity(fmt.Sprintf("person-%d", i))
				for _, score := range []int64{10, 20, 30} {
					tx := db.NewTransaction()
					tx.Set(personID, datalog.NewKeyword(":person/score"), score*int64(i))
					tx.Commit()
				}
			}

			// Query: sum of all scores
			results, err := db.ExecuteQueryWithInputs(
				`[:find (sum ?score) :where [?e :person/score ?score]]`,
			)
			require.NoError(t, err)

			// Should sum only the CURRENT scores: 30 + 60 + 90 = 180
			// NOT all historical: (10+20+30) + (20+40+60) + (30+60+90) = 360
			assert.Len(t, results, 1, "[%s] Aggregation should have 1 result", mode.name)
			if len(results) == 1 {
				sum, ok := results[0][0].(int64)
				if !ok {
					// Try float64 (some aggregations return float)
					sumFloat, _ := results[0][0].(float64)
					sum = int64(sumFloat)
				}
				assert.Equal(t, int64(180), sum, "[%s] Sum should be of CRDT resolved values only", mode.name)
			}

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_CardinalityVector tests CardinalityVector RGA resolution
func TestCacheMatrix_CardinalityVector(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":doc/content"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityVector,
			})
			db.SetSchema(s)

			docID := datalog.NewIdentity("doc-1")
			contentAttr := datalog.NewKeyword(":doc/content")

			// Set vector content multiple times
			vectors := [][]string{
				{"line1", "line2"},
				{"a", "b", "c"},
				{"final1", "final2", "final3", "final4"},
			}
			for _, vec := range vectors {
				tx := db.NewTransaction()
				vals := make([]interface{}, len(vec))
				for i, v := range vec {
					vals[i] = v
				}
				tx.Set(docID, contentAttr, vals)
				tx.Commit()
			}

			// Query with A from scalar input
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?e ?a :where [?e ?a ?v]]`,
				docID, contentAttr)
			require.NoError(t, err)

			// Should return 4 results (current vector elements)
			// NOT 9 results (all historical elements)
			assert.Len(t, results, 4, "[%s] CardinalityVector should return current vector only", mode.name)

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_ABoundViaSubquery tests when A is bound via a subquery result
// NOTE: This test uses a simpler pattern - the subquery returns the attribute keyword,
// which is then used in the main pattern.
func TestCacheMatrix_ABoundViaSubquery(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":config/attr"),
				ValueType:   schema.TypeKeyword,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			configID := datalog.NewIdentity("config-1")
			nameAttr := datalog.NewKeyword(":person/name")

			// Write multiple values for person's name
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, nameAttr, name)
				tx.Commit()
			}

			// Config stores which attribute to query
			tx := db.NewTransaction()
			tx.Set(configID, datalog.NewKeyword(":config/attr"), nameAttr)
			tx.Commit()

			// First verify the subquery works alone
			subResults, err := db.ExecuteQueryWithInputs(
				`[:find ?a :in $ ?c :where [?c :config/attr ?a]]`,
				configID)
			require.NoError(t, err)
			t.Logf("[%s] Subquery results: %v", mode.name, subResults)

			// Pattern: A bound via subquery using scalar binding
			// The subquery returns a single attribute value
			results, err := db.ExecuteQueryWithInputs(
				`[:find ?v :in $ ?person ?config :where
				  [(q [:find ?a . :in $ ?c :where [?c :config/attr ?a]] $ ?config) ?a]
				  [?person ?a ?v]]`,
				personID, configID)
			require.NoError(t, err)

			// Should return 1 result (Charlie) - CRDT resolved
			assert.Len(t, results, 1, "[%s] A bound via subquery should return 1 result", mode.name)
			if len(results) == 1 {
				assert.Equal(t, "Charlie", results[0][0], "[%s] Should return LWW winner", mode.name)
			}

			t.Logf("[%s] Results: %v", mode.name, results)
		})
	}
}

// TestCacheMatrix_AsOfQuery tests [(as-of ?tx N)] for point-in-time queries
func TestCacheMatrix_AsOfQuery(t *testing.T) {
	for _, mode := range cacheTestModes {
		t.Run(mode.name, func(t *testing.T) {
			db, cleanup := createCacheTestDB(t, mode.disableCache)
			defer cleanup()

			s := schema.NewSchema()
			s.Add(&schema.AttributeDefinition{
				Ident:       datalog.NewKeyword(":person/name"),
				ValueType:   schema.TypeString,
				Cardinality: schema.CardinalityOne,
			})
			db.SetSchema(s)

			personID := datalog.NewIdentity("person-1")
			nameAttr := datalog.NewKeyword(":person/name")

			// Write values
			for _, name := range []string{"Alice", "Bob", "Charlie"} {
				tx := db.NewTransaction()
				tx.Set(personID, nameAttr, name)
				tx.Commit()
			}

			// First, query all values with their tx times to find the Lamport values
			allResults, err := db.ExecuteQueryWithInputs(
				`[:find ?v ?tx :in $ ?e ?a :where [?e ?a ?v ?tx]]`,
				personID, nameAttr)
			require.NoError(t, err)
			t.Logf("[%s] All values with tx: %v", mode.name, allResults)

			// Find the Lamport time for "Alice" (first write)
			var aliceLamport uint64
			for _, r := range allResults {
				if r[0] == "Alice" {
					// ElementID comes back as a pointer
					switch v := r[1].(type) {
					case datalog.ElementID:
						aliceLamport = v.Lamport
					case *datalog.ElementID:
						aliceLamport = v.Lamport
					default:
						t.Logf("[%s] tx type for Alice: %T = %v", mode.name, r[1], r[1])
					}
					break
				}
			}

			if aliceLamport == 0 {
				t.Skipf("[%s] Could not determine Lamport time for Alice", mode.name)
			}

			// Query as-of the first transaction - should return "Alice"
			results, err := db.ExecuteQueryWithInputs(
				fmt.Sprintf(`[:find ?v :in $ ?e :where [?e :person/name ?v ?tx] [(as-of ?tx %d)]]`, aliceLamport),
				personID)
			require.NoError(t, err)

			// as-of should return only values at or before that Lamport time
			// For aliceLamport, that should be just "Alice"
			assert.Len(t, results, 1, "[%s] as-of first tx should return 1 result", mode.name)
			if len(results) == 1 {
				assert.Equal(t, "Alice", results[0][0], "[%s] as-of first tx should return Alice", mode.name)
			}

			t.Logf("[%s] as-of %d results: %v", mode.name, aliceLamport, results)
		})
	}
}
