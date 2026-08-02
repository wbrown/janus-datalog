package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// EntityWithLore demonstrates the pattern where an entity has an optional
// string field that may be empty initially and updated later.
type EntityWithLore struct {
	ID   datalog.Identity `datalog:"db/id,id"`
	Name string           `datalog:"entity/name"`
	Lore string           `datalog:"entity/lore"` // Optional - may be empty initially
}

// loreSchema is the schema every test in this file writes under.
func loreSchema(t *testing.T) storage.DatabaseOptions {
	t.Helper()
	s, err := dlreflect.SchemaFromStruct(EntityWithLore{})
	require.NoError(t, err)
	return storage.DatabaseOptions{Schema: s}
}

// TestCardinalityOneBehavior documents the behavior of cardinality-one attributes
// when using tx.Add vs SaveStruct for updates.
//
// Key findings (CRDT implementation):
//  1. tx.Add is schema-aware - for cardinality-one, it uses LWW semantics
//     (Last-Writer-Wins), so only the most recent value is returned by queries
//  2. SaveStruct properly handles cardinality-one upserts (uses LWW internally)
//  3. If an entity is created with SaveStruct and has empty string fields,
//     those empty strings ARE persisted to the database
//  4. Later using tx.Add will update the value using LWW (single value returned)
//
// Both tx.Add and SaveStruct are valid patterns for cardinality-one updates.
// The CRDT implementation ensures queries always return the latest value.
func TestCardinalityOneBehavior(t *testing.T) {
	t.Run("tx.Add uses LWW semantics for cardinality-one", func(t *testing.T) {
		eachBackendAndModeOpts(t, loreSchema(t), func(t *testing.T, db *storage.Database) {
			// Step 1: Create entity with empty Lore via SaveStruct
			entity := &EntityWithLore{
				Name: "Test Entity",
				Lore: "", // Empty lore - will be saved as empty string
			}

			tx1 := db.NewTransaction()
			id, err := tx1.SaveStruct(entity)
			require.NoError(t, err)
			_, err = tx1.Commit()
			require.NoError(t, err)

			// Verify: empty string was saved
			loreValues1, err := executor.CollectTuples(db.Query(
				`[:find ?lore :in $ ?e :where [?e :entity/lore ?lore]]`,
				id,
			))
			require.NoError(t, err)
			require.Len(t, loreValues1, 1, "Empty string should be saved")
			assert.Equal(t, "", loreValues1[0][0], "Value should be empty string")

			// Step 2: Use tx.Add to add a new lore value
			// With CRDT implementation, Add() uses LWW semantics for cardinality-one
			tx2 := db.NewTransaction()
			loreAttr := datalog.NewKeyword(":entity/lore")
			tx2.Add(id, loreAttr, "A detailed description.")
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Step 3: Query for lore values - with LWW, only latest value is returned
			loreValues2, err := executor.CollectTuples(db.Query(
				`[:find ?lore :in $ ?e :where [?e :entity/lore ?lore]]`,
				id,
			))
			require.NoError(t, err)

			// With CRDT LWW semantics, tx.Add updates the value (highest ElementID wins)
			t.Logf("After tx.Add: %d lore value(s): %v", len(loreValues2), loreValues2)
			assert.Len(t, loreValues2, 1, "LWW semantics: only latest value returned")
			assert.Equal(t, "A detailed description.", loreValues2[0][0], "Latest value should be returned")
		})
	})

	t.Run("SaveStruct properly handles cardinality-one upserts", func(t *testing.T) {
		eachBackendAndModeOpts(t, loreSchema(t), func(t *testing.T, db *storage.Database) {
			// Step 1: Create entity with empty Lore
			entity := &EntityWithLore{
				Name: "Test Entity",
				Lore: "", // Empty initially
			}

			tx1 := db.NewTransaction()
			_, err := tx1.SaveStruct(entity)
			require.NoError(t, err)
			_, err = tx1.Commit()
			require.NoError(t, err)

			// Step 2: Update lore via SaveStruct (CORRECT pattern)
			entity.Lore = "A detailed description."
			tx2 := db.NewTransaction()
			_, err = tx2.SaveStruct(entity)
			require.NoError(t, err)
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Step 3: Query - should have exactly ONE value
			loreValues, err := executor.CollectTuples(db.Query(
				`[:find ?lore :in $ ?e :where [?e :entity/lore ?lore]]`,
				entity.ID,
			))
			require.NoError(t, err)

			t.Logf("After SaveStruct update: %d lore value(s): %v", len(loreValues), loreValues)
			require.Len(t, loreValues, 1, "SaveStruct should maintain cardinality-one")
			assert.Equal(t, "A detailed description.", loreValues[0][0])
		})
	})

	t.Run("Correct update pattern: load, modify, SaveStruct", func(t *testing.T) {
		eachBackendAndModeOpts(t, loreSchema(t), func(t *testing.T, db *storage.Database) {
			// Create entity
			entity := &EntityWithLore{
				Name: "Test Entity",
				Lore: "Initial lore",
			}

			tx1 := db.NewTransaction()
			id, err := tx1.SaveStruct(entity)
			require.NoError(t, err)
			_, err = tx1.Commit()
			require.NoError(t, err)

			// CORRECT UPDATE PATTERN:
			// 1. Load the entity
			var loaded EntityWithLore
			err = db.PullInto(id, &loaded)
			require.NoError(t, err)

			// 2. Modify the field
			loaded.Lore = "Updated lore"

			// 3. Save with SaveStruct
			tx2 := db.NewTransaction()
			_, err = tx2.SaveStruct(&loaded)
			require.NoError(t, err)
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Verify: only one value, and it's the updated one
			loreValues, err := executor.CollectTuples(db.Query(
				`[:find ?lore :in $ ?e :where [?e :entity/lore ?lore]]`,
				id,
			))
			require.NoError(t, err)
			require.Len(t, loreValues, 1)
			assert.Equal(t, "Updated lore", loreValues[0][0])
		})
	})
}

// TestEmptyStringsAreSaved documents that SaveStruct saves empty strings
// (there is no omitempty equivalent for datalog struct tags).
func TestEmptyStringsAreSaved(t *testing.T) {
	eachBackendAndModeOpts(t, loreSchema(t), func(t *testing.T, db *storage.Database) {
		// Create entity with empty Lore
		entity := &EntityWithLore{
			Name: "Test Entity",
			Lore: "", // Empty string
		}

		tx := db.NewTransaction()
		id, err := tx.SaveStruct(entity)
		require.NoError(t, err)
		_, err = tx.Commit()
		require.NoError(t, err)

		// Query for lore - empty string IS saved
		loreValues, err := executor.CollectTuples(db.Query(
			`[:find ?lore :in $ ?e :where [?e :entity/lore ?lore]]`,
			id,
		))
		require.NoError(t, err)

		// Document: empty strings are persisted
		require.Len(t, loreValues, 1, "Empty string is saved to database")
		assert.Equal(t, "", loreValues[0][0], "Value is empty string, not nil/missing")

		t.Log("Note: There is no omitempty equivalent for datalog struct tags.")
		t.Log("Empty strings ARE persisted. Consider using *string for truly optional fields.")
	})
}
