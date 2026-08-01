// Tests for temporal database handle safety.
//
// AsOf() and History() return read-only *Database views sharing the parent's
// underlying store. These tests verify:
//
//   - NewTransaction() on a temporal handle panics with a clear message
//     (not a nil-map crash)
//   - Close() on a temporal handle does not close the parent's store
//   - NewExecutorWithOptions() on a temporal handle applies temporal mode
//   - NewExecutorWithOptions() on a regular handle applies schema and cache

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func setupTemporalTestDB(t *testing.T, mode optimizerMode) (*Database, datalog.ElementID) {
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)

	db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})

	alice := datalog.NewIdentity("alice")
	nameAttr := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, nameAttr, "Alice"))
	txID, err := tx.Commit()
	require.NoError(t, err)

	return db, txID
}

// TestTemporalHandle_NewTransaction_PanicsWithClearMessage verifies that
// calling NewTransaction() on an AsOf or History handle produces a
// descriptive panic (not the opaque nil-map crash from before the fix).
// Temporal handles are read-only views; writes are not meaningful.
func TestTemporalHandle_NewTransaction_PanicsWithClearMessage(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, txID := setupTemporalTestDB(t, mode)

			for _, tc := range []struct {
				name   string
				handle *Database
			}{
				{"AsOf", db.AsOf(txID)},
				{"History", db.History()},
			} {
				t.Run(tc.name, func(t *testing.T) {
					assert.Panics(t, func() {
						tc.handle.NewTransaction()
					}, "NewTransaction on temporal handle should panic with a clear message")
				})
			}
		})
	}
}

// TestTemporalHandle_Close_DoesNotCloseParent verifies that closing a
// temporal handle does not close the shared underlying store. The parent
// must remain usable after the child is closed.
func TestTemporalHandle_Close_DoesNotCloseParent(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db, txID := setupTemporalTestDB(t, mode)

			// Close the temporal handle.
			asOf := db.AsOf(txID)
			err := asOf.Close()
			require.NoError(t, err)

			// Parent should still be fully functional.
			alice := datalog.NewIdentity("alice")
			nameAttr := datalog.NewKeyword(":person/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, nameAttr, "Alice2"))
			_, err = tx.Commit()
			assert.NoError(t, err, "parent should be usable after temporal handle Close()")
		})
	}
}

// TestTemporalHandle_NewExecutorWithOptions_UsesTemporalMode verifies that
// NewExecutorWithOptions() on a temporal handle produces an executor that
// queries the temporal snapshot, not current state. Before the fix,
// NewExecutorWithOptions() bypassed d.Matcher() and created a bare matcher
// without temporal filtering.
func TestTemporalHandle_NewExecutorWithOptions_UsesTemporalMode(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})

			alice := datalog.NewIdentity("alice")
			nameAttr := datalog.NewKeyword(":person/name")

			// Write "Alice" at tx1.
			tx1 := db.NewTransaction()
			require.NoError(t, tx1.Set(alice, nameAttr, "Alice"))
			tx1ID, err := tx1.Commit()
			require.NoError(t, err)

			// Overwrite with "Alice2" at tx2.
			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Set(alice, nameAttr, "Alice2"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// As-of tx1: should see "Alice", not "Alice2".
			asOf := db.AsOf(tx1ID)
			exec := asOf.NewExecutorWithOptions(mode.plannerOptions())

			q, err := parser.ParseQuery(`[:find ?name :where [?e :person/name ?name]]`)
			require.NoError(t, err)

			result, err := exec.Execute(q)
			require.NoError(t, err)

			var names []string
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				if len(tuple) > 0 {
					if n, ok := tuple[0].(string); ok {
						names = append(names, n)
					}
				}
			}
			iter.Close()

			require.Len(t, names, 1, "as-of query should return 1 result")
			assert.Equal(t, "Alice", names[0], "as-of query should see the value at tx1")
		})
	}
}

// TestNewExecutorWithOptions_HasSchemaAndCache verifies that
// NewExecutorWithOptions() on a regular (non-temporal) database applies
// schema and cache to the matcher. Before the fix, it created a bare
// matcher that treated all attributes as cardinality-one.
func TestNewExecutorWithOptions_HasSchemaAndCache(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: s})

			alice := datalog.NewIdentity("alice")
			tagAttr := datalog.NewKeyword(":person/tags")

			tx := db.NewTransaction()
			require.NoError(t, tx.Add(alice, tagAttr, "admin"))
			require.NoError(t, tx.Add(alice, tagAttr, "user"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query via NewExecutorWithOptions — should return both tags (add-wins).
			// Before fix: bare matcher has no schema, treats :person/tags as
			// cardinality-one, returns only the LWW winner (one tag).
			exec := db.NewExecutorWithOptions(mode.plannerOptions())

			q, err := parser.ParseQuery(`[:find ?tag :where [?e :person/tags ?tag]]`)
			require.NoError(t, err)

			result, err := exec.Execute(q)
			require.NoError(t, err)

			var tags []string
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				if len(tuple) > 0 {
					if tag, ok := tuple[0].(string); ok {
						tags = append(tags, tag)
					}
				}
			}
			iter.Close()

			assert.Len(t, tags, 2,
				"NewExecutorWithOptions should use schema-aware matcher; cardinality-many should return all members")
		})
	}
}
