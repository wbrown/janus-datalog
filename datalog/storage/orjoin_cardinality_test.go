package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestOrJoinContainerItemCardinality verifies that the or-join test's
// data model requires cardinality-many for :container/item.
// Without schema, LWW keeps only the last write, losing item1.
func TestOrJoinContainerItemCardinality(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cardinality-check-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	containerA := datalog.NewIdentity("container:A")
	item1 := datalog.NewIdentity("item:1")
	item2 := datalog.NewIdentity("item:2")
	kw := datalog.NewKeyword

	t.Run("without_schema_lww_loses_item1", func(t *testing.T) {
		for _, mode := range optimizerModes {
			t.Run(mode.name, func(t *testing.T) {
				db := createOptimizerModeDB(t, mode, DatabaseOptions{})

				tx := db.NewTransaction()
				tx.Add(containerA, kw(":container/item"), item1)
				tx.Add(containerA, kw(":container/item"), item2)
				_, err := tx.Commit()
				require.NoError(t, err)

				results, err := executor.CollectTuples(db.Query(
					`[:find ?item :where [?c :container/item ?item]]`))
				require.NoError(t, err)
				t.Logf("Without schema: %v", results)
				// LWW: only the last write survives
				assert.Len(t, results, 1, "without schema, LWW keeps only last write")
			})
		}
	})

	t.Run("with_schema_both_items_kept", func(t *testing.T) {
		s := schema.NewBuilder().
			Attribute(":container/item").Type(schema.TypeRef).Many().Add().
			MustBuild()

		for _, mode := range optimizerModes {
			t.Run(mode.name, func(t *testing.T) {
				popts := mode.plannerOptions()
				db, err := NewDatabaseWithOptions(DatabaseOptions{
					Path:           t.TempDir(),
					Schema:         s,
					PlannerOptions: &popts,
				})
				require.NoError(t, err)
				defer db.Close()

				tx := db.NewTransaction()
				tx.Add(containerA, kw(":container/item"), item1)
				tx.Add(containerA, kw(":container/item"), item2)
				_, err = tx.Commit()
				require.NoError(t, err)

				results, err := executor.CollectTuples(db.Query(
					`[:find ?item :where [?c :container/item ?item]]`))
				require.NoError(t, err)
				t.Logf("With schema: %v", results)
				assert.Len(t, results, 2, "with schema, cardinality-many keeps both")
			})
		}
	})
}
