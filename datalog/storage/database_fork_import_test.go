package storage

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestImportIntoABranchHoldsEverythingImported: a branch holds every datom
// imported into it, as any database does, whatever ElementIDs the dump
// carries. The dump comes from a database of its own whose clock ran over the
// same Lamports the parent wrote past the snapshot.
func TestImportIntoABranchHoldsEverythingImported(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			source := createOptimizerModeDB(t, mode, DatabaseOptions{})
			var imported []string
			for i := 0; i < 16; i++ {
				name := fmt.Sprintf("imported-%02d", i)
				snapTestAddName(t, source, name, name)
				imported = append(imported, name)
			}
			var dump bytes.Buffer
			require.NoError(t, source.Export(&dump))

			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			snapTestAddName(t, parent, "alice", "Alice")
			_, err := parent.Snapshot("s")
			require.NoError(t, err)
			for i := 0; i < 32; i++ {
				snapTestAddName(t, parent, "bob", fmt.Sprintf("Bob %d", i))
			}
			branch := branchOf(t, parent, "s")
			require.NoError(t, branch.Import(&dump))

			want := append([]string{"Alice"}, imported...)
			sort.Strings(want)
			require.Equal(t, want, snapTestNames(t, branch))
		})
	}
}

// TestBranchResolvesAsItsParent: a branch resolves every attribute by its
// parent's schema, so it reads what AsOfSnapshot reads, including when the
// parent has no schema. This parent opened on an empty store with none and then
// imported a set written under a schema that makes it multi-valued.
func TestBranchResolvesAsItsParent(t *testing.T) {
	forking, _ := forkingModes(t)
	require.NotEmpty(t, forking)
	tagged, err := schema.NewBuilder().
		Attribute(":item/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)
	item := datalog.NewIdentity("item")
	tags := datalog.NewKeyword(":item/tags")
	tagsOf := func(t *testing.T, d *Database) []string {
		t.Helper()
		var found []struct {
			Tag string `datalog:"?tag"`
		}
		require.NoError(t, d.QueryInto(&found, `[:find ?tag :where [_ :item/tags ?tag]]`))
		out := make([]string, 0, len(found))
		for _, f := range found {
			out = append(out, f.Tag)
		}
		sort.Strings(out)
		return out
	}
	for _, mode := range forking {
		t.Run(mode.name, func(t *testing.T) {
			source := createOptimizerModeDB(t, mode, DatabaseOptions{Schema: tagged})
			tx := source.NewTransaction()
			require.NoError(t, tx.Add(item, tags, "x"))
			require.NoError(t, tx.Add(item, tags, "y"))
			_, err := tx.Commit()
			require.NoError(t, err)
			var dump bytes.Buffer
			require.NoError(t, source.Export(&dump))

			parent := createOptimizerModeDB(t, mode, DatabaseOptions{})
			require.NoError(t, parent.Import(&dump))
			_, err = parent.Snapshot("s")
			require.NoError(t, err)
			atSnapshot, err := parent.AsOfSnapshot("s")
			require.NoError(t, err)

			branch := branchOf(t, parent, "s")
			require.Equal(t, tagsOf(t, atSnapshot), tagsOf(t, branch))
		})
	}
}
