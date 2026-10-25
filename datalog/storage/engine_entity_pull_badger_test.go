//go:build !(js && wasm)

package storage

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// The wildcard pulls that reopen a database by path open the badger store,
// the persistent backend a native build has and a js/wasm build does not.

func TestPull_AWildcardOfASnapshotMarkerAnswersItsWords(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	marker := datalog.NewIdentity("db.snapshot/s1")
	for _, given := range []bool{false, true} {
		for _, withUser := range []bool{false, true} {
			for _, reopen := range []bool{false, true} {
				name := "no schema"
				if given {
					name = "given schema"
				}
				if withUser {
					name += "/a user datom"
				}
				if reopen {
					name += "/reopened"
				}
				t.Run(name, func(t *testing.T) {
					opts := DatabaseOptions{BackendName: "badger", Path: filepath.Join(t.TempDir(), "db")}
					if given {
						opts.Schema = s
					}
					d, err := NewDatabaseWithOptions(opts)
					require.NoError(t, err)
					if withUser {
						tx := d.NewTransaction()
						require.NoError(t, tx.Add(datalog.NewIdentity("e"), kw(":t/name"), "n"))
						_, err := tx.Commit()
						require.NoError(t, err)
					}
					_, err = d.Snapshot("s1")
					require.NoError(t, err)
					if reopen {
						require.NoError(t, d.Close())
						d, err = NewDatabaseWithOptions(opts)
						require.NoError(t, err)
					}
					defer d.Close()
					pulled, err := d.Pull(marker, `[*]`)
					require.NoError(t, err)
					requirePulledKeys(t, pulled, markerWords, "the marker's words resolve by the engine's declarations")
					named, err := d.Pull(marker, `[:db.snapshot/name]`)
					require.NoError(t, err)
					require.Equal(t, "s1", named["db.snapshot/name"])
				})
			}
		}
	}
}

func TestPull_AWildcardOfADeclarationAnswersItsWords(t *testing.T) {
	s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().Build()
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "db")
	declarationOf := func(t *testing.T, d *Database) datalog.Identity {
		t.Helper()
		var declarations []datalog.Identity
		require.NoError(t, d.QueryInto(&declarations, `[:find ?d :where [?a :db.attribute/declaration ?d]]`))
		require.Len(t, declarations, 1)
		return declarations[0]
	}
	d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger", Path: path, Schema: s})
	require.NoError(t, err)
	pulled, err := d.Pull(declarationOf(t, d), `[*]`)
	require.NoError(t, err)
	requirePulledKeys(t, pulled, declarationWordKeys, "in the session that declared it")
	require.NoError(t, d.Close())

	d, err = NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger", Path: path, Schema: s})
	require.NoError(t, err)
	pulled, err = d.Pull(declarationOf(t, d), `[*]`)
	require.NoError(t, err)
	requirePulledKeys(t, pulled, declarationWordKeys, "after a reopen with the same schema")
	require.NoError(t, d.Close())

	d, err = NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger", Path: path})
	require.NoError(t, err)
	defer d.Close()
	pulled, err = d.Pull(declarationOf(t, d), `[*]`)
	require.NoError(t, err)
	requirePulledKeys(t, pulled, declarationWordKeys, "after a reopen with no schema")
}

// A wildcard pull answers, of the attributes an entity carries, every one when
// the store declares no attribute beyond the engine's words, and the declared
// ones when it does. The definitions a reopen infers from the ops are not
// declarations (docs/reference/SCHEMA.md, "Open with no schema").
func TestPull_AWildcardOnAReopenedDatabaseReadsDeclarationsNotInferences(t *testing.T) {
	e := datalog.NewIdentity("e")
	requireWildcard := func(t *testing.T, d *Database, keys []string) {
		t.Helper()
		pulled, err := d.Pull(e, `[*]`)
		require.NoError(t, err)
		requirePulledKeys(t, pulled, keys, "through Pull")
		many, err := d.PullMany([]datalog.Identity{e}, `[*]`)
		require.NoError(t, err)
		require.Len(t, many, 1)
		requirePulledKeys(t, many[0], keys, "through PullMany")
	}
	write := func(t *testing.T, d *Database, attr string, value string) {
		t.Helper()
		tx := d.NewTransaction()
		require.NoError(t, tx.Add(e, kw(attr), value))
		_, err := tx.Commit()
		require.NoError(t, err)
	}
	t.Run("nothing declared", func(t *testing.T) {
		opts := DatabaseOptions{BackendName: "badger", Path: filepath.Join(t.TempDir(), "db")}
		d, err := NewDatabaseWithOptions(opts)
		require.NoError(t, err)
		write(t, d, ":t/name", "n")
		require.NoError(t, d.Close())
		d, err = NewDatabaseWithOptions(opts)
		require.NoError(t, err)
		defer d.Close()
		write(t, d, ":t/later", "z")
		requireWildcard(t, d, []string{"t/name", "t/later"})
	})
	t.Run("declared then reopened without a schema", func(t *testing.T) {
		s, err := schema.NewBuilder().Attribute(":t/name").Type(schema.TypeString).Add().Build()
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "db")
		d, err := NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger", Path: path, Schema: s})
		require.NoError(t, err)
		write(t, d, ":t/name", "n")
		write(t, d, ":t/before", "b")
		require.NoError(t, d.Close())
		d, err = NewDatabaseWithOptions(DatabaseOptions{BackendName: "badger", Path: path})
		require.NoError(t, err)
		defer d.Close()
		write(t, d, ":t/after", "z")
		requireWildcard(t, d, []string{"t/name"})
	})
}
