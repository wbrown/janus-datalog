//go:build !(js && wasm)

package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func TestAsOfWildcardPullUsesSnapshot(t *testing.T) {
	dir, err := os.MkdirTemp("", "asof-wildcard-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":person/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, name, "Alice"))
	tx1, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(alice, name, "Bob"))
	_, err = tx.Commit()
	require.NoError(t, err)

	latest, err := db.Pull(alice, `[*]`)
	require.NoError(t, err)
	require.Equal(t, "Bob", latest["person/name"])

	asOf := db.AsOf(tx1)
	explicit, err := asOf.Pull(alice, `[:person/name]`)
	require.NoError(t, err)
	require.Equal(t, "Alice", explicit["person/name"])

	wildcard, err := asOf.Pull(alice, `[*]`)
	require.NoError(t, err)
	require.Equal(t, "Alice", wildcard["person/name"])
}

func TestAsOfWildcardPullUsesSnapshotForCardinalityMany(t *testing.T) {
	dir, err := os.MkdirTemp("", "asof-wildcard-many-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s, err := schema.NewBuilder().
		Attribute(":person/tags").Type(schema.TypeString).Many().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(dir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	tags := datalog.NewKeyword(":person/tags")

	tx := db.NewTransaction()
	require.NoError(t, tx.Add(alice, tags, "red"))
	tx1, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Add(alice, tags, "blue"))
	_, err = tx.Commit()
	require.NoError(t, err)

	latest, err := db.Pull(alice, `[*]`)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"red", "blue"}, latest["person/tags"])

	wildcard, err := db.AsOf(tx1).Pull(alice, `[*]`)
	require.NoError(t, err)
	require.ElementsMatch(t, []interface{}{"red"}, wildcard["person/tags"])
}
