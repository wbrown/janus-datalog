// Test that PullInto honors the walk-based (A, V)-LWW entity-view
// fallback for unique attributes.
//
// Commit 5 of the CRDT-unique redesign. PullInto uses the same
// PullExecutor → EntityResolver → matcher path as db.Pull; the walk
// rule applied at ResolveLWW in Commit 3 flows through automatically.
// This test locks in the contract so the symmetric behavior cannot
// silently regress when Pull internals change.

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

type pullUser struct {
	ID    datalog.Identity `datalog:"-,id"`
	Name  string           `datalog:"user/name"`
	Email string           `datalog:"user/email"`
}

// TestPullInto_UniqueFallback: alice's email is superseded by bob. Her
// PullInto result should reflect the fallback value (v1), not the
// superseded latest (v2) and not nil.
func TestPullInto_UniqueFallback(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	email := datalog.NewKeyword(":user/email")
	name := datalog.NewKeyword(":user/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, name, "Alice"))
	require.NoError(t, tx.Set(alice, email, "v1@example.com"))
	_, err := tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(alice, email, "v2@example.com"))
	_, err = tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(bob, email, "v2@example.com"))
	_, err = tx.Commit()
	require.NoError(t, err)

	var u pullUser
	require.NoError(t, db.PullInto(alice, &u))

	assert.Equal(t, "Alice", u.Name)
	assert.Equal(t, "v1@example.com", u.Email,
		"PullInto should produce the fallback value for alice's superseded email")
}
