// Regression + investigation tests covering audit items from the
// CRDT-unique redesign audit (see the audit report in the branch
// discussion). Each test locks in or investigates one concern:
//
//   1. Wildcard pull returns fallback value (not superseded latest).
//   2. d.History() handles: PullInto / Pull should not apply the walk.
//   3. Value-encoding edge cases: []byte, cross-type equality.
//
// Some of these tests are regression guards (expected to pass today);
// others are investigations that may reveal pre-existing or new bugs.

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// ================================================================
// 1. Wildcard pull with superseded unique attribute
// ================================================================

// TestWildcardPull_UniqueFallback: db.Pull(e, "[*]") on an entity with a
// superseded unique attribute returns the walk-derived fallback value.
// Locks in the regression guard — Pull wildcard uses ResolveAllAttributes
// which uses ResolveLWW which applies the walk for unique attrs.
func TestWildcardPull_UniqueFallback(t *testing.T) {
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

	result, err := db.Pull(alice, `[*]`)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, "Alice", result["user/name"])
	assert.Equal(t, "v1@example.com", result["user/email"],
		"wildcard pull should return alice's fallback email, not superseded latest")
}

// ================================================================
// 2. History-mode interaction with Pull
// ================================================================

// TestHistoryHandle_Pull_DoesNotApplyWalk: d.History().Pull() should
// not apply the walk-based fallback. History mode semantics = return
// raw-datom-style view, bypass CRDT resolution.
//
// This test explores whether the current implementation correctly
// bypasses the walk on history handles, or whether it inappropriately
// applies walk-based resolution through the ResolveLWW cache path.
//
// Expected behavior: not fully specified. Reasonable interpretations:
//   (a) Return alice's latest raw Set (ignoring supersession by bob)
//   (b) Return nil/fail (history-mode semantics don't define a single
//       current value)
//
// Current behavior: likely applies the walk via ResolveLWW, returning
// the fallback value. That's arguably incorrect — history mode should
// bypass resolution.
//
// If this test fails, it surfaces a pre-existing ambiguity (not a
// regression from the unique redesign per se — the old ResolveLWW
// would also return something weird in history mode). Documented as
// an audit finding; fix or defer based on reviewer preference.
func TestHistoryHandle_Pull_DoesNotApplyWalk(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	email := datalog.NewKeyword(":user/email")

	tx := db.NewTransaction()
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

	// History handle.
	hist := db.History()
	result, err := hist.Pull(alice, `[:user/email]`)
	require.NoError(t, err)

	// Under history semantics, we should NOT see the walk-derived
	// fallback v1. We should see alice's latest raw Set: v2.
	// (Her assertion of v2 is real even though it's superseded by bob
	// under current-state resolution — history mode shows the raw
	// assertion.)
	if result == nil {
		t.Fatal("history handle Pull returned nil result — at minimum, alice has a Set(email, v2) assertion visible in history")
	}
	got, hasEmail := result["user/email"]
	require.True(t, hasEmail, "history handle Pull should return alice's email assertion; got %v", result)
	assert.Equal(t, "v2@example.com", got,
		"history-handle Pull should return raw latest Set (v2), not walk-derived fallback (v1)")
}

// TestHistoryHandle_WildcardPull: wildcard pull on a history handle
// goes through ResolveAllAttributes → ResolveEntityAttributes →
// ResolveLWW. The walk-based ResolveLWW does NOT check history mode,
// so alice's superseded email would be resolved via fallback to v1
// instead of returning a raw assertion.
//
// This exposes the ResolveLWW history-mode inconsistency flagged in
// the audit. If the test surfaces unexpected walk behavior, we need
// to add a history-mode guard to ResolveLWW.
func TestHistoryHandle_WildcardPull(t *testing.T) {
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

	hist := db.History()
	result, err := hist.Pull(alice, `[*]`)
	require.NoError(t, err)
	require.NotNil(t, result)

	// History semantics: alice's latest Set assertion is v2. That should
	// be what wildcard pull returns, not the walk-derived fallback v1.
	got, hasEmail := result["user/email"]
	require.True(t, hasEmail)
	assert.Equal(t, "v2@example.com", got,
		"history-handle wildcard pull should return raw latest Set (v2), "+
			"not walk-derived fallback (v1). This requires ResolveLWW to "+
			"respect history-mode semantics.")
}

// ================================================================
// 3. Value-encoding edge cases
// ================================================================

// TestValueEncoding_Int64VsString_DoesNotCrossCollide: ensure int64(5)
// is not conflated with "5" during walk resolution. Under the
// stringification-collision pattern (notOrTupleKey), this would be a
// collision. The walk uses encodeValueForSearch which preserves type
// tags, so the two should be distinct.
func TestValueEncoding_Int64VsString_DoesNotCrossCollide(t *testing.T) {
	dir := t.TempDir()
	// A unique attr accepting any type via TypeRef (bypass type check)?
	// Simpler: use two separate attrs, one string-unique and one int-unique,
	// but we want to test cross-type collision within the same (A, V)
	// comparison. Since the walk uses the encoded-value key, which includes
	// a type tag, int64(5) and string("5") are distinct under the same V
	// only if the attr accepts both. Use no type constraint.
	s, err := schema.NewBuilder().
		Attribute(":thing/id").Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(dir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	id := datalog.NewKeyword(":thing/id")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, id, int64(5)))
	_, err = tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Set(bob, id, "5"))
	_, err = tx.Commit()
	require.NoError(t, err)

	// alice owns int64(5); bob owns "5". They should NOT be conflated.
	owner, err := db.LookupByUnique(id, int64(5))
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(alice), "int64(5) should be owned by alice, not bob (string '5')")

	owner, err = db.LookupByUnique(id, "5")
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(bob), "string '5' should be owned by bob, not alice (int64 5)")
}

// TestValueEncoding_BytesComparison: []byte values should use content
// equality in the walk. Two different entities claiming the same
// []byte content should follow normal (A, V)-LWW rules.
func TestValueEncoding_BytesComparison(t *testing.T) {
	dir := t.TempDir()
	s, err := schema.NewBuilder().
		Attribute(":thing/hash").Type(schema.TypeBytes).Unique(schema.UniqueValue).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(dir, s)
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	hash := datalog.NewKeyword(":thing/hash")

	content := []byte{0x01, 0x02, 0x03, 0x04}
	differentContent := []byte{0x01, 0x02, 0x03, 0x05}

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, hash, content))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Alice currently owns [1,2,3,4]. Lookup should find her.
	ownerContent := []byte{0x01, 0x02, 0x03, 0x04} // distinct slice, same content
	owner, err := db.LookupByUnique(hash, ownerContent)
	require.NoError(t, err)
	require.NotNil(t, owner, "content-equal []byte should find alice")
	assert.True(t, owner.Equal(alice))

	// Different content: no owner.
	owner, err = db.LookupByUnique(hash, differentContent)
	require.NoError(t, err)
	assert.Nil(t, owner, "different []byte content should not collide")

	// Bob takes the value with a higher Tx.
	tx = db.NewTransaction()
	require.NoError(t, tx.Set(bob, hash, content))
	_, err = tx.Commit()
	require.NoError(t, err)

	// Now bob owns via (A, V)-LWW.
	owner, err = db.LookupByUnique(hash, content)
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(bob), "bob (higher Tx) should win over alice on []byte value")
}
