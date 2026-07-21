// Tests for (A, V)-LWW resolution of unique attributes:
//   - Database.LookupByUnique public API
//   - V-view query behavior when multiple entities claim the same unique value
//
// These tests exist against the contract for Commit 2 of the CRDT-unique
// redesign (see docs/proposals/CRDT_UNIQUE_SEMANTICS.md). Commit 1 deleted
// the write-time gate, so multi-claimant scenarios are now constructible
// via normal transactions. Commit 2 (this commit) adds the read-time
// resolution that picks the canonical owner under (A, V)-LWW.
//
// Test-first discipline: these tests are written before the implementation
// and must fail with meaningful errors (missing LookupByUnique API for the
// API tests, wrong result counts for the V-view tests) before the
// implementation is written. Once the implementation lands, all tests
// pass together.

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// uniqueTestSchema builds the schema shared by the unique-resolution tests:
// :user/email (UniqueValue) and :user/name (not unique).
func uniqueTestSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueValue).Add().
		Attribute(":user/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)
	return s
}

// setupUniqueTestDB creates a database with :user/email (UniqueValue) and
// :user/name (not unique) attributes.
func setupUniqueTestDB(t *testing.T) (*Database, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := NewDatabaseWithSchema(dir, uniqueTestSchema(t))
	require.NoError(t, err)
	return db, func() { db.Close() }
}

// openUniqueModeDB creates the same database as setupUniqueTestDB with the
// optimizer mode's planner options as the database default; query-executing
// tests construct one per mode.
func openUniqueModeDB(t *testing.T, mode optimizerMode) *Database {
	t.Helper()
	popts := mode.plannerOptions()
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           t.TempDir(),
		Schema:         uniqueTestSchema(t),
		PlannerOptions: &popts,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// setupUniqueIdentityTestDB creates a database with :user/email declared as
// UniqueIdentity. Under Position 2 this is semantically identical to
// UniqueValue for resolution; the distinction is the availability of
// LookupByUnique (both support it).
func setupUniqueIdentityTestDB(t *testing.T) (*Database, func()) {
	t.Helper()
	dir := t.TempDir()
	s, err := schema.NewBuilder().
		Attribute(":user/email").Type(schema.TypeString).Unique(schema.UniqueIdentity).Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(dir, s)
	require.NoError(t, err)
	return db, func() { db.Close() }
}

// ================================================================
// LookupByUnique: basic cases
// ================================================================

// TestLookupByUnique_NoOwner: nobody has claimed the value.
func TestLookupByUnique_NoOwner(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	email := datalog.NewKeyword(":user/email")
	owner, err := db.LookupByUnique(email, "nobody@example.com")
	require.NoError(t, err)
	assert.Nil(t, owner, "no owner for unclaimed value should return nil Identity")
}

// TestLookupByUnique_SingleOwner: one entity claims the value.
func TestLookupByUnique_SingleOwner(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	email := datalog.NewKeyword(":user/email")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, email, "alice@example.com"))
	_, err := tx.Commit()
	require.NoError(t, err)

	owner, err := db.LookupByUnique(email, "alice@example.com")
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(alice), "single claimant should be returned")
}

// TestLookupByUnique_UniqueIdentity: UniqueIdentity supports lookup the
// same way as UniqueValue (Position 2).
func TestLookupByUnique_UniqueIdentity(t *testing.T) {
	db, cleanup := setupUniqueIdentityTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	email := datalog.NewKeyword(":user/email")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, email, "alice@example.com"))
	_, err := tx.Commit()
	require.NoError(t, err)

	owner, err := db.LookupByUnique(email, "alice@example.com")
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(alice))
}

// ================================================================
// LookupByUnique: multi-claimant (A, V)-LWW
// ================================================================

// TestLookupByUnique_MultiClaimant_HighestTxWins: two entities both claim
// the same value in sequence. The second (higher Tx) wins.
func TestLookupByUnique_MultiClaimant_HighestTxWins(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	email := datalog.NewKeyword(":user/email")
	shared := "shared@example.com"

	// Alice claims first (lower Tx).
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Set(alice, email, shared))
	_, err := tx1.Commit()
	require.NoError(t, err)

	// Bob claims second (higher Tx).
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(bob, email, shared))
	_, err = tx2.Commit()
	require.NoError(t, err)

	owner, err := db.LookupByUnique(email, shared)
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(bob), "bob (higher Tx) should win (A, V)-LWW over alice; got %s", owner.String())
}

// TestLookupByUnique_MultiClaimant_ReverseOrder: bob claims first, then
// alice. Alice should win because she has the higher Tx.
func TestLookupByUnique_MultiClaimant_ReverseOrder(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	email := datalog.NewKeyword(":user/email")
	shared := "shared@example.com"

	// Bob first.
	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Set(bob, email, shared))
	_, err := tx1.Commit()
	require.NoError(t, err)

	// Alice second (higher Tx).
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, email, shared))
	_, err = tx2.Commit()
	require.NoError(t, err)

	owner, err := db.LookupByUnique(email, shared)
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(alice), "alice (higher Tx) should win (A, V)-LWW over bob; got %s", owner.String())
}

// TestLookupByUnique_CandidateMovedOn: alice once claimed V, then moved
// to a different value. She is no longer a claimant for V.
func TestLookupByUnique_CandidateMovedOn(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	email := datalog.NewKeyword(":user/email")

	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Set(alice, email, "old@example.com"))
	_, err := tx1.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, email, "new@example.com"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Old value: no current claimant (alice's current (E, A)-LWW is "new", not "old").
	owner, err := db.LookupByUnique(email, "old@example.com")
	require.NoError(t, err)
	assert.Nil(t, owner, "old value should have no current owner after alice moved on")

	// New value: alice.
	owner, err = db.LookupByUnique(email, "new@example.com")
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(alice))
}

// TestLookupByUnique_TombstonedClaimant: alice claimed V, then retracted.
// She is no longer a claimant.
func TestLookupByUnique_TombstonedClaimant(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	email := datalog.NewKeyword(":user/email")

	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Set(alice, email, "alice@example.com"))
	_, err := tx1.Commit()
	require.NoError(t, err)

	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Remove(alice, email, "alice@example.com"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	owner, err := db.LookupByUnique(email, "alice@example.com")
	require.NoError(t, err)
	assert.Nil(t, owner, "tombstoned claim should not register as current owner")
}

// TestLookupByUnique_OneMovedOneClaiming: alice moved to V2; bob claims V.
// V's owner is bob only.
func TestLookupByUnique_OneMovedOneClaiming(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	email := datalog.NewKeyword(":user/email")
	shared := "shared@example.com"

	tx1 := db.NewTransaction()
	require.NoError(t, tx1.Set(alice, email, shared))
	_, err := tx1.Commit()
	require.NoError(t, err)

	// Alice moves on.
	tx2 := db.NewTransaction()
	require.NoError(t, tx2.Set(alice, email, "alice-new@example.com"))
	_, err = tx2.Commit()
	require.NoError(t, err)

	// Bob takes the shared value.
	tx3 := db.NewTransaction()
	require.NoError(t, tx3.Set(bob, email, shared))
	_, err = tx3.Commit()
	require.NoError(t, err)

	owner, err := db.LookupByUnique(email, shared)
	require.NoError(t, err)
	require.NotNil(t, owner)
	assert.True(t, owner.Equal(bob), "only bob currently claims V; he should win regardless of Tx comparison with alice's stale claim")
}

// ================================================================
// LookupByUnique: API contract errors
// ================================================================

// TestLookupByUnique_NonUniqueAttribute: calling on a non-unique attribute
// is an error.
func TestLookupByUnique_NonUniqueAttribute(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	// :user/name exists in the schema but is not declared unique.
	name := datalog.NewKeyword(":user/name")
	_, err := db.LookupByUnique(name, "Alice")
	require.Error(t, err, "LookupByUnique on non-unique attribute should error")
	assert.Contains(t, err.Error(), "not unique")
}

// TestLookupByUnique_UnknownAttribute: attribute not in schema.
func TestLookupByUnique_UnknownAttribute(t *testing.T) {
	db, cleanup := setupUniqueTestDB(t)
	defer cleanup()

	unknown := datalog.NewKeyword(":user/unknown")
	_, err := db.LookupByUnique(unknown, "anything")
	require.Error(t, err, "LookupByUnique on unknown attribute should error")
}

// TestLookupByUnique_NoSchema: database without any schema — uniqueness
// is not declared for anything, so LookupByUnique cannot operate.
func TestLookupByUnique_NoSchema(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	email := datalog.NewKeyword(":user/email")
	_, err = db.LookupByUnique(email, "alice@example.com")
	require.Error(t, err, "LookupByUnique without schema should error")
}

// ================================================================
// V-view query returns single (A, V)-LWW winner
// ================================================================

// TestVBoundQuery_MultiClaimant_SingleWinner: a V-bound query for a unique
// attribute with multiple claimants returns exactly the (A, V)-LWW winner,
// not all claimants.
func TestVBoundQuery_MultiClaimant_SingleWinner(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")
			shared := "shared@example.com"

			tx1 := db.NewTransaction()
			require.NoError(t, tx1.Set(alice, email, shared))
			_, err := tx1.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Set(bob, email, shared))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// [?e :user/email "shared@example.com"] should return exactly one E.
			q := fmt.Sprintf(`[:find ?e :where [?e :user/email "%s"]]`, shared)
			result, err := db.Query(q)
			require.NoError(t, err)

			var entities []datalog.Identity
			iter := result.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				if len(tuple) > 0 {
					if id, ok := tuple[0].(datalog.Identity); ok {
						entities = append(entities, id)
					}
				}
			}
			iter.Close()

			require.Len(t, entities, 1, "V-bound query on unique attribute should return exactly one entity (A, V)-LWW winner; got %d", len(entities))
			assert.True(t, entities[0].Equal(bob), "winner should be bob (higher Tx); got %s", entities[0].String())
		})
	}
}

// TestVBoundQuery_NonUnique_ReturnsAll: for a non-unique cardinality-one
// attribute, the V-view still returns all entities whose current value is V.
// This is the existing behavior and must not regress.
//
// With :user/name declared cardinality-one but not unique, two entities
// can both have name="Alice" and both should appear in a V-bound query.
func TestVBoundQuery_NonUnique_ReturnsAll(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			e1 := datalog.NewIdentity("e1")
			e2 := datalog.NewIdentity("e2")
			name := datalog.NewKeyword(":user/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(e1, name, "Alice"))
			require.NoError(t, tx.Set(e2, name, "Alice"))
			_, err := tx.Commit()
			require.NoError(t, err)

			q := `[:find ?e :where [?e :user/name "Alice"]]`
			result, err := db.Query(q)
			require.NoError(t, err)

			count := 0
			iter := result.Iterator()
			for iter.Next() {
				count++
			}
			iter.Close()

			assert.Equal(t, 2, count, "non-unique attribute V-view should return all matching entities; regression check")
		})
	}
}
