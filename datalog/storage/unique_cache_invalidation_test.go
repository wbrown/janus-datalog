// Tests for cache invalidation on writes to unique attributes.
//
// Commit 4 of the CRDT-unique redesign. Per design decision D3
// (CRDT_UNIQUE_SEMANTICS.md), on any commit touching a unique attribute,
// invalidate all cached (E, A) entries for that attribute — not just the
// writer's own (E, A). Otherwise a write to (bob, email, x) can silently
// stale alice's cached (email) value.
//
// The strategy is conservative: one write invalidates every cached entry
// for the attribute. The alternative (reverse-index (A, V) → [E])
// preserves cache hits at the cost of persistent state and is left as a
// future optimization if profiling warrants it.

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestCacheInvalidation_UniqueTakeover: alice's email is cached (via an
// earlier read). Bob then claims alice's email with a higher Tx, which
// supersedes alice under (A, V)-LWW. The next read of alice's email must
// reflect the fallback semantics — alice's cached value is stale and
// must be invalidated.
func TestCacheInvalidation_UniqueTakeover(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			// Alice: v1, then v2.
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v1@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Warm the cache: reading alice's email populates her (E, A) entry.
			pre := queryEntityEmail(t, db, alice)
			require.Equal(t, "v2@example.com", pre, "before takeover, alice's email is her latest")

			// Bob takes v2 (higher Tx than alice's v2). Alice's cached value
			// is now stale under the walk rule — her entity-view must fall back
			// to v1.
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Re-read alice's email. Must reflect fallback, not the stale cache.
			post := queryEntityEmail(t, db, alice)
			assert.Equal(t, "v1@example.com", post,
				"cache must be invalidated on bob's unique-attr takeover; "+
					"alice's entity-view should fall back to v1")
		})
	}
}

// TestCacheInvalidation_UniqueAttrInvalidatesAllE: a write to (bob, email)
// invalidates (charlie, email) too, even though charlie wasn't touched
// directly — the conservative strategy invalidates the whole attribute.
//
// We verify by: caching charlie's email, then writing bob's email that
// doesn't conflict with charlie's value. Charlie's entity-view is
// unchanged semantically (nothing supersedes him), but the cache must
// have been rebuilt — verified by a second read producing a correct
// result under the walk rule.
func TestCacheInvalidation_UniqueAttrInvalidatesAllE(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			charlie := datalog.NewIdentity("charlie")
			email := datalog.NewKeyword(":user/email")

			// Alice and charlie both set their emails.
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "alice@example.com"))
			require.NoError(t, tx.Set(charlie, email, "charlie@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Warm caches for both.
			_ = queryEntityEmail(t, db, alice)
			_ = queryEntityEmail(t, db, charlie)

			// Bob takes alice's email (supersedes alice; charlie untouched semantically).
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "alice@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Alice's cache entry must be invalidated — she now has no email
			// (her single assertion has been superseded; no fallback available).
			aliceEmail := queryEntityEmail(t, db, alice)
			assert.Equal(t, "", aliceEmail, "alice's cache should be invalidated; no fallback available")

			// Charlie's entity-view should still correctly return his email
			// (whether or not his cache was invalidated, the read must be correct).
			charlieEmail := queryEntityEmail(t, db, charlie)
			assert.Equal(t, "charlie@example.com", charlieEmail)
		})
	}
}

// TestCacheInvalidation_NonUniqueAttrOnlyTouchedEntries: writes to a
// non-unique attribute do NOT trigger the attribute-wide invalidation.
// The existing per-(E, A) invalidation is still in effect for the
// writer's own keys; other entities' caches for the same attribute are
// not disturbed.
//
// Verified indirectly: after a write to alice's :user/name (non-unique),
// bob's :user/name cached value is correct for his own state. (This is
// trivially true; the test's purpose is to document that we do not
// accidentally broaden the invalidation to non-unique attributes.)
func TestCacheInvalidation_NonUniqueAttrOnlyTouchedEntries(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			name := datalog.NewKeyword(":user/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, name, "Alice1"))
			require.NoError(t, tx.Set(bob, name, "Bob"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query alice's name via pattern (warms the cache).
			q := `[:find ?v :in $ ?e :where [?e :user/name ?v]]`
			r, err := db.Query(q, alice)
			require.NoError(t, err)
			iter := r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "Alice1", iter.Tuple()[0])
			iter.Close()

			// Update only alice's name.
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(alice, name, "Alice2"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Bob's name is still correct (he wasn't touched, his cache should
			// remain valid under conservative invalidation; but even if it had
			// been invalidated, the rebuild must return the correct value).
			r, err = db.Query(q, bob)
			require.NoError(t, err)
			iter = r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "Bob", iter.Tuple()[0])
			iter.Close()

			// Alice's name reflects her update.
			r, err = db.Query(q, alice)
			require.NoError(t, err)
			iter = r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "Alice2", iter.Tuple()[0])
			iter.Close()
		})
	}
}

// TestCacheInvalidation_UniqueDoesNotAffectOtherAttrs: a unique-attr
// write invalidates only entries for that attribute, not cached entries
// for OTHER attributes on the same entities.
func TestCacheInvalidation_UniqueDoesNotAffectOtherAttrs(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")
			name := datalog.NewKeyword(":user/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, name, "Alice"))
			require.NoError(t, tx.Set(alice, email, "alice@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Warm caches for alice's email AND name.
			_ = queryEntityEmail(t, db, alice)
			qName := `[:find ?v :in $ ?e :where [?e :user/name ?v]]`
			r, err := db.Query(qName, alice)
			require.NoError(t, err)
			iter := r.Iterator()
			require.True(t, iter.Next())
			iter.Close()

			// Bob triggers a unique-attr invalidation on :user/email.
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "alice@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Alice's name should remain "Alice" — invalidation only targeted email.
			r, err = db.Query(qName, alice)
			require.NoError(t, err)
			iter = r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "Alice", iter.Tuple()[0],
				"unique-attr invalidation must not touch entries for other attributes")
			iter.Close()
		})
	}
}
