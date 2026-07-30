// Tests for entity-view fallback and walk-based (A, V)-LWW symmetry.
//
// Walk-based resolution:
//
//   For entity E's current value of unique attribute A, walk E's EATV
//   history in descending Tx order. For each entry (V_i, T_i, op):
//     - If op is Remove(V_i): record V_i as retracted at T_i, advance.
//     - If op is Set(V_i) and V_i has been retracted at higher Tx: advance.
//     - If op is Set(V_i) and no OTHER entity has an assertion of V_i
//       with Tx > T_i: emit V_i.
//     - Otherwise: advance.
//
//   V-view ("who owns V?") under this model: find the max-Tx entry for V
//   across all entities; verify that entity's walk actually emits V. The
//   two views are symmetric by construction — V-view returns the entity
//   whose walk emits V.

package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// queryEntityEmail returns the current email value for e via a
// [?e :user/email ?v] pattern with ?e bound. Empty string means no value.
func queryEntityEmail(t *testing.T, db *Database, e datalog.Identity) string {
	t.Helper()
	q := fmt.Sprintf(`[:find ?v :where [%q :user/email ?v]]`, e.String())
	// Workaround: the string form may not have entity-ref syntax.
	// Use :in to bind the entity instead.
	q = `[:find ?v :in $ ?e :where [?e :user/email ?v]]`
	result, err := db.Query(q, e)
	require.NoError(t, err)

	iter := result.Iterator()
	defer iter.Close()
	if !iter.Next() {
		return ""
	}
	tuple := iter.Tuple()
	if len(tuple) == 0 {
		return ""
	}
	if s, ok := tuple[0].(string); ok {
		return s
	}
	return ""
}

// ================================================================
// Entity-view: no supersession (happy path)
// ================================================================

// TestEntityView_NoSupersession: alice's latest assertion is not contested
// by any other entity. The walk emits the latest.
func TestEntityView_NoSupersession(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			email := datalog.NewKeyword(":user/email")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "alice@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			assert.Equal(t, "alice@example.com", queryEntityEmail(t, db, alice))
		})
	}
}

// ================================================================
// Entity-view: latest superseded, fallback to older
// ================================================================

// TestEntityView_FallbackToOlder: alice claims V1 then V2; bob claims V2
// later. Alice's latest (V2) is superseded by bob; her walk falls back
// to V1 which nobody else has claimed.
func TestEntityView_FallbackToOlder(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			// Alice: first V1, then V2.
			tx1 := db.NewTransaction()
			require.NoError(t, tx1.Set(alice, email, "alice-v1@example.com"))
			_, err := tx1.Commit()
			require.NoError(t, err)

			tx2 := db.NewTransaction()
			require.NoError(t, tx2.Set(alice, email, "alice-v2@example.com"))
			_, err = tx2.Commit()
			require.NoError(t, err)

			// Bob takes V2 with a higher Tx.
			tx3 := db.NewTransaction()
			require.NoError(t, tx3.Set(bob, email, "alice-v2@example.com"))
			_, err = tx3.Commit()
			require.NoError(t, err)

			// Alice's entity-view falls back to V1.
			assert.Equal(t, "alice-v1@example.com", queryEntityEmail(t, db, alice),
				"alice's V2 is superseded; walk should fall back to V1")

			// Bob's entity-view emits V2 (he's the (A, V2)-LWW winner).
			assert.Equal(t, "alice-v2@example.com", queryEntityEmail(t, db, bob))
		})
	}
}

// TestEntityView_MultiLayerFallback: alice claims V1, V2, V3; bob takes
// V3 then V2. Alice's walk skips V3 (bob wins) and V2 (bob wins again),
// falls back to V1.
func TestEntityView_MultiLayerFallback(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			// Alice: V1, V2, V3 in order.
			for _, v := range []string{"v1@example.com", "v2@example.com", "v3@example.com"} {
				tx := db.NewTransaction()
				require.NoError(t, tx.Set(alice, email, v))
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			// Bob takes V3 (beats alice's V3), then moves to V2 (beats alice's V2).
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "v3@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Alice's walk: V3 → bob wins, V2 → bob wins, V1 → alice wins.
			assert.Equal(t, "v1@example.com", queryEntityEmail(t, db, alice))
			// Bob's walk: V2 → bob's latest, no other higher claim → emit V2.
			assert.Equal(t, "v2@example.com", queryEntityEmail(t, db, bob))
		})
	}
}

// TestEntityView_AllSuperseded_NoValue: every alice assertion has been
// beaten by another entity's later claim of the same value. Alice has
// no current email.
func TestEntityView_AllSuperseded_NoValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			// Alice: V1, V2.
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v1@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Bob takes V1 then V2 (both after alice's respective claims).
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "v1@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Alice: no value (both her assertions superseded).
			assert.Equal(t, "", queryEntityEmail(t, db, alice))
			// Bob's walk: V2 → no one higher → emit.
			assert.Equal(t, "v2@example.com", queryEntityEmail(t, db, bob))
		})
	}
}

// ================================================================
// Entity-view: retraction interacts with walk
// ================================================================

// TestEntityView_RetractCancelsSet: alice asserted V then retracted it.
// The walk must not "fall back" to the retracted value.
func TestEntityView_RetractCancelsSet(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			email := datalog.NewKeyword(":user/email")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "alice@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Remove(alice, email, "alice@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			assert.Equal(t, "", queryEntityEmail(t, db, alice),
				"retracted value must not be resurrected by fallback")
		})
	}
}

// TestEntityView_RetractThenReassert: alice set V, retracted V, set V
// again. Her walk sees Set(V) as the highest-Tx entry, not superseded.
// Emit V.
func TestEntityView_RetractThenReassert(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			email := datalog.NewKeyword(":user/email")

			for _, op := range []func(*Transaction) error{
				func(tx *Transaction) error { return tx.Set(alice, email, "alice@example.com") },
				func(tx *Transaction) error { return tx.Remove(alice, email, "alice@example.com") },
				func(tx *Transaction) error { return tx.Set(alice, email, "alice@example.com") },
			} {
				tx := db.NewTransaction()
				require.NoError(t, op(tx))
				_, err := tx.Commit()
				require.NoError(t, err)
			}

			assert.Equal(t, "alice@example.com", queryEntityEmail(t, db, alice))
		})
	}
}

// ================================================================
// Entity-view: regression guard for non-unique attributes
// ================================================================

// TestEntityView_NonUnique_NoFallback: on a cardinality-one but
// NON-unique attribute (:user/name), the existing first-entry LWW still
// applies — no walk, no fallback. Two entities with the same name both
// have that name in their entity-view.
func TestEntityView_NonUnique_NoFallback(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			name := datalog.NewKeyword(":user/name")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, name, "Smith"))
			require.NoError(t, tx.Set(bob, name, "Smith"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Query alice's name directly via pattern.
			q := `[:find ?v :in $ ?e :where [?e :user/name ?v]]`
			r, err := db.Query(q, alice)
			require.NoError(t, err)
			iter := r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "Smith", iter.Tuple()[0])
			iter.Close()

			r, err = db.Query(q, bob)
			require.NoError(t, err)
			iter = r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "Smith", iter.Tuple()[0])
			iter.Close()
		})
	}
}

// ================================================================
// Symmetry: V-view and entity-view agree
// ================================================================

// TestVViewEntityViewSymmetry_Fallback: in the fallback scenario, the
// V-view and entity-view must agree about who owns what.
func TestVViewEntityViewSymmetry_Fallback(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			// Alice: v1 then v2. Bob: v2 (beats alice's v2).
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

			// Entity view
			assert.Equal(t, "v1@example.com", queryEntityEmail(t, db, alice), "alice falls back to v1")
			assert.Equal(t, "v2@example.com", queryEntityEmail(t, db, bob), "bob owns v2")

			// V view: owner of v1 should be alice (her fallback emits v1).
			owner, err := db.LookupByUnique(email, "v1@example.com")
			require.NoError(t, err)
			require.NotNil(t, owner, "v1 should have an owner via alice's fallback")
			assert.True(t, owner.Equal(alice), "v1 owner should be alice (from walk); got %s", owner.String())

			// V view: owner of v2 should be bob.
			owner, err = db.LookupByUnique(email, "v2@example.com")
			require.NoError(t, err)
			require.NotNil(t, owner)
			assert.True(t, owner.Equal(bob))
		})
	}
}

// TestVViewEntityViewSymmetry_MovedOnWithFallback: alice claimed V, then
// moved to V', and was superseded at V'. Her fallback should return V
// (since she's the only claimant of V still standing). V-view must agree.
func TestVViewEntityViewSymmetry_MovedOnWithFallback(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			// Alice: old@example.com (T1).
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "old@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			// Alice moves to shared@example.com (T2).
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "shared@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Bob takes shared@example.com (T3 > T2).
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "shared@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Alice's walk falls back to old@example.com.
			assert.Equal(t, "old@example.com", queryEntityEmail(t, db, alice))

			// V-view of old@example.com must return alice (the walk-derived owner).
			owner, err := db.LookupByUnique(email, "old@example.com")
			require.NoError(t, err)
			require.NotNil(t, owner, "old value has an owner via alice's fallback")
			assert.True(t, owner.Equal(alice))
		})
	}
}

// ================================================================
// History mode: no fallback; all raw datoms visible
// ================================================================

// TestEntityView_HistoryMode_RawDatoms: d.History() bypasses CRDT
// resolution entirely, returning every assertion including superseded
// ones.
func TestEntityView_HistoryMode_RawDatoms(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			// Alice: v1, v2.
			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v1@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// Bob: v2 (higher Tx).
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// History mode should return all three assertions for alice's (E, A).
			hist := db.History()
			q := `[:find ?v :in $ ?e :where [?e :user/email ?v]]`
			r, err := hist.Query(q, alice)
			require.NoError(t, err)

			seen := map[string]bool{}
			iter := r.Iterator()
			for iter.Next() {
				tuple := iter.Tuple()
				if len(tuple) > 0 {
					if s, ok := tuple[0].(string); ok {
						seen[s] = true
					}
				}
			}
			iter.Close()

			assert.True(t, seen["v1@example.com"], "history should include v1")
			assert.True(t, seen["v2@example.com"], "history should include v2 (even though superseded)")
		})
	}
}

// ================================================================
// AsOf mode: walk restricted to Tx ≤ target
// ================================================================

// TestEntityView_AsOf_BeforeSupersession: as-of the tx where alice set
// v2 (but before bob's takeover), alice's entity-view is v2.
func TestEntityView_AsOf_BeforeSupersession(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

			alice := datalog.NewIdentity("alice")
			bob := datalog.NewIdentity("bob")
			email := datalog.NewKeyword(":user/email")

			tx := db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v1@example.com"))
			_, err := tx.Commit()
			require.NoError(t, err)

			tx = db.NewTransaction()
			require.NoError(t, tx.Set(alice, email, "v2@example.com"))
			tx2ID, err := tx.Commit()
			require.NoError(t, err)

			// Bob takes v2 after alice.
			tx = db.NewTransaction()
			require.NoError(t, tx.Set(bob, email, "v2@example.com"))
			_, err = tx.Commit()
			require.NoError(t, err)

			// At the point alice committed v2, she owned v2. Fallback should NOT
			// trigger because bob's claim didn't yet exist.
			asOf := db.AsOf(tx2ID)
			q := `[:find ?v :in $ ?e :where [?e :user/email ?v]]`
			r, err := asOf.Query(q, alice)
			require.NoError(t, err)
			iter := r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "v2@example.com", iter.Tuple()[0])
			iter.Close()
		})
	}
}

// TestEntityView_AsOf_AfterSupersession: as-of a Tx after bob's takeover,
// alice's entity-view falls back to v1.
func TestEntityView_AsOf_AfterSupersession(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

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
			tx3ID, err := tx.Commit()
			require.NoError(t, err)

			asOf := db.AsOf(tx3ID)
			q := `[:find ?v :in $ ?e :where [?e :user/email ?v]]`
			r, err := asOf.Query(q, alice)
			require.NoError(t, err)
			iter := r.Iterator()
			require.True(t, iter.Next())
			assert.Equal(t, "v1@example.com", iter.Tuple()[0],
				"as-of post-takeover, alice should fall back to v1")
			iter.Close()
		})
	}
}

// ================================================================
// Pull integration: fallback value is reflected in Pull results
// ================================================================

// TestEntityView_PullReflectsFallback: Pull for alice returns her
// fallback email, not her superseded latest.
func TestEntityView_PullReflectsFallback(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := openUniqueModeDB(t, mode)

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

			result, err := db.Pull(alice, `[:user/name :user/email]`)
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, "Alice", result["user/name"])
			assert.Equal(t, "v1@example.com", result["user/email"],
				"Pull should return alice's fallback email, not the superseded latest")
		})
	}
}
