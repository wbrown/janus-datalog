// Regression tests for the correctness bugs flagged in
// docs/bugs/EXTERNAL_REVIEW_2026_04.md items 1, 2, 3 and 5. Item 4 concerns a
// component the engine no longer has, so nothing here covers it.
//
// Each test is written against the not-yet-fixed code and must fail
// (for the expected reason) before the corresponding implementation
// change lands. Together they form the red baseline for the
// correctness-cluster fix pass.

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// ================================================================
// Item 1: ResolveLWWFromDatoms ignores Op (tombstone gap)
// ================================================================

// TestResolveLWWFromDatoms_TombstoneReturnsNil verifies that when the
// highest-Tx datom for an (E, A) is a Remove, ResolveLWWFromDatoms
// returns nil (attribute does not exist), not the tombstoned V.
//
// Pre-fix: returns d.V unconditionally at the highest Tx.
func TestResolveLWWFromDatoms_TombstoneReturnsNil(t *testing.T) {
	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":user/name")

	datoms := []datalog.Datom{
		{
			E: alice, A: name, V: "Alice",
			Tx: datalog.ElementID{Lamport: 1},
			Op: datalog.OpNone,
		},
		{
			E: alice, A: name, V: "Alice",
			Tx: datalog.ElementID{Lamport: 2},
			Op: datalog.OpCRDTRemove, // tombstone at higher Tx
		},
	}

	v, maxID := ResolveLWWFromDatoms(datoms)

	assert.Nil(t, v,
		"highest-Tx datom is a Remove tombstone — ResolveLWWFromDatoms must return nil V")
	assert.Equal(t, uint64(2), maxID.Lamport,
		"maxID should still reflect the highest-Tx entry regardless of Op")
}

// ================================================================
// Item 2: LookupAttribute fallback ignores Op (tombstone gap)
// ================================================================

// TestLookupAttribute_StorageFallback_Tombstone verifies the
// cache-bypass storage fallback in matcher.LookupAttribute honors
// OpCRDTRemove. Exercised by opening a database with cache disabled
// and issuing a LookupAttribute after a Remove.
//
// Pre-fix: returns the tombstoned datom's V via line 867.
func TestLookupAttribute_StorageFallback_Tombstone(t *testing.T) {
	dir := t.TempDir()
	s, err := schema.NewBuilder().
		Attribute(":user/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:         dir,
		Schema:       s,
		DisableCache: true, // force the storage-fallback path
	})
	require.NoError(t, err)
	defer db.Close()

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":user/name")

	tx := db.NewTransaction()
	require.NoError(t, tx.Set(alice, name, "Alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	tx = db.NewTransaction()
	require.NoError(t, tx.Remove(alice, name, "Alice"))
	_, err = tx.Commit()
	require.NoError(t, err)

	matcher := db.Matcher().(*PatternMatcher)
	v, ok := requireAttributeLookup(t, matcher, alice, name)

	assert.False(t, ok, "tombstoned attribute must not report as present")
	assert.Nil(t, v, "tombstoned attribute must not return its retracted V")
}

// ================================================================
// Item 3: extractElementIDFromKey misreads Tx for indices where Op
// is at the tail.
// ================================================================

// TestExtractElementIDFromKey_TailIndices verifies that
// extractElementIDFromKey returns the correct Tx for EAVT, AEVT,
// AVET, VAET — the four indices where Tx sits before the Op byte
// (and before an optional AfterRef when Op.HasAfterRef()).
//
// Pre-fix: reads key[len-16:], which is Tx bytes shifted by one (Op
// byte at tail) or 16 bytes of AfterRef. Result: garbled ElementID.
func TestExtractElementIDFromKey_TailIndices(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	expectedTx := datalog.ElementID{Lamport: 0x1234567890ABCDEF, ReplicaID: 0xFEDCBA9876543210}
	datom := datalog.Datom{
		E:  datalog.NewIdentity("alice"),
		A:  datalog.NewKeyword(":user/name"),
		V:  "Alice",
		Tx: expectedTx,
		Op: datalog.OpNone,
	}

	for _, idx := range []IndexType{EAVT, AEVT, AVET, VAET} {
		t.Run(idx.String(), func(t *testing.T) {
			key := encoder.EncodeKey(idx, &datom)
			got := extractElementIDFromKey(idx, key)
			assert.Equal(t, expectedTx, got,
				"extractElementIDFromKey must decode the Tx correctly for %s keys; got %+v", idx.String(), got)
		})
	}
}

// TestExtractElementIDFromKey_TailIndicesWithAfterRef covers the
// RGA case: keys with OpRGAInsert have an AfterRef appended before
// the Op byte, further shifting the Tx position.
//
// Pre-fix: returns AfterRef bytes (or garbage) instead of Tx.
func TestExtractElementIDFromKey_TailIndicesWithAfterRef(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	expectedTx := datalog.ElementID{Lamport: 100, ReplicaID: 200}
	afterRef := datalog.ElementID{Lamport: 50, ReplicaID: 75}

	datom := datalog.Datom{
		E:        datalog.NewIdentity("alice"),
		A:        datalog.NewKeyword(":user/tags"),
		V:        "admin",
		Tx:       expectedTx,
		Op:       datalog.OpRGAInsert,
		AfterRef: afterRef,
	}

	// Tail-Tx indices: EAVT/AEVT/AVET/VAET. Verify each decodes
	// expectedTx (not afterRef, not garbage).
	for _, idx := range []IndexType{EAVT, AEVT, AVET, VAET} {
		t.Run(idx.String(), func(t *testing.T) {
			key := encoder.EncodeKey(idx, &datom)
			got := extractElementIDFromKey(idx, key)
			assert.Equal(t, expectedTx, got,
				"extractElementIDFromKey must decode Tx correctly even when AfterRef is present (%s)", idx.String())
			assert.NotEqual(t, afterRef, got,
				"must not return AfterRef as Tx")
		})
	}
}

// ================================================================
// Item 5: MaterializeResult false-positive panic on equal tuples
// ================================================================

// This test lives in the executor package — see
// executor/materialize_result_test.go.
