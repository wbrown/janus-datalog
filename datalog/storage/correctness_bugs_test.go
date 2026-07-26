// Regression tests for five correctness bugs flagged in
// docs/bugs/EXTERNAL_REVIEW_2026_04.md items 1-5.
//
// Each test is written against the not-yet-fixed code and must fail
// (for the expected reason) before the corresponding implementation
// change lands. Together they form the red baseline for the
// correctness-cluster fix pass.

package storage

import (
	"bytes"
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
		t.Run(indexName(idx), func(t *testing.T) {
			key := encoder.EncodeKey(idx, &datom)
			got := extractElementIDFromKey(idx, key)
			assert.Equal(t, expectedTx, got,
				"extractElementIDFromKey must decode the Tx correctly for %s keys; got %+v", indexName(idx), got)
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
		t.Run(indexName(idx), func(t *testing.T) {
			key := encoder.EncodeKey(idx, &datom)
			got := extractElementIDFromKey(idx, key)
			assert.Equal(t, expectedTx, got,
				"extractElementIDFromKey must decode Tx correctly even when AfterRef is present (%s)", indexName(idx))
			assert.NotEqual(t, afterRef, got,
				"must not return AfterRef as Tx")
		})
	}
}

// ================================================================
// Item 4: simpleBatchScanner.bindingPrefix uses pre-expansion enum
// ================================================================

// TestSimpleBatchScanner_BindingPrefix_AETV verifies that bindingPrefix
// produces a valid AETV prefix when invoked with AETV as s.index.
// The original code path routed through "case 3" which was
// labeled (incorrectly) as VAET — so the AETV case built a
// VAET-shaped key that cannot hit real data.
//
// Pre-fix: test fails because the prefix is VAET-shaped
// (or wrong bytes) when s.index == AETV.
func TestSimpleBatchScanner_BindingPrefix_AETV(t *testing.T) {
	dir := t.TempDir()
	s, err := schema.NewBuilder().
		Attribute(":user/name").Type(schema.TypeString).One().Add().
		Build()
	require.NoError(t, err)

	db, err := NewDatabaseWithSchema(dir, s)
	require.NoError(t, err)
	defer db.Close()

	matcher := db.Matcher().(*PatternMatcher)
	scanner := &simpleBatchScanner{
		matcher:  matcher,
		index:    AETV,
		position: 0, // E bound, A constant
	}

	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":user/name")
	var aBytes Attribute
	copy(aBytes[:], name.String())

	prefix, ok := scanner.bindingPrefix(alice, name, nil)
	require.True(t, ok, "bindingPrefix must name a run for AETV with E-bound + A-constant")
	got, _ := encodeScanBoundForTest(t, matcher, ScanBound{Index: AETV, Prefix: prefix})

	// The expected prefix for AETV with E+A bound is [prefix][A][E].
	encoder := matcher.store.Encoder()
	expected := encoder.EncodePrefix(AETV, aBytes[:], alice.Bytes())
	assert.True(t, bytes.Equal(got, expected),
		"bindingPrefix on AETV must produce [prefix][A][E]; got %x, expected %x", got, expected)
}

// TestSimpleBatchScanner_BindingPrefix_AllIndices verifies bindingPrefix
// handles every index it's likely to see. Indices EATV and AVET were
// not in the original switch at all — it reported no run for them,
// which silently degrades callers.
func TestSimpleBatchScanner_BindingPrefix_AllIndices(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	matcher := db.Matcher().(*PatternMatcher)
	alice := datalog.NewIdentity("alice")
	name := datalog.NewKeyword(":user/name")

	// Batch scanning's Position field names the binding component
	// (0=E, 1=A, 2=V). bindingPrefix must handle every (index, position)
	// combination its signature admits — a silent "no run" causes
	// silent zero-result scans, the exact failure mode the external
	// review flagged for this function.
	// Tx constant used by the ATEV case below; the value chosen doesn't
	// matter so long as constT is present.
	constT := datalog.ElementID{Lamport: 1, ReplicaID: 1}
	for _, tc := range []struct {
		name     string
		index    IndexType
		position int
		value    interface{}
		constA   datalog.Keyword
		constT   *datalog.ElementID
	}{
		{"EAVT_Ebound", EAVT, 0, alice, name, nil},
		{"EATV_Ebound", EATV, 0, alice, name, nil},
		{"AEVT_Ebound", AEVT, 0, alice, name, nil},
		{"AETV_Ebound", AETV, 0, alice, name, nil},
		{"ATEV_Ebound", ATEV, 0, alice, name, &constT},
		{"AVET_Abound", AVET, 1, name, nil, nil},
		{"VAET_Vbound", VAET, 0, "some-value", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scanner := &simpleBatchScanner{
				matcher:  matcher,
				index:    tc.index,
				position: tc.position,
			}
			prefix, ok := scanner.bindingPrefix(tc.value, tc.constA, tc.constT)
			require.True(t, ok,
				"bindingPrefix reported no run for index %s, position %d — scanner would silently produce no results",
				indexName(tc.index), tc.position)
			got, _ := encodeScanBoundForTest(t, matcher, ScanBound{Index: tc.index, Prefix: prefix})
			// Weaker check: at minimum, the first byte must be the correct index prefix.
			require.Greater(t, len(got), 0)
			assert.Equal(t, byte(tc.index), got[0],
				"first byte of the encoded bound must be the index prefix %d for %s",
				byte(tc.index), indexName(tc.index))
		})
	}
}

// TestSimpleBatchScanner_BindingPrefix_AVET_PositionSemantics isolates AVET
// position handling to distinguish "type assertion failed" from "branch
// not entered" when the broader BindingPrefix_AllIndices test flags AVET.
// Asserts the concrete prefix bytes against the encoder's direct output.
func TestSimpleBatchScanner_BindingPrefix_AVET_PositionSemantics(t *testing.T) {
	dir := t.TempDir()
	db, err := NewDatabase(dir)
	require.NoError(t, err)
	defer db.Close()

	matcher := db.Matcher().(*PatternMatcher)
	emailKw := datalog.NewKeyword(":user/email")
	var aBytes Attribute
	copy(aBytes[:], emailKw.String())

	// Case 1: A varies across bindings (position=1, value is a Keyword).
	// Scan range: [A] prefix over AVET.
	scanner := &simpleBatchScanner{matcher: matcher, index: AVET, position: 1}
	prefix, ok := scanner.bindingPrefix(emailKw, nil, nil)
	require.True(t, ok,
		"AVET position=1 with Keyword value: bindingPrefix reported no run. "+
			"Type of value in test: %T", emailKw)
	got, _ := encodeScanBoundForTest(t, matcher, ScanBound{Index: AVET, Prefix: prefix})

	expected := matcher.store.Encoder().EncodePrefix(AVET, aBytes[:])
	assert.True(t, bytes.Equal(got, expected),
		"AVET position=1: expected [A] prefix %x, got %x", expected, got)

	// Case 2: E varies across bindings (position=0, value is an Identity,
	// constA carries the attribute). Scan range: [A][V?][E] — but with
	// only E from bindings, the realistic prefix is [A][... V comes
	// from elsewhere]. bindingPrefix cannot produce a narrower-than-[A]
	// prefix here because V isn't known; it should still name a useful
	// run rather than reporting none.
	alice := datalog.NewIdentity("alice")
	scanner = &simpleBatchScanner{matcher: matcher, index: AVET, position: 0}
	_, ok = scanner.bindingPrefix(alice, emailKw, nil)
	require.True(t, ok,
		"AVET position=0 with Identity value: bindingPrefix reported no run")
}

// ================================================================
// Item 5: MaterializeResult false-positive panic on equal tuples
// ================================================================

// This test lives in the executor package — see
// executor/materialize_result_test.go.
