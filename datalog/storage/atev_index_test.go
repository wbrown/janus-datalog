package storage

import (
	"bytes"
	"crypto/sha1"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestATEVEncoderRoundTrip verifies that the ATEV index key encodes and decodes
// to the same datom components for both the binary and L85 encoders.
func TestATEVEncoderRoundTrip(t *testing.T) {
	entity := sha1.Sum([]byte("atev-entity"))
	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(entity),
		A:  datalog.NewKeyword(":atev/attribute"),
		V:  "atev value",
		Tx: datalog.ElementID{Lamport: 42, ReplicaID: 7},
	}

	encoders := []struct {
		name    string
		encoder KeyEncoder
	}{
		{"Binary", NewKeyEncoder(BinaryStrategy)},
		{"L85", NewKeyEncoder(L85Strategy)},
	}

	for _, tc := range encoders {
		t.Run(tc.name, func(t *testing.T) {
			key := tc.encoder.EncodeKey(ATEV, datom)
			if len(key) == 0 {
				t.Fatal("empty ATEV key")
			}
			if key[0] != byte(ATEV) {
				t.Errorf("first byte should be ATEV prefix, got %d", key[0])
			}

			e, _, v, tx, _, _, err := tc.encoder.DecodeKey(ATEV, key)
			if err != nil {
				t.Fatalf("decode ATEV: %v", err)
			}
			if !bytes.Equal(e[:], entity[:]) {
				t.Errorf("entity mismatch: got %x, want %x", e, entity)
			}
			if len(v) < 1 || !bytes.Equal(v[1:], []byte("atev value")) {
				t.Errorf("value mismatch: got %q", v)
			}
			var zeroTx [16]byte
			if tx == zeroTx {
				t.Error("decoded tx is zero")
			}
		})
	}
}

// TestATEVDescendingTxOrder verifies the key property that makes
// MaxElementIDForAttribute O(1): within a single attribute prefix, the
// lowest-byte (lexicographically first) ATEV key is the one with the highest Tx.
// This is what lets a single forward seek on [ATEV][A] yield the global max Tx.
func TestATEVDescendingTxOrder(t *testing.T) {
	entity := sha1.Sum([]byte("atev-order"))
	attr := datalog.NewKeyword(":atev/order")
	encoder := &BinaryKeyEncoder{}

	mkKey := func(lamport uint64) []byte {
		d := &datalog.Datom{
			E:  datalog.NewIdentityFromHash(entity),
			A:  attr,
			V:  "v",
			Tx: datalog.ElementID{Lamport: lamport, ReplicaID: 1},
		}
		return encoder.EncodeKey(ATEV, d)
	}

	low := mkKey(1)
	mid := mkKey(100)
	high := mkKey(10000)

	// Highest Tx (10000) must sort first; lowest Tx (1) must sort last.
	if bytes.Compare(high, mid) >= 0 {
		t.Errorf("high-Tx ATEV key should sort before mid-Tx: high=%x mid=%x", high, mid)
	}
	if bytes.Compare(mid, low) >= 0 {
		t.Errorf("mid-Tx ATEV key should sort before low-Tx: mid=%x low=%x", mid, low)
	}
}

// TestATEVIsPopulatedOnCommit catches regressions where the write path stops
// writing ATEV. If ATEV is not populated, MaxElementIDForAttribute returns zero
// even though the attribute has data, breaking cache freshness checks.
func TestATEVIsPopulatedOnCommit(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-populate-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	e := datalog.NewIdentity("e1")
	a := datalog.NewKeyword(":atev/populate")
	tx := datalog.ElementID{Lamport: 99, ReplicaID: 3}

	if err := store.Assert([]datalog.Datom{
		{E: e, A: a, V: "hello", Tx: tx},
	}); err != nil {
		t.Fatalf("assert: %v", err)
	}

	// Scan the ATEV prefix for this attribute; we should see exactly one key,
	// and decoding it should yield our Tx.
	var aStorage [32]byte
	copy(aStorage[:], a.String())
	atevPrefix := append([]byte{byte(ATEV)}, aStorage[:]...)
	atevEnd := incrementLastByte(atevPrefix)

	it, err := store.Scan(ATEV, atevPrefix, atevEnd)
	if err != nil {
		t.Fatalf("scan ATEV: %v", err)
	}
	defer it.Close()

	count := 0
	for it.Next() {
		count++
		got := it.ElementID()
		if got.Lamport != tx.Lamport || got.ReplicaID != tx.ReplicaID {
			t.Errorf("ATEV entry Tx mismatch: got %+v want %+v", got, tx)
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 ATEV entry for attribute, got %d", count)
	}
}

// TestMaxElementIDForAttributeUsesATEV verifies that MaxElementIDForAttribute
// returns the maximum Tx after multiple writes to the same attribute on different
// entities. The contract: highest Tx across all (E, V) for the attribute.
// Without ATEV (or with a broken ATEV path), this would either return a stale
// value or scan AEVT linearly.
func TestMaxElementIDForAttributeUsesATEV(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-maxid-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a := datalog.NewKeyword(":atev/maxid")
	var aBytes [32]byte
	copy(aBytes[:], a.String())

	// Write three entities with increasing Tx values; max should be Tx=300.
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("e-low"), A: a, V: "low", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}},
		{E: datalog.NewIdentity("e-mid"), A: a, V: "mid", Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}},
		{E: datalog.NewIdentity("e-high"), A: a, V: "high", Tx: datalog.ElementID{Lamport: 300, ReplicaID: 1}},
	}
	if err := store.Assert(datoms); err != nil {
		t.Fatalf("assert: %v", err)
	}

	maxID, err := store.MaxElementIDForAttribute(aBytes[:])
	if err != nil {
		t.Fatalf("MaxElementIDForAttribute: %v", err)
	}
	want := datalog.ElementID{Lamport: 300, ReplicaID: 1}
	if maxID.Lamport != want.Lamport || maxID.ReplicaID != want.ReplicaID {
		t.Errorf("MaxElementIDForAttribute returned %+v, want %+v", maxID, want)
	}

	// A non-existent attribute must return zero, not the previous attribute's max.
	other := datalog.NewKeyword(":atev/never-written")
	var otherBytes [32]byte
	copy(otherBytes[:], other.String())
	emptyID, err := store.MaxElementIDForAttribute(otherBytes[:])
	if err != nil {
		t.Fatalf("MaxElementIDForAttribute (empty): %v", err)
	}
	if emptyID != (datalog.ElementID{}) {
		t.Errorf("MaxElementIDForAttribute on empty attribute = %+v, want zero", emptyID)
	}
}

// TestChooseIndex_ABoundPlusTxBound_PicksATEV verifies that the matcher
// selects ATEV when both A and Tx are bound (and V is unbound). Regressions
// in chooseIndex would silently send these patterns back to AETV/TAEV.
func TestChooseIndex_ABoundPlusTxBound_PicksATEV(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-matcher-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewBadgerMatcher(store)
	a := datalog.NewKeyword(":atev/matcher")
	tx := datalog.ElementID{Lamport: 42, ReplicaID: 1}

	idx, start, end := matcher.chooseIndex(nil, a, nil, tx)
	if idx != ATEV {
		t.Errorf("chooseIndex(nil, A, nil, Tx) = %v, want ATEV", idx)
	}
	if len(start) == 0 || len(end) == 0 {
		t.Error("ATEV prefix range start/end should be non-empty")
	}
	if bytes.Compare(start, end) >= 0 {
		t.Error("ATEV prefix range start must sort before end")
	}
	if start[0] != byte(ATEV) {
		t.Errorf("ATEV prefix range start should begin with ATEV byte (%d), got %d", byte(ATEV), start[0])
	}
}

// TestChooseIndex_ABoundPlusTxBoundPlusVBound_DoesNotPickATEV pins the routing
// boundary: when V is also bound, AVET wins over ATEV (V-tightening is more
// selective than Tx-tightening on the typical workload). Locks the rule into
// the test suite so future planner edits don't silently shift the boundary.
func TestChooseIndex_ABoundPlusTxBoundPlusVBound_DoesNotPickATEV(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-no-v-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewBadgerMatcher(store)
	a := datalog.NewKeyword(":atev/routing")
	tx := datalog.ElementID{Lamport: 5, ReplicaID: 1}

	idx, _, _ := matcher.chooseIndex(nil, a, "hello", tx)
	if idx == ATEV {
		t.Errorf("chooseIndex with V bound should NOT pick ATEV; got ATEV anyway")
	}
}

// TestEndToEndABoundTxBoundQuery exercises the full pipeline:
// chooseIndex selects ATEV → scan over ATEV → result tuples. Writes datoms at
// several Tx values for the same attribute, runs a pattern with the Tx fixed
// to one specific value, and asserts only the matching datom comes back.
//
// This is the regression that catches breakage in chooseIndex, the ATEV scan
// range, the CRDT iterator's ATEV handling, and result conversion all at once
// — none of those is individually exercised end-to-end by other tests.
func TestEndToEndABoundTxBoundQuery(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	attr := datalog.NewKeyword(":atev/e2e")
	e1 := datalog.NewIdentity("e2e-one")
	e2 := datalog.NewIdentity("e2e-two")
	e3 := datalog.NewIdentity("e2e-three")
	tx1 := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	tx2 := datalog.ElementID{Lamport: 200, ReplicaID: 1}
	tx3 := datalog.ElementID{Lamport: 300, ReplicaID: 1}

	if err := store.Assert([]datalog.Datom{
		{E: e1, A: attr, V: "first", Tx: tx1},
		{E: e2, A: attr, V: "second", Tx: tx2},
		{E: e3, A: attr, V: "third", Tx: tx3},
	}); err != nil {
		t.Fatalf("assert: %v", err)
	}

	// History-mode matcher so the ATEV scan returns raw datoms without
	// CRDT resolution stripping non-latest entries.
	matcher := NewBadgerMatcher(store).History()
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Constant{Value: tx2},
		},
	}

	result, err := matcher.Match(pattern, nil)
	if err != nil {
		t.Fatalf("match: %v", err)
	}

	count := 0
	it := result.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		count++
		// Symbol order is [?e, ?v] (Tx is constant, not projected).
		if got, ok := tuple[0].(datalog.Identity); !ok || !got.Equal(e2) {
			t.Errorf("expected ?e = %v, got %v", e2, tuple[0])
		}
		if got, ok := tuple[1].(string); !ok || got != "second" {
			t.Errorf("expected ?v = \"second\", got %v", tuple[1])
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	it.Close()

	if count != 1 {
		t.Errorf("expected exactly 1 result for tx=%v, got %d", tx2, count)
	}
}

// TestMaxElementIDForAttribute_AfterRetraction documents the actual retract
// semantics. BadgerStore.Retract deletes the matching (E,A,V) entries from
// every index using the *stored* Tx and discards the passed-in Tx — no
// tombstone is written. So an attribute's high-water mark can drop after a
// retract, all the way to zero when the last entry is removed.
//
// Cache freshness still works because IsAttributeFresh compares cachedMax to
// storeMax for inequality (not direction): any change, even a downward one,
// stales the cache. This test pins both behaviors.
func TestMaxElementIDForAttribute_AfterRetraction(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-retract-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a := datalog.NewKeyword(":atev/retract")
	var aBytes [32]byte
	copy(aBytes[:], a.String())

	e1 := datalog.NewIdentity("retract-e1")
	e2 := datalog.NewIdentity("retract-e2")
	tx1 := datalog.ElementID{Lamport: 50, ReplicaID: 1}
	tx2 := datalog.ElementID{Lamport: 100, ReplicaID: 1}

	if err := store.Assert([]datalog.Datom{
		{E: e1, A: a, V: "v1", Tx: tx1},
		{E: e2, A: a, V: "v2", Tx: tx2},
	}); err != nil {
		t.Fatalf("assert: %v", err)
	}

	// Pre-retract: max across both entities is tx2.
	maxID, err := store.MaxElementIDForAttribute(aBytes[:])
	if err != nil {
		t.Fatalf("MaxElementIDForAttribute: %v", err)
	}
	if maxID.Lamport != tx2.Lamport || maxID.ReplicaID != tx2.ReplicaID {
		t.Fatalf("pre-retract: got %+v, want %+v", maxID, tx2)
	}

	// Retract e2 (the higher-Tx datom). The passed-in retract Tx is
	// irrelevant — Retract finds by (E,A,V) and deletes by stored Tx.
	if err := store.Retract([]datalog.Datom{
		{E: e2, A: a, V: "v2", Tx: datalog.ElementID{Lamport: 999, ReplicaID: 9}},
	}); err != nil {
		t.Fatalf("retract e2: %v", err)
	}

	// After retracting the tx2 entry, max drops back to tx1 (e1 remains).
	maxID, err = store.MaxElementIDForAttribute(aBytes[:])
	if err != nil {
		t.Fatalf("MaxElementIDForAttribute after retract: %v", err)
	}
	if maxID.Lamport != tx1.Lamport || maxID.ReplicaID != tx1.ReplicaID {
		t.Errorf("after retracting e2: got %+v, want %+v (e1's Tx)", maxID, tx1)
	}

	// Retract e1 too — no entries remain, max is zero.
	if err := store.Retract([]datalog.Datom{
		{E: e1, A: a, V: "v1", Tx: datalog.ElementID{Lamport: 1000, ReplicaID: 9}},
	}); err != nil {
		t.Fatalf("retract e1: %v", err)
	}

	maxID, err = store.MaxElementIDForAttribute(aBytes[:])
	if err != nil {
		t.Fatalf("MaxElementIDForAttribute after all retracted: %v", err)
	}
	if maxID != (datalog.ElementID{}) {
		t.Errorf("after retracting all entries: got %+v, want zero (no remaining datoms)", maxID)
	}
}

// TestMaxElementIDForAttribute_MultipleWritesToSameEA verifies that the
// high-water mark tracks the latest Tx when an attribute is overwritten on
// the same entity. Without this, freshness checks would lock onto the first
// write's Tx and miss subsequent overwrites — a real LWW-on-cardinality-one
// invalidation hazard.
func TestMaxElementIDForAttribute_MultipleWritesToSameEA(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-overwrite-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a := datalog.NewKeyword(":atev/overwrite")
	var aBytes [32]byte
	copy(aBytes[:], a.String())
	e := datalog.NewIdentity("overwrite-target")

	if err := store.Assert([]datalog.Datom{
		{E: e, A: a, V: "v1", Tx: datalog.ElementID{Lamport: 10, ReplicaID: 1}},
	}); err != nil {
		t.Fatalf("first assert: %v", err)
	}
	if err := store.Assert([]datalog.Datom{
		{E: e, A: a, V: "v2", Tx: datalog.ElementID{Lamport: 20, ReplicaID: 1}},
	}); err != nil {
		t.Fatalf("second assert: %v", err)
	}
	if err := store.Assert([]datalog.Datom{
		{E: e, A: a, V: "v3", Tx: datalog.ElementID{Lamport: 30, ReplicaID: 1}},
	}); err != nil {
		t.Fatalf("third assert: %v", err)
	}

	maxID, err := store.MaxElementIDForAttribute(aBytes[:])
	if err != nil {
		t.Fatalf("MaxElementIDForAttribute: %v", err)
	}
	want := datalog.ElementID{Lamport: 30, ReplicaID: 1}
	if maxID.Lamport != want.Lamport || maxID.ReplicaID != want.ReplicaID {
		t.Errorf("after three writes to same (E,A), MaxElementIDForAttribute = %+v, want %+v",
			maxID, want)
	}
}

// TestCache_IsAttributeFresh_Integration wires Cache.IsAttributeFresh together
// with a real BadgerStore.MaxElementIDForAttribute (now backed by the ATEV
// seek) to confirm cache freshness flips correctly after a write. The
// pre-existing cache_test.go uses a mockStore that bypasses the production
// MaxElementIDForAttribute path entirely.
func TestCache_IsAttributeFresh_Integration(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-cache-fresh-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a := datalog.NewKeyword(":atev/cache-fresh")
	var aBytes Attribute
	copy(aBytes[:], a.String())
	e := datalog.NewIdentity("cache-fresh-e")

	tx1 := datalog.ElementID{Lamport: 7, ReplicaID: 1}
	if err := store.Assert([]datalog.Datom{
		{E: e, A: a, V: "first", Tx: tx1},
	}); err != nil {
		t.Fatalf("assert: %v", err)
	}

	cache := NewCache()
	// Simulate cache warming: snapshot the current max into attrVersions.
	cache.UpdateAttributeVersion(aBytes, tx1)

	if !cache.IsAttributeFresh(aBytes, store) {
		t.Error("IsAttributeFresh = false immediately after warming; should be true")
	}

	// Write a newer datom. The cached attrVersion stays at tx1, but the store
	// max moves to tx2 — freshness must flip to false.
	tx2 := datalog.ElementID{Lamport: 17, ReplicaID: 1}
	if err := store.Assert([]datalog.Datom{
		{E: e, A: a, V: "second", Tx: tx2},
	}); err != nil {
		t.Fatalf("second assert: %v", err)
	}

	if cache.IsAttributeFresh(aBytes, store) {
		t.Error("IsAttributeFresh = true after a newer write; should be false")
	}
}

// TestChooseIndexForValues_ATEV exercises the hash-join path's prefix builder
// for ATEV. Without this, the ATEV case in hash_join_matcher.go could be
// silently wrong and every existing test would still pass — the matcher
// integration would degrade to an empty-prefix full-ATEV scan under hash joins.
func TestChooseIndexForValues_ATEV(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-hashjoin-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewBadgerMatcher(store)
	a := datalog.NewKeyword(":atev/hashjoin")
	var aStorage Attribute
	copy(aStorage[:], a.String())
	tx := datalog.ElementID{Lamport: 99, ReplicaID: 1}
	e := datalog.NewIdentity("hashjoin-e")
	eHash := e.Hash()

	t.Run("A+Tx_only", func(t *testing.T) {
		_, start, end := matcher.chooseIndexForValues(ATEV, nil, a, nil, tx)
		expected := matcher.store.encoder.EncodePrefix(ATEV, aStorage[:],
			matcher.store.encoder.EncodeTxForPrefix(NewTxFromElementID(tx)))
		if !bytes.HasPrefix(start, expected) {
			t.Errorf("ATEV [A][Tx] prefix: start = %x, expected prefix %x", start, expected)
		}
		if bytes.Compare(start, end) >= 0 {
			t.Error("range start must sort before end")
		}
	})

	t.Run("A+Tx+E", func(t *testing.T) {
		_, start, end := matcher.chooseIndexForValues(ATEV, e, a, nil, tx)
		expected := matcher.store.encoder.EncodePrefix(ATEV, aStorage[:],
			matcher.store.encoder.EncodeTxForPrefix(NewTxFromElementID(tx)),
			eHash[:])
		if !bytes.HasPrefix(start, expected) {
			t.Errorf("ATEV [A][Tx][E] prefix: start = %x, expected prefix %x", start, expected)
		}
		if bytes.Compare(start, end) >= 0 {
			t.Error("range start must sort before end")
		}
	})
}

// TestSimpleBatchScanner_BuildKey_ATEV_VVaries covers buildKey's ATEV branch
// for position=2 (V varies across bindings, with A and Tx fixed as pattern
// constants). The position=0 case is already covered by
// TestSimpleBatchScanner_BuildKey_AllIndices; this fills the other half.
func TestSimpleBatchScanner_BuildKey_ATEV_VVaries(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-batch-v-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewBadgerMatcher(store)
	a := datalog.NewKeyword(":atev/batch-v")
	var aBytes Attribute
	copy(aBytes[:], a.String())
	tx := datalog.ElementID{Lamport: 11, ReplicaID: 1}
	constT := matcher.store.encoder.EncodeTxForPrefix(NewTxFromElementID(tx))

	scanner := &simpleBatchScanner{
		matcher:  matcher,
		index:    ATEV,
		position: 2, // V varies
	}

	// V is between Tx and the trailing positions in ATEV [A][Tx][E][V]; with E
	// unbound, tightening stops at [A][Tx]. Any V value should produce that
	// same prefix.
	got := scanner.buildKey("any-value", aBytes[:], constT)
	if got == nil {
		t.Fatal("buildKey(ATEV, position=2) returned nil")
	}
	expected := matcher.store.encoder.EncodePrefix(ATEV, aBytes[:], constT)
	if !bytes.Equal(got, expected) {
		t.Errorf("ATEV position=2 key mismatch: got %x, want %x", got, expected)
	}
}

// TestChooseIndex_TxOnly_TAEV_WithElementID is the regression check for the
// uint64-fallback removal. The previous TAEV case branched on tx.(uint64)
// before DerefElementID; with the legacy branch gone, this test pins that
// passing an ElementID still routes to TAEV with a proper prefix range.
func TestChooseIndex_TxOnly_TAEV_WithElementID(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-taev-regression-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewBadgerMatcher(store)
	tx := datalog.ElementID{Lamport: 77, ReplicaID: 1}

	idx, start, end := matcher.chooseIndex(nil, nil, nil, tx)
	if idx != TAEV {
		t.Errorf("chooseIndex(nil, nil, nil, ElementID) = %v, want TAEV", idx)
	}
	if len(start) == 0 || len(end) == 0 {
		t.Error("TAEV prefix range start/end should be non-empty")
	}
	if bytes.Compare(start, end) >= 0 {
		t.Error("TAEV prefix range start must sort before end")
	}
	if start[0] != byte(TAEV) {
		t.Errorf("TAEV prefix range start should begin with TAEV byte (%d), got %d", byte(TAEV), start[0])
	}

	// Same Tx passed through *ElementID — DerefElementID handles both, so the
	// pointer form should produce identical routing.
	idxPtr, startPtr, _ := matcher.chooseIndex(nil, nil, nil, &tx)
	if idxPtr != TAEV {
		t.Errorf("chooseIndex with *ElementID = %v, want TAEV", idxPtr)
	}
	if !bytes.Equal(start, startPtr) {
		t.Errorf("ElementID and *ElementID should produce identical prefix; got %x vs %x", start, startPtr)
	}
}
