//go:build !(js && wasm)

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
// to the same datom components.
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
		encoder *BinaryKeyEncoder
	}{
		{"Binary", &BinaryKeyEncoder{}},
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

	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
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
	it, err := store.Scan(ScanBound{Index: ATEV, Prefix: []datalog.Value{a}})
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

// TestChooseIndex_ABoundPlusTxBound_PicksATEV verifies that the matcher
// selects ATEV when both A and Tx are bound (and V is unbound). Regressions
// in chooseIndex would silently send these patterns back to AETV/TAEV.
func TestChooseIndex_ABoundPlusTxBound_PicksATEV(t *testing.T) {
	dir, err := os.MkdirTemp("", "atev-matcher-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewPatternMatcher(store)
	a := datalog.NewKeyword(":atev/matcher")
	tx := datalog.ElementID{Lamport: 42, ReplicaID: 1}

	bound := matcher.chooseIndex(nil, a, nil, tx)
	if bound.Index != ATEV {
		t.Errorf("chooseIndex(nil, A, nil, Tx) = %v, want ATEV", bound.Index)
	}
	// The assertions below are about the byte range the bound addresses, so
	// they render it the way this store's keys are encoded.
	run, err := matcher.encoder.EncodeScanBound(bound)
	start := run.Start
	end := run.End
	if err != nil {
		t.Fatalf("encode ATEV scan bound: %v", err)
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

	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewPatternMatcher(store)
	a := datalog.NewKeyword(":atev/routing")
	tx := datalog.ElementID{Lamport: 5, ReplicaID: 1}

	if idx := matcher.chooseIndex(nil, a, "hello", tx).Index; idx == ATEV {
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

	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
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
	matcher := NewPatternMatcher(store).History()
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Constant{Value: tx2},
		},
	}

	result, err := matcher.Match(query.PatternQuery(pattern), nil)
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

	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewPatternMatcher(store)
	a := datalog.NewKeyword(":atev/hashjoin")
	var aStorage Attribute
	copy(aStorage[:], a.String())
	tx := datalog.ElementID{Lamport: 99, ReplicaID: 1}
	e := datalog.NewIdentity("hashjoin-e")
	eHash := e.Hash()

	t.Run("A+Tx_only", func(t *testing.T) {
		start, end := encodeScanBoundForTest(t, matcher,
			matcher.scanBoundForValues(ATEV, nil, a, nil, tx))
		expected := matcher.store.Encoder().EncodePrefix(ATEV, aStorage[:],
			matcher.store.Encoder().EncodeTxForPrefix(NewTxFromElementID(tx)))
		if !bytes.HasPrefix(start, expected) {
			t.Errorf("ATEV [A][Tx] prefix: start = %x, expected prefix %x", start, expected)
		}
		if bytes.Compare(start, end) >= 0 {
			t.Error("range start must sort before end")
		}
	})

	t.Run("A+Tx+E", func(t *testing.T) {
		start, end := encodeScanBoundForTest(t, matcher,
			matcher.scanBoundForValues(ATEV, e, a, nil, tx))
		expected := matcher.store.Encoder().EncodePrefix(ATEV, aStorage[:],
			matcher.store.Encoder().EncodeTxForPrefix(NewTxFromElementID(tx)),
			eHash[:])
		if !bytes.HasPrefix(start, expected) {
			t.Errorf("ATEV [A][Tx][E] prefix: start = %x, expected prefix %x", start, expected)
		}
		if bytes.Compare(start, end) >= 0 {
			t.Error("range start must sort before end")
		}
	})
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

	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	matcher := NewPatternMatcher(store)
	tx := datalog.ElementID{Lamport: 77, ReplicaID: 1}

	bound := matcher.chooseIndex(nil, nil, nil, tx)
	if bound.Index != TAEV {
		t.Errorf("chooseIndex(nil, nil, nil, ElementID) = %v, want TAEV", bound.Index)
	}
	start, end := encodeScanBoundForTest(t, matcher, bound)
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
	boundPtr := matcher.chooseIndex(nil, nil, nil, &tx)
	if boundPtr.Index != TAEV {
		t.Errorf("chooseIndex with *ElementID = %v, want TAEV", boundPtr.Index)
	}
	startPtr, _ := encodeScanBoundForTest(t, matcher, boundPtr)
	if !bytes.Equal(start, startPtr) {
		t.Errorf("ElementID and *ElementID should produce identical prefix; got %x vs %x", start, startPtr)
	}
}
