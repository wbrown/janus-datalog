package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestDatomFromKeyValueSemantics verifies that DatomFromKey returns
// correct values and that successive calls return independent values.
// Before Phase 4: returns independent *Datom pointers (each call allocates)
// After Phase 4: returns independent Datom values (no allocation per call)
func TestDatomFromKeyValueSemantics(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	// Create and encode a test datom
	originalDatom := &datalog.Datom{
		E:  datalog.NewIdentity("test-entity"),
		A:  datalog.NewKeyword(":test/attr"),
		V:  "test-value",
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
		Op: datalog.OpCRDTAdd,
	}
	key := encoder.EncodeKey(EAVT, originalDatom)

	// Decode twice
	d1, err1 := DatomFromKey(EAVT, key, encoder, nil)
	if err1 != nil {
		t.Fatalf("DatomFromKey first call failed: %v", err1)
	}

	d2, err2 := DatomFromKey(EAVT, key, encoder, nil)
	if err2 != nil {
		t.Fatalf("DatomFromKey second call failed: %v", err2)
	}

	// Verify both decoded correctly
	if d1.V != "test-value" {
		t.Errorf("d1.V = %v, expected 'test-value'", d1.V)
	}
	if d2.V != "test-value" {
		t.Errorf("d2.V = %v, expected 'test-value'", d2.V)
	}

	// Verify entity was decoded correctly
	if !d1.E.Equal(originalDatom.E) {
		t.Errorf("d1.E = %v, expected %v", d1.E, originalDatom.E)
	}

	// Verify attribute was decoded correctly
	if d1.A.String() != originalDatom.A.String() {
		t.Errorf("d1.A = %v, expected %v", d1.A, originalDatom.A)
	}

	// Note: After Phase 4, d1 and d2 will be value types, not pointers.
	// The test verifies correctness regardless of the return type.
}

func TestDatomFromBorrowedKeyClonesByteValue(t *testing.T) {
	encoder := &BinaryKeyEncoder{}
	datom := &datalog.Datom{
		E:  datalog.NewIdentity("borrowed-key-bytes"),
		A:  datalog.NewKeyword(":test/bytes"),
		V:  []byte("borrowed-value"),
		Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1},
	}
	key := encoder.EncodeKey(EAVT, datom)
	decoded, err := DatomFromKey(EAVT, key, encoder, nil)
	if err != nil {
		t.Fatal(err)
	}
	value := decoded.V.([]byte)
	offset := bytes.Index(key, []byte("borrowed-value"))
	if offset < 0 {
		t.Fatal("encoded byte value not found in key")
	}
	key[offset] = 'X'
	if string(value) != "borrowed-value" {
		t.Fatalf("decoded byte value aliases borrowed key: %q", value)
	}
}

// TestDatomFromKeyAllIndexTypes verifies DatomFromKey works correctly
// for all index types. This is important because each index has different
// key layouts.
func TestDatomFromKeyAllIndexTypes(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	originalDatom := &datalog.Datom{
		E:  datalog.NewIdentity("test-entity-abc"),
		A:  datalog.NewKeyword(":test/attribute"),
		V:  int64(42),
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 67890},
		Op: datalog.OpCRDTAdd,
	}

	indices := []IndexType{EAVT, EATV, AEVT, AVET, VAET, TAEV}
	indexNames := []string{"EAVT", "EATV", "AEVT", "AVET", "VAET", "TAEV"}

	for i, idx := range indices {
		t.Run(indexNames[i], func(t *testing.T) {
			key := encoder.EncodeKey(idx, originalDatom)

			decoded, err := DatomFromKey(idx, key, encoder, nil)
			if err != nil {
				t.Fatalf("DatomFromKey(%s) failed: %v", indexNames[i], err)
			}

			// Verify entity
			if !decoded.E.Equal(originalDatom.E) {
				t.Errorf("Entity mismatch: got %v, expected %v", decoded.E, originalDatom.E)
			}

			// Verify attribute
			if decoded.A.String() != originalDatom.A.String() {
				t.Errorf("Attribute mismatch: got %v, expected %v", decoded.A, originalDatom.A)
			}

			// Verify value
			if decoded.V != originalDatom.V {
				t.Errorf("Value mismatch: got %v, expected %v", decoded.V, originalDatom.V)
			}

			// Verify transaction
			if decoded.Tx != originalDatom.Tx {
				t.Errorf("Tx mismatch: got %v, expected %v", decoded.Tx, originalDatom.Tx)
			}
		})
	}
}

// TestIteratorDatomStability verifies the behavior of iterator's Datom() method.
// Before Phase 4: each Next() returns a NEW *Datom (different address each time)
// After Phase 4: each Next() overwrites the SAME field (same address, different value)
//
// This test documents the expected behavior change and ensures both behaviors work correctly.
func TestIteratorDatomStability(t *testing.T) {
	db, err := NewDatabase(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Add test data
	tx := db.NewTransaction()
	for i := 0; i < 5; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx.Add(e, datalog.NewKeyword(":test/value"), int64(i))
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	it, err := db.Store().ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	var addresses []*datalog.Datom
	var values []interface{}

	for it.Next() {
		d, err := it.Datom()
		if err != nil {
			t.Fatal(err)
		}
		addresses = append(addresses, d)
		values = append(values, d.V)
	}

	if len(addresses) < 2 {
		t.Fatalf("Expected at least 2 datoms, got %d", len(addresses))
	}

	// Check if addresses are the same (Phase 4 behavior) or different (current behavior)
	allSameAddress := true
	for i := 1; i < len(addresses); i++ {
		if addresses[i] != addresses[0] {
			allSameAddress = false
			break
		}
	}

	// Log the behavior for visibility
	if allSameAddress {
		t.Logf("Phase 4 behavior: All %d Datom() calls returned same address (workspace reuse)", len(addresses))
		// After Phase 4, the values array captures each value at call time
		// but all addresses point to the same location
	} else {
		t.Logf("Current behavior: %d Datom() calls returned different addresses", len(addresses))
	}

	// Verify we got the expected number of results
	t.Logf("Iterated %d datoms with values: %v", len(values), values)
}

// TestBadgerIteratorKeyBufferReuse verifies that the key buffer reuse optimization
// in BadgerIterator.Datom() produces correct results. This tests the fix for the
// ~2.6% regression in SimpleQuery caused by KeyCopy(nil) allocating on each call.
func TestBadgerIteratorKeyBufferReuse(t *testing.T) {
	db, err := NewDatabase(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Add test data with distinct values
	tx := db.NewTransaction()
	entities := make([]datalog.Identity, 5)
	for i := 0; i < 5; i++ {
		entities[i] = datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		tx.Add(entities[i], datalog.NewKeyword(":test/value"), int64(i*100))
		tx.Add(entities[i], datalog.NewKeyword(":test/name"), fmt.Sprintf("name-%d", i))
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("DatomValuesAreCorrectAcrossIteration", func(t *testing.T) {
		it, err := db.Store().ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		// Collect all datoms and their values
		type datomRecord struct {
			E datalog.Identity
			A datalog.Keyword
			V interface{}
		}
		var records []datomRecord

		for it.Next() {
			d, err := it.Datom()
			if err != nil {
				t.Fatal(err)
			}
			// Store copies of the values (not pointers to shared state)
			records = append(records, datomRecord{E: d.E, A: d.A, V: d.V})
		}

		// Verify we got at least the expected number of datoms (5 entities * 2 attributes = 10)
		// May have additional schema/metadata datoms
		if len(records) < 10 {
			t.Fatalf("Expected at least 10 datoms, got %d", len(records))
		}

		// Verify each record has valid, non-zero values
		for i, r := range records {
			if r.E == nil {
				t.Errorf("Record %d: entity is nil", i)
			}
			if r.A == nil {
				t.Errorf("Record %d: attribute is nil", i)
			}
			if r.V == nil {
				t.Errorf("Record %d: value is nil", i)
			}
		}

		// Verify that different datoms have different values (no corruption from buffer reuse)
		valueSet := make(map[string]bool)
		for _, r := range records {
			key := fmt.Sprintf("%s|%s|%v", r.E.L85(), r.A.String(), r.V)
			if valueSet[key] {
				t.Errorf("Duplicate datom found: %s (indicates buffer reuse corruption)", key)
			}
			valueSet[key] = true
		}
	})

	t.Run("WorkspaceReuseIsDocumented", func(t *testing.T) {
		// KeyOnlyIterator uses workspace reuse (currentDatom field).
		// This means Datom() returns a pointer to the SAME memory each time.
		// Callers must copy values if they need them after calling Next().
		it, err := db.Store().ScanKeysOnly(EAVT, []byte{byte(EAVT)}, []byte{byte(EAVT) + 1})
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		// Collect datom pointers
		var addresses []*datalog.Datom

		for it.Next() {
			d, err := it.Datom()
			if err != nil {
				t.Fatal(err)
			}
			addresses = append(addresses, d)
		}

		if len(addresses) < 2 {
			t.Fatalf("Expected at least 2 datoms, got %d", len(addresses))
		}

		// With workspace reuse, all pointers should be the SAME address
		allSame := true
		for i := 1; i < len(addresses); i++ {
			if addresses[i] != addresses[0] {
				allSame = false
				break
			}
		}

		if !allSame {
			t.Errorf("Expected workspace reuse (all same address), but got different addresses")
		} else {
			t.Logf("Confirmed: KeyOnlyIterator uses workspace reuse (%d calls, same address)", len(addresses))
		}
	})

	t.Run("MultipleDatomCallsAtSamePosition", func(t *testing.T) {
		it, err := db.Store().ScanKeysOnly(EAVT, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer it.Close()

		if !it.Next() {
			t.Fatal("Expected at least one datom")
		}

		// Call Datom() multiple times at the same position
		d1, _ := it.Datom()
		d2, _ := it.Datom()
		d3, _ := it.Datom()

		// All should return the same values
		if !d1.E.Equal(d2.E) || !d2.E.Equal(d3.E) {
			t.Errorf("Entity differs across Datom() calls: %v, %v, %v", d1.E, d2.E, d3.E)
		}
		if d1.A.String() != d2.A.String() || d2.A.String() != d3.A.String() {
			t.Errorf("Attribute differs across Datom() calls: %v, %v, %v", d1.A, d2.A, d3.A)
		}
		if d1.V != d2.V || d2.V != d3.V {
			t.Errorf("Value differs across Datom() calls: %v, %v, %v", d1.V, d2.V, d3.V)
		}
	})
}

func TestKeyOnlyIteratorRetainedByteValuesOutliveBorrowedKeys(t *testing.T) {
	db, err := NewDatabase(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	attr := datalog.NewKeyword(":test/bytes")
	first := datalog.NewIdentity("bytes:first")
	second := datalog.NewIdentity("bytes:second")
	tx := db.NewTransaction()
	if err := tx.Set(first, attr, []byte("first-value")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(second, attr, []byte("second-value")); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	iter, err := db.Store().ScanKeysOnly(
		EAVT,
		[]byte{byte(EAVT)},
		[]byte{byte(EAVT) + 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	var retained [][]byte
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			t.Fatal(err)
		}
		if datom.A == attr {
			retained = append(retained, datom.V.([]byte))
		}
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if len(retained) != 2 {
		t.Fatalf("got %d byte values, want 2", len(retained))
	}

	values := map[string]bool{
		string(retained[0]): true,
		string(retained[1]): true,
	}
	if !values["first-value"] || !values["second-value"] {
		t.Fatalf("retained byte values were corrupted after iteration: %q", retained)
	}
	secondBefore := string(retained[1])
	retained[0][0] ^= 0x20
	if string(retained[1]) != secondBefore {
		t.Fatal("retained byte values share backing storage")
	}
}
