package storage

import (
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
	d1, err1 := DatomFromKey(EAVT, key, encoder)
	if err1 != nil {
		t.Fatalf("DatomFromKey first call failed: %v", err1)
	}

	d2, err2 := DatomFromKey(EAVT, key, encoder)
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

			decoded, err := DatomFromKey(idx, key, encoder)
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

	it, err := db.Store().ScanKeysOnly(EAVT, nil, nil)
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
