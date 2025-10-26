package datalog

import (
	"strings"
	"testing"
	"time"
)

func TestDatomCreation(t *testing.T) {
	// Create a simple datom
	entity := NewIdentity("user:alice")
	attr := NewKeyword(":user/name")
	value := "Alice Smith"
	tx := uint64(1)

	datom := Datom{
		E:  entity,
		A:  attr,
		V:  value,
		Tx: tx,
	}

	// Test string representation
	str := datom.String()
	if str == "" {
		t.Error("datom string representation should not be empty")
	}

	// Should contain entity, attribute, value and tx
	if !strings.Contains(str, "user:alice") {
		t.Error("datom string should contain entity")
	}
}

func TestValueTypes(t *testing.T) {
	// Test different value types that can be stored
	tests := []struct {
		name  string
		value interface{}
	}{
		{"string", "Alice"},
		{"int", int64(42)},
		{"float", 3.14},
		{"time", time.Now()},
		{"keyword", NewKeyword(":user/name")},
		{"boolean", true},
		{"ref", NewIdentity("some-entity")},
		{"bytes", []byte("binary data")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a datom with the value
			datom := Datom{
				E:  NewIdentity("test:1"),
				A:  NewKeyword(":test/value"),
				V:  tt.value,
				Tx: 1,
			}

			// Test string representation
			str := datom.String()
			if str == "" {
				t.Error("string representation should not be empty")
			}
		})
	}
}

func TestKeyword(t *testing.T) {
	kw := NewKeyword(":user/name")

	// Test string representation
	if kw.String() != ":user/name" {
		t.Errorf("expected :user/name, got %s", kw.String())
	}

	// Keyword is just a string type, so we don't have separate namespace/name methods
	// but we can verify the string format
	if !strings.HasPrefix(kw.String(), ":") {
		t.Error("keyword should start with :")
	}
}

func TestIdentity(t *testing.T) {
	id := NewIdentity("user:alice")

	// Test string representation
	if id.String() != "user:alice" {
		t.Errorf("expected user:alice, got %s", id.String())
	}

	// Test L85 encoding
	if id.L85() == "" {
		t.Error("L85 encoding should not be empty")
	}

	// Test hash
	hash := id.Hash()
	if len(hash) != 20 {
		t.Errorf("expected 20-byte hash, got %d bytes", len(hash))
	}

	// Test that same string produces same identity
	id2 := NewIdentity("user:alice")
	if id.L85() != id2.L85() {
		t.Error("same string should produce same identity")
	}
}

func TestIdentityFromHash(t *testing.T) {
	// Create an identity
	id1 := NewIdentity("test:entity")
	hash := id1.Hash()

	// Create identity from hash
	id2 := NewIdentityFromHash(hash)

	// L85 and hash should be the same
	if id1.L85() != id2.L85() {
		t.Error("identity from hash should have same L85")
	}

	// But original string is lost
	if id2.String() == "test:entity" {
		t.Error("identity from hash should not preserve original string")
	}
}

// TestIdentityStorageRoundTrip verifies that identities from storage
// (created via NewIdentityFromHash) are equal to original identities.
// This is critical for joins to work correctly.
func TestIdentityStorageRoundTrip(t *testing.T) {
	// Create an identity
	id1 := NewIdentity("test")
	hash := id1.Hash()

	// Create another identity from the same hash (simulates storage round-trip)
	id2 := NewIdentityFromHash(hash)

	// Create pointers (simulates what tuples contain after interning)
	ptr1 := InternIdentity(id1)
	ptr2 := InternIdentityFromHash(hash)

	// Test 1: Value equality (without pointers)
	if !ValuesEqual(id1, id2) {
		t.Error("identities with same hash should be equal (value comparison)")
	}

	// Test 2: Pointer equality after interning
	if ptr1 != ptr2 {
		t.Error("interning same hash should return same pointer")
	}

	// Test 3: ValuesEqual with pointers
	if !ValuesEqual(ptr1, ptr2) {
		t.Error("identities with same hash should be equal (pointer comparison)")
	}

	// Test 4: ValuesEqual with dereferenced pointers
	if !ValuesEqual(*ptr1, *ptr2) {
		t.Error("identities with same hash should be equal (dereferenced comparison)")
	}

	// Test 5: Direct struct comparison should FAIL (different str/l85 fields)
	// This is WHY we need ValuesEqual - it only compares the hash
	if id1 == id2 {
		t.Error("direct struct comparison should fail (different fields)")
	}
}
