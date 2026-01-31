package datalog

import (
	"crypto/sha1"
	"strings"
	"testing"
	"time"
)

func TestDatomCreation(t *testing.T) {
	// Create a simple datom
	entity := NewIdentity("user:alice")
	attr := NewKeyword(":user/name")
	value := "Alice Smith"
	tx := ElementID{Lamport: 1, ReplicaID: 1}

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
				Tx: ElementID{Lamport: 1, ReplicaID: 1},
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
	// Clear interns to test fresh behavior
	ClearInterns()

	// Create identity from hash first (no original string)
	hash := sha1.Sum([]byte("test:entity"))
	id1 := NewIdentityFromHash(hash)

	// Original string is unknown when created from hash alone
	if id1.String() == "test:entity" {
		t.Error("identity from hash alone should not know original string")
	}

	// L85 should be set
	if id1.L85() == "" {
		t.Error("identity from hash should have L85")
	}

	// Now create from string - should return same interned pointer
	id2 := NewIdentity("test:entity")
	if id1 != id2 {
		t.Error("NewIdentity should return same pointer as NewIdentityFromHash for same hash")
	}
}

// TestIdentityStorageRoundTrip verifies that identities from storage
// (created via NewIdentityFromHash) are equal to original identities.
// This is critical for joins to work correctly.
func TestIdentityStorageRoundTrip(t *testing.T) {
	// Clear interns to test fresh behavior
	ClearInterns()

	// Create an identity
	id1 := NewIdentity("test")
	hash := id1.Hash()

	// Create another identity from the same hash (simulates storage round-trip)
	// Since all constructors now intern, this returns the SAME pointer
	id2 := NewIdentityFromHash(hash)

	// Test 1: Pointer equality - they are the SAME interned pointer
	if id1 != id2 {
		t.Error("NewIdentity and NewIdentityFromHash with same hash should return same pointer")
	}

	// Test 2: ValuesEqual should work
	if !ValuesEqual(id1, id2) {
		t.Error("identities with same hash should be equal (value comparison)")
	}

	// Test 3: InternIdentity is now effectively an identity function
	ptr1 := InternIdentity(id1)
	ptr2 := InternIdentityFromHash(hash)
	if ptr1 != ptr2 {
		t.Error("interning same hash should return same pointer")
	}
	if ptr1 != id1 {
		t.Error("InternIdentity should return same pointer for already-interned identity")
	}

	// Test 4: Equal method should work
	if !id1.Equal(id2) {
		t.Error("Equal should return true for same pointer")
	}
}
