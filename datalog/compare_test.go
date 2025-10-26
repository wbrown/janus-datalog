package datalog

import (
	"testing"
)

func TestValuesEqualWithPointers(t *testing.T) {
	// Test with interned values
	id1 := NewIdentity("test")
	id2 := NewIdentity("test")

	// Test direct values
	if !ValuesEqual(id1, id2) {
		t.Error("Expected equal identities to be equal")
	}

	// Test pointers
	ptr1 := InternIdentity(id1)
	ptr2 := InternIdentity(id2)

	if !ValuesEqual(ptr1, ptr2) {
		t.Error("Expected pointers to equal identities to be equal")
	}

	// Test mixed
	if !ValuesEqual(ptr1, id1) {
		t.Error("Expected pointer and value to be equal")
	}

	// Test keywords
	kw1 := NewKeyword(":test")
	kw2 := NewKeyword(":test")

	if !ValuesEqual(kw1, kw2) {
		t.Error("Expected equal keywords to be equal")
	}

	kwPtr1 := InternKeyword(":test")
	kwPtr2 := InternKeyword(":test")

	if !ValuesEqual(kwPtr1, kwPtr2) {
		t.Error("Expected keyword pointers to be equal")
	}
}
