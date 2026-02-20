package datalog

import (
	"testing"
)

func TestSymbolValueEncoding_RoundTrip(t *testing.T) {
	tests := []string{
		"my-function",
		"concat",
		"rule-name",
		"$source",
		"?var",
	}

	for _, name := range tests {
		sym := NewSymbol(name)

		// Check type
		vt := Type(sym)
		if vt != TypeSymbol {
			t.Errorf("Type(NewSymbol(%q)) = %d, want TypeSymbol (%d)", name, vt, TypeSymbol)
		}

		// Encode
		data := ValueBytes(sym)
		if string(data) != name {
			t.Errorf("ValueBytes(NewSymbol(%q)) = %q, want %q", name, string(data), name)
		}

		// Decode
		val, err := ValueFromBytes(TypeSymbol, data)
		if err != nil {
			t.Fatalf("ValueFromBytes(TypeSymbol, %q): %v", name, err)
		}

		got, ok := val.(Symbol)
		if !ok {
			t.Fatalf("ValueFromBytes(TypeSymbol, %q) returned %T, want Symbol", name, val)
		}

		// Interning: round-tripped symbol should be pointer-equal to original
		if got != sym {
			t.Errorf("round-tripped symbol %q not pointer-equal to original", name)
		}
	}
}

func TestSymbolValueType_Distinct(t *testing.T) {
	// Symbol, Keyword, and String should have distinct type tags
	sym := NewSymbol("foo")
	kw := NewKeyword(":foo")
	str := "foo"

	symType := Type(sym)
	kwType := Type(kw)
	strType := Type(str)

	if symType == kwType {
		t.Errorf("Symbol and Keyword have same type tag: %d", symType)
	}
	if symType == strType {
		t.Errorf("Symbol and String have same type tag: %d", symType)
	}
	if symType != TypeSymbol {
		t.Errorf("Symbol type = %d, want %d (TypeSymbol)", symType, TypeSymbol)
	}
}

func TestElementIDValueEncoding_RoundTrip(t *testing.T) {
	tests := []ElementID{
		{Lamport: 0, ReplicaID: 0}, // HEAD/zero
		{Lamport: 1, ReplicaID: 1}, // small values
		{Lamport: 1234, ReplicaID: 5678},
		{Lamport: 1706745600000000000, ReplicaID: 12345678901234567}, // realistic values
		{Lamport: ^uint64(0), ReplicaID: ^uint64(0)},                 // max values
	}

	for _, id := range tests {
		// Check type
		vt := Type(id)
		if vt != TypeElementID {
			t.Errorf("Type(ElementID{%d, %d}) = %d, want TypeElementID (%d)", id.Lamport, id.ReplicaID, vt, TypeElementID)
		}

		// Encode
		data := ValueBytes(id)
		if len(data) != ElementIDSize {
			t.Errorf("ValueBytes(ElementID{%d, %d}) = %d bytes, want %d", id.Lamport, id.ReplicaID, len(data), ElementIDSize)
		}

		// Decode
		val, err := ValueFromBytes(TypeElementID, data)
		if err != nil {
			t.Fatalf("ValueFromBytes(TypeElementID, ...): %v", err)
		}

		got, ok := val.(ElementID)
		if !ok {
			t.Fatalf("ValueFromBytes(TypeElementID, ...) returned %T, want ElementID", val)
		}

		// Check equality
		if got.Lamport != id.Lamport || got.ReplicaID != id.ReplicaID {
			t.Errorf("round-trip failed: got {%d, %d}, want {%d, %d}",
				got.Lamport, got.ReplicaID, id.Lamport, id.ReplicaID)
		}
	}
}

func TestElementIDValueType_Distinct(t *testing.T) {
	// ElementID should have a distinct type tag from other types
	eid := ElementID{Lamport: 100, ReplicaID: 5}
	intVal := int64(100)
	sym := NewSymbol("foo")

	eidType := Type(eid)
	intType := Type(intVal)
	symType := Type(sym)

	if eidType == intType {
		t.Errorf("ElementID and int64 have same type tag: %d", eidType)
	}
	if eidType == symType {
		t.Errorf("ElementID and Symbol have same type tag: %d", eidType)
	}
	if eidType != TypeElementID {
		t.Errorf("ElementID type = %d, want %d (TypeElementID)", eidType, TypeElementID)
	}
}

// TestElementIDPointerValueEncoding tests that *ElementID works in Type() and
// ValueBytes() without panicking. The InternedTupleBuilder stores Tx as
// *ElementID, and any code path that calls Type() or ValueBytes() on tuple
// values must handle both pointer and value forms.
func TestElementIDPointerValueEncoding(t *testing.T) {
	eid := ElementID{Lamport: 1234, ReplicaID: 5678}

	// *ElementID should return TypeElementID
	ptrType := Type(&eid)
	if ptrType != TypeElementID {
		t.Errorf("Type(&ElementID) = %d, want TypeElementID (%d)", ptrType, TypeElementID)
	}

	// *ElementID should produce same bytes as ElementID value
	ptrBytes := ValueBytes(&eid)
	valBytes := ValueBytes(eid)
	if len(ptrBytes) != len(valBytes) {
		t.Fatalf("ValueBytes(&eid) len = %d, ValueBytes(eid) len = %d", len(ptrBytes), len(valBytes))
	}
	for i := range ptrBytes {
		if ptrBytes[i] != valBytes[i] {
			t.Errorf("ValueBytes(&eid)[%d] = %d, ValueBytes(eid)[%d] = %d", i, ptrBytes[i], i, valBytes[i])
		}
	}

	// nil *ElementID should not panic in Type()
	var nilPtr *ElementID
	nilType := Type(nilPtr)
	// We don't care about the exact type for nil, just that it doesn't panic
	_ = nilType
}

func TestElementIDValueBytes_NaturalOrder(t *testing.T) {
	// Value encoding should use natural order (not bitwise NOT like key encoding)
	// This means lower Lamport values should encode to smaller bytes
	low := ElementID{Lamport: 100, ReplicaID: 0}
	high := ElementID{Lamport: 200, ReplicaID: 0}

	lowBytes := ValueBytes(low)
	highBytes := ValueBytes(high)

	// Compare bytes - lower Lamport should produce smaller bytes (natural order)
	if lowBytes[0] > highBytes[0] || (lowBytes[0] == highBytes[0] && lowBytes[7] > highBytes[7]) {
		t.Errorf("Value encoding should use natural order: low(%v) should encode to smaller bytes than high(%v)", low, high)
	}
}
