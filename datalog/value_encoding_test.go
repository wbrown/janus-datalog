package datalog

import (
	"bytes"
	"strings"
	"testing"
	"time"
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

// ---- EncodeValue Tests ----

func TestEncodeValue_BelowThreshold(t *testing.T) {
	threshold := 256
	for _, size := range []int{0, 1, 10, 100, 255} {
		input := strings.Repeat("x", size)
		vType, vData, blobData := EncodeValue(input, threshold)
		if vType != TypeString {
			t.Errorf("size=%d: got type %d, want TypeString (%d)", size, vType, TypeString)
		}
		if !bytes.Equal(vData, []byte(input)) {
			t.Errorf("size=%d: data mismatch", size)
		}
		if blobData != nil {
			t.Errorf("size=%d: expected nil blobData", size)
		}
	}
}

func TestEncodeValue_BelowThreshold_Bytes(t *testing.T) {
	threshold := 256
	input := make([]byte, 100)
	for i := range input {
		input[i] = byte(i)
	}
	vType, vData, blobData := EncodeValue(input, threshold)
	if vType != TypeBytes {
		t.Errorf("got type %d, want TypeBytes (%d)", vType, TypeBytes)
	}
	if !bytes.Equal(vData, input) {
		t.Error("data mismatch")
	}
	if blobData != nil {
		t.Error("expected nil blobData")
	}
}

func TestEncodeValue_CompressibleString(t *testing.T) {
	threshold := 256
	input := strings.Repeat("This is a compressible test string. ", 20) // ~720 bytes

	vType, vData, blobData := EncodeValue(input, threshold)
	if vType != TypeCompressedString {
		t.Fatalf("got type %d, want TypeCompressedString (%d)", vType, TypeCompressedString)
	}
	if len(vData) >= len(input) {
		t.Errorf("compressed (%d) should be smaller than original (%d)", len(vData), len(input))
	}
	if blobData != nil {
		t.Error("expected nil blobData for Tier 2")
	}

	// Round-trip through ValueFromBytes
	decoded, err := ValueFromBytes(TypeCompressedString, vData)
	if err != nil {
		t.Fatalf("ValueFromBytes: %v", err)
	}
	if decoded.(string) != input {
		t.Error("round-trip failed: decoded string doesn't match original")
	}
}

func TestEncodeValue_CompressibleBytes(t *testing.T) {
	threshold := 256
	input := []byte(strings.Repeat("binary compressible data ", 30))

	vType, vData, blobData := EncodeValue(input, threshold)
	if vType != TypeCompressedBytes {
		t.Fatalf("got type %d, want TypeCompressedBytes (%d)", vType, TypeCompressedBytes)
	}
	if blobData != nil {
		t.Error("expected nil blobData for Tier 2")
	}

	decoded, err := ValueFromBytes(TypeCompressedBytes, vData)
	if err != nil {
		t.Fatalf("ValueFromBytes: %v", err)
	}
	if !bytes.Equal(decoded.([]byte), input) {
		t.Error("round-trip failed")
	}
}

func TestEncodeValue_SafetyNet(t *testing.T) {
	threshold := 256
	// High-entropy data that won't compress
	input := make([]byte, 300)
	for i := range input {
		input[i] = byte((i*137 + 43) % 256) // pseudo-random, high entropy
	}
	str := string(input)

	vType, vData, _ := EncodeValue(str, threshold)
	if vType != TypeString {
		t.Errorf("high-entropy string: got type %d, want TypeString (safety net)", vType)
	}
	if !bytes.Equal(vData, []byte(str)) {
		t.Error("safety net: data should be raw bytes")
	}
}

func TestEncodeValue_ThresholdDisabled(t *testing.T) {
	input := strings.Repeat("compressible ", 100)

	// threshold = 0 disables compression
	vType, _, _ := EncodeValue(input, 0)
	if vType != TypeString {
		t.Errorf("threshold=0: got type %d, want TypeString", vType)
	}

	// threshold = -1 also disables
	vType, _, _ = EncodeValue(input, -1)
	if vType != TypeString {
		t.Errorf("threshold=-1: got type %d, want TypeString", vType)
	}
}

func TestEncodeValue_ThresholdHuge(t *testing.T) {
	input := strings.Repeat("x", 10000)
	vType, _, _ := EncodeValue(input, 1000000)
	if vType != TypeString {
		t.Errorf("threshold=1M: got type %d, want TypeString (below threshold)", vType)
	}
}

func TestEncodeValue_ExistingTypes_NoRegression(t *testing.T) {
	threshold := 256

	// int64
	vType, vData, blob := EncodeValue(int64(42), threshold)
	if vType != TypeInt || len(vData) != 8 || blob != nil {
		t.Errorf("int64: type=%d data_len=%d blob=%v", vType, len(vData), blob)
	}

	// float64
	vType, vData, blob = EncodeValue(3.14, threshold)
	if vType != TypeFloat || len(vData) != 8 || blob != nil {
		t.Errorf("float64: type=%d data_len=%d blob=%v", vType, len(vData), blob)
	}

	// bool
	vType, vData, blob = EncodeValue(true, threshold)
	if vType != TypeBool || len(vData) != 1 || blob != nil {
		t.Errorf("bool: type=%d data_len=%d blob=%v", vType, len(vData), blob)
	}

	// time
	vType, vData, blob = EncodeValue(time.Now(), threshold)
	if vType != TypeTime || len(vData) != 8 || blob != nil {
		t.Errorf("time: type=%d data_len=%d blob=%v", vType, len(vData), blob)
	}

	// keyword
	vType, _, blob = EncodeValue(NewKeyword(":test"), threshold)
	if vType != TypeKeyword || blob != nil {
		t.Errorf("keyword: type=%d blob=%v", vType, blob)
	}

	// identity/reference
	vType, vData, blob = EncodeValue(NewIdentity("x"), threshold)
	if vType != TypeReference || len(vData) != 20 || blob != nil {
		t.Errorf("identity: type=%d data_len=%d blob=%v", vType, len(vData), blob)
	}

	// short string (below threshold)
	vType, _, blob = EncodeValue("hello", threshold)
	if vType != TypeString || blob != nil {
		t.Errorf("short string: type=%d blob=%v", vType, blob)
	}
}

func TestEncodeValue_Determinism(t *testing.T) {
	input := strings.Repeat("determinism in value encoding ", 20)
	vType1, vData1, _ := EncodeValue(input, 256)

	for i := 0; i < 100; i++ {
		vType, vData, _ := EncodeValue(input, 256)
		if vType != vType1 {
			t.Fatalf("iteration %d: type %d != %d", i, vType, vType1)
		}
		if !bytes.Equal(vData, vData1) {
			t.Fatalf("iteration %d: data differs", i)
		}
	}
}

func TestEncodeValue_HashedString(t *testing.T) {
	// Create a string large enough that even compressed it exceeds maxKeyValueSize.
	// We need compressed > 60KB. At ~3-4x compression, need ~200-240KB of prose.
	// Use repetitive-but-varied text to avoid extremely high compression ratios.
	input := strings.Repeat(
		"The quick brown fox jumped over the lazy dog on a fine summer morning. "+
			"The birds were singing and the sun was shining brightly through the trees. "+
			"A gentle breeze carried the scent of flowers across the meadow below. "+
			"In the distance, mountains rose against the clear blue sky above. ", 800)

	vType, vData, blobData := EncodeValue(input, 256)

	if len(input) < 200000 {
		t.Skipf("input only %d bytes, may not trigger Tier 3", len(input))
	}

	if vType == TypeHashedString {
		// Tier 3: verify hash and blob data
		if len(vData) != 20 {
			t.Errorf("hash should be 20 bytes, got %d", len(vData))
		}
		if blobData == nil {
			t.Fatal("expected non-nil blobData for Tier 3")
		}
		if len(blobData.CompressedBytes) == 0 {
			t.Error("blobData.CompressedBytes should not be empty")
		}
		// Verify hash matches
		if !bytes.Equal(vData, blobData.Hash[:]) {
			t.Error("hash in vData doesn't match blobData.Hash")
		}
	} else if vType == TypeCompressedString {
		// Tier 2: compressed fits in key (text was more compressible than expected)
		t.Logf("input %d bytes compressed to %d (Tier 2, not Tier 3)", len(input), len(vData))
	} else {
		t.Errorf("expected TypeCompressedString or TypeHashedString, got %d", vType)
	}
}
