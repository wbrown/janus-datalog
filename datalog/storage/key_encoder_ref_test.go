package storage

import (
	"bytes"
	"crypto/sha1"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestBinaryRefValueEncoding(t *testing.T) {
	// Create test entities
	alice := sha1.Sum([]byte("alice"))
	bob := sha1.Sum([]byte("bob"))

	// Create datom with RefValue
	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(alice),
		A:  datalog.NewKeyword("follows"),
		V:  datalog.NewIdentityFromHash(bob),
		Tx: datalog.ElementID{Lamport: uint64(1)},
	}

	encoder := &BinaryKeyEncoder{}

	// Test AVET encoding (where RefValue is in value position)
	avetKey := encoder.EncodeKey(AVET, datom)

	// Key should have:
	// - 1 byte prefix
	// - 32 bytes for attribute
	// - 21 bytes for ref value (1 type byte + 20 raw hash bytes)
	// - 20 bytes for entity
	// - 16 bytes for Tx
	// - 1 byte for Op
	expectedLen := 1 + 32 + 21 + 20 + 16 + 1
	if len(avetKey) != expectedLen {
		t.Errorf("AVET key length = %d, want %d", len(avetKey), expectedLen)
	}

	// The RefValue has its type prefix followed by the raw identity hash.
	// Extract the value portion (after prefix and attribute)
	valueStart := 1 + 32
	valueEnd := valueStart + 21
	valueSection := avetKey[valueStart:valueEnd]

	// First byte is type (0x06 for reference)
	if valueSection[0] != 0x06 {
		t.Errorf("RefValue type byte = %02x, want 0x06", valueSection[0])
	}
	if !bytes.Equal(valueSection[1:], bob[:]) {
		t.Errorf("RefValue bytes = %x, want %x", valueSection[1:], bob)
	}

	// Test decoding
	_, _, v, _, _, _, err := encoder.DecodeKey(AVET, avetKey)
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	// Value should have type prefix + data
	if len(v) < 1 {
		t.Errorf("Decoded value too short")
	}

	// Skip type byte for comparison
	vData := v[1:]
	if len(vData) != 20 {
		t.Errorf("Decoded ref value length = %d, want 20", len(vData))
	}

	if !bytes.Equal(vData, bob[:]) {
		t.Errorf("Decoded ref value = %x, want %x", vData, bob)
	}

	// Also test VAET index where RefValue is first
	vaetKey := encoder.EncodeKey(VAET, datom)

	// In VAET, the raw reference value immediately follows the index prefix.
	vaetValueSection := vaetKey[1:22]
	if vaetValueSection[0] != 0x06 {
		t.Errorf("VAET RefValue type byte = %02x, want 0x06", vaetValueSection[0])
	}
	if !bytes.Equal(vaetValueSection[1:], bob[:]) {
		t.Errorf("VAET RefValue bytes = %x, want %x", vaetValueSection[1:], bob)
	}

	// Test prefix encoding with RefValue
	sd := ToStorageDatom(*datom)
	vBytes, _ := encoder.EncodeValueBytes(datom.V)
	prefix := encoder.EncodePrefix(AVET, sd.A[:], vBytes)
	if !bytes.Equal(prefix[1+32:], vBytes) {
		t.Errorf("AVET prefix RefValue bytes = %x, want %x", prefix[1+32:], vBytes)
	}
}
