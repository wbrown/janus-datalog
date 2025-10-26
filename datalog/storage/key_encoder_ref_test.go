package storage

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestL85RefValueEncoding(t *testing.T) {
	// Create test entities
	alice := sha1.Sum([]byte("alice"))
	bob := sha1.Sum([]byte("bob"))
	followsAttr := sha1.Sum([]byte("follows"))

	// Create datom with RefValue
	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(alice),
		A:  datalog.NewKeyword("follows"),
		V:  datalog.NewIdentityFromHash(bob),
		Tx: uint64(1),
	}

	encoder := NewKeyEncoder(L85Strategy)

	// Test AVET encoding (where RefValue is in value position)
	avetKey := encoder.EncodeKey(AVET, datom)

	// Convert to string and check structure
	keyStr := string(avetKey)
	t.Logf("AVET key: %s", hex.EncodeToString(avetKey))
	t.Logf("AVET key (string): %s", keyStr)

	// Key should have:
	// - 1 byte prefix
	// - 40 chars for attribute (L85 for 32 bytes)
	// - 26 chars for ref value (1 type byte + 25 L85 chars)
	// - 25 chars for entity (L85)
	// - 25 chars for tx (L85)
	expectedLen := 1 + 40 + 26 + 25 + 25
	if len(avetKey) != expectedLen {
		t.Errorf("AVET key length = %d, want %d", len(avetKey), expectedLen)
	}

	// The RefValue should have type prefix + L85-encoded data
	// Extract the value portion (after prefix and attribute)
	valueStart := 1 + 40        // 40 chars for 32-byte attribute
	valueEnd := valueStart + 26 // 1 type byte + 25 L85 chars
	valueSection := keyStr[valueStart:valueEnd]

	// First byte is type (0x06 for reference)
	if valueSection[0] != 0x06 {
		t.Errorf("RefValue type byte = %02x, want 0x06", valueSection[0])
	}

	// Rest should be L85 encoded
	if !isL85String(valueSection[1:]) {
		t.Errorf("RefValue not L85-encoded in AVET index: %s", hex.EncodeToString([]byte(valueSection[1:])))
	}

	// Test decoding
	_, _, v, _, err := encoder.DecodeKey(AVET, avetKey)
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

	if hex.EncodeToString(vData) != hex.EncodeToString(bob[:]) {
		t.Errorf("Decoded ref value = %x, want %x", vData, bob)
	}

	// Also test VAET index where RefValue is first
	vaetKey := encoder.EncodeKey(VAET, datom)
	t.Logf("VAET key: %s", hex.EncodeToString(vaetKey))

	// In VAET, ref value should have type + L85-encoded at position 1-27
	vaetValueSection := string(vaetKey)[1:27] // 1 type byte + 25 L85 chars
	if vaetValueSection[0] != 0x06 {
		t.Errorf("VAET RefValue type byte = %02x, want 0x06", vaetValueSection[0])
	}
	if !isL85String(vaetValueSection[1:]) {
		t.Errorf("RefValue not L85-encoded in VAET index")
	}

	// Test prefix encoding with RefValue
	prefix := encoder.EncodePrefix(AVET, followsAttr[:], bob[:])
	t.Logf("AVET prefix with ref: %s", hex.EncodeToString(prefix))

	// Prefix should have L85-encoded ref value
	prefixStr := string(prefix)
	refStart := 1 + 25 // After prefix byte and attribute
	if len(prefixStr) >= refStart+25 {
		refSection := prefixStr[refStart : refStart+25]
		if !isL85String(refSection) {
			t.Errorf("RefValue not L85-encoded in prefix")
		}
	}
}

func isL85String(s string) bool {
	// L85 alphabet from codec
	l85Alphabet := `!$%&()+,-./0123456789:;<=>@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]_` + "`" + `abcdefghijklmnopqrstuvwxyz{}`

	for _, c := range s {
		if !strings.ContainsRune(l85Alphabet, c) {
			return false
		}
	}
	return true
}
