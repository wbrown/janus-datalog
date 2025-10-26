package codec

import (
	"bytes"
	"crypto/sha1"
	"sort"
	"testing"
)

func TestL85Verify20ByteEncoding(t *testing.T) {
	// Test with SHA1 hashes (20 bytes)
	testStrings := []string{"hello", "world", "datalog", "test", "example"}

	for _, s := range testStrings {
		hash := sha1.Sum([]byte(s))
		encoded := EncodeFixed20(hash)

		// Should be exactly 25 chars
		if len(encoded) != 25 {
			t.Errorf("Wrong length for %q: got %d chars", s, len(encoded))
		}

		// Should decode back correctly
		decoded, err := DecodeFixed20(encoded)
		if err != nil {
			t.Errorf("Decode error for %q: %v", s, err)
			continue
		}

		if decoded != hash {
			t.Errorf("Round trip failed for %q", s)
		}

		t.Logf("%q -> SHA1: %x -> L85: %s", s, hash, encoded)
	}
}

func TestL85VerifySortOrder(t *testing.T) {
	// Create many hashes and verify sort order is preserved
	var data []struct {
		str     string
		hash    [20]byte
		encoded string
	}

	testStrings := []string{
		"", "a", "b", "c", "aa", "ab", "ba", "bb",
		"alice", "bob", "charlie", "diana", "eve",
		"test1", "test2", "test10", "test20",
	}

	for _, s := range testStrings {
		hash := sha1.Sum([]byte(s))
		encoded := EncodeFixed20(hash)
		data = append(data, struct {
			str     string
			hash    [20]byte
			encoded string
		}{s, hash, encoded})
	}

	// Sort by hash bytes
	sortedByHash := make([]int, len(data))
	for i := range sortedByHash {
		sortedByHash[i] = i
	}
	sort.Slice(sortedByHash, func(i, j int) bool {
		return bytes.Compare(
			data[sortedByHash[i]].hash[:],
			data[sortedByHash[j]].hash[:],
		) < 0
	})

	// Sort by encoded string
	sortedByEncoded := make([]int, len(data))
	for i := range sortedByEncoded {
		sortedByEncoded[i] = i
	}
	sort.Slice(sortedByEncoded, func(i, j int) bool {
		return data[sortedByEncoded[i]].encoded < data[sortedByEncoded[j]].encoded
	})

	// Verify same order
	allMatch := true
	for i := range sortedByHash {
		if sortedByHash[i] != sortedByEncoded[i] {
			allMatch = false
			t.Errorf("Sort order mismatch at position %d:", i)
			t.Errorf("  By hash:    %q (hash: %x)",
				data[sortedByHash[i]].str,
				data[sortedByHash[i]].hash[:8])
			t.Errorf("  By encoded: %q (hash: %x)",
				data[sortedByEncoded[i]].str,
				data[sortedByEncoded[i]].hash[:8])
		}
	}

	if allMatch {
		t.Log("✓ Sort order preserved for all test cases")
	}
}

func TestL85VerifyAlphabet(t *testing.T) {
	// Verify the alphabet is exactly 85 characters
	if len(L85Alphabet) != 85 {
		t.Errorf("Alphabet length is %d, expected 85", len(L85Alphabet))
	}

	// Verify no duplicates
	seen := make(map[rune]bool)
	for i, c := range L85Alphabet {
		if seen[c] {
			t.Errorf("Duplicate character %c at position %d", c, i)
		}
		seen[c] = true
	}

	// Verify alphabet is sorted (for sort order preservation)
	sorted := []byte(L85Alphabet)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	if string(sorted) != L85Alphabet {
		t.Error("Alphabet is not in sorted order")
		t.Logf("Expected: %s", string(sorted))
		t.Logf("Actual:   %s", L85Alphabet)
	}
}

func TestL85VerifySpecificBytes(t *testing.T) {
	// Test specific byte patterns
	tests := []struct {
		name  string
		input []byte
	}{
		{"all zeros", bytes.Repeat([]byte{0x00}, 20)},
		{"all ones", bytes.Repeat([]byte{0xFF}, 20)},
		{"ascending", []byte{
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
			0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
			0x10, 0x11, 0x12, 0x13,
		}},
		{"descending", []byte{
			0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8,
			0xF7, 0xF6, 0xF5, 0xF4, 0xF3, 0xF2, 0xF1, 0xF0,
			0xEF, 0xEE, 0xED, 0xEC,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeL85(tt.input)
			decoded, err := DecodeL85(encoded)
			if err != nil {
				t.Fatalf("Decode error: %v", err)
			}
			if !bytes.Equal(decoded, tt.input) {
				t.Errorf("Round trip failed")
				t.Logf("Input:   %x", tt.input)
				t.Logf("Decoded: %x", decoded)
			}
			t.Logf("%s: %x -> %s", tt.name, tt.input[:8], encoded)
		})
	}
}
