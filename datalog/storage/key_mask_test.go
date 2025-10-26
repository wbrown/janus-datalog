package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestKeyMaskConstraint tests if we can evaluate constraints directly on encoded keys
func TestKeyMaskConstraint(t *testing.T) {
	// Create a test datom with age = 25
	datom := &datalog.Datom{
		E:  datalog.NewIdentity("person1"),
		A:  datalog.NewKeyword(":person/age"),
		V:  int64(25),
		Tx: 100,
	}

	// Encode it as an AEVT key
	encoder := &BinaryKeyEncoder{}
	key := encoder.EncodeKey(AEVT, datom)

	// Now create a byte mask for "age = 25"
	// For AEVT: [1 prefix][32 attr][20 entity][1+8 value][20 tx]
	// We want to check bytes at position 53-61 (after prefix+attr+entity)

	// The value we're looking for: TypeInt(1) + int64(25)
	targetValue := make([]byte, 9)
	targetValue[0] = byte(datalog.TypeInt)
	binary.BigEndian.PutUint64(targetValue[1:], 25)

	// Extract value portion from key
	// Skip: 1 (prefix) + 32 (attr) + 20 (entity) = 53
	valueStart := 53
	valueEnd := valueStart + 9 // 1 type + 8 bytes for int64

	if len(key) < valueEnd {
		t.Fatalf("Key too short: %d bytes", len(key))
	}

	actualValue := key[valueStart:valueEnd]

	// Compare directly!
	if !bytes.Equal(actualValue, targetValue) {
		t.Errorf("Value mismatch:\nExpected: %x\nActual:   %x", targetValue, actualValue)
	}

	// Test with different value (age = 30)
	datom2 := &datalog.Datom{
		E:  datalog.NewIdentity("person2"),
		A:  datalog.NewKeyword(":person/age"),
		V:  int64(30),
		Tx: 101,
	}
	key2 := encoder.EncodeKey(AEVT, datom2)
	actualValue2 := key2[valueStart:valueEnd]

	if bytes.Equal(actualValue2, targetValue) {
		t.Error("Should not match age=25 when value is 30")
	}
}

// BenchmarkRawOperations tests the raw cost of operations
func BenchmarkRawOperations(b *testing.B) {
	// Create 10000 test keys
	encoder := &BinaryKeyEncoder{}
	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		datom := &datalog.Datom{
			E:  datalog.NewIdentity(fmt.Sprintf("person%d", i)),
			A:  datalog.NewKeyword(":person/age"),
			V:  int64(i%100 + 1),
			Tx: uint64(i),
		}
		keys[i] = encoder.EncodeKey(AEVT, datom)
	}

	// Target value for age = 25
	targetValue := make([]byte, 9)
	targetValue[0] = byte(datalog.TypeInt)
	binary.BigEndian.PutUint64(targetValue[1:], 25)

	b.Run("10000_ByteComparisons", func(b *testing.B) {
		valueStart := 53
		valueEnd := valueStart + 9

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matches := 0
			for _, key := range keys {
				if bytes.Equal(key[valueStart:valueEnd], targetValue) {
					matches++
				}
			}
			if matches != 100 {
				b.Fatalf("Expected 100 matches, got %d", matches)
			}
		}
	})

	b.Run("10000_Decodings", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded := 0
			for _, key := range keys {
				_, err := DatomFromKey(AEVT, key, encoder)
				if err == nil {
					decoded++
				}
			}
			if decoded != 10000 {
				b.Fatalf("Expected 10000 decoded, got %d", decoded)
			}
		}
	})

	b.Run("100_Decodings", func(b *testing.B) {
		// Only decode the matching 100 keys
		matchingKeys := make([][]byte, 0, 100)
		valueStart := 53
		valueEnd := valueStart + 9
		for _, key := range keys {
			if bytes.Equal(key[valueStart:valueEnd], targetValue) {
				matchingKeys = append(matchingKeys, key)
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded := 0
			for _, key := range matchingKeys {
				_, err := DatomFromKey(AEVT, key, encoder)
				if err == nil {
					decoded++
				}
			}
			if decoded != 100 {
				b.Fatalf("Expected 100 decoded, got %d", decoded)
			}
		}
	})

	b.Run("ByteCompare+100_Decodings", func(b *testing.B) {
		valueStart := 53
		valueEnd := valueStart + 9

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			decoded := 0
			for _, key := range keys {
				if bytes.Equal(key[valueStart:valueEnd], targetValue) {
					_, err := DatomFromKey(AEVT, key, encoder)
					if err == nil {
						decoded++
					}
				}
			}
			if decoded != 100 {
				b.Fatalf("Expected 100 decoded, got %d", decoded)
			}
		}
	})
}

// BenchmarkKeyMaskVsDecoding compares mask-based filtering vs full decoding
func BenchmarkKeyMaskVsDecoding(b *testing.B) {
	// Create 1000 test keys with varying ages
	encoder := &BinaryKeyEncoder{}
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		datom := &datalog.Datom{
			E:  datalog.NewIdentity(string(rune(i))),
			A:  datalog.NewKeyword(":person/age"),
			V:  int64(i%100 + 1),
			Tx: uint64(i),
		}
		keys[i] = encoder.EncodeKey(AEVT, datom)
	}

	// Target value mask for age = 25
	targetValue := make([]byte, 9)
	targetValue[0] = byte(datalog.TypeInt)
	binary.BigEndian.PutUint64(targetValue[1:], 25)

	b.Run("ByteMaskFiltering", func(b *testing.B) {
		valueStart := 53
		valueEnd := valueStart + 9

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matches := 0
			for _, key := range keys {
				if bytes.Equal(key[valueStart:valueEnd], targetValue) {
					matches++
				}
			}
			if matches != 10 {
				b.Fatalf("Expected 10 matches, got %d", matches)
			}
		}
	})

	b.Run("FullDecoding", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matches := 0
			for _, key := range keys {
				// Decode the full datom
				datom, err := DatomFromKey(AEVT, key, encoder)
				if err != nil {
					b.Fatal(err)
				}
				// Check constraint
				if v, ok := datom.V.(int64); ok && v == 25 {
					matches++
				}
			}
			if matches != 10 {
				b.Fatalf("Expected 10 matches, got %d", matches)
			}
		}
	})
}
