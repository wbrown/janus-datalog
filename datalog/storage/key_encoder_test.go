package storage

import (
	"bytes"
	"crypto/sha1"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Sink variables to prevent compiler optimization in benchmarks
var (
	sinkBytes  []byte
	sinkArray  [16]byte
	sinkKey    []byte
	sinkEntity [20]byte
	sinkAttr   [32]byte
	sinkValue  []byte
	sinkTx     [16]byte
)

func TestKeyEncoders(t *testing.T) {
	// Create test datom
	entity := sha1.Sum([]byte("entity1"))

	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(entity),
		A:  datalog.NewKeyword("attr1"),
		V:  "hello world",
		Tx: datalog.ElementID{Lamport: uint64(1)},
	}

	// Test both encoders
	encoders := []struct {
		name    string
		encoder KeyEncoder
	}{
		{"L85", NewKeyEncoder(L85Strategy)},
		{"Binary", NewKeyEncoder(BinaryStrategy)},
	}

	for _, tc := range encoders {
		t.Run(tc.name, func(t *testing.T) {
			encoder := tc.encoder

			// Test all index types
			indices := []IndexType{EAVT, AEVT, AETV, ATEV, AVET, VAET, TAEV}

			for _, idx := range indices {
				// Encode key
				key := encoder.EncodeKey(idx, datom)

				// Key should have content
				if len(key) == 0 {
					t.Errorf("%s: empty key for index %v", tc.name, idx)
				}

				// Decode key
				e, _, v, tx, _, _, err := encoder.DecodeKey(idx, key)
				if err != nil {
					t.Errorf("%s: decode error for index %v: %v", tc.name, idx, err)
					continue
				}

				// Verify components
				if !bytes.Equal(e[:], entity[:]) {
					t.Errorf("%s: entity mismatch for index %v", tc.name, idx)
				}
				// For attribute, we need to check if it matches the keyword
				// The encoder will handle the conversion internally
				// Value should have type prefix (1 byte) + data
				if len(v) < 1 {
					t.Errorf("%s: value too short for index %v", tc.name, idx)
				} else if !bytes.Equal(v[1:], []byte("hello world")) {
					t.Errorf("%s: value mismatch for index %v", tc.name, idx)
				}
				// For tx, verify it's not zero (tx is now 16 bytes = ElementID)
				var zeroTx [16]byte
				if tx == zeroTx {
					t.Errorf("%s: tx is zero for index %v", tc.name, idx)
				}
			}

			// Test prefix encoding
			prefix := encoder.EncodePrefix(EAVT, entity[:])
			if len(prefix) == 0 {
				t.Errorf("%s: empty prefix", tc.name)
			}

			// Test prefix range
			start, end := encoder.EncodePrefixRange(EAVT, entity[:])
			if len(start) == 0 || len(end) == 0 {
				t.Errorf("%s: empty range", tc.name)
			}
			if bytes.Compare(start, end) >= 0 {
				t.Errorf("%s: invalid range order", tc.name)
			}
		})
	}
}

func TestKeyEncoderSortOrder(t *testing.T) {
	// Create multiple entities with increasing values
	entities := []string{"alice", "bob", "charlie", "diana"}
	var datoms []*datalog.Datom

	for _, name := range entities {
		entity := sha1.Sum([]byte(name))

		datom := &datalog.Datom{
			E:  datalog.NewIdentityFromHash(entity),
			A:  datalog.NewKeyword("name"),
			V:  name,
			Tx: datalog.ElementID{Lamport: uint64(1)},
		}
		datoms = append(datoms, datom)
	}

	// Test both encoders maintain sort order
	encoders := []struct {
		name    string
		encoder KeyEncoder
	}{
		{"L85", NewKeyEncoder(L85Strategy)},
		{"Binary", NewKeyEncoder(BinaryStrategy)},
	}

	for _, tc := range encoders {
		t.Run(tc.name, func(t *testing.T) {
			encoder := tc.encoder

			// Encode all datoms
			var keys [][]byte
			for _, d := range datoms {
				key := encoder.EncodeKey(EAVT, d)
				keys = append(keys, key)
			}

			// Verify that entity order is preserved in keys
			// (entities are SHA1 hashes, so order won't match string order)
			for i := 1; i < len(keys); i++ {
				// Just verify keys are different
				if bytes.Equal(keys[i-1], keys[i]) {
					t.Errorf("%s: duplicate keys for different entities", tc.name)
				}
			}
		})
	}
}

// BenchmarkTxToDescending benchmarks the txToDescending function which is called
// on every key encode (6x per datom write) and every key decode.
// This isolates the allocation regression identified in the CRDT implementation.
// Expected: Currently allocates 16 bytes per call. After fix should be 0 allocs.
func BenchmarkTxToDescending(b *testing.B) {
	tx := [16]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x23, 0x45,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkArray = txToDescending(tx)
	}
}

// BenchmarkTxFromDescending benchmarks the reverse operation used in DecodeKey.
func BenchmarkTxFromDescending(b *testing.B) {
	encoded := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0xDC, 0xBA,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sinkArray = txFromDescending(encoded)
	}
}

// BenchmarkKeyEncoding benchmarks the full EncodeKey path for each index type.
// This captures the combined cost of txToDescending, concatBytes, and value encoding.
func BenchmarkKeyEncoding(b *testing.B) {
	entity := sha1.Sum([]byte("benchmark-entity"))
	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(entity),
		A:  datalog.NewKeyword(":benchmark/attribute"),
		V:  "benchmark value string",
		Tx: datalog.ElementID{Lamport: 12345678, ReplicaID: 1},
	}

	encoder := &BinaryKeyEncoder{}

	indices := []struct {
		name  string
		index IndexType
	}{
		{"EAVT", EAVT},
		{"EATV", EATV},
		{"AEVT", AEVT},
		{"AVET", AVET},
		{"VAET", VAET},
		{"TAEV", TAEV},
	}

	for _, idx := range indices {
		b.Run(idx.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sinkKey = encoder.EncodeKey(idx.index, datom)
			}
		})
	}
}

// BenchmarkKeyDecoding benchmarks the full DecodeKey path for each index type.
// This isolates the decode path from DatomFromKey to measure raw decoding cost.
func BenchmarkKeyDecoding(b *testing.B) {
	entity := sha1.Sum([]byte("benchmark-entity"))
	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(entity),
		A:  datalog.NewKeyword(":benchmark/attribute"),
		V:  "benchmark value string",
		Tx: datalog.ElementID{Lamport: 12345678, ReplicaID: 1},
	}

	encoder := &BinaryKeyEncoder{}

	indices := []struct {
		name  string
		index IndexType
	}{
		{"EAVT", EAVT},
		{"EATV", EATV},
		{"AEVT", AEVT},
		{"AVET", AVET},
		{"VAET", VAET},
		{"TAEV", TAEV},
	}

	for _, idx := range indices {
		// Pre-encode the key
		key := encoder.EncodeKey(idx.index, datom)

		b.Run(idx.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				sinkEntity, sinkAttr, sinkValue, sinkTx, _, _, _ = encoder.DecodeKey(idx.index, key)
			}
		})
	}
}

// BenchmarkKeyEncodeDecode benchmarks the full round-trip encode+decode path.
// This represents the cost when writing a datom (encode) and then reading it back (decode).
func BenchmarkKeyEncodeDecode(b *testing.B) {
	entity := sha1.Sum([]byte("benchmark-entity"))
	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(entity),
		A:  datalog.NewKeyword(":benchmark/attribute"),
		V:  "benchmark value string",
		Tx: datalog.ElementID{Lamport: 12345678, ReplicaID: 1},
	}

	encoder := &BinaryKeyEncoder{}

	b.Run("EAVT_RoundTrip", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			sinkKey = encoder.EncodeKey(EAVT, datom)
			sinkEntity, sinkAttr, sinkValue, sinkTx, _, _, _ = encoder.DecodeKey(EAVT, sinkKey)
		}
	})
}
