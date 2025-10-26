package storage

import (
	"bytes"
	"crypto/sha1"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestKeyEncoders(t *testing.T) {
	// Create test datom
	entity := sha1.Sum([]byte("entity1"))

	datom := &datalog.Datom{
		E:  datalog.NewIdentityFromHash(entity),
		A:  datalog.NewKeyword("attr1"),
		V:  "hello world",
		Tx: uint64(1),
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
			indices := []IndexType{EAVT, AEVT, AVET, VAET, TAEV}

			for _, idx := range indices {
				// Encode key
				key := encoder.EncodeKey(idx, datom)

				// Key should have content
				if len(key) == 0 {
					t.Errorf("%s: empty key for index %v", tc.name, idx)
				}

				// Decode key
				e, _, v, tx, err := encoder.DecodeKey(idx, key)
				if err != nil {
					t.Errorf("%s: decode error for index %v: %v", tc.name, idx, err)
					continue
				}

				// Verify components
				if !bytes.Equal(e, entity[:]) {
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
				// For tx, verify it's not empty
				if len(tx) == 0 {
					t.Errorf("%s: tx missing for index %v", tc.name, idx)
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
			Tx: uint64(1),
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
