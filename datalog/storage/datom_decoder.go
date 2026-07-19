package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// BlobReader exposes content-addressed Tier-3 values to the key decoder.
type BlobReader interface {
	ReadBlob(hash [20]byte, read func([]byte) error) error
}

// DatomFromKey reconstructs a datom from an index key.
// Pass nil when Tier-3 values are not expected.
func DatomFromKey(index IndexType, key []byte, encoder *BinaryKeyEncoder, blobs BlobReader) (datalog.Datom, error) {
	return decodeDatomFromKey(index, key, encoder, blobs)
}

func decodeDatomFromKey(index IndexType, key []byte, encoder *BinaryKeyEncoder, blobs BlobReader) (datalog.Datom, error) {
	// DecodeKey returns fixed-size arrays directly (no heap escape)
	entity, attr, vBytes, tx, op, afterRef, err := encoder.DecodeKey(index, key)
	if err != nil {
		return datalog.Datom{}, fmt.Errorf("failed to decode key: %w", err)
	}

	// Value (variable length) - first byte is type, rest is data
	if len(vBytes) < 1 {
		return datalog.Datom{}, fmt.Errorf("value bytes too short: %d", len(vBytes))
	}
	vType := datalog.ValueType(vBytes[0])
	vData := vBytes[1:]

	// Tier 3: hashed values need blob store lookup
	if vType == datalog.TypeHashedString || vType == datalog.TypeHashedBytes {
		if blobs == nil {
			return datalog.Datom{}, fmt.Errorf("hashed value requires database for blob lookup")
		}
		if len(vData) != 20 {
			return datalog.Datom{}, fmt.Errorf("hashed value hash must be 20 bytes, got %d", len(vData))
		}
		var hash [20]byte
		copy(hash[:], vData)
		var decompressed interface{}
		err := blobs.ReadBlob(hash, func(compressed []byte) error {
			var decodeErr error
			decompressed, decodeErr = datalog.ValueFromBytes(
				datalog.ValueType(vType-2), // TypeHashedString→TypeCompressedString, etc.
				compressed,
			)
			return decodeErr
		})
		if err != nil {
			return datalog.Datom{}, fmt.Errorf("failed to read blob: %w", err)
		}
		return datalog.Datom{
			E:        datalog.InternIdentityFromHash(entity),
			A:        datalog.InternKeywordFromBytes(attr),
			V:        decompressed,
			Tx:       Tx(tx).ToElementID(),
			Op:       datalog.CRDTOp(op),
			AfterRef: Tx(afterRef).ToElementID(),
		}, nil
	}

	v, err := datalog.ValueFromBytes(vType, vData)
	if err != nil {
		return datalog.Datom{}, fmt.Errorf("failed to decode value: %w", err)
	}
	if bytes, ok := v.([]byte); ok {
		v = append([]byte(nil), bytes...)
	}

	// E is reconstructed from the stored 20-byte hash only: entities are
	// content-addressed and the seed string is never persisted (see the
	// datalog.identity doc). String() renders the L85 of the hash; store the
	// name as an attribute if you need it back.
	return datalog.Datom{
		E:        datalog.InternIdentityFromHash(entity),
		A:        datalog.InternKeywordFromBytes(attr),
		V:        v,
		Tx:       Tx(tx).ToElementID(),
		Op:       datalog.CRDTOp(op),
		AfterRef: Tx(afterRef).ToElementID(),
	}, nil
}
