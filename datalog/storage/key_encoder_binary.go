package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// BinaryKeyEncoder implements KeyEncoder using raw binary for space efficiency
type BinaryKeyEncoder struct{}

// txToDescending applies bitwise NOT to Tx bytes for descending sort order.
// This ensures highest ElementID sorts first in forward scans, enabling O(1)
// current value lookup (first entry = highest Tx = current value).
func txToDescending(tx [16]byte) []byte {
	result := make([]byte, 16)
	for i := 0; i < 16; i++ {
		result[i] = ^tx[i]
	}
	return result
}

// txFromDescending reverses bitwise NOT to recover original Tx bytes.
func txFromDescending(encoded []byte) [16]byte {
	var tx [16]byte
	for i := 0; i < 16 && i < len(encoded); i++ {
		tx[i] = ^encoded[i]
	}
	return tx
}

// EncodeKey creates a binary index key from a datom
// Tx is encoded with bitwise NOT for descending sort order (highest Tx first).
func (e *BinaryKeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	// Convert to storage datom first
	sd := ToStorageDatom(*d)

	// Each index has a 1-byte prefix to separate namespaces
	prefix := []byte{byte(index)}

	// Get value bytes with type prefix (1 byte type + variable length data)
	vType := byte(datalog.Type(sd.V))
	vData := datalog.ValueBytes(sd.V)
	vBytes := append([]byte{vType}, vData...)

	// Encode Tx with bitwise NOT for descending sort order
	txDesc := txToDescending(sd.Tx)

	// Build key based on index type using raw bytes
	switch index {
	case EAVT:
		return concatBytes(prefix, sd.E[:], sd.A[:], vBytes, txDesc)
	case EATV:
		// EATV: E → A → Tx → V (for cardinality-one: first entry is current)
		return concatBytes(prefix, sd.E[:], sd.A[:], txDesc, vBytes)
	case AEVT:
		return concatBytes(prefix, sd.A[:], sd.E[:], vBytes, txDesc)
	case AVET:
		return concatBytes(prefix, sd.A[:], vBytes, sd.E[:], txDesc)
	case VAET:
		return concatBytes(prefix, vBytes, sd.A[:], sd.E[:], txDesc)
	case TAEV:
		return concatBytes(prefix, txDesc, sd.A[:], sd.E[:], vBytes)
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from a binary index key.
// Returns fixed-size arrays for entity, attr, tx to avoid heap escape.
// tx is 16 bytes: Lamport (8) + ReplicaID (8) = ElementID
// Tx is stored with bitwise NOT for descending sort, reversed on decode.
func (e *BinaryKeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	// Component sizes
	const entitySize = 20
	const attrSize = 32
	const txSize = 16 // ElementID: Lamport (8) + ReplicaID (8)

	switch index {
	case EAVT:
		minSize := entitySize + attrSize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("EAVT key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		value = key[entitySize+attrSize : len(key)-txSize]
		tx = txFromDescending(key[len(key)-txSize:])

	case EATV:
		// EATV: E → A → Tx → V
		minSize := entitySize + attrSize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("EATV key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		tx = txFromDescending(key[entitySize+attrSize : entitySize+attrSize+txSize])
		value = key[entitySize+attrSize+txSize:]

	case AEVT:
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("AEVT key too short")
		}
		copy(attr[:], key[0:attrSize])
		copy(entity[:], key[attrSize:attrSize+entitySize])
		value = key[attrSize+entitySize : len(key)-txSize]
		tx = txFromDescending(key[len(key)-txSize:])

	case AVET:
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("AVET key too short")
		}
		copy(attr[:], key[0:attrSize])
		tx = txFromDescending(key[len(key)-txSize:])
		copy(entity[:], key[len(key)-txSize-entitySize:len(key)-txSize])
		value = key[attrSize : len(key)-txSize-entitySize]

	case VAET:
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("VAET key too short")
		}
		tx = txFromDescending(key[len(key)-txSize:])
		copy(entity[:], key[len(key)-txSize-entitySize:len(key)-txSize])
		copy(attr[:], key[len(key)-txSize-entitySize-attrSize:len(key)-txSize-entitySize])
		value = key[0 : len(key)-txSize-entitySize-attrSize]

	case TAEV:
		minSize := txSize + attrSize + entitySize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("TAEV key too short")
		}
		tx = txFromDescending(key[0:txSize])
		copy(attr[:], key[txSize:txSize+attrSize])
		copy(entity[:], key[txSize+attrSize:txSize+attrSize+entitySize])
		value = key[txSize+attrSize+entitySize:]

	default:
		return entity, attr, nil, tx, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, nil
}

// EncodePrefix creates a binary prefix key for range scans
func (e *BinaryKeyEncoder) EncodePrefix(index IndexType, parts ...[]byte) []byte {
	prefix := []byte{byte(index)}
	allParts := append([][]byte{prefix}, parts...)
	return concatBytes(allParts...)
}

// EncodeTxForPrefix encodes a Tx with bitwise NOT for use in prefix keys.
// Use this when constructing scan ranges involving Tx (e.g., TAEV time-range queries).
// Note: With bitwise NOT, higher Tx values encode to lower byte values,
// so for a time range [low, high], the scan should be from encoded(high) to encoded(low).
func (e *BinaryKeyEncoder) EncodeTxForPrefix(tx Tx) []byte {
	return txToDescending(tx)
}

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *BinaryKeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
	start = e.EncodePrefix(index, parts...)
	end = incrementLastByte(start)
	return start, end
}

// EncodeHistoryKey creates a binary index key with Op appended for history indices
// DEPRECATED: History indices are being removed in favor of CRDT semantics
func (e *BinaryKeyEncoder) EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte {
	sd := ToStorageDatom(*d)
	prefix := []byte{byte(index)}

	vType := byte(datalog.Type(sd.V))
	vData := datalog.ValueBytes(sd.V)
	vBytes := append([]byte{vType}, vData...)

	opByte := byte(0x00)
	if op {
		opByte = 0x01
	}

	// Use bitwise NOT for descending Tx order
	txDesc := txToDescending(sd.Tx)

	baseIndex := historyIndexToBase(index)
	switch baseIndex {
	case EAVT:
		return concatBytes(prefix, sd.E[:], sd.A[:], vBytes, txDesc, []byte{opByte})
	case EATV:
		return concatBytes(prefix, sd.E[:], sd.A[:], txDesc, vBytes, []byte{opByte})
	case AEVT:
		return concatBytes(prefix, sd.A[:], sd.E[:], vBytes, txDesc, []byte{opByte})
	case AVET:
		return concatBytes(prefix, sd.A[:], vBytes, sd.E[:], txDesc, []byte{opByte})
	case VAET:
		return concatBytes(prefix, vBytes, sd.A[:], sd.E[:], txDesc, []byte{opByte})
	case TAEV:
		return concatBytes(prefix, txDesc, sd.A[:], sd.E[:], vBytes, []byte{opByte})
	default:
		panic(fmt.Sprintf("unknown history index type: %v", index))
	}
}

// DecodeHistoryKey extracts components including Op from a binary history index key
// tx is 16 bytes: Lamport (8) + ReplicaID (8) = ElementID
func (e *BinaryKeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op Op, err error) {
	if len(key) < 2 {
		return entity, attr, nil, tx, false, fmt.Errorf("history key too short")
	}

	opByte := key[len(key)-1]
	op = opByte == 0x01

	keyWithoutOp := key[:len(key)-1]
	baseIndex := historyIndexToBase(index)
	entity, attr, value, tx, err = e.DecodeKey(baseIndex, keyWithoutOp)
	return entity, attr, value, tx, op, err
}
