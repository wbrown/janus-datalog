package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// BinaryKeyEncoder implements KeyEncoder using raw binary for space efficiency
type BinaryKeyEncoder struct{}

// EncodeKey creates a binary index key from a datom
func (e *BinaryKeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	// Convert to storage datom first
	sd := ToStorageDatom(*d)

	// Each index has a 1-byte prefix to separate namespaces
	prefix := []byte{byte(index)}

	// Get value bytes with type prefix (1 byte type + variable length data)
	vType := byte(datalog.Type(sd.V))
	vData := datalog.ValueBytes(sd.V)
	vBytes := append([]byte{vType}, vData...)

	// Build key based on index type using raw bytes
	switch index {
	case EAVT:
		return concatBytes(prefix, sd.E[:], sd.A[:], vBytes, sd.Tx[:])
	case AEVT:
		return concatBytes(prefix, sd.A[:], sd.E[:], vBytes, sd.Tx[:])
	case AVET:
		return concatBytes(prefix, sd.A[:], vBytes, sd.E[:], sd.Tx[:])
	case VAET:
		return concatBytes(prefix, vBytes, sd.A[:], sd.E[:], sd.Tx[:])
	case TAEV:
		return concatBytes(prefix, sd.Tx[:], sd.A[:], sd.E[:], vBytes)
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from a binary index key.
// Returns fixed-size arrays for entity, attr, tx to avoid heap escape.
func (e *BinaryKeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [20]byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	// Component sizes
	const entitySize = 20
	const attrSize = 32
	const txSize = 20

	switch index {
	case EAVT:
		minSize := entitySize + attrSize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("EAVT key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		value = key[entitySize+attrSize : len(key)-txSize]
		copy(tx[:], key[len(key)-txSize:])

	case AEVT:
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("AEVT key too short")
		}
		copy(attr[:], key[0:attrSize])
		copy(entity[:], key[attrSize:attrSize+entitySize])
		value = key[attrSize+entitySize : len(key)-txSize]
		copy(tx[:], key[len(key)-txSize:])

	case AVET:
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("AVET key too short")
		}
		copy(attr[:], key[0:attrSize])
		copy(tx[:], key[len(key)-txSize:])
		copy(entity[:], key[len(key)-txSize-entitySize:len(key)-txSize])
		value = key[attrSize : len(key)-txSize-entitySize]

	case VAET:
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("VAET key too short")
		}
		copy(tx[:], key[len(key)-txSize:])
		copy(entity[:], key[len(key)-txSize-entitySize:len(key)-txSize])
		copy(attr[:], key[len(key)-txSize-entitySize-attrSize:len(key)-txSize-entitySize])
		value = key[0 : len(key)-txSize-entitySize-attrSize]

	case TAEV:
		minSize := txSize + attrSize + entitySize
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("TAEV key too short")
		}
		copy(tx[:], key[0:txSize])
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

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *BinaryKeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
	start = e.EncodePrefix(index, parts...)
	end = incrementLastByte(start)
	return start, end
}

// EncodeHistoryKey creates a binary index key with Op appended for history indices
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

	baseIndex := historyIndexToBase(index)
	switch baseIndex {
	case EAVT:
		return concatBytes(prefix, sd.E[:], sd.A[:], vBytes, sd.Tx[:], []byte{opByte})
	case AEVT:
		return concatBytes(prefix, sd.A[:], sd.E[:], vBytes, sd.Tx[:], []byte{opByte})
	case AVET:
		return concatBytes(prefix, sd.A[:], vBytes, sd.E[:], sd.Tx[:], []byte{opByte})
	case VAET:
		return concatBytes(prefix, vBytes, sd.A[:], sd.E[:], sd.Tx[:], []byte{opByte})
	case TAEV:
		return concatBytes(prefix, sd.Tx[:], sd.A[:], sd.E[:], vBytes, []byte{opByte})
	default:
		panic(fmt.Sprintf("unknown history index type: %v", index))
	}
}

// DecodeHistoryKey extracts components including Op from a binary history index key
func (e *BinaryKeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [20]byte, op Op, err error) {
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
