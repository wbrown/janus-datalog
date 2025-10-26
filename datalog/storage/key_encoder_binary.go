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
		// Entity + Attribute + Value + Tx
		return concatBytes(prefix, sd.E[:], sd.A[:], vBytes, sd.Tx[:])

	case AEVT:
		// Attribute + Entity + Value + Tx
		return concatBytes(prefix, sd.A[:], sd.E[:], vBytes, sd.Tx[:])

	case AVET:
		// Attribute + Value + Entity + Tx
		return concatBytes(prefix, sd.A[:], vBytes, sd.E[:], sd.Tx[:])

	case VAET:
		// Value + Attribute + Entity + Tx
		return concatBytes(prefix, vBytes, sd.A[:], sd.E[:], sd.Tx[:])

	case TAEV:
		// Tx + Attribute + Entity + Value
		return concatBytes(prefix, sd.Tx[:], sd.A[:], sd.E[:], vBytes)

	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from a binary index key
func (e *BinaryKeyEncoder) DecodeKey(index IndexType, key []byte) (entity, attr, value, tx []byte, err error) {
	if len(key) < 1 {
		return nil, nil, nil, nil, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	// Component sizes: Entity=20, Attribute=32, Tx=20
	const entitySize = 20
	const attrSize = 32
	const txSize = 20

	// Decode based on index type
	switch index {
	case EAVT:
		minSize := entitySize + attrSize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("EAVT key too short")
		}
		entity = key[0:entitySize]
		attr = key[entitySize : entitySize+attrSize]
		value = key[entitySize+attrSize : len(key)-txSize]
		tx = key[len(key)-txSize:]

	case AEVT:
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("AEVT key too short")
		}
		attr = key[0:attrSize]
		entity = key[attrSize : attrSize+entitySize]
		value = key[attrSize+entitySize : len(key)-txSize]
		tx = key[len(key)-txSize:]

	case AVET:
		// Value is variable length, so we work backwards
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("AVET key too short")
		}
		attr = key[0:attrSize]
		tx = key[len(key)-txSize:]
		entity = key[len(key)-txSize-entitySize : len(key)-txSize]
		value = key[attrSize : len(key)-txSize-entitySize]

	case VAET:
		// Value is at the beginning, variable length
		minSize := attrSize + entitySize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("VAET key too short")
		}
		tx = key[len(key)-txSize:]
		entity = key[len(key)-txSize-entitySize : len(key)-txSize]
		attr = key[len(key)-txSize-entitySize-attrSize : len(key)-txSize-entitySize]
		value = key[0 : len(key)-txSize-entitySize-attrSize]

	case TAEV:
		minSize := txSize + attrSize + entitySize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("TAEV key too short")
		}
		tx = key[0:txSize]
		attr = key[txSize : txSize+attrSize]
		entity = key[txSize+attrSize : txSize+attrSize+entitySize]
		value = key[txSize+attrSize+entitySize:]

	default:
		return nil, nil, nil, nil, fmt.Errorf("unknown index type: %v", index)
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

	// End key is start with last byte incremented
	end = make([]byte, len(start))
	copy(end, start)

	// Increment last byte, or append 0xFF if it would overflow
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			break
		}
		if i == 0 {
			// All bytes are 0xFF, append one more
			end = append(end, 0x00)
		}
	}

	return start, end
}
