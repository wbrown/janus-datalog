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
// Op is included between V and Tx to support CRDT semantics (add-wins for cardinality-many).
//
// Key formats (Op included for all cardinalities, 0 = none for cardinality-one):
//   EAVT: [prefix][E][A][type][value][Op][Tx↓]  - groups by value for add-wins
//   EATV: [prefix][E][A][Tx↓][type][value][Op]  - first entry is current
//   AEVT: [prefix][A][E][type][value][Op][Tx↓]  - by attribute
//   AVET: [prefix][A][type][value][Op][E][Tx↓]  - value lookup (KEY FIX: now works!)
//   VAET: [prefix][type][value][Op][A][E][Tx↓]  - reverse refs
//   TAEV: [prefix][Tx↓][A][E][type][value][Op]  - transaction log
func (e *BinaryKeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	// Convert to storage datom first
	sd := ToStorageDatom(*d)

	// Each index has a 1-byte prefix to separate namespaces
	prefix := []byte{byte(index)}

	// Get value bytes with type prefix (1 byte type + variable length data)
	vType := byte(datalog.Type(sd.V))
	vData := datalog.ValueBytes(sd.V)
	vBytes := append([]byte{vType}, vData...)

	// Op byte (0=none, 1=add, 2=remove)
	opByte := []byte{byte(sd.Op)}

	// Encode Tx with bitwise NOT for descending sort order
	txDesc := txToDescending(sd.Tx)

	// Build key based on index type using raw bytes
	// Op is placed after V in all indices to preserve value grouping
	switch index {
	case EAVT:
		// [E][A][V][Op][Tx] - groups by value, then by Op, for add-wins resolution
		return concatBytes(prefix, sd.E[:], sd.A[:], vBytes, opByte, txDesc)
	case EATV:
		// [E][A][Tx][V][Op] - for cardinality-one: first entry is current
		return concatBytes(prefix, sd.E[:], sd.A[:], txDesc, vBytes, opByte)
	case AEVT:
		// [A][E][V][Op][Tx]
		return concatBytes(prefix, sd.A[:], sd.E[:], vBytes, opByte, txDesc)
	case AVET:
		// [A][V][E][Op][Tx] - Op before Tx for add-wins, enables [A][V] prefix scans
		return concatBytes(prefix, sd.A[:], vBytes, sd.E[:], opByte, txDesc)
	case VAET:
		// [V][A][E][Op][Tx] - Op before Tx for add-wins, enables [V][A] prefix scans
		return concatBytes(prefix, vBytes, sd.A[:], sd.E[:], opByte, txDesc)
	case TAEV:
		// [Tx][A][E][V][Op]
		return concatBytes(prefix, txDesc, sd.A[:], sd.E[:], vBytes, opByte)
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from a binary index key.
// Returns fixed-size arrays for entity, attr, tx, and op to avoid heap escape.
// tx is 16 bytes: Lamport (8) + ReplicaID (8) = ElementID
// Tx is stored with bitwise NOT for descending sort, reversed on decode.
// Op is 1 byte: 0=none, 1=add, 2=remove (for CRDT semantics).
//
// Key formats (Op before Tx for add-wins indices, Op at end for Tx-primary indices):
//   EAVT: [prefix][E][A][type][value][Op][Tx↓]     - add-wins: Op before Tx
//   EATV: [prefix][E][A][Tx↓][type][value][Op]     - cardinality-one: Tx primary, Op at end
//   AEVT: [prefix][A][E][type][value][Op][Tx↓]     - add-wins: Op before Tx
//   AVET: [prefix][A][type][value][E][Op][Tx↓]     - add-wins: Op before Tx, enables [A][V] prefix
//   VAET: [prefix][type][value][A][E][Op][Tx↓]     - add-wins: Op before Tx, enables [V][A] prefix
//   TAEV: [prefix][Tx↓][A][E][type][value][Op]     - transaction log: Tx primary, Op at end
func (e *BinaryKeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, 0, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	// Component sizes
	const entitySize = 20
	const attrSize = 32
	const txSize = 16 // ElementID: Lamport (8) + ReplicaID (8)
	const opSize = 1

	switch index {
	case EAVT:
		// [E][A][V][Op][Tx]
		minSize := entitySize + attrSize + opSize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("EAVT key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		// Value is between A and Op (V includes type byte)
		vEnd := len(key) - txSize - opSize
		value = key[entitySize+attrSize : vEnd]
		op = key[vEnd]
		tx = txFromDescending(key[len(key)-txSize:])

	case EATV:
		// [E][A][Tx][V][Op]
		minSize := entitySize + attrSize + txSize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("EATV key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		tx = txFromDescending(key[entitySize+attrSize : entitySize+attrSize+txSize])
		// Value is between Tx and Op
		vStart := entitySize + attrSize + txSize
		value = key[vStart : len(key)-opSize]
		op = key[len(key)-opSize]

	case AEVT:
		// [A][E][V][Op][Tx]
		minSize := attrSize + entitySize + opSize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("AEVT key too short")
		}
		copy(attr[:], key[0:attrSize])
		copy(entity[:], key[attrSize:attrSize+entitySize])
		vEnd := len(key) - txSize - opSize
		value = key[attrSize+entitySize : vEnd]
		op = key[vEnd]
		tx = txFromDescending(key[len(key)-txSize:])

	case AVET:
		// [A][V][E][Op][Tx] - Op before Tx
		minSize := attrSize + entitySize + opSize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("AVET key too short")
		}
		copy(attr[:], key[0:attrSize])
		tx = txFromDescending(key[len(key)-txSize:])
		op = key[len(key)-txSize-opSize]
		copy(entity[:], key[len(key)-txSize-opSize-entitySize:len(key)-txSize-opSize])
		// V is between A and E
		value = key[attrSize : len(key)-txSize-opSize-entitySize]

	case VAET:
		// [V][A][E][Op][Tx] - Op before Tx
		minSize := attrSize + entitySize + opSize + txSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("VAET key too short")
		}
		tx = txFromDescending(key[len(key)-txSize:])
		op = key[len(key)-txSize-opSize]
		copy(entity[:], key[len(key)-txSize-opSize-entitySize:len(key)-txSize-opSize])
		copy(attr[:], key[len(key)-txSize-opSize-entitySize-attrSize:len(key)-txSize-opSize-entitySize])
		// V is at start, before A
		value = key[0 : len(key)-txSize-opSize-entitySize-attrSize]

	case TAEV:
		// [Tx][A][E][V][Op]
		minSize := txSize + attrSize + entitySize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("TAEV key too short")
		}
		tx = txFromDescending(key[0:txSize])
		copy(attr[:], key[txSize:txSize+attrSize])
		copy(entity[:], key[txSize+attrSize:txSize+attrSize+entitySize])
		value = key[txSize+attrSize+entitySize : len(key)-opSize]
		op = key[len(key)-opSize]

	default:
		return entity, attr, nil, tx, 0, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, op, nil
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
// DEPRECATED: History indices are being removed in favor of CRDT semantics
func (e *BinaryKeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op Op, err error) {
	if len(key) < 2 {
		return entity, attr, nil, tx, false, fmt.Errorf("history key too short")
	}

	opByte := key[len(key)-1]
	op = opByte == 0x01

	keyWithoutOp := key[:len(key)-1]
	baseIndex := historyIndexToBase(index)
	entity, attr, value, tx, _, err = e.DecodeKey(baseIndex, keyWithoutOp)
	return entity, attr, value, tx, op, err
}
