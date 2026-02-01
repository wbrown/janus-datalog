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
// Key formats (Bug #5 fix: Tx before Op, AfterRef at end for RGA ops):
//
//	EAVT: [prefix][E][A][type][value][Tx↓][Op][AfterRef?]  - groups by value for add-wins
//	EATV: [prefix][E][A][Tx↓][type][value][Op][AfterRef?]  - first entry is current
//	AEVT: [prefix][A][E][type][value][Tx↓][Op][AfterRef?]  - by attribute
//	AVET: [prefix][A][type][value][E][Tx↓][Op][AfterRef?]  - value lookup
//	VAET: [prefix][type][value][A][E][Tx↓][Op][AfterRef?]  - reverse refs
//	TAEV: [prefix][Tx↓][A][E][type][value][Op][AfterRef?]  - transaction log
//
// AfterRef? = 16 bytes present only if Op ∈ {OpRGAInsert(3), OpRGATombstone(4)}
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
		// [E][A][V][Tx][Op][AfterRef?] - groups by value, Tx before Op for Bug #5 fix
		key := concatBytes(prefix, sd.E[:], sd.A[:], vBytes, txDesc, opByte)
		if sd.Op.HasAfterRef() {
			afterRefDesc := txToDescending(sd.AfterRef)
			key = append(key, afterRefDesc...)
		}
		return key
	case EATV:
		// [E][A][Tx][V][Op][AfterRef?] - for cardinality-one: first entry is current
		key := concatBytes(prefix, sd.E[:], sd.A[:], txDesc, vBytes, opByte)
		if sd.Op.HasAfterRef() {
			afterRefDesc := txToDescending(sd.AfterRef)
			key = append(key, afterRefDesc...)
		}
		return key
	case AEVT:
		// [A][E][V][Tx][Op][AfterRef?] - Bug #5 fix: Tx before Op
		key := concatBytes(prefix, sd.A[:], sd.E[:], vBytes, txDesc, opByte)
		if sd.Op.HasAfterRef() {
			afterRefDesc := txToDescending(sd.AfterRef)
			key = append(key, afterRefDesc...)
		}
		return key
	case AVET:
		// [A][V][E][Tx][Op][AfterRef?] - Bug #5 fix: Tx before Op, enables [A][V] prefix scans
		key := concatBytes(prefix, sd.A[:], vBytes, sd.E[:], txDesc, opByte)
		if sd.Op.HasAfterRef() {
			afterRefDesc := txToDescending(sd.AfterRef)
			key = append(key, afterRefDesc...)
		}
		return key
	case VAET:
		// [V][A][E][Tx][Op][AfterRef?] - Bug #5 fix: Tx before Op, enables [V][A] prefix scans
		key := concatBytes(prefix, vBytes, sd.A[:], sd.E[:], txDesc, opByte)
		if sd.Op.HasAfterRef() {
			afterRefDesc := txToDescending(sd.AfterRef)
			key = append(key, afterRefDesc...)
		}
		return key
	case TAEV:
		// [Tx][A][E][V][Op][AfterRef?]
		key := concatBytes(prefix, txDesc, sd.A[:], sd.E[:], vBytes, opByte)
		if sd.Op.HasAfterRef() {
			afterRefDesc := txToDescending(sd.AfterRef)
			key = append(key, afterRefDesc...)
		}
		return key
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from a binary index key.
// Returns fixed-size arrays for entity, attr, tx, and op to avoid heap escape.
// tx is 16 bytes: Lamport (8) + ReplicaID (8) = ElementID
// Tx is stored with bitwise NOT for descending sort, reversed on decode.
// Op is 1 byte: 0-4 (see CRDTOp constants).
// AfterRef is optionally present for Op ∈ {OpRGAInsert(3), OpRGATombstone(4)}.
//
// Key formats (Bug #5 fix: Tx before Op, AfterRef at end for RGA ops):
//
//	EAVT: [prefix][E][A][type][value][Tx↓][Op][AfterRef?]
//	EATV: [prefix][E][A][Tx↓][type][value][Op][AfterRef?]
//	AEVT: [prefix][A][E][type][value][Tx↓][Op][AfterRef?]
//	AVET: [prefix][A][type][value][E][Tx↓][Op][AfterRef?]
//	VAET: [prefix][type][value][A][E][Tx↓][Op][AfterRef?]
//	TAEV: [prefix][Tx↓][A][E][type][value][Op][AfterRef?]
func (e *BinaryKeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op byte, afterRef [16]byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	// Component sizes
	const entitySize = 20
	const attrSize = 32
	const txSize = 16 // ElementID: Lamport (8) + ReplicaID (8)
	const opSize = 1

	const afterRefSize = 16

	switch index {
	case EAVT:
		// [E][A][V][Tx][Op][AfterRef?] - Bug #5 fix: Tx before Op
		minSize := entitySize + attrSize + txSize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("EAVT key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		// Check if AfterRef is present
		hasAfterRef := len(key) >= minSize+afterRefSize
		if hasAfterRef {
			op = key[len(key)-afterRefSize-opSize]
			if datalog.CRDTOp(op).HasAfterRef() {
				vEnd := len(key) - afterRefSize - opSize - txSize
				value = key[entitySize+attrSize : vEnd]
				tx = txFromDescending(key[vEnd : vEnd+txSize])
				afterRef = txFromDescending(key[len(key)-afterRefSize:])
			} else {
				hasAfterRef = false
			}
		}
		if !hasAfterRef {
			op = key[len(key)-opSize]
			vEnd := len(key) - opSize - txSize
			value = key[entitySize+attrSize : vEnd]
			tx = txFromDescending(key[vEnd : vEnd+txSize])
		}

	case EATV:
		// [E][A][Tx][V][Op][AfterRef?]
		minSize := entitySize + attrSize + txSize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("EATV key too short")
		}
		copy(entity[:], key[0:entitySize])
		copy(attr[:], key[entitySize:entitySize+attrSize])
		tx = txFromDescending(key[entitySize+attrSize : entitySize+attrSize+txSize])
		// Check if AfterRef is present
		hasAfterRef := len(key) >= minSize+afterRefSize
		if hasAfterRef {
			op = key[len(key)-afterRefSize-opSize]
			if datalog.CRDTOp(op).HasAfterRef() {
				vStart := entitySize + attrSize + txSize
				value = key[vStart : len(key)-afterRefSize-opSize]
				afterRef = txFromDescending(key[len(key)-afterRefSize:])
			} else {
				hasAfterRef = false
			}
		}
		if !hasAfterRef {
			vStart := entitySize + attrSize + txSize
			value = key[vStart : len(key)-opSize]
			op = key[len(key)-opSize]
		}

	case AEVT:
		// [A][E][V][Tx][Op][AfterRef?] - Bug #5 fix: Tx before Op
		minSize := attrSize + entitySize + txSize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AEVT key too short")
		}
		copy(attr[:], key[0:attrSize])
		copy(entity[:], key[attrSize:attrSize+entitySize])
		// Check if AfterRef is present
		hasAfterRef := len(key) >= minSize+afterRefSize
		if hasAfterRef {
			op = key[len(key)-afterRefSize-opSize]
			if datalog.CRDTOp(op).HasAfterRef() {
				vEnd := len(key) - afterRefSize - opSize - txSize
				value = key[attrSize+entitySize : vEnd]
				tx = txFromDescending(key[vEnd : vEnd+txSize])
				afterRef = txFromDescending(key[len(key)-afterRefSize:])
			} else {
				hasAfterRef = false
			}
		}
		if !hasAfterRef {
			op = key[len(key)-opSize]
			vEnd := len(key) - opSize - txSize
			value = key[attrSize+entitySize : vEnd]
			tx = txFromDescending(key[vEnd : vEnd+txSize])
		}

	case AVET:
		// [A][V][E][Tx][Op][AfterRef?] - Bug #5 fix: Tx before Op
		minSize := attrSize + entitySize + txSize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AVET key too short")
		}
		copy(attr[:], key[0:attrSize])
		// Check if AfterRef is present
		hasAfterRef := len(key) >= minSize+afterRefSize
		if hasAfterRef {
			op = key[len(key)-afterRefSize-opSize]
			if datalog.CRDTOp(op).HasAfterRef() {
				eStart := len(key) - afterRefSize - opSize - txSize - entitySize
				copy(entity[:], key[eStart:eStart+entitySize])
				tx = txFromDescending(key[eStart+entitySize : eStart+entitySize+txSize])
				value = key[attrSize:eStart]
				afterRef = txFromDescending(key[len(key)-afterRefSize:])
			} else {
				hasAfterRef = false
			}
		}
		if !hasAfterRef {
			op = key[len(key)-opSize]
			eStart := len(key) - opSize - txSize - entitySize
			copy(entity[:], key[eStart:eStart+entitySize])
			tx = txFromDescending(key[eStart+entitySize : eStart+entitySize+txSize])
			value = key[attrSize:eStart]
		}

	case VAET:
		// [V][A][E][Tx][Op][AfterRef?] - Bug #5 fix: Tx before Op
		minSize := attrSize + entitySize + txSize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("VAET key too short")
		}
		// Check if AfterRef is present
		hasAfterRef := len(key) >= minSize+afterRefSize
		if hasAfterRef {
			op = key[len(key)-afterRefSize-opSize]
			if datalog.CRDTOp(op).HasAfterRef() {
				eStart := len(key) - afterRefSize - opSize - txSize - entitySize
				aStart := eStart - attrSize
				copy(entity[:], key[eStart:eStart+entitySize])
				copy(attr[:], key[aStart:aStart+attrSize])
				tx = txFromDescending(key[eStart+entitySize : eStart+entitySize+txSize])
				value = key[0:aStart]
				afterRef = txFromDescending(key[len(key)-afterRefSize:])
			} else {
				hasAfterRef = false
			}
		}
		if !hasAfterRef {
			op = key[len(key)-opSize]
			eStart := len(key) - opSize - txSize - entitySize
			aStart := eStart - attrSize
			copy(entity[:], key[eStart:eStart+entitySize])
			copy(attr[:], key[aStart:aStart+attrSize])
			tx = txFromDescending(key[eStart+entitySize : eStart+entitySize+txSize])
			value = key[0:aStart]
		}

	case TAEV:
		// [Tx][A][E][V][Op][AfterRef?]
		minSize := txSize + attrSize + entitySize + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("TAEV key too short")
		}
		tx = txFromDescending(key[0:txSize])
		copy(attr[:], key[txSize:txSize+attrSize])
		copy(entity[:], key[txSize+attrSize:txSize+attrSize+entitySize])
		// Check if AfterRef is present
		hasAfterRef := len(key) >= minSize+afterRefSize
		if hasAfterRef {
			op = key[len(key)-afterRefSize-opSize]
			if datalog.CRDTOp(op).HasAfterRef() {
				value = key[txSize+attrSize+entitySize : len(key)-afterRefSize-opSize]
				afterRef = txFromDescending(key[len(key)-afterRefSize:])
			} else {
				hasAfterRef = false
			}
		}
		if !hasAfterRef {
			value = key[txSize+attrSize+entitySize : len(key)-opSize]
			op = key[len(key)-opSize]
		}

	default:
		return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, op, afterRef, nil
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
