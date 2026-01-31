package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// L85KeyEncoder implements KeyEncoder using L85 encoding for human-readable keys
type L85KeyEncoder struct{}

// EncodeKey creates an L85-encoded index key from a datom
// Op is included between V and Tx for CRDT semantics.
func (e *L85KeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	sd := ToStorageDatom(*d)
	prefix := []byte{byte(index)}

	// Encode components to L85
	eL85 := codec.EncodeFixed20(sd.E)
	aL85 := codec.EncodeFixed32(sd.A)
	txL85 := codec.EncodeFixed16(sd.Tx)

	// Get value bytes with type prefix
	vType := byte(datalog.Type(sd.V))
	var vBytes []byte
	if datalog.Type(sd.V) == datalog.TypeReference {
		var vArr [20]byte
		copy(vArr[:], datalog.ValueBytes(sd.V))
		vBytes = append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
	} else {
		vData := datalog.ValueBytes(sd.V)
		vBytes = append([]byte{vType}, vData...)
	}

	// Op byte (0=none, 1=add, 2=remove)
	opByte := []byte{byte(sd.Op)}

	switch index {
	case EAVT:
		// [E][A][V][Op][Tx]
		return concatBytes(prefix, []byte(eL85), []byte(aL85), vBytes, opByte, []byte(txL85))
	case EATV:
		// [E][A][Tx][V][Op]
		return concatBytes(prefix, []byte(eL85), []byte(aL85), []byte(txL85), vBytes, opByte)
	case AEVT:
		// [A][E][V][Op][Tx]
		return concatBytes(prefix, []byte(aL85), []byte(eL85), vBytes, opByte, []byte(txL85))
	case AVET:
		// [A][V][E][Op][Tx] - Op before Tx for add-wins, enables [A][V] prefix scans
		return concatBytes(prefix, []byte(aL85), vBytes, []byte(eL85), opByte, []byte(txL85))
	case VAET:
		// [V][A][E][Op][Tx] - Op before Tx for add-wins, enables [V][A] prefix scans
		return concatBytes(prefix, vBytes, []byte(aL85), []byte(eL85), opByte, []byte(txL85))
	case TAEV:
		// [Tx][A][E][V][Op]
		return concatBytes(prefix, []byte(txL85), []byte(aL85), []byte(eL85), vBytes, opByte)
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from an L85-encoded index key.
// Returns fixed-size arrays for entity, attr, tx, and op to avoid heap escape.
// Op is 1 byte: 0=none, 1=add, 2=remove (for CRDT semantics).
//
// Key formats (Op before Tx for add-wins indices, Op at end for Tx-primary indices):
//   EAVT: [prefix][E][A][V][Op][Tx]       - add-wins: Op before Tx
//   EATV: [prefix][E][A][Tx][V][Op]       - cardinality-one: Tx primary, Op at end
//   AEVT: [prefix][A][E][V][Op][Tx]       - add-wins: Op before Tx
//   AVET: [prefix][A][V][E][Op][Tx]       - add-wins: Op before Tx, enables [A][V] prefix
//   VAET: [prefix][V][A][E][Op][Tx]       - add-wins: Op before Tx, enables [V][A] prefix
//   TAEV: [prefix][Tx][A][E][V][Op]       - transaction log: Tx primary, Op at end
func (e *L85KeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, 0, fmt.Errorf("key too short")
	}

	key = key[1:]

	const l85Size20 = 25   // 20-byte components (entity)
	const l85Size16 = 20   // 16-byte components (tx = ElementID)
	const l85SizeAttr = 40 // 32-byte components (attr)
	const opSize = 1

	switch index {
	case EAVT:
		// [E][A][V][Op][Tx]
		minSize := l85Size20 + l85SizeAttr + opSize + l85Size16
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("EAVT key too short")
		}
		entity, _ = codec.DecodeFixed20(string(key[0:l85Size20]))
		attr, _ = codec.DecodeFixed32(string(key[l85Size20 : l85Size20+l85SizeAttr]))
		// V is between A and Op
		vEnd := len(key) - l85Size16 - opSize
		valueBytes := key[l85Size20+l85SizeAttr : vEnd]
		if len(valueBytes) == l85Size20 {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		op = key[vEnd]
		tx, _ = codec.DecodeFixed16(string(key[len(key)-l85Size16:]))

	case EATV:
		// [E][A][Tx][V][Op]
		minSize := l85Size20 + l85SizeAttr + l85Size16 + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("EATV key too short")
		}
		entity, _ = codec.DecodeFixed20(string(key[0:l85Size20]))
		attr, _ = codec.DecodeFixed32(string(key[l85Size20 : l85Size20+l85SizeAttr]))
		tx, _ = codec.DecodeFixed16(string(key[l85Size20+l85SizeAttr : l85Size20+l85SizeAttr+l85Size16]))
		// V is between Tx and Op
		vStart := l85Size20 + l85SizeAttr + l85Size16
		valueBytes := key[vStart : len(key)-opSize]
		if len(valueBytes) == l85Size20 {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		op = key[len(key)-opSize]

	case AEVT:
		// [A][E][V][Op][Tx]
		minSize := l85SizeAttr + l85Size20 + opSize + l85Size16
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("AEVT key too short")
		}
		attr, _ = codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		entity, _ = codec.DecodeFixed20(string(key[l85SizeAttr : l85SizeAttr+l85Size20]))
		vEnd := len(key) - l85Size16 - opSize
		valueBytes := key[l85SizeAttr+l85Size20 : vEnd]
		if len(valueBytes) == l85Size20 {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		op = key[vEnd]
		tx, _ = codec.DecodeFixed16(string(key[len(key)-l85Size16:]))

	case AVET:
		// [A][V][E][Op][Tx] - Op before Tx
		if len(key) < l85SizeAttr+l85Size20+opSize+l85Size16 {
			return entity, attr, nil, tx, 0, fmt.Errorf("AVET key too short")
		}
		attr, _ = codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		tx, _ = codec.DecodeFixed16(string(key[len(key)-l85Size16:]))
		op = key[len(key)-l85Size16-opSize]
		entity, _ = codec.DecodeFixed20(string(key[len(key)-l85Size16-opSize-l85Size20 : len(key)-l85Size16-opSize]))
		// V is between A and E
		valueBytes := key[l85SizeAttr : len(key)-l85Size16-opSize-l85Size20]
		if len(valueBytes) == l85Size20+1 && valueBytes[0] == byte(datalog.TypeReference) {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes[1:])); decErr == nil {
				value = append([]byte{valueBytes[0]}, decoded[:]...)
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case VAET:
		// [V][A][E][Op][Tx] - Op before Tx
		if len(key) < l85SizeAttr+l85Size20+opSize+l85Size16 {
			return entity, attr, nil, tx, 0, fmt.Errorf("VAET key too short")
		}
		tx, _ = codec.DecodeFixed16(string(key[len(key)-l85Size16:]))
		op = key[len(key)-l85Size16-opSize]
		entity, _ = codec.DecodeFixed20(string(key[len(key)-l85Size16-opSize-l85Size20 : len(key)-l85Size16-opSize]))
		aStart := len(key) - l85Size16 - opSize - l85Size20 - l85SizeAttr
		attr, _ = codec.DecodeFixed32(string(key[aStart : aStart+l85SizeAttr]))
		// V is at start, before A
		valueBytes := key[0:aStart]
		if len(valueBytes) == l85Size20+1 && valueBytes[0] == byte(datalog.TypeReference) {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes[1:])); decErr == nil {
				value = append([]byte{valueBytes[0]}, decoded[:]...)
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case TAEV:
		// [Tx][A][E][V][Op]
		if len(key) < l85Size16+l85SizeAttr+l85Size20+opSize {
			return entity, attr, nil, tx, 0, fmt.Errorf("TAEV key too short")
		}
		tx, _ = codec.DecodeFixed16(string(key[0:l85Size16]))
		attr, _ = codec.DecodeFixed32(string(key[l85Size16 : l85Size16+l85SizeAttr]))
		entity, _ = codec.DecodeFixed20(string(key[l85Size16+l85SizeAttr : l85Size16+l85SizeAttr+l85Size20]))
		valueBytes := key[l85Size16+l85SizeAttr+l85Size20 : len(key)-opSize]
		if len(valueBytes) == l85Size20 {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		op = key[len(key)-opSize]

	default:
		return entity, attr, nil, tx, 0, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, op, nil
}

// EncodePrefix creates an L85-encoded prefix key for range scans
func (e *L85KeyEncoder) EncodePrefix(index IndexType, parts ...[]byte) []byte {
	prefix := []byte{byte(index)}
	encoded := make([][]byte, len(parts)+1)
	encoded[0] = prefix

	for i, part := range parts {
		shouldEncode := false
		isValuePosition := false

		switch index {
		case EAVT:
			shouldEncode = (i == 0 || i == 1 || i == 3)
			isValuePosition = (i == 2)
		case AEVT:
			shouldEncode = (i == 0 || i == 1 || i == 3)
			isValuePosition = (i == 2)
		case AVET:
			shouldEncode = (i == 0 || i == 2 || i == 3)
			isValuePosition = (i == 1)
		case VAET:
			shouldEncode = (i >= 1)
			isValuePosition = (i == 0)
		case TAEV:
			shouldEncode = (i <= 2)
			isValuePosition = (i == 3)
		}

		if shouldEncode && len(part) == 20 {
			var arr [20]byte
			copy(arr[:], part)
			encoded[i+1] = []byte(codec.EncodeFixed20(arr))
		} else if shouldEncode && len(part) == 32 {
			var arr [32]byte
			copy(arr[:], part)
			encoded[i+1] = []byte(codec.EncodeFixed32(arr))
		} else if isValuePosition && len(part) == 20 {
			var arr [20]byte
			copy(arr[:], part)
			encoded[i+1] = []byte(codec.EncodeFixed20(arr))
		} else {
			encoded[i+1] = part
		}
	}

	return concatBytes(encoded...)
}

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *L85KeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
	start = e.EncodePrefix(index, parts...)
	end = incrementLastByte(start)
	return start, end
}

// EncodeTxForPrefix encodes a Tx with bitwise NOT for use in prefix keys.
// L85 encoder uses the same bitwise NOT logic as binary encoder for consistency.
func (e *L85KeyEncoder) EncodeTxForPrefix(tx Tx) []byte {
	return txToDescending(tx)
}

// EncodeHistoryKey creates an L85-encoded index key with Op appended for history indices
func (e *L85KeyEncoder) EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte {
	sd := ToStorageDatom(*d)
	prefix := []byte{byte(index)}

	eL85 := codec.EncodeFixed20(sd.E)
	aL85 := codec.EncodeFixed32(sd.A)
	txL85 := codec.EncodeFixed16(sd.Tx)

	vType := byte(datalog.Type(sd.V))
	var vBytes []byte
	if datalog.Type(sd.V) == datalog.TypeReference {
		var vArr [20]byte
		copy(vArr[:], datalog.ValueBytes(sd.V))
		vBytes = append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
	} else {
		vData := datalog.ValueBytes(sd.V)
		vBytes = append([]byte{vType}, vData...)
	}

	opByte := byte(0x00)
	if op {
		opByte = 0x01
	}

	baseIndex := historyIndexToBase(index)
	switch baseIndex {
	case EAVT:
		return concatBytes(prefix, []byte(eL85), []byte(aL85), vBytes, []byte(txL85), []byte{opByte})
	case AEVT:
		return concatBytes(prefix, []byte(aL85), []byte(eL85), vBytes, []byte(txL85), []byte{opByte})
	case AVET:
		return concatBytes(prefix, []byte(aL85), vBytes, []byte(eL85), []byte(txL85), []byte{opByte})
	case VAET:
		return concatBytes(prefix, vBytes, []byte(aL85), []byte(eL85), []byte(txL85), []byte{opByte})
	case TAEV:
		return concatBytes(prefix, []byte(txL85), []byte(aL85), []byte(eL85), vBytes, []byte{opByte})
	default:
		panic(fmt.Sprintf("unknown history index type: %v", index))
	}
}

// DecodeHistoryKey extracts components including Op from an L85-encoded history index key
// DEPRECATED: History indices are being removed in favor of CRDT semantics
func (e *L85KeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op Op, err error) {
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
