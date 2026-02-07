package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// L85KeyEncoder implements KeyEncoder using L85 encoding for human-readable keys
type L85KeyEncoder struct{}

// EncodeKey creates an L85-encoded index key from a datom
// Op is always the LAST byte of every key, matching BinaryKeyEncoder convention.
// See docs/reference/OP_POSITION_PROOF.md for the correctness proof.
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

	// Op byte (0=none, 1=add, 2=remove, 3=rga-insert, 4=rga-tombstone)
	opByte := []byte{byte(sd.Op)}

	switch index {
	case EAVT:
		// [E][A][V][Tx][Op]
		return concatBytes(prefix, []byte(eL85), []byte(aL85), vBytes, []byte(txL85), opByte)
	case EATV:
		// [E][A][Tx][V][Op]
		return concatBytes(prefix, []byte(eL85), []byte(aL85), []byte(txL85), vBytes, opByte)
	case AEVT:
		// [A][E][V][Tx][Op]
		return concatBytes(prefix, []byte(aL85), []byte(eL85), vBytes, []byte(txL85), opByte)
	case AETV:
		// [A][E][Tx][V][Op]
		return concatBytes(prefix, []byte(aL85), []byte(eL85), []byte(txL85), vBytes, opByte)
	case AVET:
		// [A][V][E][Tx][Op]
		return concatBytes(prefix, []byte(aL85), vBytes, []byte(eL85), []byte(txL85), opByte)
	case VAET:
		// [V][A][E][Tx][Op]
		return concatBytes(prefix, vBytes, []byte(aL85), []byte(eL85), []byte(txL85), opByte)
	case TAEV:
		// [Tx][A][E][V][Op]
		return concatBytes(prefix, []byte(txL85), []byte(aL85), []byte(eL85), vBytes, opByte)
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from an L85-encoded index key.
// Returns fixed-size arrays for entity, attr, tx, afterRef to avoid heap escape.
// Op is 1 byte: 0=none, 1=add, 2=remove, 3=rga-insert, 4=rga-tombstone.
// Op is always the LAST byte of every key: op = key[len(key)-1].
// AfterRef is 16 bytes: present only when Op.HasAfterRef() is true, otherwise zero.
//
// Key formats (Op always last — see docs/reference/OP_POSITION_PROOF.md):
//
//	EAVT: [prefix][E][A][V][Tx][Op]
//	EATV: [prefix][E][A][Tx][V][Op]
//	AEVT: [prefix][A][E][V][Tx][Op]
//	AETV: [prefix][A][E][Tx][V][Op]
//	AVET: [prefix][A][V][E][Tx][Op]
//	VAET: [prefix][V][A][E][Tx][Op]
//	TAEV: [prefix][Tx][A][E][V][Op]
//
// NOTE: L85 encoder does NOT yet support AfterRef - returns zero. Update pending.
func (e *L85KeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [16]byte, op byte, afterRef [16]byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("key too short")
	}

	key = key[1:]

	const l85Size20 = 25   // 20-byte components (entity)
	const l85Size16 = 20   // 16-byte components (tx = ElementID)
	const l85SizeAttr = 40 // 32-byte components (attr)
	const opSize = 1

	// Op is always the last byte
	op = key[len(key)-opSize]

	switch index {
	case EAVT:
		// [E][A][V][Tx][Op]
		minSize := l85Size20 + l85SizeAttr + l85Size16 + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("EAVT key too short")
		}
		entity, _ = codec.DecodeFixed20(string(key[0:l85Size20]))
		attr, _ = codec.DecodeFixed32(string(key[l85Size20 : l85Size20+l85SizeAttr]))
		// Tx is before Op (last l85Size16 bytes before Op)
		txStart := len(key) - opSize - l85Size16
		tx, _ = codec.DecodeFixed16(string(key[txStart : txStart+l85Size16]))
		// V is between A and Tx
		valueBytes := key[l85Size20+l85SizeAttr : txStart]
		if len(valueBytes) == l85Size20 {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case EATV:
		// [E][A][Tx][V][Op]
		minSize := l85Size20 + l85SizeAttr + l85Size16 + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("EATV key too short")
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

	case AEVT:
		// [A][E][V][Tx][Op]
		minSize := l85SizeAttr + l85Size20 + l85Size16 + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AEVT key too short")
		}
		attr, _ = codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		entity, _ = codec.DecodeFixed20(string(key[l85SizeAttr : l85SizeAttr+l85Size20]))
		// Tx is before Op
		txStart := len(key) - opSize - l85Size16
		tx, _ = codec.DecodeFixed16(string(key[txStart : txStart+l85Size16]))
		// V is between E and Tx
		valueBytes := key[l85SizeAttr+l85Size20 : txStart]
		if len(valueBytes) == l85Size20 {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case AETV:
		// [A][E][Tx][V][Op]
		minSize := l85SizeAttr + l85Size20 + l85Size16 + opSize
		if len(key) < minSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AETV key too short")
		}
		attr, _ = codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		entity, _ = codec.DecodeFixed20(string(key[l85SizeAttr : l85SizeAttr+l85Size20]))
		tx, _ = codec.DecodeFixed16(string(key[l85SizeAttr+l85Size20 : l85SizeAttr+l85Size20+l85Size16]))
		// V is between Tx and Op
		vStart := l85SizeAttr + l85Size20 + l85Size16
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

	case AVET:
		// [A][V][E][Tx][Op]
		if len(key) < l85SizeAttr+l85Size20+l85Size16+opSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("AVET key too short")
		}
		attr, _ = codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		// E and Tx are at the end, before Op
		eStart := len(key) - opSize - l85Size16 - l85Size20
		entity, _ = codec.DecodeFixed20(string(key[eStart : eStart+l85Size20]))
		tx, _ = codec.DecodeFixed16(string(key[eStart+l85Size20 : eStart+l85Size20+l85Size16]))
		// V is between A and E
		valueBytes := key[l85SizeAttr:eStart]
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
		// [V][A][E][Tx][Op]
		if len(key) < l85SizeAttr+l85Size20+l85Size16+opSize {
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("VAET key too short")
		}
		// E and Tx are at the end, before Op
		eStart := len(key) - opSize - l85Size16 - l85Size20
		entity, _ = codec.DecodeFixed20(string(key[eStart : eStart+l85Size20]))
		tx, _ = codec.DecodeFixed16(string(key[eStart+l85Size20 : eStart+l85Size20+l85Size16]))
		// A is before E
		aStart := eStart - l85SizeAttr
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
			return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("TAEV key too short")
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

	default:
		return entity, attr, nil, tx, 0, afterRef, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, op, afterRef, nil
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
	result := txToDescending(tx)
	return result[:]
}
