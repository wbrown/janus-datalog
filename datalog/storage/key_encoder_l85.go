package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// L85KeyEncoder implements KeyEncoder using L85 encoding for human-readable keys
type L85KeyEncoder struct{}

// EncodeKey creates an L85-encoded index key from a datom
func (e *L85KeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	sd := ToStorageDatom(*d)
	prefix := []byte{byte(index)}

	// Encode components to L85
	eL85 := codec.EncodeFixed20(sd.E)
	aL85 := codec.EncodeFixed32(sd.A)
	txL85 := codec.EncodeFixed20(sd.Tx)

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

	switch index {
	case EAVT:
		return concatBytes(prefix, []byte(eL85), []byte(aL85), vBytes, []byte(txL85))
	case AEVT:
		return concatBytes(prefix, []byte(aL85), []byte(eL85), vBytes, []byte(txL85))
	case AVET:
		return concatBytes(prefix, []byte(aL85), vBytes, []byte(eL85), []byte(txL85))
	case VAET:
		return concatBytes(prefix, vBytes, []byte(aL85), []byte(eL85), []byte(txL85))
	case TAEV:
		return concatBytes(prefix, []byte(txL85), []byte(aL85), []byte(eL85), vBytes)
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from an L85-encoded index key.
// Returns fixed-size arrays for entity, attr, tx to avoid heap escape.
func (e *L85KeyEncoder) DecodeKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [20]byte, err error) {
	if len(key) < 1 {
		return entity, attr, nil, tx, fmt.Errorf("key too short")
	}

	key = key[1:]

	const l85Size = 25     // 20-byte components
	const l85SizeAttr = 40 // 32-byte components

	switch index {
	case EAVT:
		minSize := l85Size + l85SizeAttr + l85Size
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("EAVT key too short")
		}
		entity, _ = codec.DecodeFixed20(string(key[0:l85Size]))
		attr, _ = codec.DecodeFixed32(string(key[l85Size : l85Size+l85SizeAttr]))
		valueBytes := key[l85Size+l85SizeAttr : len(key)-l85Size]
		if len(valueBytes) == l85Size {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		tx, _ = codec.DecodeFixed20(string(key[len(key)-l85Size:]))

	case AEVT:
		minSize := l85SizeAttr + l85Size + l85Size
		if len(key) < minSize {
			return entity, attr, nil, tx, fmt.Errorf("AEVT key too short")
		}
		attr, _ = codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		entity, _ = codec.DecodeFixed20(string(key[l85SizeAttr : l85SizeAttr+l85Size]))
		valueBytes := key[l85SizeAttr+l85Size : len(key)-l85Size]
		if len(valueBytes) == l85Size {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		tx, _ = codec.DecodeFixed20(string(key[len(key)-l85Size:]))

	case AVET:
		if len(key) < 2*l85Size {
			return entity, attr, nil, tx, fmt.Errorf("AVET key too short")
		}
		attr, _ = codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		tx, _ = codec.DecodeFixed20(string(key[len(key)-l85Size:]))
		entity, _ = codec.DecodeFixed20(string(key[len(key)-2*l85Size : len(key)-l85Size]))
		valueBytes := key[l85SizeAttr : len(key)-2*l85Size]
		if len(valueBytes) == l85Size+1 && valueBytes[0] == byte(datalog.TypeReference) {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes[1:])); decErr == nil {
				value = append([]byte{valueBytes[0]}, decoded[:]...)
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case VAET:
		if len(key) < 3*l85Size {
			return entity, attr, nil, tx, fmt.Errorf("VAET key too short")
		}
		tx, _ = codec.DecodeFixed20(string(key[len(key)-l85Size:]))
		entity, _ = codec.DecodeFixed20(string(key[len(key)-2*l85Size : len(key)-l85Size]))
		aStart := len(key) - 2*l85Size - l85SizeAttr
		attr, _ = codec.DecodeFixed32(string(key[aStart : aStart+l85SizeAttr]))
		valueBytes := key[0:aStart]
		if len(valueBytes) == l85Size {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case TAEV:
		if len(key) < 3*l85Size {
			return entity, attr, nil, tx, fmt.Errorf("TAEV key too short")
		}
		tx, _ = codec.DecodeFixed20(string(key[0:l85Size]))
		attr, _ = codec.DecodeFixed32(string(key[l85Size : l85Size+l85SizeAttr]))
		entity, _ = codec.DecodeFixed20(string(key[l85Size+l85SizeAttr : l85Size+l85SizeAttr+l85Size]))
		valueBytes := key[l85Size+l85SizeAttr+l85Size:]
		if len(valueBytes) == l85Size {
			if decoded, decErr := codec.DecodeFixed20(string(valueBytes)); decErr == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	default:
		return entity, attr, nil, tx, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, nil
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

// EncodeHistoryKey creates an L85-encoded index key with Op appended for history indices
func (e *L85KeyEncoder) EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte {
	sd := ToStorageDatom(*d)
	prefix := []byte{byte(index)}

	eL85 := codec.EncodeFixed20(sd.E)
	aL85 := codec.EncodeFixed32(sd.A)
	txL85 := codec.EncodeFixed20(sd.Tx)

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
func (e *L85KeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity [20]byte, attr [32]byte, value []byte, tx [20]byte, op Op, err error) {
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
