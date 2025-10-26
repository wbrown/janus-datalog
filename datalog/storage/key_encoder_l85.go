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
	// Convert to storage datom first
	sd := ToStorageDatom(*d)

	// Each index has a 1-byte prefix to separate namespaces
	prefix := []byte{byte(index)}

	// Encode the components to L85 (E and Tx are 20 bytes, A is 32 bytes)
	eL85 := codec.EncodeFixed20(sd.E)
	aL85 := codec.EncodeFixed32(sd.A)
	txL85 := codec.EncodeFixed20(sd.Tx)

	// Get value bytes with type prefix
	// RefValues are 20-byte entity references and should be L85-encoded
	vType := byte(datalog.Type(sd.V))
	var vBytes []byte
	if datalog.Type(sd.V) == datalog.TypeReference {
		// RefValue is exactly 20 bytes, encode it
		var vArr [20]byte
		copy(vArr[:], datalog.ValueBytes(sd.V))
		// Type prefix + L85-encoded reference
		vBytes = append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
	} else {
		// Other values: type prefix + raw bytes
		vData := datalog.ValueBytes(sd.V)
		vBytes = append([]byte{vType}, vData...)
	}

	// Build key based on index type
	switch index {
	case EAVT:
		// Entity + Attribute + Value + Tx
		return concatBytes(prefix, []byte(eL85), []byte(aL85), vBytes, []byte(txL85))

	case AEVT:
		// Attribute + Entity + Value + Tx
		return concatBytes(prefix, []byte(aL85), []byte(eL85), vBytes, []byte(txL85))

	case AVET:
		// Attribute + Value + Entity + Tx
		return concatBytes(prefix, []byte(aL85), vBytes, []byte(eL85), []byte(txL85))

	case VAET:
		// Value + Attribute + Entity + Tx
		return concatBytes(prefix, vBytes, []byte(aL85), []byte(eL85), []byte(txL85))

	case TAEV:
		// Tx + Attribute + Entity + Value
		return concatBytes(prefix, []byte(txL85), []byte(aL85), []byte(eL85), vBytes)

	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from an L85-encoded index key
func (e *L85KeyEncoder) DecodeKey(index IndexType, key []byte) (entity, attr, value, tx []byte, err error) {
	if len(key) < 1 {
		return nil, nil, nil, nil, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	// L85-encoded component sizes
	const l85Size = 25     // For 20-byte components (Entity, Tx)
	const l85SizeAttr = 40 // For 32-byte components (Attribute)

	// Decode based on index type
	switch index {
	case EAVT:
		minSize := l85Size + l85SizeAttr + l85Size // E + A + Tx
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("EAVT key too short")
		}
		e, _ := codec.DecodeFixed20(string(key[0:l85Size]))
		a, _ := codec.DecodeFixed32(string(key[l85Size : l85Size+l85SizeAttr]))
		entity = e[:]
		attr = a[:]
		// Value is between A and Tx
		valueBytes := key[l85Size+l85SizeAttr : len(key)-l85Size]
		if len(valueBytes) == l85Size {
			// Try to decode as L85 (likely a RefValue)
			if decoded, err := codec.DecodeFixed20(string(valueBytes)); err == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		t, _ := codec.DecodeFixed20(string(key[len(key)-l85Size:]))
		tx = t[:]

	case AEVT:
		minSize := l85SizeAttr + l85Size + l85Size // A + E + Tx
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("AEVT key too short")
		}
		a, _ := codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		e, _ := codec.DecodeFixed20(string(key[l85SizeAttr : l85SizeAttr+l85Size]))
		attr = a[:]
		entity = e[:]
		// Value is between E and Tx
		valueBytes := key[l85SizeAttr+l85Size : len(key)-l85Size]
		if len(valueBytes) == l85Size {
			// Try to decode as L85 (likely a RefValue)
			if decoded, err := codec.DecodeFixed20(string(valueBytes)); err == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}
		t, _ := codec.DecodeFixed20(string(key[len(key)-l85Size:]))
		tx = t[:]

	case AVET:
		// Value is variable length, so we work backwards
		if len(key) < 2*l85Size {
			return nil, nil, nil, nil, fmt.Errorf("AVET key too short")
		}
		a, _ := codec.DecodeFixed32(string(key[0:l85SizeAttr]))
		attr = a[:]
		t, _ := codec.DecodeFixed20(string(key[len(key)-l85Size:]))
		tx = t[:]
		e, _ := codec.DecodeFixed20(string(key[len(key)-2*l85Size : len(key)-l85Size]))
		entity = e[:]
		// Value is between A and E+Tx
		valueBytes := key[l85SizeAttr : len(key)-2*l85Size]
		// Check if this is a type-prefixed L85-encoded reference
		if len(valueBytes) == l85Size+1 && valueBytes[0] == byte(datalog.TypeReference) {
			// Type prefix + L85-encoded reference
			if decoded, err := codec.DecodeFixed20(string(valueBytes[1:])); err == nil {
				value = append([]byte{valueBytes[0]}, decoded[:]...)
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case VAET:
		// Value is at the beginning, variable length
		if len(key) < 3*l85Size {
			return nil, nil, nil, nil, fmt.Errorf("VAET key too short")
		}
		t, _ := codec.DecodeFixed20(string(key[len(key)-l85Size:]))
		tx = t[:]
		e, _ := codec.DecodeFixed20(string(key[len(key)-2*l85Size : len(key)-l85Size]))
		entity = e[:]
		// Need to adjust for the larger attribute size
		aStart := len(key) - 2*l85Size - l85SizeAttr
		a, _ := codec.DecodeFixed32(string(key[aStart : aStart+l85SizeAttr]))
		attr = a[:]
		// Value is at the beginning
		valueBytes := key[0:aStart]
		if len(valueBytes) == l85Size {
			// Try to decode as L85 (likely a RefValue)
			if decoded, err := codec.DecodeFixed20(string(valueBytes)); err == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	case TAEV:
		if len(key) < 3*l85Size {
			return nil, nil, nil, nil, fmt.Errorf("TAEV key too short")
		}
		t, _ := codec.DecodeFixed20(string(key[0:l85Size]))
		tx = t[:]
		a, _ := codec.DecodeFixed32(string(key[l85Size : l85Size+l85SizeAttr]))
		attr = a[:]
		e, _ := codec.DecodeFixed20(string(key[l85Size+l85SizeAttr : l85Size+l85SizeAttr+l85Size]))
		entity = e[:]
		// Value is at the end
		valueBytes := key[l85Size+l85SizeAttr+l85Size:]
		if len(valueBytes) == l85Size {
			// Try to decode as L85 (likely a RefValue)
			if decoded, err := codec.DecodeFixed20(string(valueBytes)); err == nil {
				value = decoded[:]
			} else {
				value = valueBytes
			}
		} else {
			value = valueBytes
		}

	default:
		return nil, nil, nil, nil, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, nil
}

// EncodePrefix creates an L85-encoded prefix key for range scans
func (e *L85KeyEncoder) EncodePrefix(index IndexType, parts ...[]byte) []byte {
	prefix := []byte{byte(index)}
	encoded := make([][]byte, len(parts)+1)
	encoded[0] = prefix

	// Encode parts based on index type and position
	for i, part := range parts {
		shouldEncode := false
		isValuePosition := false

		// Determine if this part should be L85-encoded based on index type
		switch index {
		case EAVT:
			// E, A are encoded (positions 0, 1), V is value (position 2), Tx is encoded (position 3)
			shouldEncode = (i == 0 || i == 1 || i == 3)
			isValuePosition = (i == 2)
		case AEVT:
			// A, E are encoded (positions 0, 1), V is value (position 2), Tx is encoded (position 3)
			shouldEncode = (i == 0 || i == 1 || i == 3)
			isValuePosition = (i == 2)
		case AVET:
			// A is encoded (position 0), V is value (position 1), E, Tx are encoded (positions 2, 3)
			shouldEncode = (i == 0 || i == 2 || i == 3)
			isValuePosition = (i == 1)
		case VAET:
			// V is value (position 0), A, E, Tx are encoded (positions 1, 2, 3)
			shouldEncode = (i >= 1)
			isValuePosition = (i == 0)
		case TAEV:
			// Tx, A, E are encoded (positions 0, 1, 2), V is value (position 3)
			shouldEncode = (i <= 2)
			isValuePosition = (i == 3)
		}

		if shouldEncode && len(part) == 20 {
			// Entity or Tx (20-byte components)
			var arr [20]byte
			copy(arr[:], part)
			encoded[i+1] = []byte(codec.EncodeFixed20(arr))
		} else if shouldEncode && len(part) == 32 {
			// Attribute (32-byte component)
			var arr [32]byte
			copy(arr[:], part)
			encoded[i+1] = []byte(codec.EncodeFixed32(arr))
		} else if isValuePosition && len(part) == 20 {
			// This is a value position with exactly 20 bytes - likely a RefValue
			// RefValues should be L85-encoded
			var arr [20]byte
			copy(arr[:], part)
			encoded[i+1] = []byte(codec.EncodeFixed20(arr))
		} else {
			// Variable-length values or other data
			encoded[i+1] = part
		}
	}

	return concatBytes(encoded...)
}

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *L85KeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
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
