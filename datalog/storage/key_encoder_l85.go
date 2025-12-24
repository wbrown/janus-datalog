package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// l85ComponentEncoder implements ComponentEncoder using L85 encoding
type l85ComponentEncoder struct{}

func (c *l85ComponentEncoder) EncodeEntity(e [20]byte) []byte {
	return []byte(codec.EncodeFixed20(e))
}

func (c *l85ComponentEncoder) EncodeAttr(a [32]byte) []byte {
	return []byte(codec.EncodeFixed32(a))
}

func (c *l85ComponentEncoder) EncodeTx(tx [20]byte) []byte {
	return []byte(codec.EncodeFixed20(tx))
}

func (c *l85ComponentEncoder) EncodeValue(v datalog.Value) []byte {
	vType := byte(datalog.Type(v))
	// RefValues are 20-byte entity references and should be L85-encoded
	if datalog.Type(v) == datalog.TypeReference {
		var vArr [20]byte
		copy(vArr[:], datalog.ValueBytes(v))
		return append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
	}
	// Other values: type prefix + raw bytes
	vData := datalog.ValueBytes(v)
	return append([]byte{vType}, vData...)
}

func (c *l85ComponentEncoder) DecodeEntity(b []byte) ([20]byte, error) {
	return codec.DecodeFixed20(string(b))
}

func (c *l85ComponentEncoder) DecodeAttr(b []byte) ([32]byte, error) {
	return codec.DecodeFixed32(string(b))
}

func (c *l85ComponentEncoder) DecodeTx(b []byte) ([20]byte, error) {
	return codec.DecodeFixed20(string(b))
}

func (c *l85ComponentEncoder) DecodeValue(b []byte) []byte {
	const l85Size = 25 // L85-encoded 20-byte value

	// Check if this is a type-prefixed L85-encoded reference
	// Format: [type_byte][25 L85 chars]
	if len(b) == l85Size+1 && b[0] == byte(datalog.TypeReference) {
		// Type prefix + L85-encoded reference
		if decoded, err := codec.DecodeFixed20(string(b[1:])); err == nil {
			return append([]byte{b[0]}, decoded[:]...)
		}
	}

	// For L85-only values (no type prefix, exactly 25 chars)
	if len(b) == l85Size {
		if decoded, err := codec.DecodeFixed20(string(b)); err == nil {
			return decoded[:]
		}
	}

	// Return as-is for other values
	return b
}

func (c *l85ComponentEncoder) EntitySize() int { return 25 } // L85 expands 20 bytes to 25 chars
func (c *l85ComponentEncoder) AttrSize() int   { return 40 } // L85 expands 32 bytes to 40 chars
func (c *l85ComponentEncoder) TxSize() int     { return 25 } // L85 expands 20 bytes to 25 chars

func (c *l85ComponentEncoder) EncodePrefixParts(index IndexType, parts [][]byte) []byte {
	encoded := make([][]byte, len(parts))

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
			encoded[i] = []byte(codec.EncodeFixed20(arr))
		} else if shouldEncode && len(part) == 32 {
			// Attribute (32-byte component)
			var arr [32]byte
			copy(arr[:], part)
			encoded[i] = []byte(codec.EncodeFixed32(arr))
		} else if isValuePosition && len(part) == 20 {
			// This is a value position with exactly 20 bytes - likely a RefValue
			var arr [20]byte
			copy(arr[:], part)
			encoded[i] = []byte(codec.EncodeFixed20(arr))
		} else {
			// Variable-length values or other data
			encoded[i] = part
		}
	}

	return concatBytes(encoded...)
}

// L85KeyEncoder implements KeyEncoder using L85 encoding for human-readable keys
type L85KeyEncoder struct {
	baseKeyEncoder
}

// ensureInitialized lazily initializes the component encoder
func (e *L85KeyEncoder) ensureInitialized() {
	if e.comp == nil {
		e.comp = &l85ComponentEncoder{}
	}
}

// EncodeKey creates an L85-encoded index key from a datom
func (e *L85KeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodeKey(index, d)
}

// DecodeKey extracts components from an L85-encoded index key
func (e *L85KeyEncoder) DecodeKey(index IndexType, key []byte) (entity, attr, value, tx []byte, err error) {
	e.ensureInitialized()
	return e.baseKeyEncoder.DecodeKey(index, key)
}

// EncodePrefix creates an L85-encoded prefix key for range scans
func (e *L85KeyEncoder) EncodePrefix(index IndexType, parts ...[]byte) []byte {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodePrefix(index, parts...)
}

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *L85KeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodePrefixRange(index, parts...)
}

// EncodeHistoryKey creates an L85-encoded index key with Op appended for history indices
func (e *L85KeyEncoder) EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodeHistoryKey(index, d, op)
}

// DecodeHistoryKey extracts components including Op from an L85-encoded history index key
func (e *L85KeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity, attr, value, tx []byte, op Op, err error) {
	e.ensureInitialized()
	return e.baseKeyEncoder.DecodeHistoryKey(index, key)
}

// NewL85KeyEncoder creates a new L85 key encoder
func NewL85KeyEncoder() *L85KeyEncoder {
	return &L85KeyEncoder{
		baseKeyEncoder: baseKeyEncoder{comp: &l85ComponentEncoder{}},
	}
}

// Ensure L85KeyEncoder satisfies KeyEncoder interface
var _ KeyEncoder = (*L85KeyEncoder)(nil)
