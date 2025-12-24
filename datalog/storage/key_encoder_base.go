package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
)

// ComponentEncoder handles encoding/decoding of individual key components.
// This interface abstracts the difference between L85 and Binary encoding.
type ComponentEncoder interface {
	// Encoding
	EncodeEntity(e [20]byte) []byte
	EncodeAttr(a [32]byte) []byte
	EncodeTx(tx [20]byte) []byte
	EncodeValue(v datalog.Value) []byte

	// Decoding
	DecodeEntity(b []byte) ([20]byte, error)
	DecodeAttr(b []byte) ([32]byte, error)
	DecodeTx(b []byte) ([20]byte, error)
	// DecodeValue decodes value bytes extracted from a key
	// For L85, this detects and decodes L85-encoded RefValues
	DecodeValue(b []byte) []byte

	// Sizes (L85 expands bytes, Binary keeps them same size)
	EntitySize() int // 25 for L85, 20 for Binary
	AttrSize() int   // 40 for L85, 32 for Binary
	TxSize() int     // 25 for L85, 20 for Binary

	// Prefix encoding (L85 needs special handling)
	EncodePrefixParts(index IndexType, parts [][]byte) []byte
}

// baseKeyEncoder implements KeyEncoder using a pluggable ComponentEncoder.
// This consolidates the shared index ordering logic.
type baseKeyEncoder struct {
	comp ComponentEncoder
}

// EncodeKey creates an index key from a datom using the component encoder
func (e *baseKeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	sd := ToStorageDatom(*d)
	prefix := []byte{byte(index)}

	eEnc := e.comp.EncodeEntity(sd.E)
	aEnc := e.comp.EncodeAttr(sd.A)
	txEnc := e.comp.EncodeTx(sd.Tx)
	vEnc := e.comp.EncodeValue(sd.V)

	// Build key based on index type - this ordering is shared by all encoders
	switch index {
	case EAVT:
		return concatBytes(prefix, eEnc, aEnc, vEnc, txEnc)
	case AEVT:
		return concatBytes(prefix, aEnc, eEnc, vEnc, txEnc)
	case AVET:
		return concatBytes(prefix, aEnc, vEnc, eEnc, txEnc)
	case VAET:
		return concatBytes(prefix, vEnc, aEnc, eEnc, txEnc)
	case TAEV:
		return concatBytes(prefix, txEnc, aEnc, eEnc, vEnc)
	default:
		panic(fmt.Sprintf("unknown index type: %v", index))
	}
}

// DecodeKey extracts components from an index key
func (e *baseKeyEncoder) DecodeKey(index IndexType, key []byte) (entity, attr, value, tx []byte, err error) {
	if len(key) < 1 {
		return nil, nil, nil, nil, fmt.Errorf("key too short")
	}

	// Skip the 1-byte prefix
	key = key[1:]

	eSize := e.comp.EntitySize()
	aSize := e.comp.AttrSize()
	txSize := e.comp.TxSize()

	switch index {
	case EAVT:
		minSize := eSize + aSize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("EAVT key too short")
		}
		eArr, _ := e.comp.DecodeEntity(key[0:eSize])
		entity = eArr[:]
		aArr, _ := e.comp.DecodeAttr(key[eSize : eSize+aSize])
		attr = aArr[:]
		value = e.comp.DecodeValue(key[eSize+aSize : len(key)-txSize])
		txArr, _ := e.comp.DecodeTx(key[len(key)-txSize:])
		tx = txArr[:]

	case AEVT:
		minSize := aSize + eSize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("AEVT key too short")
		}
		aArr, _ := e.comp.DecodeAttr(key[0:aSize])
		attr = aArr[:]
		eArr, _ := e.comp.DecodeEntity(key[aSize : aSize+eSize])
		entity = eArr[:]
		value = e.comp.DecodeValue(key[aSize+eSize : len(key)-txSize])
		txArr, _ := e.comp.DecodeTx(key[len(key)-txSize:])
		tx = txArr[:]

	case AVET:
		minSize := aSize + eSize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("AVET key too short")
		}
		aArr, _ := e.comp.DecodeAttr(key[0:aSize])
		attr = aArr[:]
		txArr, _ := e.comp.DecodeTx(key[len(key)-txSize:])
		tx = txArr[:]
		eArr, _ := e.comp.DecodeEntity(key[len(key)-txSize-eSize : len(key)-txSize])
		entity = eArr[:]
		value = e.comp.DecodeValue(key[aSize : len(key)-txSize-eSize])

	case VAET:
		minSize := aSize + eSize + txSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("VAET key too short")
		}
		txArr, _ := e.comp.DecodeTx(key[len(key)-txSize:])
		tx = txArr[:]
		eArr, _ := e.comp.DecodeEntity(key[len(key)-txSize-eSize : len(key)-txSize])
		entity = eArr[:]
		aArr, _ := e.comp.DecodeAttr(key[len(key)-txSize-eSize-aSize : len(key)-txSize-eSize])
		attr = aArr[:]
		value = e.comp.DecodeValue(key[0 : len(key)-txSize-eSize-aSize])

	case TAEV:
		minSize := txSize + aSize + eSize
		if len(key) < minSize {
			return nil, nil, nil, nil, fmt.Errorf("TAEV key too short")
		}
		txArr, _ := e.comp.DecodeTx(key[0:txSize])
		tx = txArr[:]
		aArr, _ := e.comp.DecodeAttr(key[txSize : txSize+aSize])
		attr = aArr[:]
		eArr, _ := e.comp.DecodeEntity(key[txSize+aSize : txSize+aSize+eSize])
		entity = eArr[:]
		value = e.comp.DecodeValue(key[txSize+aSize+eSize:])

	default:
		return nil, nil, nil, nil, fmt.Errorf("unknown index type: %v", index)
	}

	return entity, attr, value, tx, nil
}

// EncodePrefix creates a prefix key for range scans
func (e *baseKeyEncoder) EncodePrefix(index IndexType, parts ...[]byte) []byte {
	prefix := []byte{byte(index)}
	encodedParts := e.comp.EncodePrefixParts(index, parts)
	return concatBytes(prefix, encodedParts)
}

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *baseKeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
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

// historyIndexToBase maps history index types to their base current-state equivalents
func historyIndexToBase(index IndexType) IndexType {
	switch index {
	case EAVT_HISTORY:
		return EAVT
	case AEVT_HISTORY:
		return AEVT
	case AVET_HISTORY:
		return AVET
	case VAET_HISTORY:
		return VAET
	case TAEV_HISTORY:
		return TAEV
	default:
		return index
	}
}

// EncodeHistoryKey creates an index key with Op appended for history indices
func (e *baseKeyEncoder) EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte {
	sd := ToStorageDatom(*d)
	prefix := []byte{byte(index)}

	eEnc := e.comp.EncodeEntity(sd.E)
	aEnc := e.comp.EncodeAttr(sd.A)
	txEnc := e.comp.EncodeTx(sd.Tx)
	vEnc := e.comp.EncodeValue(sd.V)

	// Op encoding: 0x01 = assert (true), 0x00 = retract (false)
	opByte := byte(0x00)
	if op {
		opByte = 0x01
	}

	// Build key based on base index type, then append Op
	baseIndex := historyIndexToBase(index)
	switch baseIndex {
	case EAVT:
		return concatBytes(prefix, eEnc, aEnc, vEnc, txEnc, []byte{opByte})
	case AEVT:
		return concatBytes(prefix, aEnc, eEnc, vEnc, txEnc, []byte{opByte})
	case AVET:
		return concatBytes(prefix, aEnc, vEnc, eEnc, txEnc, []byte{opByte})
	case VAET:
		return concatBytes(prefix, vEnc, aEnc, eEnc, txEnc, []byte{opByte})
	case TAEV:
		return concatBytes(prefix, txEnc, aEnc, eEnc, vEnc, []byte{opByte})
	default:
		panic(fmt.Sprintf("unknown history index type: %v", index))
	}
}

// DecodeHistoryKey extracts components including Op from a history index key
func (e *baseKeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity, attr, value, tx []byte, op Op, err error) {
	if len(key) < 2 { // At minimum: prefix + op
		return nil, nil, nil, nil, false, fmt.Errorf("history key too short")
	}

	// Op is the last byte
	opByte := key[len(key)-1]
	op = opByte == 0x01

	// Decode the rest as a normal key (without the Op byte)
	keyWithoutOp := key[:len(key)-1]

	// Use the base index type for decoding
	baseIndex := historyIndexToBase(index)
	entity, attr, value, tx, err = e.DecodeKey(baseIndex, keyWithoutOp)
	return entity, attr, value, tx, op, err
}
