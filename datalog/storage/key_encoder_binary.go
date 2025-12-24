package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// binaryComponentEncoder implements ComponentEncoder using raw binary
type binaryComponentEncoder struct{}

func (c *binaryComponentEncoder) EncodeEntity(e [20]byte) []byte {
	return e[:]
}

func (c *binaryComponentEncoder) EncodeAttr(a [32]byte) []byte {
	return a[:]
}

func (c *binaryComponentEncoder) EncodeTx(tx [20]byte) []byte {
	return tx[:]
}

func (c *binaryComponentEncoder) EncodeValue(v datalog.Value) []byte {
	vType := byte(datalog.Type(v))
	vData := datalog.ValueBytes(v)
	return append([]byte{vType}, vData...)
}

func (c *binaryComponentEncoder) DecodeEntity(b []byte) ([20]byte, error) {
	var arr [20]byte
	copy(arr[:], b)
	return arr, nil
}

func (c *binaryComponentEncoder) DecodeAttr(b []byte) ([32]byte, error) {
	var arr [32]byte
	copy(arr[:], b)
	return arr, nil
}

func (c *binaryComponentEncoder) DecodeTx(b []byte) ([20]byte, error) {
	var arr [20]byte
	copy(arr[:], b)
	return arr, nil
}

func (c *binaryComponentEncoder) DecodeValue(b []byte) []byte {
	// Binary encoding doesn't transform values
	return b
}

func (c *binaryComponentEncoder) EntitySize() int { return 20 }
func (c *binaryComponentEncoder) AttrSize() int   { return 32 }
func (c *binaryComponentEncoder) TxSize() int     { return 20 }

func (c *binaryComponentEncoder) EncodePrefixParts(index IndexType, parts [][]byte) []byte {
	// Binary encoding doesn't transform parts
	return concatBytes(parts...)
}

// BinaryKeyEncoder implements KeyEncoder using raw binary for space efficiency
type BinaryKeyEncoder struct {
	baseKeyEncoder
}

// ensureInitialized lazily initializes the component encoder
func (e *BinaryKeyEncoder) ensureInitialized() {
	if e.comp == nil {
		e.comp = &binaryComponentEncoder{}
	}
}

// EncodeKey creates a binary index key from a datom
func (e *BinaryKeyEncoder) EncodeKey(index IndexType, d *datalog.Datom) []byte {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodeKey(index, d)
}

// DecodeKey extracts components from a binary index key
func (e *BinaryKeyEncoder) DecodeKey(index IndexType, key []byte) (entity, attr, value, tx []byte, err error) {
	e.ensureInitialized()
	return e.baseKeyEncoder.DecodeKey(index, key)
}

// EncodePrefix creates a binary prefix key for range scans
func (e *BinaryKeyEncoder) EncodePrefix(index IndexType, parts ...[]byte) []byte {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodePrefix(index, parts...)
}

// EncodePrefixRange creates start and end keys for a prefix scan
func (e *BinaryKeyEncoder) EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte) {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodePrefixRange(index, parts...)
}

// EncodeHistoryKey creates a binary index key with Op appended for history indices
func (e *BinaryKeyEncoder) EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte {
	e.ensureInitialized()
	return e.baseKeyEncoder.EncodeHistoryKey(index, d, op)
}

// DecodeHistoryKey extracts components including Op from a binary history index key
func (e *BinaryKeyEncoder) DecodeHistoryKey(index IndexType, key []byte) (entity, attr, value, tx []byte, op Op, err error) {
	e.ensureInitialized()
	return e.baseKeyEncoder.DecodeHistoryKey(index, key)
}

// NewBinaryKeyEncoder creates a new binary key encoder
func NewBinaryKeyEncoder() *BinaryKeyEncoder {
	return &BinaryKeyEncoder{
		baseKeyEncoder: baseKeyEncoder{comp: &binaryComponentEncoder{}},
	}
}

// Ensure BinaryKeyEncoder satisfies KeyEncoder interface
var _ KeyEncoder = (*BinaryKeyEncoder)(nil)
