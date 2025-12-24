package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// KeyEncoder builds and parses index keys from datoms
type KeyEncoder interface {
	// EncodeKey creates an index key from a datom (for current-state indices)
	EncodeKey(index IndexType, d *datalog.Datom) []byte

	// DecodeKey extracts components from an index key (for current-state indices)
	DecodeKey(index IndexType, key []byte) (e, a, v, tx []byte, err error)

	// EncodePrefix creates a prefix key for range scans
	EncodePrefix(index IndexType, parts ...[]byte) []byte

	// EncodePrefixRange creates start and end keys for a prefix scan
	EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte)

	// EncodeHistoryKey creates an index key with Op for history indices
	EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte

	// DecodeHistoryKey extracts components including Op from a history index key
	DecodeHistoryKey(index IndexType, key []byte) (e, a, v, tx []byte, op Op, err error)
}

// KeyEncodingStrategy represents different encoding strategies
type KeyEncodingStrategy int

const (
	// L85Strategy uses L85 encoding for human-readable keys
	L85Strategy KeyEncodingStrategy = iota

	// BinaryStrategy uses raw binary for space efficiency
	BinaryStrategy

	// HybridStrategy uses binary storage with L85 for external APIs
	HybridStrategy
)

// NewKeyEncoder creates a key encoder with the specified strategy
func NewKeyEncoder(strategy KeyEncodingStrategy) KeyEncoder {
	switch strategy {
	case L85Strategy:
		return NewL85KeyEncoder()
	case BinaryStrategy:
		return NewBinaryKeyEncoder()
	default:
		// Default to L85 for debugging
		return NewL85KeyEncoder()
	}
}
