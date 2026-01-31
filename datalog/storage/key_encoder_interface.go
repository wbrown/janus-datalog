package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// KeyEncoder builds and parses index keys from datoms
type KeyEncoder interface {
	// EncodeKey creates an index key from a datom (for current-state indices)
	EncodeKey(index IndexType, d *datalog.Datom) []byte

	// DecodeKey extracts components from an index key (for current-state indices)
	// Returns fixed-size arrays for e, a, tx to avoid heap allocations from slice escape.
	// Only v (value) is variable-length.
	// tx is 16 bytes: Lamport (8) + ReplicaID (8) = ElementID
	// op is 1 byte: 0=none, 1=add, 2=remove (for CRDT semantics)
	DecodeKey(index IndexType, key []byte) (e [20]byte, a [32]byte, v []byte, tx [16]byte, op byte, err error)

	// EncodePrefix creates a prefix key for range scans
	EncodePrefix(index IndexType, parts ...[]byte) []byte

	// EncodePrefixRange creates start and end keys for a prefix scan
	EncodePrefixRange(index IndexType, parts ...[]byte) (start, end []byte)

	// EncodeTxForPrefix encodes a Tx with bitwise NOT for use in prefix keys.
	// Use this when constructing scan ranges involving Tx (e.g., TAEV time-range queries).
	// Note: With bitwise NOT, higher Tx values encode to lower byte values.
	EncodeTxForPrefix(tx Tx) []byte

	// EncodeHistoryKey creates an index key with Op for history indices
	EncodeHistoryKey(index IndexType, d *datalog.Datom, op Op) []byte

	// DecodeHistoryKey extracts components including Op from a history index key
	// Returns fixed-size arrays for e, a, tx to avoid heap allocations from slice escape.
	// tx is 16 bytes: Lamport (8) + ReplicaID (8) = ElementID
	DecodeHistoryKey(index IndexType, key []byte) (e [20]byte, a [32]byte, v []byte, tx [16]byte, op Op, err error)
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
		return &L85KeyEncoder{}
	case BinaryStrategy:
		return &BinaryKeyEncoder{}
	default:
		// Default to L85 for debugging
		return &L85KeyEncoder{}
	}
}
