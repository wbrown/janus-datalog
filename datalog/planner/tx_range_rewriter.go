package planner

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// TxRangeRewriter provides utilities for handling Tx range queries.
// Since Tx (ElementID) is encoded so highest sorts first (using bitwise NOT),
// range scans require bound inversion for correct storage iteration.
//
// User writes intuitive: [(tx-between ?tx 1000 2000)] meaning "Tx in [1000, 2000]"
// Storage needs: scan from encoded(2000) to encoded(1000) because:
//   - encoded(2000) < encoded(1000) in byte order (due to bitwise NOT)
//   - Forward iteration goes from lowest bytes to highest
//   - So we scan from encoded(2000) to encoded(1000) to get Tx in [1000, 2000]

// ElementIDEncoder encodes/decodes ElementIDs for storage keys.
// The encoding uses bitwise NOT so highest Lamport values sort first.
type ElementIDEncoder struct{}

// EncodeForKey encodes a Lamport value for use in a storage key.
// Uses bitwise NOT so highest values sort first (first entry = current value).
func (e ElementIDEncoder) EncodeForKey(lamport uint64) []byte {
	buf := make([]byte, 16)
	// Big-endian encoding with bitwise NOT for descending sort
	inverted := ^lamport
	buf[0] = byte(inverted >> 56)
	buf[1] = byte(inverted >> 48)
	buf[2] = byte(inverted >> 40)
	buf[3] = byte(inverted >> 32)
	buf[4] = byte(inverted >> 24)
	buf[5] = byte(inverted >> 16)
	buf[6] = byte(inverted >> 8)
	buf[7] = byte(inverted)
	// ReplicaID = 0 (wildcard) for range queries
	// Also inverted for consistency
	buf[8] = 0xFF
	buf[9] = 0xFF
	buf[10] = 0xFF
	buf[11] = 0xFF
	buf[12] = 0xFF
	buf[13] = 0xFF
	buf[14] = 0xFF
	buf[15] = 0xFF
	return buf
}

// DecodeFromKey decodes a Lamport value from a storage key.
func (e ElementIDEncoder) DecodeFromKey(buf []byte) uint64 {
	if len(buf) < 8 {
		return 0
	}
	inverted := uint64(buf[0])<<56 |
		uint64(buf[1])<<48 |
		uint64(buf[2])<<40 |
		uint64(buf[3])<<32 |
		uint64(buf[4])<<24 |
		uint64(buf[5])<<16 |
		uint64(buf[6])<<8 |
		uint64(buf[7])
	return ^inverted
}

// StorageRange represents a range of keys for scanning.
type StorageRange struct {
	Index    int    // Index type (TAEV, EAVT, etc.)
	StartKey []byte // Start of range (inclusive)
	EndKey   []byte // End of range (exclusive)
}

// RewriteTxRange inverts bounds for storage scan.
// User specifies intuitive (low, high); storage needs (encoded_high, encoded_low).
//
// Because Tx encoding uses bitwise NOT:
//   - Higher Tx values encode to smaller byte sequences
//   - Lower Tx values encode to larger byte sequences
//
// To scan for Tx in [low, high]:
//   - Start at encoded(high) (smallest encoded value in range)
//   - End at encoded(low) (largest encoded value in range)
func RewriteTxRange(low, high uint64) (startKey, endKey []byte) {
	enc := ElementIDEncoder{}

	// High Tx encodes to smaller value, so it's the start bound
	startKey = enc.EncodeForKey(high)

	// Low Tx encodes to larger value, so it's the end bound
	// We need to increment the end bound for exclusive semantics
	// but for inclusive semantics on low, we use low directly
	endKey = enc.EncodeForKey(low)

	return startKey, endKey
}

// TxRangeBounds represents inverted bounds ready for storage scanning.
type TxRangeBounds struct {
	StartKey []byte // Encoded high bound (sorts first)
	EndKey   []byte // Encoded low bound (sorts last)
	Low      uint64 // Original low bound (for reference)
	High     uint64 // Original high bound (for reference)
}

// NewTxRangeBounds creates inverted bounds for Tx range scanning.
func NewTxRangeBounds(low, high uint64) TxRangeBounds {
	start, end := RewriteTxRange(low, high)
	return TxRangeBounds{
		StartKey: start,
		EndKey:   end,
		Low:      low,
		High:     high,
	}
}

// InRange checks if a Lamport value is within the original range.
// This is used for post-scan filtering if needed.
func (b TxRangeBounds) InRange(lamport uint64) bool {
	return lamport >= b.Low && lamport <= b.High
}

// ElementIDFromLamport creates an ElementID-like value from just a Lamport timestamp.
// Used for comparisons and range checks.
func ElementIDFromLamport(lamport uint64) datalog.ElementID {
	return datalog.ElementID{Lamport: lamport}
}

// IsLamportInRange checks if a Lamport value falls within bounds (inclusive).
func IsLamportInRange(lamport, low, high uint64) bool {
	return lamport >= low && lamport <= high
}
