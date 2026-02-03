package storage

import (
	"encoding/binary"

	"github.com/wbrown/janus-datalog/datalog"
)

// ElementID is a type alias for datalog.ElementID.
// The core type is defined in the datalog package so it can be used as a value type.
// This package adds key-encoding methods for storage.
type ElementID = datalog.ElementID

// ElementIDSize is the size in bytes of an encoded ElementID
const ElementIDSize = datalog.ElementIDSize

// HEAD is the zero value representing the HEAD sentinel for RGA vectors.
// All vector elements reference HEAD or another element's ID as their "after" reference.
var HEAD = datalog.ElementIDZero

// EncodeElementIDForKey writes 16 bytes for storage keys.
// Uses bitwise NOT so highest ElementID sorts first in forward scans.
// This enables O(1) "current value" lookups: first entry = highest Tx = current.
//
// Example:
//
//	EncodeElementIDForKey(ElementID{Lamport: 100, ReplicaID: 5})
//	→ bytes where 100 becomes ^100 (0xFFFFFFFFFFFFFF9B)
//	→ higher Lamports produce smaller byte values, sorting first
func EncodeElementIDForKey(a ElementID) []byte {
	buf := make([]byte, ElementIDSize)
	// Bitwise NOT inverts sort order: highest Lamport → smallest bytes → sorts first
	binary.BigEndian.PutUint64(buf[0:8], ^a.Lamport)
	binary.BigEndian.PutUint64(buf[8:16], ^a.ReplicaID)
	return buf
}

// EncodeElementIDInto writes 16 bytes into the provided buffer starting at offset.
// The buffer must have at least offset+16 bytes available.
// Uses bitwise NOT for descending sort order (same as EncodeElementIDForKey).
func EncodeElementIDInto(a ElementID, buf []byte, offset int) {
	binary.BigEndian.PutUint64(buf[offset:offset+8], ^a.Lamport)
	binary.BigEndian.PutUint64(buf[offset+8:offset+16], ^a.ReplicaID)
}

// DecodeElementID reads 16 bytes from storage key format.
// Reverses the bitwise NOT applied during EncodeElementIDForKey().
func DecodeElementID(buf []byte) ElementID {
	return ElementID{
		Lamport:   ^binary.BigEndian.Uint64(buf[0:8]),
		ReplicaID: ^binary.BigEndian.Uint64(buf[8:16]),
	}
}

// Max returns the larger of two ElementIDs according to the total order.
func Max(a, b ElementID) ElementID {
	return datalog.ElementIDMax(a, b)
}

// Min returns the smaller of two ElementIDs according to the total order.
func Min(a, b ElementID) ElementID {
	return datalog.ElementIDMin(a, b)
}
