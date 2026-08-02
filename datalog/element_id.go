package datalog

import (
	"encoding/binary"
	"fmt"
)

// ElementID uniquely identifies a datom version across all replicas.
//
// Lamport: Logical timestamp from this replica's clock. Provides causal ordering.
// ReplicaID: Identifies which Database instance generated this timestamp. Used for
// tiebreaking when two replicas generate the same Lamport value.
//
// Within a single Database instance:
//   - All goroutines share one atomic clock
//   - Every write gets a unique Lamport value (atomic increment)
//   - ReplicaID tiebreaking never activates (same ReplicaID, different Lamports)
//
// Across multiple Database instances (replication/merge scenarios):
//   - Each instance should have a unique ReplicaID
//   - Independent clocks may generate the same Lamport value
//   - ReplicaID provides deterministic tiebreaking for LWW resolution
//
// ElementID can be used as a value type in datoms, allowing users to:
//   - Store ElementIDs as attribute values (e.g., :entity/created-at)
//   - Query by ElementID ranges
//   - Compare ElementIDs in predicates
//   - Return ElementIDs in query results
type ElementID struct {
	Lamport   uint64 // Logical timestamp (causal ordering)
	ReplicaID uint64 // Database instance identifier (tiebreaker when merging)
}

// ElementIDSize is the size in bytes of an encoded ElementID
const ElementIDSize = 16

// ElementIDZero is the zero value representing the HEAD sentinel for RGA vectors.
var ElementIDZero = ElementID{0, 0}

// Less implements total ordering for ElementIDs.
// Returns true if a < b in the total order.
// Ordering: first by Lamport (logical time), then by ReplicaID (tiebreaker).
func (a ElementID) Less(b ElementID) bool {
	if a.Lamport != b.Lamport {
		return a.Lamport < b.Lamport
	}
	return a.ReplicaID < b.ReplicaID
}

// Equal returns true if two ElementIDs are identical.
func (a ElementID) Equal(b ElementID) bool {
	return a.Lamport == b.Lamport && a.ReplicaID == b.ReplicaID
}

// IsZero returns true for the HEAD sentinel (zero value).
func (a ElementID) IsZero() bool {
	return a.Lamport == 0 && a.ReplicaID == 0
}

// Bytes returns 16 bytes in natural order (for value encoding).
// This is different from key encoding which uses bitwise NOT for descending order.
//
// Value encoding uses natural order so AVET range scans work intuitively:
//   - Lowest ElementID sorts first
//   - Highest ElementID sorts last
//
// Key encoding (in storage package) uses bitwise NOT:
//   - Highest ElementID sorts first (for O(1) current value lookup)
func (a ElementID) Bytes() []byte {
	buf := make([]byte, ElementIDSize)
	binary.BigEndian.PutUint64(buf[0:8], a.Lamport)
	binary.BigEndian.PutUint64(buf[8:16], a.ReplicaID)
	return buf
}

// ElementIDFromBytes reads 16 bytes in natural order (for value decoding).
func ElementIDFromBytes(buf []byte) ElementID {
	return ElementID{
		Lamport:   binary.BigEndian.Uint64(buf[0:8]),
		ReplicaID: binary.BigEndian.Uint64(buf[8:16]),
	}
}

// String returns human-readable representation: "L{lamport}@R{replicaID}"
// Example: "L1234@R5678"
func (a ElementID) String() string {
	if a.IsZero() {
		return "HEAD"
	}
	return fmt.Sprintf("L%d@R%d", a.Lamport, a.ReplicaID)
}

// Compare returns -1 if a < b, 0 if a == b, 1 if a > b, in the total order
// Less defines: Lamport, then ReplicaID.
func (a ElementID) Compare(b ElementID) int {
	switch {
	case a.Lamport < b.Lamport:
		return -1
	case a.Lamport > b.Lamport:
		return 1
	case a.ReplicaID < b.ReplicaID:
		return -1
	case a.ReplicaID > b.ReplicaID:
		return 1
	}
	return 0
}

// Max returns the larger of two ElementIDs according to the total order.
func ElementIDMax(a, b ElementID) ElementID {
	if a.Less(b) {
		return b
	}
	return a
}

// Min returns the smaller of two ElementIDs according to the total order.
func ElementIDMin(a, b ElementID) ElementID {
	if a.Less(b) {
		return a
	}
	return b
}
