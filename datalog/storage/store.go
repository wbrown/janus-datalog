package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// IndexType represents different index orderings
type IndexType uint8

const (
	EAVT IndexType = iota // Entity-Attribute-Value-Tx (for cardinality-many: group by V)
	EATV                  // Entity-Attribute-Tx-Value (for cardinality-one: first = current)
	AEVT                  // Attribute-Entity-Value-Tx
	AVET                  // Attribute-Value-Entity-Tx
	VAET                  // Value-Attribute-Entity-Tx
	TAEV                  // Tx-Attribute-Entity-Value (for clock recovery, audit log)
	// Legacy history indices - DEPRECATED, to be removed
	// History is now built-in via CRDT semantics (all versions stored in main indices)
	EAVT_HISTORY // DEPRECATED
	AEVT_HISTORY // DEPRECATED
	AVET_HISTORY // DEPRECATED
	VAET_HISTORY // DEPRECATED
	TAEV_HISTORY // DEPRECATED
)

// CurrentStateIndices are the indices used for queries (6 indices for CRDT support)
var CurrentStateIndices = []IndexType{EAVT, EATV, AEVT, AVET, VAET, TAEV}

// HistoryIndices - DEPRECATED, kept for backward compatibility during migration
// History is now built-in via CRDT semantics
var HistoryIndices = []IndexType{EAVT_HISTORY, AEVT_HISTORY, AVET_HISTORY, VAET_HISTORY, TAEV_HISTORY}

// Op represents the operation type for a datom (assert or retract)
type Op bool

const (
	OpAssert  Op = true  // Datom was asserted
	OpRetract Op = false // Datom was retracted
)

// RetractMode controls how retractions are handled
type RetractMode int

const (
	// RetractDelete removes datoms from the store (default, current behavior)
	RetractDelete RetractMode = iota
	// RetractHistory keeps full history - retractions append Op=false to history indices
	RetractHistory
)

// Store is the interface for datom storage
type Store interface {
	// Write operations
	Assert(datoms []datalog.Datom) error
	Retract(datoms []datalog.Datom) error

	// Read operations
	Scan(index IndexType, start, end []byte) (Iterator, error)
	Get(index IndexType, key []byte) (*datalog.Datom, error)

	// MaxElementID returns the highest ElementID in the store.
	// Used to restore the Lamport clock on database open.
	// Returns zero ElementID if store is empty.
	MaxElementID() (datalog.ElementID, error)

	// Transaction support
	BeginTx() (StoreTx, error)

	// Lifecycle
	Close() error
}

// Iterator provides sequential access to datoms
type Iterator interface {
	Next() bool
	Datom() (*datalog.Datom, error)
	Close() error
	Seek(key []byte) // Position iterator at or after the given key

	// ElementID returns the transaction ElementID of the current entry.
	// This is more efficient than Datom() when only the ElementID is needed
	// (e.g., for cache freshness checks, CRDT version comparisons).
	// Returns zero ElementID if iterator is not positioned on a valid entry.
	ElementID() datalog.ElementID
}

// StoreTx represents a storage transaction
type StoreTx interface {
	Assert(datoms []datalog.Datom) error
	Retract(datoms []datalog.Datom) error
	Commit() error
	Rollback() error
}
