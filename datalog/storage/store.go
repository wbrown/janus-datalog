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
	AETV                  // Attribute-Entity-Tx-Value (for A-primary CRDT: first = current)
	AVET                  // Attribute-Value-Entity-Tx
	VAET                  // Value-Attribute-Entity-Tx
	TAEV                  // Tx-Attribute-Entity-Value (for clock recovery, audit log)
)

// Indices lists all indices used for queries
var Indices = []IndexType{EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV}

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

	// MaxElementIDForAttribute returns the highest ElementID for any (E, A) with this attribute.
	// Used for fast cache freshness checks on A-bound queries.
	// Performs an O(1) reverse seek on the AEVT index.
	// Returns zero ElementID if no data exists for this attribute.
	MaxElementIDForAttribute(a []byte) (datalog.ElementID, error)

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
