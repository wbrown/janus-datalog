package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// IndexType represents different index orderings
type IndexType uint8

const (
	EAVT IndexType = iota // Entity-Attribute-Value-Tx
	AEVT                  // Attribute-Entity-Value-Tx
	AVET                  // Attribute-Value-Entity-Tx
	VAET                  // Value-Attribute-Entity-Tx
	TAEV                  // Tx-Attribute-Entity-Value
	// History indices (only used in RetractHistory mode)
	// These mirror the current-state indices but include Op and are append-only
	EAVT_HISTORY // Entity-Attribute-Value-Tx-Op
	AEVT_HISTORY // Attribute-Entity-Value-Tx-Op
	AVET_HISTORY // Attribute-Value-Entity-Tx-Op
	VAET_HISTORY // Value-Attribute-Entity-Tx-Op
	TAEV_HISTORY // Tx-Attribute-Entity-Value-Op
)

// CurrentStateIndices are the indices used for current-state queries
var CurrentStateIndices = []IndexType{EAVT, AEVT, AVET, VAET, TAEV}

// HistoryIndices are the indices used for history queries (include Op)
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
}

// StoreTx represents a storage transaction
type StoreTx interface {
	Assert(datoms []datalog.Datom) error
	Retract(datoms []datalog.Datom) error
	Commit() error
	Rollback() error
}
