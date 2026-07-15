package storage

import (
	"github.com/wbrown/janus-datalog/datalog"
)

const (
	metadataPrefix = "_meta:"
	blobKeyPrefix  = byte(0xFF)
)

// IndexType represents different index orderings
type IndexType uint8

const (
	EAVT IndexType = iota // Entity-Attribute-Value-Tx (for cardinality-many: group by V)
	EATV                  // Entity-Attribute-Tx-Value (for cardinality-one: first = current)
	AEVT                  // Attribute-Entity-Value-Tx
	AETV                  // Attribute-Entity-Tx-Value (for A-primary CRDT: first = current)
	ATEV                  // Attribute-Tx-Entity-Value (for O(1) attribute high-water mark + AsOf-by-attribute)
	AVET                  // Attribute-Value-Entity-Tx
	VAET                  // Value-Attribute-Entity-Tx
	TAEV                  // Tx-Attribute-Entity-Value (for clock recovery, audit log)
)

// Indices lists all indices used for queries
var Indices = []IndexType{EAVT, EATV, AEVT, AETV, ATEV, AVET, VAET, TAEV}

// Store is the interface for datom storage
type Store interface {
	Encoder() *BinaryKeyEncoder

	// Write operations
	Assert(datoms []datalog.Datom) error
	Retract(datoms []datalog.Datom) error
	DeleteDatoms(datoms []datalog.Datom) (int, error)

	// Read operations
	Scan(index IndexType, start, end []byte) (Iterator, error)
	ScanKeysOnly(index IndexType, start, end []byte) (Iterator, error)
	DatomsAfter(eid datalog.ElementID) ([]datalog.Datom, error)
	MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error)
	GetMetadataUint64(key string) (uint64, bool, error)
	SetMetadataUint64(key string, value uint64) error

	// MaxElementID returns the highest ElementID in the store.
	// Used to restore the Lamport clock on database open.
	// Returns zero ElementID if store is empty.
	MaxElementID() (datalog.ElementID, error)

	// MaxElementIDForAttribute returns the highest ElementID for any (E, A) with this attribute.
	// Used for fast cache freshness checks on A-bound queries.
	// Performs an O(1) forward seek on the ATEV index — first entry under [A]
	// is the global max-Tx datom because ATEV orders A → Tx↓ → E → V.
	// Returns zero ElementID if no data exists for this attribute.
	MaxElementIDForAttribute(a []byte) (datalog.ElementID, error)

	// Transaction support
	BeginTx() (StoreTx, error)

	// Lifecycle
	Close() error
}

// Iterator provides sequential access to datoms.
//
// Iteration pattern:
//
//	for iter.Next() {
//	    d, err := iter.Datom()
//	    if err != nil { handle }
//	    // use d
//	}
//	if err := iter.Error(); err != nil {
//	    // iteration was aborted by an internal failure
//	}
//
// Error() must be checked after Next() returns false. A nil result
// indicates normal exhaustion; non-nil indicates that iteration
// aborted partway through (storage scan failure, sub-scan failure
// inside a wrapping iterator, or a decode failure retained as a sticky
// Error after Next() returned false).
//
// Workspace contract: Datom() returns the iterator's current datom
// workspace until Next, Seek, or Close. Callers that retain values past
// those calls must copy. Scan and ScanKeysOnly share this contract.
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

	// Error returns the first error encountered during iteration.
	// Implementations that cannot error return nil. Wrapping iterators
	// propagate from their inner iterator (return the first non-nil
	// between the outer's own error and inner.Error()).
	Error() error
}

// StoreTx represents a storage transaction
type StoreTx interface {
	Assert(datoms []datalog.Datom) error
	Retract(datoms []datalog.Datom) error
	Commit() error
	Rollback() error
}
