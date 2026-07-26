package storage

import (
	"fmt"

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
	ATEV                  // Attribute-Tx-Entity-Value (for AsOf-by-attribute: A-bound + Tx-bound scans seek straight to the transaction)
	AVET                  // Attribute-Value-Entity-Tx
	VAET                  // Value-Attribute-Entity-Tx
	TAEV                  // Tx-Attribute-Entity-Value (for clock recovery, audit log)
)

// Indices lists all indices used for queries
var Indices = []IndexType{EAVT, EATV, AEVT, AETV, ATEV, AVET, VAET, TAEV}

// String names the index. It is the one rendering of an IndexType, so %v in an
// error or an annotation reads "AVET" rather than "5" — the numbering is an
// implementation detail of the key prefix and means nothing to a reader.
func (i IndexType) String() string {
	switch i {
	case EAVT:
		return "EAVT"
	case EATV:
		return "EATV"
	case AEVT:
		return "AEVT"
	case AETV:
		return "AETV"
	case ATEV:
		return "ATEV"
	case AVET:
		return "AVET"
	case VAET:
		return "VAET"
	case TAEV:
		return "TAEV"
	default:
		return fmt.Sprintf("IndexType(%d)", uint8(i))
	}
}

// Store is the interface for datom storage
type Store interface {
	Encoder() *BinaryKeyEncoder

	// Write operations
	Assert(datoms []datalog.Datom) error
	Retract(datoms []datalog.Datom) error
	DeleteDatoms(datoms []datalog.Datom) (int, error)

	// Read operations
	//
	// There is no point lookup. A complete index key names one (E, A, V, Tx),
	// but Tx is what CRDT resolution determines — first entry of the ordered
	// group wins — so a reader that already knew it would have nothing left to
	// ask. Every read is a prefix scan; a scan whose range binds all four
	// components is the point lookup, and returns at most one datom because Tx
	// is unique per operation.
	//
	// CountKeys is likewise not on Store — it remains *BadgerStore-only
	// (debug/test); see docs/BREAKING_RELEASE_UPGRADE_v0.15.0.md.
	Scan(bound ScanBound) (Iterator, error)
	ScanKeysOnly(bound ScanBound) (Iterator, error)
	DatomsAfter(eid datalog.ElementID) ([]datalog.Datom, error)
	MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error)
	GetMetadataUint64(key string) (uint64, bool, error)
	SetMetadataUint64(key string, value uint64) error

	// MaxElementID returns the highest ElementID in the store.
	// Used to restore the Lamport clock on database open.
	// Returns zero ElementID if store is empty.
	MaxElementID() (datalog.ElementID, error)

	// NewReadSession opens a consistent read view at the store's current
	// state: every read through the session observes one snapshot,
	// regardless of writes committed after it opened. A query executes all
	// its storage reads through one session, so it can never observe two
	// different database states mid-execution.
	NewReadSession() (ReadSession, error)

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

	// Seek repositions at or after the bound's start. A bound that cannot be
	// encoded is recorded as the iterator's sticky error rather than dropped:
	// Seek has no return, so Error() is where the failure surfaces.
	Seek(bound ScanBound)

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
