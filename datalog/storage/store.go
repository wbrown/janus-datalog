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

	// AssertEach writes the datoms produce yields as one write unit, without the
	// caller gathering them first. The store calls produce once, handing it an
	// add to call per datom, and owns the unit's setup and teardown, so an early
	// return from produce cannot skip either.
	//
	// This is the write path for a producer whose extent is neither known nor
	// bounded — a decoder walking a dump chunk, where an entity is the floor and
	// nothing caps the top. What arrival means is each backend's own: the tree
	// store inserts into the version it is building, Badger encodes into a write
	// batch that splits at its own transaction ceiling.
	//
	// The datom add receives is the producer's workspace, valid for that call
	// only; a backend that retains one past the call copies it.
	//
	// A backend may leave the write open when the call returns, in which case
	// FinishBatch is what completes it. A failed producer stores an unspecified
	// part of its run; re-asserting is safe.
	AssertEach(produce func(add func(*datalog.Datom) error) error) error

	// FinishBatch completes a run of AssertEach calls, and is what makes the run
	// visible for a backend that left it open. A caller writing in a run — the
	// dump importers — calls it once when the run ends; a backend that completed
	// each AssertEach as it returned has nothing to do and returns nil.
	//
	// The tree store leaves runs open because publishing a version costs a
	// copy-on-write clone of every node that version touches, so a run that
	// publishes once pays for those nodes once rather than per call.
	FinishBatch() error

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
	// (debug/test).
	//
	// A scan yields EXACTLY the datoms whose bound components equal the
	// ScanBound's values, and narrowing to them is the implementation's
	// obligation. It is not free for a backend that projects the bound onto
	// byte keys: a V payload carries no length, so the keys for "abcd" sort
	// inside the range for "abc" interleaved with them and no endpoints
	// separate the two — the in-tree stores narrow by key length (EncodedRun,
	// runMembership). A backend comparing typed components directly has no such
	// gap. Returning everything inside a range returns datoms the caller did not
	// ask for, and no test above this seam will say so.
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

	// Seek narrows iteration to the run the bound names, and to all of it: the
	// bound repositions the cursor at or after its start, and the same bound
	// supplies where the run ends and the membership rule governing what lies
	// between. An implementation adopting only the start walks past the sought
	// bound into whatever the scan's wider range still holds, and leaves the
	// caller to work out where its own run ended from the encoded key — which is
	// how key-layout arithmetic gets back above this seam.
	//
	// A bound that cannot be encoded is recorded as the iterator's sticky error
	// rather than dropped: Seek has no return, so Error() is where the failure
	// surfaces.
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

	// Scanned reports how many datoms this iterator has taken in from the
	// index so far — intake, before any narrowing this iterator performs.
	// A key the bound's membership rule rejects is counted: the range covered
	// it and the scan paid to look at it.
	//
	// This is what makes narrowing auditable. Against the consumer's own count
	// of what survived, the ratio is the amplification the index charged, and
	// it is the only way to see whether a bound narrowed anything. Required
	// rather than optional for that reason: an absent count is indistinguishable
	// from a scan that read nothing and from instrumentation that was never
	// wired, which is the failure this seam's annotations exist to prevent.
	//
	// Wrapping iterators delegate — a wrapper reads no index of its own.
	Scanned() int
}

// StoreTx represents a storage transaction
type StoreTx interface {
	Assert(datoms []datalog.Datom) error
	Retract(datoms []datalog.Datom) error
	Commit() error
	Rollback() error
}
