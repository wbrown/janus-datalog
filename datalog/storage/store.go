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
