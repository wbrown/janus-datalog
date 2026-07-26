package storage

import (
	"errors"

	"github.com/wbrown/janus-datalog/datalog"
)

var errReadSessionClosed = errors.New("read session closed")

// StoreReader is the read subset of Store. Both a Store (each call opens its
// own storage transaction) and a ReadSession (every call observes one shared
// snapshot) satisfy it, so read paths written against StoreReader run
// identically in either mode.
//
// The vocabulary is store-agnostic: a scan names a typed ScanBound, not a byte
// range, and no binary encoder is exposed. A backend that keys on bytes
// projects the bound through BinaryKeyEncoder.EncodeScanBound at its own
// boundary; a backend that compares typed components directly does neither.
type StoreReader interface {
	Scan(bound ScanBound) (Iterator, error)
	ScanKeysOnly(bound ScanBound) (Iterator, error)
	MaxElementID() (datalog.ElementID, error)
	MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error)
}

// ReadSession is a consistent read view over the store: every read through
// the session observes one snapshot of the data, regardless of writes
// committed after the session opened. A query executes all its reads through
// one session, so it can never observe two different states mid-execution.
//
// Close releases the snapshot. Sessions carry a finalizer backstop, but
// deterministic release comes from the query result: the session closes when
// the result relation is exhausted or closed, whichever comes first.
type ReadSession interface {
	StoreReader
	Close() error
}

// maxElementIDByScan is the index-order derivation of the store-wide
// ElementID high-water mark: TAEV orders Tx descending, so the first entry
// carries the highest ElementID.
func maxElementIDByScan(r StoreReader) (datalog.ElementID, error) {
	iter, err := r.ScanKeysOnly(ScanBound{Index: TAEV})
	if err != nil {
		return datalog.ElementID{}, err
	}
	defer iter.Close()
	if !iter.Next() {
		return datalog.ElementID{}, iter.Error()
	}
	return iter.ElementID(), nil
}

// maxTxForEntityByScan walks the entity's EAVT range for its highest
// ElementID (EAVT is not Tx-ordered, so the full range is examined).
func maxTxForEntityByScan(r StoreReader, e datalog.Identity) (datalog.ElementID, bool, error) {
	iter, err := r.ScanKeysOnly(ScanBound{Index: EAVT, Prefix: []datalog.Value{e}})
	if err != nil {
		return datalog.ElementID{}, false, err
	}
	defer iter.Close()
	var maxID datalog.ElementID
	found := false
	for iter.Next() {
		tx := iter.ElementID()
		if !found || maxID.Less(tx) {
			maxID = tx
			found = true
		}
	}
	if err := iter.Error(); err != nil {
		return datalog.ElementID{}, false, err
	}
	return maxID, found, nil
}
