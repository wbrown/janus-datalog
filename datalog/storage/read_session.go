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
type StoreReader interface {
	Encoder() *BinaryKeyEncoder
	Scan(index IndexType, start, end []byte) (Iterator, error)
	ScanKeysOnly(index IndexType, start, end []byte) (Iterator, error)
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
	iter, err := r.ScanKeysOnly(TAEV, []byte{byte(TAEV)}, []byte{byte(TAEV) + 1})
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
	start, end := r.Encoder().EncodePrefixRange(EAVT, e.Bytes())
	iter, err := r.ScanKeysOnly(EAVT, start, end)
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
