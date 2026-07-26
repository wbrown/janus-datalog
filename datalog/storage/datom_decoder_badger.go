//go:build !(js && wasm)

package storage

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
)

// KeyOnlyIterator wraps a BadgerIterator to decode datoms from keys.
type KeyOnlyIterator struct {
	*BadgerIterator
	encoder *BinaryKeyEncoder
	blobs   BlobReader
	// run is the bound this iterator is walking, projected onto keys. Its
	// range positioned the Badger cursor; its membership test drops the keys
	// the range over-covers, which happens whenever a bound component is a
	// variable-length V. Seek replaces it, because a seek names a new run.
	run          EncodedRun
	currentDatom datalog.Datom
	hasDatom     bool
	currentError error
}

func NewKeyOnlyIterator(store *BadgerStore, index IndexType, run EncodedRun) (Iterator, error) {
	if store.db.IsClosed() {
		return nil, badger.ErrDBClosed
	}
	txn := store.db.NewTransaction(false)
	options := badger.DefaultIteratorOptions
	options.PrefetchSize = 10000
	options.PrefetchValues = false
	iterator := txn.NewIterator(options)
	badgerIterator := &BadgerIterator{
		txn:   txn,
		it:    iterator,
		start: run.Start,
		end:   run.End,
		index: index,
	}
	runtime.SetFinalizer(badgerIterator, (*BadgerIterator).Close)
	return &KeyOnlyIterator{
		BadgerIterator: badgerIterator,
		encoder:        store.encoder,
		blobs:          badgerTxnBlobReader{txn: txn},
		run:            run,
	}, nil
}

// Next advances to the next key the run holds, stepping over the keys its byte
// range over-covers.
func (i *KeyOnlyIterator) Next() bool {
	if i.currentError != nil {
		return false
	}
	i.hasDatom = false
	for i.BadgerIterator.Next() {
		if i.run.Holds(i.BadgerIterator.Key()) {
			return true
		}
	}
	return false
}

func (i *KeyOnlyIterator) Key() []byte {
	if !i.positionedInRange() {
		return nil
	}
	return i.BadgerIterator.Key()
}

func (i *KeyOnlyIterator) Datom() (*datalog.Datom, error) {
	if i.currentError != nil {
		return nil, i.currentError
	}
	// Next() returns false at the exclusive end bound while the underlying
	// Badger iterator can still be Valid() on the successor key. Refuse that
	// out-of-range position so Datom() matches the ScanKeysOnly contract.
	if !i.positionedInRange() {
		return nil, fmt.Errorf("no current datom")
	}
	if !i.hasDatom {
		datom, err := decodeDatomFromKey(
			i.index,
			i.it.Item().Key(),
			i.encoder,
			i.blobs,
		)
		if err != nil {
			i.currentError = err
			return nil, err
		}
		i.currentDatom = datom
		i.hasDatom = true
	}
	return &i.currentDatom, nil
}

func (i *KeyOnlyIterator) positionedInRange() bool {
	if !i.BadgerIterator.valid || i.it == nil || !i.it.Valid() {
		return false
	}
	if i.end == nil {
		return true
	}
	return bytes.Compare(i.it.Item().Key(), i.end) < 0
}

func (i *KeyOnlyIterator) Seek(bound ScanBound) {
	if i.currentError != nil {
		return
	}
	run, err := i.encoder.EncodeScanBound(bound)
	if err != nil {
		// Seek cannot return; the failure becomes the iterator's sticky error
		// rather than a silently unmoved cursor.
		i.currentError = err
		return
	}
	// The seek names a new run inside the scan's range: its start repositions
	// the cursor, and its membership test governs what follows. The range end
	// stays the scan's — a seek moves within a scan, it does not open one.
	i.run.exact, i.run.memberSize = run.exact, run.memberSize
	i.BadgerIterator.Seek(run.Start)
	i.hasDatom = false
}

func (i *KeyOnlyIterator) Close() error {
	i.hasDatom = false
	return i.BadgerIterator.Close()
}

func (i *KeyOnlyIterator) Error() error {
	return i.currentError
}
