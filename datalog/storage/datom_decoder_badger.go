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
	// membership decides which keys in the current run's range the bound names,
	// dropping the ones the range over-covers when a bound component is a
	// variable-length V. Seek replaces it alongside the embedded iterator's end,
	// because a run is its start, its end and its membership rule together, and
	// adopting a subset of the three yields a run nobody asked for.
	membership   runMembership
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
		membership:     run.Membership,
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
		if i.positioned() {
			return true
		}
	}
	return false
}

func (i *KeyOnlyIterator) Key() []byte {
	if !i.positioned() {
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
	if !i.positioned() {
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

// positioned reports whether the cursor is on a key this scan may expose: in
// the byte range, and one the bound's membership rule holds. Next, Key and
// Datom all consult it, so there is one notion of "current".
func (i *KeyOnlyIterator) positioned() bool {
	if !i.BadgerIterator.valid || i.it == nil || !i.it.Valid() {
		return false
	}
	key := i.it.Item().Key()
	if i.end != nil && bytes.Compare(key, i.end) >= 0 {
		return false
	}
	return i.membership.holds(key)
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
	// The seek names a new run inside the scan's keys, and it names all of it:
	// the start repositions the cursor, the end stops it, and the membership
	// rule governs what lies between. All three come from one EncodedRun, so a
	// caller gets the run it asked for.
	//
	// Adopting only the start would leave the iterator walking past the sought
	// bound into whatever the scan's wider range still held, and leave the
	// caller to work out where its own run ended — from the encoded key, the
	// only material it has. That is how key-layout arithmetic gets back above
	// this seam.
	i.membership = run.Membership
	i.end = run.End
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
