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
	encoder      *BinaryKeyEncoder
	blobs        BlobReader
	currentDatom datalog.Datom
	hasDatom     bool
	currentError error
}

func NewKeyOnlyIterator(store *BadgerStore, index IndexType, start, end []byte) (Iterator, error) {
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
		start: start,
		end:   end,
		index: index,
	}
	runtime.SetFinalizer(badgerIterator, (*BadgerIterator).Close)
	return &KeyOnlyIterator{
		BadgerIterator: badgerIterator,
		encoder:        store.encoder,
		blobs:          badgerTxnBlobReader{txn: txn},
	}, nil
}

func (i *KeyOnlyIterator) Next() bool {
	if i.currentError != nil {
		return false
	}
	i.hasDatom = false
	return i.BadgerIterator.Next()
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
	start, _, err := i.encoder.EncodeScanBound(bound)
	if err != nil {
		// Seek cannot return; the failure becomes the iterator's sticky error
		// rather than a silently unmoved cursor.
		i.currentError = err
		return
	}
	i.BadgerIterator.Seek(start)
	i.hasDatom = false
}

func (i *KeyOnlyIterator) Close() error {
	i.hasDatom = false
	return i.BadgerIterator.Close()
}

func (i *KeyOnlyIterator) Error() error {
	return i.currentError
}
