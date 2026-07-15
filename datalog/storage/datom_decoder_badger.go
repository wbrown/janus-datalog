//go:build !(js && wasm)

package storage

import (
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
	if !i.BadgerIterator.Next() {
		return false
	}
	i.currentDatom, i.currentError = decodeDatomFromKey(
		i.index,
		i.it.Item().Key(),
		i.encoder,
		i.blobs,
	)
	if i.currentError != nil {
		return false
	}
	i.hasDatom = true
	return true
}

func (i *KeyOnlyIterator) Datom() (*datalog.Datom, error) {
	if i.currentError != nil {
		return nil, i.currentError
	}
	if !i.hasDatom {
		return nil, fmt.Errorf("no current datom")
	}
	return &i.currentDatom, nil
}

func (i *KeyOnlyIterator) Seek(key []byte) {
	if i.currentError != nil {
		return
	}
	i.BadgerIterator.Seek(key)
	i.hasDatom = false
}

func (i *KeyOnlyIterator) Close() error {
	i.hasDatom = false
	return i.BadgerIterator.Close()
}

func (i *KeyOnlyIterator) Error() error {
	return i.currentError
}
