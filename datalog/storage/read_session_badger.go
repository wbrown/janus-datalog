//go:build !(js && wasm)

package storage

import (
	"runtime"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
)

// badgerReadSession pins one read-only Badger transaction for a query's
// lifetime. Handle creation (NewIterator, Get) is serialized behind mu per
// Badger's documented contract — Txn APIs are called serially — while
// iteration on the created iterators runs concurrently, which the same
// contract permits for read-only transactions.
type badgerReadSession struct {
	store *BadgerStore

	mu        sync.Mutex
	txn       *badger.Txn
	openIters map[*badger.Iterator]struct{}
	closed    bool
}

// NewReadSession opens a consistent read view at the store's current state.
func (s *BadgerStore) NewReadSession() (ReadSession, error) {
	if s.db.IsClosed() {
		return nil, badger.ErrDBClosed
	}
	session := &badgerReadSession{
		store:     s,
		txn:       s.db.NewTransaction(false),
		openIters: make(map[*badger.Iterator]struct{}),
	}
	runtime.SetFinalizer(session, func(rs *badgerReadSession) { _ = rs.Close() })
	return session, nil
}

func (s *badgerReadSession) Scan(bound ScanBound) (Iterator, error) {
	return s.newIterator(bound)
}

func (s *badgerReadSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.newIterator(bound)
}

func (s *badgerReadSession) newIterator(bound ScanBound) (Iterator, error) {
	run, err := s.store.encoder.EncodeScanBound(bound)
	if err != nil {
		return nil, err
	}
	index := bound.Index

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errReadSessionClosed
	}
	options := badger.DefaultIteratorOptions
	options.PrefetchSize = 10000
	options.PrefetchValues = false
	iterator := s.txn.NewIterator(options)
	s.openIters[iterator] = struct{}{}
	badgerIterator := &BadgerIterator{
		txn:     s.txn,
		it:      iterator,
		start:   run.Start,
		end:     run.End,
		index:   index,
		release: func() { s.releaseIterator(iterator) },
	}
	// No per-iterator finalizer: the session force-closes stragglers on
	// Close, and the session's own finalizer backstops the transaction.
	return &KeyOnlyIterator{
		BadgerIterator: badgerIterator,
		encoder:        s.store.encoder,
		blobs:          badgerTxnBlobReader{txn: s.txn},
		run:            run,
	}, nil
}

func (s *badgerReadSession) releaseIterator(it *badger.Iterator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.openIters, it)
}

func (s *badgerReadSession) MaxElementID() (datalog.ElementID, error) {
	return maxElementIDByScan(s)
}

func (s *badgerReadSession) MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error) {
	return maxTxForEntityByScan(s, e)
}

// Close discards the snapshot. Iterators still open — leaks — are closed
// first, because Badger refuses to discard a transaction with live
// iterators. Safe to call multiple times; a straggler's own Close after the
// session closed is a no-op on the already-closed Badger iterator.
func (s *badgerReadSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	runtime.SetFinalizer(s, nil)
	for it := range s.openIters {
		it.Close()
	}
	s.openIters = nil
	s.txn.Discard()
	s.txn = nil
	return nil
}
