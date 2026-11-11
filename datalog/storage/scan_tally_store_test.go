package storage

import "sync/atomic"

// scanTallyStore counts the scans it and its read sessions open and the
// scans closed.
type scanTallyStore struct {
	Store
	opened, closed *atomic.Int32
}

func (s scanTallyStore) Scan(bound ScanBound) (Iterator, error) {
	return s.counted(s.Store.Scan(bound))
}

func (s scanTallyStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.counted(s.Store.ScanKeysOnly(bound))
}

func (s scanTallyStore) NewReadSession() (ReadSession, error) {
	session, err := s.Store.NewReadSession()
	if err != nil {
		return nil, err
	}
	return scanTallySession{ReadSession: session, store: s}, nil
}

func (s scanTallyStore) counted(it Iterator, err error) (Iterator, error) {
	if err != nil {
		return it, err
	}
	s.opened.Add(1)
	return &scanTallyIterator{Iterator: it, closed: s.closed}, nil
}

type scanTallySession struct {
	ReadSession
	store scanTallyStore
}

func (s scanTallySession) Scan(bound ScanBound) (Iterator, error) {
	return s.store.counted(s.ReadSession.Scan(bound))
}

func (s scanTallySession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.store.counted(s.ReadSession.ScanKeysOnly(bound))
}

type scanTallyIterator struct {
	Iterator
	closed *atomic.Int32
	done   bool
}

func (it *scanTallyIterator) Close() error {
	if !it.done {
		it.done = true
		it.closed.Add(1)
	}
	return it.Iterator.Close()
}
