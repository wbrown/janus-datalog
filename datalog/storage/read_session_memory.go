package storage

import (
	"sync"

	"github.com/google/btree"
	"github.com/wbrown/janus-datalog/datalog"
)

// memoryReadSession is a consistent read view over MemoryStore: the key
// B-tree is cloned copy-on-write at open (O(1)), so writes committed after
// the session opened never surface through it. Datoms are wholly encoded in
// their keys and blobs are content-addressed, so sharing the entries map is
// sound for append-only operation; a hard DeleteDatoms mid-session surfaces
// as a blob-read miss — the same exposure today's per-scan key copy has.
type memoryReadSession struct {
	store *MemoryStore
	keys  *btree.BTreeG[string]

	mu     sync.Mutex
	closed bool
}

// NewReadSession opens a consistent read view at the store's current state.
// Clone mutates the source tree's copy-on-write context, so it runs under
// the store's write lock, exclusive with committers and other sessions.
func (s *MemoryStore) NewReadSession() (ReadSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	return &memoryReadSession{store: s, keys: s.keys.Clone()}, nil
}

func (s *memoryReadSession) Encoder() *BinaryKeyEncoder { return s.store.encoder }

func (s *memoryReadSession) Scan(bound ScanBound) (Iterator, error) {
	return s.scan(bound)
}

func (s *memoryReadSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.scan(bound)
}

func (s *memoryReadSession) scan(bound ScanBound) (Iterator, error) {
	run, err := s.store.encoder.EncodeScanBound(bound)
	if err != nil {
		return nil, err
	}
	index := bound.Index

	s.mu.Lock()
	keysTree := s.keys
	closed := s.closed
	s.mu.Unlock()
	if closed || keysTree == nil {
		return nil, errReadSessionClosed
	}
	// The cloned tree is never written, so the walk needs no store lock.
	keys := make([][]byte, 0)
	keysTree.AscendRange(string(run.Start), string(run.End), func(encoded string) bool {
		key := []byte(encoded)
		if len(key) == 21 && key[0] == blobKeyPrefix {
			return true
		}
		keys = append(keys, key)
		return true
	})
	return &memoryIterator{
		index:    index,
		keys:     keys,
		position: -1,
		run:      run,
		encoder:  s.store.encoder,
		blobs:    memoryBlobReader{store: s.store},
	}, nil
}

func (s *memoryReadSession) MaxElementID() (datalog.ElementID, error) {
	return maxElementIDByScan(s)
}

func (s *memoryReadSession) MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error) {
	return maxTxForEntityByScan(s, e)
}

// Close releases the snapshot. Safe to call multiple times.
func (s *memoryReadSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.keys = nil
	return nil
}
