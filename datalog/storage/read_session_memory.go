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

func (s *memoryReadSession) Scan(index IndexType, start, end []byte) (Iterator, error) {
	return s.scan(index, start, end)
}

func (s *memoryReadSession) ScanKeysOnly(index IndexType, start, end []byte) (Iterator, error) {
	return s.scan(index, start, end)
}

func (s *memoryReadSession) scan(index IndexType, start, end []byte) (Iterator, error) {
	s.mu.Lock()
	keysTree := s.keys
	closed := s.closed
	s.mu.Unlock()
	if closed || keysTree == nil {
		return nil, errReadSessionClosed
	}
	if start == nil {
		start = []byte{byte(index)}
	}
	if end == nil {
		end = []byte{byte(index) + 1}
	}
	// The cloned tree is never written, so the walk needs no store lock.
	keys := make([][]byte, 0)
	keysTree.AscendRange(string(start), string(end), func(encoded string) bool {
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
		encoder:  s.store.encoder,
		blobs:    memoryBlobReader{store: s.store},
	}, nil
}

func (s *memoryReadSession) MaxElementID() (datalog.ElementID, error) {
	return maxElementIDByScan(s)
}

func (s *memoryReadSession) MaxElementIDForAttribute(a []byte) (datalog.ElementID, error) {
	return maxElementIDForAttributeByScan(s, a)
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
