package storage

import (
	"errors"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
)

var errMemoryTreeStoreClosed = errors.New("memory tree store is closed")

// MemoryTreeStore is the typed in-memory backend: eight sorted trees of
// datalog.Datom, one per index order, published together as a version.
//
// It is the other half of a pair. MemoryStore emulates Badger — it holds the
// same binary index keys a disk store would, so a process with no disk pays
// BinaryKeyEncoder on every assert and scan. This one compares typed values
// directly and encodes only at the JDZL and EDN boundaries. Both satisfy Store,
// so the backend contract runs against both and any divergence is a test
// failure rather than a discovery.
//
// See MEMORY_DATOM_INDEXES.md. The one place the two backends genuinely differ
// is scan coverage for values whose encodings are prefixes of one another:
// BUG_V_PAYLOAD_NOT_PREFIX_FREE.
type MemoryTreeStore struct {
	versions *versionHolder

	// encoder serves the boundaries only — JDZL and EDN export and import.
	// Nothing on the read or write path consults it, which is the point.
	encoder *BinaryKeyEncoder

	mu       sync.RWMutex
	metadata map[string]uint64
	closed   bool
}

// NewMemoryTreeStore returns an empty typed store.
func NewMemoryTreeStore(encoder *BinaryKeyEncoder) *MemoryTreeStore {
	if encoder == nil {
		encoder = &BinaryKeyEncoder{}
	}
	return &MemoryTreeStore{
		versions: newVersionHolder(),
		encoder:  encoder,
		metadata: make(map[string]uint64),
	}
}

func (s *MemoryTreeStore) Encoder() *BinaryKeyEncoder { return s.encoder }

func (s *MemoryTreeStore) checkOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return errMemoryTreeStoreClosed
	}
	return nil
}

// Assert adds datoms. Every datom enters all eight orders in one batch, so the
// version that publishes them is indivisible.
func (s *MemoryTreeStore) Assert(datoms []datalog.Datom) error {
	return s.applyBatch(nil, datoms)
}

// Retract physically removes every stored datom sharing an (E, A, V) with one
// of those given, whatever its Tx.
//
// This is deletion, not a CRDT Remove: nothing is tombstoned and the history is
// gone. Visible deletion in a user transaction is an OpCRDTRemove datom, which
// Assert writes like any other; truncate removes through DeleteDatoms.
// See BUG_RETRACT_NAMES_TWO_OPPOSITE_OPERATIONS on the name.
func (s *MemoryTreeStore) Retract(datoms []datalog.Datom) error {
	return s.applyBatch(datoms, nil)
}

// applyBatch is the one write path: retractions and assertions land in a single
// version, so no reader can observe half of them.
//
// The (E, A, V) matches a retraction expands to are read from the batch's own
// base version, under the write lock the batch already holds. Reading the store
// before taking that lock would let a concurrent assert slip in between, and the
// retraction would silently miss it.
func (s *MemoryTreeStore) applyBatch(retracts, asserts []datalog.Datom) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(retracts) == 0 && len(asserts) == 0 {
		return nil
	}

	b := s.versions.begin()

	var doomed []*datalog.Datom
	for i := range retracts {
		matches, err := matchingStoredDatoms(b.base, &retracts[i])
		if err != nil {
			s.versions.abandon(b)
			return err
		}
		doomed = append(doomed, matches...)
	}
	for _, d := range doomed {
		b.removeDatom(d)
	}

	for i := range asserts {
		d := asserts[i]
		b.addDatom(&d)
	}

	s.versions.publish(b)
	return nil
}

// DeleteDatoms removes exactly the datoms given — matched on every component,
// Tx included — and reports how many were there.
func (s *MemoryTreeStore) DeleteDatoms(datoms []datalog.Datom) (int, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	b := s.versions.begin()
	removed := 0
	for i := range datoms {
		d := datoms[i]
		if b.removeDatom(&d) {
			removed++
		}
	}
	s.versions.publish(b)
	return removed, nil
}

// matchingStoredDatoms collects every stored datom sharing the (E, A, V) of the
// one given. The scan is drained before anything is removed: a batch reads the
// version it started from, and collecting first keeps the walk independent of
// the mutation.
func matchingStoredDatoms(v *storeVersion, target *datalog.Datom) ([]*datalog.Datom, error) {
	iter, err := v.scan(ScanBound{
		Index:  EAVT,
		Prefix: []datalog.Value{target.E, target.A, target.V},
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var matches []*datalog.Datom
	for iter.Next() {
		d, err := iter.Datom()
		if err != nil {
			return nil, err
		}
		matches = append(matches, d)
	}
	return matches, iter.Error()
}

func (s *MemoryTreeStore) Scan(bound ScanBound) (Iterator, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	return s.versions.read().scan(bound)
}

// ScanKeysOnly is Scan. The distinction exists for a backend whose values live
// apart from its keys; a tree holds whole datoms, so there is nothing to skip
// fetching and nothing to defer.
func (s *MemoryTreeStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.Scan(bound)
}

func (s *MemoryTreeStore) MaxElementID() (datalog.ElementID, error) {
	if err := s.checkOpen(); err != nil {
		return datalog.ElementID{}, err
	}
	return maxElementIDByScan(s)
}

func (s *MemoryTreeStore) MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error) {
	if err := s.checkOpen(); err != nil {
		return datalog.ElementID{}, false, err
	}
	return maxTxForEntityByScan(s, e)
}

// DatomsAfter returns every datom committed after eid, in TAEV order.
//
// TAEV orders Tx descending, so the walk starts at the newest and stops at the
// first entry not after eid — everything past that point is older still.
func (s *MemoryTreeStore) DatomsAfter(eid datalog.ElementID) ([]datalog.Datom, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	iter, err := s.Scan(ScanBound{Index: TAEV})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var out []datalog.Datom
	for iter.Next() {
		d, err := iter.Datom()
		if err != nil {
			return nil, err
		}
		if !eid.Less(d.Tx) {
			break
		}
		out = append(out, *d)
	}
	return out, iter.Error()
}

// GetMetadataUint64 and SetMetadataUint64 sit outside the version. The only key
// is the replica id, written and read at database open, so it never reaches a
// query path and cannot participate in a torn snapshot.
func (s *MemoryTreeStore) GetMetadataUint64(key string) (uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, false, errMemoryTreeStoreClosed
	}
	value, ok := s.metadata[key]
	return value, ok, nil
}

func (s *MemoryTreeStore) SetMetadataUint64(key string, value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryTreeStoreClosed
	}
	s.metadata[key] = value
	return nil
}

// NewReadSession retains the current version. Every read through the session
// walks that one state however many batches commit afterward — the snapshot is
// a held pointer, not a lock and not a copy.
func (s *MemoryTreeStore) NewReadSession() (ReadSession, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	return &memoryTreeReadSession{version: s.versions.read()}, nil
}

func (s *MemoryTreeStore) BeginTx() (StoreTx, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	return &memoryTreeStoreTx{store: s}, nil
}

func (s *MemoryTreeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// memoryTreeReadSession is a retained version. It needs no close-time release:
// dropping the reference is what frees the state, once no other session or
// iterator holds it.
type memoryTreeReadSession struct {
	version *storeVersion
	closed  bool
}

// Close releases the snapshot. Dropping the version reference is what frees the
// state; the flag is what makes a later read a loud error instead of a silent
// one against a session the caller believes it has finished with.
func (s *memoryTreeReadSession) Close() error {
	s.closed = true
	s.version = nil
	return nil
}

func (s *memoryTreeReadSession) Scan(bound ScanBound) (Iterator, error) {
	if s.closed {
		return nil, errReadSessionClosed
	}
	return s.version.scan(bound)
}

func (s *memoryTreeReadSession) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.Scan(bound)
}

func (s *memoryTreeReadSession) MaxElementID() (datalog.ElementID, error) {
	return maxElementIDByScan(s)
}

func (s *memoryTreeReadSession) MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error) {
	return maxTxForEntityByScan(s, e)
}

// memoryTreeStoreTx accumulates a transaction's writes and applies them as one
// batch at commit.
//
// Buffering until commit, rather than holding an open builder, keeps the write
// lock out of the caller's hands: a transaction lives as long as its user wants
// it to, and a builder blocks every other writer for its whole life. Rollback
// is then discarding the buffer — there is no undo journal to get wrong,
// because nothing was applied.
type memoryTreeStoreTx struct {
	store    *MemoryTreeStore
	asserts  []datalog.Datom
	retracts []datalog.Datom
	done     bool
}

func (tx *memoryTreeStoreTx) Assert(datoms []datalog.Datom) error {
	if tx.done {
		return errors.New("memory tree transaction closed")
	}
	tx.asserts = append(tx.asserts, datoms...)
	return nil
}

func (tx *memoryTreeStoreTx) Retract(datoms []datalog.Datom) error {
	if tx.done {
		return errors.New("memory tree transaction closed")
	}
	tx.retracts = append(tx.retracts, datoms...)
	return nil
}

func (tx *memoryTreeStoreTx) Commit() error {
	if tx.done {
		return errors.New("memory tree transaction closed")
	}
	tx.done = true
	return tx.store.applyBatch(tx.retracts, tx.asserts)
}

func (tx *memoryTreeStoreTx) Rollback() error {
	tx.done = true
	tx.asserts = nil
	tx.retracts = nil
	return nil
}
