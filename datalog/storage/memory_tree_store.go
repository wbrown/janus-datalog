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

	// batchMu guards pending and bulk, and serializes the producers writing into
	// them. A builder is a plain B-tree with no locking of its own, so one writer
	// at a time is the whole of its concurrency contract.
	//
	// pending is a batch left open across AssertEach calls, holding the version
	// holder's write lock until FinishBatch publishes it. Readers are unaffected:
	// they load the last published version through an atomic pointer and never
	// take that lock.
	//
	// bulk carries a batch opened over an empty base: its datoms gather here for
	// one sorted build per index at FinishBatch, instead of per-datom inserts.
	batchMu sync.Mutex
	pending *versionBuilder
	bulk    []*datalog.Datom
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
	if len(datoms) == 0 {
		return s.checkOpen()
	}
	if err := s.AssertEach(func(add func(*datalog.Datom) error) error {
		for i := range datoms {
			if err := add(&datoms[i]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return s.FinishBatch()
}

// AssertEach adds what produce yields to the open batch, leaving the batch open
// for the next call; FinishBatch publishes it, and until then the run is
// invisible: readers hold the previously published version.
//
// A batch over an empty base gathers its datoms — duplicate-free, the dump
// format's own property, trusted rather than swept for — and builds every index
// in one sorted pass at FinishBatch, leaves packed full. A batch over existing
// data inserts per datom into the version being built, paying one copy-on-write
// generation for the whole run.
func (s *MemoryTreeStore) AssertEach(produce func(add func(*datalog.Datom) error) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	if s.pending == nil {
		s.pending = s.versions.begin()
	}
	b := s.pending

	// d is each call's workspace; the batch's slab takes the copy.
	var add func(*datalog.Datom) error
	if b.base.datomCount() == 0 {
		add = func(d *datalog.Datom) error {
			s.bulk = append(s.bulk, b.slab.put(d))
			return nil
		}
	} else {
		add = func(d *datalog.Datom) error {
			b.addDatom(d)
			return nil
		}
	}

	if err := produce(add); err != nil {
		// The whole batch goes, not just this call's share of it: a builder is a
		// tree of edits with no record of which call made which, and abandoning
		// is what releases the write lock.
		s.pending = nil
		s.bulk = nil
		s.versions.abandon(b)
		return err
	}
	return nil
}

// FinishBatch publishes an open batch, and is what makes a run of AssertEach
// calls visible. With no batch open there is nothing to publish and it says so
// by doing nothing.
func (s *MemoryTreeStore) FinishBatch() error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	if s.pending == nil {
		return nil
	}
	b := s.pending
	s.pending = nil
	if b.base.datomCount() == 0 {
		datoms := s.bulk
		s.bulk = nil
		s.versions.publishBuilt(b, versionFromDatoms(datoms))
		return nil
	}
	s.versions.publish(b)
	return nil
}

// Retract physically removes every stored datom sharing an (E, A, V) with one
// of those given, whatever its Tx.
//
// This is deletion, not a CRDT Remove: nothing is tombstoned and the history is
// gone. Visible deletion in a user transaction is an OpCRDTRemove datom, which
// Assert writes like any other; truncate removes through DeleteDatoms.
// See BUG_RETRACT_NAMES_TWO_OPPOSITE_OPERATIONS on the name.
func (s *MemoryTreeStore) Retract(datoms []datalog.Datom) error {
	if len(datoms) == 0 {
		return s.checkOpen()
	}
	tx, err := s.BeginTx()
	if err != nil {
		return err
	}
	if err := tx.Retract(datoms); err != nil {
		// Retract released the lock itself when it failed; the transaction is
		// already closed and Rollback would be a second release.
		return err
	}
	return tx.Commit()
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
	return &memoryTreeStoreTx{store: s, b: s.versions.begin()}, nil
}

func (s *MemoryTreeStore) Close() error {
	// An abandoned import leaves its batch open, and with it the write lock. The
	// datoms are discarded rather than published: a run that never finished is a
	// partial dump, and closing is not the caller saying otherwise.
	s.batchMu.Lock()
	if s.pending != nil {
		b := s.pending
		s.pending = nil
		s.bulk = nil
		s.versions.abandon(b)
	}
	s.batchMu.Unlock()

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

// memoryTreeStoreTx holds one open builder and writes into it as each datom
// arrives, so a transaction never accumulates its own copy of what it is about
// to store. The builder owns the write lock from BeginTx until Commit or
// Rollback; Store.BeginTx has one caller, Database.Transaction's commit, which
// opens it, writes already-materialized slices and closes it with no caller
// code in between, so the lock is held for the same window buffering held it.
//
// Rollback is dropping the root: nothing the builder touched was ever reachable,
// so there is no undo journal to get wrong.
//
// The (E, A, V) matches a retraction expands to are read from the transaction's
// own base version, under the write lock it already holds. Reading the store
// before taking that lock would let a concurrent assert slip in between and the
// retraction would silently miss it. Resolving against the base — not the
// builder — is also what keeps a retract from taking a datom asserted beside it
// in the same transaction, whatever order the two arrive in; see
// TestTreeStoreTxRetractAndAssertOrderIndependent.
type memoryTreeStoreTx struct {
	store *MemoryTreeStore
	b     *versionBuilder
	done  bool
}

func (tx *memoryTreeStoreTx) Assert(datoms []datalog.Datom) error {
	if tx.done {
		return errors.New("memory tree transaction closed")
	}
	for i := range datoms {
		tx.b.addDatom(&datoms[i])
	}
	return nil
}

func (tx *memoryTreeStoreTx) Retract(datoms []datalog.Datom) error {
	if tx.done {
		return errors.New("memory tree transaction closed")
	}
	for i := range datoms {
		doomed, err := matchingStoredDatoms(tx.b.base, &datoms[i])
		if err != nil {
			// The builder holds the write lock, so a failed retraction must
			// release it here rather than wait for a Rollback the caller may
			// not make.
			tx.done = true
			tx.store.versions.abandon(tx.b)
			return err
		}
		for _, d := range doomed {
			tx.b.removeDatom(d)
		}
	}
	return nil
}

func (tx *memoryTreeStoreTx) Commit() error {
	if tx.done {
		return errors.New("memory tree transaction closed")
	}
	tx.done = true
	tx.store.versions.publish(tx.b)
	return nil
}

func (tx *memoryTreeStoreTx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	tx.store.versions.abandon(tx.b)
	return nil
}
