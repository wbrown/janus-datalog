package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/google/btree"
	"github.com/wbrown/janus-datalog/datalog"
)

var errMemoryStoreClosed = errors.New("memory store closed")

// memoryKeyTreeDegree is the B-tree branching factor for the key index.
// Degree 32 is a common in-memory default (nodes hold up to 63 items).
const memoryKeyTreeDegree = 32

// MemoryStore is an ordered, transactional, in-memory implementation of Store.
// It persists the same binary keys as BadgerStore, including all eight indices,
// metadata, and content-addressed blobs.
//
// Commits apply Assert/Retract in call order against the live map under the
// write lock, journaling prior values so a failed apply can restore them.
// This matches BadgerTx immediacy for interleaving and avoids cloning the
// whole store on every commit (required for Import and other batched writes).
//
// keys is a B-tree of entries map keys, kept in lockstep with puts and deletes
// so retract and scan can Seek/prefix-iterate in O(log N + range) instead of
// scanning the whole map (Badger's Seek/ValidForPrefix analogue). Insert and
// delete are O(log N) per key, so Import/batched Assert stay O(M log N) in the
// key index.
type MemoryStore struct {
	mu      sync.RWMutex
	encoder *BinaryKeyEncoder
	entries map[string][]byte
	keys    *btree.BTreeG[string]
	closed  bool
}

var _ Store = (*MemoryStore)(nil)

func newMemoryKeyTree() *btree.BTreeG[string] {
	return btree.NewOrderedG[string](memoryKeyTreeDegree)
}

func NewMemoryStore(encoder *BinaryKeyEncoder) *MemoryStore {
	if encoder == nil {
		encoder = &BinaryKeyEncoder{}
	}
	return &MemoryStore{
		encoder: encoder,
		entries: make(map[string][]byte),
		keys:    newMemoryKeyTree(),
	}
}

func (s *MemoryStore) Encoder() *BinaryKeyEncoder {
	return s.encoder
}

func (s *MemoryStore) Assert(datoms []datalog.Datom) error {
	tx, err := s.BeginTx()
	if err != nil {
		return err
	}
	if err := tx.Assert(datoms); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// AssertEach gathers what produce yields and asserts it. Gathering is the whole
// of it here: entries are a map keyed by encoded key, so there is no incremental
// structure to build into and a per-datom path would save nothing.
func (s *MemoryStore) AssertEach(produce func(add func(*datalog.Datom) error) error) error {
	var datoms []datalog.Datom
	if err := produce(func(d *datalog.Datom) error {
		datoms = append(datoms, *d)
		return nil
	}); err != nil {
		return err
	}
	return s.Assert(datoms)
}

// FinishBatch has nothing to complete: AssertEach writes its datoms before
// returning.
func (s *MemoryStore) FinishBatch() error { return nil }

func (s *MemoryStore) Retract(datoms []datalog.Datom) error {
	tx, err := s.BeginTx()
	if err != nil {
		return err
	}
	if err := tx.Retract(datoms); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// DeleteDatoms removes each datom's keys from every index, and then any blob
// those datoms were the last to refer to.
func (s *MemoryStore) DeleteDatoms(datoms []datalog.Datom) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errMemoryStoreClosed
	}
	candidates := make(map[[20]byte]struct{})
	for i := range datoms {
		sd := ToStorageDatom(datoms[i])
		vBytes, blobData := s.encoder.EncodeValueBytes(sd.V)
		if blobData != nil {
			candidates[blobData.Hash] = struct{}{}
		}
		for _, index := range Indices {
			key := string(s.encoder.encodeKeyWithParts(index, &sd, vBytes))
			if _, ok := s.entries[key]; !ok {
				continue
			}
			delete(s.entries, key)
			s.keys.Delete(key)
		}
	}
	s.reclaimBlobs(candidates)
	return len(datoms), nil
}

// unreferencedBlobs returns the candidates no datom still refers to, counted
// against the index as it stands — so a caller that has already applied its
// deletes gets survivors. The caller holds the lock.
func (s *MemoryStore) unreferencedBlobs(candidates map[[20]byte]struct{}) [][20]byte {
	var garbage [][20]byte
	for hash := range candidates {
		if !blobIsReferenced(s.encoder, hash, s.hasKeyWithPrefix) {
			garbage = append(garbage, hash)
		}
	}
	return garbage
}

// reclaimBlobs deletes each candidate blob that no datom still refers to. The
// count is taken here, against the index the delete loop has already finished
// mutating, so it counts survivors rather than the population before the delete.
// Held under the same write lock as those deletes, so nothing can add a
// reference between the count and the removal.
func (s *MemoryStore) reclaimBlobs(candidates map[[20]byte]struct{}) {
	for _, hash := range s.unreferencedBlobs(candidates) {
		key := blobKey(hash)
		delete(s.entries, string(key[:]))
		s.keys.Delete(string(key[:]))
	}
}

// hasKeyWithPrefix reports whether any stored key begins with prefix, seeking
// the key index rather than walking the map. The caller holds the lock.
func (s *MemoryStore) hasKeyWithPrefix(prefix []byte) bool {
	pivot := string(prefix)
	found := false
	s.keys.AscendGreaterOrEqual(pivot, func(key string) bool {
		found = strings.HasPrefix(key, pivot)
		return false
	})
	return found
}

func (s *MemoryStore) Scan(bound ScanBound) (Iterator, error) {
	return s.scan(bound)
}

func (s *MemoryStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	return s.scan(bound)
}

// scan projects the bound through the binary encoder because MemoryStore keys
// on the same bytes Badger does. That is this backend's choice, not the seam's:
// a store comparing typed components directly would satisfy the same interface
// without encoding anything.
func (s *MemoryStore) scan(bound ScanBound) (Iterator, error) {
	run, err := s.encoder.EncodeScanBound(bound)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	index := bound.Index
	startKey := string(run.Start)
	endKey := string(run.End)
	keys := make([][]byte, 0)
	s.keys.AscendRange(startKey, endKey, func(encoded string) bool {
		key := []byte(encoded)
		if len(key) == 21 && key[0] == blobKeyPrefix {
			return true
		}
		keys = append(keys, append([]byte(nil), key...))
		return true
	})
	return &memoryIterator{
		index:      index,
		keys:       keys,
		position:   -1,
		membership: run.Membership,
		end:        run.End,
		encoder:    s.encoder,
		blobs:      memoryBlobReader{store: s},
	}, nil
}

func (s *MemoryStore) MaxElementID() (datalog.ElementID, error) {
	iter, err := s.ScanKeysOnly(ScanBound{Index: TAEV})
	if err != nil {
		return datalog.ElementID{}, err
	}
	defer iter.Close()
	if !iter.Next() {
		return datalog.ElementID{}, iter.Error()
	}
	return iter.ElementID(), nil
}

func (s *MemoryStore) MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error) {
	iter, err := s.ScanKeysOnly(ScanBound{Index: EAVT, Prefix: []datalog.Value{e}})
	if err != nil {
		return datalog.ElementID{}, false, err
	}
	defer iter.Close()
	var maxID datalog.ElementID
	found := false
	for iter.Next() {
		tx := iter.ElementID()
		if !found || maxID.Less(tx) {
			maxID = tx
			found = true
		}
	}
	if err := iter.Error(); err != nil {
		return datalog.ElementID{}, false, err
	}
	return maxID, found, nil
}

func (s *MemoryStore) DatomsAfter(eid datalog.ElementID) ([]datalog.Datom, error) {
	iter, err := s.ScanKeysOnly(ScanBound{Index: TAEV})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var datoms []datalog.Datom
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			return nil, err
		}
		if !eid.Less(datom.Tx) {
			break
		}
		datoms = append(datoms, *datom)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return datoms, nil
}

func (s *MemoryStore) GetMetadataUint64(key string) (uint64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, false, errMemoryStoreClosed
	}
	value, ok := s.entries[metadataPrefix+key]
	if !ok {
		return 0, false, nil
	}
	if len(value) != 8 {
		return 0, false, fmt.Errorf("metadata %s has invalid length %d, expected 8", key, len(value))
	}
	return binary.BigEndian.Uint64(value), true, nil
}

func (s *MemoryStore) SetMetadataUint64(key string, value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errMemoryStoreClosed
	}
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	metaKey := metadataPrefix + key
	if _, had := s.entries[metaKey]; !had {
		s.keys.ReplaceOrInsert(metaKey)
	}
	s.entries[metaKey] = encoded
	return nil
}

func (s *MemoryStore) BeginTx() (StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	return &memoryStoreTx{store: s}, nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

type memoryTxOpKind uint8

const (
	memoryTxAssert memoryTxOpKind = iota
	memoryTxRetract
)

type memoryTxOp struct {
	kind   memoryTxOpKind
	datoms []datalog.Datom
}

type memoryEntryUndo struct {
	key string
	had bool
	val []byte
}

type memoryStoreTx struct {
	store *MemoryStore
	ops   []memoryTxOp
	done  bool
}

func (tx *memoryStoreTx) Assert(datoms []datalog.Datom) error {
	if tx.done {
		return errors.New("memory transaction closed")
	}
	if len(datoms) == 0 {
		return nil
	}
	tx.ops = append(tx.ops, memoryTxOp{
		kind:   memoryTxAssert,
		datoms: append([]datalog.Datom(nil), datoms...),
	})
	return nil
}

func (tx *memoryStoreTx) Retract(datoms []datalog.Datom) error {
	if tx.done {
		return errors.New("memory transaction closed")
	}
	if len(datoms) == 0 {
		return nil
	}
	tx.ops = append(tx.ops, memoryTxOp{
		kind:   memoryTxRetract,
		datoms: append([]datalog.Datom(nil), datoms...),
	})
	return nil
}

func (tx *memoryStoreTx) Commit() error {
	if tx.done {
		return errors.New("memory transaction closed")
	}
	tx.done = true
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	if tx.store.closed {
		return errMemoryStoreClosed
	}
	var undo []memoryEntryUndo
	// The journal exists only for restoreMemoryEntries below, which only a
	// failing retract reaches: assertMemoryDatom has no error path, so once an
	// assert-only transaction starts writing it cannot stop. Journaling it would
	// record every index entry for a rollback that cannot happen — 30% of the
	// allocation in a bulk import.
	var journal *[]memoryEntryUndo
	if opsContainRetract(tx.ops) {
		journal = &undo
	}
	// Commit already holds the write lock; blob reads must not re-enter the mutex.
	reader := memoryEntriesBlobReader{entries: tx.store.entries}
	blobCandidates := make(map[[20]byte]struct{})
	for i := range tx.ops {
		op := &tx.ops[i]
		switch op.kind {
		case memoryTxAssert:
			for j := range op.datoms {
				assertMemoryDatom(tx.store, &op.datoms[j], journal)
			}
		case memoryTxRetract:
			for j := range op.datoms {
				if err := retractMemoryDatom(tx.store, reader, &op.datoms[j], journal, blobCandidates); err != nil {
					restoreMemoryEntries(tx.store, undo)
					return err
				}
			}
		}
	}
	// Every op has been applied, so the count below sees this transaction's final
	// state — a value retracted and asserted again keeps its blob. The deletes are
	// journaled like any other, so a rollback restores the blob with the datoms.
	for _, hash := range tx.store.unreferencedBlobs(blobCandidates) {
		key := blobKey(hash)
		deleteMemoryEntry(tx.store, &undo, string(key[:]))
	}
	return nil
}

func (tx *memoryStoreTx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	return nil
}

// opsContainRetract reports whether the transaction can still fail once it
// starts writing. Only a retract can: it decodes what it finds in the keyspace,
// and a malformed key rejects.
func opsContainRetract(ops []memoryTxOp) bool {
	for i := range ops {
		if ops[i].kind == memoryTxRetract {
			return true
		}
	}
	return false
}

// A nil undo means the caller has established that this transaction cannot
// fail, so there is nothing to restore to.
func putMemoryEntry(store *MemoryStore, undo *[]memoryEntryUndo, key string, value []byte) {
	old, had := store.entries[key]
	if undo != nil {
		*undo = append(*undo, memoryEntryUndo{
			key: key,
			had: had,
			val: append([]byte(nil), old...),
		})
	}
	if !had {
		store.keys.ReplaceOrInsert(key)
	}
	if value == nil {
		store.entries[key] = nil
		return
	}
	store.entries[key] = append([]byte(nil), value...)
}

func deleteMemoryEntry(store *MemoryStore, undo *[]memoryEntryUndo, key string) {
	old, had := store.entries[key]
	if !had {
		return
	}
	*undo = append(*undo, memoryEntryUndo{
		key: key,
		had: true,
		val: append([]byte(nil), old...),
	})
	delete(store.entries, key)
	store.keys.Delete(key)
}

func restoreMemoryEntries(store *MemoryStore, undo []memoryEntryUndo) {
	for i := len(undo) - 1; i >= 0; i-- {
		entry := undo[i]
		if entry.had {
			_, exists := store.entries[entry.key]
			store.entries[entry.key] = entry.val
			if !exists {
				store.keys.ReplaceOrInsert(entry.key)
			}
			continue
		}
		delete(store.entries, entry.key)
		store.keys.Delete(entry.key)
	}
}

func memoryKeysWithPrefix(keys *btree.BTreeG[string], prefix []byte) []string {
	p := string(prefix)
	var matches []string
	keys.AscendGreaterOrEqual(p, func(encoded string) bool {
		if !strings.HasPrefix(encoded, p) {
			return false
		}
		matches = append(matches, encoded)
		return true
	})
	return matches
}

func assertMemoryDatom(store *MemoryStore, datom *datalog.Datom, undo *[]memoryEntryUndo) {
	valueBytes, blob := store.encoder.EncodeValueBytes(datom.V)
	if blob != nil {
		var key [21]byte
		key[0] = blobKeyPrefix
		copy(key[1:], blob.Hash[:])
		putMemoryEntry(store, undo, string(key[:]), blob.CompressedBytes)
	}
	sd := ToStorageDatom(*datom)
	for _, index := range Indices {
		key := store.encoder.encodeKeyWithParts(index, &sd, valueBytes)
		putMemoryEntry(store, undo, string(key), nil)
	}
}

// retractMemoryDatom deletes every stored datom matching the given (E, A, V) at
// any Tx, recording in candidates the blob hash of each value that lived out of
// line so the commit can ask afterwards whether anything still refers to it.
func retractMemoryDatom(
	store *MemoryStore,
	blobs BlobReader,
	datom *datalog.Datom,
	undo *[]memoryEntryUndo,
	candidates map[[20]byte]struct{},
) error {
	storageDatom := ToStorageDatom(*datom)
	valueBytes := encodeValueForSearch(storageDatom.V, store.encoder)
	searchPrefix := concatBytes(
		[]byte{byte(EAVT)},
		storageDatom.E[:],
		storageDatom.A[:],
		valueBytes,
	)
	matches := memoryKeysWithPrefix(store.keys, searchPrefix)
	for _, encoded := range matches {
		stored, err := decodeDatomFromKey(EAVT, []byte(encoded), store.encoder, blobs)
		if err != nil {
			return err
		}
		sdStored := ToStorageDatom(stored)
		storedVBytes, blobData := store.encoder.EncodeValueBytes(sdStored.V)
		if blobData != nil {
			candidates[blobData.Hash] = struct{}{}
		}
		for _, index := range Indices {
			deleteMemoryEntry(store, undo, string(store.encoder.encodeKeyWithParts(index, &sdStored, storedVBytes)))
		}
	}
	return nil
}

type memoryBlobReader struct {
	store *MemoryStore
}

type memoryEntriesBlobReader struct {
	entries map[string][]byte
}

func (r memoryBlobReader) ReadBlob(hash [20]byte, read func([]byte) error) error {
	r.store.mu.RLock()
	defer r.store.mu.RUnlock()
	if r.store.closed {
		return errMemoryStoreClosed
	}
	return readMemoryBlob(r.store.entries, hash, read)
}

func (r memoryEntriesBlobReader) ReadBlob(hash [20]byte, read func([]byte) error) error {
	return readMemoryBlob(r.entries, hash, read)
}

func readMemoryBlob(entries map[string][]byte, hash [20]byte, read func([]byte) error) error {
	var key [21]byte
	key[0] = blobKeyPrefix
	copy(key[1:], hash[:])
	value, ok := entries[string(key[:])]
	if !ok {
		return fmt.Errorf("blob not found for hash %x", hash)
	}
	return read(value)
}

type memoryIterator struct {
	index    IndexType
	keys     [][]byte
	position int
	// scanned counts keys taken from the range, including the ones the
	// membership rule then rejects — see Scanned.
	scanned int
	// membership decides which of the selected keys the current bound names,
	// dropping the ones the range over-covers when a bound component is a
	// variable-length V. Seek replaces it alongside end, because a run is its
	// start, its end and its membership rule together, and adopting a subset of
	// the three yields a run nobody asked for.
	membership runMembership
	// end is the current run's exclusive upper bound. keys already holds only
	// the scan's own range, so this is what a seek to a narrower run inside it
	// stops at.
	end          []byte
	encoder      *BinaryKeyEncoder
	blobs        BlobReader
	currentDatom datalog.Datom
	hasDatom     bool
	err          error
	closed       bool
}

// positioned reports whether the cursor is on a key this scan may expose: a key
// exists at the cursor, and the bound's membership rule holds it. Next, Key and
// Datom all consult it, so there is one notion of "current".
func (i *memoryIterator) positioned() bool {
	if i.closed || i.position < 0 || i.position >= len(i.keys) {
		return false
	}
	if i.end != nil && bytes.Compare(i.keys[i.position], i.end) >= 0 {
		return false
	}
	return i.membership.holds(i.keys[i.position])
}

// Next advances to the next key the run holds, stepping over the keys its byte
// range over-covers.
func (i *memoryIterator) Next() bool {
	if i.closed || i.err != nil {
		return false
	}
	i.hasDatom = false
	for {
		i.position++
		if i.position >= len(i.keys) {
			return false
		}
		// Past the run's end ends the iteration; it is not a key to step over.
		// The membership rule rejects keys *inside* the run, so treating this
		// one the same way would walk the rest of the scan looking for a member
		// that cannot be there, and count every key of it as intake.
		if i.end != nil && bytes.Compare(i.keys[i.position], i.end) >= 0 {
			return false
		}
		// Counted before the membership test, not after: a key the range
		// covered and the bound rejected is still intake.
		i.scanned++
		if i.positioned() {
			return true
		}
	}
}

// Scanned reports keys taken from the index inside this iterator's range.
func (i *memoryIterator) Scanned() int { return i.scanned }

func (i *memoryIterator) Key() []byte {
	if !i.positioned() {
		return nil
	}
	return i.keys[i.position]
}

func (i *memoryIterator) Datom() (*datalog.Datom, error) {
	if i.err != nil {
		return nil, i.err
	}
	if !i.positioned() {
		return nil, errors.New("no current datom")
	}
	if !i.hasDatom {
		datom, err := decodeDatomFromKey(
			i.index,
			i.keys[i.position],
			i.encoder,
			i.blobs,
		)
		if err != nil {
			i.err = err
			return nil, err
		}
		i.currentDatom = datom
		i.hasDatom = true
	}
	return &i.currentDatom, nil
}

func (i *memoryIterator) Close() error {
	i.closed = true
	i.hasDatom = false
	return nil
}

func (i *memoryIterator) Seek(bound ScanBound) {
	if i.closed || i.err != nil {
		return
	}
	run, err := i.encoder.EncodeScanBound(bound)
	if err != nil {
		// Seek cannot return; the failure becomes the iterator's sticky error
		// rather than a silently unmoved cursor.
		i.err = err
		return
	}
	// The seek names a new run inside the scan's keys, and it names all of it:
	// the start repositions the cursor, the end stops it, and the membership
	// rule governs what lies between. All three come from one EncodedRun, so a
	// caller gets the run it asked for rather than its start and the scan's
	// remainder.
	i.membership = run.Membership
	i.end = run.End
	i.position = sort.Search(len(i.keys), func(index int) bool {
		return bytes.Compare(i.keys[index], run.Start) >= 0
	}) - 1
	i.hasDatom = false
}

func (i *memoryIterator) ElementID() datalog.ElementID {
	if !i.positioned() {
		return datalog.ElementID{}
	}
	return extractElementIDFromKey(i.index, i.keys[i.position])
}

func (i *memoryIterator) Error() error {
	return i.err
}
