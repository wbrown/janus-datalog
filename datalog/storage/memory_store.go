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
// key index (not O(M²) from mid-slice shifts on a sorted []string).
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

func (s *MemoryStore) DeleteDatoms(datoms []datalog.Datom) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errMemoryStoreClosed
	}
	for i := range datoms {
		sd := ToStorageDatom(datoms[i])
		vBytes, _ := s.encoder.EncodeValueBytes(sd.V)
		for _, index := range Indices {
			key := string(s.encoder.encodeKeyWithParts(index, &sd, vBytes))
			if _, ok := s.entries[key]; !ok {
				continue
			}
			delete(s.entries, key)
			s.keys.Delete(key)
		}
	}
	return len(datoms), nil
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
	// Commit already holds the write lock; blob reads must not re-enter the mutex.
	reader := memoryEntriesBlobReader{entries: tx.store.entries}
	for i := range tx.ops {
		op := &tx.ops[i]
		switch op.kind {
		case memoryTxAssert:
			for j := range op.datoms {
				assertMemoryDatom(tx.store, &op.datoms[j], &undo)
			}
		case memoryTxRetract:
			for j := range op.datoms {
				if err := retractMemoryDatom(tx.store, reader, &op.datoms[j], &undo); err != nil {
					restoreMemoryEntries(tx.store, undo)
					return err
				}
			}
		}
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

func putMemoryEntry(store *MemoryStore, undo *[]memoryEntryUndo, key string, value []byte) {
	old, had := store.entries[key]
	*undo = append(*undo, memoryEntryUndo{
		key: key,
		had: had,
		val: append([]byte(nil), old...),
	})
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

func retractMemoryDatom(
	store *MemoryStore,
	blobs BlobReader,
	datom *datalog.Datom,
	undo *[]memoryEntryUndo,
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
		storedVBytes, _ := store.encoder.EncodeValueBytes(sdStored.V)
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
	if i.closed || i.position < 0 || i.position >= len(i.keys) {
		return datalog.ElementID{}
	}
	return extractElementIDFromKey(i.index, i.keys[i.position])
}

func (i *memoryIterator) Error() error {
	return i.err
}
