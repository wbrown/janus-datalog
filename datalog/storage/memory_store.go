package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/wbrown/janus-datalog/datalog"
)

var errMemoryStoreClosed = errors.New("memory store closed")

// MemoryStore is an ordered, transactional, in-memory implementation of Store.
// It persists the same binary keys as BadgerStore, including all eight indices,
// metadata, and content-addressed blobs.
//
// Commits apply Assert/Retract in call order against the live map under the
// write lock, journaling prior values so a failed apply can restore them.
// This matches BadgerTx immediacy for interleaving and avoids cloning the
// whole store on every commit (required for Import and other batched writes).
type MemoryStore struct {
	mu      sync.RWMutex
	encoder *BinaryKeyEncoder
	entries map[string][]byte
	closed  bool
}

var _ Store = (*MemoryStore)(nil)

func NewMemoryStore(encoder *BinaryKeyEncoder) *MemoryStore {
	if encoder == nil {
		encoder = &BinaryKeyEncoder{}
	}
	return &MemoryStore{
		encoder: encoder,
		entries: make(map[string][]byte),
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
		for _, index := range Indices {
			delete(s.entries, string(s.encoder.EncodeKey(index, &datoms[i])))
		}
	}
	return len(datoms), nil
}

func (s *MemoryStore) Get(index IndexType, key []byte) (*datalog.Datom, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	if _, ok := s.entries[string(key)]; !ok {
		return nil, nil
	}
	// Already under RLock; blob reads must not re-enter the mutex.
	datom, err := decodeDatomFromKey(index, key, s.encoder, memoryEntriesBlobReader{entries: s.entries})
	if err != nil {
		return nil, err
	}
	return &datom, nil
}

func (s *MemoryStore) Scan(index IndexType, start, end []byte) (Iterator, error) {
	return s.scan(index, start, end)
}

func (s *MemoryStore) ScanKeysOnly(index IndexType, start, end []byte) (Iterator, error) {
	return s.scan(index, start, end)
}

func (s *MemoryStore) scan(index IndexType, start, end []byte) (Iterator, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errMemoryStoreClosed
	}
	if start == nil {
		start = []byte{byte(index)}
	}
	if end == nil {
		end = []byte{byte(index) + 1}
	}
	keys := make([][]byte, 0)
	for encoded := range s.entries {
		key := []byte(encoded)
		if len(key) == 21 && key[0] == blobKeyPrefix {
			continue
		}
		if bytes.Compare(key, start) < 0 || bytes.Compare(key, end) >= 0 {
			continue
		}
		keys = append(keys, append([]byte(nil), key...))
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	return &memoryIterator{
		index:    index,
		keys:     keys,
		position: -1,
		encoder:  s.encoder,
		blobs:    memoryBlobReader{store: s},
	}, nil
}

func (s *MemoryStore) MaxElementID() (datalog.ElementID, error) {
	iter, err := s.ScanKeysOnly(TAEV, []byte{byte(TAEV)}, []byte{byte(TAEV) + 1})
	if err != nil {
		return datalog.ElementID{}, err
	}
	defer iter.Close()
	if !iter.Next() {
		return datalog.ElementID{}, iter.Error()
	}
	return iter.ElementID(), nil
}

func (s *MemoryStore) MaxElementIDForAttribute(a []byte) (datalog.ElementID, error) {
	start, end := s.encoder.EncodePrefixRange(ATEV, a)
	iter, err := s.ScanKeysOnly(ATEV, start, end)
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
	start, end := s.encoder.EncodePrefixRange(EAVT, e.Bytes())
	iter, err := s.ScanKeysOnly(EAVT, start, end)
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
	iter, err := s.ScanKeysOnly(TAEV, []byte{byte(TAEV)}, []byte{byte(TAEV) + 1})
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
	s.entries[metadataPrefix+key] = encoded
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
				assertMemoryDatom(tx.store.entries, tx.store.encoder, &op.datoms[j], &undo)
			}
		case memoryTxRetract:
			for j := range op.datoms {
				if err := retractMemoryDatom(tx.store.entries, tx.store.encoder, reader, &op.datoms[j], &undo); err != nil {
					restoreMemoryEntries(tx.store.entries, undo)
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

func putMemoryEntry(entries map[string][]byte, undo *[]memoryEntryUndo, key string, value []byte) {
	old, had := entries[key]
	*undo = append(*undo, memoryEntryUndo{
		key: key,
		had: had,
		val: append([]byte(nil), old...),
	})
	if value == nil {
		entries[key] = nil
		return
	}
	entries[key] = append([]byte(nil), value...)
}

func deleteMemoryEntry(entries map[string][]byte, undo *[]memoryEntryUndo, key string) {
	old, had := entries[key]
	if !had {
		return
	}
	*undo = append(*undo, memoryEntryUndo{
		key: key,
		had: true,
		val: append([]byte(nil), old...),
	})
	delete(entries, key)
}

func restoreMemoryEntries(entries map[string][]byte, undo []memoryEntryUndo) {
	for i := len(undo) - 1; i >= 0; i-- {
		entry := undo[i]
		if entry.had {
			entries[entry.key] = entry.val
			continue
		}
		delete(entries, entry.key)
	}
}

func assertMemoryDatom(entries map[string][]byte, encoder *BinaryKeyEncoder, datom *datalog.Datom, undo *[]memoryEntryUndo) {
	valueBytes, blob := encoder.EncodeValueBytes(datom.V)
	if blob != nil {
		var key [21]byte
		key[0] = blobKeyPrefix
		copy(key[1:], blob.Hash[:])
		putMemoryEntry(entries, undo, string(key[:]), blob.CompressedBytes)
	}
	for _, index := range Indices {
		key := encoder.EncodeKeyWithValueBytes(index, datom, valueBytes)
		putMemoryEntry(entries, undo, string(key), nil)
	}
}

func retractMemoryDatom(
	entries map[string][]byte,
	encoder *BinaryKeyEncoder,
	blobs BlobReader,
	datom *datalog.Datom,
	undo *[]memoryEntryUndo,
) error {
	storageDatom := ToStorageDatom(*datom)
	valueBytes := encodeValueForSearch(storageDatom.V, encoder)
	searchPrefix := concatBytes(
		[]byte{byte(EAVT)},
		storageDatom.E[:],
		storageDatom.A[:],
		valueBytes,
	)
	var matches [][]byte
	for encoded := range entries {
		key := []byte(encoded)
		if bytes.HasPrefix(key, searchPrefix) {
			matches = append(matches, append([]byte(nil), key...))
		}
	}
	for _, key := range matches {
		stored, err := decodeDatomFromKey(EAVT, key, encoder, blobs)
		if err != nil {
			return err
		}
		for _, index := range Indices {
			deleteMemoryEntry(entries, undo, string(encoder.EncodeKey(index, &stored)))
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
	index        IndexType
	keys         [][]byte
	position     int
	encoder      *BinaryKeyEncoder
	blobs        BlobReader
	currentDatom datalog.Datom
	hasDatom     bool
	err          error
	closed       bool
}

func (i *memoryIterator) Next() bool {
	if i.closed || i.err != nil {
		return false
	}
	i.hasDatom = false
	i.position++
	return i.position < len(i.keys)
}

func (i *memoryIterator) Key() []byte {
	if i.closed || i.position < 0 || i.position >= len(i.keys) {
		return nil
	}
	return i.keys[i.position]
}

func (i *memoryIterator) Datom() (*datalog.Datom, error) {
	if i.err != nil {
		return nil, i.err
	}
	if i.closed || i.position < 0 || i.position >= len(i.keys) {
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

func (i *memoryIterator) Seek(key []byte) {
	if i.closed || i.err != nil {
		return
	}
	i.position = sort.Search(len(i.keys), func(index int) bool {
		return bytes.Compare(i.keys[index], key) >= 0
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
