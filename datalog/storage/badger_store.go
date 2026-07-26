//go:build !(js && wasm)

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
)

// metadataPrefix is used to store database metadata (e.g., ReplicaID)
// separate from datom indices

// BadgerStore implements Store using BadgerDB
type BadgerStore struct {
	db        *badger.DB
	encoder   *BinaryKeyEncoder
	closeOnce sync.Once
	closeErr  error
}

var _ Store = (*BadgerStore)(nil)

func (s *BadgerStore) Encoder() *BinaryKeyEncoder {
	return s.encoder
}

// NewBadgerStore creates a new BadgerDB-backed store.
func NewBadgerStore(path string, encoder *BinaryKeyEncoder) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Disable BadgerDB logs for now

	// Performance optimizations for read-heavy workload
	opts.MemTableSize = 128 << 20   // 128MB memtables (default 64MB)
	opts.BlockCacheSize = 256 << 20 // 256MB block cache for faster reads
	opts.IndexCacheSize = 100 << 20 // 100MB index cache
	opts.DetectConflicts = false    // Disable conflict detection for better performance
	opts.NumCompactors = 4          // Parallel compaction
	opts.ValueThreshold = 1 << 10   // 1KB - store small values in LSM tree

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger: %w", err)
	}

	// Physical storage keys always use the binary format.
	if encoder == nil {
		encoder = &BinaryKeyEncoder{}
	}

	return &BadgerStore{
		db:      db,
		encoder: encoder,
	}, nil
}

// Assert adds datoms to the store.
//
// The writes go through a WriteBatch rather than one transaction. A caller
// hands this arbitrarily many datoms, and at eight index keys each plus a blob
// per out-of-line value, Badger's per-transaction ceiling arrives long before
// a caller has reason to suspect a limit exists — a single entity's history is
// enough. The batch splits at that ceiling itself, so the arithmetic stays
// Badger's and cannot drift from it.
//
// The cost is that Assert is not atomic: a mid-way failure leaves the datoms
// already committed in place. Re-asserting is safe, because an index key is
// derived wholly from its datom and a repeated write reproduces it exactly.
// A caller that needs a boundary uses BeginTx, whose StoreTx.Assert still
// writes into the one transaction that caller owns.
func (s *BadgerStore) Assert(datoms []datalog.Datom) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for _, d := range datoms {
		if err := s.assertDatom(wb.Set, &d); err != nil {
			return err
		}
	}
	return wb.Flush()
}

// assertDatom writes one datom's eight index keys, and its blob when the value
// is stored out of line, through set: a transaction's Set where the caller owns
// the transaction boundary, a write batch's where it does not.
func (s *BadgerStore) assertDatom(set func(key, value []byte) error, d *datalog.Datom) error {
	// Pre-encode value bytes once (avoids recomputing compression 7 times)
	vBytes, blobData := s.encoder.EncodeValueBytes(d.V)

	// Tier 3: write compressed data to blob store
	if blobData != nil {
		if err := putBlob(set, blobData.Hash, blobData.CompressedBytes); err != nil {
			return fmt.Errorf("failed to write blob: %w", err)
		}
	}

	// Write to all indices using pre-encoded value bytes and one storage
	// conversion (E/A/Tx fixed arrays are index-independent)
	sd := ToStorageDatom(*d)
	for _, idx := range Indices {
		key := s.encoder.encodeKeyWithParts(idx, &sd, vBytes)
		if err := set(key, nil); err != nil {
			return fmt.Errorf("failed to write to %v index: %w", idx, err)
		}
	}

	return nil
}

// Retract removes datoms from the store
func (s *BadgerStore) Retract(datoms []datalog.Datom) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, d := range datoms {
			if err := s.retractDatom(txn, &d); err != nil {
				return err
			}
		}
		return nil
	})
}

// retractDatom removes a single datom from current-state indices and optionally
// records the retraction in history indices.
// NOTE: The Tx field in the passed datom is ignored for finding stored datoms.
// We find the actual stored datom(s) matching E+A+V and delete those.
// In history mode, the Tx from the passed datom is used for the retraction record.
func (s *BadgerStore) retractDatom(txn *badger.Txn, d *datalog.Datom) error {
	// Convert to storage format for prefix scanning
	sd := ToStorageDatom(*d)

	// Use EAVT index to find matching datoms (E+A+V, any Tx)
	// Build prefix: index byte + E + A + V (without Tx)
	// Must encode value the same way EncodeKey does (compression-aware)
	prefix := []byte{byte(EAVT)}
	vBytes := encodeValueForSearch(sd.V, s.encoder)
	searchPrefix := concatBytes(prefix, sd.E[:], sd.A[:], vBytes)

	// Iterate to find matching keys with their actual Tx values
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // Keys only for faster iteration
	it := txn.NewIterator(opts)
	defer it.Close()

	var keysToDelete [][]byte
	for it.Seek(searchPrefix); it.ValidForPrefix(searchPrefix); it.Next() {
		// Found a matching key - save it for deletion
		key := it.Item().KeyCopy(nil)
		keysToDelete = append(keysToDelete, key)
	}

	if len(keysToDelete) == 0 {
		// No matching datom found - not an error, just nothing to retract
		return nil
	}

	// For each matching EAVT key, decode it and delete from all indices
	for _, eavtKey := range keysToDelete {
		// Decode the EAVT key to get the full datom including Tx
		// DatomFromKey handles all the complexity of decoding components
		storedDatom, err := DatomFromKey(EAVT, eavtKey, s.encoder, badgerTxnBlobReader{txn: txn})
		if err != nil {
			return fmt.Errorf("failed to decode key for retraction: %w", err)
		}

		// Delete from all CRDT indices using the actual stored Tx; one
		// storage conversion and value encoding for all eight keys
		sdStored := ToStorageDatom(storedDatom)
		storedVBytes, _ := s.encoder.EncodeValueBytes(sdStored.V)
		for _, idx := range Indices {
			key := s.encoder.encodeKeyWithParts(idx, &sdStored, storedVBytes)
			if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
				return fmt.Errorf("failed to delete from %v index: %w", idx, err)
			}
		}
	}

	return nil
}

// MaxTxForEntity returns the highest Tx among a single entity's datoms. The bool is
// false when the entity has no datoms. TruncateTo uses it to find the floor below which
// a snapshot marker must be preserved.
func (s *BadgerStore) MaxTxForEntity(e datalog.Identity) (datalog.ElementID, bool, error) {
	prefix := concatBytes([]byte{byte(EAVT)}, e.Bytes())
	var maxID datalog.ElementID
	found := false
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			datom, err := DatomFromKey(EAVT, it.Item().KeyCopy(nil), s.encoder, badgerTxnBlobReader{txn: txn})
			if err != nil {
				return fmt.Errorf("decode EAVT key: %w", err)
			}
			if !found || maxID.Less(datom.Tx) {
				maxID = datom.Tx
				found = true
			}
		}
		return nil
	})
	if err != nil {
		return datalog.ElementID{}, false, err
	}
	return maxID, found, nil
}

// DatomsAfter returns every datom whose Tx is strictly greater than eid. It scans TAEV,
// whose Tx-descending order puts the affected datoms first, and stops at the first datom
// with Tx <= eid. Read-only; the caller removes them with DeleteDatoms, doing whatever
// cache/clock coordination it needs in between.
func (s *BadgerStore) DatomsAfter(eid datalog.ElementID) ([]datalog.Datom, error) {
	taevPrefix := []byte{byte(TAEV)}
	var datoms []datalog.Datom
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(taevPrefix); it.ValidForPrefix(taevPrefix); it.Next() {
			datom, err := DatomFromKey(TAEV, it.Item().KeyCopy(nil), s.encoder, badgerTxnBlobReader{txn: txn})
			if err != nil {
				return fmt.Errorf("decode TAEV key: %w", err)
			}
			if !eid.Less(datom.Tx) {
				// Tx <= eid; TAEV is Tx-descending, so nothing later exceeds eid either.
				break
			}
			datoms = append(datoms, datom)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return datoms, nil
}

// DeleteDatoms physically removes each given datom from all eight indices in a single
// transaction, returning the count removed. Unlike Retract (which appends a tombstone at a
// higher Tx), this is a true rewind: the keys are gone, so the datoms vanish from
// History() too.
func (s *BadgerStore) DeleteDatoms(datoms []datalog.Datom) (int, error) {
	deleted := 0
	err := s.db.Update(func(txn *badger.Txn) error {
		for i := range datoms {
			sd := ToStorageDatom(datoms[i])
			vBytes, _ := s.encoder.EncodeValueBytes(sd.V)
			for _, idx := range Indices {
				key := s.encoder.encodeKeyWithParts(idx, &sd, vBytes)
				if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
					return fmt.Errorf("delete from %v index: %w", idx, err)
				}
			}
			deleted++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// Scan returns a workspace-decoded iterator for a range of keys.
// Scan and ScanKeysOnly share the same KeyOnlyIterator contract: Datom()
// returns the iterator's current workspace until Next, Seek, or Close.
func (s *BadgerStore) Scan(index IndexType, start, end []byte) (Iterator, error) {
	return NewKeyOnlyIterator(s, index, start, end)
}

// Get retrieves a single datom by key
// Values are not stored - all datom information is decoded from the key
func (s *BadgerStore) Get(index IndexType, key []byte) (*datalog.Datom, error) {
	var result *datalog.Datom

	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return err
		}

		// Decode datom from key - values are not stored
		datom, err := DatomFromKey(index, key, s.encoder, badgerTxnBlobReader{txn: txn})
		if err != nil {
			return err
		}
		result = &datom
		return nil
	})

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return result, err
}

// MaxElementID returns the highest ElementID in the store by scanning TAEV index.
// With bitwise NOT encoding for Tx, forward scan gives highest Tx first (O(1)).
// Returns zero ElementID if store is empty.
func (s *BadgerStore) MaxElementID() (datalog.ElementID, error) {
	var maxID datalog.ElementID

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Keys only

		it := txn.NewIterator(opts)
		defer it.Close()

		// TAEV prefix - forward scan, first entry has highest Tx due to bitwise NOT encoding
		taevPrefix := []byte{byte(TAEV)}
		it.Seek(taevPrefix)

		if it.Valid() {
			key := it.Item().Key()
			if len(key) > 0 && key[0] == byte(TAEV) {
				// Found a TAEV key - decode ElementID from Tx position
				// TAEV layout: [prefix:1][Tx:16][A:32][E:20][V:var][Op:1][AfterRef?:16]
				// DecodeKey handles the bitwise NOT reversal
				_, _, _, tx, _, _, err := s.encoder.DecodeKey(TAEV, key)
				if err != nil {
					return fmt.Errorf("failed to decode TAEV key: %w", err)
				}
				maxID = Tx(tx).ToElementID()
				return nil
			}
		}

		// No TAEV entries found - store is empty
		return nil
	})

	return maxID, err
}

// MaxElementIDForAttribute returns the highest ElementID for any (E, A) with this attribute.
// Used for fast cache freshness checks on A-bound queries.
//
// O(1): single forward seek on the ATEV index. ATEV orders A → Tx↓ → E → V, so the
// first entry under prefix [A] has the global maximum Tx for the attribute (Tx is
// encoded with bitwise NOT for descending sort within (A) groups).
//
// Returns zero ElementID if no ATEV data exists for this attribute.
func (s *BadgerStore) MaxElementIDForAttribute(a []byte) (datalog.ElementID, error) {
	var maxID datalog.ElementID

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Keys only

		it := txn.NewIterator(opts)
		defer it.Close()

		// Build ATEV prefix for this attribute: [prefix:1][A:32]
		atevPrefix := make([]byte, 1+32)
		atevPrefix[0] = byte(ATEV)
		copy(atevPrefix[1:33], a)

		it.Seek(atevPrefix)
		if !it.Valid() {
			return nil
		}

		// Seek may land past the attribute's ATEV range (different index or
		// different attribute) when no ATEV entries exist for this attribute.
		key := it.Item().Key()
		if key[0] != byte(ATEV) || !bytesEqual(key[1:33], atevPrefix[1:33]) {
			return nil
		}

		// First entry under [ATEV][A] holds the global max Tx for the attribute.
		_, _, _, tx, _, _, decodeErr := s.encoder.DecodeKey(ATEV, key)
		if decodeErr != nil {
			return decodeErr
		}
		maxID = Tx(tx).ToElementID()
		return nil
	})

	return maxID, err
}

// bytesEqual compares two byte slices for equality
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// BeginTx starts a new transaction
func (s *BadgerStore) BeginTx() (StoreTx, error) {
	txn := s.db.NewTransaction(true)
	return &BadgerTx{
		store: s,
		txn:   txn,
	}, nil
}

// Close closes the store
func (s *BadgerStore) Close() error {
	s.closeOnce.Do(func() {
		syncErr := s.db.Sync()
		closeErr := s.db.Close()
		if syncErr != nil {
			s.closeErr = syncErr
		} else {
			s.closeErr = closeErr
		}
	})
	return s.closeErr
}

// GetMetadataUint64 retrieves a uint64 metadata value by key.
// Returns (value, true) if found, (0, false) if not found.
func (s *BadgerStore) GetMetadataUint64(key string) (uint64, bool, error) {
	metaKey := []byte(metadataPrefix + key)
	var result uint64
	var found bool

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey)
		if err == badger.ErrKeyNotFound {
			return nil // Not found is not an error
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("metadata %s has invalid length %d, expected 8", key, len(val))
			}
			result = binary.BigEndian.Uint64(val)
			found = true
			return nil
		})
	})

	return result, found, err
}

// SetMetadataUint64 stores a uint64 metadata value by key.
func (s *BadgerStore) SetMetadataUint64(key string, value uint64) error {
	metaKey := []byte(metadataPrefix + key)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, value)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey, val)
	})
}

// ScanKeysOnly returns an iterator that decodes datoms from keys without fetching values
// This is much faster than regular scanning as it avoids the redundant value fetch
func (s *BadgerStore) ScanKeysOnly(index IndexType, start, end []byte) (Iterator, error) {
	return NewKeyOnlyIterator(s, index, start, end)
}

// CountKeys counts keys in a range without fetching values (fast counting)
func (s *BadgerStore) CountKeys(index IndexType, start, end []byte) (int64, error) {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // KEY ONLY - no values!
	opts.PrefetchSize = 10000   // Prefetch many keys

	it := txn.NewIterator(opts)
	defer it.Close()

	var count int64
	it.Seek(start)
	for it.Valid() {
		key := it.Item().Key()
		if end != nil && bytes.Compare(key, end) >= 0 {
			break
		}
		count++
		it.Next()
	}

	return count, nil
}

// BadgerIterator implements Iterator for BadgerDB
type BadgerIterator struct {
	txn     *badger.Txn
	it      *badger.Iterator
	start   []byte
	end     []byte
	index   IndexType
	valid   bool
	encoder *BinaryKeyEncoder // For decoding Op from key
	blobs   BlobReader        // Uses the scan transaction for Tier 3 blobs
	// release, when set, returns the iterator to its owning read session
	// instead of discarding the transaction: session-owned iterators share
	// the session's transaction, which outlives any one scan.
	release func()
}

// Next advances the iterator
func (i *BadgerIterator) Next() bool {
	if !i.valid {
		// First call - seek to start
		i.it.Seek(i.start)
		i.valid = true
	} else {
		// Subsequent calls - advance
		i.it.Next()
	}

	// Check if we're still in range
	if !i.it.Valid() {
		return false
	}

	if i.end != nil {
		key := i.it.Item().Key()
		if bytes.Compare(key, i.end) >= 0 {
			// Leave the Badger cursor on the successor key (Valid() may still
			// be true) but report not-current: Key()/Datom() must not expose it.
			return false
		}
	}

	return true
}

// Key returns the current index key without decoding the datom or resolving blobs.
// Valid after Next returns true until the next Next, Seek, or Close.
func (i *BadgerIterator) Key() []byte {
	if i.it == nil || !i.it.Valid() {
		return nil
	}
	key := i.it.Item().Key()
	if i.end != nil && bytes.Compare(key, i.end) >= 0 {
		return nil
	}
	return key
}

// Datom returns the current datom decoded from the key
// Values are not stored - all datom information is in the key
func (i *BadgerIterator) Datom() (*datalog.Datom, error) {
	if i.it == nil || !i.it.Valid() {
		return nil, fmt.Errorf("no current datom")
	}
	key := i.it.Item().Key()
	if i.end != nil && bytes.Compare(key, i.end) >= 0 {
		return nil, fmt.Errorf("no current datom")
	}

	datom, err := decodeDatomFromKey(
		i.index,
		key,
		i.encoder,
		i.blobs,
	)
	if err != nil {
		return nil, err
	}

	return &datom, nil
}

// Close closes the iterator and releases the underlying BadgerDB transaction.
// Session-owned iterators return themselves to the session instead — the
// shared transaction is discarded by the session, not by any one scan.
// Safe to call multiple times.
func (i *BadgerIterator) Close() error {
	if i.txn == nil {
		return nil
	}
	runtime.SetFinalizer(i, nil)
	i.it.Close()
	if i.release != nil {
		i.release()
	} else {
		i.txn.Discard()
	}
	i.txn = nil
	return nil
}

// Seek positions the iterator at or after the given key
func (i *BadgerIterator) Seek(key []byte) {
	i.it.Seek(key)
	// Update start to the seek position so Next() doesn't re-seek to original start
	i.start = key
	// Leave valid=false so Next() positions us correctly
	i.valid = false
}

// ElementID extracts the transaction ElementID from the current key.
// This is more efficient than Datom() when only the ElementID is needed.
func (i *BadgerIterator) ElementID() datalog.ElementID {
	if i.it == nil || !i.it.Valid() {
		return datalog.ElementID{}
	}
	key := i.it.Item().Key()
	if i.end != nil && bytes.Compare(key, i.end) >= 0 {
		return datalog.ElementID{}
	}
	return extractElementIDFromKey(i.index, key)
}

// Error returns nil — BadgerIterator.Next() does not perform any
// fallible operations; errors surface exclusively through Datom() on
// the current item. Returning nil satisfies the Iterator interface
// and tells wrapping iterators there is no deferred error to collect.
func (i *BadgerIterator) Error() error { return nil }

// BadgerTx implements Tx for BadgerDB
type BadgerTx struct {
	store *BadgerStore
	txn   *badger.Txn
}

// Assert adds datoms within a transaction. Unlike BadgerStore.Assert this does
// not split: the caller owns the transaction boundary, so an oversized commit
// surfaces Badger's ErrTxnTooBig for the caller to handle.
func (t *BadgerTx) Assert(datoms []datalog.Datom) error {
	for _, d := range datoms {
		if err := t.store.assertDatom(t.txn.Set, &d); err != nil {
			return err
		}
	}
	return nil
}

// Retract removes datoms within a transaction
func (t *BadgerTx) Retract(datoms []datalog.Datom) error {
	for _, d := range datoms {
		if err := t.store.retractDatom(t.txn, &d); err != nil {
			return err
		}
	}
	return nil
}

// Commit commits the transaction
func (t *BadgerTx) Commit() error {
	return t.txn.Commit()
}

// Rollback rolls back the transaction
func (t *BadgerTx) Rollback() error {
	t.txn.Discard()
	return nil
}
