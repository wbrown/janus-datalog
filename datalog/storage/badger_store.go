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
func (s *BadgerStore) Assert(datoms []datalog.Datom) error {
	return s.AssertEach(func(add func(*datalog.Datom) error) error {
		for i := range datoms {
			if err := add(&datoms[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// AssertEach writes through a WriteBatch rather than one transaction. A producer
// yields arbitrarily many datoms, and at eight index keys each plus a blob per
// out-of-line value, Badger's per-transaction ceiling arrives long before a
// caller has reason to suspect a limit exists — a single entity's history is
// enough. The batch splits at that ceiling itself, so the arithmetic stays
// Badger's and cannot drift from it.
//
// The cost is that this is not atomic: a mid-way failure leaves the datoms
// already committed in place. Re-asserting is safe, because an index key is
// derived wholly from its datom and a repeated write reproduces it exactly.
// A caller that needs a boundary uses BeginTx, whose StoreTx.Assert still
// writes into the one transaction that caller owns.
func (s *BadgerStore) AssertEach(produce func(add func(*datalog.Datom) error) error) error {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	if err := produce(func(d *datalog.Datom) error {
		return s.assertDatom(wb.Set, d)
	}); err != nil {
		return err
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

// FinishBatch has nothing to complete: AssertEach flushes its write batch before
// returning.
func (s *BadgerStore) FinishBatch() error { return nil }

// Retract removes datoms from the store
func (s *BadgerStore) Retract(datoms []datalog.Datom) error {
	return s.db.Update(func(txn *badger.Txn) error {
		candidates := make(map[[20]byte]struct{})
		for _, d := range datoms {
			if err := s.retractDatom(txn, &d, candidates); err != nil {
				return err
			}
		}
		return s.reclaimBlobsInTxn(txn, candidates)
	})
}

// reclaimBlobsInTxn deletes each candidate blob no datom still refers to, inside
// a transaction someone else owns, so the removal is atomic with the retract
// that orphaned it. The probe reads that transaction's pending writes, so it
// counts what survives the retract rather than what preceded it.
//
// Unlike the reclamation DeleteDatoms performs, this one is best-effort against
// concurrent writers, and the window is the whole transaction rather than the
// span from the count to the commit. Badger fixes a transaction's read timestamp
// when it opens, so a writer committing any time after that is invisible to the
// probe however late the probe runs. That writer commits a tier-3 value's blob
// alongside its index keys, and NewBadgerStore disables conflict detection, so
// nothing aborts the transaction whose read set it invalidated: its datom is left
// pointing at a blob this transaction removed. Nothing gates the commit path the
// way TruncateTo gates the rewind. The exposure is accepted:
// physical retraction is a maintenance primitive rather than an ordinary write,
// and the alternative is a lock over every tier-3 write. See
// BUG_BLOBS_ARE_NEVER_RECLAIMED.
func (s *BadgerStore) reclaimBlobsInTxn(txn *badger.Txn, candidates map[[20]byte]struct{}) error {
	if len(candidates) == 0 {
		return nil
	}

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = []byte{byte(VAET)}
	it := txn.NewIterator(opts)
	exists := func(prefix []byte) bool {
		it.Seek(prefix)
		return it.ValidForPrefix(prefix)
	}
	var garbage [][20]byte
	for hash := range candidates {
		if !blobIsReferenced(s.encoder, hash, exists) {
			garbage = append(garbage, hash)
		}
	}
	// Closed before the deletes below, and before any commit: Badger panics on a
	// transaction committed with an iterator still open.
	it.Close()

	for _, hash := range garbage {
		key := blobKey(hash)
		if err := txn.Delete(key[:]); err != nil {
			return fmt.Errorf("delete blob %x: %w", hash, err)
		}
	}
	return nil
}

// retractDatom removes a single datom from current-state indices and optionally
// records the retraction in history indices.
// NOTE: The Tx field in the passed datom is ignored for finding stored datoms.
// We find the actual stored datom(s) matching E+A+V and delete those.
// In history mode, the Tx from the passed datom is used for the retraction record.
//
// Every stored value that lived out of line records its blob hash in candidates,
// so the caller can ask, once its transaction is otherwise complete, whether
// anything still refers to that content.
func (s *BadgerStore) retractDatom(txn *badger.Txn, d *datalog.Datom, candidates map[[20]byte]struct{}) error {
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
		storedVBytes, blobData := s.encoder.EncodeValueBytes(sdStored.V)
		if blobData != nil {
			candidates[blobData.Hash] = struct{}{}
		}
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

// DeleteDatoms physically removes each given datom from all eight indices,
// returning the count handed over. Unlike Retract (which appends a tombstone at a
// higher Tx), this is a true rewind: the keys are gone, so the datoms vanish from
// History() too.
//
// The deletes go through a WriteBatch for the same reason AssertEach's writes do,
// and it is the delete side that needs it more: a rewind's extent is the whole
// post-snapshot tail, and at eight index keys per datom the pending-write set is
// eight times a count the caller has no reason to think is bounded. Badger's
// per-transaction ceiling arrives around 26,000 datoms. The batch splits at that
// ceiling itself, so the arithmetic stays Badger's and cannot drift from it.
//
// The cost is that this is not atomic: a mid-way failure leaves the datoms
// already deleted gone, and History() shows a torn tail until the rewind is
// re-run. Re-running completes it — TruncateTo restores the clock only after
// every delete succeeds, so a second pass collects the datoms that survived, and
// sweepUnreferencedBlobs below reaches the blobs of those that did not, which no
// pass could name from the datoms it can still see. A reader whose MVCC snapshot
// lands between chunks can observe a partially deleted tail for the duration.
//
// When the call returns, no blob without a referring datom remains — including
// blobs this call never touched. The tier is this store's own answer to values
// too wide for a key, so nothing above the Store seam hears about it.
func (s *BadgerStore) DeleteDatoms(datoms []datalog.Datom) (int, error) {
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for i := range datoms {
		sd := ToStorageDatom(datoms[i])
		vBytes, _ := s.encoder.EncodeValueBytes(sd.V)
		for _, idx := range Indices {
			key := s.encoder.encodeKeyWithParts(idx, &sd, vBytes)
			if err := wb.Delete(key); err != nil {
				return 0, fmt.Errorf("delete from %v index: %w", idx, err)
			}
		}
	}
	if err := wb.Flush(); err != nil {
		return 0, fmt.Errorf("flush index deletes: %w", err)
	}
	if err := s.sweepUnreferencedBlobs(); err != nil {
		return 0, err
	}
	return len(datoms), nil
}

// sweepUnreferencedBlobs deletes every blob no datom refers to, leaving none
// behind when it returns.
//
// It asks the blob keyspace what is dead rather than asking this call's datoms
// what they orphaned, so the answer never depends on knowing which datoms went.
// A blob whose datom vanished when an earlier rewind stopped part-way is as
// reclaimable as one this call removed — which is what makes re-running a torn
// rewind recover, since a retry sees only surviving datoms and could never name
// the missing ones.
//
// Reference is asked of the hash under every tag a blob can be named by (see
// blobIsReferenced), because one blob is shared by every datom holding that
// content — another entity, or the same entity at an earlier Tx that History
// still answers from. Each probe stops at the first surviving reference, which
// already settles the count above zero.
//
// Cost is two seeks per blob. A blob exists only where a compressed value
// exceeded the key ceiling, so blobs are few and large, and the walk is small
// against a call already deleting eight index keys for every datom it was given.
//
// The caller owns exclusion: a writer commits a tier-3 value's blob alongside its
// index keys, so a probe that ran before such a commit would strand it.
// TruncateTo drains writers for the whole rewind.
func (s *BadgerStore) sweepUnreferencedBlobs() error {
	var hashes [][20]byte
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{blobKeyPrefix}
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{blobKeyPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != blobKeyLen {
				continue
			}
			var hash [20]byte
			copy(hash[:], key[1:])
			hashes = append(hashes, hash)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan blob keys: %w", err)
	}
	if len(hashes) == 0 {
		return nil
	}

	var garbage [][20]byte
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte{byte(VAET)}
		it := txn.NewIterator(opts)
		defer it.Close()
		exists := func(prefix []byte) bool {
			it.Seek(prefix)
			return it.ValidForPrefix(prefix)
		}
		for _, hash := range hashes {
			if !blobIsReferenced(s.encoder, hash, exists) {
				garbage = append(garbage, hash)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("count blob references: %w", err)
	}
	if len(garbage) == 0 {
		return nil
	}

	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for _, hash := range garbage {
		key := blobKey(hash)
		if err := wb.Delete(key[:]); err != nil {
			return fmt.Errorf("delete blob %x: %w", hash, err)
		}
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("flush blob deletes: %w", err)
	}
	return nil
}

// Scan returns a workspace-decoded iterator for a range of keys.
// Scan and ScanKeysOnly share the same KeyOnlyIterator contract: Datom()
// returns the iterator's current workspace until Next, Seek, or Close.
func (s *BadgerStore) Scan(bound ScanBound) (Iterator, error) {
	run, err := s.encoder.EncodeScanBound(bound)
	if err != nil {
		return nil, err
	}
	return NewKeyOnlyIterator(s, bound.Index, run)
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
func (s *BadgerStore) ScanKeysOnly(bound ScanBound) (Iterator, error) {
	run, err := s.encoder.EncodeScanBound(bound)
	if err != nil {
		return nil, err
	}
	return NewKeyOnlyIterator(s, bound.Index, run)
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
	// scanned counts keys taken from the index inside the byte range. The
	// key one past the range is examined but not counted — it is not part of
	// the run. KeyOnlyIterator's membership test runs above this, so keys it
	// rejects are counted here, which is the point.
	scanned int
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

	i.scanned++
	return true
}

// Scanned reports keys taken from the index inside this iterator's range.
func (i *BadgerIterator) Scanned() int { return i.scanned }

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

	// blobCandidates accumulates the out-of-line values this transaction's
	// retracts removed, for Commit to reclaim. It is asked at commit rather
	// than per Retract call so the count sees the transaction's final state:
	// one transaction may retract a value and assert it again.
	blobCandidates map[[20]byte]struct{}
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
	if t.blobCandidates == nil {
		t.blobCandidates = make(map[[20]byte]struct{})
	}
	for _, d := range datoms {
		if err := t.store.retractDatom(t.txn, &d, t.blobCandidates); err != nil {
			return err
		}
	}
	return nil
}

// Commit reclaims any blob this transaction's retracts left unreferenced, then
// commits. Reclaiming here rather than inside Retract is what lets the count see
// asserts the same transaction made afterwards.
func (t *BadgerTx) Commit() error {
	if err := t.store.reclaimBlobsInTxn(t.txn, t.blobCandidates); err != nil {
		return err
	}
	return t.txn.Commit()
}

// Rollback rolls back the transaction
func (t *BadgerTx) Rollback() error {
	t.txn.Discard()
	return nil
}
