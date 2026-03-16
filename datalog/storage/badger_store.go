package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
)

// metadataPrefix is used to store database metadata (e.g., ReplicaID)
// separate from datom indices
const metadataPrefix = "_meta:"

// BadgerStore implements Store using BadgerDB
type BadgerStore struct {
	db      *badger.DB
	encoder KeyEncoder
}

// NewBadgerStore creates a new BadgerDB-backed store with the specified encoder
func NewBadgerStore(path string, encoder KeyEncoder) (*BadgerStore, error) {
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

	// Default to Binary encoding for performance
	if encoder == nil {
		encoder = NewKeyEncoder(BinaryStrategy)
	}

	return &BadgerStore{
		db:      db,
		encoder: encoder,
	}, nil
}

// Assert adds datoms to the store
func (s *BadgerStore) Assert(datoms []datalog.Datom) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, d := range datoms {
			if err := s.assertDatom(txn, &d); err != nil {
				return err
			}
		}
		return nil
	})
}

// assertDatom adds a single datom to all indices
func (s *BadgerStore) assertDatom(txn *badger.Txn, d *datalog.Datom) error {
	// Write to all CRDT indices
	// Value is nil - all datom information is encoded in the key
	for _, idx := range Indices {
		key := s.encoder.EncodeKey(idx, d)
		if err := txn.Set(key, nil); err != nil {
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
	prefix := []byte{byte(EAVT)}
	vType := byte(datalog.Type(sd.V))
	vData := datalog.ValueBytes(sd.V)
	vBytes := append([]byte{vType}, vData...)
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
		storedDatom, err := DatomFromKey(EAVT, eavtKey, s.encoder)
		if err != nil {
			return fmt.Errorf("failed to decode key for retraction: %w", err)
		}

		// Delete from all CRDT indices using the actual stored Tx
		for _, idx := range Indices {
			key := s.encoder.EncodeKey(idx, &storedDatom)
			if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
				return fmt.Errorf("failed to delete from %v index: %w", idx, err)
			}
		}
	}

	return nil
}

// Scan returns an iterator for a range of keys
func (s *BadgerStore) Scan(index IndexType, start, end []byte) (Iterator, error) {
	txn := s.db.NewTransaction(false)

	opts := badger.DefaultIteratorOptions
	// All datom data is encoded in keys — values are not stored.
	// PrefetchValues must be false to avoid spawning prefetch goroutines
	// per iterator, which causes scheduler thrashing with thousands of
	// short-lived iterators (e.g., per-(E,A) cache resolution).
	// PrefetchSize is ignored when PrefetchValues is false.
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)

	iter := &BadgerIterator{
		txn:     txn,
		it:      it,
		start:   start,
		end:     end,
		index:   index,
		encoder: s.encoder, // For decoding Op from key
	}
	runtime.SetFinalizer(iter, (*BadgerIterator).Close)
	return iter, nil
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
		datom, err := DatomFromKey(index, key, s.encoder)
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
// Uses AEVT index with forward scan - first entry has highest Tx due to bitwise NOT encoding.
// Returns zero ElementID if no data exists for this attribute.
func (s *BadgerStore) MaxElementIDForAttribute(a []byte) (datalog.ElementID, error) {
	var maxID datalog.ElementID

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // Keys only

		it := txn.NewIterator(opts)
		defer it.Close()

		// Build AEVT prefix for this attribute: [prefix:1][A:32]
		aevtPrefix := make([]byte, 1+32)
		aevtPrefix[0] = byte(AEVT)
		copy(aevtPrefix[1:33], a)

		it.Seek(aevtPrefix)

		if it.Valid() {
			key := it.Item().Key()
			// Verify this key is for our attribute (prefix match)
			if len(key) >= 33 && key[0] == byte(AEVT) {
				if !bytesEqual(key[1:33], aevtPrefix[1:33]) {
					// Different attribute - no data for this attribute
					return nil
				}
				// Found an AEVT key for this attribute
				// AEVT layout: [prefix:1][A:32][E:20][V:var][Tx:16][Op:1][AfterRef?:16]
				// However, Tx is NOT the first component after A, so we can't rely on first entry
				// having highest Tx. We need to scan through entries for this attribute.
				//
				// Actually, for AEVT, the key order is: A → E → V → Tx (descending)
				// So within the same (A, E, V), first entry has highest Tx.
				// But we want highest across ALL (E, V) for this A.
				//
				// For a true O(1) solution, we'd need an index like EATV where Tx comes earlier.
				// For now, we scan entries until we find a different attribute.
				// In practice, we only need to find ONE entry to know the attribute has data,
				// and track the max as we go.
				for it.Valid() {
					key := it.Item().Key()
					// Check if still in our attribute's prefix
					if len(key) < 33 || key[0] != byte(AEVT) {
						break
					}
					if !bytesEqual(key[1:33], aevtPrefix[1:33]) {
						break // Different attribute
					}

					// Decode the Tx from this key
					_, _, _, tx, _, _, err := s.encoder.DecodeKey(AEVT, key)
					if err != nil {
						it.Next()
						continue
					}
					elemID := Tx(tx).ToElementID()
					if elemID.Compare(maxID) > 0 {
						maxID = elemID
					}

					it.Next()
				}
				return nil
			}
		}

		// No AEVT entries found for this attribute
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
	return s.db.Close()
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

// ScanKeysOnlyWithMask - DEPRECATED: Key mask filtering was benchmarked slower
// Just use regular key-only scanning with filtering in the matcher
func (s *BadgerStore) ScanKeysOnlyWithMask(index IndexType, start, end []byte, mask *KeyMaskConstraint) (Iterator, error) {
	// Key mask iterator was removed - benchmarked slower than regular filtering
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
	encoder KeyEncoder // For decoding Op from key
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
			return false
		}
	}

	return true
}

// Datom returns the current datom decoded from the key
// Values are not stored - all datom information is in the key
func (i *BadgerIterator) Datom() (*datalog.Datom, error) {
	item := i.it.Item()

	// Must copy key since BadgerDB reuses the buffer.
	// Note: We cannot reuse a key buffer here because DecodeKey returns a slice
	// into the key bytes for the value component. Reusing the buffer would cause
	// the value slice to be overwritten on subsequent calls.
	key := item.KeyCopy(nil)

	// Decode datom from key - all information is in the key
	datom, err := DatomFromKey(i.index, key, i.encoder)
	if err != nil {
		return nil, err
	}

	return &datom, nil
}

// Close closes the iterator and releases the underlying BadgerDB transaction.
// Safe to call multiple times.
func (i *BadgerIterator) Close() error {
	if i.txn == nil {
		return nil
	}
	runtime.SetFinalizer(i, nil)
	i.it.Close()
	i.txn.Discard()
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
	if !i.it.Valid() {
		return datalog.ElementID{}
	}
	key := i.it.Item().Key()
	return extractElementIDFromKey(i.index, key)
}

// extractElementIDFromKey extracts the ElementID from a key based on index type.
// The Tx is encoded with bitwise NOT, so we reverse it here.
func extractElementIDFromKey(index IndexType, key []byte) datalog.ElementID {
	const (
		prefixSize = 1
		entitySize = 20
		attrSize   = 32
		txSize     = 16
	)

	if len(key) < prefixSize+txSize {
		return datalog.ElementID{}
	}

	var txBytes []byte

	switch index {
	case TAEV:
		// TAEV: [prefix:1][Tx:16][A:32][E:20][V:var]
		// Tx is right after prefix
		txBytes = key[prefixSize : prefixSize+txSize]

	case EATV:
		// EATV: [prefix:1][E:20][A:32][Tx:16][V:var][Op:1]
		// Tx is after E+A
		offset := prefixSize + entitySize + attrSize
		if len(key) < offset+txSize {
			return datalog.ElementID{}
		}
		txBytes = key[offset : offset+txSize]

	case AETV:
		// AETV: [prefix:1][A:32][E:20][Tx:16][V:var][Op:1]
		// Tx is after A+E
		offset := prefixSize + attrSize + entitySize
		if len(key) < offset+txSize {
			return datalog.ElementID{}
		}
		txBytes = key[offset : offset+txSize]

	case EAVT, AEVT, AVET, VAET:
		// These have Tx at the end: [...][Tx:16]
		if len(key) < txSize {
			return datalog.ElementID{}
		}
		txBytes = key[len(key)-txSize:]

	default:
		return datalog.ElementID{}
	}

	// Reverse bitwise NOT to get original ElementID
	return DecodeElementID(txBytes)
}

// BadgerTx implements Tx for BadgerDB
type BadgerTx struct {
	store *BadgerStore
	txn   *badger.Txn
}

// Assert adds datoms within a transaction
func (t *BadgerTx) Assert(datoms []datalog.Datom) error {
	for _, d := range datoms {
		if err := t.store.assertDatom(t.txn, &d); err != nil {
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
