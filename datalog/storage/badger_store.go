package storage

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
)

// BadgerStore implements Store using BadgerDB
type BadgerStore struct {
	db          *badger.DB
	encoder     KeyEncoder
	retractMode RetractMode // Controls how retractions are handled
}

// NewBadgerStore creates a new BadgerDB-backed store with the specified encoder
// Uses RetractDelete mode (current behavior) by default
func NewBadgerStore(path string, encoder KeyEncoder) (*BadgerStore, error) {
	return NewBadgerStoreWithRetractMode(path, encoder, RetractDelete)
}

// NewBadgerStoreWithRetractMode creates a new BadgerDB-backed store with specified retract mode
func NewBadgerStoreWithRetractMode(path string, encoder KeyEncoder, retractMode RetractMode) (*BadgerStore, error) {
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
		db:          db,
		encoder:     encoder,
		retractMode: retractMode,
	}, nil
}

// RetractMode returns the store's retract mode
func (s *BadgerStore) RetractMode() RetractMode {
	return s.retractMode
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
	// Serialize the datom
	sd := ToStorageDatom(*d)
	value := sd.Bytes()

	// Write to all current-state indices
	for _, idx := range CurrentStateIndices {
		key := s.encoder.EncodeKey(idx, d)
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to write to %v index: %w", idx, err)
		}
	}

	// If history mode, also write to history indices with Op=true
	if s.retractMode == RetractHistory {
		if err := s.writeToHistoryIndices(txn, d, OpAssert); err != nil {
			return err
		}
	}

	return nil
}

// writeToHistoryIndices writes a datom to all history indices with the given Op
func (s *BadgerStore) writeToHistoryIndices(txn *badger.Txn, d *datalog.Datom, op Op) error {
	// Value is just the Op byte - all other data is in the key
	value := []byte{0x00}
	if op {
		value[0] = 0x01
	}

	for _, idx := range HistoryIndices {
		key := s.encoder.EncodeHistoryKey(idx, d, op)
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to write to %v history index: %w", idx, err)
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

	// For each matching EAVT key, decode it and delete from current-state indices
	for _, eavtKey := range keysToDelete {
		// Decode the EAVT key to get the full datom including Tx
		// DatomFromKey handles all the complexity of decoding components
		storedDatom, err := DatomFromKey(EAVT, eavtKey, s.encoder)
		if err != nil {
			return fmt.Errorf("failed to decode key for retraction: %w", err)
		}

		// Delete from all current-state indices using the actual stored Tx
		for _, idx := range CurrentStateIndices {
			key := s.encoder.EncodeKey(idx, storedDatom)
			if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
				return fmt.Errorf("failed to delete from %v index: %w", idx, err)
			}
		}

		// If history mode, write retraction to history indices
		// Use the Tx from the passed datom (the retraction transaction)
		if s.retractMode == RetractHistory {
			retractDatom := &datalog.Datom{
				E:  storedDatom.E,
				A:  storedDatom.A,
				V:  storedDatom.V,
				Tx: d.Tx, // Use the retraction Tx, not the original assertion Tx
			}
			if err := s.writeToHistoryIndices(txn, retractDatom, OpRetract); err != nil {
				return err
			}
		}
	}

	return nil
}

// Scan returns an iterator for a range of keys
func (s *BadgerStore) Scan(index IndexType, start, end []byte) (Iterator, error) {
	txn := s.db.NewTransaction(false)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 1000   // Increased from 10 for better bulk scan performance
	opts.PrefetchValues = true // We need values for datom construction

	it := txn.NewIterator(opts)

	return &BadgerIterator{
		txn:   txn,
		it:    it,
		start: start,
		end:   end,
		index: index,
	}, nil
}

// Get retrieves a single datom by key
func (s *BadgerStore) Get(index IndexType, key []byte) (*datalog.Datom, error) {
	var result *datalog.Datom

	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			sd, err := StorageDatomFromBytes(val)
			if err != nil {
				return err
			}
			// Convert to user-facing datom
			// TODO: Need proper resolver for attribute names
			result = &datalog.Datom{
				E:  *datalog.InternIdentity(datalog.NewIdentity(sd.E.String())),
				A:  *datalog.InternKeyword(sd.A.String()),
				V:  sd.V,
				Tx: sd.Tx.Uint64(),
			}
			return nil
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return result, err
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
	txn   *badger.Txn
	it    *badger.Iterator
	start []byte
	end   []byte
	index IndexType
	valid bool
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

// Datom returns the current datom
func (i *BadgerIterator) Datom() (*datalog.Datom, error) {
	item := i.it.Item()

	var result *datalog.Datom
	err := item.Value(func(val []byte) error {
		sd, err := StorageDatomFromBytes(val)
		if err != nil {
			return err
		}
		// Convert to user-facing datom
		// TODO: Need proper resolver for attribute names
		// Note: StorageDatomFromBytes already decodes the value properly,
		// so sd.V is already the decoded value
		result = &datalog.Datom{
			E:  *datalog.InternIdentityFromHash(sd.E),
			A:  *datalog.InternKeyword(sd.A.String()),
			V:  sd.V,
			Tx: sd.Tx.Uint64(),
		}
		return nil
	})

	return result, err
}

// Close closes the iterator
func (i *BadgerIterator) Close() error {
	i.it.Close()
	i.txn.Discard()
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
