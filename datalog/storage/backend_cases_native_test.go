//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func expectedBackendNames() []string {
	return []string{"memory", "memory-trees", "badger"}
}

// appendNativeReopenCases builds the Badger store explicitly rather than setting
// opts.Path, so the case named "badger" is Badger whatever openDefaultStore
// returns. Owning the store means closing it here: a Database over an injected
// store does not close it, and the next open would meet Badger's directory lock.
func appendNativeReopenCases(cases []reopenBackendCase) []reopenBackendCase {
	badgerCase := reopenBackendCase{name: "badger"}
	var (
		path       string
		prior      *Database
		priorStore Store
	)
	badgerCase.open = func(t testing.TB, opts DatabaseOptions) *Database {
		t.Helper()
		if path == "" {
			path = t.TempDir()
			t.Cleanup(func() {
				if prior != nil {
					_ = prior.Close()
				}
				if priorStore != nil {
					_ = priorStore.Close()
				}
			})
		}
		if prior != nil {
			require.NoError(t, prior.Close())
		}
		if priorStore != nil {
			require.NoError(t, priorStore.Close())
			priorStore = nil
		}

		encoder := &BinaryKeyEncoder{}
		if opts.CompressionThreshold != 0 {
			encoder.CompressionThreshold = opts.CompressionThreshold
		}
		store, err := NewBadgerStore(path, encoder)
		require.NoError(t, err)
		priorStore = store

		opts.Store = store
		db, err := NewDatabaseWithOptions(opts)
		require.NoError(t, err)
		prior = db
		return db
	}
	return append(cases, badgerCase)
}

func nativeBlobKeys(t *testing.T, store Store) ([][]byte, bool) {
	t.Helper()
	badgerStore, ok := store.(*BadgerStore)
	if !ok {
		return nil, false
	}
	var keys [][]byte
	require.NoError(t, badgerStore.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte{blobKeyPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}))
	return keys, true
}

func deleteNativeBlobKeys(t *testing.T, store Store, keys [][]byte) bool {
	t.Helper()
	badgerStore, ok := store.(*BadgerStore)
	if !ok {
		return false
	}
	require.NoError(t, badgerStore.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}))
	return true
}
