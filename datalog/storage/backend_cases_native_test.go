//go:build !(js && wasm)

package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func appendNativeBackendCases(cases []storeContractCase) []storeContractCase {
	return append(cases, storeContractCase{
		name: "badger",
		open: func(tb testing.TB, encoder *BinaryKeyEncoder) Store {
			store, err := NewBadgerStore(tb.TempDir(), encoder)
			require.NoError(tb, err)
			return store
		},
	})
}

func appendNativeReopenCases(cases []reopenBackendCase) []reopenBackendCase {
	badgerCase := reopenBackendCase{name: "badger"}
	var (
		path  string
		prior *Database
	)
	badgerCase.open = func(t testing.TB, opts DatabaseOptions) *Database {
		t.Helper()
		if path == "" {
			path = t.TempDir()
			t.Cleanup(func() {
				if prior != nil {
					_ = prior.Close()
				}
			})
		}
		if prior != nil {
			require.NoError(t, prior.Close())
		}
		opts.Path = path
		db, err := NewDatabaseWithOptions(opts)
		require.NoError(t, err)
		prior = db
		return db
	}
	return append(cases, badgerCase)
}

func deleteNativeStoreBlobs(t *testing.T, store Store) (int, bool) {
	t.Helper()
	badgerStore, ok := store.(*BadgerStore)
	if !ok {
		return 0, false
	}
	deleted := 0
	require.NoError(t, badgerStore.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		prefix := []byte{blobKeyPrefix}
		var keys [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
			deleted++
		}
		return nil
	}))
	return deleted, true
}
