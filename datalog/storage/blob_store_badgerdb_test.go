//go:build !(js && wasm)

package storage

import (
	"crypto/sha1"
	"os"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Blob Store Unit Tests ----

func openTestBadger(t *testing.T) (*badger.DB, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "blob-test-*")
	require.NoError(t, err)

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	require.NoError(t, err)

	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func TestBlobStore_PutGet(t *testing.T) {
	db, cleanup := openTestBadger(t)
	defer cleanup()

	data := []byte("compressed blob content " + strings.Repeat("x", 100))
	hash := sha1.Sum(data)

	// Put
	err := db.Update(func(txn *badger.Txn) error {
		return putBlob(txn.Set, hash, data)
	})
	require.NoError(t, err)

	// Get
	result, err := getBlob(db, hash)
	require.NoError(t, err)
	assert.Equal(t, data, result)
}

func TestBlobStore_ContentAddressing(t *testing.T) {
	db, cleanup := openTestBadger(t)
	defer cleanup()

	data := []byte("deduplicated content")
	hash := sha1.Sum(data)

	// Put twice
	for i := 0; i < 2; i++ {
		err := db.Update(func(txn *badger.Txn) error {
			return putBlob(txn.Set, hash, data)
		})
		require.NoError(t, err)
	}

	// Should still read correctly
	result, err := getBlob(db, hash)
	require.NoError(t, err)
	assert.Equal(t, data, result)

	// Count blob keys
	count := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{blobKeyPrefix}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, count, "should have exactly 1 blob key")
}

func TestBlobStore_Missing(t *testing.T) {
	db, cleanup := openTestBadger(t)
	defer cleanup()

	hash := sha1.Sum([]byte("nonexistent"))
	_, err := getBlob(db, hash)
	assert.Error(t, err)
}

func TestBlobStore_KeyPrefix(t *testing.T) {
	// Verify blob prefix doesn't collide with any index prefix
	assert.Greater(t, blobKeyPrefix, byte(TAEV), "blob prefix must be > highest index prefix")
}
