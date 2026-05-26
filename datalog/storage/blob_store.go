package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Blob store: content-addressed storage for Tier 3 compressed values.
// Values whose compressed representation exceeds the key size limit are
// stored here, with a SHA1 hash in the index key pointing to the blob.
//
// Blob key format: [0xFF][sha1_hash:20]
// The 0xFF prefix separates blobs from all index keys (0x00-0x06).

const blobKeyPrefix = byte(0xFF)

// putBlob stores compressed data at its content-addressed key.
// Idempotent: writing the same content twice is a no-op.
func putBlob(txn *badger.Txn, hash [20]byte, data []byte) error {
	key := make([]byte, 21)
	key[0] = blobKeyPrefix
	copy(key[1:], hash[:])
	return txn.Set(key, data)
}

// getBlob retrieves compressed data by its SHA1 hash.
func getBlob(db *badger.DB, hash [20]byte) ([]byte, error) {
	key := make([]byte, 21)
	key[0] = blobKeyPrefix
	copy(key[1:], hash[:])

	var result []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("blob not found for hash %x: %w", hash, err)
	}
	return result, nil
}
