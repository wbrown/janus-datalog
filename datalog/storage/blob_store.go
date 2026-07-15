//go:build !(js && wasm)

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

type badgerDBBlobReader struct {
	db *badger.DB
}

func (r badgerDBBlobReader) ReadBlob(hash [20]byte, read func([]byte) error) error {
	return r.db.View(func(txn *badger.Txn) error {
		return badgerTxnBlobReader{txn: txn}.ReadBlob(hash, read)
	})
}

type badgerTxnBlobReader struct {
	txn *badger.Txn
}

func (r badgerTxnBlobReader) ReadBlob(hash [20]byte, read func([]byte) error) error {
	var key [21]byte
	key[0] = blobKeyPrefix
	copy(key[1:], hash[:])
	item, err := r.txn.Get(key[:])
	if err != nil {
		return fmt.Errorf("blob not found for hash %x: %w", hash, err)
	}
	if err := item.Value(read); err != nil {
		return fmt.Errorf("reading blob for hash %x: %w", hash, err)
	}
	return nil
}

// putBlob stores compressed data at its content-addressed key.
// Idempotent: writing the same content twice is a no-op.
func putBlob(txn *badger.Txn, hash [20]byte, data []byte) error {
	var key [21]byte
	key[0] = blobKeyPrefix
	copy(key[1:], hash[:])
	return txn.Set(key[:], data)
}

// getBlob retrieves compressed data by its SHA1 hash.
func getBlob(db *badger.DB, hash [20]byte) ([]byte, error) {
	var result []byte
	err := (badgerDBBlobReader{db: db}).ReadBlob(hash, func(value []byte) error {
		result = append([]byte(nil), value...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
