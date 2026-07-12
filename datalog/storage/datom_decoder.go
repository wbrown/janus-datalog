package storage

import (
	"fmt"
	"runtime"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
)

// DatomFromKey reconstructs a datom from an index key.
// For Tier 3 (hashed) values, db is required to fetch compressed data from the blob store.
// Pass nil for db when Tier 3 values are not expected.
func DatomFromKey(index IndexType, key []byte, encoder *BinaryKeyEncoder, db *badger.DB) (datalog.Datom, error) {
	// DecodeKey returns fixed-size arrays directly (no heap escape)
	entity, attr, vBytes, tx, op, afterRef, err := encoder.DecodeKey(index, key)
	if err != nil {
		return datalog.Datom{}, fmt.Errorf("failed to decode key: %w", err)
	}

	// Value (variable length) - first byte is type, rest is data
	if len(vBytes) < 1 {
		return datalog.Datom{}, fmt.Errorf("value bytes too short: %d", len(vBytes))
	}
	vType := datalog.ValueType(vBytes[0])
	vData := vBytes[1:]

	// Tier 3: hashed values need blob store lookup
	if vType == datalog.TypeHashedString || vType == datalog.TypeHashedBytes {
		if db == nil {
			return datalog.Datom{}, fmt.Errorf("hashed value requires database for blob lookup")
		}
		if len(vData) != 20 {
			return datalog.Datom{}, fmt.Errorf("hashed value hash must be 20 bytes, got %d", len(vData))
		}
		var hash [20]byte
		copy(hash[:], vData)
		compressed, err := getBlob(db, hash)
		if err != nil {
			return datalog.Datom{}, fmt.Errorf("failed to read blob: %w", err)
		}
		// Decompress and return as the original type
		decompressed, err := datalog.ValueFromBytes(
			datalog.ValueType(vType-2), // TypeHashedString→TypeCompressedString, etc.
			compressed,
		)
		if err != nil {
			return datalog.Datom{}, fmt.Errorf("failed to decompress blob: %w", err)
		}
		return datalog.Datom{
			E:        datalog.InternIdentityFromHash(entity),
			A:        datalog.InternKeywordFromBytes(attr),
			V:        decompressed,
			Tx:       Tx(tx).ToElementID(),
			Op:       datalog.CRDTOp(op),
			AfterRef: Tx(afterRef).ToElementID(),
		}, nil
	}

	v, err := datalog.ValueFromBytes(vType, vData)
	if err != nil {
		return datalog.Datom{}, fmt.Errorf("failed to decode value: %w", err)
	}

	// E is reconstructed from the stored 20-byte hash only: the original name
	// string is not persisted (entities are content-addressed — see the
	// datalog.identity doc). The returned Identity's String() is therefore the
	// L85 hash unless the same name was also interned in-process. Store the name
	// as an attribute if you need it back.
	return datalog.Datom{
		E:        datalog.InternIdentityFromHash(entity),
		A:        datalog.InternKeywordFromBytes(attr),
		V:        v,
		Tx:       Tx(tx).ToElementID(),
		Op:       datalog.CRDTOp(op),
		AfterRef: Tx(afterRef).ToElementID(),
	}, nil
}

// KeyOnlyIterator wraps a BadgerIterator to decode datoms from keys
// This avoids fetching values entirely (except for Tier 3 blob lookups)
type KeyOnlyIterator struct {
	*BadgerIterator
	encoder      *BinaryKeyEncoder
	db           *badger.DB
	currentDatom datalog.Datom
	hasDatom     bool
	currentError error
}

// NewKeyOnlyIterator creates an iterator that decodes datoms from keys
func NewKeyOnlyIterator(store *BadgerStore, index IndexType, start, end []byte) (Iterator, error) {
	txn := store.db.NewTransaction(false)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10000   // Much higher for key-only
	opts.PrefetchValues = false // Don't fetch values!

	it := txn.NewIterator(opts)

	bi := &BadgerIterator{
		txn:   txn,
		it:    it,
		start: start,
		end:   end,
		index: index,
	}
	runtime.SetFinalizer(bi, (*BadgerIterator).Close)
	return &KeyOnlyIterator{
		BadgerIterator: bi,
		encoder:        store.encoder,
		db:             store.db,
	}, nil
}

// Next advances the iterator
func (i *KeyOnlyIterator) Next() bool {
	// Clear previous state
	i.hasDatom = false
	i.currentError = nil

	// Use parent's Next
	hasNext := i.BadgerIterator.Next()
	if !hasNext {
		return false
	}

	// Decode datom from key - must copy since BadgerDB reuses the key buffer
	key := i.it.Item().KeyCopy(nil)

	i.currentDatom, i.currentError = DatomFromKey(i.index, key, i.encoder, i.db)

	if i.currentError != nil {
		return false
	}

	i.hasDatom = true
	return true
}

// Datom returns the current datom decoded from the key
func (i *KeyOnlyIterator) Datom() (*datalog.Datom, error) {
	if i.currentError != nil {
		return nil, i.currentError
	}
	if !i.hasDatom {
		return nil, fmt.Errorf("no current datom")
	}
	return &i.currentDatom, nil
}

// Error returns the latest decode error if any. Unlike Datom(), which
// returns an error only when called on a failing current item, Error()
// surfaces the error persistently so that callers who have already
// abandoned iteration (Next() returned false) can still detect it.
func (i *KeyOnlyIterator) Error() error { return i.currentError }
