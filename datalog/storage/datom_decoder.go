package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// DatomFromKey reconstructs a datom from an index key
// This allows us to avoid fetching values since the key contains all information
func DatomFromKey(index IndexType, key []byte, encoder KeyEncoder) (*datalog.Datom, error) {
	// DecodeKey returns fixed-size arrays directly (no heap escape)
	entity, attr, vBytes, tx, op, afterRef, err := encoder.DecodeKey(index, key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key: %w", err)
	}

	// Value (variable length) - first byte is type, rest is data
	if len(vBytes) < 1 {
		return nil, fmt.Errorf("value bytes too short: %d", len(vBytes))
	}
	vType := datalog.ValueType(vBytes[0])
	vData := vBytes[1:]

	// Special handling for references which might be L85-encoded
	if vType == datalog.TypeReference && len(vData) == 25 {
		// L85-encoded reference - decode it first
		if refBytes, err := codec.DecodeFixed20(string(vData)); err == nil {
			vData = refBytes[:]
		}
	}

	v, err := datalog.ValueFromBytes(vType, vData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value: %w", err)
	}

	// Convert to user datom using direct array-based interning
	// No intermediate copies needed - arrays go straight to intern cache lookup
	// tx is [16]byte, convert to Tx type then to uint64
	return &datalog.Datom{
		E:        datalog.InternIdentityFromHash(entity),
		A:        datalog.InternKeywordFromBytes(attr),
		V:        v,
		Tx:       Tx(tx).ToElementID(),
		Op:       datalog.CRDTOp(op),
		AfterRef: Tx(afterRef).ToElementID(),
	}, nil
}

// KeyOnlyIterator wraps a BadgerIterator to decode datoms from keys
// This avoids fetching values entirely
type KeyOnlyIterator struct {
	*BadgerIterator
	encoder      KeyEncoder
	currentDatom *datalog.Datom
	currentError error
}

// NewKeyOnlyIterator creates an iterator that decodes datoms from keys
func NewKeyOnlyIterator(store *BadgerStore, index IndexType, start, end []byte) (Iterator, error) {
	txn := store.db.NewTransaction(false)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10000   // Much higher for key-only
	opts.PrefetchValues = false // Don't fetch values!

	it := txn.NewIterator(opts)

	return &KeyOnlyIterator{
		BadgerIterator: &BadgerIterator{
			txn:   txn,
			it:    it,
			start: start,
			end:   end,
			index: index,
		},
		encoder: store.encoder,
	}, nil
}

// Next advances the iterator
func (i *KeyOnlyIterator) Next() bool {
	// Clear previous state
	i.currentDatom = nil
	i.currentError = nil

	// Use parent's Next
	hasNext := i.BadgerIterator.Next()
	if !hasNext {
		return false
	}

	// Decode datom from key - must copy since BadgerDB reuses the key buffer
	key := i.it.Item().KeyCopy(nil)

	i.currentDatom, i.currentError = DatomFromKey(i.index, key, i.encoder)

	if i.currentError != nil {
		return false
	}

	return true
}

// Datom returns the current datom decoded from the key
func (i *KeyOnlyIterator) Datom() (*datalog.Datom, error) {
	if i.currentError != nil {
		return nil, i.currentError
	}
	if i.currentDatom == nil {
		return nil, fmt.Errorf("no current datom")
	}
	return i.currentDatom, nil
}
