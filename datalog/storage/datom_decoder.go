package storage

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/codec"
)

// Cache for attribute strings to avoid repeated string allocations
// Since attribute sets are small (typically 6-20 unique attributes per schema),
// this cache effectively eliminates string allocations for attribute decoding
var attrStringCache sync.Map // map[[32]byte]string

// DatomFromKey reconstructs a datom from an index key
// This allows us to avoid fetching values since the key contains all information
func DatomFromKey(index IndexType, key []byte, encoder KeyEncoder) (*datalog.Datom, error) {
	// DecodeKey already handles the index-specific ordering and returns
	// components in standard EAVT order
	eBytes, aBytes, vBytes, txBytes, err := encoder.DecodeKey(index, key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode key: %w", err)
	}

	// Create storage datom from the decoded components
	var sd StorageDatom

	// Entity (20 bytes)
	if len(eBytes) != 20 {
		return nil, fmt.Errorf("invalid entity size: %d", len(eBytes))
	}
	copy(sd.E[:], eBytes)

	// Attribute (32 bytes)
	if len(aBytes) != 32 {
		return nil, fmt.Errorf("invalid attribute size: %d", len(aBytes))
	}
	copy(sd.A[:], aBytes)

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
	sd.V = v

	// Transaction (20 bytes)
	if len(txBytes) != 20 {
		return nil, fmt.Errorf("invalid tx size: %d", len(txBytes))
	}
	copy(sd.Tx[:], txBytes)

	// Convert storage datom to user datom
	// Note: aBytes is already decoded from L85 if applicable, it contains the raw keyword string

	// Use cached attribute string to avoid repeated allocations
	var attrString string
	if cached, ok := attrStringCache.Load(sd.A); ok {
		attrString = cached.(string)
	} else {
		attrString = string(bytes.TrimRight(aBytes, "\x00"))
		attrStringCache.Store(sd.A, attrString)
	}

	// Allocate datom directly
	// Note: Entity and Keyword are already interned via InternIdentityFromHash/InternKeyword,
	// so we're only allocating the Datom struct itself (96 bytes) plus the attribute string
	// which is cached above. This is much cheaper than the original 8GB of allocations.
	return &datalog.Datom{
		E:  *datalog.InternIdentityFromHash(sd.E),
		A:  *datalog.InternKeyword(attrString),
		V:  sd.V,
		Tx: sd.Tx.Uint64(),
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

	// Decode datom from key
	key := i.it.Item().Key()

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
