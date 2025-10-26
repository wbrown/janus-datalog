package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// KeyMaskConstraint represents a constraint that can be evaluated on encoded keys
type KeyMaskConstraint struct {
	IndexType   IndexType
	Position    int    // 0=E, 1=A, 2=V, 3=T
	TargetBytes []byte // The bytes to match
	Offset      int    // Where in the key to look
	Length      int    // How many bytes to compare
}

// CreateKeyMaskConstraint creates a mask constraint for a given value
func CreateKeyMaskConstraint(index IndexType, position int, value interface{}) (*KeyMaskConstraint, error) {
	// Calculate offset based on index type and position
	var offset int
	switch index {
	case EAVT:
		// [1 prefix][20 entity][32 attr][value][20 tx]
		switch position {
		case 0: // Entity
			offset = 1
		case 1: // Attribute
			offset = 1 + 20
		case 2: // Value
			offset = 1 + 20 + 32
		case 3: // Transaction
			return nil, fmt.Errorf("transaction position not yet supported for key masks")
		default:
			return nil, fmt.Errorf("invalid position %d", position)
		}
	case AEVT:
		// [1 prefix][32 attr][20 entity][value][20 tx]
		switch position {
		case 0: // Entity (but in position 2 in storage)
			offset = 1 + 32
		case 1: // Attribute (in position 1 in storage)
			offset = 1
		case 2: // Value
			offset = 1 + 32 + 20
		case 3: // Transaction
			return nil, fmt.Errorf("transaction position not yet supported for key masks")
		default:
			return nil, fmt.Errorf("invalid position %d", position)
		}
	case AVET:
		// [1 prefix][32 attr][value][20 entity][20 tx]
		switch position {
		case 0: // Entity (but in position 3 in storage)
			offset = -1 // Variable position after value, can't use fixed offset
			return nil, fmt.Errorf("entity position in AVET requires variable offset")
		case 1: // Attribute
			offset = 1
		case 2: // Value
			offset = 1 + 32
		case 3: // Transaction
			return nil, fmt.Errorf("transaction position not yet supported for key masks")
		default:
			return nil, fmt.Errorf("invalid position %d", position)
		}
	case VAET:
		// [1 prefix][value][32 attr][20 entity][20 tx]
		// Value is variable length, so can only match on value itself
		if position != 2 {
			return nil, fmt.Errorf("VAET index only supports value position for key masks")
		}
		offset = 1
	default:
		return nil, fmt.Errorf("unsupported index type %v for key masks", index)
	}

	// Create the target bytes based on position and value type
	var targetBytes []byte

	// Handle entity and attribute positions specially (they're fixed-size)
	if position == 0 { // Entity position
		switch v := value.(type) {
		case *datalog.Identity:
			hash := v.Hash()
			targetBytes = hash[:]
		case datalog.Identity:
			hash := v.Hash()
			targetBytes = hash[:]
		default:
			return nil, fmt.Errorf("entity position requires Identity value, got %T", value)
		}
	} else if position == 1 { // Attribute position
		switch v := value.(type) {
		case *datalog.Keyword:
			// Attributes are stored as 32-byte padded strings
			targetBytes = make([]byte, 32)
			copy(targetBytes, v.String())
		case datalog.Keyword:
			targetBytes = make([]byte, 32)
			copy(targetBytes, v.String())
		case string:
			// Allow string for convenience
			targetBytes = make([]byte, 32)
			copy(targetBytes, v)
		default:
			return nil, fmt.Errorf("attribute position requires Keyword value, got %T", value)
		}
	} else {
		// Value position - include type byte
		switch v := value.(type) {
		case int64:
			targetBytes = make([]byte, 9)
			targetBytes[0] = byte(datalog.TypeInt)
			binary.BigEndian.PutUint64(targetBytes[1:], uint64(v))
		case int:
			// Convert int to int64
			targetBytes = make([]byte, 9)
			targetBytes[0] = byte(datalog.TypeInt)
			binary.BigEndian.PutUint64(targetBytes[1:], uint64(v))
		case float64:
			// IEEE 754 encoding for float64
			targetBytes = make([]byte, 9)
			targetBytes[0] = byte(datalog.TypeFloat)
			binary.BigEndian.PutUint64(targetBytes[1:], math.Float64bits(v))
		case string:
			targetBytes = make([]byte, 1+len(v))
			targetBytes[0] = byte(datalog.TypeString)
			copy(targetBytes[1:], v)
		case bool:
			targetBytes = make([]byte, 2)
			targetBytes[0] = byte(datalog.TypeBool)
			if v {
				targetBytes[1] = 1
			}
		case time.Time:
			// Time is stored as Unix nano timestamp
			targetBytes = make([]byte, 9)
			targetBytes[0] = byte(datalog.TypeTime)
			binary.BigEndian.PutUint64(targetBytes[1:], uint64(v.UnixNano()))
		case *datalog.Identity:
			// References are stored as 20-byte hashes
			targetBytes = make([]byte, 21) // 1 type + 20 hash
			targetBytes[0] = byte(datalog.TypeReference)
			hash := v.Hash()
			copy(targetBytes[1:], hash[:])
		case datalog.Identity:
			// Handle non-pointer Identity too
			targetBytes = make([]byte, 21)
			targetBytes[0] = byte(datalog.TypeReference)
			hash := v.Hash()
			copy(targetBytes[1:], hash[:])
		case *datalog.Keyword:
			// Keywords can be values too
			targetBytes = make([]byte, 1+len(v.String()))
			targetBytes[0] = byte(datalog.TypeKeyword)
			copy(targetBytes[1:], v.String())
		case datalog.Keyword:
			// Handle non-pointer Keyword
			targetBytes = make([]byte, 1+len(v.String()))
			targetBytes[0] = byte(datalog.TypeKeyword)
			copy(targetBytes[1:], v.String())
		default:
			// Can't optimize this type, return nil (safe fallback)
			return nil, fmt.Errorf("unsupported value type for key mask: %T", value)
		}
	}

	// Return the constraint with the calculated offset
	return &KeyMaskConstraint{
		IndexType:   index,
		Position:    position,
		TargetBytes: targetBytes,
		Offset:      offset,
		Length:      len(targetBytes),
	}, nil
}

// Matches checks if a key matches this constraint
func (c *KeyMaskConstraint) Matches(key []byte) bool {
	if len(key) < c.Offset+c.Length {
		return false
	}
	return bytes.Equal(key[c.Offset:c.Offset+c.Length], c.TargetBytes)
}

// KeyMaskIterator wraps a BadgerIterator and filters using key masks
type KeyMaskIterator struct {
	*BadgerIterator
	mask         *KeyMaskConstraint
	encoder      KeyEncoder
	currentDatom *datalog.Datom
	currentError error

	// Stats for annotations
	keysScanned   int
	keysMatched   int
	datomsDecoded int
}

// NewKeyMaskIterator creates an iterator that filters using key masks
func NewKeyMaskIterator(store *BadgerStore, index IndexType, start, end []byte, mask *KeyMaskConstraint) (Iterator, error) {
	txn := store.db.NewTransaction(false)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10000
	opts.PrefetchValues = false // Key-only scanning

	it := txn.NewIterator(opts)

	return &KeyMaskIterator{
		BadgerIterator: &BadgerIterator{
			txn:   txn,
			it:    it,
			start: start,
			end:   end,
			index: index,
		},
		mask:    mask,
		encoder: store.encoder,
	}, nil
}

// NewKeyMaskIteratorFromStore creates an iterator using the store's existing transaction
// This is more efficient when you want to reuse the store's transaction management
func NewKeyMaskIteratorFromStore(store *BadgerStore, index IndexType, start, end []byte, mask *KeyMaskConstraint) (Iterator, error) {
	// Reuse the store's existing ScanKeysOnly which manages transactions efficiently
	baseIter, err := store.ScanKeysOnly(index, start, end)
	if err != nil {
		return nil, err
	}

	// We need to wrap this in our own type that filters
	return &KeyMaskFilterWrapper{
		baseIter: baseIter,
		mask:     mask,
		encoder:  store.encoder,
		index:    index,
	}, nil
}

// KeyMaskFilterWrapper wraps an existing iterator and applies key mask filtering
type KeyMaskFilterWrapper struct {
	baseIter     Iterator
	mask         *KeyMaskConstraint
	encoder      KeyEncoder
	index        IndexType
	currentDatom *datalog.Datom
	currentError error

	// Stats
	keysScanned   int
	keysMatched   int
	datomsDecoded int
}

func (w *KeyMaskFilterWrapper) Next() bool {
	for w.baseIter.Next() {
		w.keysScanned++

		// Get the key from the base iterator
		// We need to get the raw key - check for both BadgerIterator and KeyOnlyIterator
		var key []byte
		gotKey := false

		if badgerIter, ok := w.baseIter.(*BadgerIterator); ok {
			key = badgerIter.it.Item().Key()
			gotKey = true
		} else if keyOnlyIter, ok := w.baseIter.(*KeyOnlyIterator); ok {
			// KeyOnlyIterator embeds BadgerIterator
			key = keyOnlyIter.it.Item().Key()
			gotKey = true
		}

		if gotKey {

			// Apply mask constraint
			if !w.mask.Matches(key) {
				continue
			}

			w.keysMatched++
			w.datomsDecoded++

			// Decode the datom
			w.currentDatom, w.currentError = DatomFromKey(w.index, key, w.encoder)
			if w.currentError != nil {
				continue
			}

			return true
		}

		// If it's not a BadgerIterator, fall back to regular filtering
		datom, err := w.baseIter.Datom()
		if err != nil {
			continue
		}

		// Can't do byte-level filtering without access to the key
		// Just check the value directly
		// For int64, the target bytes are [type_byte, 8_value_bytes]
		if v, ok := datom.V.(int64); ok && len(w.mask.TargetBytes) == 9 {
			// Extract the int64 value from the target bytes
			targetValue := int64(binary.BigEndian.Uint64(w.mask.TargetBytes[1:9]))
			if v == targetValue {
				w.currentDatom = datom
				w.keysMatched++
				return true
			}
		}
	}

	return false
}

func (w *KeyMaskFilterWrapper) Datom() (*datalog.Datom, error) {
	if w.currentError != nil {
		return nil, w.currentError
	}
	if w.currentDatom == nil {
		return nil, fmt.Errorf("no current datom")
	}
	return w.currentDatom, nil
}

func (w *KeyMaskFilterWrapper) Close() error {
	return w.baseIter.Close()
}

func (w *KeyMaskFilterWrapper) Seek(key []byte) {
	w.baseIter.Seek(key)
}

func (w *KeyMaskFilterWrapper) Stats() (keysScanned, keysMatched, datomsDecoded int) {
	return w.keysScanned, w.keysMatched, w.datomsDecoded
}

// Next advances to the next matching key
func (i *KeyMaskIterator) Next() bool {
	for i.BadgerIterator.Next() {
		i.keysScanned++

		key := i.it.Item().Key()

		// Apply mask constraint
		if !i.mask.Matches(key) {
			continue
		}

		i.keysMatched++

		// Only decode if the mask matches
		i.datomsDecoded++
		i.currentDatom, i.currentError = DatomFromKey(i.index, key, i.encoder)

		if i.currentError != nil {
			continue
		}

		return true
	}

	return false
}

// Datom returns the current datom
func (i *KeyMaskIterator) Datom() (*datalog.Datom, error) {
	if i.currentError != nil {
		return nil, i.currentError
	}
	if i.currentDatom == nil {
		return nil, fmt.Errorf("no current datom")
	}
	return i.currentDatom, nil
}

// Stats returns scanning statistics
func (i *KeyMaskIterator) Stats() (keysScanned, keysMatched, datomsDecoded int) {
	return i.keysScanned, i.keysMatched, i.datomsDecoded
}

// TryConvertConstraintsToMasks attempts to convert storage constraints to key masks
// Returns the first convertible constraint as a mask, or nil if none can be converted
// This function is safe to call with any constraints - it will return nil if optimization isn't possible
func TryConvertConstraintsToMasks(constraints []executor.StorageConstraint, index IndexType) *KeyMaskConstraint {
	for _, constraint := range constraints {
		// Use reflection to check if it's an equality constraint on value position
		// This is a bit hacky but avoids exposing internal types
		v := reflect.ValueOf(constraint)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		// Check if it has position and value fields
		if v.Kind() == reflect.Struct {
			posField := v.FieldByName("position")
			valField := v.FieldByName("value")

			if posField.IsValid() && valField.IsValid() {
				// Use unsafe to access unexported fields
				// This is necessary because the constraint types have unexported fields
				posField = reflect.NewAt(posField.Type(), unsafe.Pointer(posField.UnsafeAddr())).Elem()
				valField = reflect.NewAt(valField.Type(), unsafe.Pointer(valField.UnsafeAddr())).Elem()

				// Check if position is 2 (value position)
				if posField.Kind() == reflect.Int && posField.Int() == 2 {
					// Try to create a key mask for this value
					value := valField.Interface()
					mask, err := CreateKeyMaskConstraint(index, 2, value)
					if err == nil {
						return mask
					}
				}
			}
		}
	}

	return nil
}
