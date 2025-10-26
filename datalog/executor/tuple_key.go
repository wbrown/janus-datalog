package executor

import (
	"unsafe"

	"github.com/wbrown/janus-datalog/datalog"
)

// TupleKey represents a hashable key for a tuple or subset of tuple values
// It avoids string allocations by directly hashing the underlying data
type TupleKey struct {
	// We'll store a hash of the tuple values
	hash uint64
	// And keep references to the values for equality checking
	values []interface{}
}

// NewTupleKey creates a key from specific tuple positions
func NewTupleKey(tuple Tuple, indices []int) TupleKey {
	// Special case for single column - avoid allocation
	if len(indices) == 1 {
		val := tuple[indices[0]]
		return TupleKey{
			hash:   hashValue(val),
			values: []interface{}{val},
		}
	}

	values := make([]interface{}, len(indices))
	for i, idx := range indices {
		values[i] = tuple[idx]
	}
	return TupleKey{
		hash:   hashValues(values),
		values: values,
	}
}

// NewTupleKeyFull creates a key from an entire tuple
func NewTupleKeyFull(tuple Tuple) TupleKey {
	// Don't copy - just reference the original tuple
	// The tuple is already immutable in our usage
	return TupleKey{
		hash:   hashValues(tuple),
		values: tuple,
	}
}

// hashValues computes a hash for a slice of values without string conversion
func hashValues(values []interface{}) uint64 {
	// FNV-1a hash
	const prime = 1099511628211
	hash := uint64(14695981039346656037)

	for _, v := range values {
		// Hash based on type and value
		hash ^= hashValue(v)
		hash *= prime
	}

	return hash
}

// hashValue hashes a single value without string conversion
func hashValue(v interface{}) uint64 {
	// Handle pointers first - with interning, we'll see these often
	switch ptr := v.(type) {
	case *datalog.Identity:
		// For interned pointers, we can use pointer equality as a fast path
		// But for hashing, we need consistent hash based on value
		bytes := ptr.Hash()
		return hashBytes(bytes[:])
	case *datalog.Keyword:
		str := ptr.String()
		return hashString(str)
	case *uint64:
		return *ptr
	}

	// Handle regular values
	switch val := v.(type) {
	case datalog.Identity:
		// Hash the raw bytes directly
		bytes := val.Hash()
		return hashBytes(bytes[:])

	case datalog.Keyword:
		// Hash the string representation
		str := val.String()
		return hashString(str)

	case string:
		return hashString(val)

	case int:
		return uint64(val)

	case int64:
		return uint64(val)

	case uint64:
		return val

	case float64:
		// Use unsafe to get float bits
		return *(*uint64)(unsafe.Pointer(&val))

	case bool:
		if val {
			return 1
		}
		return 0

	case nil:
		return 0

	default:
		// Fallback: use pointer as hash
		return uint64(uintptr(unsafe.Pointer(&v)))
	}
}

// hashBytes hashes a byte slice
func hashBytes(b []byte) uint64 {
	const prime = 1099511628211
	hash := uint64(14695981039346656037)

	for _, byte := range b {
		hash ^= uint64(byte)
		hash *= prime
	}

	return hash
}

// hashString hashes a string without allocation
func hashString(s string) uint64 {
	const prime = 1099511628211
	hash := uint64(14695981039346656037)

	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime
	}

	return hash
}

// Equal checks if two keys are equal
func (k TupleKey) Equal(other TupleKey) bool {
	// Quick hash check first
	if k.hash != other.hash {
		return false
	}

	// Then check actual values
	if len(k.values) != len(other.values) {
		return false
	}

	for i, v1 := range k.values {
		v2 := other.values[i]
		if !datalog.ValuesEqual(v1, v2) {
			return false
		}
	}

	return true
}

// TupleKeyMap wraps a simple Go map for better performance
// We use the hash directly as the key and handle collisions
type TupleKeyMap struct {
	// Use native Go map with hash as key
	m map[uint64][]mapEntry
}

type mapEntry struct {
	values []interface{} // The actual tuple values for collision checking
	value  interface{}   // The stored value
}

// NewTupleKeyMap creates a new TupleKeyMap
func NewTupleKeyMap() *TupleKeyMap {
	return &TupleKeyMap{
		m: make(map[uint64][]mapEntry),
	}
}

// NewTupleKeyMapWithCapacity creates a new TupleKeyMap pre-sized to hold expectedSize entries
func NewTupleKeyMapWithCapacity(expectedSize int) *TupleKeyMap {
	// Pre-size the map to avoid reallocation
	// Use expectedSize directly as map capacity
	return &TupleKeyMap{
		m: make(map[uint64][]mapEntry, expectedSize),
	}
}

// Put adds or updates a key-value pair
func (m *TupleKeyMap) Put(key TupleKey, value interface{}) {
	entries := m.m[key.hash]

	// Check if key already exists by comparing values
	for i := range entries {
		if tupleValuesEqual(entries[i].values, key.values) {
			entries[i].value = value
			return
		}
	}

	// Add new entry
	m.m[key.hash] = append(entries, mapEntry{
		values: key.values,
		value:  value,
	})
}

// Get retrieves a value by key
func (m *TupleKeyMap) Get(key TupleKey) (interface{}, bool) {
	entries, ok := m.m[key.hash]
	if !ok {
		return nil, false
	}

	for _, entry := range entries {
		if tupleValuesEqual(entry.values, key.values) {
			return entry.value, true
		}
	}

	return nil, false
}

// Exists checks if a key exists
func (m *TupleKeyMap) Exists(key TupleKey) bool {
	entries, ok := m.m[key.hash]
	if !ok {
		return false
	}

	for _, entry := range entries {
		if tupleValuesEqual(entry.values, key.values) {
			return true
		}
	}

	return false
}

// tupleValuesEqual checks if two value slices are equal
func tupleValuesEqual(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !datalog.ValuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
