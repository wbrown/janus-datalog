package executor

import (
	"time"
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
	// Special case for single symbol - avoid allocation
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
	case datalog.Identity:
		// Check for nil - Identity is a pointer type alias
		if ptr == nil {
			return 0
		}
		// For interned pointers, we can use pointer equality as a fast path
		// But for hashing, we need consistent hash based on value
		bytes := ptr.Hash()
		return hashBytes(bytes[:])
	case datalog.Keyword:
		// Check for nil - Keyword is a pointer type alias
		if ptr == nil {
			return 0
		}
		str := ptr.String()
		return hashString(str)
	case *uint64:
		if ptr == nil {
			return 0
		}
		return *ptr
	}

	// Handle regular values
	switch val := v.(type) {
	case datalog.Identity:
		// Check for nil - Identity is a pointer type alias
		if val == nil {
			return 0
		}
		// Hash the raw bytes directly
		bytes := val.Hash()
		return hashBytes(bytes[:])

	case datalog.Keyword:
		// Check for nil - Keyword is a pointer type alias
		if val == nil {
			return 0
		}
		// Use pointer value for hash - interned keywords are unique pointers
		return uint64(uintptr(unsafe.Pointer(val)))

	case string:
		return hashString(val)

	case []byte:
		// Hash by content. Without this case, []byte falls through to the
		// default pointer-address hash below, so two equal byte slices get
		// different (nondeterministic) hashes and never collide in a
		// TupleKeyMap — breaking joins and dedup on byte-valued attributes.
		return hashBytes(val)

	case time.Time:
		// Hash by the absolute instant. Without this case, time.Time falls
		// through to the address-based default below, so equal times hash
		// differently across call sites and never match in a TupleKeyMap —
		// breaking joins and dedup on time-valued attributes (e.g. :created-at)
		// nondeterministically by platform/stack layout. Consistent with
		// datalog.ValuesEqual, which compares time.Time by instant (Equal).
		return hashTime(val)

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

	case datalog.ElementID:
		return val.Lamport ^ (val.ReplicaID * 1099511628211)

	case *datalog.ElementID:
		if val == nil {
			return 0
		}
		return val.Lamport ^ (val.ReplicaID * 1099511628211)

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

// hashTime hashes a time.Time by its absolute instant (seconds + nanoseconds),
// consistent with datalog.ValuesEqual's instant-based comparison: two times that
// compare Equal hash identically. Uses Unix()+Nanosecond() rather than UnixNano()
// to avoid int64 overflow for out-of-range times (e.g. the 0001-01-01 default).
func hashTime(t time.Time) uint64 {
	const prime = 1099511628211
	hash := uint64(14695981039346656037)
	hash ^= uint64(t.Unix())
	hash *= prime
	hash ^= uint64(t.Nanosecond())
	hash *= prime
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

// PutIfAbsent inserts key with the given value only if the key is not
// already present, and reports whether it already existed. It walks the
// hash bucket exactly once, where a separate Exists+Put pair would walk it
// twice (running tupleValuesEqual against every entry both times). This is
// the hot path for join deduplication, where every matched row probes the
// seen set.
func (m *TupleKeyMap) PutIfAbsent(key TupleKey, value interface{}) (existed bool) {
	entries := m.m[key.hash]
	for i := range entries {
		if tupleValuesEqual(entries[i].values, key.values) {
			return true
		}
	}
	m.m[key.hash] = append(entries, mapEntry{
		values: key.values,
		value:  value,
	})
	return false
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
