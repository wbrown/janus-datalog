package executor

import (
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
			hash:   datalog.HashValue(val),
			values: []interface{}{val},
		}
	}

	values := make([]interface{}, len(indices))
	for i, idx := range indices {
		values[i] = tuple[idx]
	}
	return TupleKey{
		hash:   datalog.HashValues(values),
		values: values,
	}
}

// NewTupleKeyFull creates a key from an entire tuple
func NewTupleKeyFull(tuple Tuple) TupleKey {
	// Don't copy - just reference the original tuple
	// The tuple is already immutable in our usage
	return TupleKey{
		hash:   datalog.HashValues(tuple),
		values: tuple,
	}
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

// TupleKeyMap wraps a hash-indexed table holding each hash's first entry
// inline in the map slot — no per-key backing array. Genuine hash
// collisions are rare by construction, so second and later entries sharing
// a hash spill into a separate overflow map that stays nil until the first
// collision. Every lookup compares full values, so collisions cost time,
// never correctness.
type TupleKeyMap struct {
	entries  map[uint64]mapEntry
	overflow map[uint64][]mapEntry // nil until the first genuine collision
}

type mapEntry struct {
	values []interface{} // The actual tuple values for collision checking
	value  interface{}   // The stored value
}

// NewTupleKeyMap creates a new TupleKeyMap
func NewTupleKeyMap() *TupleKeyMap {
	return &TupleKeyMap{
		entries: make(map[uint64]mapEntry),
	}
}

// NewTupleKeyMapWithCapacity creates a new TupleKeyMap pre-sized to hold
// expectedSize entries. A negative expectedSize, a stream's size answer,
// starts the map empty.
func NewTupleKeyMapWithCapacity(expectedSize int) *TupleKeyMap {
	// Pre-size the map to avoid reallocation
	// Use expectedSize directly as map capacity
	return &TupleKeyMap{
		entries: make(map[uint64]mapEntry, expectedSize),
	}
}

// Put adds or updates a key-value pair
func (m *TupleKeyMap) Put(key TupleKey, value interface{}) {
	first, ok := m.entries[key.hash]
	if !ok {
		m.entries[key.hash] = mapEntry{values: key.values, value: value}
		return
	}
	if tupleValuesEqual(first.values, key.values) {
		first.value = value
		m.entries[key.hash] = first
		return
	}
	over := m.overflow[key.hash]
	for i := range over {
		if tupleValuesEqual(over[i].values, key.values) {
			over[i].value = value
			return
		}
	}
	if m.overflow == nil {
		m.overflow = make(map[uint64][]mapEntry)
	}
	m.overflow[key.hash] = append(over, mapEntry{values: key.values, value: value})
}

// PutValue adds or replaces a single-value key without constructing a
// one-element TupleKey values slice on lookup-heavy paths.
func (m *TupleKeyMap) PutValue(keyValue, value interface{}) {
	hash := datalog.HashValue(keyValue)
	first, ok := m.entries[hash]
	if !ok {
		m.entries[hash] = mapEntry{values: []interface{}{keyValue}, value: value}
		return
	}
	if len(first.values) == 1 && datalog.ValuesEqual(first.values[0], keyValue) {
		first.value = value
		m.entries[hash] = first
		return
	}
	over := m.overflow[hash]
	for i := range over {
		if len(over[i].values) == 1 && datalog.ValuesEqual(over[i].values[0], keyValue) {
			over[i].value = value
			return
		}
	}
	if m.overflow == nil {
		m.overflow = make(map[uint64][]mapEntry)
	}
	m.overflow[hash] = append(over, mapEntry{values: []interface{}{keyValue}, value: value})
}

// PutIfAbsent inserts key with the given value only if the key is not
// already present, and reports whether it already existed. It walks the
// hash's entries exactly once.
// This is the hot path for join deduplication, where every matched tuple
// probes the seen set.
func (m *TupleKeyMap) PutIfAbsent(key TupleKey, value interface{}) (existed bool) {
	first, ok := m.entries[key.hash]
	if !ok {
		m.entries[key.hash] = mapEntry{values: key.values, value: value}
		return false
	}
	if tupleValuesEqual(first.values, key.values) {
		return true
	}
	over := m.overflow[key.hash]
	for i := range over {
		if tupleValuesEqual(over[i].values, key.values) {
			return true
		}
	}
	if m.overflow == nil {
		m.overflow = make(map[uint64][]mapEntry)
	}
	m.overflow[key.hash] = append(over, mapEntry{values: key.values, value: value})
	return false
}

// Get retrieves a value by key
func (m *TupleKeyMap) Get(key TupleKey) (interface{}, bool) {
	first, ok := m.entries[key.hash]
	if !ok {
		return nil, false
	}
	if tupleValuesEqual(first.values, key.values) {
		return first.value, true
	}
	for _, entry := range m.overflow[key.hash] {
		if tupleValuesEqual(entry.values, key.values) {
			return entry.value, true
		}
	}
	return nil, false
}

// GetValue retrieves a single-value key without allocating a TupleKey.
func (m *TupleKeyMap) GetValue(keyValue interface{}) (interface{}, bool) {
	hash := datalog.HashValue(keyValue)
	first, ok := m.entries[hash]
	if !ok {
		return nil, false
	}
	if len(first.values) == 1 && datalog.ValuesEqual(first.values[0], keyValue) {
		return first.value, true
	}
	for _, entry := range m.overflow[hash] {
		if len(entry.values) == 1 && datalog.ValuesEqual(entry.values[0], keyValue) {
			return entry.value, true
		}
	}
	return nil, false
}

// Exists checks if a key exists
func (m *TupleKeyMap) Exists(key TupleKey) bool {
	first, ok := m.entries[key.hash]
	if !ok {
		return false
	}
	if tupleValuesEqual(first.values, key.values) {
		return true
	}
	for _, entry := range m.overflow[key.hash] {
		if tupleValuesEqual(entry.values, key.values) {
			return true
		}
	}
	return false
}

// hashTuplePositions mirrors NewTupleKey's hashing exactly without building
// a key: a single position hashes the bare value; multiple positions
// FNV-fold the per-value hashes in position order.
func hashTuplePositions(tuple Tuple, indices []int) uint64 {
	if len(indices) == 1 {
		return datalog.HashValue(tuple[indices[0]])
	}
	const prime = 1099511628211
	hash := uint64(14695981039346656037)
	for _, idx := range indices {
		hash ^= datalog.HashValue(tuple[idx])
		hash *= prime
	}
	return hash
}

// tupleValuesEqualPositions compares a stored key's values against the
// selected tuple positions without materializing a slice.
func tupleValuesEqualPositions(stored []interface{}, tuple Tuple, indices []int) bool {
	if len(stored) != len(indices) {
		return false
	}
	for i, idx := range indices {
		if !datalog.ValuesEqual(stored[i], tuple[idx]) {
			return false
		}
	}
	return true
}

// GetPositions retrieves the value stored under the key formed by the
// selected tuple positions, without materializing a TupleKey. Probes
// allocate nothing.
func (m *TupleKeyMap) GetPositions(tuple Tuple, indices []int) (interface{}, bool) {
	hash := hashTuplePositions(tuple, indices)
	first, ok := m.entries[hash]
	if !ok {
		return nil, false
	}
	if tupleValuesEqualPositions(first.values, tuple, indices) {
		return first.value, true
	}
	for _, entry := range m.overflow[hash] {
		if tupleValuesEqualPositions(entry.values, tuple, indices) {
			return entry.value, true
		}
	}
	return nil, false
}

// PutIfAbsentPositions inserts the key formed by the selected tuple
// positions only if absent, and reports whether it already existed. The
// key's owned values slice materializes only on actual insertion, so the
// already-seen path — the common case in deduplication — allocates nothing.
func (m *TupleKeyMap) PutIfAbsentPositions(tuple Tuple, indices []int, value interface{}) (existed bool) {
	hash := hashTuplePositions(tuple, indices)
	first, ok := m.entries[hash]
	if ok {
		if tupleValuesEqualPositions(first.values, tuple, indices) {
			return true
		}
		for _, entry := range m.overflow[hash] {
			if tupleValuesEqualPositions(entry.values, tuple, indices) {
				return true
			}
		}
	}
	values := make([]interface{}, len(indices))
	for i, idx := range indices {
		values[i] = tuple[idx]
	}
	if !ok {
		m.entries[hash] = mapEntry{values: values, value: value}
		return false
	}
	if m.overflow == nil {
		m.overflow = make(map[uint64][]mapEntry)
	}
	m.overflow[hash] = append(m.overflow[hash], mapEntry{values: values, value: value})
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
