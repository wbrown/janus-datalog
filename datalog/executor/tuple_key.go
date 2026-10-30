package executor

import "github.com/wbrown/janus-datalog/datalog"

// TupleKeyMap holds one value per key, over the one table, TupleSet
// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md): each member is a key tuple the
// caller owns, its key read in place at the stored positions, probed by a
// tuple at the probing positions, with a value held beside it. A member is
// held as given and never copied. A put under a key held replaces its value
// and keeps the member first given. A probe allocates nothing. A capacity
// hint sizes the table and the members at the first put, and nothing before
// it.
type TupleKeyMap struct {
	keys   TupleSet
	values []any // the value per member, at the member's index
}

// NewTupleKeyMap is an empty map over the key's positions in the probing
// tuples and in the held tuples, sized at its first put for capacity keys
// when that is above zero.
func NewTupleKeyMap(probePos, storedPos []int, capacity int) *TupleKeyMap {
	return &TupleKeyMap{keys: TupleSet{probePos: probePos, storedPos: storedPos, hint: capacity}}
}

// Get answers the value held under tuple's key at the probing positions,
// and whether one is held.
func (m *TupleKeyMap) Get(tuple Tuple) (any, bool) {
	i, ok := m.keys.Find(tuple)
	if !ok {
		return nil, false
	}
	return m.values[i], true
}

// Put holds value under member's key at the stored positions. A key held
// keeps its member and takes the value.
func (m *TupleKeyMap) Put(member Tuple, value any) {
	if i, ok := m.keys.FindAt(member, m.keys.storedPos); ok {
		m.values[i] = value
		return
	}
	m.hold(member, value)
}

// PutIfAbsent holds value under member's key at the stored positions when
// the key is new, and reports whether the key was held.
func (m *TupleKeyMap) PutIfAbsent(member Tuple, value any) (existed bool) {
	if _, ok := m.keys.FindAt(member, m.keys.storedPos); ok {
		return true
	}
	m.hold(member, value)
	return false
}

// hold adds a member whose key no member holds and its value at the index
// the set gives it. The values take the members' capacity when they are
// full, so that they grow with the set and no put between the set's
// doublings grows them.
func (m *TupleKeyMap) hold(member Tuple, value any) {
	m.keys.Add(member)
	if len(m.values) == cap(m.values) {
		values := make([]any, len(m.values), cap(m.keys.members))
		copy(values, m.values)
		m.values = values
	}
	m.values = append(m.values, value)
}

// Len is the keys held.
func (m *TupleKeyMap) Len() int {
	return m.keys.Len()
}

// hashTuplePositions is the hash every tuple key of this package hashes by:
// a single position hashes the bare value, and several positions FNV-fold
// the per-value hashes in position order.
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
