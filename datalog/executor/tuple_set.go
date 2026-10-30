package executor

import "math/bits"

// TupleSet is a set of keys held in place in the tuples that carry them, and
// the one table under JoinIndex and TupleKeyMap
// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md): each member is a tuple the
// caller added, its key read at the stored positions, probed by a tuple of
// another relation at that relation's positions of the same key, the
// positions given at construction or per probe. A key is read from its
// member and never copied, so a member is held as given and is stable for
// the set's life. A probe allocates nothing. The hash is hashTuplePositions',
// the one every tuple key of this package hashes by, computed from a member
// at its stored positions whenever the table is built. The table is
// open-addressed over member indices, a power of two in length and at most
// half full, probed linearly from the slot the hash names, the top bits of
// the hash's product with the golden ratio's fraction, so keys whose hashes
// agree in their low bits, an interned pointer's address among them, spread
// over the table; two members under one hash sit in two slots and are told
// apart by their keys' values. A capacity hint sizes the table and the
// members at the first add, and nothing before it.
type TupleSet struct {
	probePos  []int
	storedPos []int
	hint      int
	members   []Tuple
	table     []int32 // each slot a member's index, or -1 when empty
	shift     uint    // 64 less the table's width in bits, the shift a slot is taken by
}

// NewTupleSet is an empty set over the key's positions in the probing tuples
// and in the held tuples, sized at its first add for capacity members when
// that is above zero.
func NewTupleSet(probePos, storedPos []int, capacity int) *TupleSet {
	return &TupleSet{probePos: probePos, storedPos: storedPos, hint: capacity}
}

// Find answers the index of the member whose key equals tuple's key at the
// probing positions, and whether one exists.
func (s *TupleSet) Find(tuple Tuple) (int, bool) {
	return s.FindAt(tuple, s.probePos)
}

// FindAt is Find with the probing positions given.
func (s *TupleSet) FindAt(tuple Tuple, probePos []int) (int, bool) {
	if len(s.table) == 0 {
		return 0, false
	}
	mask := uint64(len(s.table) - 1)
	for slot := s.slot(hashTuplePositions(tuple, probePos)); ; slot = (slot + 1) & mask {
		idx := s.table[slot]
		if idx < 0 {
			return 0, false
		}
		if tupleKeysMatch(s.members[idx], s.storedPos, tuple, probePos) {
			return int(idx), true
		}
	}
}

// slot is the table slot a key's hash names, the first slot of the key's
// probe run: the top bits of the hash's product with the golden ratio's
// fraction, as many as the table is wide.
func (s *TupleSet) slot(hash uint64) uint64 {
	return (hash * 0x9E3779B97F4A7C15) >> s.shift
}

// Probe reports whether a member's key equals tuple's key at the probing
// positions.
func (s *TupleSet) Probe(tuple Tuple) bool {
	_, ok := s.Find(tuple)
	return ok
}

// Add holds member under its key at the stored positions and answers the
// index it took. The caller has found no member holding its key.
func (s *TupleSet) Add(member Tuple) int {
	if 2*(len(s.members)+1) > len(s.table) {
		s.grow()
	}
	s.members = append(s.members, member)
	idx := len(s.members) - 1
	s.place(int32(idx))
	return idx
}

// grow builds the table, at the first add from eight slots or the power of
// two at or above twice the hint, and by doubling after, gives the members
// the capacity the table admits before its next doubling, so that no add
// between doublings grows them, and places every member again.
func (s *TupleSet) grow() {
	size := 2 * len(s.table)
	if size == 0 {
		size = 8
		for size < 2*s.hint {
			size *= 2
		}
	}
	s.table = make([]int32, size)
	for i := range s.table {
		s.table[i] = -1
	}
	s.shift = uint(64 - bits.TrailingZeros(uint(size)))
	members := make([]Tuple, len(s.members), size/2)
	copy(members, s.members)
	s.members = members
	for idx := range s.members {
		s.place(int32(idx))
	}
}

// place writes a member's index into the first empty slot at or after the
// slot its key's hash names.
func (s *TupleSet) place(idx int32) {
	mask := uint64(len(s.table) - 1)
	slot := s.slot(hashTuplePositions(s.members[idx], s.storedPos))
	for s.table[slot] >= 0 {
		slot = (slot + 1) & mask
	}
	s.table[slot] = idx
}

// Len is the members held.
func (s *TupleSet) Len() int {
	return len(s.members)
}

// wholeTupleKeyWidth is the widest tuple whose whole-tuple key is a prefix
// of wholeTupleKey.
const wholeTupleKeyWidth = 64

// wholeTupleKey is the positions 0 to wholeTupleKeyWidth−1, the one slice
// every whole-tuple key within that width is a prefix of.
var wholeTupleKey = func() []int {
	positions := make([]int, wholeTupleKeyWidth)
	for i := range positions {
		positions[i] = i
	}
	return positions
}()

// allPositions is the positions 0 to width−1, the key of a whole tuple of
// that width: a prefix of wholeTupleKey, capped at the width so that an
// append cannot reach the positions past it, for a width within
// wholeTupleKeyWidth, and a slice of its own past it. A reader keying a
// whole tuple holds no symbol list, so query.SymbolIndexTable, which finds
// positions by symbol and allocates its table per call, cannot answer it.
func allPositions(width int) []int {
	if width <= wholeTupleKeyWidth {
		return wholeTupleKey[:width:width]
	}
	positions := make([]int, width)
	for i := range positions {
		positions[i] = i
	}
	return positions
}
