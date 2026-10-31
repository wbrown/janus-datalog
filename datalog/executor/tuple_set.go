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
// the one every tuple key of this package hashes by. A find answers the hash
// it computed and an add takes it, so a key found absent and then added is
// hashed once (docs/proposals/TAGGED_SLOT_PROBE_PROOF.md, T3). The table is
// open-addressed over member indices, a power of two in length and at most
// half full, probed linearly from the slot the hash names, the top bits of
// the hash's product with the golden ratio's fraction, so keys whose hashes
// agree in their low bits, an interned pointer's address among them, spread
// over the table. Each slot carries a tag beside the index, the eight bits
// of the product below the slot's with the lowest set, and a walk reads a
// member's key only at a slot whose tag agrees with the probing key's
// (TAGGED_SLOT_PROBE_PROOF.md, T2); two members under one hash sit in two
// slots and are told apart by their keys' values. A capacity hint sizes the
// table and the members at the first add, and nothing before it.
type TupleSet struct {
	probePos  []int
	storedPos []int
	hint      int
	members   []Tuple
	tags      []uint8 // each slot the member's key's tag, or zero when empty
	table     []int32 // each slot the member's index, read where the tag agrees
	shift     uint    // 64 less the table's width in bits, the shift a slot is taken by
}

// goldenRatioFraction is the constant a hash is multiplied by before its
// slot and its tag are read from the product.
const goldenRatioFraction = 0x9E3779B97F4A7C15

// NewTupleSet is an empty set over the key's positions in the probing tuples
// and in the held tuples, sized at its first add for capacity members when
// that is above zero.
func NewTupleSet(probePos, storedPos []int, capacity int) *TupleSet {
	return &TupleSet{probePos: probePos, storedPos: storedPos, hint: capacity}
}

// Find answers the index of the member whose key equals tuple's key at the
// probing positions, the key's hash, and whether such a member exists.
func (s *TupleSet) Find(tuple Tuple) (int, uint64, bool) {
	return s.FindAt(tuple, s.probePos)
}

// FindAt is Find with the probing positions given.
func (s *TupleSet) FindAt(tuple Tuple, probePos []int) (int, uint64, bool) {
	hash := hashTuplePositions(tuple, probePos)
	if len(s.tags) == 0 {
		return 0, hash, false
	}
	mask := uint64(len(s.tags) - 1)
	mixed := hash * goldenRatioFraction
	tag := s.tag(mixed)
	for slot := s.slot(mixed); ; slot = (slot + 1) & mask {
		slotTag := s.tags[slot]
		if slotTag == 0 {
			return 0, hash, false
		}
		if slotTag != tag {
			continue
		}
		if idx := s.table[slot]; tupleKeysMatch(s.members[idx], s.storedPos, tuple, probePos) {
			return int(idx), hash, true
		}
	}
}

// slot is the table slot a key's mixed hash names, the first slot of the
// key's walk: the top bits of the mix, as many as the table is wide.
func (s *TupleSet) slot(mixed uint64) uint64 {
	return mixed >> s.shift
}

// tag is the mark a key's mixed hash leaves in its slot: the eight bits of
// the mix below the slot's, with the lowest set, so that a tag is never the
// zero an empty slot holds.
func (s *TupleSet) tag(mixed uint64) uint8 {
	return uint8(mixed>>(s.shift-8)) | 1
}

// Probe reports whether a member's key equals tuple's key at the probing
// positions.
func (s *TupleSet) Probe(tuple Tuple) bool {
	_, _, ok := s.Find(tuple)
	return ok
}

// Add holds member under its key at the stored positions and answers the
// index it took. hash is the key's, as the find that found no member holding
// it answered it, or as hashTuplePositions computes it at the stored
// positions.
func (s *TupleSet) Add(member Tuple, hash uint64) int {
	if 2*(len(s.members)+1) > len(s.tags) {
		s.grow()
	}
	s.members = append(s.members, member)
	idx := len(s.members) - 1
	s.place(int32(idx), hash)
	return idx
}

// grow builds the table, at the first add from eight slots or the power of
// two at or above twice the hint, and by doubling after, gives the members
// the capacity the table admits before its next doubling, so that no add
// between doublings grows them, and places every member again under its
// key's hash at the stored positions. The tags are made zero, the empty
// mark, so the table needs no fill.
func (s *TupleSet) grow() {
	size := 2 * len(s.tags)
	if size == 0 {
		size = 8
		for size < 2*s.hint {
			size *= 2
		}
	}
	s.tags = make([]uint8, size)
	s.table = make([]int32, size)
	s.shift = uint(64 - bits.TrailingZeros(uint(size)))
	members := make([]Tuple, len(s.members), size/2)
	copy(members, s.members)
	s.members = members
	for idx := range s.members {
		s.place(int32(idx), hashTuplePositions(s.members[idx], s.storedPos))
	}
}

// place writes a member's index and its key's tag into the first empty slot
// at or after the slot the key's hash names.
func (s *TupleSet) place(idx int32, hash uint64) {
	mask := uint64(len(s.tags) - 1)
	mixed := hash * goldenRatioFraction
	slot := s.slot(mixed)
	for s.tags[slot] != 0 {
		slot = (slot + 1) & mask
	}
	s.tags[slot] = s.tag(mixed)
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
