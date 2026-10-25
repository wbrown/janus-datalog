package executor

import "github.com/wbrown/janus-datalog/datalog"

// JoinIndex is the build side of a hash join: a relation's tuples held in
// groups by their values at the join key's positions, each group found by a
// tuple of the probing relation at that relation's positions of the same
// key. A key is read in place from the first tuple of its group and never
// copied, so a held tuple is stable for the index's life: a tuple from a
// source that reuses its tuple workspace is copied once, at the boundary
// where it is retained, by the caller of Add. A probe allocates nothing.
// With no key position every tuple is one group, and every probe answers
// it.
//
// The hash is hashTuplePositions', the one every tuple key of this package
// hashes by, and the groups under one hash are told apart by their keys'
// values: each hash maps to its first group, and a group chains to the next
// distinct key under its hash.
type JoinIndex struct {
	probePos  []int // the key's positions in a probing tuple
	storedPos []int // the key's positions in a held tuple
	groups    []joinGroup
	byHash    map[uint64]int32 // each hash's first group, by index into groups
	held      int
	fanout    bool // some key groups more than one tuple
}

// joinGroup is one key's tuples, in the order they were added, chained to
// the next group under the same hash.
type joinGroup struct {
	tuples []Tuple
	next   int32 // the next group under this group's hash, -1 at the chain's end
}

// NewJoinIndex is an empty index over the key's positions in the probing
// tuples and in the held tuples, sized for capacity tuples when that is
// known and for none when it is zero.
func NewJoinIndex(probePos, storedPos []int, capacity int) *JoinIndex {
	return &JoinIndex{
		probePos:  probePos,
		storedPos: storedPos,
		groups:    make([]joinGroup, 0, capacity),
		byHash:    make(map[uint64]int32, capacity),
	}
}

// Add holds tuple under its key, after the key's earlier tuples. The tuple
// is stable: a source reusing its workspace has had it copied already.
func (ix *JoinIndex) Add(tuple Tuple) {
	ix.held++
	hash := hashTuplePositions(tuple, ix.storedPos)
	first, chained := ix.byHash[hash]
	if chained {
		for i := first; i >= 0; i = ix.groups[i].next {
			group := &ix.groups[i]
			if tupleKeysMatch(group.tuples[0], ix.storedPos, tuple, ix.storedPos) {
				group.tuples = append(group.tuples, tuple)
				ix.fanout = true
				return
			}
		}
	}
	next := int32(-1)
	if chained {
		next = first
	}
	ix.groups = append(ix.groups, joinGroup{tuples: []Tuple{tuple}, next: next})
	ix.byHash[hash] = int32(len(ix.groups) - 1)
}

// Probe returns the held tuples whose key equals tuple's, in the order they
// were added, and nil when none does.
func (ix *JoinIndex) Probe(tuple Tuple) []Tuple {
	first, chained := ix.byHash[hashTuplePositions(tuple, ix.probePos)]
	if !chained {
		return nil
	}
	for i := first; i >= 0; i = ix.groups[i].next {
		group := &ix.groups[i]
		if tupleKeysMatch(group.tuples[0], ix.storedPos, tuple, ix.probePos) {
			return group.tuples
		}
	}
	return nil
}

// KeysUnique reports whether every key groups exactly one tuple, the
// candidate-key proof a hash join checks its build side against.
func (ix *JoinIndex) KeysUnique() bool {
	return !ix.fanout
}

// Len is the tuples held.
func (ix *JoinIndex) Len() int {
	return ix.held
}

// tupleKeysMatch reports whether a held tuple's key, read at storedPos,
// equals a probing tuple's, read at probePos.
func tupleKeysMatch(stored Tuple, storedPos []int, probe Tuple, probePos []int) bool {
	for i, pos := range storedPos {
		if !datalog.ValuesEqual(stored[pos], probe[probePos[i]]) {
			return false
		}
	}
	return true
}
