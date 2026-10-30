package executor

import "github.com/wbrown/janus-datalog/datalog"

// JoinIndex is the build side of a hash join: a relation's tuples held in
// groups by their values at the join key's positions, each group found by a
// tuple of the probing relation at that relation's positions of the same
// key. It is the one table, TupleSet, over each key's first tuple, with the
// key's later tuples in a group beside the member
// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md): a key of one tuple is answered
// from the members themselves, and a group is allocated at a key's second
// tuple. A key is read in place from the first tuple and never copied, so a
// held tuple is stable for the index's life: a tuple from a source that
// reuses its tuple workspace is copied once, at the boundary where it is
// retained, by the caller of Add. A probe allocates nothing. With no key
// position every tuple is one group, and every probe answers it.
type JoinIndex struct {
	keys TupleSet
	// groups holds, per member, the key's tuples from the first on when the
	// key took a second tuple, and nil for a key of one. It is nil until
	// some key takes a second tuple.
	groups [][]Tuple
	held   int
	fanout bool // some key groups more than one tuple
}

// NewJoinIndex is an empty index over the key's positions in the probing
// tuples and in the held tuples, sized at its first add for capacity tuples
// when that is known and for none when it is zero.
func NewJoinIndex(probePos, storedPos []int, capacity int) *JoinIndex {
	return &JoinIndex{keys: TupleSet{probePos: probePos, storedPos: storedPos, hint: capacity}}
}

// Add holds tuple under its key, after the key's earlier tuples, and
// reports whether the key is new to the index. The tuple is stable: a
// source reusing its workspace has had it copied already.
func (ix *JoinIndex) Add(tuple Tuple) bool {
	ix.held++
	if i, ok := ix.keys.FindAt(tuple, ix.keys.storedPos); ok {
		if ix.groups == nil {
			// Sized to the keys held, not to the members' capacity, which the
			// hint sets from the tuple count.
			ix.groups = make([][]Tuple, len(ix.keys.members))
		}
		if ix.groups[i] == nil {
			ix.groups[i] = []Tuple{ix.keys.members[i], tuple}
		} else {
			ix.groups[i] = append(ix.groups[i], tuple)
		}
		ix.fanout = true
		return false
	}
	ix.keys.Add(tuple)
	if ix.groups != nil {
		ix.groups = append(ix.groups, nil)
	}
	return true
}

// Probe returns the held tuples whose key equals tuple's, in the order they
// were added, and nil when none does. A key of one tuple is answered as a
// slice of one over the members, capped at one so an append cannot reach
// the next member.
func (ix *JoinIndex) Probe(tuple Tuple) []Tuple {
	i, ok := ix.keys.Find(tuple)
	if !ok {
		return nil
	}
	if ix.groups != nil {
		if group := ix.groups[i]; group != nil {
			return group
		}
	}
	return ix.keys.members[i : i+1 : i+1]
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
