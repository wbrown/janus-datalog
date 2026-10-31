package executor

import "sort"

// ordinalIndex is the index a derived set keeps at one key, the positions of
// its tuples the key reads
// (docs/proposals/A_HELD_RELATION_KEEPS_ITS_JOIN_INDEX_PROOF.md): the one
// table, TupleSet, over the first tuple of each distinct key, with, per
// member, the ordinals of the set's tuples holding that key, ascending. A
// tuple enters once, at the extension past its ordinal, so the index holds
// the set's first held tuples (T4). A probe by a tuple of another relation,
// at that relation's positions of the key and bounded at a view's count,
// answers the ordinals under the tuple's key below the count, ascending,
// nil when none: the view's tuples holding the key, and none the set
// admitted after the view (T2). A probe allocates nothing.
type ordinalIndex struct {
	positions []int
	keys      TupleSet
	// first is each member's own ordinal, and groups holds, per member, the
	// key's ordinals from the first on when the key took a second tuple, and
	// nil for a key of one; groups is nil until some key takes a second
	// tuple, the layout of JoinIndex.
	first  []int32
	groups [][]int32
	held   int  // the set's tuples that have entered: its first held
	fanout bool // some key holds more than one tuple
}

// newOrdinalIndex is an empty index at the key positions, the set's
// positions the key reads.
func newOrdinalIndex(positions []int) *ordinalIndex {
	return &ordinalIndex{positions: positions, keys: TupleSet{probePos: positions, storedPos: positions}}
}

// extend enters the tuples past the ones held, tuples being the set's from
// its first: each under its key at the index's positions, its ordinal after
// its key's earlier ordinals.
func (ix *ordinalIndex) extend(tuples []Tuple) {
	for ; ix.held < len(tuples); ix.held++ {
		ordinal := int32(ix.held)
		tuple := tuples[ix.held]
		i, hash, ok := ix.keys.FindAt(tuple, ix.positions)
		if ok {
			if ix.groups == nil {
				ix.groups = make([][]int32, len(ix.keys.members))
			}
			if ix.groups[i] == nil {
				ix.groups[i] = []int32{ix.first[i], ordinal}
			} else {
				ix.groups[i] = append(ix.groups[i], ordinal)
			}
			ix.fanout = true
			continue
		}
		ix.keys.Add(tuple, hash)
		ix.first = append(ix.first, ordinal)
		if ix.groups != nil {
			ix.groups = append(ix.groups, nil)
		}
	}
}

// probe answers the ordinals of the held tuples whose key equals tuple's key
// at probePos and lie below bound, ascending, and nil when none does. A key
// of one ordinal is answered as a slice of one over the members' ordinals,
// capped at one so an append cannot reach the next member's.
func (ix *ordinalIndex) probe(tuple Tuple, probePos []int, bound int) []int32 {
	i, _, ok := ix.keys.FindAt(tuple, probePos)
	if !ok {
		return nil
	}
	var ordinals []int32
	if ix.groups != nil && ix.groups[i] != nil {
		ordinals = ix.groups[i]
	} else {
		ordinals = ix.first[i : i+1 : i+1]
	}
	// Every held ordinal lies below the bound when the index holds no more
	// than the bound, and the key's run is ascending, so its last ordinal
	// decides whether any lies at or past it.
	if ix.held <= bound || int(ordinals[len(ordinals)-1]) < bound {
		return ordinals
	}
	n := sort.Search(len(ordinals), func(k int) bool { return int(ordinals[k]) >= bound })
	if n == 0 {
		return nil
	}
	return ordinals[:n:n]
}
