package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// groupedTupleIndex holds tuples grouped for per-probe-tuple lookup. All
// tuples live in one shared backing, contiguous by key hash; a probe hashes
// the probe tuple's key positions, verifies the key against the span's first
// tuple, and returns the subslice — zero allocations. A span whose tuples
// carry more than one distinct key (same-hash collision) is marked mixed and
// diverts to per-key tuple groups for that hash only, so correctness never
// rests on hash uniqueness.
//
// Key positions come in pairs: probePos are positions in the probing tuple,
// storedPos the corresponding positions in the stored tuples. The stored
// tuples carry their own key values, so no key is ever materialized separately
// — grouping, probing, and verification all read the tuples in place.
type groupedTupleIndex struct {
	tuples     []Tuple
	spans      map[uint64]tupleSpan
	collisions map[uint64][][]Tuple
	probePos   []int
	storedPos  []int
}

// tupleSpan is a contiguous region of groupedTupleIndex.tuples holding every
// tuple whose key hashes to the span's map key.
type tupleSpan struct {
	start, end int32
	mixed      bool
}

// groupTuples arranges collected tuples into hash-contiguous spans over one
// shared backing. Counting-sort placement: one pass counts tuples per key
// hash, a second places each tuple into its span, spans laid out in
// first-seen order. Tuples within a span keep their collection order.
func groupTuples(collected []Tuple, probePos, storedPos []int) *groupedTupleIndex {
	offsets := make(map[uint64]int32, len(collected))
	for _, t := range collected {
		offsets[hashTuplePositions(t, storedPos)]++
	}
	tuples := make([]Tuple, len(collected))
	spans := make(map[uint64]tupleSpan, len(offsets))
	cursor := int32(0)
	for _, t := range collected {
		h := hashTuplePositions(t, storedPos)
		if _, ok := spans[h]; !ok {
			count := offsets[h]
			spans[h] = tupleSpan{start: cursor, end: cursor + count}
			// offsets[h] becomes the span's fill position from here on.
			offsets[h] = cursor
			cursor += count
		}
		tuples[offsets[h]] = t
		offsets[h]++
	}
	g := &groupedTupleIndex{
		tuples:    tuples,
		spans:     spans,
		probePos:  probePos,
		storedPos: storedPos,
	}
	g.regroupCollidingSpans()
	return g
}

// regroupCollidingSpans finds spans whose tuples carry more than one distinct
// key — distinct keys sharing a hash — marks them mixed, and builds per-key
// tuple groups for those hashes. Unmixed spans (the overwhelmingly common
// case) are untouched.
func (g *groupedTupleIndex) regroupCollidingSpans() {
	for h, span := range g.spans {
		segment := g.tuples[span.start:span.end]
		if len(segment) < 2 {
			continue
		}
		mixed := false
		for _, tuple := range segment[1:] {
			if !tupleKeysEqual(segment[0], tuple, g.storedPos) {
				mixed = true
				break
			}
		}
		if !mixed {
			continue
		}
		if g.collisions == nil {
			g.collisions = make(map[uint64][][]Tuple)
		}
		var groups [][]Tuple
		for _, tuple := range segment {
			placed := false
			for gi := range groups {
				if tupleKeysEqual(groups[gi][0], tuple, g.storedPos) {
					groups[gi] = append(groups[gi], tuple)
					placed = true
					break
				}
			}
			if !placed {
				groups = append(groups, []Tuple{tuple})
			}
		}
		g.collisions[h] = groups
		span.mixed = true
		g.spans[h] = span
	}
}

// tupleKeysEqual reports whether two tuples carry equal values at the key
// positions.
func tupleKeysEqual(a, b Tuple, storedPos []int) bool {
	for _, idx := range storedPos {
		if !datalog.ValuesEqual(a[idx], b[idx]) {
			return false
		}
	}
	return true
}

// probe returns the tuples whose key positions equal the probe tuple's key
// positions, or nil. The hit path returns a subslice of the shared backing —
// zero allocations. A hash hit is not a key match: the key is verified
// against the span's first tuple (every tuple of an unmixed span carries the
// same key).
func (g *groupedTupleIndex) probe(probeTuple Tuple) []Tuple {
	h := hashTuplePositions(probeTuple, g.probePos)
	span, ok := g.spans[h]
	if !ok {
		return nil
	}
	if span.mixed {
		for _, group := range g.collisions[h] {
			if g.probeKeyMatches(probeTuple, group[0]) {
				return group
			}
		}
		return nil
	}
	if !g.probeKeyMatches(probeTuple, g.tuples[span.start]) {
		return nil
	}
	return g.tuples[span.start:span.end]
}

// probeKeyMatches compares the probe tuple's key positions against a stored
// tuple's key positions.
func (g *groupedTupleIndex) probeKeyMatches(probe, stored Tuple) bool {
	for k, pi := range g.probePos {
		if !datalog.ValuesEqual(probe[pi], stored[g.storedPos[k]]) {
			return false
		}
	}
	return true
}

// keysUnique reports whether every key groups exactly one tuple. Callers with
// a candidate-key proof over the key positions use it to check the proof
// against the grouped data.
func (g *groupedTupleIndex) keysUnique() bool {
	for h, span := range g.spans {
		if span.mixed {
			for _, group := range g.collisions[h] {
				if len(group) > 1 {
					return false
				}
			}
			continue
		}
		if span.end-span.start > 1 {
			return false
		}
	}
	return true
}
