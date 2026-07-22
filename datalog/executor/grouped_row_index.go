package executor

import (
	"github.com/wbrown/janus-datalog/datalog"
)

// groupedRowIndex holds rows grouped for per-probe-tuple lookup. All rows
// live in one shared backing, contiguous by key hash; a probe hashes the
// probe tuple's key positions, verifies the key against the span's first
// row, and returns the subslice — zero allocations. A span whose rows carry
// more than one distinct key (same-hash collision) is marked mixed and
// diverts to per-key row groups for that hash only, so correctness never
// rests on hash uniqueness.
//
// Key positions come in pairs: probePos are positions in the probing tuple,
// rowPos the corresponding positions in the stored rows. The stored rows
// carry their own key values, so no key is ever materialized separately —
// grouping, probing, and verification all read the tuples in place.
type groupedRowIndex struct {
	rows       []Tuple
	spans      map[uint64]rowSpan
	collisions map[uint64][][]Tuple
	probePos   []int
	rowPos     []int
}

// rowSpan is a contiguous region of groupedRowIndex.rows holding every row
// whose key hashes to the span's map key.
type rowSpan struct {
	start, end int32
	mixed      bool
}

// groupRows arranges collected rows into hash-contiguous spans over one
// shared backing. Counting-sort placement: one pass counts rows per key
// hash, a second places each row into its span, spans laid out in
// first-seen order. Rows within a span keep their collection order.
func groupRows(collected []Tuple, probePos, rowPos []int) *groupedRowIndex {
	offsets := make(map[uint64]int32, len(collected))
	for _, t := range collected {
		offsets[hashTuplePositions(t, rowPos)]++
	}
	rows := make([]Tuple, len(collected))
	spans := make(map[uint64]rowSpan, len(offsets))
	cursor := int32(0)
	for _, t := range collected {
		h := hashTuplePositions(t, rowPos)
		if _, ok := spans[h]; !ok {
			count := offsets[h]
			spans[h] = rowSpan{start: cursor, end: cursor + count}
			// offsets[h] becomes the span's fill position from here on.
			offsets[h] = cursor
			cursor += count
		}
		rows[offsets[h]] = t
		offsets[h]++
	}
	g := &groupedRowIndex{
		rows:     rows,
		spans:    spans,
		probePos: probePos,
		rowPos:   rowPos,
	}
	g.regroupCollidingSpans()
	return g
}

// regroupCollidingSpans finds spans whose rows carry more than one distinct
// key — distinct keys sharing a hash — marks them mixed, and builds per-key
// row groups for those hashes. Unmixed spans (the overwhelmingly common
// case) are untouched.
func (g *groupedRowIndex) regroupCollidingSpans() {
	for h, span := range g.spans {
		segment := g.rows[span.start:span.end]
		if len(segment) < 2 {
			continue
		}
		mixed := false
		for _, row := range segment[1:] {
			if !rowKeysEqual(segment[0], row, g.rowPos) {
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
		for _, row := range segment {
			placed := false
			for gi := range groups {
				if rowKeysEqual(groups[gi][0], row, g.rowPos) {
					groups[gi] = append(groups[gi], row)
					placed = true
					break
				}
			}
			if !placed {
				groups = append(groups, []Tuple{row})
			}
		}
		g.collisions[h] = groups
		span.mixed = true
		g.spans[h] = span
	}
}

// rowKeysEqual reports whether two rows carry equal values at the key
// positions.
func rowKeysEqual(a, b Tuple, rowPos []int) bool {
	for _, idx := range rowPos {
		if !datalog.ValuesEqual(a[idx], b[idx]) {
			return false
		}
	}
	return true
}

// probe returns the rows whose key positions equal the probe tuple's key
// positions, or nil. The hit path returns a subslice of the shared backing —
// zero allocations. A hash hit is not a key match: the key is verified
// against the span's first row (every row of an unmixed span carries the
// same key).
func (g *groupedRowIndex) probe(probeTuple Tuple) []Tuple {
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
	if !g.probeKeyMatches(probeTuple, g.rows[span.start]) {
		return nil
	}
	return g.rows[span.start:span.end]
}

// probeKeyMatches compares the probe tuple's key positions against a stored
// row's key positions.
func (g *groupedRowIndex) probeKeyMatches(probe, row Tuple) bool {
	for k, pi := range g.probePos {
		if !datalog.ValuesEqual(probe[pi], row[g.rowPos[k]]) {
			return false
		}
	}
	return true
}

// keysUnique reports whether every key groups exactly one row. Callers with
// a candidate-key proof over the key positions use it to check the proof
// against the grouped data.
func (g *groupedRowIndex) keysUnique() bool {
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
