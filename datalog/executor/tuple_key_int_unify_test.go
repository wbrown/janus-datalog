package executor

import "testing"

// TestTupleSet_UnifiesIntegerWidths proves the join machinery treats integer
// widths as one key: a tuple keyed by int(5) hashes as one keyed by int64(5)
// and finds the member holding it, so a hash join matches them. This is the
// defense-in-depth behind boundary normalization — even a relation that
// somehow carries a raw int still joins against int64 data. Different
// magnitudes must stay distinct.
func TestTupleSet_UnifiesIntegerWidths(t *testing.T) {
	positions := allPositions(1)
	h64 := hashTuplePositions(Tuple{int64(5)}, positions)
	s := NewTupleSet(positions, positions, 0)
	s.Add(Tuple{int64(5)}, h64)

	for _, probe := range []Tuple{{int(5)}, {int8(5)}, {int16(5)}, {int32(5)}} {
		if h := hashTuplePositions(probe, positions); h != h64 {
			t.Errorf("hash(%T(5))=%d != hash(int64(5))=%d — would reach different slots", probe[0], h, h64)
		}
		if !s.Probe(probe) {
			t.Errorf("%T(5) does not find the member holding int64(5)", probe[0])
		}
	}

	// Different magnitude must not match.
	if s.Probe(Tuple{int(6)}) {
		t.Error("int(6) found the member holding int64(5)")
	}
}
