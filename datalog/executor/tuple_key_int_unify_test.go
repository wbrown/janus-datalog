package executor

import "testing"

// TestTupleKey_UnifiesIntegerWidths proves the join machinery treats integer
// widths as one key: a tuple keyed by int(5) lands in the same TupleKeyMap
// bucket (equal hash) as one keyed by int64(5) and compares Equal, so a hash
// join matches them. This is the defense-in-depth behind boundary normalization
// — even a relation that somehow carries a raw int still joins against int64
// data. Different magnitudes must stay distinct.
func TestTupleKey_UnifiesIntegerWidths(t *testing.T) {
	k1 := NewTupleKeyFull(Tuple{int(5)})
	k64 := NewTupleKeyFull(Tuple{int64(5)})

	if k1.hash != k64.hash {
		t.Errorf("hash(int(5))=%d != hash(int64(5))=%d — would land in different buckets",
			k1.hash, k64.hash)
	}
	if !k1.Equal(k64) {
		t.Error("TupleKey{int(5)}.Equal(TupleKey{int64(5)}) = false, want true")
	}

	// Other widths unify too.
	for _, k := range []TupleKey{
		NewTupleKeyFull(Tuple{int8(5)}),
		NewTupleKeyFull(Tuple{int16(5)}),
		NewTupleKeyFull(Tuple{int32(5)}),
	} {
		if k.hash != k64.hash || !k.Equal(k64) {
			t.Errorf("width key (hash %d) did not unify with int64(5) (hash %d)", k.hash, k64.hash)
		}
	}

	// Different magnitude must not match.
	if NewTupleKeyFull(Tuple{int(5)}).Equal(NewTupleKeyFull(Tuple{int64(6)})) {
		t.Error("TupleKey{int(5)}.Equal(TupleKey{int64(6)}) = true, want false")
	}
}
