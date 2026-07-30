package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

// hashValueAtDepth calls hashValue from `depth` extra stack frames down, so the
// address of hashValue's parameter differs from a shallow call. Recursion is not
// inlined, so this reliably changes the stack address.
func hashValueAtDepth(v interface{}, depth int) uint64 {
	if depth <= 0 {
		return hashValue(v)
	}
	return hashValueAtDepth(v, depth-1)
}

// TestHashValue_TimeIsDeterministic is a regression test for the time.Time gap in
// hashValue. Like the []byte case above it, a time.Time value must hash by its
// content, not by the address-based default — otherwise equal times hash to
// different buckets in a TupleKeyMap and never match, making joins and dedup on
// time-valued tuples nondeterministic across platforms.
func TestHashValue_TimeIsDeterministic(t *testing.T) {
	tm := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	shallow := hashValue(tm)
	deep := hashValueAtDepth(tm, 32)
	if shallow != deep {
		t.Fatalf("hashValue(time.Time) is not deterministic across call sites: %d vs %d "+
			"(time.Time falls through to the pointer-address default)", shallow, deep)
	}
}

// TestTupleKeyMap_TimeValuedTuplesRoundTrip ensures a TupleKeyMap keyed on a
// tuple containing a time.Time can find that tuple again when the lookup key is
// built from a separately-constructed equal time at a different stack depth —
// the pattern a hash join exercises (build vs probe). With the bug the lookup
// misses and the tuple is silently dropped from the join.
func TestTupleKeyMap_TimeValuedTuplesRoundTrip(t *testing.T) {
	id := datalog.NewIdentity("e1")
	tm := time.Date(2026, 5, 24, 9, 30, 0, 0, time.UTC)

	m := NewTupleKeyMap()
	m.Put(NewTupleKeyFull(Tuple{id, tm}), "v")

	// Build the lookup key from an equal but separately-constructed time, hashed
	// from a different stack depth (mirroring build-side vs probe-side hashing).
	lookupTime := time.Date(2026, 5, 24, 9, 30, 0, 0, time.UTC)
	key := tupleKeyAtDepth(Tuple{id, lookupTime}, 32)
	if _, ok := m.Get(key); !ok {
		t.Fatalf("TupleKeyMap lost a time-valued tuple: Get missed an equal key " +
			"(time.Time hashed nondeterministically)")
	}
}

func tupleKeyAtDepth(tuple Tuple, depth int) TupleKey {
	if depth <= 0 {
		return NewTupleKeyFull(tuple)
	}
	return tupleKeyAtDepth(tuple, depth-1)
}
