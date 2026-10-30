package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TupleSet is a set of keys held in place in the tuples that carry them,
// each member's key read at the stored positions and probed by a tuple at
// the probing positions (docs/proposals/FRAME_RUN_RECORD_PROOF.md, T5). Each
// contract below is the subject of one test.

// tupleSetOver holds the tuples, keyed at position 0 on both sides, each
// added when a probe finds its key absent.
func tupleSetOver(tuples ...Tuple) *TupleSet {
	s := NewTupleSet([]int{0}, []int{0}, 0)
	for _, tuple := range tuples {
		if !s.Probe(tuple) {
			s.Add(tuple)
		}
	}
	return s
}

// Find answers the index of the member holding the probing tuple's key, the
// index Add answered when the member was added, and FindAt reads the probing
// tuple's key at positions given per probe, which need not be the set's own
// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T2).
func TestTupleSet_FindAnswersTheMembersIndexAtThePositionsGiven(t *testing.T) {
	e1, e2, e3 := datalog.NewIdentity("ts:e1"), datalog.NewIdentity("ts:e2"), datalog.NewIdentity("ts:e3")
	s := NewTupleSet([]int{0}, []int{1}, 0)
	first := s.Add(Tuple{"folded", e1})
	second := s.Add(Tuple{"sewn", e2})
	assert.Equal(t, 0, first)
	assert.Equal(t, 1, second)

	idx, ok := s.Find(Tuple{e2})
	assert.True(t, ok)
	assert.Equal(t, second, idx)
	idx, ok = s.FindAt(Tuple{"first quire", "pressed", e1}, []int{2})
	assert.True(t, ok, "the key read at the positions given per probe")
	assert.Equal(t, first, idx)
	_, ok = s.FindAt(Tuple{"third quire", "pressed", e3}, []int{2})
	assert.False(t, ok)
	_, ok = s.Find(Tuple{e3})
	assert.False(t, ok)
}

// The set holds one member per distinct key, probed by the probing
// relation's positions, which need not be the held relation's: a member
// carrying the key first is found by a probing tuple carrying it last, after
// symbols the set never reads, and a key of several positions matches only
// where every position agrees.
func TestTupleSet_HoldsOneMemberPerKeyProbedByThePositionsGiven(t *testing.T) {
	e1, e2, e3 := datalog.NewIdentity("ts:e1"), datalog.NewIdentity("ts:e2"), datalog.NewIdentity("ts:e3")
	s := tupleSetOver(Tuple{e1, "first"}, Tuple{e2, "second"}, Tuple{e1, "again"})
	assert.Equal(t, 2, s.Len(), "a key probed as held is not added again")
	assert.True(t, s.Probe(Tuple{e1}))
	assert.True(t, s.Probe(Tuple{e2}))
	assert.False(t, s.Probe(Tuple{e3}))

	t.Run("one position", func(t *testing.T) {
		s := NewTupleSet([]int{2}, []int{0}, 0)
		s.Add(Tuple{e1, int64(5)})
		s.Add(Tuple{e2, int64(3)})
		assert.True(t, s.Probe(Tuple{"first quire", "folded", e1}))
		assert.True(t, s.Probe(Tuple{"second quire", "sewn", e2}))
		assert.False(t, s.Probe(Tuple{"third quire", "pressed", e3}))
	})
	t.Run("two positions", func(t *testing.T) {
		// Held as [sheet quire], probed as [quire sheet].
		s := NewTupleSet([]int{0, 1}, []int{1, 0}, 0)
		s.Add(Tuple{int64(1), e1})
		s.Add(Tuple{int64(2), e1})
		s.Add(Tuple{int64(1), e2})
		assert.True(t, s.Probe(Tuple{e1, int64(1)}))
		assert.True(t, s.Probe(Tuple{e1, int64(2)}))
		assert.True(t, s.Probe(Tuple{e2, int64(1)}))
		assert.False(t, s.Probe(Tuple{e1, int64(3)}))
		assert.False(t, s.Probe(Tuple{e3, int64(1)}))
		assert.Equal(t, 3, s.Len())
	})
}

// Two distinct keys sharing one hash are told apart: each is held and
// probed as its own, and a third key under the same hash misses. int64(0)
// and false are such keys under HashValue, as is the zero ElementID.
func TestTupleSet_TwoKeysUnderOneHashAreToldApart(t *testing.T) {
	zero, no, absent := int64(0), false, datalog.ElementID{}
	require.Equal(t, datalog.HashValue(zero), datalog.HashValue(no), "the fixture's two keys share one hash")
	require.Equal(t, datalog.HashValue(zero), datalog.HashValue(absent), "the fixture's absent key shares it")
	require.False(t, datalog.ValuesEqual(zero, no), "the fixture's two keys are distinct values")
	require.False(t, datalog.ValuesEqual(zero, absent), "the fixture's absent key is distinct from the first")
	require.False(t, datalog.ValuesEqual(no, absent), "the fixture's absent key is distinct from the second")

	s := tupleSetOver(Tuple{zero, "zero"}, Tuple{no, "no"}, Tuple{zero, "nil"})
	assert.Equal(t, 2, s.Len(), "the third tuple's key is the first's")
	assert.True(t, s.Probe(Tuple{zero}))
	assert.True(t, s.Probe(Tuple{no}))
	assert.False(t, s.Probe(Tuple{absent}))
}

// A probe allocates nothing: a hit, a miss, and a hit and a miss under a
// hash two keys share.
func TestTupleSet_ProbeAllocatesNothing(t *testing.T) {
	e1, e2 := datalog.NewIdentity("ts:e1"), datalog.NewIdentity("ts:e2")
	s := tupleSetOver(Tuple{e1, "x"}, Tuple{e2, "y"}, Tuple{int64(0), "zero"}, Tuple{false, "no"})
	probes := []struct {
		name  string
		tuple Tuple
	}{
		{"hit", Tuple{e1}},
		{"miss", Tuple{datalog.NewIdentity("ts:absent")}},
		{"hit under a shared hash", Tuple{false}},
		{"miss under a shared hash", Tuple{datalog.ElementID{}}},
	}
	var held bool
	for _, probe := range probes {
		assert.Zero(t, testing.AllocsPerRun(1000, func() { held = s.Probe(probe.tuple) }), probe.name)
	}
	_ = held
}

// A member is held as given: the set holds the slice the caller added and
// never a copy of it, through every doubling of its table, so a key tuple
// the run holds is the record's own member and is stable for the set's
// life (docs/proposals/FRAME_RUN_RECORD_PROOF.md, T5).
func TestTupleSet_AMemberIsHeldAsGiven(t *testing.T) {
	first := Tuple{datalog.NewIdentity("ts:first"), "x"}
	s := NewTupleSet([]int{0}, []int{0}, 0)
	s.Add(first)
	require.Len(t, s.members, 1)
	assert.Same(t, &first[0], &s.members[0][0], "the set holds the slice it was given")
	for i := 0; i < 64; i++ {
		s.Add(Tuple{datalog.NewIdentity(fmt.Sprintf("ts:later%d", i)), "y"})
	}
	require.Len(t, s.members, 65, "the table doubled under the later members")
	assert.Same(t, &first[0], &s.members[0][0], "the first member is the slice given, after the doublings")
	assert.True(t, s.Probe(Tuple{first[0]}), "the first member's key is held")
}

// An add allocates nothing beyond the set's own growth: a thousand members
// added in turn cost the member and chain slices' doublings and the map's,
// amortized to a fraction of an allocation each, which AllocsPerRun's
// integral average reports as zero. AllocsPerRun runs its function once
// unmeasured before the runs it counts, so the members outnumber the runs
// by one.
func TestTupleSet_AddAllocatesOnlyItsGrowth(t *testing.T) {
	s := NewTupleSet([]int{0}, []int{0}, 0)
	const runs = 1000
	members := make([]Tuple, runs+1)
	for i := range members {
		members[i] = Tuple{datalog.NewIdentity(fmt.Sprintf("ts:member%d", i))}
	}
	next := 0
	perAdd := testing.AllocsPerRun(runs, func() {
		s.Add(members[next])
		next++
	})
	assert.Less(t, perAdd, 0.1, "allocations per add, beyond the set's growth")
	assert.Equal(t, len(members), s.Len())
}

// allPositions answers the positions 0 to width−1, the whole-tuple key of a
// tuple of that width: for a width within wholeTupleKeyWidth a prefix of one
// shared slice, capped at the width, so two widths' keys share one backing
// array and a key allocates nothing, and past it a slice of its own holding
// the same positions (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T4, §6).
func TestAllPositions_AnswersAWidthsOwnPositionsAsAPrefixOfOneSlice(t *testing.T) {
	for _, width := range []int{0, 1, 3, wholeTupleKeyWidth, wholeTupleKeyWidth + 1, 2 * wholeTupleKeyWidth} {
		positions := allPositions(width)
		require.Len(t, positions, width)
		for i, pos := range positions {
			assert.Equal(t, i, pos, "width %d, position %d", width, i)
		}
	}
	three, five := allPositions(3), allPositions(5)
	assert.Same(t, &three[0], &five[0], "two widths within the bound are prefixes of one slice")
	assert.Equal(t, 3, cap(three), "a prefix is capped at its width")
	var key []int
	assert.Zero(t, testing.AllocsPerRun(100, func() { key = allPositions(wholeTupleKeyWidth) }), "a key within the bound allocates nothing")
	_ = key
}

// Members whose hashes agree in every low bit, integers that are multiples
// of a power of two, sit within a bounded run of slots from their home, the
// slot the member takes alone in a table of the same size, so a probe reads
// a bounded run however the hash's low bits fall
// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, P2, T3).
func TestTupleSet_KeysAgreeingInTheirLowHashBitsSpreadOverTheTable(t *testing.T) {
	const members = 4096
	const longestRun = 64
	positions := allPositions(1)
	s := NewTupleSet(positions, positions, members)
	for i := 0; i < members; i++ {
		s.Add(Tuple{int64(i) << 32})
	}
	mask := uint64(len(s.table) - 1)
	longest := 0
	for slot, idx := range s.table {
		if idx < 0 {
			continue
		}
		alone := NewTupleSet(positions, positions, members)
		alone.Add(s.members[idx])
		require.Len(t, alone.table, len(s.table), "a set hinted alike builds a table of the same size")
		home := 0
		for alone.table[home] < 0 {
			home++
		}
		if run := int((uint64(slot) - uint64(home)) & mask); run > longest {
			longest = run
		}
	}
	assert.LessOrEqual(t, longest, longestRun, "the longest run, in slots, from a member's home over %d members whose hashes agree in their low 32 bits", members)
}
