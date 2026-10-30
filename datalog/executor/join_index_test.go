package executor

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// JoinIndex is the build side of a hash join: a relation's tuples held in
// groups by their values at the join key's positions, each group found by a
// tuple of the probing relation at its positions of the same key. Each
// contract below is the subject of one test.

// joinIndexOver holds the tuples, keyed at position 0 on both sides.
func joinIndexOver(tuples ...Tuple) *JoinIndex {
	ix := NewJoinIndex([]int{0}, []int{0}, len(tuples))
	for _, tuple := range tuples {
		ix.Add(tuple)
	}
	return ix
}

// A probe returns the key's tuples in the order they were added, and nil
// for a key no tuple carries.
func TestJoinIndex_GroupsTuplesByTheirKeyInAddedOrder(t *testing.T) {
	e1, e2, e3 := datalog.NewIdentity("ji:e1"), datalog.NewIdentity("ji:e2"), datalog.NewIdentity("ji:e3")
	ix := joinIndexOver(
		Tuple{e1, int64(10)}, Tuple{e2, int64(20)}, Tuple{e1, int64(11)},
		Tuple{e3, int64(30)}, Tuple{e2, int64(21)}, Tuple{e1, int64(12)},
	)
	assert.Equal(t, 6, ix.Len())
	assert.Equal(t, []Tuple{{e1, int64(10)}, {e1, int64(11)}, {e1, int64(12)}}, ix.Probe(Tuple{e1}))
	assert.Equal(t, []Tuple{{e2, int64(20)}, {e2, int64(21)}}, ix.Probe(Tuple{e2}))
	assert.Equal(t, []Tuple{{e3, int64(30)}}, ix.Probe(Tuple{e3}))
	assert.Nil(t, ix.Probe(Tuple{datalog.NewIdentity("ji:absent")}))
}

// The key is read at the probing relation's positions, which need not be
// the held relation's: a held tuple carrying the key first is found by a
// probing tuple carrying it last, after symbols the index never reads, and
// a key of several positions matches only where every position agrees.
func TestJoinIndex_ProbesByTheProbingRelationsPositions(t *testing.T) {
	q1, q2, q3 := datalog.NewIdentity("ji:quire1"), datalog.NewIdentity("ji:quire2"), datalog.NewIdentity("ji:quire3")
	t.Run("one position", func(t *testing.T) {
		ix := NewJoinIndex([]int{2}, []int{0}, 0)
		ix.Add(Tuple{q1, int64(5)})
		ix.Add(Tuple{q2, int64(3)})
		assert.Equal(t, []Tuple{{q1, int64(5)}}, ix.Probe(Tuple{"first quire", "folded", q1}))
		assert.Equal(t, []Tuple{{q2, int64(3)}}, ix.Probe(Tuple{"second quire", "sewn", q2}))
		assert.Nil(t, ix.Probe(Tuple{"third quire", "pressed", q3}))
	})
	t.Run("two positions", func(t *testing.T) {
		// Held as [sheet quire], probed as [quire sheet].
		ix := NewJoinIndex([]int{0, 1}, []int{1, 0}, 0)
		ix.Add(Tuple{int64(1), q1})
		ix.Add(Tuple{int64(2), q1})
		ix.Add(Tuple{int64(1), q2})
		assert.Equal(t, []Tuple{{int64(1), q1}}, ix.Probe(Tuple{q1, int64(1)}))
		assert.Equal(t, []Tuple{{int64(1), q2}}, ix.Probe(Tuple{q2, int64(1)}))
		assert.Nil(t, ix.Probe(Tuple{q1, int64(3)}))
		assert.Nil(t, ix.Probe(Tuple{q3, int64(1)}))
	})
}

// With no key position every tuple is one group, and every probe answers
// it.
func TestJoinIndex_WithNoKeyEveryProbeAnswersEveryTuple(t *testing.T) {
	ix := NewJoinIndex(nil, nil, 0)
	ix.Add(Tuple{int64(1)})
	ix.Add(Tuple{int64(2)})
	ix.Add(Tuple{int64(3)})
	assert.Equal(t, []Tuple{{int64(1)}, {int64(2)}, {int64(3)}}, ix.Probe(Tuple{"anything"}))
	assert.Equal(t, 3, ix.Len())
	assert.False(t, ix.KeysUnique())
}

// Two distinct keys sharing one hash are told apart: each probe answers its
// own key's tuples, and a third key under the same hash misses. int64(0)
// and false are such keys under HashValue, as is the zero ElementID.
func TestJoinIndex_TwoKeysUnderOneHashAreToldApart(t *testing.T) {
	zero, no, absent := int64(0), false, datalog.ElementID{}
	require.Equal(t, datalog.HashValue(zero), datalog.HashValue(no), "the fixture's two keys share one hash")
	require.Equal(t, datalog.HashValue(zero), datalog.HashValue(absent), "the fixture's absent key shares it")
	require.False(t, datalog.ValuesEqual(zero, no), "the fixture's two keys are distinct values")
	require.False(t, datalog.ValuesEqual(zero, absent), "the fixture's absent key is distinct from the first")
	require.False(t, datalog.ValuesEqual(no, absent), "the fixture's absent key is distinct from the second")

	ix := joinIndexOver(Tuple{zero, "zero"}, Tuple{no, "no"}, Tuple{zero, "nil"})
	assert.Equal(t, []Tuple{{zero, "zero"}, {zero, "nil"}}, ix.Probe(Tuple{zero}))
	assert.Equal(t, []Tuple{{no, "no"}}, ix.Probe(Tuple{no}))
	assert.Nil(t, ix.Probe(Tuple{absent}))
	assert.False(t, ix.KeysUnique(), "zero groups two tuples")
}

// A key added again joins its group: the group grows and keeps its order,
// which is how an index kept across windows takes a later window's tuples.
func TestJoinIndex_AKeyAddedAgainJoinsItsGroup(t *testing.T) {
	e1, e2 := datalog.NewIdentity("ji:e1"), datalog.NewIdentity("ji:e2")
	ix := joinIndexOver(Tuple{e1, int64(10)}, Tuple{e2, int64(20)})
	require.Equal(t, []Tuple{{e1, int64(10)}}, ix.Probe(Tuple{e1}))
	require.True(t, ix.KeysUnique())
	ix.Add(Tuple{e1, int64(11)})
	ix.Add(Tuple{e2, int64(21)})
	ix.Add(Tuple{e1, int64(12)})
	assert.Equal(t, []Tuple{{e1, int64(10)}, {e1, int64(11)}, {e1, int64(12)}}, ix.Probe(Tuple{e1}))
	assert.Equal(t, []Tuple{{e2, int64(20)}, {e2, int64(21)}}, ix.Probe(Tuple{e2}))
	assert.Equal(t, 5, ix.Len())
	assert.False(t, ix.KeysUnique())
}

// KeysUnique holds when every key groups one tuple: the candidate-key proof
// a hash join reads to leave its result unchecked for duplicates.
func TestJoinIndex_KeysUniqueIsTheCandidateKeyProof(t *testing.T) {
	e1, e2 := datalog.NewIdentity("ji:e1"), datalog.NewIdentity("ji:e2")
	t.Run("one tuple per key", func(t *testing.T) {
		assert.True(t, joinIndexOver(Tuple{e1, int64(10)}, Tuple{e2, int64(20)}).KeysUnique())
	})
	t.Run("a key of two tuples", func(t *testing.T) {
		assert.False(t, joinIndexOver(Tuple{e1, int64(10)}, Tuple{e1, int64(11)}, Tuple{e2, int64(20)}).KeysUnique())
	})
	t.Run("empty", func(t *testing.T) {
		ix := joinIndexOver()
		assert.True(t, ix.KeysUnique())
		assert.Zero(t, ix.Len())
		assert.Nil(t, ix.Probe(Tuple{e1}))
	})
	t.Run("distinct keys under one hash, one tuple each", func(t *testing.T) {
		assert.True(t, joinIndexOver(Tuple{int64(0), "zero"}, Tuple{false, "no"}).KeysUnique())
	})
}

// A probe allocates nothing: a hit, a miss, and a hit and a miss under a
// hash two keys share.
func TestJoinIndex_ProbeAllocatesNothing(t *testing.T) {
	e1, e2 := datalog.NewIdentity("ji:e1"), datalog.NewIdentity("ji:e2")
	ix := joinIndexOver(
		Tuple{e1, int64(10)}, Tuple{e1, int64(11)}, Tuple{e2, int64(20)},
		Tuple{int64(0), "zero"}, Tuple{false, "no"},
	)
	probes := []struct {
		name  string
		tuple Tuple
	}{
		{"hit", Tuple{e1}},
		{"miss", Tuple{datalog.NewIdentity("ji:absent")}},
		{"hit under a shared hash", Tuple{false}},
		{"miss under a shared hash", Tuple{datalog.ElementID{}}},
	}
	var got []Tuple
	for _, probe := range probes {
		assert.Zero(t, testing.AllocsPerRun(1000, func() { got = ix.Probe(probe.tuple) }), probe.name)
	}
	_ = got
}

// A probe matches a key by value: an identity constructed again from its
// seed finds the tuples held under the identity constructed first.
func TestJoinIndex_MatchesAnIdentityByValue(t *testing.T) {
	ix := joinIndexOver(Tuple{datalog.NewIdentity("ji:same"), int64(10)})
	assert.Equal(t, []Tuple{{datalog.NewIdentity("ji:same"), int64(10)}}, ix.Probe(Tuple{datalog.NewIdentity("ji:same")}))
}

// A key of one tuple allocates no group: adding distinct keys allocates the
// table's growth and nothing per key, and a probe of such a key answers the
// member itself as a slice of one, capped at one so an append cannot reach
// the next member (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T3, L3).
// AllocsPerRun runs its function once unmeasured before the runs it counts,
// so the tuples outnumber the runs by one.
func TestJoinIndex_AKeyOfOneTupleAllocatesNoGroup(t *testing.T) {
	const runs = 1000
	tuples := make([]Tuple, runs+1)
	for i := range tuples {
		tuples[i] = Tuple{datalog.NewIdentity(fmt.Sprintf("ji:key%d", i)), int64(i)}
	}
	ix := NewJoinIndex([]int{0}, []int{0}, 0)
	next := 0
	perAdd := testing.AllocsPerRun(runs, func() {
		ix.Add(tuples[next])
		next++
	})
	assert.Less(t, perAdd, 0.1, "allocations per add of a new key, beyond the table's growth")
	group := ix.Probe(Tuple{tuples[0][0]})
	assert.Equal(t, []Tuple{tuples[0]}, group)
	assert.Equal(t, 1, cap(group), "a key of one tuple is answered as a slice of one, capped at one")
}

// An index given a capacity hint allocates nothing at construction beyond
// its own struct: the tags, the table and the members are built at the
// first add, sized to the hint, so a build that turns out empty costs the
// struct alone, and adds up to the hint allocate nothing beyond that first
// build (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T3, L4;
// docs/proposals/TAGGED_SLOT_PROBE_PROOF.md, T4).
func TestJoinIndex_ACapacityHintAllocatesNothingBeforeTheFirstAdd(t *testing.T) {
	const hint = 1000
	// The positions are the caller's, made once outside the measured calls.
	positions := []int{0}
	var ix *JoinIndex
	atConstruction := testing.AllocsPerRun(100, func() { ix = NewJoinIndex(positions, positions, hint) })
	assert.Equal(t, 1.0, atConstruction, "the index's struct alone")

	tuples := make([]Tuple, hint)
	for i := range tuples {
		tuples[i] = Tuple{datalog.NewIdentity(fmt.Sprintf("ji:hint%d", i))}
	}
	filled := testing.AllocsPerRun(1, func() {
		ix = NewJoinIndex(positions, positions, hint)
		for _, tuple := range tuples {
			ix.Add(tuple)
		}
	})
	assert.LessOrEqual(t, filled, 4.0, "the struct, the tags, the table and the members, for adds up to the hint")
	assert.Equal(t, hint, ix.Len())
}

// An index hinted for many tuples over few keys allocates its groups for
// the keys it holds, not for the hint: a key's second tuple, in an index
// hinted for a thousand tuples, allocates the group of two and the groups'
// entries for the one key held, under a kilobyte
// (docs/proposals/ONE_TUPLE_TABLE_PROOF.md, T3). The bytes are the
// runtime's allocation total around the one add, read after a collection.
func TestJoinIndex_GroupsAreSizedToTheKeysHeldNotTheHint(t *testing.T) {
	const hint = 1000
	positions := []int{0}
	e1 := datalog.NewIdentity("ji:grouped")
	first, second := Tuple{e1, int64(1)}, Tuple{e1, int64(2)}
	ix := NewJoinIndex(positions, positions, hint)
	ix.Add(first)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	ix.Add(second)
	runtime.ReadMemStats(&after)
	secondBytes := after.TotalAlloc - before.TotalAlloc
	assert.LessOrEqual(t, secondBytes, uint64(1024), "bytes a key's second tuple allocates in an index hinted for %d tuples", hint)
	assert.Equal(t, []Tuple{first, second}, ix.Probe(Tuple{e1}))
}
