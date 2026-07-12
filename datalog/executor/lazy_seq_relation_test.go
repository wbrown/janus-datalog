package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestLazySeqRelation_Symbols verifies that Symbols() returns the symbols
// the relation was constructed with.
func TestLazySeqRelation_Symbols(t *testing.T) {
	syms := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}
	seq := makeTupleSeq([]Tuple{{1, "a"}, {2, "b"}})
	rel := NewLazySeqRelation(seq, syms)

	assert.Equal(t, syms, rel.Symbols())
}

// TestLazySeqRelation_Iterator verifies that Iterator() walks the LazySeq
// producing tuples in order.
func TestLazySeqRelation_Iterator(t *testing.T) {
	tuples := []Tuple{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}}
	syms := []query.Symbol{datalog.NewSymbol("?i"), datalog.NewSymbol("?s")}
	seq := makeTupleSeq(tuples)
	rel := NewLazySeqRelation(seq, syms)

	var collected []Tuple
	it := rel.Iterator()
	for it.Next() {
		cp := make(Tuple, len(it.Tuple()))
		copy(cp, it.Tuple())
		collected = append(collected, cp)
	}
	it.Close()

	require.Len(t, collected, 5)
	for i, tup := range collected {
		assert.Equal(t, tuples[i][0], tup[0])
		assert.Equal(t, tuples[i][1], tup[1])
	}
}

// TestLazySeqRelation_MultipleIterators verifies that two Iterator() calls
// on the same LazySeqRelation produce independent cursors that both see
// all tuples. The second iterator reads from cached cells.
func TestLazySeqRelation_MultipleIterators(t *testing.T) {
	tuples := []Tuple{{1}, {2}, {3}, {4}, {5}}
	seq := makeTupleSeq(tuples)
	rel := NewLazySeqRelation(seq, []query.Symbol{datalog.NewSymbol("?x")})

	// First iterator consumes all
	var first []Tuple
	it1 := rel.Iterator()
	for it1.Next() {
		cp := make(Tuple, len(it1.Tuple()))
		copy(cp, it1.Tuple())
		first = append(first, cp)
	}
	it1.Close()
	require.Len(t, first, 5)

	// Second iterator reads from same seq (cached cells)
	var second []Tuple
	it2 := rel.Iterator()
	for it2.Next() {
		cp := make(Tuple, len(it2.Tuple()))
		copy(cp, it2.Tuple())
		second = append(second, cp)
	}
	it2.Close()
	require.Len(t, second, 5)

	for i := range first {
		assert.Equal(t, first[i][0], second[i][0])
	}
}

// TestLazySeqRelation_SymbolRemapping verifies that a LazySeqRelation
// with remapped symbols returns the new symbols but unchanged tuples.
func TestLazySeqRelation_SymbolRemapping(t *testing.T) {
	tuples := []Tuple{{"entity:1", "value:1"}, {"entity:2", "value:2"}}
	origSyms := []query.Symbol{datalog.NewSymbol("?t"), datalog.NewSymbol("?s")}
	seq := makeTupleSeq(tuples)
	rel := NewLazySeqRelation(seq, origSyms)

	// Remap symbols
	newSyms := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	remapped := NewLazySeqRelation(seq, newSyms)

	assert.Equal(t, origSyms, rel.Symbols())
	assert.Equal(t, newSyms, remapped.Symbols())

	// Both should produce same tuples
	it := remapped.Iterator()
	require.True(t, it.Next())
	assert.Equal(t, "entity:1", it.Tuple()[0])
	assert.Equal(t, "value:1", it.Tuple()[1])
	it.Close()
}

// TestLazySeqRelation_Materialize verifies that Materialize() realizes
// the entire seq and returns a relation that can be iterated.
func TestLazySeqRelation_Materialize(t *testing.T) {
	tuples := []Tuple{{10}, {20}, {30}}
	seq := makeTupleSeq(tuples)
	rel := NewLazySeqRelation(seq, []query.Symbol{datalog.NewSymbol("?x")})

	mat := rel.Materialize()
	require.NotNil(t, mat)

	var collected []Tuple
	it := mat.Iterator()
	for it.Next() {
		cp := make(Tuple, len(it.Tuple()))
		copy(cp, it.Tuple())
		collected = append(collected, cp)
	}
	it.Close()

	require.Len(t, collected, 3)
	assert.Equal(t, 10, collected[0][0])
	assert.Equal(t, 20, collected[1][0])
	assert.Equal(t, 30, collected[2][0])
}

// makeTupleSeq creates a LazySeq from a slice of tuples for testing.
func makeTupleSeq(tuples []Tuple) *LazySeq {
	it := &sliceTupleIterator{tuples: tuples}
	return NewTupleSeq(it, false)
}

// sliceTupleIterator is a simple iterator over a tuple slice for testing.
type sliceTupleIterator struct {
	tuples []Tuple
	pos    int
	err    error
}

func (it *sliceTupleIterator) Next() bool {
	if it.pos >= len(it.tuples) {
		return false
	}
	it.pos++
	return true
}

func (it *sliceTupleIterator) Tuple() Tuple {
	return it.tuples[it.pos-1]
}

func (it *sliceTupleIterator) Close() error {
	return nil
}

func (it *sliceTupleIterator) Error() error { return it.err }

// TestLazySeqRelation_MultipleIteratorsSafe verifies that calling Iterator()
// multiple times on a LazySeqRelation produces independent cursors that all
// see the same data, and the underlying iterator advances only once.
func TestLazySeqRelation_MultipleIteratorsSafe(t *testing.T) {
	callCount := 0
	tuples := make([]Tuple, 10)
	for i := range tuples {
		tuples[i] = Tuple{i, "val"}
	}

	// Wrap in a counting iterator to track how many Next() calls reach storage
	countingIt := &countingSliceIterator{tuples: tuples, nextCalls: &callCount}
	seq := NewTupleSeq(countingIt, false)
	syms := []query.Symbol{datalog.NewSymbol("?i"), datalog.NewSymbol("?v")}
	rel := NewLazySeqRelation(seq, syms)

	// Create three independent cursors
	var results [3][]Tuple
	for c := 0; c < 3; c++ {
		it := rel.Iterator()
		for it.Next() {
			results[c] = append(results[c], it.Tuple())
		}
		it.Close()
	}

	// All three cursors see the same 10 tuples
	for c := 0; c < 3; c++ {
		require.Len(t, results[c], 10, "cursor %d should see 10 tuples", c)
		for i, tuple := range results[c] {
			assert.Equal(t, i, tuple[0], "cursor %d tuple %d", c, i)
		}
	}

	// The underlying iterator advanced exactly 10 steps (not 30)
	assert.Equal(t, 10, callCount, "underlying iterator should advance only 10 times total")
}

// TestLazySeqRelation_FromStreamingRelation verifies the specific wrapping
// pattern: StreamingRelation → NewTupleSeq → LazySeqRelation. Two Iterator()
// calls on the result both produce identical data with no panic.
func TestLazySeqRelation_FromStreamingRelation(t *testing.T) {
	tuples := []Tuple{{1, "a"}, {2, "b"}, {3, "c"}}
	syms := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

	// Create a StreamingRelation (single-use)
	baseIt := &sliceTupleIterator{tuples: tuples}
	sr := NewStreamingRelation(syms, baseIt)

	// Wrap: consume the streaming iterator once via NewTupleSeq, then
	// create a LazySeqRelation that supports multiple cursors.
	seq := NewTupleSeq(sr.Iterator(), sr.RequiresCopy())
	lazyRel := NewLazySeqRelation(seq, sr.Symbols())

	// First cursor
	var results1 []Tuple
	it1 := lazyRel.Iterator()
	for it1.Next() {
		results1 = append(results1, it1.Tuple())
	}
	it1.Close()

	// Second cursor — must not panic
	var results2 []Tuple
	it2 := lazyRel.Iterator()
	for it2.Next() {
		results2 = append(results2, it2.Tuple())
	}
	it2.Close()

	require.Len(t, results1, 3)
	require.Len(t, results2, 3)
	for i := range results1 {
		assert.Equal(t, results1[i][0], results2[i][0])
		assert.Equal(t, results1[i][1], results2[i][1])
	}
}

func TestLazySeqRelationMaterializeReplaysSourceError(t *testing.T) {
	source := newFailingRelation(1, Tuple{int64(1)}, Tuple{int64(2)})
	relation := NewLazySeqRelation(
		NewTupleSeq(source.Iterator(), false),
		testSymbols(),
	)

	materialized := relation.Materialize()
	require.ErrorIs(t, driveErr(materialized), errInjectedIterator)
}

// countingSliceIterator tracks how many times Next() is called.
type countingSliceIterator struct {
	tuples    []Tuple
	pos       int
	nextCalls *int
	err       error
}

func (it *countingSliceIterator) Next() bool {
	if it.pos >= len(it.tuples) {
		return false
	}
	*it.nextCalls++
	it.pos++
	return true
}

func (it *countingSliceIterator) Tuple() Tuple {
	return it.tuples[it.pos-1]
}

func (it *countingSliceIterator) Close() error { return nil }
func (it *countingSliceIterator) Error() error { return it.err }
