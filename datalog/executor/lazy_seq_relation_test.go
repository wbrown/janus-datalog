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
