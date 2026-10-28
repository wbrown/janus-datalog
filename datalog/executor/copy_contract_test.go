package executor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Whether a tuple must be copied is the iterator's fact: its Tuple() hands
// back either a slice the next Next() overwrites or a slice the caller may
// keep. A streaming relation answers from the iterator it hands out. So a
// retaining reader over an iterator handing out stable tuples keeps the
// iterator's own slices and allocates nothing per tuple, a reader over a
// workspace-reusing iterator copies, and a relation whose cache holds the
// tuples answers that they are its own.
func TestAStreamingRelationAnswersTheCopyContractFromItsIterator(t *testing.T) {
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	stable := []Tuple{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}
	reused := [][]interface{}{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}

	t.Run("an iterator handing out stable tuples", func(t *testing.T) {
		rel := NewStreamingRelation(symbols, newMockIterator(stable))
		require.False(t, rel.RequiresCopy(), "the iterator's tuples are the caller's to keep")
		var collected []Tuple
		require.NoError(t, collectTuplesInto(&collected, rel))
		require.Len(t, collected, len(stable))
		for i := range collected {
			require.True(t, &collected[i][0] == &stable[i][0], "tuple %d is kept as the iterator handed it out", i)
		}
	})

	t.Run("a retaining reader allocates nothing per tuple over stable tuples", func(t *testing.T) {
		allocsOver := func(tuples []Tuple) float64 {
			return testing.AllocsPerRun(50, func() {
				rel := NewStreamingRelation(symbols, newMockIterator(tuples))
				collected := make([]Tuple, 0, len(tuples))
				_ = collectTuplesInto(&collected, rel)
			})
		}
		many := make([]Tuple, 0, 300)
		for i := 0; i < 300; i++ {
			many = append(many, Tuple{int64(i), "v"})
		}
		require.Equal(t, allocsOver(stable), allocsOver(many), "the allocations do not grow with the tuple count")
	})

	t.Run("an iterator reusing its workspace", func(t *testing.T) {
		rel := NewStreamingRelation(symbols, newWorkspaceReusingIterator(len(symbols), reused))
		require.True(t, rel.RequiresCopy(), "the iterator's next pull overwrites the tuple")
		var collected []Tuple
		require.NoError(t, collectTuplesInto(&collected, rel))
		require.Equal(t, stable, collected, "each kept tuple holds its own values")
		require.True(t, &collected[0][0] != &collected[1][0], "the kept tuples are copies")
	})

	t.Run("a relation whose cache holds the tuples", func(t *testing.T) {
		rel := NewStreamingRelation(symbols, newWorkspaceReusingIterator(len(symbols), reused))
		rel.Materialize()
		require.False(t, rel.RequiresCopy(), "the cache's tuples are the relation's own")
		var first, second []Tuple
		require.NoError(t, collectTuplesInto(&first, rel))
		require.NoError(t, collectTuplesInto(&second, rel))
		require.Equal(t, stable, first)
		require.Equal(t, stable, second)
		for i := range first {
			require.True(t, &first[i][0] == &second[i][0], "tuple %d is the cache's own on every read", i)
		}
	})
}
