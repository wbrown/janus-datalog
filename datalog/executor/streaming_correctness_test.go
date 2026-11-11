package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestStreamingVsMaterializedCorrectness verifies that streaming and materialized
// execution produce identical results
func TestStreamingVsMaterializedCorrectness(t *testing.T) {
	t.Run("FilterAndProject", func(t *testing.T) {
		// Create test data
		size := 10000
		var tuples []Tuple
		for i := 0; i < size; i++ {
			// ?y cycles so the 1% filter below selects every 100th tuple —
			// scattered matches interleave pass and skip in the filter iterator.
			tuples = append(tuples, Tuple{i, i % 100, i * 3})
		}
		symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}

		// Materialized side: the same pipeline through MaterializedRelation's
		// own filter and project implementation.
		matRel := NewMaterializedRelation(symbols, tuples)

		// Filter to 1% (every 100th tuple)
		matFiltered := matRel.FilterWithPredicate(&query.Comparison{
			Op:    datalog.SymEQ,
			Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
			Right: query.ConstantTerm{Value: int64(0)},
		})

		// Project
		matProjected, err := matFiltered.Project([]query.Symbol{datalog.NewSymbol("?x")})
		assert.NoError(t, err)

		// Collect results
		var matResults []Tuple
		it := matProjected.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			matResults = append(matResults, tupleCopy)
		}
		it.Close()

		// Streaming side: the same pipeline through a StreamingRelation.
		streamSource := &sliceIterator{tuples: tuples, pos: -1}
		streamRel := NewStreamingRelation(symbols, streamSource)

		// Filter to 1% (every 100th tuple)
		streamFiltered := streamRel.FilterWithPredicate(&query.Comparison{
			Op:    datalog.SymEQ,
			Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
			Right: query.ConstantTerm{Value: int64(0)},
		})

		// Project
		streamProjected, err := streamFiltered.Project([]query.Symbol{datalog.NewSymbol("?x")})
		assert.NoError(t, err)

		// Collect results
		var streamResults []Tuple
		it2 := streamProjected.Iterator()
		for it2.Next() {
			tuple := it2.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			streamResults = append(streamResults, tupleCopy)
		}
		it2.Close()

		// Verify results match
		assert.Equal(t, len(matResults), len(streamResults), "Result counts should match")
		assert.Equal(t, 100, len(matResults), "Should have exactly 100 results (1% of 10000)")

		for i := range matResults {
			assert.Equal(t, matResults[i], streamResults[i], "Tuple %d should match", i)
		}
	})

	t.Run("ComplexPipeline", func(t *testing.T) {
		// Create test data - two relations for join
		leftTuples := []Tuple{
			{1, "alice", 100},
			{2, "bob", 200},
			{3, "charlie", 300},
		}
		leftSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?score")}

		rightTuples := []Tuple{
			{"alice", "NYC"},
			{"bob", "LA"},
			{"charlie", "Chicago"},
		}
		rightSymbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?city")}

		// Materialized side: the join of two MaterializedRelations.
		matLeft := NewMaterializedRelation(leftSymbols, leftTuples)
		matRight := NewMaterializedRelation(rightSymbols, rightTuples)

		matJoined := HashJoin(matLeft, matRight, []query.Symbol{datalog.NewSymbol("?name")})
		matProjected, err := matJoined.Project([]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?score"), datalog.NewSymbol("?city")})
		assert.NoError(t, err)

		var matResults []Tuple
		it := matProjected.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			matResults = append(matResults, tupleCopy)
		}
		it.Close()

		// Streaming side: the join of two StreamingRelations.
		streamLeft := NewStreamingRelation(leftSymbols, &sliceIterator{tuples: leftTuples, pos: -1})
		streamRight := NewStreamingRelation(rightSymbols, &sliceIterator{tuples: rightTuples, pos: -1})

		streamJoined := HashJoin(streamLeft, streamRight, []query.Symbol{datalog.NewSymbol("?name")})
		streamProjected, err := streamJoined.Project([]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?score"), datalog.NewSymbol("?city")})
		assert.NoError(t, err)

		var streamResults []Tuple
		it2 := streamProjected.Iterator()
		for it2.Next() {
			tuple := it2.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			streamResults = append(streamResults, tupleCopy)
		}
		it2.Close()

		// Verify results match
		assert.Equal(t, len(matResults), len(streamResults), "Result counts should match")
		assert.Equal(t, 3, len(matResults), "Should have exactly 3 results (all join)")

		// Sort both for comparison (join order may vary)
		sortTuples := func(tuples []Tuple) {
			// Simple sort by first symbol (name)
			for i := 0; i < len(tuples); i++ {
				for j := i + 1; j < len(tuples); j++ {
					if tuples[i][0].(string) > tuples[j][0].(string) {
						tuples[i], tuples[j] = tuples[j], tuples[i]
					}
				}
			}
		}
		sortTuples(matResults)
		sortTuples(streamResults)

		for i := range matResults {
			assert.Equal(t, matResults[i], streamResults[i], "Tuple %d should match", i)
		}
	})
}
