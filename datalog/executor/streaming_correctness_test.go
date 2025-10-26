package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestStreamingVsMaterializedCorrectness verifies that streaming and materialized
// execution produce identical results
func TestStreamingVsMaterializedCorrectness(t *testing.T) {
	t.Run("FilterAndProject", func(t *testing.T) {
		// Create test data
		size := 10000
		var tuples []Tuple
		for i := 0; i < size; i++ {
			tuples = append(tuples, Tuple{i, i * 2, i * 3})
		}
		columns := []query.Symbol{"?x", "?y", "?z"}

		// Test with materialized (EnableTrueStreaming=false)
		matOpts := ExecutorOptions{
			EnableIteratorComposition: false,
			EnableTrueStreaming:       false,
		}

		matSource := newMockIterator(tuples)
		matRel := NewStreamingRelationWithOptions(columns, matSource, matOpts)

		// Filter to 1%
		matFiltered := matRel.Filter(NewSimpleFilter(func(t Tuple) bool {
			return t[0].(int)%100 == 0
		}))

		// Project
		matProjected, err := matFiltered.Project([]query.Symbol{"?x"})
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

		// Test with streaming (EnableTrueStreaming=true)
		streamOpts := ExecutorOptions{
			EnableIteratorComposition: true,
			EnableTrueStreaming:       true,
		}

		streamSource := newMockIterator(tuples)
		streamRel := NewStreamingRelationWithOptions(columns, streamSource, streamOpts)

		// Filter to 1%
		streamFiltered := streamRel.Filter(NewSimpleFilter(func(t Tuple) bool {
			return t[0].(int)%100 == 0
		}))

		// Project
		streamProjected, err := streamFiltered.Project([]query.Symbol{"?x"})
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
		leftColumns := []query.Symbol{"?id", "?name", "?score"}

		rightTuples := []Tuple{
			{"alice", "NYC"},
			{"bob", "LA"},
			{"charlie", "Chicago"},
		}
		rightColumns := []query.Symbol{"?name", "?city"}

		// Test with materialized
		matOpts := ExecutorOptions{
			EnableIteratorComposition: false,
			EnableTrueStreaming:       false,
		}

		matLeft := NewStreamingRelationWithOptions(leftColumns, newMockIterator(leftTuples), matOpts)
		matRight := NewStreamingRelationWithOptions(rightColumns, newMockIterator(rightTuples), matOpts)

		matJoined := HashJoinWithOptions(matLeft, matRight, []query.Symbol{"?name"}, matOpts)
		matProjected, err := matJoined.Project([]query.Symbol{"?name", "?score", "?city"})
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

		// Test with streaming
		streamOpts := ExecutorOptions{
			EnableIteratorComposition: true,
			EnableTrueStreaming:       true,
		}

		streamLeft := NewStreamingRelationWithOptions(leftColumns, newMockIterator(leftTuples), streamOpts)
		streamRight := NewStreamingRelationWithOptions(rightColumns, newMockIterator(rightTuples), streamOpts)

		streamJoined := HashJoinWithOptions(streamLeft, streamRight, []query.Symbol{"?name"}, streamOpts)
		streamProjected, err := streamJoined.Project([]query.Symbol{"?name", "?score", "?city"})
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
			// Simple sort by first column (name)
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
