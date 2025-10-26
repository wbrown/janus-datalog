package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestSymmetricHashJoin(t *testing.T) {
	// Test data for left relation
	leftTuples := []Tuple{
		{1, "alice", 100},
		{2, "bob", 200},
		{3, "charlie", 300},
		{4, "diana", 400},
	}
	leftColumns := []query.Symbol{"?id", "?name", "?score"}

	// Test data for right relation
	rightTuples := []Tuple{
		{"alice", "NYC", 25},
		{"bob", "LA", 30},
		{"charlie", "Chicago", 35},
		{"eve", "Miami", 28},
	}
	rightColumns := []query.Symbol{"?name", "?city", "?age"}

	t.Run("BasicJoin", func(t *testing.T) {
		// Create streaming relations
		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelation(leftColumns, leftIter)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelation(rightColumns, rightIter)

		// Perform symmetric hash join on ?name
		joinCols := []query.Symbol{"?name"}
		result := SymmetricHashJoin(leftRel, rightRel, joinCols)

		// Collect results
		var results []Tuple
		it := result.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}
		it.Close()

		// Verify results
		assert.Len(t, results, 3) // alice, bob, charlie match

		// Check output columns
		expectedCols := []query.Symbol{"?id", "?name", "?score", "?city", "?age"}
		assert.Equal(t, expectedCols, result.Columns())

		// Verify specific results (order may vary due to hash tables)
		foundAlice, foundBob, foundCharlie := false, false, false
		for _, r := range results {
			name := r[1].(string)
			switch name {
			case "alice":
				assert.Equal(t, 1, r[0])
				assert.Equal(t, 100, r[2])
				assert.Equal(t, "NYC", r[3])
				assert.Equal(t, 25, r[4])
				foundAlice = true
			case "bob":
				assert.Equal(t, 2, r[0])
				assert.Equal(t, 200, r[2])
				assert.Equal(t, "LA", r[3])
				assert.Equal(t, 30, r[4])
				foundBob = true
			case "charlie":
				assert.Equal(t, 3, r[0])
				assert.Equal(t, 300, r[2])
				assert.Equal(t, "Chicago", r[3])
				assert.Equal(t, 35, r[4])
				foundCharlie = true
			}
		}
		assert.True(t, foundAlice, "alice join result not found")
		assert.True(t, foundBob, "bob join result not found")
		assert.True(t, foundCharlie, "charlie join result not found")
	})

	t.Run("MultiColumnJoin", func(t *testing.T) {
		// Test data with multiple join columns
		leftTuples := []Tuple{
			{1, "alice", 100},
			{2, "bob", 200},
			{1, "charlie", 300},
		}
		rightTuples := []Tuple{
			{1, "alice", "NYC"},
			{2, "bob", "LA"},
			{1, "alice", "Boston"},    // Duplicate key, different city
			{3, "charlie", "Chicago"}, // Different id, won't match
		}

		leftCols := []query.Symbol{"?id", "?name", "?score"}
		rightCols := []query.Symbol{"?id", "?name", "?city"}

		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelation(leftCols, leftIter)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelation(rightCols, rightIter)

		// Join on both ?id and ?name
		joinCols := []query.Symbol{"?id", "?name"}
		result := SymmetricHashJoin(leftRel, rightRel, joinCols)

		// Collect results
		var results []Tuple
		it := result.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}
		it.Close()

		// Should match (1, alice) with both NYC and Boston entries, and (2, bob) with LA
		assert.Len(t, results, 3)

		// Verify deduplication works
		seen := make(map[string]bool)
		for _, r := range results {
			key := fmt.Sprintf("%v-%v-%v-%v", r[0], r[1], r[2], r[3])
			assert.False(t, seen[key], "Duplicate result found")
			seen[key] = true
		}
	})

	t.Run("EmptyRelations", func(t *testing.T) {
		// Test with empty left relation
		emptyIter := newMockIterator([]Tuple{})
		emptyRel := NewStreamingRelation(leftColumns, emptyIter)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelation(rightColumns, rightIter)

		joinCols := []query.Symbol{"?name"}
		result := SymmetricHashJoin(emptyRel, rightRel, joinCols)

		it := result.Iterator()
		assert.False(t, it.Next(), "Should have no results with empty left")
		it.Close()

		// Test with empty right relation
		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelation(leftColumns, leftIter)

		emptyIter2 := newMockIterator([]Tuple{})
		emptyRel2 := NewStreamingRelation(rightColumns, emptyIter2)

		result = SymmetricHashJoin(leftRel, emptyRel2, joinCols)

		it = result.Iterator()
		assert.False(t, it.Next(), "Should have no results with empty right")
		it.Close()
	})

	t.Run("NoMatchingJoinKeys", func(t *testing.T) {
		// Test where no tuples match on join key
		leftTuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
		}
		rightTuples := []Tuple{
			{3, "charlie"},
			{4, "diana"},
		}

		leftCols := []query.Symbol{"?id", "?name"}
		rightCols := []query.Symbol{"?id", "?name2"}

		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelation(leftCols, leftIter)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelation(rightCols, rightIter)

		// Join on ?id which has no matches
		joinCols := []query.Symbol{"?id"}
		result := SymmetricHashJoin(leftRel, rightRel, joinCols)

		it := result.Iterator()
		assert.False(t, it.Next(), "Should have no results when keys don't match")
		it.Close()
	})

	t.Run("DuplicateHandling", func(t *testing.T) {
		// Test that duplicates are properly handled
		leftTuples := []Tuple{
			{1, "alice"},
			{1, "alice"}, // Exact duplicate
			{2, "bob"},
		}
		rightTuples := []Tuple{
			{1, "NYC"},
			{1, "Boston"}, // Same key, different value
		}

		leftCols := []query.Symbol{"?id", "?name"}
		rightCols := []query.Symbol{"?id", "?city"}

		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelation(leftCols, leftIter)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelation(rightCols, rightIter)

		joinCols := []query.Symbol{"?id"}
		result := SymmetricHashJoin(leftRel, rightRel, joinCols)

		var results []Tuple
		it := result.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}
		it.Close()

		// Should have 2 results for id=1 (alice with NYC and Boston)
		// Duplicates should be deduplicated
		assert.Len(t, results, 2)

		// Verify deduplication
		seen := make(map[string]bool)
		for _, r := range results {
			key := fmt.Sprintf("%v-%v-%v", r[0], r[1], r[2])
			assert.False(t, seen[key], "Found duplicate result")
			seen[key] = true
		}
	})

	t.Run("StreamingBehavior", func(t *testing.T) {
		// Test that join truly streams (processes incrementally)
		// This is harder to test directly, but we can verify it returns
		// a StreamingRelation and doesn't materialize

		// Enable true streaming for this test
		opts := ExecutorOptions{EnableTrueStreaming: true}

		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelationWithOptions(leftColumns, leftIter, opts)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelationWithOptions(rightColumns, rightIter, opts)

		joinCols := []query.Symbol{"?name"}
		result := SymmetricHashJoin(leftRel, rightRel, joinCols)

		// Verify result is a StreamingRelation
		_, ok := result.(*StreamingRelation)
		assert.True(t, ok, "Result should be a StreamingRelation")

		// Verify Size() returns -1 (unknown) for streaming when EnableTrueStreaming is on
		assert.Equal(t, -1, result.Size())
	})

	t.Run("InvalidJoinColumn", func(t *testing.T) {
		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelation(leftColumns, leftIter)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelation(rightColumns, rightIter)

		// Try to join on non-existent column
		joinCols := []query.Symbol{"?nonexistent"}
		result := SymmetricHashJoin(leftRel, rightRel, joinCols)

		// Should return empty relation
		it := result.Iterator()
		assert.False(t, it.Next(), "Should have no results with invalid join column")
		it.Close()
	})
}

func TestChooseJoinStrategy(t *testing.T) {
	// Create test relations
	matTuples := []Tuple{{1, "a"}, {2, "b"}}
	matColumns := []query.Symbol{"?x", "?y"}
	matRel := NewMaterializedRelation(matColumns, matTuples)

	streamIter := newMockIterator(matTuples)
	streamRel := NewStreamingRelation(matColumns, streamIter)

	joinCols := []query.Symbol{"?x"}

	t.Run("BothMaterialized", func(t *testing.T) {
		opts := ExecutorOptions{}
		strategy := ChooseJoinStrategy(matRel, matRel, joinCols, opts)
		assert.Equal(t, "standard", strategy)
	})

	t.Run("BothStreamingWithFeatureEnabled", func(t *testing.T) {
		opts := ExecutorOptions{EnableSymmetricHashJoin: true}
		strategy := ChooseJoinStrategy(streamRel, streamRel, joinCols, opts)
		assert.Equal(t, "symmetric", strategy)
	})

	t.Run("BothStreamingWithFeatureDisabled", func(t *testing.T) {
		opts := ExecutorOptions{EnableSymmetricHashJoin: false}
		strategy := ChooseJoinStrategy(streamRel, streamRel, joinCols, opts)
		assert.Equal(t, "standard", strategy)
	})

	t.Run("MixedTypes", func(t *testing.T) {
		opts := ExecutorOptions{}
		strategy := ChooseJoinStrategy(matRel, streamRel, joinCols, opts)
		assert.Equal(t, "asymmetric", strategy)

		strategy = ChooseJoinStrategy(streamRel, matRel, joinCols, opts)
		assert.Equal(t, "asymmetric", strategy)
	})
}

// BenchmarkSymmetricHashJoin compares symmetric vs standard hash join
func BenchmarkSymmetricHashJoin(b *testing.B) {
	// Create larger test data
	size := 1000
	var leftTuples, rightTuples []Tuple
	for i := 0; i < size; i++ {
		leftTuples = append(leftTuples, Tuple{i, fmt.Sprintf("name%d", i), i * 10})
		// Only half match
		if i%2 == 0 {
			rightTuples = append(rightTuples, Tuple{fmt.Sprintf("name%d", i), fmt.Sprintf("city%d", i)})
		}
	}

	leftColumns := []query.Symbol{"?id", "?name", "?score"}
	rightColumns := []query.Symbol{"?name", "?city"}
	joinCols := []query.Symbol{"?name"}

	b.Run("SymmetricHashJoin", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			leftIter := newMockIterator(leftTuples)
			leftRel := NewStreamingRelation(leftColumns, leftIter)

			rightIter := newMockIterator(rightTuples)
			rightRel := NewStreamingRelation(rightColumns, rightIter)

			result := SymmetricHashJoin(leftRel, rightRel, joinCols)

			// Consume results
			it := result.Iterator()
			count := 0
			for it.Next() {
				count++
			}
			it.Close()
		}
	})

	b.Run("StandardHashJoin", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			leftRel := NewMaterializedRelation(leftColumns, leftTuples)
			rightRel := NewMaterializedRelation(rightColumns, rightTuples)

			result := HashJoin(leftRel, rightRel, joinCols)

			// Consume results
			it := result.Iterator()
			count := 0
			for it.Next() {
				count++
			}
			it.Close()
		}
	})
}
