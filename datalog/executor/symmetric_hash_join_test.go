package executor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestSymmetricHashJoin(t *testing.T) {
	// Test data for left relation
	leftTuples := []Tuple{
		{1, "alice", 100},
		{2, "bob", 200},
		{3, "charlie", 300},
		{4, "diana", 400},
	}
	leftSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?score")}

	// Test data for right relation
	rightTuples := []Tuple{
		{"alice", "NYC", 25},
		{"bob", "LA", 30},
		{"charlie", "Chicago", 35},
		{"eve", "Miami", 28},
	}
	rightSymbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?city"), datalog.NewSymbol("?age")}

	t.Run("BasicJoin", func(t *testing.T) {
		// Create streaming relations
		leftIter := &sliceIterator{tuples: leftTuples, pos: -1}
		leftRel := NewStreamingRelation(leftSymbols, leftIter)

		rightIter := &sliceIterator{tuples: rightTuples, pos: -1}
		rightRel := NewStreamingRelation(rightSymbols, rightIter)

		// Perform symmetric hash join on ?name
		joinSymbols := []query.Symbol{datalog.NewSymbol("?name")}
		result := SymmetricHashJoin(leftRel, rightRel, joinSymbols)

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

		// Check output symbols
		expectedSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?score"), datalog.NewSymbol("?city"), datalog.NewSymbol("?age")}
		assert.Equal(t, expectedSymbols, result.Symbols())

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

	t.Run("MultiSymbolJoin", func(t *testing.T) {
		// Test data with multiple join symbols
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

		leftSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?score")}
		rightSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?city")}

		leftIter := &sliceIterator{tuples: leftTuples, pos: -1}
		leftRel := NewStreamingRelation(leftSymbols, leftIter)

		rightIter := &sliceIterator{tuples: rightTuples, pos: -1}
		rightRel := NewStreamingRelation(rightSymbols, rightIter)

		// Join on both ?id and ?name
		joinSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name")}
		result := SymmetricHashJoin(leftRel, rightRel, joinSymbols)

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
		emptyIter := &sliceIterator{tuples: []Tuple{}, pos: -1}
		emptyRel := NewStreamingRelation(leftSymbols, emptyIter)

		rightIter := &sliceIterator{tuples: rightTuples, pos: -1}
		rightRel := NewStreamingRelation(rightSymbols, rightIter)

		joinSymbols := []query.Symbol{datalog.NewSymbol("?name")}
		result := SymmetricHashJoin(emptyRel, rightRel, joinSymbols)

		it := result.Iterator()
		assert.False(t, it.Next(), "Should have no results with empty left")
		it.Close()

		// Test with empty right relation
		leftIter := &sliceIterator{tuples: leftTuples, pos: -1}
		leftRel := NewStreamingRelation(leftSymbols, leftIter)

		emptyIter2 := &sliceIterator{tuples: []Tuple{}, pos: -1}
		emptyRel2 := NewStreamingRelation(rightSymbols, emptyIter2)

		result = SymmetricHashJoin(leftRel, emptyRel2, joinSymbols)

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

		leftSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name")}
		rightSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name2")}

		leftIter := &sliceIterator{tuples: leftTuples, pos: -1}
		leftRel := NewStreamingRelation(leftSymbols, leftIter)

		rightIter := &sliceIterator{tuples: rightTuples, pos: -1}
		rightRel := NewStreamingRelation(rightSymbols, rightIter)

		// Join on ?id which has no matches
		joinSymbols := []query.Symbol{datalog.NewSymbol("?id")}
		result := SymmetricHashJoin(leftRel, rightRel, joinSymbols)

		it := result.Iterator()
		assert.False(t, it.Next(), "Should have no results when keys don't match")
		it.Close()
	})

	t.Run("StreamingBehavior", func(t *testing.T) {
		// Test that join truly streams (processes incrementally)
		// This is harder to test directly, but we can verify it returns
		// a StreamingRelation and doesn't materialize

		// Enable true streaming for this test
		opts := ExecutorOptions{EnableTrueStreaming: true}

		leftIter := &sliceIterator{tuples: leftTuples, pos: -1}
		leftRel := NewStreamingRelationWithOptions(leftSymbols, leftIter, opts)

		rightIter := &sliceIterator{tuples: rightTuples, pos: -1}
		rightRel := NewStreamingRelationWithOptions(rightSymbols, rightIter, opts)

		joinSymbols := []query.Symbol{datalog.NewSymbol("?name")}
		result := SymmetricHashJoin(leftRel, rightRel, joinSymbols)

		// Verify result is a StreamingRelation
		_, ok := result.(*StreamingRelation)
		assert.True(t, ok, "Result should be a StreamingRelation")

		// Verify Size() returns -1 (unknown) for streaming when EnableTrueStreaming is on
		assert.Equal(t, -1, result.Size())
	})

	t.Run("InvalidJoinSymbol", func(t *testing.T) {
		leftIter := &sliceIterator{tuples: leftTuples, pos: -1}
		leftRel := NewStreamingRelation(leftSymbols, leftIter)

		rightIter := &sliceIterator{tuples: rightTuples, pos: -1}
		rightRel := NewStreamingRelation(rightSymbols, rightIter)

		// Try to join on non-existent symbol
		joinSymbols := []query.Symbol{datalog.NewSymbol("?nonexistent")}
		result := SymmetricHashJoin(leftRel, rightRel, joinSymbols)

		// Should return empty relation
		it := result.Iterator()
		assert.False(t, it.Next(), "Should have no results with invalid join symbol")
		it.Close()
	})
}

func TestChooseJoinStrategy(t *testing.T) {
	// Create test relations
	matTuples := []Tuple{{1, "a"}, {2, "b"}}
	matSymbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	matRel := NewMaterializedRelation(matSymbols, matTuples)

	streamIter := &sliceIterator{tuples: matTuples, pos: -1}
	streamRel := NewStreamingRelation(matSymbols, streamIter)

	joinSymbols := []query.Symbol{datalog.NewSymbol("?x")}

	t.Run("BothMaterialized", func(t *testing.T) {
		opts := ExecutorOptions{}
		strategy := ChooseJoinStrategy(matRel, matRel, joinSymbols, opts)
		assert.Equal(t, "standard", strategy)
	})

	t.Run("BothStreamingWithFeatureEnabled", func(t *testing.T) {
		opts := ExecutorOptions{EnableSymmetricHashJoin: true}
		strategy := ChooseJoinStrategy(streamRel, streamRel, joinSymbols, opts)
		assert.Equal(t, "symmetric", strategy)
	})

	t.Run("BothStreamingWithFeatureDisabled", func(t *testing.T) {
		opts := ExecutorOptions{EnableSymmetricHashJoin: false}
		strategy := ChooseJoinStrategy(streamRel, streamRel, joinSymbols, opts)
		assert.Equal(t, "standard", strategy)
	})

	t.Run("MixedTypes", func(t *testing.T) {
		opts := ExecutorOptions{}
		strategy := ChooseJoinStrategy(matRel, streamRel, joinSymbols, opts)
		assert.Equal(t, "asymmetric", strategy)

		strategy = ChooseJoinStrategy(streamRel, matRel, joinSymbols, opts)
		assert.Equal(t, "asymmetric", strategy)
	})
}

// TestSymmetricHashJoinNextAdvancesAndTupleAnswersTheCurrent pins the
// Iterator contract on the symmetric join's iterator: Next advances to the
// next joined tuple and Tuple answers the current one, so a reader that
// pulls with Next alone, as a count does, reaches the end.
func TestSymmetricHashJoinNextAdvancesAndTupleAnswersTheCurrent(t *testing.T) {
	leftSymbols := []query.Symbol{datalog.NewSymbol("?quire"), datalog.NewSymbol("?leaves")}
	rightSymbols := []query.Symbol{datalog.NewSymbol("?quire"), datalog.NewSymbol("?press")}
	left := NewStreamingRelation(leftSymbols, &sliceIterator{tuples: []Tuple{
		{int64(1), int64(8)}, {int64(2), int64(12)}, {int64(3), int64(16)},
	}, pos: -1})
	right := NewStreamingRelation(rightSymbols, &sliceIterator{tuples: []Tuple{
		{int64(1), "north"}, {int64(2), "south"}, {int64(3), "east"},
	}, pos: -1})
	joinSymbols := []query.Symbol{datalog.NewSymbol("?quire")}

	it := SymmetricHashJoin(left, right, joinSymbols).Iterator()
	defer it.Close()

	if !it.Next() {
		t.Fatal("the join of three matching pairs answers a first tuple")
	}
	first := it.Tuple()
	if again := it.Tuple(); !tupleValuesEqual(first, again) {
		t.Fatalf("Tuple answers the current tuple until the next Next: got %v then %v", first, again)
	}
	if !it.Next() {
		t.Fatal("the join of three matching pairs answers a second tuple")
	}
	second := it.Tuple()
	if tupleValuesEqual(first, second) {
		t.Fatalf("Next advances to the next joined tuple: got %v twice", first)
	}
	pulled := 2
	for it.Next() {
		pulled++
	}
	if pulled != 3 {
		t.Fatalf("a reader pulling with Next alone reaches the end: pulled %d of 3 pairs", pulled)
	}
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

	leftSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?score")}
	rightSymbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?city")}
	joinSymbols := []query.Symbol{datalog.NewSymbol("?name")}

	b.Run("SymmetricHashJoin", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			leftIter := &sliceIterator{tuples: leftTuples, pos: -1}
			leftRel := NewStreamingRelation(leftSymbols, leftIter)

			rightIter := &sliceIterator{tuples: rightTuples, pos: -1}
			rightRel := NewStreamingRelation(rightSymbols, rightIter)

			result := SymmetricHashJoin(leftRel, rightRel, joinSymbols)

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
			leftRel := NewMaterializedRelation(leftSymbols, leftTuples)
			rightRel := NewMaterializedRelation(rightSymbols, rightTuples)

			result := HashJoin(leftRel, rightRel, joinSymbols)

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
