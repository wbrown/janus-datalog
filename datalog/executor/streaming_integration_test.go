package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

func TestStreamingIntegration(t *testing.T) {
	t.Run("FullStreamingPipeline", func(t *testing.T) {
		// Create options with all streaming features enabled
		opts := ExecutorOptions{
			EnableTrueStreaming:     true,
			EnableSymmetricHashJoin: true,
		}

		// Create test data for left relation
		leftTuples := []Tuple{
			{1, "alice", 100},
			{2, "bob", 200},
			{3, "charlie", 300},
			{4, "diana", 400},
			{5, "eve", 500},
		}
		leftSymbols := []query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name"), datalog.NewSymbol("?score")}

		// Create test data for right relation
		rightTuples := []Tuple{
			{"alice", "NYC", 25},
			{"bob", "LA", 30},
			{"charlie", "Chicago", 35},
			{"diana", "Boston", 40},
		}
		rightSymbols := []query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?city"), datalog.NewSymbol("?age")}

		// Create streaming relations with options
		leftIter := newMockIterator(leftTuples)
		leftRel := NewStreamingRelationWithOptions(leftSymbols, leftIter, opts)

		rightIter := newMockIterator(rightTuples)
		rightRel := NewStreamingRelationWithOptions(rightSymbols, rightIter, opts)

		// Verify relations report as streaming
		assert.Equal(t, -1, leftRel.Size())
		assert.Equal(t, -1, rightRel.Size())

		// Apply filter on left (score > 150)
		filteredLeft := leftRel.FilterWithPredicate(&query.Comparison{
			Op:    datalog.SymGT,
			Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?score")},
			Right: query.ConstantTerm{Value: int64(150)},
		})

		// Verify filter returns streaming relation
		_, isStreaming := filteredLeft.(*StreamingRelation)
		assert.True(t, isStreaming, "Filter should return StreamingRelation")

		// Apply filter on right (age >= 30)
		filteredRight := rightRel.FilterWithPredicate(&query.Comparison{
			Op:    datalog.SymGTE,
			Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
			Right: query.ConstantTerm{Value: int64(30)},
		})

		// Verify filter returns streaming relation
		_, isStreaming = filteredRight.(*StreamingRelation)
		assert.True(t, isStreaming, "Filter should return StreamingRelation")

		// Perform join on ?name
		joinSymbols := []query.Symbol{datalog.NewSymbol("?name")}
		joined := SymmetricHashJoin(filteredLeft, filteredRight, joinSymbols)

		// Verify join returns streaming relation
		_, isStreaming = joined.(*StreamingRelation)
		assert.True(t, isStreaming, "Join should return StreamingRelation")

		// Project to keep only certain symbols
		projected, err := joined.Project([]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?score"), datalog.NewSymbol("?city")})
		assert.NoError(t, err)

		// Verify projection returns streaming relation
		_, isStreaming = projected.(*StreamingRelation)
		assert.True(t, isStreaming, "Project should return StreamingRelation")

		// Check empty without materialization
		assert.False(t, projected.IsEmpty())

		// Iterate results
		it := projected.Iterator()
		var results []Tuple
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}
		it.Close()

		// Verify results
		// Should have: bob(200), charlie(300), diana(400) after filters and join
		assert.Len(t, results, 3)

		// Verify we can iterate the collected results
		// Create a materialized relation from the results we already collected
		materialized := NewMaterializedRelation(projected.Symbols(), results)
		it2 := materialized.Iterator()
		count := 0
		for it2.Next() {
			count++
		}
		it2.Close()
		assert.Equal(t, 3, count, "Should support iteration of materialized results")
	})

	t.Run("MixedStreamingAndMaterialized", func(t *testing.T) {
		// Create options with streaming features (no symmetric hash join for mixed)
		opts := ExecutorOptions{
			EnableTrueStreaming:     true,
			EnableSymmetricHashJoin: false,
		}

		// Create streaming relation
		streamTuples := []Tuple{
			{1, "alice"},
			{2, "bob"},
			{3, "charlie"},
		}
		streamIter := newMockIterator(streamTuples)
		streamRel := NewStreamingRelationWithOptions([]query.Symbol{datalog.NewSymbol("?id"), datalog.NewSymbol("?name")}, streamIter, opts)

		// Create materialized relation with options
		matTuples := []Tuple{
			{"alice", 100},
			{"bob", 200},
		}
		matRel := NewMaterializedRelationWithOptions([]query.Symbol{datalog.NewSymbol("?name"), datalog.NewSymbol("?value")}, matTuples, opts)

		// Join streaming with materialized
		joinSymbols := []query.Symbol{datalog.NewSymbol("?name")}
		joined := HashJoin(streamRel, matRel, joinSymbols)

		// Iterate results
		it := joined.Iterator()
		var results []Tuple
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}
		it.Close()

		// Should have alice and bob matches
		assert.Len(t, results, 2)
	})

	t.Run("StreamingWithPredicates", func(t *testing.T) {
		// Create options with streaming features
		opts := ExecutorOptions{
			EnableTrueStreaming: true,
		}

		tuples := []Tuple{
			{10, 20},
			{30, 40},
			{50, 60},
			{70, 80},
		}
		symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

		source := newMockIterator(tuples)
		rel := NewStreamingRelationWithOptions(symbols, source, opts)

		// Apply predicate filter (x > 30)
		pred := &query.Comparison{
			Op:    datalog.SymGT,
			Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
			Right: query.ConstantTerm{Value: 30},
		}

		filtered := rel.FilterWithPredicate(pred)

		// Verify it's still streaming
		_, isStreaming := filtered.(*StreamingRelation)
		assert.True(t, isStreaming)

		// Iterate and check results
		it := filtered.Iterator()
		var results []Tuple
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}
		it.Close()

		assert.Len(t, results, 2) // 50,60 and 70,80
		assert.Equal(t, 50, results[0][0])
		assert.Equal(t, 70, results[1][0])
	})

	t.Run("StreamingWithFunctions", func(t *testing.T) {
		// Create options with streaming features
		opts := ExecutorOptions{
			EnableTrueStreaming: true,
		}

		tuples := []Tuple{
			{10, 20},
			{30, 40},
		}
		symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}

		source := newMockIterator(tuples)
		rel := NewStreamingRelationWithOptions(symbols, source, opts)

		// Apply function evaluation (x + y)
		fn := query.ArithmeticFunction{
			Op: datalog.SymAdd,
			Args: []query.Term{
				query.VariableTerm{Symbol: datalog.NewSymbol("?x")},
				query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
			},
		}

		withFunction := rel.EvaluateFunction(fn, datalog.NewSymbol("?sum"))

		// Verify it's still streaming
		_, isStreaming := withFunction.(*StreamingRelation)
		assert.True(t, isStreaming)

		// Check symbols
		assert.Equal(t, []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?sum")}, withFunction.Symbols())

		// Iterate results
		it := withFunction.Iterator()
		var results []Tuple
		for it.Next() {
			tuple := it.Tuple()
			tupleCopy := make(Tuple, len(tuple))
			copy(tupleCopy, tuple)
			results = append(results, tupleCopy)
		}
		it.Close()

		assert.Len(t, results, 2)
		assert.Equal(t, int64(30), results[0][2]) // 10 + 20
		assert.Equal(t, int64(70), results[1][2]) // 30 + 40
	})

	t.Run("StreamingPerformanceCharacteristics", func(t *testing.T) {
		// Create options with streaming features
		opts := ExecutorOptions{
			EnableTrueStreaming: true,
		}

		// Create a large dataset
		var largeTuples []Tuple
		for i := 0; i < 1000; i++ {
			// ?y cycles so the 1% filter below selects every 100th tuple —
			// scattered matches interleave pass and skip in the filter iterator.
			largeTuples = append(largeTuples, Tuple{i, i % 100, i * 3})
		}
		symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}

		source := newMockIterator(largeTuples)
		rel := NewStreamingRelationWithOptions(symbols, source, opts)

		// Apply aggressive filter (only 1% pass, every 100th tuple)
		filtered := rel.FilterWithPredicate(&query.Comparison{
			Op:    datalog.SymEQ,
			Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
			Right: query.ConstantTerm{Value: int64(0)},
		})

		// Project to single symbol
		projected, err := filtered.Project([]query.Symbol{datalog.NewSymbol("?x")})
		assert.NoError(t, err)

		// With streaming, this entire pipeline should process lazily
		// Only materializing the 10 tuples that pass the filter

		// Verify still streaming
		_, isStreaming := projected.(*StreamingRelation)
		assert.True(t, isStreaming)

		// Iterate and count
		it := projected.Iterator()
		count := 0
		for it.Next() {
			count++
		}
		it.Close()

		assert.Equal(t, 10, count) // Only 0, 100, 200, ..., 900 pass
	})
}

// BenchmarkStreamingVsMaterialized compares memory usage patterns
func BenchmarkStreamingVsMaterialized(b *testing.B) {
	size := 10000
	var tuples []Tuple
	for i := 0; i < size; i++ {
		// ?y cycles so the 1% filter below selects every 100th tuple.
		tuples = append(tuples, Tuple{i, i % 100, i * 3})
	}
	symbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y"), datalog.NewSymbol("?z")}

	b.Run("Materialized", func(b *testing.B) {
		opts := ExecutorOptions{}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rel := NewMaterializedRelationWithOptions(symbols, tuples, opts)

			// Filter to 1% (every 100th tuple)
			filtered := rel.FilterWithPredicate(&query.Comparison{
				Op:    datalog.SymEQ,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
				Right: query.ConstantTerm{Value: int64(0)},
			})

			// Project
			projected, _ := filtered.Project([]query.Symbol{datalog.NewSymbol("?x")})

			// Consume
			it := projected.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
			it.Close()
		}
	})

	b.Run("Streaming", func(b *testing.B) {
		opts := ExecutorOptions{
			EnableTrueStreaming: true,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			source := newMockIterator(tuples)
			rel := NewStreamingRelationWithOptions(symbols, source, opts)

			// Filter to 1% (every 100th tuple)
			filtered := rel.FilterWithPredicate(&query.Comparison{
				Op:    datalog.SymEQ,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
				Right: query.ConstantTerm{Value: int64(0)},
			})

			// Project
			projected, _ := filtered.Project([]query.Symbol{datalog.NewSymbol("?x")})

			// Consume
			it := projected.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
			it.Close()
		}
	})

	b.Run("StreamingWithoutTrueStreaming", func(b *testing.B) {
		// Composed iterators with EnableTrueStreaming off: the streaming
		// pipeline that still allows re-iteration through caching.
		opts := ExecutorOptions{}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			source := newMockIterator(tuples)
			rel := NewStreamingRelationWithOptions(symbols, source, opts)

			// Filter to 1% (every 100th tuple)
			filtered := rel.FilterWithPredicate(&query.Comparison{
				Op:    datalog.SymEQ,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?y")},
				Right: query.ConstantTerm{Value: int64(0)},
			})

			// Project
			projected, _ := filtered.Project([]query.Symbol{datalog.NewSymbol("?x")})

			// Consume
			it := projected.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
			it.Close()
		}
	})
}
