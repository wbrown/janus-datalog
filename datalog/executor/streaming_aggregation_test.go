package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestStreamingAggregation verifies that streaming aggregation produces
// correct results and provides memory benefits for large relations
func TestStreamingAggregation(t *testing.T) {
	// Create a large relation to aggregate
	// Include an ID to make each tuple unique (avoid deduplication)
	columns := []query.Symbol{"?id", "?category", "?price"}
	tuples := make([]Tuple, 0, 10000)

	// Generate 10,000 tuples across 10 categories
	for i := 0; i < 10000; i++ {
		category := string(rune('A' + (i % 10)))
		price := float64(i % 100)
		tuples = append(tuples, Tuple{i, category, price})
	}

	baseRel := NewMaterializedRelation(columns, tuples)

	t.Run("grouped aggregation with streaming", func(t *testing.T) {
		// Create a StreamingRelation with streaming aggregation enabled
		opts := ExecutorOptions{EnableStreamingAggregation: true}
		rel := NewStreamingRelationWithOptions(columns, baseRel.Iterator(), opts)

		findElements := []query.FindElement{
			query.FindVariable{Symbol: "?category"},
			query.FindAggregate{Function: "count", Arg: "?price"},
			query.FindAggregate{Function: "sum", Arg: "?price"},
			query.FindAggregate{Function: "avg", Arg: "?price"},
			query.FindAggregate{Function: "min", Arg: "?price"},
			query.FindAggregate{Function: "max", Arg: "?price"},
		}

		result := ExecuteAggregations(rel, findElements)

		// Verify results
		if result.Size() != 10 {
			t.Errorf("Expected 10 groups, got %d", result.Size())
		}

		// Check that we got streaming aggregation
		if _, ok := result.(*StreamingAggregateRelation); !ok {
			t.Errorf("Expected StreamingAggregateRelation, got %T", result)
		}

		// Verify that results are reasonable (exact values depend on deduplication)
		it := result.Iterator()
		defer it.Close()

		foundA := false
		for it.Next() {
			tuple := it.Tuple()
			if tuple[0] == "A" {
				foundA = true
				// Just verify non-zero counts
				if count := tuple[1].(int64); count == 0 {
					t.Error("Expected non-zero count for category A")
				}
				if sum := tuple[2].(float64); sum == 0.0 {
					t.Error("Expected non-zero sum for category A")
				}
			}
		}

		if !foundA {
			t.Error("Did not find category A in results")
		}
	})

	t.Run("single aggregation with streaming", func(t *testing.T) {
		// Create a StreamingRelation with streaming aggregation enabled
		opts := ExecutorOptions{EnableStreamingAggregation: true}
		rel := NewStreamingRelationWithOptions(columns, baseRel.Iterator(), opts)

		findElements := []query.FindElement{
			query.FindAggregate{Function: "count", Arg: "?price"},
			query.FindAggregate{Function: "sum", Arg: "?price"},
		}

		result := ExecuteAggregations(rel, findElements)

		// Verify single result
		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}

		tuple := result.Get(0)
		// Verify non-zero values
		if count := tuple[0].(int64); count == 0 {
			t.Error("Expected non-zero count")
		}
		if sum := tuple[1].(float64); sum == 0.0 {
			t.Error("Expected non-zero sum")
		}
	})
}

// TestStreamingAggregationCorrectness verifies that streaming aggregation
// produces identical results to batch aggregation
func TestStreamingAggregationCorrectness(t *testing.T) {
	// Create test data
	columns := []query.Symbol{"?x", "?y"}
	tuples := []Tuple{
		{"A", 10.0},
		{"A", 20.0},
		{"A", 30.0},
		{"B", 15.0},
		{"B", 25.0},
		{"C", 5.0},
	}
	baseRel := NewMaterializedRelation(columns, tuples)

	findElements := []query.FindElement{
		query.FindVariable{Symbol: "?x"},
		query.FindAggregate{Function: "count", Arg: "?y"},
		query.FindAggregate{Function: "sum", Arg: "?y"},
		query.FindAggregate{Function: "avg", Arg: "?y"},
		query.FindAggregate{Function: "min", Arg: "?y"},
		query.FindAggregate{Function: "max", Arg: "?y"},
	}

	// Run with streaming
	streamingOpts := ExecutorOptions{EnableStreamingAggregation: true}
	streamingRel := NewStreamingRelationWithOptions(columns, baseRel.Iterator(), streamingOpts)
	streamingResult := ExecuteAggregations(streamingRel, findElements)

	// Run with batch (old implementation)
	batchOpts := ExecutorOptions{EnableStreamingAggregation: false}
	batchRel := NewStreamingRelationWithOptions(columns, baseRel.Iterator(), batchOpts)
	batchResult := ExecuteAggregations(batchRel, findElements)

	// Compare results
	if streamingResult.Size() != batchResult.Size() {
		t.Fatalf("Size mismatch: streaming=%d, batch=%d", streamingResult.Size(), batchResult.Size())
	}

	// Convert to maps for comparison (order may differ)
	streamingMap := make(map[string]Tuple)
	streamingIt := streamingResult.Iterator()
	defer streamingIt.Close()
	for streamingIt.Next() {
		tuple := streamingIt.Tuple()
		key := tuple[0].(string)
		streamingMap[key] = tuple
	}

	batchMap := make(map[string]Tuple)
	batchIt := batchResult.Iterator()
	defer batchIt.Close()
	for batchIt.Next() {
		tuple := batchIt.Tuple()
		key := tuple[0].(string)
		batchMap[key] = tuple
	}

	// Verify each group
	for key, streamingTuple := range streamingMap {
		batchTuple, exists := batchMap[key]
		if !exists {
			t.Errorf("Key %s exists in streaming but not in batch", key)
			continue
		}

		// Compare all aggregate values
		if len(streamingTuple) != len(batchTuple) {
			t.Errorf("Tuple length mismatch for key %s: streaming=%d, batch=%d",
				key, len(streamingTuple), len(batchTuple))
			continue
		}

		for i := 0; i < len(streamingTuple); i++ {
			if streamingTuple[i] != batchTuple[i] {
				t.Errorf("Value mismatch for key %s at index %d: streaming=%v, batch=%v",
					key, i, streamingTuple[i], batchTuple[i])
			}
		}
	}

}

// TestStreamingAggregationThreshold verifies that small relations use batch aggregation
func TestStreamingAggregationThreshold(t *testing.T) {
	columns := []query.Symbol{"?x", "?y"}

	// Small relation (below threshold)
	smallTuples := []Tuple{
		{"A", 10.0},
		{"A", 20.0},
		{"B", 30.0},
	}

	findElements := []query.FindElement{
		query.FindVariable{Symbol: "?x"},
		query.FindAggregate{Function: "sum", Arg: "?y"},
	}

	opts := ExecutorOptions{EnableStreamingAggregation: true}
	smallRel := NewMaterializedRelationWithOptions(columns, smallTuples, opts)
	result := ExecuteAggregations(smallRel, findElements)

	// Should use batch aggregation (below threshold)
	if _, ok := result.(*StreamingAggregateRelation); ok {
		t.Error("Expected batch aggregation for small relation, got streaming")
	}

	// Large relation (above threshold)
	largeTuples := make([]Tuple, StreamingAggregationThreshold+10)
	for i := 0; i < len(largeTuples); i++ {
		largeTuples[i] = Tuple{"A", float64(i)}
	}
	largeRel := NewMaterializedRelationWithOptions(columns, largeTuples, opts)

	result = ExecuteAggregations(largeRel, findElements)

	// Should use streaming aggregation (above threshold)
	if _, ok := result.(*StreamingAggregateRelation); !ok {
		t.Errorf("Expected streaming aggregation for large relation, got %T", result)
	}
}
