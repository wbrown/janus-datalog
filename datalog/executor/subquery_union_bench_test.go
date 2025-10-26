package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkSubqueryUnionComparison compares streaming union vs materialized union
// This benchmark tests a subquery-heavy query similar to OHLC
func BenchmarkSubqueryUnionComparison(b *testing.B) {
	// Create test data: Use OHLC data with 200 bars (4 subqueries × 50 outer tuples = 200 iterations)
	datoms := createOHLCData(0, 200) // 200 hours of data
	matcher := NewMemoryPatternMatcher(datoms)

	// Simple query with one subquery that gets executed for each hour
	// Each hour executes a subquery to find max high for that hour
	queryStr := `
		[:find ?day ?hour ?max-high
		 :where
		 [?s :symbol/ticker "TEST"]
		 [?b :price/symbol ?s]
		 [?b :price/time ?t]
		 [(day ?t) ?day]
		 [(hour ?t) ?hour]
		 [(q [:find (max ?hv)
		      :in $ ?sym ?d ?h
		      :where
		      [?bar :price/symbol ?sym]
		      [?bar :price/time ?time]
		      [(day ?time) ?bd]
		      [(hour ?time) ?bh]
		      [(= ?bd ?d)]
		      [(= ?bh ?h)]
		      [?bar :price/high ?hv]]
		     $ ?s ?day ?hour) [[?max-high]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.Run("streaming_union", func(b *testing.B) {
		// Create executor with streaming union enabled (default)
		opts := planner.PlannerOptions{
			EnableDynamicReordering:     true,
			EnablePredicatePushdown:     true,
			UseStreamingSubqueryUnion:   true,
			EnableIteratorComposition:   true,
			EnableTrueStreaming:         true,
			EnableStreamingJoins:        true,
			EnableParallelSubqueries:    false, // Sequential for cleaner comparison
		}
		exec := NewExecutorWithOptions(matcher, opts)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			// Force materialization to ensure fair comparison
			_ = result.Size()
		}
	})

	b.Run("materialized_union", func(b *testing.B) {
		// Create executor with streaming union disabled
		opts := planner.PlannerOptions{
			EnableDynamicReordering:     true,
			EnablePredicatePushdown:     true,
			UseStreamingSubqueryUnion:   false, // DISABLED - use materialized
			EnableIteratorComposition:   true,
			EnableTrueStreaming:         true,
			EnableStreamingJoins:        true,
			EnableParallelSubqueries:    false, // Sequential for cleaner comparison
		}
		exec := NewExecutorWithOptions(matcher, opts)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			// Force materialization to ensure fair comparison
			_ = result.Size()
		}
	})
}
