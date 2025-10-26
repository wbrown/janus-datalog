package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkHashJoinPreSizing compares hash join performance with and without pre-sizing
func BenchmarkHashJoinPreSizing(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			// Create two relations to join
			leftCols := []query.Symbol{"?a", "?b", "?c"}
			rightCols := []query.Symbol{"?b", "?d"}
			joinCols := []query.Symbol{"?b"}

			// Generate test data
			leftTuples := make([]Tuple, size)
			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{i, i % 100, i * 2} // ?a, ?b, ?c
			}

			rightTuples := make([]Tuple, size/2)
			for i := 0; i < size/2; i++ {
				rightTuples[i] = Tuple{i % 100, i * 3} // ?b, ?d
			}

			left := NewMaterializedRelation(leftCols, leftTuples)
			right := NewMaterializedRelation(rightCols, rightTuples)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := HashJoin(left, right, joinCols)
				if result.Size() == 0 {
					b.Fatal("Empty result")
				}
			}
		})
	}
}

// BenchmarkTupleKeyMapPreSizing specifically benchmarks TupleKeyMap operations
func BenchmarkTupleKeyMapPreSizing(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		// Test without pre-sizing
		b.Run(fmt.Sprintf("NoPresize_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := NewTupleKeyMap()
				for j := 0; j < size; j++ {
					key := NewTupleKeyFull(Tuple{j, j * 2})
					m.Put(key, j)
				}
			}
		})

		// Test with pre-sizing
		b.Run(fmt.Sprintf("WithPresize_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := NewTupleKeyMapWithCapacity(size)
				for j := 0; j < size; j++ {
					key := NewTupleKeyFull(Tuple{j, j * 2})
					m.Put(key, j)
				}
			}
		})
	}
}

// BenchmarkDeduplicationPreSizing benchmarks deduplication with pre-sizing
func BenchmarkDeduplicationPreSizing(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			// Create tuples with some duplicates
			tuples := make([]Tuple, size)
			for i := 0; i < size; i++ {
				// Create ~10% duplicates
				tuples[i] = Tuple{i % (size / 10), i * 2}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := deduplicateTuples(tuples)
				if len(result) == 0 {
					b.Fatal("Empty result")
				}
			}
		})
	}
}

// BenchmarkOHLCWithPreSizing measures the full OHLC query impact
func BenchmarkOHLCWithPreSizing(b *testing.B) {
	// This reuses the OHLC test from ohlc_chain_profile_test.go
	// but focuses on allocation metrics

	datoms := createOHLCData(0, 260) // 260 hours
	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	queryStr := `
	[:find ?day ?hour ?open ?high ?low ?close
	 :where
	 [?s :symbol/ticker "TEST"]
	 [?b :price/symbol ?s]
	 [?b :price/time ?t]
	 [(day ?t) ?day]
	 [(hour ?t) ?hour]

	 [(q [:find (min ?o)
	      :in $ ?sym ?d ?h
	      :where
	      [?bar :price/symbol ?sym]
	      [?bar :price/time ?time]
	      [(day ?time) ?bd]
	      [(hour ?time) ?bh]
	      [(= ?bd ?d)]
	      [(= ?bh ?h)]
	      [?bar :price/open ?o]]
	     $ ?s ?day ?hour) [[?open]]]

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
	     $ ?s ?day ?hour) [[?high]]]

	 [(q [:find (min ?l)
	      :in $ ?sym ?d ?h
	      :where
	      [?bar :price/symbol ?sym]
	      [?bar :price/time ?time]
	      [(day ?time) ?bd]
	      [(hour ?time) ?bh]
	      [(= ?bd ?d)]
	      [(= ?bh ?h)]
	      [?bar :price/low ?l]]
	     $ ?s ?day ?hour) [[?low]]]

	 [(q [:find (max ?c)
	      :in $ ?sym ?d ?h
	      :where
	      [?bar :price/symbol ?sym]
	      [?bar :price/time ?time]
	      [(day ?time) ?bd]
	      [(hour ?time) ?bh]
	      [(= ?bd ?d)]
	      [(= ?bh ?h)]
	      [?bar :price/close ?c]]
	     $ ?s ?day ?hour) [[?close]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(q)
		if err != nil {
			b.Fatalf("Query execution failed: %v", err)
		}
	}
}
