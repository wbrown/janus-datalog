package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// countRelationResults counts results by iterating (streaming relations return -1 from Size())
func countRelationResults(r Relation) int {
	count := 0
	iter := r.Iterator()
	defer iter.Close()
	for iter.Next() {
		count++
	}
	return count
}

// createBenchmarkDataset creates a test dataset with specified size
// Creates realistic OHLC-style data with entities, multiple attributes, and diverse value types
func createBenchmarkDataset(numEntities int) []datalog.Datom {
	datoms := make([]datalog.Datom, 0, numEntities*4) // 4 attributes per entity

	for i := 0; i < numEntities; i++ {
		entityID := fmt.Sprintf("bar%d", i)
		entity := datalog.NewIdentity(entityID)

		// Add 4 attributes per entity (similar to OHLC queries)
		datoms = append(datoms,
			datalog.Datom{E: entity, A: datalog.NewKeyword("price/open"), V: int64(100 + i), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: entity, A: datalog.NewKeyword("price/high"), V: int64(110 + i), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: entity, A: datalog.NewKeyword("price/low"), V: int64(90 + i), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			datalog.Datom{E: entity, A: datalog.NewKeyword("price/close"), V: int64(105 + i), Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		)
	}

	return datoms
}

// BenchmarkPatternMatch_LinearVsIndexed compares performance across different pattern types
func BenchmarkPatternMatch_LinearVsIndexed(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		datoms := createBenchmarkDataset(size)

		// Test different pattern types
		patterns := []struct {
			name    string
			pattern *query.DataPattern
		}{
			{
				name: "EA_bound",
				pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Constant{Value: datalog.NewIdentity("bar50")},
						query.Constant{Value: datalog.NewKeyword("price/open")},
						query.Variable{Name: datalog.NewSymbol("?v")},
					},
				},
			},
			{
				name: "E_bound",
				pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Constant{Value: datalog.NewIdentity("bar50")},
						query.Variable{Name: datalog.NewSymbol("?a")},
						query.Variable{Name: datalog.NewSymbol("?v")},
					},
				},
			},
			{
				name: "A_bound",
				pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword("price/open")},
						query.Variable{Name: datalog.NewSymbol("?v")},
					},
				},
			},
			{
				name: "V_bound",
				pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Variable{Name: datalog.NewSymbol("?a")},
						query.Constant{Value: int64(150)},
					},
				},
			},
			{
				name: "Nothing_bound",
				pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Variable{Name: datalog.NewSymbol("?a")},
						query.Variable{Name: datalog.NewSymbol("?v")},
					},
				},
			},
		}

		for _, pattern := range patterns {
			// Benchmark linear scan
			b.Run(fmt.Sprintf("Linear/%d/%s", size, pattern.name), func(b *testing.B) {
				matcher := NewMemoryPatternMatcher(datoms)
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					result, err := matcher.Match(query.PatternQuery(pattern.pattern), nil)
					if err != nil {
						b.Fatalf("Match failed: %v", err)
					}
					if result.Materialize().Size() == 0 && pattern.name != "V_bound" {
						b.Fatalf("Expected non-empty result for %s", pattern.name)
					}
				}
			})

			// Benchmark indexed scan
			b.Run(fmt.Sprintf("Indexed/%d/%s", size, pattern.name), func(b *testing.B) {
				matcher := NewIndexedMemoryMatcher(datoms)
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					result, err := matcher.Match(query.PatternQuery(pattern.pattern), nil)
					if err != nil {
						b.Fatalf("Match failed: %v", err)
					}
					if result.Materialize().Size() == 0 && pattern.name != "V_bound" {
						b.Fatalf("Expected non-empty result for %s", pattern.name)
					}
				}
			})
		}
	}
}

// BenchmarkIndexBuildTime measures the one-time cost of building indices
func BenchmarkIndexBuildTime(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		datoms := createBenchmarkDataset(size)

		b.Run(fmt.Sprintf("BuildIndices_%d", size), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				matcher := NewIndexedMemoryMatcher(datoms)
				b.StartTimer()
				matcher.buildIndices()
				b.StopTimer()
			}
		})
	}
}

// BenchmarkOHLCStyle_LinearVsIndexed simulates OHLC-style queries
// This is the real-world use case that motivated the optimization
func BenchmarkOHLCStyle_LinearVsIndexed(b *testing.B) {
	// Create datasets matching OHLC sizes
	datasets := []struct {
		name        string
		numBars     int // Number of price bars
		totalDatoms int // 4 attributes per bar
	}{
		{"Daily_22", 22, 88},
		{"Hourly_260", 260, 1040},
		{"Hourly_2600", 2600, 10400},
	}

	for _, ds := range datasets {
		datoms := createBenchmarkDataset(ds.numBars)

		// Typical OHLC query pattern: [?bar :price/open ?open]
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?bar")},
				query.Constant{Value: datalog.NewKeyword("price/open")},
				query.Variable{Name: datalog.NewSymbol("?open")},
			},
		}

		b.Run(fmt.Sprintf("Linear/%s", ds.name), func(b *testing.B) {
			matcher := NewMemoryPatternMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != ds.numBars {
					b.Fatalf("Expected %d results, got %d", ds.numBars, count)
				}
			}
		})

		b.Run(fmt.Sprintf("Indexed/%s", ds.name), func(b *testing.B) {
			matcher := NewIndexedMemoryMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != ds.numBars {
					b.Fatalf("Expected %d results, got %d", ds.numBars, count)
				}
			}
		})
	}
}

// BenchmarkSingleEntityLookup measures performance for highly selective queries
func BenchmarkSingleEntityLookup(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		datoms := createBenchmarkDataset(size)

		// Lookup single entity (most selective)
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Constant{Value: datalog.NewIdentity(fmt.Sprintf("bar%d", size/2))},
				query.Variable{Name: datalog.NewSymbol("?a")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		b.Run(fmt.Sprintf("Linear/%d", size), func(b *testing.B) {
			matcher := NewMemoryPatternMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != 4 {
					b.Fatalf("Expected 4 results, got %d", count)
				}
			}
		})

		b.Run(fmt.Sprintf("Indexed/%d", size), func(b *testing.B) {
			matcher := NewIndexedMemoryMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != 4 {
					b.Fatalf("Expected 4 results, got %d", count)
				}
			}
		})
	}
}

// BenchmarkAttributeScan measures performance for attribute-based queries
func BenchmarkAttributeScan(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		datoms := createBenchmarkDataset(size)

		// Scan by attribute (typical aggregation pattern)
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: datalog.NewKeyword("price/high")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		b.Run(fmt.Sprintf("Linear/%d", size), func(b *testing.B) {
			matcher := NewMemoryPatternMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != size {
					b.Fatalf("Expected %d results, got %d", size, count)
				}
			}
		})

		b.Run(fmt.Sprintf("Indexed/%d", size), func(b *testing.B) {
			matcher := NewIndexedMemoryMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != size {
					b.Fatalf("Expected %d results, got %d", size, count)
				}
			}
		})
	}
}

// BenchmarkWorstCase_FullScan measures performance when no index applies
func BenchmarkWorstCase_FullScan(b *testing.B) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		datoms := createBenchmarkDataset(size)

		// Full scan (nothing bound)
		pattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Variable{Name: datalog.NewSymbol("?a")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		b.Run(fmt.Sprintf("Linear/%d", size), func(b *testing.B) {
			matcher := NewMemoryPatternMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != size*4 {
					b.Fatalf("Expected %d results, got %d", size*4, count)
				}
			}
		})

		b.Run(fmt.Sprintf("Indexed/%d", size), func(b *testing.B) {
			matcher := NewIndexedMemoryMatcher(datoms)
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(query.PatternQuery(pattern), nil)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}
				if count := countRelationResults(result); count != size*4 {
					b.Fatalf("Expected %d results, got %d", size*4, count)
				}
			}
		})
	}
}
