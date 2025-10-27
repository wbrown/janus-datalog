package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// Build Size Optimization Benchmarks
// =============================================================================
// These benchmarks determine the optimal DefaultHashTableSize for unknown-size
// streaming relations, balancing initial allocation vs rehashing cost.

// BenchmarkHashJoinBuildSize tests different DefaultHashTableSize values
// This reveals the optimal default for unknown-size streaming relations
func BenchmarkHashJoinBuildSize(b *testing.B) {
	// Test different build sizes
	buildSizes := []int{64, 128, 256, 512, 1024, 2048}

	// Test across different actual data sizes
	dataSizes := []int{50, 100, 250, 500, 1000, 2500, 5000, 10000}

	for _, buildSize := range buildSizes {
		for _, dataSize := range dataSizes {
			b.Run(fmt.Sprintf("build_%d/data_%d", buildSize, dataSize), func(b *testing.B) {
				leftCols := []query.Symbol{"?x", "?name"}
				rightCols := []query.Symbol{"?x", "?value"}

				leftTuples := make([]Tuple, dataSize)
				rightTuples := make([]Tuple, dataSize)

				for i := 0; i < dataSize; i++ {
					leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
					rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// BOTH relations must be streaming to trigger DefaultHashTableSize
					// If one is materialized, it will be chosen as build (known size)
					left := &StreamingRelation{
						columns:  leftCols,
						iterator: &sliceIterator{tuples: leftTuples, pos: -1},
						size:     -1,
						options: ExecutorOptions{
							EnableStreamingJoins: true,
							DefaultHashTableSize: buildSize,
						},
					}

					right := &StreamingRelation{
						columns:  rightCols,
						iterator: &sliceIterator{tuples: rightTuples, pos: -1},
						size:     -1,
						options: ExecutorOptions{
							EnableStreamingJoins: true,
							DefaultHashTableSize: buildSize,
						},
					}

					result := left.Join(right)

					// Consume result
					it := result.Iterator()
					for it.Next() {
						_ = it.Tuple()
					}
					it.Close()
				}
			})
		}
	}
}

// BenchmarkHashJoinBuildSizeOptimal focuses on the most promising candidates
// Run this after identifying the best range from BenchmarkHashJoinBuildSize
func BenchmarkHashJoinBuildSizeOptimal(b *testing.B) {
	// Focus on the sweet spot range
	buildSizes := []int{128, 192, 256, 320, 384, 512}
	dataSizes := []int{100, 500, 1000, 5000}

	for _, buildSize := range buildSizes {
		for _, dataSize := range dataSizes {
			b.Run(fmt.Sprintf("build_%d/data_%d", buildSize, dataSize), func(b *testing.B) {
				leftCols := []query.Symbol{"?x", "?name"}
				rightCols := []query.Symbol{"?x", "?value"}

				leftTuples := make([]Tuple, dataSize)
				rightTuples := make([]Tuple, dataSize)

				for i := 0; i < dataSize; i++ {
					leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
					rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
				}

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// BOTH streaming to trigger DefaultHashTableSize
					left := &StreamingRelation{
						columns:  leftCols,
						iterator: &sliceIterator{tuples: leftTuples, pos: -1},
						size:     -1,
						options: ExecutorOptions{
							EnableStreamingJoins: true,
							DefaultHashTableSize: buildSize,
						},
					}

					right := &StreamingRelation{
						columns:  rightCols,
						iterator: &sliceIterator{tuples: rightTuples, pos: -1},
						size:     -1,
						options: ExecutorOptions{
							EnableStreamingJoins: true,
							DefaultHashTableSize: buildSize,
						},
					}

					result := left.Join(right)

					it := result.Iterator()
					for it.Next() {
						_ = it.Tuple()
					}
					it.Close()
				}
			})
		}
	}
}

// =============================================================================
// Input Type Comparison Benchmarks
// =============================================================================
// These benchmarks compare performance across different combinations of
// materialized and streaming inputs to understand optimization opportunities.

// BenchmarkHashJoinInputTypes compares performance across different input combinations
// This reveals the cost of streaming vs materialized inputs
func BenchmarkHashJoinInputTypes(b *testing.B) {
	sizes := []int{100, 1000, 5000}

	for _, size := range sizes {
		leftCols := []query.Symbol{"?x", "?name"}
		rightCols := []query.Symbol{"?x", "?value"}

		leftTuples := make([]Tuple, size)
		rightTuples := make([]Tuple, size)

		for i := 0; i < size; i++ {
			leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
			rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
		}

		// Case 1: Both Materialized (baseline - optimal)
		b.Run(fmt.Sprintf("mat_x_mat/size_%d", size), func(b *testing.B) {
			left := NewMaterializedRelation(leftCols, leftTuples)
			right := NewMaterializedRelation(rightCols, rightTuples)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := left.Join(right)
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})

		// Case 2: Streaming Left × Materialized Right
		// Uses RIGHT as build (known size) → optimal pre-sizing
		b.Run(fmt.Sprintf("stream_x_mat/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options:  ExecutorOptions{EnableStreamingJoins: true},
				}
				right := NewMaterializedRelation(rightCols, rightTuples)

				result := left.Join(right)
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})

		// Case 3: Materialized Left × Streaming Right
		// Uses LEFT as build (known size) → optimal pre-sizing
		b.Run(fmt.Sprintf("mat_x_stream/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := NewMaterializedRelation(leftCols, leftTuples)
				right := &StreamingRelation{
					columns:  rightCols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options:  ExecutorOptions{EnableStreamingJoins: true},
				}

				result := left.Join(right)
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})

		// Case 4: Both Streaming (worst case)
		// Uses LEFT as build (unknown size) → DefaultHashTableSize = 256
		b.Run(fmt.Sprintf("stream_x_stream/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins: true,
						DefaultHashTableSize: 256,
					},
				}
				right := &StreamingRelation{
					columns:  rightCols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins: true,
						DefaultHashTableSize: 256,
					},
				}

				result := left.Join(right)
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})
	}
}

// BenchmarkHashJoinMaterializedVsStreaming directly compares the two modes
func BenchmarkHashJoinMaterializedVsStreaming(b *testing.B) {
	size := 1000

	b.Run("materialized", func(b *testing.B) {
		leftCols := []query.Symbol{"?x", "?name"}
		rightCols := []query.Symbol{"?x", "?value"}

		leftTuples := make([]Tuple, size)
		rightTuples := make([]Tuple, size)

		for i := 0; i < size; i++ {
			leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
			rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
		}

		left := NewMaterializedRelation(leftCols, leftTuples)
		right := NewMaterializedRelation(rightCols, rightTuples)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			result := left.Join(right)
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
			it.Close()
		}
	})

	b.Run("streaming", func(b *testing.B) {
		leftCols := []query.Symbol{"?x", "?name"}
		rightCols := []query.Symbol{"?x", "?value"}

		leftTuples := make([]Tuple, size)
		rightTuples := make([]Tuple, size)

		for i := 0; i < size; i++ {
			leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
			rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			left := &StreamingRelation{
				columns:  leftCols,
				iterator: &sliceIterator{tuples: leftTuples, pos: -1},
				size:     -1,
				options:  ExecutorOptions{EnableStreamingJoins: true},
			}
			right := NewMaterializedRelation(rightCols, rightTuples)

			result := left.Join(right)
			it := result.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
			it.Close()
		}
	})
}

// =============================================================================
// Streaming Behavior Benchmarks
// =============================================================================
// These benchmarks test various streaming scenarios to validate that removing
// forced materialization provides the expected performance benefits.

// BenchmarkHashJoinStreaming measures HashJoin with streaming relations
// This benchmark isolates the impact of removing forced materialization
func BenchmarkHashJoinStreaming(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Create two relations with common column
			leftCols := []query.Symbol{"?x", "?name"}
			rightCols := []query.Symbol{"?x", "?value"}

			leftTuples := make([]Tuple, size)
			rightTuples := make([]Tuple, size)

			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
				rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
			}

			left := NewMaterializedRelation(leftCols, leftTuples)
			right := NewMaterializedRelation(rightCols, rightTuples)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := left.Join(right)

				// Consume result to measure full pipeline
				it := result.Iterator()
				count := 0
				for it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				if count != size {
					b.Fatalf("expected %d results, got %d", size, count)
				}
			}
		})
	}
}

// BenchmarkHashJoinSingleIteration measures single-pass consumption
// This is the common case - result is consumed once
func BenchmarkHashJoinSingleIteration(b *testing.B) {
	size := 1000
	leftCols := []query.Symbol{"?x", "?name"}
	rightCols := []query.Symbol{"?x", "?value"}

	leftTuples := make([]Tuple, size)
	rightTuples := make([]Tuple, size)

	for i := 0; i < size; i++ {
		leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
		rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
	}

	left := NewMaterializedRelation(leftCols, leftTuples)
	right := NewMaterializedRelation(rightCols, rightTuples)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := left.Join(right)

		// Single iteration - most common pattern
		it := result.Iterator()
		for it.Next() {
			_ = it.Tuple()
		}
		it.Close()
	}
}

// BenchmarkHashJoinStreamingInput tests join with streaming (unknown size) inputs
// This reveals whether the default buildSize = 256 is appropriate
func BenchmarkHashJoinStreamingInput(b *testing.B) {
	sizes := []int{50, 100, 500, 1000, 5000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			leftCols := []query.Symbol{"?x", "?name"}
			rightCols := []query.Symbol{"?x", "?value"}

			leftTuples := make([]Tuple, size)
			rightTuples := make([]Tuple, size)

			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
				rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Create streaming left relation (Size() = -1)
				// This triggers DefaultHashTableSize
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options:  ExecutorOptions{EnableStreamingJoins: true},
				}

				// Right side materialized (has known size)
				right := NewMaterializedRelation(rightCols, rightTuples)

				result := left.Join(right)

				// Consume result
				it := result.Iterator()
				count := 0
				for it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()

				if count != size {
					b.Fatalf("expected %d results, got %d", size, count)
				}
			}
		})
	}
}

// =============================================================================
// Memory and Scale Benchmarks
// =============================================================================
// These benchmarks test performance under memory pressure and large datasets.

// BenchmarkHashJoinLargeResult measures memory pressure with large joins
func BenchmarkHashJoinLargeResult(b *testing.B) {
	sizes := []int{10000, 50000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			leftCols := []query.Symbol{"?x", "?data"}
			rightCols := []query.Symbol{"?x", "?value"}

			leftTuples := make([]Tuple, size)
			rightTuples := make([]Tuple, size)

			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{int64(i), fmt.Sprintf("data_%d", i)}
				rightTuples[i] = Tuple{int64(i), int64(i * 100)}
			}

			left := NewMaterializedRelation(leftCols, leftTuples)
			right := NewMaterializedRelation(rightCols, rightTuples)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				result := left.Join(right)

				// Consume all results
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})
	}
}
