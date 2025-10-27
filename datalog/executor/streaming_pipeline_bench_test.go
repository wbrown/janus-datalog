package executor

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkTimeToFirstResult measures how quickly each join strategy produces the first result
// Symmetric should win here by producing results incrementally
func BenchmarkTimeToFirstResult(b *testing.B) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		leftCols := []query.Symbol{"?x", "?name"}
		rightCols := []query.Symbol{"?x", "?value"}

		leftTuples := make([]Tuple, size)
		rightTuples := make([]Tuple, size)

		for i := 0; i < size; i++ {
			leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
			rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
		}

		// Asymmetric
		b.Run(fmt.Sprintf("asymmetric/size_%d", size), func(b *testing.B) {
			b.ReportAllocs()

			var totalTime time.Duration
			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: false,
						DefaultHashTableSize:    256,
					},
				}
				right := &StreamingRelation{
					columns:  rightCols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: false,
						DefaultHashTableSize:    256,
					},
				}

				start := time.Now()
				result := left.Join(right)
				it := result.Iterator()
				it.Next() // Get first result
				totalTime += time.Since(start)
				it.Close()
			}
			b.ReportMetric(float64(totalTime.Nanoseconds())/float64(b.N), "ns/first")
		})

		// Symmetric
		b.Run(fmt.Sprintf("symmetric/size_%d", size), func(b *testing.B) {
			b.ReportAllocs()

			var totalTime time.Duration
			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: true,
						DefaultHashTableSize:    256,
					},
				}
				right := &StreamingRelation{
					columns:  rightCols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: true,
						DefaultHashTableSize:    256,
					},
				}

				start := time.Now()
				result := left.Join(right)
				it := result.Iterator()
				it.Next() // Get first result
				totalTime += time.Since(start)
				it.Close()
			}
			b.ReportMetric(float64(totalTime.Nanoseconds())/float64(b.N), "ns/first")
		})
	}
}

// BenchmarkLimitQueries measures performance when only consuming first N results
// Symmetric should win by stopping input iteration early
func BenchmarkLimitQueries(b *testing.B) {
	dataSize := 10000
	limits := []int{10, 100, 1000}

	leftCols := []query.Symbol{"?x", "?name"}
	rightCols := []query.Symbol{"?x", "?value"}

	leftTuples := make([]Tuple, dataSize)
	rightTuples := make([]Tuple, dataSize)

	for i := 0; i < dataSize; i++ {
		leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
		rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
	}

	for _, limit := range limits {
		// Asymmetric
		b.Run(fmt.Sprintf("asymmetric/limit_%d", limit), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: false,
						DefaultHashTableSize:    256,
					},
				}
				right := &StreamingRelation{
					columns:  rightCols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: false,
						DefaultHashTableSize:    256,
					},
				}

				result := left.Join(right)
				it := result.Iterator()
				count := 0
				for count < limit && it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()
			}
		})

		// Symmetric
		b.Run(fmt.Sprintf("symmetric/limit_%d", limit), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: true,
						DefaultHashTableSize:    256,
					},
				}
				right := &StreamingRelation{
					columns:  rightCols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: true,
						DefaultHashTableSize:    256,
					},
				}

				result := left.Join(right)
				it := result.Iterator()
				count := 0
				for count < limit && it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()
			}
		})
	}
}

// BenchmarkPeakMemory measures peak memory usage during execution
// Symmetric should have lower peak by processing incrementally
func BenchmarkPeakMemory(b *testing.B) {
	size := 50000 // Large dataset to see memory difference

	leftCols := []query.Symbol{"?x", "?name"}
	rightCols := []query.Symbol{"?x", "?value"}

	leftTuples := make([]Tuple, size)
	rightTuples := make([]Tuple, size)

	for i := 0; i < size; i++ {
		leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
		rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
	}

	// Asymmetric
	b.Run("asymmetric", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Force GC before measurement
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			left := &StreamingRelation{
				columns:  leftCols,
				iterator: &sliceIterator{tuples: leftTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
				},
			}
			right := &StreamingRelation{
				columns:  rightCols,
				iterator: &sliceIterator{tuples: rightTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
				},
			}

			result := left.Join(right)
			it := result.Iterator()

			// Measure peak during iteration
			var maxAlloc uint64
			for it.Next() {
				_ = it.Tuple()

				// Sample memory every 1000 tuples
				if (i % 1000) == 0 {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					if m.Alloc > maxAlloc {
						maxAlloc = m.Alloc
					}
				}
			}
			it.Close()

			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			peakIncrease := memAfter.Alloc - memBefore.Alloc
			b.ReportMetric(float64(peakIncrease)/1024/1024, "MB/peak")
		}
	})

	// Symmetric
	b.Run("symmetric", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Force GC before measurement
			runtime.GC()
			var memBefore runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			left := &StreamingRelation{
				columns:  leftCols,
				iterator: &sliceIterator{tuples: leftTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: true,
					DefaultHashTableSize:    256,
				},
			}
			right := &StreamingRelation{
				columns:  rightCols,
				iterator: &sliceIterator{tuples: rightTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: true,
					DefaultHashTableSize:    256,
				},
			}

			result := left.Join(right)
			it := result.Iterator()

			// Measure peak during iteration
			var maxAlloc uint64
			for it.Next() {
				_ = it.Tuple()

				// Sample memory every 1000 tuples
				if (i % 1000) == 0 {
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					if m.Alloc > maxAlloc {
						maxAlloc = m.Alloc
					}
				}
			}
			it.Close()

			var memAfter runtime.MemStats
			runtime.ReadMemStats(&memAfter)

			peakIncrease := memAfter.Alloc - memBefore.Alloc
			b.ReportMetric(float64(peakIncrease)/1024/1024, "MB/peak")
		}
	})
}

// BenchmarkMultiStagePipeline measures throughput of chained operations
// Symmetric should allow better pipelining across stages
func BenchmarkMultiStagePipeline(b *testing.B) {
	size := 10000

	leftCols := []query.Symbol{"?x", "?name"}
	rightCols := []query.Symbol{"?x", "?value"}

	leftTuples := make([]Tuple, size)
	rightTuples := make([]Tuple, size)

	for i := 0; i < size; i++ {
		leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
		rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
	}

	// Asymmetric
	b.Run("asymmetric", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			left := &StreamingRelation{
				columns:  leftCols,
				iterator: &sliceIterator{tuples: leftTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
				},
			}
			right := &StreamingRelation{
				columns:  rightCols,
				iterator: &sliceIterator{tuples: rightTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: false,
					DefaultHashTableSize:    256,
				},
			}

			// Multi-stage pipeline: Join → Filter → Project
			joined := left.Join(right)

			// Filter: only even IDs
			filtered := joined.Select(func(t Tuple) bool {
				if id, ok := t[0].(int64); ok {
					return id%2 == 0
				}
				return false
			})

			// Project: drop the ID column
			projected, err := filtered.Project([]query.Symbol{"?name", "?value"})
			if err != nil {
				b.Fatal(err)
			}

			// Consume results
			it := projected.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
			it.Close()
		}
	})

	// Symmetric
	b.Run("symmetric", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			left := &StreamingRelation{
				columns:  leftCols,
				iterator: &sliceIterator{tuples: leftTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: true,
					DefaultHashTableSize:    256,
				},
			}
			right := &StreamingRelation{
				columns:  rightCols,
				iterator: &sliceIterator{tuples: rightTuples, pos: -1},
				size:     -1,
				options: ExecutorOptions{
					EnableStreamingJoins:    true,
					EnableSymmetricHashJoin: true,
					DefaultHashTableSize:    256,
				},
			}

			// Multi-stage pipeline: Join → Filter → Project
			joined := left.Join(right)

			// Filter: only even IDs
			filtered := joined.Select(func(t Tuple) bool {
				if id, ok := t[0].(int64); ok {
					return id%2 == 0
				}
				return false
			})

			// Project: drop the ID column
			projected, err := filtered.Project([]query.Symbol{"?name", "?value"})
			if err != nil {
				b.Fatal(err)
			}

			// Consume results
			it := projected.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
			it.Close()
		}
	})
}
