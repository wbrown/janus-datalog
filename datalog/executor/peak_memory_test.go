package executor

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestPeakMemoryFullConsumption measures actual peak memory for full consumption
// This is NOT a benchmark - it's a one-time measurement with detailed reporting
func TestPeakMemoryFullConsumption(t *testing.T) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			leftCols := []query.Symbol{"?x", "?name"}
			rightCols := []query.Symbol{"?x", "?value"}

			leftTuples := make([]Tuple, size)
			rightTuples := make([]Tuple, size)

			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
				rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
			}

			// Test asymmetric
			t.Run("asymmetric", func(t *testing.T) {
				runtime.GC()
				runtime.GC() // Double GC to ensure clean baseline
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

				// Consume all and track peak
				count := 0
				var maxAlloc uint64
				for it.Next() {
					_ = it.Tuple()
					count++

					// Sample every 100 tuples
					if (count % 100) == 0 {
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

				peakIncrease := maxAlloc - memBefore.Alloc
				finalIncrease := memAfter.Alloc - memBefore.Alloc

				t.Logf("Asymmetric (size %d):", size)
				t.Logf("  Peak increase:  %.2f MB", float64(peakIncrease)/1024/1024)
				t.Logf("  Final increase: %.2f MB", float64(finalIncrease)/1024/1024)
				t.Logf("  Results: %d", count)
			})

			// Test symmetric
			t.Run("symmetric", func(t *testing.T) {
				runtime.GC()
				runtime.GC() // Double GC to ensure clean baseline
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

				// Consume all and track peak
				count := 0
				var maxAlloc uint64
				for it.Next() {
					_ = it.Tuple()
					count++

					// Sample every 100 tuples
					if (count % 100) == 0 {
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

				peakIncrease := maxAlloc - memBefore.Alloc
				finalIncrease := memAfter.Alloc - memBefore.Alloc

				t.Logf("Symmetric (size %d):", size)
				t.Logf("  Peak increase:  %.2f MB", float64(peakIncrease)/1024/1024)
				t.Logf("  Final increase: %.2f MB", float64(finalIncrease)/1024/1024)
				t.Logf("  Results: %d", count)
			})
		})
	}
}
