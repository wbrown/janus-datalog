package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkSymmetricVsAsymmetricHashJoin compares symmetric and asymmetric hash joins
// for stream × stream cases with different data sizes
func BenchmarkSymmetricVsAsymmetricHashJoin(b *testing.B) {
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

		// Test asymmetric (regular) hash join
		b.Run(fmt.Sprintf("asymmetric/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: false, // Disable symmetric
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
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})

		// Test symmetric hash join
		b.Run(fmt.Sprintf("symmetric/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					columns:  leftCols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins:    true,
						EnableSymmetricHashJoin: true, // Enable symmetric
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
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})
	}
}

// BenchmarkSymmetricHashJoinTableSize tests different DefaultHashTableSize values
// for symmetric hash join to find optimal cache locality
func BenchmarkSymmetricHashJoinTableSize(b *testing.B) {
	sizes := []int{100, 1000, 5000}
	tableSizes := []int{64, 128, 256, 512, 1024}

	for _, dataSize := range sizes {
		for _, tableSize := range tableSizes {
			b.Run(fmt.Sprintf("data_%d/table_%d", dataSize, tableSize), func(b *testing.B) {
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
					left := &StreamingRelation{
						columns:  leftCols,
						iterator: &sliceIterator{tuples: leftTuples, pos: -1},
						size:     -1,
						options: ExecutorOptions{
							EnableStreamingJoins:    true,
							EnableSymmetricHashJoin: true,
							DefaultHashTableSize:    tableSize,
						},
					}
					right := &StreamingRelation{
						columns:  rightCols,
						iterator: &sliceIterator{tuples: rightTuples, pos: -1},
						size:     -1,
						options: ExecutorOptions{
							EnableStreamingJoins:    true,
							EnableSymmetricHashJoin: true,
							DefaultHashTableSize:    tableSize,
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
