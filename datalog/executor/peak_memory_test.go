package executor

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/query"

	"github.com/wbrown/janus-datalog/datalog"
)

// TestPeakMemoryFullConsumption measures actual peak memory for full consumption
// This is NOT a benchmark - it's a one-time measurement with detailed reporting
func TestPeakMemoryFullConsumption(t *testing.T) {
	sizes := []int{1000, 10000}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			leftSymbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?name")}
			rightSymbols := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?value")}

			leftTuples := make([]Tuple, size)
			rightTuples := make([]Tuple, size)

			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{int64(i), fmt.Sprintf("name%d", i)}
				rightTuples[i] = Tuple{int64(i), fmt.Sprintf("value%d", i)}
			}

			// Test asymmetric
			t.Run("asymmetric", func(t *testing.T) {
				seen := make([]bool, size)
				runtime.GC()
				runtime.GC() // Double GC to ensure clean baseline
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				left := &StreamingRelation{
					symbols:  leftSymbols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableSymmetricHashJoin: false,
						DefaultHashTableSize:    256,
					},
				}
				right := &StreamingRelation{
					symbols:  rightSymbols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableSymmetricHashJoin: false,
						DefaultHashTableSize:    256,
					},
				}

				result := left.Join(right)
				symbols := result.Symbols()
				xi, ni, vi := query.SymbolIndex(symbols, leftSymbols[0]), query.SymbolIndex(symbols, leftSymbols[1]), query.SymbolIndex(symbols, rightSymbols[1])
				if xi < 0 || ni < 0 || vi < 0 {
					t.Fatalf("the join carries ?x, ?name and ?value; got %v", symbols)
				}
				it := result.Iterator()

				// Consume all and track peak. Each ?x is joined once, with the
				// name and the value its own tuples carry.
				count := 0
				var maxAlloc uint64
				for it.Next() {
					tuple := it.Tuple()
					x := tuple[xi].(int64)
					if seen[x] || tuple[ni] != leftTuples[x][1] || tuple[vi] != rightTuples[x][1] {
						t.Fatalf("tuple %v is not ?x %d joined once with its own name and value", tuple, x)
					}
					seen[x] = true
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
				if err := it.Error(); err != nil {
					t.Fatalf("iterate: %v", err)
				}
				if err := it.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}

				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				peakIncrease := maxAlloc - memBefore.Alloc
				finalIncrease := memAfter.Alloc - memBefore.Alloc

				t.Logf("Asymmetric (size %d):", size)
				t.Logf("  Peak increase:  %.2f MB", float64(peakIncrease)/1024/1024)
				t.Logf("  Final increase: %.2f MB", float64(finalIncrease)/1024/1024)
				t.Logf("  Results: %d", count)
				if count != size {
					t.Fatalf("the join answered %d tuples, want one per ?x, %d", count, size)
				}
			})

			// Test symmetric
			t.Run("symmetric", func(t *testing.T) {
				seen := make([]bool, size)
				runtime.GC()
				runtime.GC() // Double GC to ensure clean baseline
				var memBefore runtime.MemStats
				runtime.ReadMemStats(&memBefore)

				left := &StreamingRelation{
					symbols:  leftSymbols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableSymmetricHashJoin: true,
						DefaultHashTableSize:    256,
					},
				}
				right := &StreamingRelation{
					symbols:  rightSymbols,
					iterator: &sliceIterator{tuples: rightTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableSymmetricHashJoin: true,
						DefaultHashTableSize:    256,
					},
				}

				result := left.Join(right)
				symbols := result.Symbols()
				xi, ni, vi := query.SymbolIndex(symbols, leftSymbols[0]), query.SymbolIndex(symbols, leftSymbols[1]), query.SymbolIndex(symbols, rightSymbols[1])
				if xi < 0 || ni < 0 || vi < 0 {
					t.Fatalf("the join carries ?x, ?name and ?value; got %v", symbols)
				}
				it := result.Iterator()

				// Consume all and track peak. Each ?x is joined once, with the
				// name and the value its own tuples carry.
				count := 0
				var maxAlloc uint64
				for it.Next() {
					tuple := it.Tuple()
					x := tuple[xi].(int64)
					if seen[x] || tuple[ni] != leftTuples[x][1] || tuple[vi] != rightTuples[x][1] {
						t.Fatalf("tuple %v is not ?x %d joined once with its own name and value", tuple, x)
					}
					seen[x] = true
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
				if err := it.Error(); err != nil {
					t.Fatalf("iterate: %v", err)
				}
				if err := it.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}

				var memAfter runtime.MemStats
				runtime.ReadMemStats(&memAfter)

				peakIncrease := maxAlloc - memBefore.Alloc
				finalIncrease := memAfter.Alloc - memBefore.Alloc

				t.Logf("Symmetric (size %d):", size)
				t.Logf("  Peak increase:  %.2f MB", float64(peakIncrease)/1024/1024)
				t.Logf("  Final increase: %.2f MB", float64(finalIncrease)/1024/1024)
				t.Logf("  Results: %d", count)
				if count != size {
					t.Fatalf("the join answered %d tuples, want one per ?x, %d", count, size)
				}
			})
		})
	}
}
