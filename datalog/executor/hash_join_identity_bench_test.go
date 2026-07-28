package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkHashJoinIdentityKeys mirrors BenchmarkHashJoinInputTypes but uses
// datalog.Identity values as the join key instead of int64. This exercises
// code paths that the int64-keyed benchmarks do not:
//
//   - hashValue's datalog.Identity branch (tuple_key.go:73-78, 98-102)
//     runs an FNV-1a loop over the full 20-byte SHA1 hash.
//   - ValuesEqual's interned-Identity branch goes through type-assertion +
//     Equal() rather than short-circuiting on the cheap interface
//     pointer-equality of a primitive int64.
//
// Real datalog queries join on entity references (Identity) far more often
// than on integers, so this benchmark is closer to production join shape.
func BenchmarkHashJoinIdentityKeys(b *testing.B) {
	sizes := []int{100, 1000, 5000}

	for _, size := range sizes {
		// Build the Identity values once outside the b.N loop. Interning means
		// the same string returns the same pointer; both relations share the
		// same Identity instances on the join symbol.
		ids := make([]datalog.Identity, size)
		for i := 0; i < size; i++ {
			ids[i] = datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		}

		leftSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?name")}
		rightSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?value")}

		leftTuples := make([]Tuple, size)
		rightTuples := make([]Tuple, size)
		for i := 0; i < size; i++ {
			leftTuples[i] = Tuple{ids[i], fmt.Sprintf("name%d", i)}
			rightTuples[i] = Tuple{ids[i], fmt.Sprintf("value%d", i)}
		}

		// Case 1: Both Materialized
		b.Run(fmt.Sprintf("mat_x_mat/size_%d", size), func(b *testing.B) {
			left := NewMaterializedRelation(leftSymbols, leftTuples)
			right := NewMaterializedRelation(rightSymbols, rightTuples)

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

		// Case 2: Streaming Left x Materialized Right
		b.Run(fmt.Sprintf("stream_x_mat/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					symbols:  leftSymbols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options:  ExecutorOptions{EnableStreamingJoins: true},
				}
				right := NewMaterializedRelation(rightSymbols, rightTuples)

				result := left.Join(right)
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
				it.Close()
			}
		})

		// Case 3: Materialized Left x Streaming Right
		b.Run(fmt.Sprintf("mat_x_stream/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := NewMaterializedRelation(leftSymbols, leftTuples)
				right := &StreamingRelation{
					symbols:  rightSymbols,
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

		// Case 4: Both Streaming
		b.Run(fmt.Sprintf("stream_x_stream/size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				left := &StreamingRelation{
					symbols:  leftSymbols,
					iterator: &sliceIterator{tuples: leftTuples, pos: -1},
					size:     -1,
					options: ExecutorOptions{
						EnableStreamingJoins: true,
						DefaultHashTableSize: 256,
					},
				}
				right := &StreamingRelation{
					symbols:  rightSymbols,
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

// BenchmarkHashJoinIdentityHighFanout exercises a realistic cardinality-many
// join shape: K distinct entities, each appearing M times on the left
// (different payloads) and M times on the right (different payloads). The
// join on ?e produces K * M * M result tuples.
//
// Stresses:
//   - Same-key Put updates in TupleKeyMap during build accumulation. Each
//     Get + Put after the first hits an existing bucket entry, calling
//     ValuesEqual twice (finding #2 path).
//   - High result-tuple volume drives combineTuples, seen dedup with many
//     distinct tuples, and tuple-copy paths.
//
// This shape is closer to real datalog joins (person x posts, account x
// transactions) than the 1:1 cardinality of BenchmarkHashJoinIdentityKeys.
func BenchmarkHashJoinIdentityHighFanout(b *testing.B) {
	shapes := []struct {
		name string
		keys int
		m    int
	}{
		{"keys100/fanout10", 100, 10}, // 10k result tuples
		{"keys100/fanout50", 100, 50}, // 250k result tuples
		{"keys500/fanout20", 500, 20}, // 200k result tuples
	}

	for _, sh := range shapes {
		b.Run(sh.name, func(b *testing.B) {
			ids := make([]datalog.Identity, sh.keys)
			for i := 0; i < sh.keys; i++ {
				ids[i] = datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
			}

			leftSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?attr_l")}
			rightSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?attr_r")}

			leftTuples := make([]Tuple, 0, sh.keys*sh.m)
			rightTuples := make([]Tuple, 0, sh.keys*sh.m)
			for i := 0; i < sh.keys; i++ {
				for j := 0; j < sh.m; j++ {
					leftTuples = append(leftTuples, Tuple{ids[i], fmt.Sprintf("L-%d-%d", i, j)})
					rightTuples = append(rightTuples, Tuple{ids[i], fmt.Sprintf("R-%d-%d", i, j)})
				}
			}

			left := NewMaterializedRelation(leftSymbols, leftTuples)
			right := NewMaterializedRelation(rightSymbols, rightTuples)

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
	}
}

// BenchmarkHashJoinIdentityDuplicates engineers a high-duplicate-rate join
// to stress the seen dedup map. K distinct entities, each repeated R times
// on each side with SHARED payload within each key. After join, only K
// unique result tuples exist; seen rejects (K * R * R - K) duplicates.
//
// Inputs are constructed as raw struct literals to preserve the repeated
// input tuples — a deliberate relational-invariant violation no constructor
// admits (every constructor either deduplicates or warrants set-ness).
//
// Stresses:
//   - it.seen.PutIfAbsent() with a non-empty bucket on the rejection path:
//     one ValuesEqual call per rejected duplicate.
func BenchmarkHashJoinIdentityDuplicates(b *testing.B) {
	shapes := []struct {
		name string
		keys int
		reps int
	}{
		{"keys10/reps100", 10, 100}, // 10*100*100 = 100k raw, 10 unique
		{"keys50/reps50", 50, 50},   // 50*50*50  = 125k raw, 50 unique
	}

	for _, sh := range shapes {
		b.Run(sh.name, func(b *testing.B) {
			ids := make([]datalog.Identity, sh.keys)
			for i := 0; i < sh.keys; i++ {
				ids[i] = datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
			}

			leftSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?attr_l")}
			rightSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?attr_r")}

			// SHARED payload per key — every repetition of a given key
			// carries the same value, so result tuples collapse to K unique
			// tuples after dedup. Raw struct construction keeps input
			// duplicates intact for the join.
			leftTuples := make([]Tuple, 0, sh.keys*sh.reps)
			rightTuples := make([]Tuple, 0, sh.keys*sh.reps)
			for i := 0; i < sh.keys; i++ {
				lPayload := fmt.Sprintf("L-%d", i)
				rPayload := fmt.Sprintf("R-%d", i)
				for j := 0; j < sh.reps; j++ {
					leftTuples = append(leftTuples, Tuple{ids[i], lPayload})
					rightTuples = append(rightTuples, Tuple{ids[i], rPayload})
				}
			}

			left := &MaterializedRelation{symbols: leftSymbols, tuples: leftTuples}
			right := &MaterializedRelation{symbols: rightSymbols, tuples: rightTuples}

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
	}
}

// BenchmarkHashJoinIdentityLargeResult is the Identity-keyed analogue of
// BenchmarkHashJoinLargeResult. Used as the high-signal profile target where
// application-level symbols clear scheduler noise.
func BenchmarkHashJoinIdentityLargeResult(b *testing.B) {
	sizes := []int{10000, 50000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			ids := make([]datalog.Identity, size)
			for i := 0; i < size; i++ {
				ids[i] = datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
			}

			leftSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?data")}
			rightSymbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?value")}

			leftTuples := make([]Tuple, size)
			rightTuples := make([]Tuple, size)
			for i := 0; i < size; i++ {
				leftTuples[i] = Tuple{ids[i], fmt.Sprintf("data_%d", i)}
				rightTuples[i] = Tuple{ids[i], int64(i * 100)}
			}

			left := NewMaterializedRelation(leftSymbols, leftTuples)
			right := NewMaterializedRelation(rightSymbols, rightTuples)

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
	}
}
