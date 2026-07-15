//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// getDefaultExecutorOptions returns the default executor options for testing
func getDefaultExecutorOptions() executor.ExecutorOptions {
	opts := DefaultPlannerOptions()
	return executor.ExecutorOptions{
		EnableIteratorComposition: opts.EnableIteratorComposition,
		EnableTrueStreaming:       opts.EnableTrueStreaming,
		EnableStreamingJoins:      opts.EnableStreamingJoins,
		EnableSymmetricHashJoin:   opts.EnableSymmetricHashJoin,
		DefaultHashTableSize:      256,
	}
}

// BenchmarkDatomDecoding benchmarks the DatomFromKey function
func BenchmarkDatomDecoding(b *testing.B) {
	// Create a minimal store just for the encoder
	dir, _ := os.MkdirTemp("", "bench-decode-*")
	defer os.RemoveAll(dir)

	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Create a sample key
	encoder := store.Encoder()
	entity := datalog.NewIdentity("test-entity-1")
	attr := datalog.NewKeyword(":task/scenario")
	val := datalog.NewIdentity("scenario-1")
	datom := &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  val,
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 1},
	}
	key := encoder.EncodeKey(AVET, datom)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DatomFromKey(AVET, key, encoder, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHashJoinIteration benchmarks the hashJoinIterator path
func BenchmarkHashJoinIteration(b *testing.B) {
	dir, _ := os.MkdirTemp("", "bench-hashjoin-*")
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Add 100 tasks
	scenario1 := datalog.NewIdentity("scenario-1")
	tx := db.NewTransaction()
	for i := 0; i < 100; i++ {
		task := datalog.NewIdentity("task-" + string(rune('A'+i)))
		tx.Add(task, datalog.NewKeyword(":task/scenario"), scenario1)
	}
	_, _ = tx.Commit()

	// Warm up
	_, _ = executor.CollectTuples(db.Query(`[:find ?e :where [?e :task/scenario ?s]]`))

	matcher := db.Matcher().(*BadgerMatcher)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":task/scenario")},
			query.Variable{Name: datalog.NewSymbol("?scenario")},
		},
	}
	inputSymbols := []query.Symbol{datalog.NewSymbol("?scenario")}
	inputTuples := []executor.Tuple{{scenario1}}
	inputRel := executor.NewMaterializedRelationWithOptions(inputSymbols, inputTuples, getDefaultExecutorOptions())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, _ := matcher.Match(query.PatternQuery(pattern), executor.Relations{inputRel})
		it := result.Iterator()
		for it.Next() {
			_ = it.Tuple()
		}
		it.Close()
	}
}

// BenchmarkIteratorTupleAllocation measures per-tuple allocation in the iterator path.
// This exercises: KeyOnlyIterator.Next() → DatomFromKey() → BuildTupleInterned()
// The analysis shows each tuple currently allocates:
// - 80 bytes for *Datom in DatomFromKey (datom_decoder.go:43)
// - 64 bytes for Tuple slice in BuildTupleInterned (tuple_builder_interned.go:58)
// - 16 bytes for *ElementID in getTxPtr on cache miss (tuple_builder_interned.go:50)
func BenchmarkIteratorTupleAllocation(b *testing.B) {
	sizes := []int{10, 100, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("%d_tuples", size), func(b *testing.B) {
			dir := b.TempDir()
			db, err := NewDatabase(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Create entities
			tx := db.NewTransaction()
			for i := 0; i < size; i++ {
				e := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
				tx.Add(e, datalog.NewKeyword(":entity/value"), int64(i))
			}
			_, _ = tx.Commit()

			matcher := db.Matcher().(*BadgerMatcher)
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":entity/value")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			}

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result, _ := matcher.Match(query.PatternQuery(pattern), nil)
				count := 0
				it := result.Iterator()
				for it.Next() {
					_ = it.Tuple()
					count++
				}
				it.Close()
				if count != size {
					b.Fatalf("expected %d, got %d", size, count)
				}
			}
		})
	}
}

// BenchmarkDatomFromKeyToTuple measures the full decode path:
// encoded key → DatomFromKey → BuildTupleInterned
// This is the hot path for all storage-backed iteration.
func BenchmarkDatomFromKeyToTuple(b *testing.B) {
	dir := b.TempDir()
	store, err := NewBadgerStore(dir, &BinaryKeyEncoder{})
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Create a sample key
	encoder := store.Encoder()
	entity := datalog.NewIdentity("test-entity-1")
	attr := datalog.NewKeyword(":test/attribute")
	datom := &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  "test value",
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 1},
	}
	key := encoder.EncodeKey(EAVT, datom)

	// Setup tuple builder
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}
	symbols := []query.Symbol{datalog.NewSymbol("?e"), datalog.NewSymbol("?v")}
	tupleBuilder := query.NewInternedTupleBuilder(pattern, symbols)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// This is the hot path in iterator.Next()
		d, err := DatomFromKey(EAVT, key, encoder, nil)
		if err != nil {
			b.Fatal(err)
		}
		_ = tupleBuilder.BuildTupleInterned(&d)
	}
}
