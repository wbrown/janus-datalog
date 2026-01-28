package storage

import (
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

	store, err := NewBadgerStore(dir, NewKeyEncoder(BinaryStrategy))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Create a sample key
	encoder := store.encoder
	entity := datalog.NewIdentity("test-entity-1")
	attr := datalog.NewKeyword(":task/scenario")
	val := datalog.NewIdentity("scenario-1")
	datom := &datalog.Datom{
		E:  entity,
		A:  attr,
		V:  val,
		Tx: 12345,
	}
	key := encoder.EncodeKey(AVET, datom)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := DatomFromKey(AVET, key, encoder)
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
	_, _ = db.ExecuteQuery(`[:find ?e :where [?e :task/scenario ?s]]`)

	matcher := db.Matcher().(*BadgerMatcher)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":task/scenario")},
			query.Variable{Name: datalog.NewSymbol("?scenario")},
		},
	}
	inputCols := []query.Symbol{datalog.NewSymbol("?scenario")}
	inputTuples := []executor.Tuple{{scenario1}}
	inputRel := executor.NewMaterializedRelationWithOptions(inputCols, inputTuples, getDefaultExecutorOptions())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, _ := matcher.Match(pattern, executor.Relations{inputRel})
		it := result.Iterator()
		for it.Next() {
			_ = it.Tuple()
		}
		it.Close()
	}
}
