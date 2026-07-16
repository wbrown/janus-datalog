//go:build !(js && wasm)

package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

func BenchmarkReadyPredicateScheduling(b *testing.B) {
	options := DefaultPlannerOptions()
	options.EnableAlgebraOptimizer = false
	options.EnableAttributeFetchFusion = false
	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:           b.TempDir(),
		PlannerOptions: &options,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	scoreAttr := datalog.NewKeyword(":item/score")
	payloadAttr := datalog.NewKeyword(":item/payload")
	tx := db.NewTransaction()
	for i := 0; i < 10_000; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("ready-filter-%d", i))
		if err := tx.Add(entity, scoreAttr, int64(i)); err != nil {
			b.Fatal(err)
		}
		if err := tx.Add(entity, payloadAttr, fmt.Sprintf("payload-%d", i)); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	queryText := `[:find ?e ?payload
	              :where
	              [?e :item/score ?score]
	              [?e :item/payload ?payload]
	              [(> ?score 9900)]]`
	warm, err := db.Query(queryText)
	if err != nil {
		b.Fatal(err)
	}
	warmRows, err := executor.CollectTuples(warm, nil)
	if err != nil {
		b.Fatal(err)
	}
	if len(warmRows) != 99 {
		b.Fatalf("warmup got %d rows, want 99", len(warmRows))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := db.Query(queryText)
		if err != nil {
			b.Fatal(err)
		}
		rows, err := executor.CollectTuples(result, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) != 99 {
			b.Fatalf("got %d rows, want 99", len(rows))
		}
	}
}
