package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func BenchmarkIndexOrderedLimit(b *testing.B) {
	benchmarkIndexOrderedLimit(b, false)
}

func BenchmarkIndexOrderedLimitScanCount(b *testing.B) {
	benchmarkIndexOrderedLimit(b, true)
}

func benchmarkIndexOrderedLimit(b *testing.B, countScans bool) {
	const entityCount = 10_000

	nameAttr := datalog.NewKeyword(":person/name")
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       nameAttr,
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})

	var scanned atomic.Int64
	var handler annotations.Handler
	if countScans {
		handler = func(event annotations.Event) {
			if event.Name != "pattern/storage-scan" {
				return
			}
			// Intake: what the benchmark reports is scan volume, not the row
			// count the query returned.
			if count, ok := event.Data["datoms.scanned"].(int); ok {
				scanned.Add(int64(count))
			}
		}
	}

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:              b.TempDir(),
		Schema:            s,
		AnnotationHandler: handler,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const batchSize = 500
	for start := 0; start < entityCount; start += batchSize {
		tx := db.NewTransaction()
		for i := start; i < start+batchSize; i++ {
			entity := datalog.NewIdentity(fmt.Sprintf("ordered-person:%d", i))
			if err := tx.Set(entity, nameAttr, fmt.Sprintf("Person %d", i)); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}

	for _, direction := range []string{"asc", "desc"} {
		for _, limit := range []int{1, 10, 100} {
			queryText := fmt.Sprintf(
				`[:find ?e :where [?e :person/name ?name] :order-by [[?e :%s]] :limit %d]`,
				direction,
				limit,
			)
			name := fmt.Sprintf("%s/limit_%d", direction, limit)
			b.Run(name, func(b *testing.B) {
				warm, err := db.Query(queryText)
				if err != nil {
					b.Fatal(err)
				}
				warmRows, err := executor.CollectTuples(warm, nil)
				if err != nil {
					b.Fatal(err)
				}
				if len(warmRows) != limit {
					b.Fatalf("warmup got %d rows, want %d", len(warmRows), limit)
				}

				scanned.Store(0)
				operations := int64(0)
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
					if len(rows) != limit {
						b.Fatalf("got %d rows, want %d", len(rows), limit)
					}
					operations++
				}
				b.StopTimer()
				if countScans && operations > 0 {
					b.ReportMetric(float64(scanned.Load())/float64(operations), "datoms/op")
				}
			})
		}
	}
}
