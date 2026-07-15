//go:build !(js && wasm)

package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/executor"
)

func BenchmarkHistoryIndexOrderedLimit(b *testing.B) {
	benchmarkHistoryOrderedLimitQuery(b, false, historyOrderedLimitQuery)
}

func BenchmarkHistoryIndexOrderedLimitScanCount(b *testing.B) {
	benchmarkHistoryOrderedLimitQuery(b, true, historyOrderedLimitQuery)
}

func BenchmarkHistoryTAEVOrderedLimit(b *testing.B) {
	benchmarkHistoryOrderedLimitQuery(b, false, historyTransactionOrderedLimitQuery)
}

func BenchmarkHistoryTAEVOrderedLimitScanCount(b *testing.B) {
	benchmarkHistoryOrderedLimitQuery(b, true, historyTransactionOrderedLimitQuery)
}

func BenchmarkHistoryAETVOrderedLimit(b *testing.B) {
	benchmarkHistoryOrderedLimitQuery(b, false, historyEntityOrderedLimitQuery)
}

func BenchmarkHistoryAETVOrderedLimitScanCount(b *testing.B) {
	benchmarkHistoryOrderedLimitQuery(b, true, historyEntityOrderedLimitQuery)
}

func BenchmarkHistoryEATVOrderedLimit(b *testing.B) {
	benchmarkHistoryEATVOrderedLimit(b, false)
}

func BenchmarkHistoryEATVOrderedLimitScanCount(b *testing.B) {
	benchmarkHistoryEATVOrderedLimit(b, true)
}

func benchmarkHistoryOrderedLimitQuery(
	b *testing.B,
	countScans bool,
	queryForLimit func(int) string,
) {
	var capture *historyOrderScanCapture
	if countScans {
		capture = &historyOrderScanCapture{}
	}
	db := openHistoryOrderDatabase(b, capture)
	history := db.History()

	for _, limit := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("limit_%d", limit), func(b *testing.B) {
			queryText := queryForLimit(limit)
			warm, err := history.Query(queryText)
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

			if capture != nil {
				capture.reset()
			}
			operations := 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := history.Query(queryText)
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
			if capture != nil && operations > 0 {
				scanned, _ := capture.snapshot()
				b.ReportMetric(float64(scanned)/float64(operations), "datoms/op")
			}
		})
	}
}

func benchmarkHistoryEATVOrderedLimit(b *testing.B, countScans bool) {
	var capture *historyOrderScanCapture
	if countScans {
		capture = &historyOrderScanCapture{}
	}
	db, entity := openHistoryEntityOrderDatabase(b, capture)
	history := db.History()

	for _, limit := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("limit_%d", limit), func(b *testing.B) {
			queryText := historyAttributeOrderedLimitQuery(entity, limit)
			warm, err := history.Query(queryText)
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

			if capture != nil {
				capture.reset()
			}
			operations := 0
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := history.Query(queryText)
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
			if capture != nil && operations > 0 {
				scanned, _ := capture.snapshot()
				b.ReportMetric(float64(scanned)/float64(operations), "datoms/op")
			}
		})
	}
}
