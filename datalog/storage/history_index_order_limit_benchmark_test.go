package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog/executor"
)

func BenchmarkHistoryIndexOrderedLimit(b *testing.B) {
	benchmarkHistoryIndexOrderedLimit(b, false)
}

func BenchmarkHistoryIndexOrderedLimitScanCount(b *testing.B) {
	benchmarkHistoryIndexOrderedLimit(b, true)
}

func benchmarkHistoryIndexOrderedLimit(b *testing.B, countScans bool) {
	var capture *historyOrderScanCapture
	if countScans {
		capture = &historyOrderScanCapture{}
	}
	db := openHistoryOrderDatabase(b, capture)
	history := db.History()

	for _, limit := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("limit_%d", limit), func(b *testing.B) {
			queryText := historyOrderedLimitQuery(limit)
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
