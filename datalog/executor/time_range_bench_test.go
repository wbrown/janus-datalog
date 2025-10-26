package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkExtractTimeRanges benchmarks the time range extraction with various sizes
func BenchmarkExtractTimeRanges(b *testing.B) {
	benchmarks := []struct {
		name  string
		size  int
		daily bool
	}{
		{"daily_22_ranges", 22, true},
		{"daily_100_ranges", 100, true},
		{"hourly_260_ranges", 260, false},
		{"hourly_1000_ranges", 1000, false},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Create input relation with specified size
			var inputTuples []Tuple
			startDate := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)

			if bm.daily {
				// Daily ranges: year, month, day only
				for i := 0; i < bm.size; i++ {
					dt := startDate.AddDate(0, 0, i)
					inputTuples = append(inputTuples, Tuple{
						int64(dt.Year()),
						int64(dt.Month()),
						int64(dt.Day()),
					})
				}
			} else {
				// Hourly ranges: year, month, day, hour
				for i := 0; i < bm.size; i++ {
					dt := startDate.Add(time.Duration(i) * time.Hour)
					inputTuples = append(inputTuples, Tuple{
						int64(dt.Year()),
						int64(dt.Month()),
						int64(dt.Day()),
						int64(dt.Hour()),
					})
				}
			}

			var columns []query.Symbol
			var correlationKeys []query.Symbol
			if bm.daily {
				columns = []query.Symbol{"?year", "?month", "?day"}
				correlationKeys = []query.Symbol{"$", "?s", "?year", "?month", "?day"}
			} else {
				columns = []query.Symbol{"?year", "?month", "?day", "?hour"}
				correlationKeys = []query.Symbol{"$", "?s", "?year", "?month", "?day", "?hour"}
			}

			inputRel := NewMaterializedRelation(columns, inputTuples)

			// Reset timer before actual benchmark
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := extractTimeRanges(inputRel, correlationKeys)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMetadataOperations benchmarks the metadata set/get operations
func BenchmarkMetadataOperations(b *testing.B) {
	ctx := NewContext(nil)

	// Create sample time ranges
	ranges := make([]TimeRange, 260)
	start := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
	for i := range ranges {
		ranges[i] = TimeRange{
			Start: start.Add(time.Duration(i) * time.Hour),
			End:   start.Add(time.Duration(i+1) * time.Hour),
		}
	}

	b.Run("SetMetadata", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ctx.SetMetadata("time_ranges", ranges)
		}
	})

	b.Run("GetMetadata", func(b *testing.B) {
		ctx.SetMetadata("time_ranges", ranges)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = ctx.GetMetadata("time_ranges")
		}
	})

	b.Run("GetMetadata_with_type_assertions", func(b *testing.B) {
		ctx.SetMetadata("time_ranges", ranges)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if rangesVal, ok := ctx.GetMetadata("time_ranges"); ok {
				if _, ok := rangesVal.([]TimeRange); ok {
					// Simulates actual usage in executor
				}
			}
		}
	})
}
