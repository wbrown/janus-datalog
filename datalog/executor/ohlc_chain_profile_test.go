package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// BenchmarkOHLCFullChain benchmarks the complete execution chain for OHLC queries
// with time range optimization enabled. This is designed to be profiled with:
//
//	go test -bench=BenchmarkOHLCFullChain -cpuprofile=cpu.prof -memprofile=mem.prof -benchtime=5s
func BenchmarkOHLCFullChain(b *testing.B) {
	benchmarks := []struct {
		name     string
		numDays  int
		numHours int
	}{
		{"Daily_22days", 22, 0},
		{"Hourly_260hours", 0, 260},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Create test data
			datoms := createOHLCData(bm.numDays, bm.numHours)
			matcher := NewMemoryPatternMatcher(datoms)
			exec := NewExecutor(matcher)

			// Build the query
			var queryStr string
			if bm.numDays > 0 {
				// Daily OHLC query with 4 decorrelated subqueries
				queryStr = `
				[:find ?day ?open ?high ?low ?close
				 :where
				 [?s :symbol/ticker "TEST"]
				 [?b :price/symbol ?s]
				 [?b :price/time ?t]
				 [(day ?t) ?day]

				 [(q [:find (min ?o)
				      :in $ ?sym ?d
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(= ?bd ?d)]
				      [?bar :price/open ?o]]
				     $ ?s ?day) [[?open]]]

				 [(q [:find (max ?h)
				      :in $ ?sym ?d
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(= ?bd ?d)]
				      [?bar :price/high ?h]]
				     $ ?s ?day) [[?high]]]

				 [(q [:find (min ?l)
				      :in $ ?sym ?d
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(= ?bd ?d)]
				      [?bar :price/low ?l]]
				     $ ?s ?day) [[?low]]]

				 [(q [:find (max ?c)
				      :in $ ?sym ?d
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(= ?bd ?d)]
				      [?bar :price/close ?c]]
				     $ ?s ?day) [[?close]]]]`
			} else {
				// Hourly OHLC query with 4 decorrelated subqueries
				queryStr = `
				[:find ?day ?hour ?open ?high ?low ?close
				 :where
				 [?s :symbol/ticker "TEST"]
				 [?b :price/symbol ?s]
				 [?b :price/time ?t]
				 [(day ?t) ?day]
				 [(hour ?t) ?hour]

				 [(q [:find (min ?o)
				      :in $ ?sym ?d ?h
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(hour ?time) ?bh]
				      [(= ?bd ?d)]
				      [(= ?bh ?h)]
				      [?bar :price/open ?o]]
				     $ ?s ?day ?hour) [[?open]]]

				 [(q [:find (max ?hv)
				      :in $ ?sym ?d ?h
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(hour ?time) ?bh]
				      [(= ?bd ?d)]
				      [(= ?bh ?h)]
				      [?bar :price/high ?hv]]
				     $ ?s ?day ?hour) [[?high]]]

				 [(q [:find (min ?l)
				      :in $ ?sym ?d ?h
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(hour ?time) ?bh]
				      [(= ?bd ?d)]
				      [(= ?bh ?h)]
				      [?bar :price/low ?l]]
				     $ ?s ?day ?hour) [[?low]]]

				 [(q [:find (max ?c)
				      :in $ ?sym ?d ?h
				      :where
				      [?bar :price/symbol ?sym]
				      [?bar :price/time ?time]
				      [(day ?time) ?bd]
				      [(hour ?time) ?bh]
				      [(= ?bd ?d)]
				      [(= ?bh ?h)]
				      [?bar :price/close ?c]]
				     $ ?s ?day ?hour) [[?close]]]]`
			}

			q, err := parser.ParseQuery(queryStr)
			if err != nil {
				b.Fatalf("Failed to parse query: %v", err)
			}

			// Reset timer and run benchmark
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query execution failed: %v", err)
				}
			}
		})
	}
}

// createOHLCData generates realistic OHLC test data
func createOHLCData(numDays, numHours int) []datalog.Datom {
	datoms := []datalog.Datom{}
	symbolID := datalog.NewIdentity("symbol:TEST")

	// Add the symbol
	datoms = append(datoms, datalog.Datom{
		E:  symbolID,
		A:  datalog.NewKeyword(":symbol/ticker"),
		V:  "TEST",
		Tx: 1,
	})

	barID := 1000

	if numDays > 0 {
		// Generate daily data - 100 bars per day
		startDate := time.Date(2025, 6, 1, 9, 30, 0, 0, time.UTC)
		for day := 0; day < numDays; day++ {
			dayTime := startDate.AddDate(0, 0, day)
			for minute := 0; minute < 100; minute++ {
				currentBarID := datalog.NewIdentity(fmt.Sprintf("bar:%d", barID))
				barTime := dayTime.Add(time.Duration(minute) * time.Minute)

				datoms = append(datoms,
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/open"), V: 100.0 + float64(day) + float64(minute)*0.1, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/high"), V: 102.0 + float64(day) + float64(minute)*0.2, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/low"), V: 98.0 + float64(day) - float64(minute)*0.1, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/close"), V: 101.0 + float64(day) + float64(minute)*0.15, Tx: uint64(barID)},
				)
				barID++
			}
		}
	} else {
		// Generate hourly data - 10 bars per hour
		startTime := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
		for hour := 0; hour < numHours; hour++ {
			hourTime := startTime.Add(time.Duration(hour) * time.Hour)
			for minute := 0; minute < 10; minute++ {
				currentBarID := datalog.NewIdentity(fmt.Sprintf("bar:%d", barID))
				barTime := hourTime.Add(time.Duration(minute) * time.Minute)

				datoms = append(datoms,
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/open"), V: 100.0 + float64(hour) + float64(minute)*0.1, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/high"), V: 102.0 + float64(hour) + float64(minute)*0.2, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/low"), V: 98.0 + float64(hour) - float64(minute)*0.1, Tx: uint64(barID)},
					datalog.Datom{E: currentBarID, A: datalog.NewKeyword(":price/close"), V: 101.0 + float64(hour) + float64(minute)*0.15, Tx: uint64(barID)},
				)
				barID++
			}
		}
	}

	return datoms
}

// BenchmarkMetadataPropagation specifically profiles the metadata propagation overhead
func BenchmarkMetadataPropagation(b *testing.B) {
	// Create a context and benchmark Set/Get metadata operations
	ctx := NewContext(nil)

	// Create sample time ranges
	ranges := make([]TimeRange, 260)
	startTime := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 260; i++ {
		ranges[i] = TimeRange{
			Start: startTime.Add(time.Duration(i) * time.Hour),
			End:   startTime.Add(time.Duration(i+1) * time.Hour),
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

	b.Run("GetAndTypeAssert", func(b *testing.B) {
		ctx.SetMetadata("time_ranges", ranges)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if val, ok := ctx.GetMetadata("time_ranges"); ok {
				_ = val.([]TimeRange)
			}
		}
	})
}

// BenchmarkTimeExtractionFunctions profiles time extraction function performance
func BenchmarkTimeExtractionFunctions(b *testing.B) {
	// Create test data
	datoms := createOHLCData(0, 260)
	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	// Query with multiple time extractions
	queryStr := `
	[:find ?d ?h ?m ?open
	 :where
	 [?s :symbol/ticker "TEST"]
	 [?b :price/symbol ?s]
	 [?b :price/time ?t]
	 [(day ?t) ?d]
	 [(hour ?t) ?h]
	 [(minute ?t) ?m]
	 [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(q)
		if err != nil {
			b.Fatalf("Query execution failed: %v", err)
		}
	}
}
