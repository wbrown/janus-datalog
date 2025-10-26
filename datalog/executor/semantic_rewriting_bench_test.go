package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkSemanticRewriting compares query performance with and without semantic rewriting
// on a realistic OHLC dataset with year-based filtering
func BenchmarkSemanticRewritingYearFilter(b *testing.B) {
	// Generate test data: 3 years of daily OHLC bars
	var datoms []datalog.Datom

	symbolID := datalog.NewIdentity("symbol-NVDA")
	datoms = append(datoms, datalog.Datom{
		E: symbolID, A: datalog.NewKeyword(":symbol/ticker"), V: "NVDA", Tx: 1,
	})

	loc := time.UTC
	barID := 0

	// Generate 3 years × 250 trading days = 750 bars
	for year := 2023; year <= 2025; year++ {
		for day := 1; day <= 250; day++ {
			barTime := time.Date(year, 1, 1, 0, 0, 0, 0, loc).AddDate(0, 0, day)
			barEntity := datalog.NewIdentity("bar-" + string(rune('0'+barID/100)) + string(rune('0'+(barID/10)%10)) + string(rune('0'+barID%10)))
			barID++

			open := float64(100 + year - 2023 + day%10)

			datoms = append(datoms,
				datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: 1},
				datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
				datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/open"), V: open, Tx: 1},
			)
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query that filters by year
	queryStr := `[:find ?time ?open
	             :where
	               [?s :symbol/ticker "NVDA"]
	               [?b :price/symbol ?s]
	               [?b :price/time ?time]
	               [(year ?time) ?y]
	               [(= ?y 2024)]
	               [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Parse error: %v", err)
	}

	// Benchmark WITHOUT semantic rewriting
	b.Run("WithoutRewriting", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSemanticRewriting: false,
			EnableFineGrainedPhases: true,
			MaxPhases:               10,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execution failed: %v", err)
			}
			if result.Size() != 250 {
				b.Fatalf("Expected 250 results, got %d", result.Size())
			}
		}
	})

	// Benchmark WITH semantic rewriting
	b.Run("WithRewriting", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSemanticRewriting: true,
			EnableFineGrainedPhases: true,
			MaxPhases:               10,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execution failed: %v", err)
			}
			if result.Size() != 250 {
				b.Fatalf("Expected 250 results, got %d", result.Size())
			}
		}
	})
}

// BenchmarkSemanticRewritingMultiComponent benchmarks performance with multiple time constraints
func BenchmarkSemanticRewritingMultiComponent(b *testing.B) {
	// Generate test data: 3 days × 24 hours × 12 5-min bars = 864 bars
	var datoms []datalog.Datom

	symbolID := datalog.NewIdentity("symbol-AAPL")
	datoms = append(datoms, datalog.Datom{
		E: symbolID, A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1,
	})

	loc := time.UTC
	barID := 0

	for day := 19; day <= 21; day++ {
		for hour := 0; hour < 24; hour++ {
			for minute := 0; minute < 60; minute += 5 {
				barTime := time.Date(2025, 6, day, hour, minute, 0, 0, loc)
				barEntity := datalog.NewIdentity("bar-" + string(rune('0'+barID/100)) + string(rune('0'+(barID/10)%10)) + string(rune('0'+barID%10)))
				barID++

				open := float64(150 + day + hour + minute)

				datoms = append(datoms,
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/open"), V: open, Tx: 1},
				)
			}
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query with year, month, day, and hour constraints
	queryStr := `[:find ?time ?open
	             :where
	               [?s :symbol/ticker "AAPL"]
	               [?b :price/symbol ?s]
	               [?b :price/time ?time]
	               [(year ?time) ?y]
	               [(month ?time) ?m]
	               [(day ?time) ?d]
	               [(hour ?time) ?h]
	               [(= ?y 2025)]
	               [(= ?m 6)]
	               [(= ?d 20)]
	               [(= ?h 10)]
	               [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Parse error: %v", err)
	}

	// Benchmark WITHOUT semantic rewriting
	b.Run("WithoutRewriting", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSemanticRewriting: false,
			EnableFineGrainedPhases: true,
			MaxPhases:               10,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execution failed: %v", err)
			}
			if result.Size() != 12 {
				b.Fatalf("Expected 12 results, got %d", result.Size())
			}
		}
	})

	// Benchmark WITH semantic rewriting
	b.Run("WithRewriting", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSemanticRewriting: true,
			EnableFineGrainedPhases: true,
			MaxPhases:               10,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execution failed: %v", err)
			}
			if result.Size() != 12 {
				b.Fatalf("Expected 12 results, got %d", result.Size())
			}
		}
	})
}

// BenchmarkSemanticRewritingOHLCScale tests performance at the scale of the OHLC query
// This mirrors the decorr_ohlc_scale_test.go dataset for comparison
func BenchmarkSemanticRewritingOHLCScale(b *testing.B) {
	// Generate 8 trading days of 5-minute bar data (same as decorr scale test)
	var datoms []datalog.Datom

	symbolID := datalog.NewIdentity("symbol-CRWV")
	datoms = append(datoms, datalog.Datom{
		E: symbolID, A: datalog.NewKeyword(":symbol/ticker"), V: "CRWV", Tx: 1,
	})

	loc := time.UTC
	startDate := time.Date(2025, 6, 1, 0, 0, 0, 0, loc)

	barID := 0

	// Generate 8 trading days
	for day := 0; day < 8; day++ {
		currentDate := startDate.AddDate(0, 0, day)

		// Trading hours: 9:30 AM to 4:00 PM
		for hour := 9; hour <= 15; hour++ {
			startMinute := 0
			endMinute := 60

			if hour == 9 {
				startMinute = 30
			}
			if hour == 15 {
				endMinute = 60
			}

			for minute := startMinute; minute < endMinute; minute += 5 {
				barTime := time.Date(
					currentDate.Year(), currentDate.Month(), currentDate.Day(),
					hour, minute, 0, 0, loc,
				)

				barEntity := datalog.NewIdentity("bar-" + string(rune('0'+barID/1000)) +
					string(rune('0'+(barID/100)%10)) +
					string(rune('0'+(barID/10)%10)) +
					string(rune('0'+barID%10)))
				barID++

				open := 100.0 + float64(day)*0.5 + float64(hour-9)*0.2

				datoms = append(datoms,
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/open"), V: open, Tx: 1},
				)
			}
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Simple year+month+day filter query
	queryStr := `[:find ?time ?open
	             :where
	               [?s :symbol/ticker "CRWV"]
	               [?b :price/symbol ?s]
	               [?b :price/time ?time]
	               [(year ?time) ?y]
	               [(month ?time) ?m]
	               [(day ?time) ?d]
	               [(= ?y 2025)]
	               [(= ?m 6)]
	               [(= ?d 5)]
	               [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Parse error: %v", err)
	}

	// Benchmark WITHOUT semantic rewriting
	b.Run("WithoutRewriting", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSemanticRewriting: false,
			EnableFineGrainedPhases: true,
			MaxPhases:               10,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execution failed: %v", err)
			}
			// Should get 1 day of bars
			if result.Size() == 0 {
				b.Fatalf("Expected non-zero results")
			}
		}
	})

	// Benchmark WITH semantic rewriting
	b.Run("WithRewriting", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSemanticRewriting: true,
			EnableFineGrainedPhases: true,
			MaxPhases:               10,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Execution failed: %v", err)
			}
			// Should get 1 day of bars
			if result.Size() == 0 {
				b.Fatalf("Expected non-zero results")
			}
		}
	})
}
