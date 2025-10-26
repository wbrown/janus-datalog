package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkSubqueryExecution compares legacy vs componentized subquery execution
// This benchmark measures the memory and performance impact of the new componentized path
func BenchmarkSubqueryExecution(b *testing.B) {
	// Create test data: a symbol with price bars for multiple days
	datoms := []datalog.Datom{}
	symbolID := datalog.NewIdentity("symbol:TEST")

	// Add the symbol
	datoms = append(datoms, datalog.Datom{
		E:  symbolID,
		A:  datalog.NewKeyword(":symbol/ticker"),
		V:  "TEST",
		Tx: 1,
	})

	// Create price bars: 10 days, 100 bars per day = 1000 bars total
	// This creates enough data to see memory differences
	barID := 1000
	for day := 1; day <= 10; day++ {
		dayTime := time.Date(2025, 1, day, 9, 30, 0, 0, time.UTC)

		for minute := 0; minute < 100; minute++ {
			currentBarID := datalog.NewIdentity(fmt.Sprintf("bar:%d", barID))
			barTime := dayTime.Add(time.Duration(minute) * time.Minute)

			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/symbol"),
				V:  symbolID,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/time"),
				V:  barTime,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/minute-of-day"),
				V:  int64(570 + minute),
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/open"),
				V:  100.0 + float64(day) + float64(minute)*0.1,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/high"),
				V:  102.0 + float64(day) + float64(minute)*0.2,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/low"),
				V:  98.0 + float64(day) - float64(minute)*0.1,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/close"),
				V:  101.0 + float64(day) + float64(minute)*0.05,
				Tx: uint64(barID),
			})

			barID++
		}
	}

	// Query with 4 subqueries (like the OHLC query)
	// Each subquery scans the same data with different aggregations
	queryStr := `
	[:find ?day ?open ?high ?low ?close
	 :where
	 [?s :symbol/ticker "TEST"]
	 [?morning :price/symbol ?s]
	 [?morning :price/minute-of-day 570]
	 [?morning :price/time ?t]
	 [(day ?t) ?day]

	 [(q [:find (min ?o)
	      :in $ ?sym ?d
	      :where
	      [?b :price/symbol ?sym]
	      [?b :price/time ?time]
	      [(day ?time) ?bd]
	      [(= ?bd ?d)]
	      [?b :price/open ?o]]
	     $ ?s ?day) [[?open]]]

	 [(q [:find (max ?h)
	      :in $ ?sym ?d
	      :where
	      [?b :price/symbol ?sym]
	      [?b :price/time ?time]
	      [(day ?time) ?bd]
	      [(= ?bd ?d)]
	      [?b :price/high ?h]]
	     $ ?s ?day) [[?high]]]

	 [(q [:find (min ?l)
	      :in $ ?sym ?d
	      :where
	      [?b :price/symbol ?sym]
	      [?b :price/time ?time]
	      [(day ?time) ?bd]
	      [(= ?bd ?d)]
	      [?b :price/low ?l]]
	     $ ?s ?day) [[?low]]]

	 [(q [:find (max ?c)
	      :in $ ?sym ?d
	      :where
	      [?b :price/symbol ?sym]
	      [?b :price/time ?time]
	      [(day ?time) ?bd]
	      [(= ?bd ?d)]
	      [?b :price/close ?c]]
	     $ ?s ?day) [[?close]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	// Benchmark Legacy Path (UseComponentizedSubquery: false)
	b.Run("Legacy", func(b *testing.B) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseComponentizedSubquery:  false,
			EnableParallelSubqueries:  false,
		})

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			if result.Size() != 10 {
				b.Errorf("Expected 10 rows, got %d", result.Size())
			}
		}
	})

	// Benchmark Componentized Path - Sequential (UseComponentizedSubquery: true, no parallel)
	b.Run("Componentized_Sequential", func(b *testing.B) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseComponentizedSubquery: true,
			EnableParallelSubqueries: false,
		})

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			if result.Size() != 10 {
				b.Errorf("Expected 10 rows, got %d", result.Size())
			}
		}
	})

	// Benchmark Componentized Path - Parallel (UseComponentizedSubquery: true, with parallel)
	b.Run("Componentized_Parallel", func(b *testing.B) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseComponentizedSubquery:  true,
			EnableParallelSubqueries:  true,
			MaxSubqueryWorkers:        4,
			UseStreamingSubqueryUnion: true, // Enable streaming union
		})

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			if result.Size() != 10 {
				b.Errorf("Expected 10 rows, got %d", result.Size())
			}
		}
	})
}

// BenchmarkSubqueryExecutionLarge tests with a larger dataset
// This should show more dramatic memory differences
func BenchmarkSubqueryExecutionLarge(b *testing.B) {
	// Create test data: 50 days, 100 bars per day = 5000 bars total
	datoms := []datalog.Datom{}
	symbolID := datalog.NewIdentity("symbol:LARGE")

	datoms = append(datoms, datalog.Datom{
		E:  symbolID,
		A:  datalog.NewKeyword(":symbol/ticker"),
		V:  "LARGE",
		Tx: 1,
	})

	barID := 1000
	for day := 1; day <= 50; day++ {
		dayTime := time.Date(2025, 1, day, 9, 30, 0, 0, time.UTC)

		for minute := 0; minute < 100; minute++ {
			currentBarID := datalog.NewIdentity(fmt.Sprintf("bar:%d", barID))
			barTime := dayTime.Add(time.Duration(minute) * time.Minute)

			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/symbol"),
				V:  symbolID,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/time"),
				V:  barTime,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/open"),
				V:  100.0 + float64(day) + float64(minute)*0.1,
				Tx: uint64(barID),
			})
			datoms = append(datoms, datalog.Datom{
				E:  currentBarID,
				A:  datalog.NewKeyword(":price/high"),
				V:  102.0 + float64(day) + float64(minute)*0.2,
				Tx: uint64(barID),
			})

			barID++
		}
	}

	// Simpler query with 2 subqueries
	queryStr := `
	[:find ?day ?open ?high
	 :where
	 [?s :symbol/ticker "LARGE"]
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
	     $ ?s ?day) [[?high]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	// Benchmark Legacy Path
	b.Run("Legacy_Large", func(b *testing.B) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseComponentizedSubquery:  false,
		})

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			// Query groups by day, should have 50 rows (50 days) or less
			if result.Size() < 1 {
				b.Errorf("Expected at least 1 row, got %d", result.Size())
			}
		}
	})

	// Benchmark Componentized Path
	b.Run("Componentized_Large", func(b *testing.B) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseComponentizedSubquery: true,
			EnableParallelSubqueries: true,
			MaxSubqueryWorkers:       4,
		})

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			// Query groups by day, should have 50 rows (50 days) or less
			if result.Size() < 1 {
				b.Errorf("Expected at least 1 row, got %d", result.Size())
			}
		}
	})
}
