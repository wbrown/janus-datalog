package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// BenchmarkOHLCBadgerDBTimeRanges profiles the complete time range optimization
// path with actual BadgerDB storage and AVET index scanning.
//
//	Run with: go test -bench=BenchmarkOHLCBadgerDBTimeRanges -benchtime=3s \
//	          -cpuprofile=badger_cpu.prof -memprofile=badger_mem.prof -run=^$ ./datalog/storage
func BenchmarkOHLCBadgerDBTimeRanges(b *testing.B) {
	scenarios := []struct {
		name        string
		numHours    int
		barsPerHour int
	}{
		{"Hourly_260hours_10bars", 260, 10},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			// Create temporary database
			tempDir := b.TempDir()
			db, err := NewDatabase(tempDir)
			if err != nil {
				b.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			// Generate test data
			symbolID := datalog.NewIdentity("symbol:TEST")
			symbolKw := datalog.NewKeyword(":symbol/ticker")
			priceSymbol := datalog.NewKeyword(":price/symbol")
			priceTime := datalog.NewKeyword(":price/time")
			priceOpen := datalog.NewKeyword(":price/open")
			priceHigh := datalog.NewKeyword(":price/high")
			priceLow := datalog.NewKeyword(":price/low")
			priceClose := datalog.NewKeyword(":price/close")

			// Add symbol
			tx := db.NewTransaction()
			if err := tx.Add(symbolID, symbolKw, "TEST"); err != nil {
				b.Fatalf("Failed to add symbol: %v", err)
			}
			if _, err := tx.Commit(); err != nil {
				b.Fatalf("Failed to commit symbol: %v", err)
			}

			// Generate price data - commit in batches to avoid large transactions
			startTime := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
			batchSize := 50 // Commit every 50 hours

			for hourBatch := 0; hourBatch < scenario.numHours; hourBatch += batchSize {
				tx = db.NewTransaction()
				endHour := hourBatch + batchSize
				if endHour > scenario.numHours {
					endHour = scenario.numHours
				}

				for hour := hourBatch; hour < endHour; hour++ {
					hourTime := startTime.Add(time.Duration(hour) * time.Hour)

					for minute := 0; minute < scenario.barsPerHour; minute++ {
						barID := datalog.NewIdentity(fmt.Sprintf("bar:%d-%d", hour, minute))
						barTime := hourTime.Add(time.Duration(minute) * time.Minute)

						tx.Add(barID, priceSymbol, symbolID)
						tx.Add(barID, priceTime, barTime)
						tx.Add(barID, priceOpen, 100.0+float64(hour)+float64(minute)*0.1)
						tx.Add(barID, priceHigh, 102.0+float64(hour)+float64(minute)*0.2)
						tx.Add(barID, priceLow, 98.0+float64(hour)-float64(minute)*0.1)
						tx.Add(barID, priceClose, 101.0+float64(hour)+float64(minute)*0.15)
					}
				}

				if _, err := tx.Commit(); err != nil {
					b.Fatalf("Failed to commit batch %d-%d: %v", hourBatch, endHour, err)
				}
			}

			// The OHLC query with decorrelated subqueries
			queryStr := `
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

			q, err := parser.ParseQuery(queryStr)
			if err != nil {
				b.Fatalf("Failed to parse query: %v", err)
			}

			// Create executor with BadgerMatcher
			matcher := NewBadgerMatcher(db.store)
			exec := executor.NewExecutor(matcher)

			// Warmup
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			expectedResults := scenario.numHours
			if result.Size() != expectedResults {
				b.Logf("Warning: Expected %d results, got %d", expectedResults, result.Size())
			}

			// Reset timer and benchmark
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkSimpleTimeQuery tests time-based queries with and without optimization
func BenchmarkSimpleTimeQuery(b *testing.B) {
	// Create database with 260 hours of data
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Generate test data
	symbolID := datalog.NewIdentity("symbol:TEST")
	tx := db.NewTransaction()
	tx.Add(symbolID, datalog.NewKeyword(":symbol/ticker"), "TEST")
	tx.Commit()

	// Add 260 hours of price data
	startTime := time.Date(2025, 6, 20, 9, 0, 0, 0, time.UTC)
	for hour := 0; hour < 260; hour++ {
		tx = db.NewTransaction()
		for minute := 0; minute < 10; minute++ {
			barID := datalog.NewIdentity(fmt.Sprintf("bar:%d-%d", hour, minute))
			barTime := startTime.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute)

			tx.Add(barID, datalog.NewKeyword(":price/symbol"), symbolID)
			tx.Add(barID, datalog.NewKeyword(":price/time"), barTime)
			tx.Add(barID, datalog.NewKeyword(":price/open"), 100.0)
		}
		tx.Commit()
	}

	// Query that gets all price times
	queryStr := `[:find ?t
	              :where
	              [?b :price/time ?t]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	matcher := NewBadgerMatcher(db.store)
	exec := executor.NewExecutor(matcher)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(q)
		if err != nil {
			b.Fatal(err)
		}
	}
}
