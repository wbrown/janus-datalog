package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkOHLCQuery tests the real-world OHLC query performance
func BenchmarkOHLCQuery(b *testing.B) {
	// Create test database with realistic OHLC data
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Generate test data - 10 days of OHLC data for 3 symbols
	// 390 bars per day per symbol = 11,700 bars total
	symbols := []string{"AAPL", "GOOG", "CRWV"}

	symbolKw := datalog.NewKeyword(":symbol/ticker")
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")
	priceOpen := datalog.NewKeyword(":price/open")
	priceHigh := datalog.NewKeyword(":price/high")
	priceLow := datalog.NewKeyword(":price/low")
	priceClose := datalog.NewKeyword(":price/close")
	priceVolume := datalog.NewKeyword(":price/volume")

	loc, _ := time.LoadLocation("America/New_York")

	// Create symbols
	tx := db.NewTransaction()
	symbolEntities := make(map[string]datalog.Identity)
	for _, sym := range symbols {
		entity := datalog.NewIdentity(sym)
		symbolEntities[sym] = entity
		if err := tx.Add(entity, symbolKw, sym); err != nil {
			b.Fatalf("Failed to write symbol %s: %v", sym, err)
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatalf("Failed to commit symbols: %v", err)
	}

	// Generate 10 days of price data - commit each symbol separately to avoid large transactions
	for day := 1; day <= 10; day++ {
		baseTime := time.Date(2025, 6, day, 9, 30, 0, 0, loc)

		for _, sym := range symbols {
			tx = db.NewTransaction()
			symbolEntity := symbolEntities[sym]
			basePrice := 100.0 + float64(len(sym)*10) // Different base price per symbol

			for i := 0; i < 390; i++ {
				barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%s-%d-%d", sym, day, i))
				barTime := baseTime.Add(time.Duration(i) * time.Minute)

				open := basePrice + float64(day) + float64(i)*0.01
				high := open + 0.5
				low := open - 0.3
				close := open + 0.2
				volume := int64(1000000 + i*1000)

				tx.Add(barEntity, priceSymbol, symbolEntity)
				tx.Add(barEntity, priceTime, barTime)
				tx.Add(barEntity, priceOpen, open)
				tx.Add(barEntity, priceHigh, high)
				tx.Add(barEntity, priceLow, low)
				tx.Add(barEntity, priceClose, close)
				tx.Add(barEntity, priceVolume, volume)
			}

			if _, err := tx.Commit(); err != nil {
				b.Fatalf("Failed to commit day %d symbol %s: %v", day, sym, err)
			}
		}
	}

	// The OHLC query for day 5 of CRWV
	queryStr := `[:find ?b ?t ?h ?l ?c ?v
	              :where 
	              [?s :symbol/ticker "CRWV"]
	              [?b :price/symbol ?s]
	              [?b :price/time ?t]
	              [?b :price/high ?h]
	              [?b :price/low ?l]
	              [?b :price/close ?c]
	              [?b :price/volume ?v]
	              [(day ?t) ?d]
	              [(= ?d 5)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	// Benchmark without predicate pushdown
	b.Run("WithoutPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnablePredicatePushdown: false,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Should get 390 bars for day 20
			if result.Size() != 390 {
				b.Errorf("Expected 390 results, got %d", result.Size())
			}
		}
	})

	// Benchmark with predicate pushdown
	b.Run("WithPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnablePredicatePushdown: true,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Should get 390 bars for day 20
			if result.Size() != 390 {
				b.Errorf("Expected 390 results, got %d", result.Size())
			}
		}
	})

	// Benchmark with time-range optimization (semantic rewriting)
	b.Run("WithTimeRangeOpt", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnablePredicatePushdown:  true,
			EnableSemanticRewriting:  true, // Enables time-range optimization
			EnableFineGrainedPhases:  true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Should get 390 bars for day 5
			if result.Size() != 390 {
				b.Errorf("Expected 390 results, got %d", result.Size())
			}
		}
	})
}

// BenchmarkOHLCQueryLargeDataset tests with even more data
func BenchmarkOHLCQueryLargeDataset(b *testing.B) {
	// Create test database
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Generate test data - 90 days of OHLC data for 50 symbols
	// 390 bars per day per symbol = 1,755,000 bars total
	numSymbols := 50
	numDays := 90

	symbolKw := datalog.NewKeyword(":symbol/ticker")
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")
	priceOpen := datalog.NewKeyword(":price/open")
	priceHigh := datalog.NewKeyword(":price/high")
	priceLow := datalog.NewKeyword(":price/low")
	priceClose := datalog.NewKeyword(":price/close")
	priceVolume := datalog.NewKeyword(":price/volume")

	loc, _ := time.LoadLocation("America/New_York")

	// Create symbols including CRWV
	tx := db.NewTransaction()
	symbolEntities := make(map[string]datalog.Identity)

	// Add CRWV first
	crwvEntity := datalog.NewIdentity("CRWV")
	symbolEntities["CRWV"] = crwvEntity
	tx.Add(crwvEntity, symbolKw, "CRWV")

	// Add other symbols
	for i := 1; i < numSymbols; i++ {
		sym := fmt.Sprintf("SYM%03d", i)
		entity := datalog.NewIdentity(sym)
		symbolEntities[sym] = entity
		tx.Add(entity, symbolKw, sym)
	}

	if _, err := tx.Commit(); err != nil {
		b.Fatalf("Failed to commit symbols: %v", err)
	}

	// Generate price data - commit per symbol per day to stay within BadgerDB transaction limits
	for day := 1; day <= numDays; day++ {
		baseTime := time.Date(2025, time.Month(3+(day-1)/30), ((day-1)%30)+1, 9, 30, 0, 0, loc)

		// Only generate full data for CRWV and a few others
		// For most symbols, only generate sparse data
		for sym, symbolEntity := range symbolEntities {
			// For most symbols, only generate data every 10th day
			if sym != "CRWV" && day%10 != 0 {
				continue
			}

			// Create a new transaction for each symbol/day combination
			tx = db.NewTransaction()
			basePrice := 100.0

			for i := 0; i < 390; i++ {
				barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%s-%d-%d", sym, day, i))
				barTime := baseTime.Add(time.Duration(i) * time.Minute)

				open := basePrice + float64(day) + float64(i)*0.01
				high := open + 0.5
				low := open - 0.3
				close := open + 0.2
				volume := int64(1000000 + i*1000)

				tx.Add(barEntity, priceSymbol, symbolEntity)
				tx.Add(barEntity, priceTime, barTime)
				tx.Add(barEntity, priceOpen, open)
				tx.Add(barEntity, priceHigh, high)
				tx.Add(barEntity, priceLow, low)
				tx.Add(barEntity, priceClose, close)
				tx.Add(barEntity, priceVolume, volume)
			}

			if _, err := tx.Commit(); err != nil {
				b.Fatalf("Failed to commit day %d symbol %s: %v", day, sym, err)
			}
		}
	}

	// The OHLC query for day 5 of CRWV
	queryStr := `[:find ?b ?t ?h ?l ?c ?v
	              :where 
	              [?s :symbol/ticker "CRWV"]
	              [?b :price/symbol ?s]
	              [?b :price/time ?t]
	              [?b :price/high ?h]
	              [?b :price/low ?l]
	              [?b :price/close ?c]
	              [?b :price/volume ?v]
	              [(day ?t) ?d]
	              [(= ?d 5)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.Run("WithoutPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnablePredicatePushdown: false,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Should get 390 bars for day 20 (3 occurrences in 90 days)
			if result.Size() != 390*3 {
				b.Errorf("Expected %d results, got %d", 390*3, result.Size())
			}
		}
	})

	b.Run("WithPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnablePredicatePushdown: true,
			EnableFineGrainedPhases: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}

			// Should get 390 bars for day 20 (3 occurrences in 90 days)
			if result.Size() != 390*3 {
				b.Errorf("Expected %d results, got %d", 390*3, result.Size())
			}
		}
	})
}
