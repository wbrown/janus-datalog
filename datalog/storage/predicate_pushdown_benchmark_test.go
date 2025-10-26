package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkPredicatePushdown measures the performance impact of predicate pushdown
func BenchmarkPredicatePushdown(b *testing.B) {
	// Create test database with more data for meaningful benchmark
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Generate test data - 30 days of data, 390 bars per day = 11,700 bars total
	// Create a symbol
	symbol := datalog.NewKeyword(":symbol/ticker")
	symbolEntity := datalog.NewIdentity("CRWV")

	tx := db.NewTransaction()
	if err := tx.Add(symbolEntity, symbol, "CRWV"); err != nil {
		b.Fatalf("Failed to write symbol: %v", err)
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatalf("Failed to commit symbol: %v", err)
	}

	// Create price datoms
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")
	priceOpen := datalog.NewKeyword(":price/open")

	loc, _ := time.LoadLocation("America/New_York")

	// Generate 10 days of data - commit each day separately
	for day := 1; day <= 10; day++ {
		tx = db.NewTransaction()
		baseTime := time.Date(2025, 6, day, 9, 30, 0, 0, loc)
		for i := 0; i < 390; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%d-%d", day, i))
			barTime := baseTime.Add(time.Duration(i) * time.Minute)

			if err := tx.Add(barEntity, priceSymbol, symbolEntity); err != nil {
				b.Fatalf("Failed to write price symbol: %v", err)
			}

			if err := tx.Add(barEntity, priceTime, barTime); err != nil {
				b.Fatalf("Failed to write price time: %v", err)
			}

			if err := tx.Add(barEntity, priceOpen, 100.0+float64(day)+float64(i)*0.01); err != nil {
				b.Fatalf("Failed to write price open: %v", err)
			}
		}
		if _, err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit day %d: %v", day, err)
		}
	}

	// Patterns for testing
	symbolPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Constant{Value: symbolEntity},
		},
	}

	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: datalog.NewKeyword(":price/time")},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	// Benchmark without predicate pushdown
	b.Run("WithoutPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// First get all bars for symbol
			symbolRel, err := matcher.Match(symbolPattern, nil)
			if err != nil {
				b.Fatalf("Failed to match symbol pattern: %v", err)
			}

			// Then get times for all bars
			timeRel, err := matcher.Match(timePattern, executor.Relations{symbolRel})
			if err != nil {
				b.Fatalf("Failed to match time pattern: %v", err)
			}

			// Filter in memory for day=20
			count := 0
			it := timeRel.Iterator()
			for it.Next() {
				tuple := it.Tuple()
				// Find the time value
				for i, col := range timeRel.Columns() {
					if col == "?t" {
						if t, ok := tuple[i].(time.Time); ok {
							if t.Day() == 5 {
								count++
							}
						}
						break
					}
				}
			}

			if count != 390 {
				b.Errorf("Expected 390 bars for day 5, got %d", count)
			}
		}
	})

	// Benchmark with predicate pushdown
	b.Run("WithPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)

		// Create time extraction constraint for day=5
		constraint := &timeExtractionConstraint{
			position:  2, // Value position
			extractFn: "day",
			expected:  int64(5),
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// First get all bars for symbol
			symbolRel, err := matcher.Match(symbolPattern, nil)
			if err != nil {
				b.Fatalf("Failed to match symbol pattern: %v", err)
			}

			// Then match with time constraint pushed down
			timeRel, err := matcher.MatchWithConstraints(
				timePattern,
				executor.Relations{symbolRel},
				[]executor.StorageConstraint{constraint},
			)
			if err != nil {
				b.Fatalf("Failed to match time pattern with constraint: %v", err)
			}

			// Just count results - no filtering needed
			count := 0
			it := timeRel.Iterator()
			for it.Next() {
				count++
			}

			if count != 390 {
				b.Errorf("Expected 390 bars for day 5, got %d", count)
			}
		}
	})

	// Benchmark the worst case - filtering to 1 day out of 10 (10% selectivity)
	b.Run("WorstCase-1of10Days", func(b *testing.B) {
		b.Run("WithoutPushdown", func(b *testing.B) {
			matcher := NewBadgerMatcher(db.store)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Get ALL 3,900 bars
				symbolRel, err := matcher.Match(symbolPattern, nil)
				if err != nil {
					b.Fatal(err)
				}

				timeRel, err := matcher.Match(timePattern, executor.Relations{symbolRel})
				if err != nil {
					b.Fatal(err)
				}

				// Filter to just day 15 in memory
				count := 0
				it := timeRel.Iterator()
				for it.Next() {
					tuple := it.Tuple()
					for i, col := range timeRel.Columns() {
						if col == "?t" {
							if t, ok := tuple[i].(time.Time); ok {
								if t.Day() == 5 {
									count++
								}
							}
							break
						}
					}
				}

				if count != 390 {
					b.Errorf("Expected 390, got %d", count)
				}
			}
		})

		b.Run("WithPushdown", func(b *testing.B) {
			matcher := NewBadgerMatcher(db.store)
			constraint := &timeExtractionConstraint{
				position:  2,
				extractFn: "day",
				expected:  int64(5),
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				symbolRel, err := matcher.Match(symbolPattern, nil)
				if err != nil {
					b.Fatal(err)
				}

				// Only fetch the 390 bars for day 5
				timeRel, err := matcher.MatchWithConstraints(
					timePattern,
					executor.Relations{symbolRel},
					[]executor.StorageConstraint{constraint},
				)
				if err != nil {
					b.Fatal(err)
				}

				count := 0
				it := timeRel.Iterator()
				for it.Next() {
					count++
				}

				if count != 390 {
					b.Errorf("Expected 390, got %d", count)
				}
			}
		})
	})
}

// Benchmark memory allocations
func BenchmarkPredicatePushdownAllocs(b *testing.B) {
	// Setup same as above
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	symbolEntity := datalog.NewIdentity("TEST")

	// Create minimal test data - 10 days, commit each day
	loc, _ := time.LoadLocation("America/New_York")
	for day := 1; day <= 10; day++ {
		tx := db.NewTransaction()
		baseTime := time.Date(2025, 6, day, 9, 30, 0, 0, loc)
		for i := 0; i < 390; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%d-%d", day, i))
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
			tx.Add(barEntity, datalog.NewKeyword(":price/time"), baseTime.Add(time.Duration(i)*time.Minute))
		}
		tx.Commit()
	}

	symbolPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Constant{Value: symbolEntity},
		},
	}

	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: datalog.NewKeyword(":price/time")},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	b.Run("AllocsWithoutPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			symbolRel, _ := matcher.Match(symbolPattern, nil)
			timeRel, _ := matcher.Match(timePattern, executor.Relations{symbolRel})

			// Filter all 3900 results in memory
			it := timeRel.Iterator()
			for it.Next() {
				tuple := it.Tuple()
				for i, col := range timeRel.Columns() {
					if col == "?t" {
						if t, ok := tuple[i].(time.Time); ok {
							_ = t.Day() == 5
						}
						break
					}
				}
			}
		}
	})

	b.Run("AllocsWithPushdown", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)
		constraint := &timeExtractionConstraint{
			position:  2,
			extractFn: "day",
			expected:  int64(5),
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			symbolRel, _ := matcher.Match(symbolPattern, nil)

			// Only fetch 390 results
			timeRel, _ := matcher.MatchWithConstraints(
				timePattern,
				executor.Relations{symbolRel},
				[]executor.StorageConstraint{constraint},
			)

			it := timeRel.Iterator()
			for it.Next() {
				_ = it.Tuple()
			}
		}
	})
}
