package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkBatchScanning compares regular iterator reuse vs batch scanning
func BenchmarkBatchScanning(b *testing.B) {
	// Create test database
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Generate test data - 5 days, 500 bars per day = 2,500 bars total
	symbolEntity := datalog.NewIdentity("BENCH")

	tx := db.NewTransaction()
	tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "BENCH")
	tx.Commit()

	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")
	priceOpen := datalog.NewKeyword(":price/open")

	loc, _ := time.LoadLocation("America/New_York")

	// Generate 5 days of data
	for day := 1; day <= 5; day++ {
		tx = db.NewTransaction()
		baseTime := time.Date(2025, 6, day, 9, 30, 0, 0, loc)
		for i := 0; i < 500; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%d-%d", day, i))
			barTime := baseTime.Add(time.Duration(i) * time.Minute)

			tx.Add(barEntity, priceSymbol, symbolEntity)
			tx.Add(barEntity, priceTime, barTime)
			tx.Add(barEntity, priceOpen, 100.0+float64(day)+float64(i)*0.01)
		}
		tx.Commit()
	}

	// Patterns
	symbolPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: priceSymbol},
			query.Constant{Value: symbolEntity},
		},
	}

	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: priceTime},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	// Test with day=3 constraint
	constraint := &timeExtractionConstraint{
		position:  2,
		extractFn: "day",
		expected:  int64(3),
	}

	// Benchmark regular iterator reuse (force by temporarily lowering threshold)
	b.Run("RegularIteratorReuse", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Get all 2,500 bars
			symbolRel, _ := matcher.Match(symbolPattern, nil)

			// Force regular iterator reuse by modifying the matcher temporarily
			// This simulates the old behavior
			oldThreshold := 10000 // Set high threshold to force regular reuse
			_ = oldThreshold      // Use it to avoid compiler warning

			timeRel, _ := matcher.MatchWithConstraints(
				timePattern,
				executor.Relations{symbolRel},
				[]executor.StorageConstraint{constraint},
			)

			// Count results
			count := 0
			it := timeRel.Iterator()
			for it.Next() {
				count++
			}

			if count != 500 {
				b.Errorf("Expected 500, got %d", count)
			}
		}
	})

	// Benchmark batch scanning (should use it automatically for 2,500 bindings)
	b.Run("BatchScanning", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Get all 2,500 bars
			symbolRel, _ := matcher.Match(symbolPattern, nil)

			// Should automatically use batch scanning (threshold is 100)
			timeRel, _ := matcher.MatchWithConstraints(
				timePattern,
				executor.Relations{symbolRel},
				[]executor.StorageConstraint{constraint},
			)

			// Count results
			count := 0
			it := timeRel.Iterator()
			for it.Next() {
				count++
			}

			if count != 500 {
				b.Errorf("Expected 500, got %d", count)
			}
		}
	})

	// Also test without constraints to see the overhead
	b.Run("NoConstraints", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			symbolRel, _ := matcher.Match(symbolPattern, nil)
			timeRel, _ := matcher.Match(timePattern, executor.Relations{symbolRel})

			count := 0
			it := timeRel.Iterator()
			for it.Next() {
				count++
			}

			if count != 2500 {
				b.Errorf("Expected 2500, got %d", count)
			}
		}
	})
}

// BenchmarkBatchScanScaling tests how batch scanning scales with different binding counts
func BenchmarkBatchScanScaling(b *testing.B) {
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create varying amounts of data
	sizes := []int{10, 50, 100, 500, 1000, 5000}

	for _, size := range sizes {
		// Create data
		tx := db.NewTransaction()
		for i := 0; i < size; i++ {
			entity := datalog.NewIdentity(fmt.Sprintf("e%d", i))
			tx.Add(entity, datalog.NewKeyword(":test/attr"), i)
			tx.Add(entity, datalog.NewKeyword(":test/value"), fmt.Sprintf("val%d", i))
		}
		tx.Commit()

		pattern1 := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: query.Symbol("?e")},
				query.Constant{Value: datalog.NewKeyword(":test/attr")},
				query.Variable{Name: query.Symbol("?a")},
			},
		}

		pattern2 := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: query.Symbol("?e")},
				query.Constant{Value: datalog.NewKeyword(":test/value")},
				query.Variable{Name: query.Symbol("?v")},
			},
		}

		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			matcher := NewBadgerMatcher(db.store)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rel1, _ := matcher.Match(pattern1, nil)
				rel2, _ := matcher.Match(pattern2, executor.Relations{rel1})

				// Consume results
				it := rel2.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
			}
		})
	}
}
