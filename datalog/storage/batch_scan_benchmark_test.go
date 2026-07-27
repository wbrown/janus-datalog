package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkBatchScanning measures the binding-driven scan of an attribute over
// 2,500 bindings. PERFORMANCE_STATUS.md cites it by full name as
// BatchScanning/NoConstraints, so the name is load-bearing.
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
			query.Variable{Name: datalog.NewSymbol("?b")},
			query.Constant{Value: priceSymbol},
			query.Constant{Value: symbolEntity},
		},
	}

	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?b")},
			query.Constant{Value: priceTime},
			query.Variable{Name: datalog.NewSymbol("?t")},
		},
	}

	b.Run("NoConstraints", func(b *testing.B) {
		matcher := NewPatternMatcher(db.store)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			symbolRel, _ := matcher.Match(query.PatternQuery(symbolPattern), nil)
			timeRel, _ := matcher.Match(query.PatternQuery(timePattern), executor.Relations{symbolRel})

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

// BenchmarkBatchScanScaling measures how binding-driven scans scale with binding
// count. The sizes span chooseJoinStrategy's 1000-binding boundary, so the series
// covers both HashJoinScan and MergeJoin. The name says "batch scan" because
// archived benchmark records cite it by that name; renaming it orphans them.
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
			tx.Add(entity, datalog.NewKeyword(":test/attr"), int64(i))
			tx.Add(entity, datalog.NewKeyword(":test/value"), fmt.Sprintf("val%d", i))
		}
		tx.Commit()

		pattern1 := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: datalog.NewKeyword(":test/attr")},
				query.Variable{Name: datalog.NewSymbol("?a")},
			},
		}

		pattern2 := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: datalog.NewSymbol("?e")},
				query.Constant{Value: datalog.NewKeyword(":test/value")},
				query.Variable{Name: datalog.NewSymbol("?v")},
			},
		}

		b.Run(fmt.Sprintf("Size-%d", size), func(b *testing.B) {
			matcher := NewPatternMatcher(db.store)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rel1, _ := matcher.Match(query.PatternQuery(pattern1), nil)
				rel2, _ := matcher.Match(query.PatternQuery(pattern2), executor.Relations{rel1})

				// Consume results
				it := rel2.Iterator()
				for it.Next() {
					_ = it.Tuple()
				}
			}
		})
	}
}
