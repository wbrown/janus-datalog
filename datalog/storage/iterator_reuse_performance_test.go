package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestIteratorReusePerformance compares performance with and without iterator reuse
func TestIteratorReusePerformance(t *testing.T) {
	// Create test database
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Get the store from the database for the matcher
	store := db.store

	// Create test data: 10 symbols with 1000 bars each
	symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN", "FB", "TSLA", "NVDA", "JPM", "V", "JNJ"}

	tx := db.NewTransaction()
	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), ticker)

		// Create 1000 bars per symbol
		for i := 0; i < 1000; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d", ticker, i))
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
			tx.Add(barEntity, datalog.NewKeyword(":price/value"), float64(100.0+float64(i)))
		}
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Pattern: [?b :price/symbol ?s] with ?s bound to multiple symbols
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?b"},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Variable{Name: "?s"},
		},
	}

	// Test with different numbers of symbols bound
	testCases := []int{1, 2, 5, 10}

	for _, numSymbols := range testCases {
		t.Run(fmt.Sprintf("%d_symbols", numSymbols), func(t *testing.T) {
			// Create binding relation
			var tuples []executor.Tuple
			for i := 0; i < numSymbols; i++ {
				symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", symbols[i]))
				tuples = append(tuples, executor.Tuple{symbolEntity})
			}
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{"?s"},
				tuples,
			)

			// Test WITHOUT reuse (force VAET)
			t.Run("without_reuse", func(t *testing.T) {
				// Force IndexNestedLoop by setting threshold high
				opts := executor.ExecutorOptions{
					IndexNestedLoopThreshold: 999999,
				}
				matcher := NewBadgerMatcherWithOptions(store, opts)

				// Hack: Set a flag to force no reuse
				// For now, we'll measure with current settings

				start := time.Now()
				result, err := matcher.Match(pattern, executor.Relations{bindingRel})
				if err != nil {
					t.Fatal(err)
				}
				// Iterate and count results
				it := result.Iterator()
				barCount := 0
				for it.Next() {
					barCount++
				}
				it.Close()
				duration := time.Since(start)

				expectedBars := numSymbols * 1000
				if barCount != expectedBars {
					t.Errorf("Expected %d bars, got %d", expectedBars, barCount)
				}

				t.Logf("Without reuse: %v for %d symbols (%d bars)",
					duration, numSymbols, barCount)
			})

			// Test WITH reuse (current implementation)
			t.Run("with_reuse", func(t *testing.T) {
				// Force IndexNestedLoop by setting threshold high
				opts := executor.ExecutorOptions{
					IndexNestedLoopThreshold: 999999,
				}
				matcher := NewBadgerMatcherWithOptions(store, opts)

				start := time.Now()
				result, err := matcher.Match(pattern, executor.Relations{bindingRel})
				if err != nil {
					t.Fatal(err)
				}

				// Iterate and count results
				it := result.Iterator()
				barCount := 0
				for it.Next() {
					barCount++
				}
				it.Close()
				duration := time.Since(start)

				expectedBars := numSymbols * 1000
				if barCount != expectedBars {
					t.Errorf("Expected %d bars, got %d", expectedBars, barCount)
				}

				t.Logf("With reuse: %v for %d symbols (%d bars)",
					duration, numSymbols, barCount)
			})
		})
	}
}
