package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkIteratorReuseClean benchmarks just the query execution without setup
func BenchmarkIteratorReuseClean(b *testing.B) {
	// Do all setup before benchmark
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	store := db.store

	// Create test data
	symbols := make([]string, 10)
	for i := 0; i < 10; i++ {
		symbols[i] = fmt.Sprintf("SYM%02d", i)
	}

	tx := db.NewTransaction()
	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), ticker)

		for i := 0; i < 200; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d", ticker, i))
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
			tx.Add(barEntity, datalog.NewKeyword(":price/value"), float64(100.0+float64(i)))
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Setup pattern and binding
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?b"},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Variable{Name: "?s"},
		},
	}

	// Test with different binding sizes
	testCases := []struct {
		name       string
		numSymbols int
	}{
		{"1_symbol", 1},
		{"3_symbols", 3},
		{"5_symbols", 5},
		{"10_symbols", 10},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create binding relation
			var tuples []executor.Tuple
			for i := 0; i < tc.numSymbols; i++ {
				symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", symbols[i]))
				tuples = append(tuples, executor.Tuple{symbolEntity})
			}
			bindingRel := executor.NewMaterializedRelation(
				[]query.Symbol{"?s"},
				tuples,
			)

			// Create the matcher once
			matcher := NewBadgerMatcher(store)

			// Reset timer after all setup
			b.ResetTimer()

			// Run the actual benchmark
			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(pattern, executor.Relations{bindingRel})
				if err != nil {
					b.Fatal(err)
				}
				expectedSize := tc.numSymbols * 200
				if result.Size() != expectedSize {
					b.Fatalf("Expected %d bars, got %d", expectedSize, result.Size())
				}
			}
		})
	}
}
