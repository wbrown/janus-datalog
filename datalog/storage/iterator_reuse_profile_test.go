package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkIteratorReuse benchmarks iterator reuse for profiling
func BenchmarkIteratorReuse(b *testing.B) {
	// Create test database
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	store := db.store

	// Create test data: 10 symbols with 100 bars each
	symbols := make([]string, 10)
	for i := 0; i < 10; i++ {
		symbols[i] = fmt.Sprintf("SYM%02d", i)
	}

	tx := db.NewTransaction()
	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), ticker)

		// Create 100 bars per symbol
		for i := 0; i < 100; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d", ticker, i))
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
			tx.Add(barEntity, datalog.NewKeyword(":price/value"), float64(100.0+float64(i)))
			tx.Add(barEntity, datalog.NewKeyword(":price/minute"), int64(i))
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Pattern: [?b :price/symbol ?s] with ?s bound to 5 symbols
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?b"},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Variable{Name: "?s"},
		},
	}

	// Create binding relation with 5 symbols
	var tuples []executor.Tuple
	for i := 0; i < 5; i++ {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", symbols[i]))
		tuples = append(tuples, executor.Tuple{symbolEntity})
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{"?s"},
		tuples,
	)

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matcher := NewBadgerMatcher(store)
		result, err := matcher.Match(pattern, executor.Relations{bindingRel})
		if err != nil {
			b.Fatal(err)
		}
		if result.Size() != 500 { // 5 symbols * 100 bars
			b.Fatalf("Expected 500 bars, got %d", result.Size())
		}
	}
}

// BenchmarkNoIteratorReuse benchmarks without iterator reuse for comparison
func BenchmarkNoIteratorReuse(b *testing.B) {
	// Create test database
	tempDir := b.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	store := db.store

	// Create identical test data
	symbols := make([]string, 10)
	for i := 0; i < 10; i++ {
		symbols[i] = fmt.Sprintf("SYM%02d", i)
	}

	tx := db.NewTransaction()
	for _, ticker := range symbols {
		symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", ticker))
		tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), ticker)

		for i := 0; i < 100; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar:%s:%d", ticker, i))
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
			tx.Add(barEntity, datalog.NewKeyword(":price/value"), float64(100.0+float64(i)))
			tx.Add(barEntity, datalog.NewKeyword(":price/minute"), int64(i))
		}
	}
	if _, err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Use a pattern that won't trigger reuse - each symbol queried separately
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var allResults []datalog.Datom

		// Query each symbol separately (simulating no reuse)
		for j := 0; j < 5; j++ {
			symbolEntity := datalog.NewIdentity(fmt.Sprintf("symbol:%s", symbols[j]))
			pattern := &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?b"},
					query.Constant{Value: datalog.NewKeyword(":price/symbol")},
					query.Constant{Value: symbolEntity}, // Constant instead of variable
				},
			}

			matcher := NewBadgerMatcher(store)
			result, err := matcher.Match(pattern, nil)
			if err != nil {
				b.Fatal(err)
			}

			// Count results
			for k := 0; k < result.Size(); k++ {
				allResults = append(allResults, datalog.Datom{}) // Dummy for counting
			}
		}

		if len(allResults) != 500 {
			b.Fatalf("Expected 500 bars total, got %d", len(allResults))
		}
	}
}
