package storage

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkPrebuiltDatabase_PatternMatching profiles pattern matching on a pre-built database
// This eliminates database setup overhead from the profile
//
// To build the test database first:
//
//	go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x
//
// Then profile query execution:
//
//	go test -bench=^BenchmarkPrebuiltDatabase -cpuprofile=cpu.prof -memprofile=mem.prof ./datalog/storage
//	go tool pprof -http=:8080 cpu.prof
func BenchmarkPrebuiltDatabase_PatternMatching(b *testing.B) {
	// Open pre-built database (read-only, no setup overhead!)
	db, err := OpenTestDatabase("testdata/ohlc_benchmark.db")
	if err != nil {
		b.Skipf("Test database not found: %v\nRun: go test -run=^$ -bench=^BenchmarkBuildTestDatabase$ ./datalog/storage -benchtime=1x", err)
		return
	}
	defer db.Close()

	// Test patterns that represent real production queries
	testCases := []struct {
		name    string
		pattern *query.DataPattern
		setup   func() executor.Relations // Optional binding setup
	}{
		{
			name: "UnboundAttribute_PriceTime",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?bar"},
					query.Constant{Value: datalog.NewKeyword("price/time")},
					query.Variable{Name: "?time"},
				},
			},
		},
		{
			name: "UnboundAttribute_PriceOpen",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?bar"},
					query.Constant{Value: datalog.NewKeyword("price/open")},
					query.Variable{Name: "?open"},
				},
			},
		},
		{
			name: "BoundEntity_SingleBar",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Constant{Value: datalog.NewIdentity("bar500")},
					query.Variable{Name: "?attr"},
					query.Variable{Name: "?value"},
				},
			},
		},
		{
			name: "BoundSymbol_SpecificStock",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?bar"},
					query.Constant{Value: datalog.NewKeyword("price/symbol")},
					query.Constant{Value: datalog.NewIdentity("TICK0005")},
				},
			},
		},
		{
			name: "LargeBindingSet_260Entities",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: "?bar"},
					query.Constant{Value: datalog.NewKeyword("price/open")},
					query.Variable{Name: "?open"},
				},
			},
			setup: func() executor.Relations {
				// Create a binding relation with 260 bar entities (typical hourly OHLC)
				tuples := make([]executor.Tuple, 260)
				for i := 0; i < 260; i++ {
					tuples[i] = executor.Tuple{datalog.NewIdentity(fmt.Sprintf("bar%d", i+1))}
				}
				return executor.Relations{
					executor.NewMaterializedRelation(
						[]query.Symbol{"?bar"},
						tuples,
					),
				}
			},
		},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			matcher := NewBadgerMatcher(db.store)

			// Setup bindings if provided
			var bindings executor.Relations
			if tc.setup != nil {
				bindings = tc.setup()
			}

			// Reset timer to exclude setup
			b.ResetTimer()
			b.ReportAllocs()

			// Run benchmark
			for i := 0; i < b.N; i++ {
				result, err := matcher.Match(tc.pattern, bindings)
				if err != nil {
					b.Fatalf("Match failed: %v", err)
				}

				// Force materialization
				size := result.Size()
				if size == 0 && tc.name != "BoundEntity_SingleBar" {
					b.Fatalf("Expected non-empty result for %s", tc.name)
				}
			}
		})
	}
}

// BenchmarkPrebuiltDatabase_FullQuery profiles complete query execution
func BenchmarkPrebuiltDatabase_FullQuery(b *testing.B) {
	// Open pre-built database
	db, err := OpenTestDatabase("testdata/ohlc_benchmark.db")
	if err != nil {
		b.Skipf("Test database not found: %v", err)
		return
	}
	defer db.Close()

	// Simulate a realistic OHLC aggregation query
	// This would be executed by the query engine, but we'll benchmark the storage layer
	b.Run("DailyOHLC_30Days", func(b *testing.B) {
		matcher := NewBadgerMatcher(db.store)

		// Pattern: [?bar :price/symbol ?symbol]
		symbolPattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: "?bar"},
				query.Constant{Value: datalog.NewKeyword("price/symbol")},
				query.Constant{Value: datalog.NewIdentity("TICK0001")},
			},
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Get all bars for symbol
			barsResult, err := matcher.Match(symbolPattern, nil)
			if err != nil {
				b.Fatalf("Symbol match failed: %v", err)
			}

			// For each bar, get OHLC attributes
			barCount := 0
			iter := barsResult.Iterator()
			for iter.Next() {
				barEntity := iter.Tuple()[0]

				// Get open price
				openPattern := &query.DataPattern{
					Elements: []query.PatternElement{
						query.Constant{Value: barEntity},
						query.Constant{Value: datalog.NewKeyword("price/open")},
						query.Variable{Name: "?open"},
					},
				}
				openResult, _ := matcher.Match(openPattern, nil)
				if openResult.Size() > 0 {
					barCount++
				}
			}
			iter.Close()

			if barCount == 0 {
				b.Fatal("Expected some bars for TICK0001")
			}
		}
	})
}

// To build the test database, run:
//   go run cmd/build-testdb/main.go
// Or build it inline:
//   cd datalog/storage && go test -run=^TestBuildDatabase$
