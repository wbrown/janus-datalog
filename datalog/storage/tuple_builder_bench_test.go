package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func BenchmarkTupleBuilding(b *testing.B) {
	// Create a sample datom
	datom := &datalog.Datom{
		E:  datalog.NewIdentity("entity1"),
		A:  datalog.NewKeyword(":test/attr"),
		V:  "value",
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 1},
	}

	// Create a pattern with all variables
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Variable{Name: datalog.NewSymbol("?a")},
			query.Variable{Name: datalog.NewSymbol("?v")},
			query.Variable{Name: datalog.NewSymbol("?t")},
		},
	}

	symbols := []query.Symbol{
		datalog.NewSymbol("?e"),
		datalog.NewSymbol("?a"),
		datalog.NewSymbol("?v"),
		datalog.NewSymbol("?t"),
	}

	b.Run("DatomToTuple", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = query.DatomToTuple(*datom, pattern, symbols)
		}
	})

	b.Run("TupleBuilder", func(b *testing.B) {
		tb := query.NewTupleBuilder(pattern, symbols)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tb.BuildTuple(datom)
		}
	})

	b.Run("OptimizedTupleBuilder", func(b *testing.B) {
		tb := query.NewOptimizedTupleBuilder(pattern, symbols)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tb.BuildTupleCopy(datom)
		}
	})

	b.Run("OptimizedTupleBuilder_Pooled", func(b *testing.B) {
		tb := query.NewOptimizedTupleBuilder(pattern, symbols)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tuple := tb.BuildTuplePooled(datom)
			// Simulate releasing back to pool after use
			query.PutTuple(tuple)
		}
	})

	b.Run("OptimizedTupleBuilder_Into", func(b *testing.B) {
		tb := query.NewOptimizedTupleBuilder(pattern, symbols)
		// Pre-allocate workspace
		workspace := make(query.Tuple, len(symbols))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tb.BuildTupleInto(datom, workspace)
		}
	})

	b.Run("InternedTupleBuilder", func(b *testing.B) {
		tb := query.NewInternedTupleBuilder(pattern, symbols)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tb.BuildTupleInterned(datom)
		}
	})

	b.Run("InternedTupleBuilder_Into", func(b *testing.B) {
		tb := query.NewInternedTupleBuilder(pattern, symbols)
		// Pre-allocate workspace
		workspace := make(query.Tuple, len(symbols))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tb.BuildTupleInternedInto(datom, workspace)
		}
	})
}

func BenchmarkTupleBuildingScenarios(b *testing.B) {
	// Test different scenarios
	scenarios := []struct {
		name    string
		pattern *query.DataPattern
		symbols []query.Symbol
	}{
		{
			name: "2_vars",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":test/attr")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			symbols: []query.Symbol{
				datalog.NewSymbol("?e"),
				datalog.NewSymbol("?v"),
			},
		},
		{
			name: "3_vars",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
				},
			},
			symbols: []query.Symbol{
				datalog.NewSymbol("?e"),
				datalog.NewSymbol("?a"),
				datalog.NewSymbol("?v"),
			},
		},
		{
			name: "4_vars",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?e")},
					query.Variable{Name: datalog.NewSymbol("?a")},
					query.Variable{Name: datalog.NewSymbol("?v")},
					query.Variable{Name: datalog.NewSymbol("?t")},
				},
			},
			symbols: []query.Symbol{
				datalog.NewSymbol("?e"),
				datalog.NewSymbol("?a"),
				datalog.NewSymbol("?v"),
				datalog.NewSymbol("?t"),
			},
		},
	}

	datom := &datalog.Datom{
		E:  datalog.NewIdentity("entity1"),
		A:  datalog.NewKeyword(":test/attr"),
		V:  "value",
		Tx: datalog.ElementID{Lamport: 12345, ReplicaID: 1},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.Run("DatomToTuple", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = query.DatomToTuple(*datom, scenario.pattern, scenario.symbols)
				}
			})

			b.Run("OptimizedTupleBuilder", func(b *testing.B) {
				tb := query.NewOptimizedTupleBuilder(scenario.pattern, scenario.symbols)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = tb.BuildTupleCopy(datom)
				}
			})
		})
	}
}
