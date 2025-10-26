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
		Tx: 12345,
	}

	// Create a pattern with all variables
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?e")},
			query.Variable{Name: query.Symbol("?a")},
			query.Variable{Name: query.Symbol("?v")},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	columns := []query.Symbol{
		query.Symbol("?e"),
		query.Symbol("?a"),
		query.Symbol("?v"),
		query.Symbol("?t"),
	}

	b.Run("DatomToTuple", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = query.DatomToTuple(*datom, pattern, columns)
		}
	})

	b.Run("TupleBuilder", func(b *testing.B) {
		tb := query.NewTupleBuilder(pattern, columns)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tb.BuildTuple(datom)
		}
	})

	b.Run("OptimizedTupleBuilder", func(b *testing.B) {
		tb := query.NewOptimizedTupleBuilder(pattern, columns)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tb.BuildTupleCopy(datom)
		}
	})

	b.Run("OptimizedTupleBuilder_Pooled", func(b *testing.B) {
		tb := query.NewOptimizedTupleBuilder(pattern, columns)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tuple := tb.BuildTuplePooled(datom)
			// Simulate releasing back to pool after use
			query.PutTuple(tuple)
		}
	})

	b.Run("OptimizedTupleBuilder_Into", func(b *testing.B) {
		tb := query.NewOptimizedTupleBuilder(pattern, columns)
		// Pre-allocate workspace
		workspace := make(query.Tuple, len(columns))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tb.BuildTupleInto(datom, workspace)
		}
	})

	b.Run("InternedTupleBuilder", func(b *testing.B) {
		tb := query.NewInternedTupleBuilder(pattern, columns)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = tb.BuildTupleInterned(datom)
		}
	})

	b.Run("InternedTupleBuilder_Into", func(b *testing.B) {
		tb := query.NewInternedTupleBuilder(pattern, columns)
		// Pre-allocate workspace
		workspace := make(query.Tuple, len(columns))
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
		columns []query.Symbol
	}{
		{
			name: "2_vars",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Constant{Value: datalog.NewKeyword(":test/attr")},
					query.Variable{Name: query.Symbol("?v")},
				},
			},
			columns: []query.Symbol{
				query.Symbol("?e"),
				query.Symbol("?v"),
			},
		},
		{
			name: "3_vars",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Variable{Name: query.Symbol("?a")},
					query.Variable{Name: query.Symbol("?v")},
				},
			},
			columns: []query.Symbol{
				query.Symbol("?e"),
				query.Symbol("?a"),
				query.Symbol("?v"),
			},
		},
		{
			name: "4_vars",
			pattern: &query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?e")},
					query.Variable{Name: query.Symbol("?a")},
					query.Variable{Name: query.Symbol("?v")},
					query.Variable{Name: query.Symbol("?t")},
				},
			},
			columns: []query.Symbol{
				query.Symbol("?e"),
				query.Symbol("?a"),
				query.Symbol("?v"),
				query.Symbol("?t"),
			},
		},
	}

	datom := &datalog.Datom{
		E:  datalog.NewIdentity("entity1"),
		A:  datalog.NewKeyword(":test/attr"),
		V:  "value",
		Tx: 12345,
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.Run("DatomToTuple", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					_ = query.DatomToTuple(*datom, scenario.pattern, scenario.columns)
				}
			})

			b.Run("OptimizedTupleBuilder", func(b *testing.B) {
				tb := query.NewOptimizedTupleBuilder(scenario.pattern, scenario.columns)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = tb.BuildTupleCopy(datom)
				}
			})
		})
	}
}
