package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// plannerBindingCases are the bound sets the planner benchmarks plan the
// complex query under: none, and its entity bound as a query input is.
func plannerBindingCases() []struct {
	name     string
	bindings query.Bindings
} {
	return []struct {
		name     string
		bindings query.Bindings
	}{
		{"unbound", query.Bindings{}},
		{"bound", query.Bindings{Symbols: map[query.Symbol]bool{datalog.NewSymbol("?p"): true}}},
	}
}

// BenchmarkPlannerWithoutCache measures planning performance without cache,
// under no bound symbol and under one.
func BenchmarkPlannerWithoutCache(b *testing.B) {
	for _, tc := range plannerBindingCases() {
		b.Run(tc.name, func(b *testing.B) {
			opts := PlannerOptions{}
			opts.Cache = nil // Disable cache
			planner := NewClauseBasedPlanner(nil, opts)
			q := createComplexQuery()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := planner.Plan(q, tc.bindings, nil)
				if err != nil {
					b.Fatalf("Failed to plan query: %v", err)
				}
			}
		})
	}
}

// BenchmarkPlannerWithCache measures planning performance with cache, under
// no bound symbol and under one: every plan enters the cache under the
// symbols bound when it was made, so the bound case is served as the unbound
// one is.
func BenchmarkPlannerWithCache(b *testing.B) {
	for _, tc := range plannerBindingCases() {
		b.Run(tc.name, func(b *testing.B) {
			cache := NewPlanCache(1000, 0)
			opts := PlannerOptions{}
			opts.Cache = cache
			planner := NewClauseBasedPlanner(nil, opts)
			q := createComplexQuery()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := planner.Plan(q, tc.bindings, nil)
				if err != nil {
					b.Fatalf("Failed to plan query: %v", err)
				}
			}

			hits, misses, size := cache.Stats()
			if misses != 1 || size != 1 {
				b.Fatalf("cache stats: hits=%d, misses=%d, size=%d; want one miss and one entry, every later plan served", hits, misses, size)
			}
		})
	}
}

// BenchmarkPlannerCacheMissOverhead measures the overhead of a cache miss
func BenchmarkPlannerCacheMissOverhead(b *testing.B) {
	cache := NewPlanCache(1000, 0)
	opts := PlannerOptions{}
	opts.Cache = cache
	planner := NewClauseBasedPlanner(nil, opts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a unique query each time to force cache miss
		q := &query.Query{
			Find: []query.FindElement{
				query.FindVariable{Symbol: datalog.NewSymbol("?e")},
			},
			Where: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: datalog.NewSymbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":test/id")},
						query.Constant{Value: int64(i)}, // Different value each time
					},
				},
			},
		}

		_, err := planner.Plan(q, query.Bindings{}, nil)
		if err != nil {
			b.Fatalf("Failed to plan query: %v", err)
		}
	}
}

// createComplexQuery creates a moderately complex query for benchmarking
func createComplexQuery() *query.Query {
	return &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: datalog.NewSymbol("?p")},
			query.FindVariable{Symbol: datalog.NewSymbol("?name")},
			query.FindVariable{Symbol: datalog.NewSymbol("?age")},
			query.FindVariable{Symbol: datalog.NewSymbol("?city")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?p")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Variable{Name: datalog.NewSymbol("?name")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?p")},
					query.Constant{Value: datalog.NewKeyword(":person/age")},
					query.Variable{Name: datalog.NewSymbol("?age")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?p")},
					query.Constant{Value: datalog.NewKeyword(":person/address")},
					query.Variable{Name: datalog.NewSymbol("?addr")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: datalog.NewSymbol("?addr")},
					query.Constant{Value: datalog.NewKeyword(":address/city")},
					query.Variable{Name: datalog.NewSymbol("?city")},
				},
			},
			// Add some predicates
			&query.Comparison{
				Op:    datalog.SymGT,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
				Right: query.ConstantTerm{Value: int64(18)},
			},
			&query.Comparison{
				Op:    datalog.SymNE,
				Left:  query.VariableTerm{Symbol: datalog.NewSymbol("?city")},
				Right: query.ConstantTerm{Value: "Unknown"},
			},
			// Add an expression
			&query.Expression{
				Function: query.ArithmeticFunction{
					Op: datalog.SymAdd,
					Args: []query.Term{
						query.VariableTerm{Symbol: datalog.NewSymbol("?age")},
						query.ConstantTerm{Value: int64(10)},
					},
				},
				Binding: datalog.NewSymbol("?future_age"),
			},
		},
	}
}
