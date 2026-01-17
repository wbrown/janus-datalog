package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// BenchmarkPlannerWithoutCache measures planning performance without cache
func BenchmarkPlannerWithoutCache(b *testing.B) {
	opts := PlannerOptions{}
	opts.Cache = nil // Disable cache
	planner := NewClauseBasedPlanner(nil, opts)

	// Create a complex query
	q := createComplexQuery()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.Plan(q)
		if err != nil {
			b.Fatalf("Failed to plan query: %v", err)
		}
	}
}

// BenchmarkPlannerWithCache measures planning performance with cache
func BenchmarkPlannerWithCache(b *testing.B) {
	cache := NewPlanCache(1000, 0)
	opts := PlannerOptions{}
	opts.Cache = cache
	planner := NewClauseBasedPlanner(nil, opts)

	// Create a complex query
	q := createComplexQuery()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.Plan(q)
		if err != nil {
			b.Fatalf("Failed to plan query: %v", err)
		}
	}

	// Report cache statistics
	hits, misses, size := cache.Stats()
	b.Logf("Cache stats: hits=%d, misses=%d, size=%d", hits, misses, size)
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
				query.FindVariable{Symbol: query.Symbol("?e")},
			},
			Where: []query.Clause{
				&query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: query.Symbol("?e")},
						query.Constant{Value: datalog.NewKeyword(":test/id")},
						query.Constant{Value: int64(i)}, // Different value each time
					},
				},
			},
		}

		_, err := planner.Plan(q)
		if err != nil {
			b.Fatalf("Failed to plan query: %v", err)
		}
	}
}

// createComplexQuery creates a moderately complex query for benchmarking
func createComplexQuery() *query.Query {
	return &query.Query{
		Find: []query.FindElement{
			query.FindVariable{Symbol: query.Symbol("?p")},
			query.FindVariable{Symbol: query.Symbol("?name")},
			query.FindVariable{Symbol: query.Symbol("?age")},
			query.FindVariable{Symbol: query.Symbol("?city")},
		},
		Where: []query.Clause{
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?p")},
					query.Constant{Value: datalog.NewKeyword(":person/name")},
					query.Variable{Name: query.Symbol("?name")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?p")},
					query.Constant{Value: datalog.NewKeyword(":person/age")},
					query.Variable{Name: query.Symbol("?age")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?p")},
					query.Constant{Value: datalog.NewKeyword(":person/address")},
					query.Variable{Name: query.Symbol("?addr")},
				},
			},
			&query.DataPattern{
				Elements: []query.PatternElement{
					query.Variable{Name: query.Symbol("?addr")},
					query.Constant{Value: datalog.NewKeyword(":address/city")},
					query.Variable{Name: query.Symbol("?city")},
				},
			},
			// Add some predicates
			&query.Comparison{
				Op:    query.OpGT,
				Left:  query.VariableTerm{Symbol: query.Symbol("?age")},
				Right: query.ConstantTerm{Value: int64(18)},
			},
			&query.Comparison{
				Op:    query.OpNE,
				Left:  query.VariableTerm{Symbol: query.Symbol("?city")},
				Right: query.ConstantTerm{Value: "Unknown"},
			},
			// Add an expression
			&query.Expression{
				Function: query.ArithmeticFunction{
					Op:    query.OpAdd,
					Left:  query.VariableTerm{Symbol: query.Symbol("?age")},
					Right: query.ConstantTerm{Value: int64(10)},
				},
				Binding: query.Symbol("?future_age"),
			},
		},
	}
}
