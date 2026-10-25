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

// reachRuleSet is a recursive predicate over :edge, reach, with a base rule
// and a recursive one.
func reachRuleSet(b *testing.B) *query.RuleSet {
	edge := func(from, to string) *query.DataPattern {
		return &query.DataPattern{Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol(from)},
			query.Constant{Value: datalog.NewKeyword(":edge")},
			query.Variable{Name: datalog.NewSymbol(to)},
		}}
	}
	head := []query.Symbol{datalog.NewSymbol("?x"), datalog.NewSymbol("?y")}
	rs, err := query.NewRuleSet([]*query.Rule{
		{Name: datalog.NewSymbol("reach"), Head: head, Body: []query.Clause{edge("?x", "?y")}},
		{Name: datalog.NewSymbol("reach"), Head: head, Body: []query.Clause{edge("?x", "?z"), invocation("reach", "?z", "?y")}},
	})
	if err != nil {
		b.Fatalf("NewRuleSet: %v", err)
	}
	return rs
}

// BenchmarkPlannerWithCacheUnderRules measures a cache hit for a query
// invoking a recursive predicate under a rule set, the invocation free and
// bound by a query input: the first plan and the bodies it plans enter the
// cache, and every plan after it is served.
func BenchmarkPlannerWithCacheUnderRules(b *testing.B) {
	find := []query.FindElement{query.FindVariable{Symbol: datalog.NewSymbol("?y")}}
	queries := []struct {
		name string
		q    *query.Query
	}{
		{"free", &query.Query{
			Find:  find,
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
			Where: []query.Clause{invocation("reach", "?x", "?y")},
		}},
		{"bound", &query.Query{
			Find:  find,
			In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: datalog.NewSymbol("?x")}},
			Where: []query.Clause{invocation("reach", "?x", "?y")},
		}},
	}
	for _, tc := range queries {
		b.Run(tc.name, func(b *testing.B) {
			cache := NewPlanCache(1000, 0)
			opts := PlannerOptions{}
			opts.Cache = cache
			planner := NewClauseBasedPlanner(nil, opts)
			bindings := query.Bindings{Rules: reachRuleSet(b)}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := planner.Plan(tc.q, bindings, nil); err != nil {
					b.Fatalf("Plan: %v", err)
				}
			}

			hits, misses, size := cache.Stats()
			if hits != int64(b.N-1) || misses != int64(size) {
				b.Fatalf("cache stats: hits=%d, misses=%d, size=%d; want every plan after the first served and one entry per miss", hits, misses, size)
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
