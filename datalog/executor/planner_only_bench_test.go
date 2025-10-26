package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkPlannerOnly measures ONLY the planning overhead (no execution)
// This isolates planning time from execution time to understand planner efficiency
func BenchmarkPlannerOnly(b *testing.B) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "simple_pattern",
			query: `[:find ?name ?age
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]]`,
		},
		{
			name: "join_with_predicate",
			query: `[:find ?name ?age
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]
			                [(> ?age 25)]]`,
		},
		{
			name: "aggregation",
			query: `[:find ?name (max ?age)
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]]`,
		},
		{
			name: "expression_and_filter",
			query: `[:find ?name ?doubled
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]
			                [(* ?age 2) ?doubled]
			                [(> ?doubled 50)]]`,
		},
		{
			name: "multi_join",
			query: `[:find ?customer-name ?product-name ?price
			         :where [?person :person/name ?customer-name]
			                [?person :person/age ?age]
			                [(>= ?age 30)]
			                [?order :order/customer ?person]
			                [?order :order/product ?product]
			                [?product :product/name ?product-name]
			                [?product :product/price ?price]]`,
		},
		{
			name: "multiple_expressions",
			query: `[:find ?name ?result
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]
			                [?e :person/score ?score]
			                [(* ?age 2) ?doubled]
			                [(+ ?doubled ?score) ?result]
			                [(> ?result 100)]]`,
		},
	}

	for _, tt := range tests {
		q, err := parser.ParseQuery(tt.query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		b.Run(tt.name+"/old_planner", func(b *testing.B) {
			// Create old planner (phase-based)
			oldPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
				UseClauseBasedPlanner: false,
			})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := oldPlanner.PlanQuery(q)
				if err != nil {
					b.Fatalf("Planning failed: %v", err)
				}
			}
		})

		b.Run(tt.name+"/new_planner", func(b *testing.B) {
			// Create new planner (clause-based)
			newPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
				UseClauseBasedPlanner: true,
			})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := newPlanner.PlanQuery(q)
				if err != nil {
					b.Fatalf("Planning failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPlannerOnlySubqueries measures planning overhead for queries with subqueries
func BenchmarkPlannerOnlySubqueries(b *testing.B) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name: "single_subquery",
			query: `[:find ?name ?max-age
			         :where [?p :person/name ?name]
			                [(q [:find (max ?age)
			                     :in $ ?n
			                     :where [?e :person/name ?n]
			                            [?e :person/age ?age]]
			                    $ ?name) [[?max-age]]]]`,
		},
		{
			name: "two_subqueries",
			query: `[:find ?name ?max ?min
			         :where [?p :person/name ?name]
			                [(q [:find (max ?age)
			                     :in $ ?n
			                     :where [?e :person/name ?n]
			                            [?e :person/age ?age]]
			                    $ ?name) [[?max]]]
			                [(q [:find (min ?age)
			                     :in $ ?n
			                     :where [?e :person/name ?n]
			                            [?e :person/age ?age]]
			                    $ ?name) [[?min]]]]`,
		},
		{
			name: "ohlc_style_query",
			query: `[:find ?hour ?high ?low
			         :where
			           [?s :symbol/ticker "AAPL"]
			           [?b :price/symbol ?s]
			           [?b :price/hour ?hour]

			           [(q [:find (max ?h)
			                :in $ [[?sym ?hr] ...]
			                :where
			                  [?bar :price/symbol ?sym]
			                  [?bar :price/hour ?hr]
			                  [?bar :price/high ?h]]
			               $ ?s ?hour) [[?high]]]

			           [(q [:find (min ?l)
			                :in $ [[?sym ?hr] ...]
			                :where
			                  [?bar :price/symbol ?sym]
			                  [?bar :price/hour ?hr]
			                  [?bar :price/low ?l]]
			               $ ?s ?hour) [[?low]]]]`,
		},
	}

	for _, tt := range tests {
		q, err := parser.ParseQuery(tt.query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		b.Run(tt.name+"/old_planner", func(b *testing.B) {
			oldPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
				UseClauseBasedPlanner: false,
			})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := oldPlanner.PlanQuery(q)
				if err != nil {
					b.Fatalf("Planning failed: %v", err)
				}
			}
		})

		b.Run(tt.name+"/new_planner", func(b *testing.B) {
			newPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
				UseClauseBasedPlanner: true,
			})

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := newPlanner.PlanQuery(q)
				if err != nil {
					b.Fatalf("Planning failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPlannerOnlyComplexQueries measures planning overhead for complex queries
func BenchmarkPlannerOnlyComplexQueries(b *testing.B) {
	// Complex query with many clauses to stress-test the planner
	complexQuery := `[:find ?name ?total
	                  :where
	                    [?p :person/name ?name]
	                    [?p :person/age ?age]
	                    [?p :person/score ?score]
	                    [(> ?age 20)]
	                    [(< ?age 40)]
	                    [(* ?age 2) ?age2]
	                    [(+ ?score ?age2) ?combined]
	                    [?o1 :order/customer ?p]
	                    [?o1 :order/product ?prod1]
	                    [?prod1 :product/price ?price1]
	                    [?o2 :order/customer ?p]
	                    [?o2 :order/product ?prod2]
	                    [?prod2 :product/price ?price2]
	                    [(+ ?price1 ?price2) ?total]
	                    [(> ?total 50)]]`

	q, err := parser.ParseQuery(complexQuery)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.Run("old_planner", func(b *testing.B) {
		oldPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
			UseClauseBasedPlanner: false,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := oldPlanner.PlanQuery(q)
			if err != nil {
				b.Fatalf("Planning failed: %v", err)
			}
		}
	})

	b.Run("new_planner", func(b *testing.B) {
		newPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
			UseClauseBasedPlanner: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := newPlanner.PlanQuery(q)
			if err != nil {
				b.Fatalf("Planning failed: %v", err)
			}
		}
	})
}

// BenchmarkPlannerOnlyWithCache measures planning overhead with cache enabled
func BenchmarkPlannerOnlyWithCache(b *testing.B) {
	query := `[:find ?name ?age
	           :where [?e :person/name ?name]
	                  [?e :person/age ?age]
	                  [(> ?age 25)]]`

	q, err := parser.ParseQuery(query)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.Run("old_planner/no_cache", func(b *testing.B) {
		oldPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
			UseClauseBasedPlanner: false,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := oldPlanner.PlanQuery(q)
			if err != nil {
				b.Fatalf("Planning failed: %v", err)
			}
		}
	})

	b.Run("old_planner/with_cache", func(b *testing.B) {
		oldPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
			UseClauseBasedPlanner: false,
		})
		cache := planner.NewPlanCache(1000, 5*time.Minute)
		oldPlanner.SetCache(cache)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := oldPlanner.PlanQuery(q)
			if err != nil {
				b.Fatalf("Planning failed: %v", err)
			}
		}
	})

	b.Run("new_planner/no_cache", func(b *testing.B) {
		newPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
			UseClauseBasedPlanner: true,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := newPlanner.PlanQuery(q)
			if err != nil {
				b.Fatalf("Planning failed: %v", err)
			}
		}
	})

	b.Run("new_planner/with_cache", func(b *testing.B) {
		newPlanner := planner.CreatePlanner(nil, planner.PlannerOptions{
			UseClauseBasedPlanner: true,
		})
		cache := planner.NewPlanCache(1000, 5*time.Minute)
		newPlanner.SetCache(cache)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := newPlanner.PlanQuery(q)
			if err != nil {
				b.Fatalf("Planning failed: %v", err)
			}
		}
	})
}
