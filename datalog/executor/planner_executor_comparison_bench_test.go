package executor

import (
	"fmt"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkFullQueryOldVsNewPlanner compares full query execution (planning + execution)
// using old planner vs new clause-based planner
func BenchmarkFullQueryOldVsNewPlanner(b *testing.B) {
	// Create test dataset
	datoms := createTestDataset(1000)
	matcher := NewMemoryPatternMatcher(datoms)

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
	}

	for _, tt := range tests {
		q, err := parser.ParseQuery(tt.query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		b.Run(tt.name+"/old_planner", func(b *testing.B) {
			exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: false,
			})
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})

		b.Run(tt.name+"/new_planner", func(b *testing.B) {
			exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: true,
			})
			exec.SetUseQueryExecutor(true)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkFullQueryOldVsNewExecutor compares full query execution
// using old executor vs QueryExecutor (both with new planner)
func BenchmarkFullQueryOldVsNewExecutor(b *testing.B) {
	// Create test dataset
	datoms := createTestDataset(1000)
	matcher := NewMemoryPatternMatcher(datoms)

	tests := []struct {
		name  string
		query string
	}{
		{
			name: "simple_join",
			query: `[:find ?name ?age
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]]`,
		},
		{
			name: "multi_attribute_join",
			query: `[:find ?name ?age ?score
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]
			                [?e :person/score ?score]]`,
		},
		{
			name: "grouped_aggregation",
			query: `[:find ?name (sum ?score)
			         :where [?e :person/name ?name]
			                [?e :person/score ?score]]`,
		},
	}

	for _, tt := range tests {
		q, err := parser.ParseQuery(tt.query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		b.Run(tt.name+"/old_executor", func(b *testing.B) {
			exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: false, // Use old planner with old executor
			})
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})

		b.Run(tt.name+"/query_executor", func(b *testing.B) {
			exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: true,
			})
			exec.SetUseQueryExecutor(true) // Use QueryExecutor
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkDecorrelationComparison compares full query execution
// with/without decorrelation optimization enabled
func BenchmarkDecorrelationComparison(b *testing.B) {
	// Create OHLC-style dataset with multiple hours
	datoms := createOHLCDataset(100, 10) // 100 hours, 10 bars per hour
	matcher := NewMemoryPatternMatcher(datoms)

	// OHLC-style query with 2 correlated subqueries
	queryStr := `[:find ?hour ?high ?low
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
	                    $ ?s ?hour) [[?low]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.Run("without_decorrelation", func(b *testing.B) {
		executor := NewQueryExecutor(matcher, ExecutorOptions{
			EnableSubqueryDecorrelation: false,
		})
		ctx := NewContext(nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := executor.Execute(ctx, q, []Relation{})
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})

	b.Run("with_decorrelation", func(b *testing.B) {
		executor := NewQueryExecutor(matcher, ExecutorOptions{
			EnableSubqueryDecorrelation: true,
		})
		ctx := NewContext(nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := executor.Execute(ctx, q, []Relation{})
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}

// BenchmarkOHLCFullComparison compares full query execution across all combinations
// on realistic OHLC-style queries: old planner+executor, new planner+executor with/without decorrelation
func BenchmarkOHLCFullComparison(b *testing.B) {
	// Create larger OHLC dataset for realistic testing
	datoms := createOHLCDataset(200, 20) // 200 hours, 20 bars per hour
	matcher := NewMemoryPatternMatcher(datoms)

	// OHLC query with 4 subqueries (high, low, avg open, avg close)
	// Using only available aggregates: max, min, avg, sum, count
	queryStr := `[:find ?hour ?avg_open ?high ?low ?avg_close
	              :where
	                [?s :symbol/ticker "AAPL"]
	                [?b :price/symbol ?s]
	                [?b :price/hour ?hour]

	                [(q [:find (avg ?o)
	                     :in $ [[?sym ?hr] ...]
	                     :where
	                       [?bar :price/symbol ?sym]
	                       [?bar :price/hour ?hr]
	                       [?bar :price/open ?o]]
	                    $ ?s ?hour) [[?avg_open]]]

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
	                    $ ?s ?hour) [[?low]]]

	                [(q [:find (avg ?c)
	                     :in $ [[?sym ?hr] ...]
	                     :where
	                       [?bar :price/symbol ?sym]
	                       [?bar :price/hour ?hr]
	                       [?bar :price/close ?c]]
	                    $ ?s ?hour) [[?avg_close]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	b.Run("old_planner_old_executor", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseClauseBasedPlanner: false,
		})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})

	b.Run("new_planner_query_executor_no_decorr", func(b *testing.B) {
		// Use QueryExecutor directly for precise control
		executor := NewQueryExecutor(matcher, ExecutorOptions{
			EnableSubqueryDecorrelation: false,
		})
		ctx := NewContext(nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := executor.Execute(ctx, q, []Relation{})
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})

	b.Run("new_planner_query_executor_with_decorr", func(b *testing.B) {
		// Use QueryExecutor directly for precise control
		executor := NewQueryExecutor(matcher, ExecutorOptions{
			EnableSubqueryDecorrelation: true,
		})
		ctx := NewContext(nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := executor.Execute(ctx, q, []Relation{})
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}

// BenchmarkScaleComparison compares full query execution performance
// across different data scales (small/medium/large) with/without decorrelation
func BenchmarkScaleComparison(b *testing.B) {
	scales := []struct {
		name  string
		size  int
		hours int
		bars  int
	}{
		{"small", 100, 10, 5},
		{"medium", 1000, 50, 10},
		{"large", 5000, 200, 20},
	}

	queryStr := `[:find ?hour ?high ?low
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
	                    $ ?s ?hour) [[?low]]]]`

	for _, scale := range scales {
		datoms := createOHLCDataset(scale.hours, scale.bars)
		matcher := NewMemoryPatternMatcher(datoms)

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		b.Run(scale.name+"/new_with_decorr", func(b *testing.B) {
			executor := NewQueryExecutor(matcher, ExecutorOptions{
				EnableSubqueryDecorrelation: true,
			})
			ctx := NewContext(nil)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := executor.Execute(ctx, q, []Relation{})
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})

		b.Run(scale.name+"/new_without_decorr", func(b *testing.B) {
			executor := NewQueryExecutor(matcher, ExecutorOptions{
				EnableSubqueryDecorrelation: false,
			})
			ctx := NewContext(nil)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := executor.Execute(ctx, q, []Relation{})
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPlanQuality compares plan quality by running BOTH planners through
// the SAME executor (QueryExecutor). This isolates planning improvements from
// execution improvements.
func BenchmarkPlanQuality(b *testing.B) {
	// Create test dataset
	datoms := createTestDataset(1000)
	matcher := NewMemoryPatternMatcher(datoms)

	tests := []struct {
		name  string
		query string
	}{
		{
			name: "simple_join",
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
			query: `[:find ?name ?age ?score
			         :where [?e :person/name ?name]
			                [?e :person/age ?age]
			                [?e :person/score ?score]]`,
		},
	}

	for _, tt := range tests {
		q, err := parser.ParseQuery(tt.query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}

		// OLD planner → QueryExecutor
		b.Run(tt.name+"/old_planner_query_executor", func(b *testing.B) {
			exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: false,
			})
			exec.SetUseQueryExecutor(true) // Force use of QueryExecutor
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})

		// NEW planner → QueryExecutor
		b.Run(tt.name+"/new_planner_query_executor", func(b *testing.B) {
			exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
				UseClauseBasedPlanner: true,
			})
			exec.SetUseQueryExecutor(true)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkPlanQualityOHLC compares plan quality on OHLC-style queries
// using the same executor (QueryExecutor) to isolate planning improvements
func BenchmarkPlanQualityOHLC(b *testing.B) {
	// Create OHLC dataset
	datoms := createOHLCDataset(100, 10) // 100 hours, 10 bars per hour
	matcher := NewMemoryPatternMatcher(datoms)

	// OHLC query with 2 subqueries
	queryStr := `[:find ?hour ?high ?low
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
	                    $ ?s ?hour) [[?low]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Failed to parse query: %v", err)
	}

	// OLD planner → QueryExecutor (no decorrelation)
	b.Run("old_planner_query_executor", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseClauseBasedPlanner: false,
		})
		exec.SetUseQueryExecutor(true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})

	// NEW planner → QueryExecutor (no decorrelation)
	b.Run("new_planner_query_executor", func(b *testing.B) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseClauseBasedPlanner: true,
		})
		exec.SetUseQueryExecutor(true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}

// Helper functions

func createTestDataset(size int) []datalog.Datom {
	var datoms []datalog.Datom
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	scoreAttr := datalog.NewKeyword(":person/score")

	for i := 0; i < size; i++ {
		person := datalog.NewIdentity(fmt.Sprintf("person:%d", i))
		datoms = append(datoms,
			datalog.Datom{E: person, A: nameAttr, V: fmt.Sprintf("Person%d", i), Tx: 1},
			datalog.Datom{E: person, A: ageAttr, V: int64(20 + i%40), Tx: 1},
			datalog.Datom{E: person, A: scoreAttr, V: float64(50 + i%50), Tx: 1},
		)
	}
	return datoms
}

func createOHLCDataset(hours, barsPerHour int) []datalog.Datom {
	var datoms []datalog.Datom
	symbol := datalog.NewIdentity("AAPL")

	// Add symbol ticker
	datoms = append(datoms,
		datalog.Datom{E: symbol, A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
	)

	// Add price bars
	for hour := 0; hour < hours; hour++ {
		for bar := 0; bar < barsPerHour; bar++ {
			barID := datalog.NewIdentity(fmt.Sprintf("bar:%d:%d", hour, bar))
			basePrice := 100.0 + float64(hour)*0.5
			datoms = append(datoms,
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/symbol"), V: symbol, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/hour"), V: int64(hour), Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/open"), V: basePrice + float64(bar)*0.1, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/high"), V: basePrice + float64(bar)*0.1 + 2.0, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/low"), V: basePrice + float64(bar)*0.1 - 1.0, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/close"), V: basePrice + float64(bar)*0.1 + 0.5, Tx: 1},
			)
		}
	}

	return datoms
}
