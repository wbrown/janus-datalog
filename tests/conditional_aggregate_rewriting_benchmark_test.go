package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// setupConditionalAggregateBenchmark creates the test database and query
func setupConditionalAggregateBenchmark(b *testing.B) (*storage.Database, *query.Query, func()) {
	dir, err := os.MkdirTemp("", "cond-agg-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	db, err := storage.NewDatabase(dir)
	if err != nil {
		os.RemoveAll(dir)
		b.Fatalf("Failed to create database: %v", err)
	}

	// Insert test data: events with timestamps
	tx := db.NewTransaction()

	// Create 3 people
	people := make([]datalog.Identity, 3)
	for i := 0; i < 3; i++ {
		person := datalog.NewIdentity(fmt.Sprintf("person:%d", i))
		tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Person %d", i))
		people[i] = person
	}

	// Create 20 events per person across 10 days
	eventID := 0
	for personIdx, person := range people {
		for day := 1; day <= 10; day++ {
			for eventNum := 0; eventNum < 20; eventNum++ {
				e := datalog.NewIdentity(fmt.Sprintf("event:%d", eventID))
				tx.Add(e, datalog.NewKeyword(":event/person"), person)
				tx.Add(e, datalog.NewKeyword(":event/time"), time.Date(2025, 1, day, 10+eventNum/10, eventNum%10, 0, 0, time.UTC))
				// Value varies by person and event
				value := int64((personIdx+1)*100 + eventNum)
				tx.Add(e, datalog.NewKeyword(":event/value"), value)
				eventID++
			}
		}
	}

	_, err = tx.Commit()
	if err != nil {
		db.Close()
		os.RemoveAll(dir)
		b.Fatalf("Failed to commit: %v", err)
	}

	// Query: get max value per person per day
	queryStr := `[:find ?name ?day ?max-value
	             :where
	             [?p :person/name ?name]
	             [?e :event/person ?p]
	             [?e :event/time ?time]
	             [(day ?time) ?day]

	             ; Subquery: max value for this person and day
	             [(q [:find (max ?v)
	                  :in $ ?person ?d
	                  :where
	                  [?ev :event/person ?person]
	                  [?ev :event/time ?t]
	                  [(day ?t) ?pd]
	                  [(= ?pd ?d)]
	                  [?ev :event/value ?v]]
	               $ ?p ?day) [[?max-value]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		db.Close()
		os.RemoveAll(dir)
		b.Fatalf("Failed to parse query: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return db, q, cleanup
}

// BenchmarkConditionalAggregateRewriting measures the performance impact
// of conditional aggregate rewriting optimization
func BenchmarkConditionalAggregateRewriting(b *testing.B) {
	db, q, cleanup := setupConditionalAggregateBenchmark(b)
	defer cleanup()

	matcher := storage.NewBadgerMatcher(db.Store())

	// Benchmark WITH rewriting
	b.Run("With_rewriting", func(b *testing.B) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: true,
			EnableSubqueryDecorrelation:         false,
		}
		exec := executor.NewExecutorWithOptions(matcher, opts)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query execution failed: %v", err)
			}
			_ = result.Size()
		}
	})

	// Benchmark WITHOUT rewriting (baseline)
	b.Run("Without_rewriting", func(b *testing.B) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: false,
			EnableSubqueryDecorrelation:         false,
		}
		exec := executor.NewExecutorWithOptions(matcher, opts)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query execution failed: %v", err)
			}
			_ = result.Size()
		}
	})
}

// BenchmarkConditionalAggregateExecutorComparison compares legacy vs QueryExecutor
// for conditional aggregate rewriting performance
func BenchmarkConditionalAggregateExecutorComparison(b *testing.B) {
	db, q, cleanup := setupConditionalAggregateBenchmark(b)
	defer cleanup()

	matcher := storage.NewBadgerMatcher(db.Store())

	// Test matrix: executor × rewriting
	testCases := []struct {
		name       string
		useLegacy  bool
		rewriting  bool
	}{
		{"Legacy_WithRewriting", true, true},
		{"Legacy_NoRewriting", true, false},
		{"QueryExecutor_WithRewriting", false, true},
		{"QueryExecutor_NoRewriting", false, false},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			opts := planner.PlannerOptions{
				EnableDynamicReordering:             true,
				EnableConditionalAggregateRewriting: tc.rewriting,
				EnableSubqueryDecorrelation:         false,
				UseLegacyExecutor:                   tc.useLegacy,
			}
			exec := executor.NewExecutorWithOptions(matcher, opts)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(q)
				if err != nil {
					b.Fatalf("Query execution failed: %v", err)
				}
				_ = result.Size()
			}
		})
	}
}

// BenchmarkConditionalAggregateScale tests how the optimization scales with data size
func BenchmarkConditionalAggregateScale(b *testing.B) {
	scales := []struct {
		name         string
		people       int
		days         int
		eventsPerDay int
	}{
		{"Small_3p_10d_20e", 3, 10, 20},   // 600 events, 30 groups
		{"Medium_5p_20d_30e", 5, 20, 30},  // 3,000 events, 100 groups
	}

	for _, scale := range scales {
		b.Run(scale.name, func(b *testing.B) {
			dir, err := os.MkdirTemp("", "cond-agg-scale-*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)

			db, err := storage.NewDatabase(dir)
			if err != nil {
				b.Fatalf("Failed to create database: %v", err)
			}
			defer db.Close()

			// Insert scaled test data
			tx := db.NewTransaction()

			people := make([]datalog.Identity, scale.people)
			for i := 0; i < scale.people; i++ {
				person := datalog.NewIdentity(fmt.Sprintf("person:%d", i))
				tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Person %d", i))
				people[i] = person
			}

			eventID := 0
			for personIdx, person := range people {
				for day := 1; day <= scale.days; day++ {
					for eventNum := 0; eventNum < scale.eventsPerDay; eventNum++ {
						e := datalog.NewIdentity(fmt.Sprintf("event:%d", eventID))
						tx.Add(e, datalog.NewKeyword(":event/person"), person)
						tx.Add(e, datalog.NewKeyword(":event/time"), time.Date(2025, 1, day, 10, eventNum, 0, 0, time.UTC))
						value := int64((personIdx+1)*100 + eventNum)
						tx.Add(e, datalog.NewKeyword(":event/value"), value)
						eventID++
					}
				}
			}

			_, err = tx.Commit()
			if err != nil {
				b.Fatalf("Failed to commit: %v", err)
			}

			queryStr := `[:find ?name ?day ?max-value
			             :where
			             [?p :person/name ?name]
			             [?e :event/person ?p]
			             [?e :event/time ?time]
			             [(day ?time) ?day]
			             [(q [:find (max ?v)
			                  :in $ ?person ?d
			                  :where
			                  [?ev :event/person ?person]
			                  [?ev :event/time ?t]
			                  [(day ?t) ?pd]
			                  [(= ?pd ?d)]
			                  [?ev :event/value ?v]]
			               $ ?p ?day) [[?max-value]]]]`

			q, err := parser.ParseQuery(queryStr)
			if err != nil {
				b.Fatalf("Failed to parse query: %v", err)
			}

			matcher := storage.NewBadgerMatcher(db.Store())

			// Compare both executors with rewriting enabled
			for _, variant := range executor.DualTestExecutorVariants() {
				b.Run(variant.Name, func(b *testing.B) {
					opts := planner.PlannerOptions{
						EnableDynamicReordering:             true,
						EnableConditionalAggregateRewriting: true,
						EnableSubqueryDecorrelation:         false,
						UseLegacyExecutor:                   variant.Opts.UseLegacyExecutor,
					}
					exec := executor.NewExecutorWithOptions(matcher, opts)

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						result, err := exec.Execute(q)
						if err != nil {
							b.Fatalf("Query execution failed: %v", err)
						}
						_ = result.Size()
					}
				})
			}
		})
	}
}
