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
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// BenchmarkConditionalAggregateRewriting measures the performance impact
// of conditional aggregate rewriting optimization
func BenchmarkConditionalAggregateRewriting(b *testing.B) {
	dir, err := os.MkdirTemp("", "cond-agg-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data: events with timestamps
	// Scale this up to make the benchmark more realistic
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
		b.Fatalf("Failed to parse query: %v", err)
	}

	// Benchmark WITH rewriting FIRST (to check cache issues)
	b.Run("With rewriting", func(b *testing.B) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: true,
			EnableSubqueryDecorrelation:         false, // Disable to isolate conditional aggregate rewriting
		}
		exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query execution failed: %v", err)
			}
			// Ensure result is materialized
			_ = result.Size()
		}
	})

	// Benchmark WITHOUT rewriting (baseline)
	b.Run("Without rewriting", func(b *testing.B) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: false,
			EnableSubqueryDecorrelation:         false, // Disable to isolate conditional aggregate rewriting
		}
		exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result, err := exec.Execute(q)
			if err != nil {
				b.Fatalf("Query execution failed: %v", err)
			}
			// Ensure result is materialized
			_ = result.Size()
		}
	})
}
