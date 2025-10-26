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

// TestConditionalAggregateDebug debugs the non-deterministic failure
func TestConditionalAggregateDebug(t *testing.T) {
	// Run the test multiple times to catch non-deterministic behavior
	for iteration := 0; iteration < 10; iteration++ {
		t.Logf("\n=== Iteration %d ===", iteration)

		dir, err := os.MkdirTemp("", "cond-agg-debug-*")
		if err != nil {
			t.Fatal(err)
		}

		db, err := storage.NewDatabase(dir)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
			os.RemoveAll(dir)
			continue
		}

		// Create test data - same as benchmark
		tx := db.NewTransaction()
		people := make([]datalog.Identity, 3)
		for i := 0; i < 3; i++ {
			person := datalog.NewIdentity(fmt.Sprintf("person:%d", i))
			tx.Add(person, datalog.NewKeyword(":person/name"), fmt.Sprintf("Person %d", i))
			people[i] = person
		}

		eventID := 0
		for personIdx, person := range people {
			for day := 1; day <= 10; day++ {
				for eventNum := 0; eventNum < 20; eventNum++ {
					e := datalog.NewIdentity(fmt.Sprintf("event:%d", eventID))
					tx.Add(e, datalog.NewKeyword(":event/person"), person)
					tx.Add(e, datalog.NewKeyword(":event/time"), time.Date(2025, 1, day, 10+eventNum/10, eventNum%10, 0, 0, time.UTC))
					value := int64((personIdx+1)*100 + eventNum)
					tx.Add(e, datalog.NewKeyword(":event/value"), value)
					eventID++
				}
			}
		}

		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Failed to commit: %v", err)
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
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Test with conditional aggregate rewriting but streaming DISABLED
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: true,
			EnableSubqueryDecorrelation:         false,
			EnableIteratorComposition:           false,
			EnableTrueStreaming:                 false,
			EnableStreamingAggregation:          false,
		}

		// Create planner first to inspect the plan
		p := planner.NewPlanner(nil, opts)
		plan, err := p.Plan(q)
		if err != nil {
			t.Fatalf("Planning failed: %v", err)
		}

		// Log plan details
		t.Logf("Plan has %d phases", len(plan.Phases))
		for i, phase := range plan.Phases {
			t.Logf("Phase %d:", i)
			t.Logf("  Available: %v", phase.Available)
			t.Logf("  Provides (len=%d): %v", len(phase.Provides), phase.Provides)
			t.Logf("  Keep: %v", phase.Keep)
			t.Logf("  Patterns: %d", len(phase.Patterns))
			t.Logf("  Expressions: %d", len(phase.Expressions))
			for j, expr := range phase.Expressions {
				t.Logf("    Expr %d: output=%v, inputs=%v", j, expr.Output, expr.Inputs)
			}
			if phase.Metadata != nil {
				if aggCols, ok := phase.Metadata["aggregate_required_columns"]; ok {
					t.Logf("  Metadata aggregate_required_columns: %v", aggCols)
				}
			}
		}

		// Now execute
		exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)
		result, err := exec.Execute(q)

		db.Close()
		os.RemoveAll(dir)

		if err != nil {
			t.Logf("Iteration %d FAILED: %v", iteration, err)
			// Don't fail immediately - let's see if pattern emerges
			continue
		}

		if result.Size() != 30 {
			t.Logf("Iteration %d: Wrong result count: expected 30, got %d", iteration, result.Size())
		} else {
			t.Logf("Iteration %d: SUCCESS (%d rows)", iteration, result.Size())
		}
	}
}
