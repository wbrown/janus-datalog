package tests

import (
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestPhaseReorderingDebug prints detailed phase information to understand reordering
func TestPhaseReorderingDebug(t *testing.T) {
	dir, err := os.MkdirTemp("", "phase-reorder-debug-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Minimal test data
	tx := db.NewTransaction()
	alice := datalog.NewIdentity("person:alice")
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
	e := datalog.NewIdentity("event:1")
	tx.Add(e, datalog.NewKeyword(":event/person"), alice)
	tx.Add(e, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC))
	tx.Add(e, datalog.NewKeyword(":event/value"), int64(100))
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

	// Plan WITHOUT reordering
	optsWithout := planner.PlannerOptions{
		EnableDynamicReordering: false,
	}
	plannerWithout := planner.NewPlanner(nil, optsWithout)
	planWithout, err := plannerWithout.Plan(q)
	if err != nil {
		t.Fatalf("Planning without reordering failed: %v", err)
	}

	// Plan WITH reordering
	optsWith := planner.PlannerOptions{
		EnableDynamicReordering: true,
	}
	plannerWith := planner.NewPlanner(nil, optsWith)
	planWith, err := plannerWith.Plan(q)
	if err != nil {
		t.Fatalf("Planning with reordering failed: %v", err)
	}

	// Print detailed phase information
	t.Logf("\n=== Plan WITHOUT reordering ===")
	for i, phase := range planWithout.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Keep: %v", phase.Keep)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		t.Logf("  Subqueries: %d", len(phase.Subqueries))
		if len(phase.Subqueries) > 0 {
			for j, sq := range phase.Subqueries {
				t.Logf("    Subquery %d inputs: %v", j, sq.Inputs)
			}
		}
	}

	t.Logf("\n=== Plan WITH reordering ===")
	for i, phase := range planWith.Phases {
		t.Logf("Phase %d:", i)
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Keep: %v", phase.Keep)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		t.Logf("  Subqueries: %d", len(phase.Subqueries))
		if len(phase.Subqueries) > 0 {
			for j, sq := range phase.Subqueries {
				t.Logf("    Subquery %d inputs: %v", j, sq.Inputs)
			}
		}
	}

	// Try executing both plans
	t.Logf("\n=== Execution Results ===")

	// Without reordering
	matcherWithout := storage.NewBadgerMatcher(db.Store())
	executorWithout := executor.NewExecutorWithOptions(matcherWithout, optsWithout)
	resultWithout, err := executorWithout.Execute(q)
	if err != nil {
		t.Logf("Execution WITHOUT reordering: ERROR - %v", err)
	} else {
		t.Logf("Execution WITHOUT reordering: %d rows", resultWithout.Size())
	}

	// With reordering
	matcherWith := storage.NewBadgerMatcher(db.Store())
	executorWith := executor.NewExecutorWithOptions(matcherWith, optsWith)
	resultWith, err := executorWith.Execute(q)
	if err != nil {
		t.Logf("Execution WITH reordering: ERROR - %v", err)
	} else {
		t.Logf("Execution WITH reordering: %d rows", resultWith.Size())
	}
}
