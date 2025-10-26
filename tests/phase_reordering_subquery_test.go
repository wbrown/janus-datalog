package tests

import (
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestPhaseReorderingPreservesSubqueries verifies that phase reordering
// doesn't lose subquery information
func TestPhaseReorderingPreservesSubqueries(t *testing.T) {
	dir, err := os.MkdirTemp("", "phase-reorder-subq-*")
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

	// Query with subquery
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

	// Count subqueries in both plans
	subqCountWithout := 0
	for _, phase := range planWithout.Phases {
		subqCountWithout += len(phase.Subqueries)
	}

	subqCountWith := 0
	for _, phase := range planWith.Phases {
		subqCountWith += len(phase.Subqueries)
	}

	t.Logf("Subqueries WITHOUT reordering: %d", subqCountWithout)
	t.Logf("Subqueries WITH reordering: %d", subqCountWith)

	if subqCountWithout == 0 {
		t.Fatal("Query should have subqueries, but plan without reordering has 0")
	}

	if subqCountWith != subqCountWithout {
		t.Errorf("Phase reordering lost subqueries! Without: %d, With: %d",
			subqCountWithout, subqCountWith)

		// Print detailed phase information
		t.Logf("\n=== Plan WITHOUT reordering ===")
		for i, phase := range planWithout.Phases {
			t.Logf("Phase %d: %d patterns, %d subqueries", i, len(phase.Patterns), len(phase.Subqueries))
			for j, sq := range phase.Subqueries {
				t.Logf("  Subquery %d: inputs=%v", j, sq.Inputs)
			}
		}

		t.Logf("\n=== Plan WITH reordering ===")
		for i, phase := range planWith.Phases {
			t.Logf("Phase %d: %d patterns, %d subqueries", i, len(phase.Patterns), len(phase.Subqueries))
			for j, sq := range phase.Subqueries {
				t.Logf("  Subquery %d: inputs=%v", j, sq.Inputs)
			}
		}
	}
}

// TestPhaseReorderingSubqueryInputs verifies that subquery input symbols
// are properly preserved after reordering
func TestPhaseReorderingSubqueryInputs(t *testing.T) {
	dir, err := os.MkdirTemp("", "phase-reorder-inputs-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Minimal data
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

	// Plan with reordering
	opts := planner.PlannerOptions{
		EnableDynamicReordering: true,
	}
	p := planner.NewPlanner(nil, opts)
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed: %v", err)
	}

	// Find the phase with subqueries
	for i, phase := range plan.Phases {
		for j, sq := range phase.Subqueries {
			// Subquery inputs must be available to the phase
			// (in Available or Provides, or be the database parameter "$")
			availableSet := make(map[string]bool)
			for _, sym := range phase.Available {
				availableSet[string(sym)] = true
			}
			for _, sym := range phase.Provides {
				availableSet[string(sym)] = true
			}

			for _, input := range sq.Inputs {
				// Skip database parameter - it's always available
				if input == "$" {
					continue
				}
				// All other inputs must be in Available or Provides
				if !availableSet[string(input)] {
					t.Errorf("Phase %d, Subquery %d: input symbol %v not available",
						i, j, input)
					t.Logf("  Available: %v", phase.Available)
					t.Logf("  Provides: %v", phase.Provides)
					t.Logf("  Keep: %v", phase.Keep)
				}
			}
		}
	}
}
