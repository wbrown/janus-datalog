package tests

import (
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestConditionalAggregateRewritingE2E tests the complete rewriting pipeline
// Uses a pattern that matches what the rewriter expects: expression + equality predicate
//
// CRITICAL TEST: This specifically covers the multi-phase metadata propagation bug where:
// - Phase 0: Executes conditional aggregate rewriting, stores metadata with aggregate_required_columns
// - Phase 1: Adds person name lookup [?p :person/name ?name]
// - Bug: Phase 1 would drop ?v and ?__cond_?pd because it didn't propagate metadata from Phase 0
// - Fix: updatePhaseSymbols now checks ALL previous phases for aggregate_required_columns
//
// This test would fail before the fix (returning "Alice" instead of 150) and passes after.
// See: docs/bugs/resolved/CONDITIONAL_AGGREGATE_REWRITING_BUG.md for full details.
func TestConditionalAggregateRewritingE2E(t *testing.T) {
	dir, err := os.MkdirTemp("", "cond-agg-e2e-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data: events with timestamps
	tx := db.NewTransaction()

	person := datalog.NewIdentity("person:1")
	tx.Add(person, datalog.NewKeyword(":person/name"), "Alice")

	// Day 15 events
	e1 := datalog.NewIdentity("event:1")
	tx.Add(e1, datalog.NewKeyword(":event/person"), person)
	tx.Add(e1, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC))
	tx.Add(e1, datalog.NewKeyword(":event/value"), int64(100))

	e2 := datalog.NewIdentity("event:2")
	tx.Add(e2, datalog.NewKeyword(":event/person"), person)
	tx.Add(e2, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 14, 0, 0, 0, time.UTC))
	tx.Add(e2, datalog.NewKeyword(":event/value"), int64(150))

	// Day 16 events
	e3 := datalog.NewIdentity("event:3")
	tx.Add(e3, datalog.NewKeyword(":event/person"), person)
	tx.Add(e3, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 16, 10, 0, 0, 0, time.UTC))
	tx.Add(e3, datalog.NewKeyword(":event/value"), int64(200))

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query matching EXACTLY the unit test structure
	// Get all events and compute max value for specific day
	queryStr := `[:find ?name ?day ?max-value
	             :where
	             [?p :person/name ?name]
	             [?e :event/person ?p]
	             [?e :event/time ?time]
	             [(day ?time) ?day]

	             ; Subquery: max value for this day (correlated)
	             ; Matches unit test pattern exactly
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

	// Test WITHOUT rewriting (baseline)
	t.Run("Without rewriting", func(t *testing.T) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: false,
		}
		exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// Should get 2 rows: one for each day
		// Day 15: max=150, Day 16: max=200
		if result.Size() != 2 {
			t.Errorf("Expected 2 rows, got %d", result.Size())
		}

		dayMaxes := make(map[int64]int64)
		it := result.Iterator()
		defer it.Close()

		for it.Next() {
			tuple := it.Tuple()
			name := tuple[0].(string)
			day := tuple[1].(int64)
			maxValue := tuple[2].(int64)

			if name != "Alice" {
				t.Errorf("Expected name Alice, got %v", name)
			}
			dayMaxes[day] = maxValue
		}

		if dayMaxes[15] != 150 {
			t.Errorf("Day 15: expected max 150, got %v", dayMaxes[15])
		}
		if dayMaxes[16] != 200 {
			t.Errorf("Day 16: expected max 200, got %v", dayMaxes[16])
		}
		t.Logf("✓ Without rewriting: day 15 max=%d, day 16 max=%d", dayMaxes[15], dayMaxes[16])
	})

	// Test WITH rewriting (optimized)
	t.Run("With rewriting", func(t *testing.T) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: true,
		}
		exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

		// Track annotations
		var rewriteDetected bool
		handler := func(event annotations.Event) {
			if event.Name == "query/rewrite.conditional-aggregates" {
				rewriteDetected = true
				t.Logf("✓ Rewriting annotation detected: %v", event.Data)
			}
		}

		ctx := executor.NewContext(handler)
		result, err := exec.ExecuteWithContext(ctx, q)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if !rewriteDetected {
			t.Error("Expected rewriting annotation, but none was found")
		}

		// Should get same results as without rewriting
		if result.Size() != 2 {
			t.Errorf("Expected 2 rows, got %d", result.Size())
		}

		dayMaxes := make(map[int64]int64)
		it := result.Iterator()
		defer it.Close()

		for it.Next() {
			tuple := it.Tuple()
			t.Logf("DEBUG Result tuple: len=%d, %v", len(tuple), tuple)
			for i, val := range tuple {
				t.Logf("  [%d] %v (%T)", i, val, val)
			}
			name := tuple[0].(string)
			day := tuple[1].(int64)
			maxValue := tuple[2].(int64)

			if name != "Alice" {
				t.Errorf("Expected name Alice, got %v", name)
			}
			dayMaxes[day] = maxValue
		}

		if dayMaxes[15] != 150 {
			t.Errorf("Day 15: expected max 150, got %v", dayMaxes[15])
		}
		if dayMaxes[16] != 200 {
			t.Errorf("Day 16: expected max 200, got %v", dayMaxes[16])
		}
		t.Logf("✓ With rewriting: day 15 max=%d, day 16 max=%d (same as without rewriting)", dayMaxes[15], dayMaxes[16])
	})
}
