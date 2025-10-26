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

// TestOptimizationComposition verifies that optimizations compose correctly
// This is a fundamental property: if Optimization A works and Optimization B works,
// then A + B should also work and produce identical results.
func TestOptimizationComposition(t *testing.T) {
	dir, err := os.MkdirTemp("", "opt-composition-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Setup test data: people and events
	tx := db.NewTransaction()

	alice := datalog.NewIdentity("person:alice")
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")

	// Create events for day 15 and day 16
	for day := 15; day <= 16; day++ {
		for eventNum := 0; eventNum < 10; eventNum++ {
			e := datalog.NewIdentity(fmt.Sprintf("event:d%d-e%d", day, eventNum))
			tx.Add(e, datalog.NewKeyword(":event/person"), alice)
			tx.Add(e, datalog.NewKeyword(":event/time"), time.Date(2025, 1, day, 10, 0, 0, 0, time.UTC))
			// Values: day 15 has max 150, day 16 has max 200
			value := int64(day*10 + eventNum)
			tx.Add(e, datalog.NewKeyword(":event/value"), value)
		}
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query: get max value per person per day using subquery
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
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Test all combinations of optimizations
	testCases := []struct {
		name         string
		opts         planner.PlannerOptions
		shouldPass   bool
		expectedRows int
	}{
		{
			name: "Baseline (no optimizations)",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:             false,
				EnableConditionalAggregateRewriting: false,
				EnableSubqueryDecorrelation:         false, // Disable to isolate the bug
			},
			shouldPass:   true,
			expectedRows: 2, // Alice day 15, Alice day 16
		},
		{
			name: "Phase reordering only",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:             true,
				EnableConditionalAggregateRewriting: false,
				EnableSubqueryDecorrelation:         false,
			},
			shouldPass:   true,
			expectedRows: 2,
		},
		{
			name: "Conditional aggregates only",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:             false,
				EnableConditionalAggregateRewriting: true,
				EnableSubqueryDecorrelation:         false,
			},
			shouldPass:   true,
			expectedRows: 2,
		},
		{
			name: "Both optimizations (COMPOSITION TEST)",
			opts: planner.PlannerOptions{
				EnableDynamicReordering:             true,
				EnableConditionalAggregateRewriting: true,
				EnableSubqueryDecorrelation:         false,
			},
			shouldPass:   true, // Should pass but currently fails!
			expectedRows: 2,
		},
	}

	// Expected results
	expectedResults := map[string]map[int64]int64{
		"Alice": {
			15: 159, // day 15, event 9: 15*10 + 9 = 159
			16: 169, // day 16, event 9: 16*10 + 9 = 169
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), tc.opts)

			result, err := exec.Execute(q)
			if err != nil {
				if tc.shouldPass {
					t.Fatalf("Query execution failed: %v", err)
				} else {
					t.Logf("Expected failure: %v", err)
					return
				}
			}

			if !tc.shouldPass {
				t.Fatalf("Query should have failed but succeeded")
			}

			// Check result count
			size := result.Size()
			if size != tc.expectedRows {
				t.Errorf("Expected %d rows, got %d", tc.expectedRows, size)
			}

			// Check result values
			iter := result.Iterator()
			defer iter.Close()

			rowCount := 0
			for iter.Next() {
				tuple := iter.Tuple()
				rowCount++

				name, ok := tuple[0].(string)
				if !ok {
					t.Errorf("Row %d: expected string name, got %T", rowCount, tuple[0])
					continue
				}

				day, ok := tuple[1].(int64)
				if !ok {
					t.Errorf("Row %d: expected int64 day, got %T", rowCount, tuple[1])
					continue
				}

				maxValue, ok := tuple[2].(int64)
				if !ok {
					t.Errorf("Row %d: expected int64 max value, got %T", rowCount, tuple[2])
					continue
				}

				expectedMax, exists := expectedResults[name][day]
				if !exists {
					t.Errorf("Row %d: unexpected person/day: %s/%d", rowCount, name, day)
					continue
				}

				if maxValue != expectedMax {
					t.Errorf("Row %d: %s day %d: expected max %d, got %d",
						rowCount, name, day, expectedMax, maxValue)
				}
			}

			// Note: Iterator interface doesn't have Error() method in this implementation
		})
	}
}

// TestMetadataInvariantAfterReordering verifies that phase metadata remains valid
// after phase reordering. This checks the representation invariant:
// "All symbols in phase metadata must be valid symbols in that phase"
func TestMetadataInvariantAfterReordering(t *testing.T) {
	dir, err := os.MkdirTemp("", "metadata-invariant-*")
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

	// Plan with both optimizations
	opts := planner.PlannerOptions{
		EnableDynamicReordering:             true,
		EnableConditionalAggregateRewriting: true,
		EnableSubqueryDecorrelation:         false,
	}

	// Create planner and plan the query
	p := planner.NewPlanner(nil, opts) // nil statistics
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed: %v", err)
	}

	// Check metadata invariant for all phases
	for i, phase := range plan.Phases {
		if phase.Metadata == nil {
			continue
		}

		// Build set of available symbols
		available := make(map[string]bool)
		for _, sym := range phase.Available {
			available[string(sym)] = true
		}
		for _, sym := range phase.Provides {
			available[string(sym)] = true
		}

		// Check aggregate_required_columns
		if aggCols, ok := phase.Metadata["aggregate_required_columns"]; ok {
			if cols, ok := aggCols.([]string); ok {
				for _, sym := range cols {
					if !available[sym] {
						t.Errorf("Phase %d: metadata references undefined symbol %s", i, sym)
						t.Errorf("  Available: %v", phase.Available)
						t.Errorf("  Provides: %v", phase.Provides)
					}
				}
			}
		}
	}
}
