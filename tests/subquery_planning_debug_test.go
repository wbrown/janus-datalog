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

func TestSubqueryPlanningDebug(t *testing.T) {
	dir, err := os.MkdirTemp("", "test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	person := datalog.NewIdentity("person:1")
	tx.Add(person, datalog.NewKeyword(":person/name"), "Alice")

	e1 := datalog.NewIdentity("event:1")
	tx.Add(e1, datalog.NewKeyword(":event/person"), person)
	tx.Add(e1, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC))
	tx.Add(e1, datalog.NewKeyword(":event/value"), int64(100))

	e2 := datalog.NewIdentity("event:2")
	tx.Add(e2, datalog.NewKeyword(":event/person"), person)
	tx.Add(e2, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 14, 0, 0, 0, time.UTC))
	tx.Add(e2, datalog.NewKeyword(":event/value"), int64(150))

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
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
		t.Fatalf("Parse error: %v", err)
	}

	opts := planner.PlannerOptions{
		EnableDynamicReordering:             true,
		EnableConditionalAggregateRewriting: false,
	}

	// Plan the query
	pl := planner.NewPlanner(nil, opts)
	plan, err := pl.Plan(q)
	if err != nil {
		t.Fatalf("Planning error: %v", err)
	}

	// Print plan details
	t.Logf("Query plan has %d phases", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("\nPhase %d:", i)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		t.Logf("  Expressions: %d", len(phase.Expressions))
		t.Logf("  Subqueries: %d", len(phase.Subqueries))
		t.Logf("  Available: %v", phase.Available)
		t.Logf("  Provides: %v", phase.Provides)
		t.Logf("  Keep: %v", phase.Keep)

		for j, sq := range phase.Subqueries {
			t.Logf("  Subquery %d:", j)
			t.Logf("    Pattern: %s", sq.Subquery.String())
			t.Logf("    Inputs: %v", sq.Inputs)
			t.Logf("    Decorrelated: %v", sq.Decorrelated)
			if sq.NestedPlan != nil {
				t.Logf("    Nested plan phases: %d", len(sq.NestedPlan.Phases))
			}
		}
	}

	// Execute
	exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

	// Enable new QueryExecutor (Stage B)
	exec.SetUseLegacyExecutor(false)

	// Enable annotations to see execution flow
	ctx := executor.NewContext(func(event annotations.Event) {
		t.Logf("[ANNOTATION] %s: %v", event.Name, event.Data)
	})

	result, err := exec.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Fatalf("Execution error: %v", err)
	}

	t.Logf("\nResult size: %d", result.Size())
	t.Logf("Result columns: %v", result.Columns())

	if result.Size() != 1 {
		t.Errorf("Expected 1 row, got %d", result.Size())
	}
}

// TestSubqueryDualExecutorParity tests that both executors produce identical results
// for subqueries WITHOUT conditional aggregate rewriting
func TestSubqueryDualExecutorParity(t *testing.T) {
	dir, err := os.MkdirTemp("", "dual-exec-subquery-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	person := datalog.NewIdentity("person:1")
	tx.Add(person, datalog.NewKeyword(":person/name"), "Alice")

	e1 := datalog.NewIdentity("event:1")
	tx.Add(e1, datalog.NewKeyword(":event/person"), person)
	tx.Add(e1, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC))
	tx.Add(e1, datalog.NewKeyword(":event/value"), int64(100))

	e2 := datalog.NewIdentity("event:2")
	tx.Add(e2, datalog.NewKeyword(":event/person"), person)
	tx.Add(e2, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 14, 0, 0, 0, time.UTC))
	tx.Add(e2, datalog.NewKeyword(":event/value"), int64(150))

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
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
		t.Fatalf("Parse error: %v", err)
	}

	// Test both executors WITHOUT conditional aggregate rewriting
	baseOpts := planner.PlannerOptions{
		EnableDynamicReordering:             true,
		EnableConditionalAggregateRewriting: false, // Both executors should work
	}

	matcher := storage.NewBadgerMatcher(db.Store())

	// Run with both executor variants
	for _, variant := range executor.DualTestExecutorVariants() {
		t.Run(variant.Name, func(t *testing.T) {
			opts := baseOpts
			opts.UseLegacyExecutor = variant.Opts.UseLegacyExecutor

			exec := executor.NewExecutorWithOptions(matcher, opts)
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Execution failed: %v", err)
			}

			// Should get 1 row: Alice, day 15, max 150
			if result.Size() != 1 {
				t.Errorf("Expected 1 row, got %d", result.Size())
			}

			it := result.Iterator()
			if it.Next() {
				tuple := it.Tuple()
				if len(tuple) != 3 {
					t.Errorf("Expected 3 columns, got %d", len(tuple))
				} else {
					name := tuple[0].(string)
					day := tuple[1].(int64)
					maxVal := tuple[2].(int64)

					if name != "Alice" {
						t.Errorf("Expected name 'Alice', got %v", name)
					}
					if day != 15 {
						t.Errorf("Expected day 15, got %v", day)
					}
					if maxVal != 150 {
						t.Errorf("Expected max 150, got %v", maxVal)
					}
				}
			}
			it.Close()
		})
	}
}

// TestConditionalAggregateRewritingBug demonstrates the known bug where QueryExecutor
// fails when conditional aggregate rewriting is enabled. This test documents the
// expected behavior and will pass once ScalarBinding is implemented.
func TestConditionalAggregateRewritingBug(t *testing.T) {
	dir, err := os.MkdirTemp("", "cond-agg-bug-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	person := datalog.NewIdentity("person:1")
	tx.Add(person, datalog.NewKeyword(":person/name"), "Alice")

	e1 := datalog.NewIdentity("event:1")
	tx.Add(e1, datalog.NewKeyword(":event/person"), person)
	tx.Add(e1, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC))
	tx.Add(e1, datalog.NewKeyword(":event/value"), int64(100))

	e2 := datalog.NewIdentity("event:2")
	tx.Add(e2, datalog.NewKeyword(":event/person"), person)
	tx.Add(e2, datalog.NewKeyword(":event/time"), time.Date(2025, 1, 15, 14, 0, 0, 0, time.UTC))
	tx.Add(e2, datalog.NewKeyword(":event/value"), int64(150))

	_, err = tx.Commit()
	if err != nil {
		t.Fatal(err)
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
		t.Fatalf("Parse error: %v", err)
	}

	// Test with conditional aggregate rewriting ENABLED
	// Both legacy and QueryExecutor should work now that conditional aggregate
	// injection is implemented in ExecuteRealized
	baseOpts := planner.PlannerOptions{
		EnableDynamicReordering:             true,
		EnableConditionalAggregateRewriting: true,
	}

	matcher := storage.NewBadgerMatcher(db.Store())

	// Test both executors using dual-executor fixture
	for _, variant := range executor.DualTestExecutorVariants() {
		t.Run(variant.Name, func(t *testing.T) {
			opts := baseOpts
			opts.UseLegacyExecutor = variant.Opts.UseLegacyExecutor

			exec := executor.NewExecutorWithOptions(matcher, opts)
			result, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Execution failed: %v", err)
			}

			if result.Size() != 1 {
				t.Errorf("Expected 1 row, got %d", result.Size())
			}
		})
	}
}
