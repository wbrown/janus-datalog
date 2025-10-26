package tests

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestSubqueryWithInputParameter tests basic subquery with input parameter and predicate
// This is the MOST BASIC case that should work without any optimizations
func TestSubqueryWithInputParameter(t *testing.T) {
	dir, err := os.MkdirTemp("", "subquery-input-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert simple test data
	tx := db.NewTransaction()

	// Create a person entity
	person := datalog.NewIdentity("person:1")
	tx.Add(person, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(person, datalog.NewKeyword(":person/age"), int64(30))

	// Create some events for this person
	e1 := datalog.NewIdentity("event:1")
	tx.Add(e1, datalog.NewKeyword(":event/person"), person)
	tx.Add(e1, datalog.NewKeyword(":event/type"), "login")
	tx.Add(e1, datalog.NewKeyword(":event/value"), int64(10))

	e2 := datalog.NewIdentity("event:2")
	tx.Add(e2, datalog.NewKeyword(":event/person"), person)
	tx.Add(e2, datalog.NewKeyword(":event/type"), "purchase")
	tx.Add(e2, datalog.NewKeyword(":event/value"), int64(20))

	e3 := datalog.NewIdentity("event:3")
	tx.Add(e3, datalog.NewKeyword(":event/person"), person)
	tx.Add(e3, datalog.NewKeyword(":event/type"), "login")
	tx.Add(e3, datalog.NewKeyword(":event/value"), int64(15))

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Ultra-simple query: count events of a specific type for each person
	queryStr := `[:find ?name ?count
	             :where
	             [?p :person/name ?name]

	             ; Subquery: count events of specific type
	             [(q [:find (count ?e)
	                  :in $ ?person ?type
	                  :where
	                  [?e :event/person ?person]
	                  [?e :event/type ?t]
	                  [(= ?t ?type)]]
	               $ ?p "login") [[?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	t.Logf("Parsed query:\n%s", q.String())

	opts := planner.PlannerOptions{
		EnableDynamicReordering: true,
	}

	// Create planner and plan the query
	pl := planner.NewPlanner(nil, opts)
	plan, err := pl.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed: %v", err)
	}

	t.Logf("Query plan has %d phases", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("\nPhase %d:", i)
		t.Logf("  Patterns: %d", len(phase.Patterns))
		t.Logf("  Expressions: %d", len(phase.Expressions))
		t.Logf("  Subqueries: %d", len(phase.Subqueries))
		t.Logf("  DecorrelatedSubqueries: %d", len(phase.DecorrelatedSubqueries))
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

	exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

	// Enable new QueryExecutor (Stage B)
	exec.SetUseQueryExecutor(true)

	// Enable annotations to see execution flow
	ctx := executor.NewContext(func(event annotations.Event) {
		t.Logf("[ANNOTATION] %s: %v", event.Name, event.Data)
	})

	result, err := exec.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Result size: %d", result.Size())
	t.Logf("Result columns: %v", result.Columns())

	// Should get 1 row: Alice with count=2 (two login events)
	if result.Size() != 1 {
		t.Errorf("Expected 1 row, got %d", result.Size())
	}

	it := result.Iterator()
	defer it.Close()

	if it.Next() {
		tuple := it.Tuple()
		name := tuple[0].(string)
		count := tuple[1].(int64)

		if name != "Alice" {
			t.Errorf("Expected name Alice, got %v", name)
		}
		if count != 2 {
			t.Errorf("Expected count 2, got %v", count)
		}
		t.Logf("✓ Result: name=%s, count=%d", name, count)
	}
}
