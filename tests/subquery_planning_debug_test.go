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
	exec.SetUseQueryExecutor(true)

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
