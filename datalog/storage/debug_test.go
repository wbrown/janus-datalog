package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

func TestDebugBasicQuery(t *testing.T) {
	dbPath := "/tmp/test-debug"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add test data
	tx := db.NewTransaction()
	person1 := datalog.NewIdentity("person1")
	fmt.Printf("Created entity: %#v\n", person1)
	tx.Add(person1, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(person1, datalog.NewKeyword(":person/age"), int64(30))

	txID, err := tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	fmt.Printf("Committed transaction %d\n", txID)

	// Verify data is in database
	testQuery := `[:find ?e :where [?e :person/name _]]`
	testQ, _ := parser.ParseQuery(testQuery)
	exec := db.NewExecutor()  // Use default options
	testResult, _ := exec.Execute(testQ)
	fmt.Printf("Verify query found %d entities\n", testResult.Size())

	// Query with join (like TestDatabasePlanCache)
	queryStr := `[:find ?e ?name ?age
	              :where [?e :person/name ?name]
	                     [?e :person/age ?age]]`
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	// Create FRESH executor for join query with debug logging
	fmt.Printf("Creating fresh executor for join query\n")
	opts := DefaultPlannerOptions()
	opts.EnableDebugLogging = true
	execDebug := db.NewExecutorWithOptions(opts)

	// Check the plan (only works with old planner)
	queryPlanner := execDebug.GetPlanner()
	adapter, ok := queryPlanner.(*planner.PlannerAdapter)
	if !ok {
		fmt.Printf("Skipping plan inspection (using new clause-based planner)\n")
	} else {
		oldPlanner := adapter.GetUnderlyingPlanner()
		plan, err := oldPlanner.Plan(q)
		if err != nil {
			t.Fatalf("Failed to plan: %v", err)
		}

		fmt.Printf("Plan has %d phases:\n", len(plan.Phases))
		for i, phase := range plan.Phases {
			fmt.Printf("  Phase %d: patterns=%d, keep=%v\n", i+1, len(phase.Patterns), phase.Keep)
		}
	}

	// Try executing each pattern individually to see where the problem is
	fmt.Printf("\nTesting individual patterns:\n")

	exec = db.NewExecutor() // Fresh executor for pattern 1
	q1, _ := parser.ParseQuery(`[:find ?e ?name :where [?e :person/name ?name]]`)
	r1, _ := exec.Execute(q1)
	fmt.Printf("Pattern 1 ([?e :person/name ?name]): %d results\n", r1.Size())
	it1 := r1.Iterator()
	for it1.Next() {
		tuple := it1.Tuple()
		fmt.Printf("  Tuple len=%d: ", len(tuple))
		for i, v := range tuple {
			fmt.Printf("[%d] type=%T value='%#v' ", i, v, v)
		}
		fmt.Printf("\n")
		// Check if entity IDs are comparable
		if tuple[0] == person1 {
			fmt.Printf("  ?e matches person1!\n")
		} else {
			fmt.Printf("  ?e does NOT match person1\n")
		}
	}
	it1.Close()

	exec = db.NewExecutor() // Fresh executor for pattern 2
	q2, _ := parser.ParseQuery(`[:find ?e ?age :where [?e :person/age ?age]]`)
	r2, _ := exec.Execute(q2)
	fmt.Printf("Pattern 2 ([?e :person/age ?age]): %d results\n", r2.Size())
	it2 := r2.Iterator()
	for it2.Next() {
		tuple := it2.Tuple()
		fmt.Printf("  Tuple len=%d: ", len(tuple))
		for i, v := range tuple {
			fmt.Printf("[%d] type=%T value='%#v' ", i, v, v)
		}
		fmt.Printf("\n")
		// Check if entity IDs are comparable
		if tuple[0] == person1 {
			fmt.Printf("  ?e matches person1!\n")
		} else {
			fmt.Printf("  ?e does NOT match person1\n")
		}
	}
	it2.Close()

	// Debug: Check if the identities from both patterns are interned to the same pointer
	fmt.Printf("\nChecking identity interning:\n")
	it1_check := r1.Iterator()
	it2_check := r2.Iterator()
	it1_check.Next()
	it2_check.Next()
	e1 := it1_check.Tuple()[0]
	e2 := it2_check.Tuple()[0]
	fmt.Printf("Pattern 1 ?e pointer: %p\n", e1)
	fmt.Printf("Pattern 2 ?e pointer: %p\n", e2)
	fmt.Printf("Are they the same pointer? %v\n", e1 == e2)
	fmt.Printf("ValuesEqual(e1, e2): %v\n", datalog.ValuesEqual(e1, e2))

	// Check hash values
	if id1, ok := e1.(*datalog.Identity); ok {
		hash1 := id1.Hash()
		fmt.Printf("Pattern 1 hash: %x\n", hash1)
	}
	if id2, ok := e2.(*datalog.Identity); ok {
		hash2 := id2.Hash()
		fmt.Printf("Pattern 2 hash: %x\n", hash2)
	}

	it1_check.Close()
	it2_check.Close()

	result, err := execDebug.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute: %v", err)
	}

	fmt.Printf("\nJoin result size: %d\n", result.Size())
	fmt.Printf("Result columns: %v\n", result.Columns())

	it := result.Iterator()
	for it.Next() {
		fmt.Printf("Tuple: %v\n", it.Tuple())
	}
	it.Close()

	if result.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", result.Size())
	}
}
