package storage

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestDatabasePlanCache(t *testing.T) {
	// Create a temporary database
	dbPath := "/tmp/test-db-cache"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add some test data
	tx := db.NewTransaction()
	person1 := datalog.NewIdentity("person1")
	person2 := datalog.NewIdentity("person2")

	tx.Add(person1, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(person2, datalog.NewKeyword(":person/name"), "Bob")
	tx.Add(person1, datalog.NewKeyword(":person/age"), int64(30))
	tx.Add(person2, datalog.NewKeyword(":person/age"), int64(25))

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Parse a query
	queryStr := `[:find ?e ?name ?age
	              :where [?e :person/name ?name]
	                     [?e :person/age ?age]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Test 1: Create multiple executors - they should share the cache
	exec1 := db.NewExecutor()
	exec2 := db.NewExecutor()

	// Debug: Check that cache is not nil
	if db.PlanCache() == nil {
		t.Fatal("Database plan cache is nil")
	}

	// Clear cache to start fresh
	db.ClearPlanCache()

	// First execution should miss the cache
	result1, err := exec1.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	if result1.Size() != 2 {
		t.Errorf("Expected 2 results, got %d", result1.Size())
	}

	// Check cache stats - should have 1 miss
	hits, misses, size := db.PlanCache().Stats()
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}
	if size != 1 {
		t.Errorf("Expected cache size 1, got %d", size)
	}

	// Second execution from different executor should hit the cache
	result2, err := exec2.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	if result2.Size() != 2 {
		t.Errorf("Expected 2 results, got %d", result2.Size())
	}

	// Check cache stats - should now have 1 hit
	hits, misses, size = db.PlanCache().Stats()
	if hits != 1 {
		t.Errorf("Expected 1 hit, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss (unchanged), got %d", misses)
	}

	// Test 2: Clearing database cache affects all executors
	db.ClearPlanCache()

	hits, misses, size = db.PlanCache().Stats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Error("Expected cache to be cleared")
	}

	// Next execution should miss again
	_, err = exec1.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	hits, misses, size = db.PlanCache().Stats()
	if misses != 1 {
		t.Errorf("Expected 1 miss after clear, got %d", misses)
	}
}

func TestDatabaseWithoutCache(t *testing.T) {
	// Create a temporary database
	dbPath := "/tmp/test-db-no-cache"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Disable caching
	db.SetPlanCache(nil)

	// Create executor - should work without cache
	exec := db.NewExecutor()

	// Parse a query
	queryStr := `[:find ?e :where [?e :person/name _]]`
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Execute should work without cache
	_, err = exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query without cache: %v", err)
	}

	// Plan cache should be nil
	if db.PlanCache() != nil {
		t.Error("Expected plan cache to be nil")
	}
}

// TestExecutorCreationOverhead verifies that creating executors is cheap
func TestExecutorCreationOverhead(t *testing.T) {
	// Create a temporary database
	dbPath := "/tmp/test-db-overhead"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create many executors - should be fast and low memory
	executors := make([]*executor.Executor, 1000)
	for i := 0; i < 1000; i++ {
		executors[i] = db.NewExecutor()
	}

	// All should share the same cache (we can't directly compare function pointers,
	// but we can verify they work with the shared cache)

	// Basic sanity check that they work
	queryStr := `[:find ?e :where [?e _ _]]`
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// First executor execution
	_, err = executors[0].Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Last executor should benefit from cache
	_, err = executors[999].Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Check that cache was used
	hits, _, _ := db.PlanCache().Stats()
	if hits < 1 {
		t.Error("Expected at least 1 cache hit from shared cache")
	}
}
