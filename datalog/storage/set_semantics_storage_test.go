package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
)

// =============================================================================
// Storage-Level Set Semantics Tests
//
// These tests verify that queries against real storage maintain set semantics.
// The core scenario: multiple entities with the same attribute value should
// produce only one result when projecting to just the value.
// =============================================================================

// Helper: check for duplicates in query results
func assertResultNoDuplicates(t *testing.T, name string, results [][]interface{}) {
	t.Helper()
	seen := make(map[string]int)

	for idx, tuple := range results {
		key := fmt.Sprintf("%v", tuple)
		if firstIdx, exists := seen[key]; exists {
			t.Errorf("%s: duplicate tuple at index %d (first seen at %d): %v", name, idx, firstIdx, tuple)
		}
		seen[key] = idx
	}
}

func TestSetSemantics_StorageQuery(t *testing.T) {
	// Create temporary database
	tmpDir, err := os.MkdirTemp("", "set_semantics_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create multiple entities with the SAME attribute value
	// This is the core bug scenario
	tx := db.NewTransaction()
	attr := datalog.NewKeyword(":test/value")

	e1 := datalog.NewIdentity("entity:1")
	e2 := datalog.NewIdentity("entity:2")
	e3 := datalog.NewIdentity("entity:3")
	e4 := datalog.NewIdentity("entity:4")

	// All have the same value "shared"
	tx.Add(e1, attr, "shared")
	tx.Add(e2, attr, "shared")
	tx.Add(e3, attr, "unique")
	tx.Add(e4, attr, "shared")

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	t.Run("Query projecting to value only - [:find ?v :where [_ :test/value ?v]]", func(t *testing.T) {
		result, err := executor.CollectTuples(db.Query(`[:find ?v :where [_ :test/value ?v]]`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Should have exactly 2 unique values: "shared" and "unique"
		if len(result) != 2 {
			t.Errorf("expected 2 unique values, got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "Value-only projection", result)
	})

	t.Run("Query projecting entity and value - [:find ?e ?v :where [?e :test/value ?v]]", func(t *testing.T) {
		result, err := executor.CollectTuples(db.Query(`[:find ?e ?v :where [?e :test/value ?v]]`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Should have 4 unique (entity, value) tuples
		if len(result) != 4 {
			t.Errorf("expected 4 tuples, got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "Entity+value projection", result)
	})
}

func TestSetSemantics_StorageQuery_MultipleAttributes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "set_semantics_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create data with overlapping values across different attributes
	tx := db.NewTransaction()
	name := datalog.NewKeyword(":person/name")
	city := datalog.NewKeyword(":person/city")

	e1 := datalog.NewIdentity("person:1")
	e2 := datalog.NewIdentity("person:2")
	e3 := datalog.NewIdentity("person:3")

	tx.Add(e1, name, "Alice")
	tx.Add(e1, city, "NYC")
	tx.Add(e2, name, "Bob")
	tx.Add(e2, city, "NYC")   // Same city as e1
	tx.Add(e3, name, "Alice") // Same name as e1
	tx.Add(e3, city, "LA")

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	t.Run("Join then project to overlapping values", func(t *testing.T) {
		// This query joins on entity, producing tuples that might duplicate after projection
		result, err := executor.CollectTuples(db.Query(`
			[:find ?city
			 :where [?e :person/name ?name]
			        [?e :person/city ?city]]
		`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Should have 2 unique cities: "NYC" and "LA"
		if len(result) != 2 {
			t.Errorf("expected 2 unique cities, got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "City projection after join", result)
	})

	t.Run("Multiple projection symbols", func(t *testing.T) {
		result, err := executor.CollectTuples(db.Query(`
			[:find ?name ?city
			 :where [?e :person/name ?name]
			        [?e :person/city ?city]]
		`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Should have 3 unique (name, city) pairs
		if len(result) != 3 {
			t.Errorf("expected 3 tuples, got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "Name+city projection", result)
	})
}

func TestSetSemantics_StorageQuery_WithPredicates(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "set_semantics_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	age := datalog.NewKeyword(":person/age")

	for i := 0; i < 10; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("person:%d", i))
		// Ages cycle: 20, 25, 30, 20, 25, 30, ...
		// This creates multiple entities with the same age
		ageVal := int64(20 + (i%3)*5)
		tx.Add(e, age, ageVal)
	}

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	t.Run("Filter then project to repeated values", func(t *testing.T) {
		result, err := executor.CollectTuples(db.Query(`
			[:find ?age
			 :where [?e :person/age ?age]
			        [(>= ?age 25)]]
		`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Should have 2 unique ages: 25 and 30
		if len(result) != 2 {
			t.Errorf("expected 2 unique ages (25, 30), got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "Age projection with filter", result)
	})
}

func TestSetSemantics_StorageQuery_Aggregation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "set_semantics_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	dept := datalog.NewKeyword(":employee/dept")
	salary := datalog.NewKeyword(":employee/salary")

	// Create employees with duplicate (dept, salary) combinations
	// If duplicates aren't removed, count will be wrong
	e1 := datalog.NewIdentity("emp:1")
	e2 := datalog.NewIdentity("emp:2")
	e3 := datalog.NewIdentity("emp:3")
	e4 := datalog.NewIdentity("emp:4")

	tx.Add(e1, dept, "Engineering")
	tx.Add(e1, salary, int64(100000))
	tx.Add(e2, dept, "Engineering")
	tx.Add(e2, salary, int64(100000)) // Same dept and salary as e1
	tx.Add(e3, dept, "Engineering")
	tx.Add(e3, salary, int64(120000))
	tx.Add(e4, dept, "Sales")
	tx.Add(e4, salary, int64(80000))

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	t.Run("Count with potential duplicates", func(t *testing.T) {
		// Count employees per department
		result, err := executor.CollectTuples(db.Query(`
			[:find ?dept (count ?e)
			 :where [?e :employee/dept ?dept]
			        [?e :employee/salary ?salary]]
		`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Should have 2 groups (Engineering, Sales)
		// Engineering should count 3 employees
		// Sales should count 1 employee
		if len(result) != 2 {
			t.Errorf("expected 2 department groups, got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "Aggregation result", result)
	})
}

func TestSetSemantics_StorageQuery_NotClause(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "set_semantics_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	status := datalog.NewKeyword(":user/status")
	role := datalog.NewKeyword(":user/role")

	e1 := datalog.NewIdentity("user:1")
	e2 := datalog.NewIdentity("user:2")
	e3 := datalog.NewIdentity("user:3")

	tx.Add(e1, status, "active")
	tx.Add(e1, role, "admin")
	tx.Add(e2, status, "active")
	tx.Add(e2, role, "user")
	tx.Add(e3, status, "inactive")
	tx.Add(e3, role, "admin")

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	t.Run("NOT clause with projection", func(t *testing.T) {
		result, err := executor.CollectTuples(db.Query(`
			[:find ?role
			 :where [?e :user/role ?role]
			        (not [?e :user/status "inactive"])]
		`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Active users have roles: "admin" and "user"
		// Should be 2 unique roles
		if len(result) != 2 {
			t.Errorf("expected 2 unique roles, got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "NOT clause projection", result)
	})
}

func TestSetSemantics_StorageQuery_OrClause(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "set_semantics_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction()
	status := datalog.NewKeyword(":item/status")
	priority := datalog.NewKeyword(":item/priority")

	e1 := datalog.NewIdentity("item:1")
	e2 := datalog.NewIdentity("item:2")
	e3 := datalog.NewIdentity("item:3")
	e4 := datalog.NewIdentity("item:4")

	tx.Add(e1, status, "pending")
	tx.Add(e1, priority, "high")
	tx.Add(e2, status, "pending")
	tx.Add(e2, priority, "low")
	tx.Add(e3, status, "done")
	tx.Add(e3, priority, "high") // high priority but done
	tx.Add(e4, status, "pending")
	tx.Add(e4, priority, "high") // Same as e1

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	t.Run("OR clause with overlapping results", func(t *testing.T) {
		// Items that are either pending OR high priority
		// e1, e2, e3, e4 match - but projecting to priority should dedupe
		result, err := executor.CollectTuples(db.Query(`
			[:find ?priority
			 :where [?e :item/priority ?priority]
			        (or [?e :item/status "pending"]
			            [?e :item/priority "high"])]
		`))
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		// Should have 2 unique priorities: "high" and "low"
		if len(result) != 2 {
			t.Errorf("expected 2 unique priorities, got %d: %v", len(result), result)
		}
		assertResultNoDuplicates(t, "OR clause projection", result)
	})
}
