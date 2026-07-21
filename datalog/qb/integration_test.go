package qb_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/qb"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// Test attributes - defined once, reused throughout tests
var (
	PersonName   = qb.Kw(":person/name")
	PersonAge    = qb.Kw(":person/age")
	PersonCity   = qb.Kw(":person/city")
	PersonSalary = qb.Kw(":person/salary")
	PersonDept   = qb.Kw(":person/dept")
	PersonActive = qb.Kw(":person/active")
)

// setupTestDB creates a test database with sample data. popts sets the
// database's planner options (nil = defaults); the optimizer mode matrix
// (docs/wip/OPTIMIZER_MODE_MATRIX.md) passes each mode's options so every
// executing test in this file runs under both optimizer paths.
func setupTestDB(t *testing.T, popts *planner.PlannerOptions) (*storage.Database, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "qb-integration-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
		Path:           dbPath,
		PlannerOptions: popts,
	})
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create database: %v", err)
	}

	// Insert test data
	tx := db.NewTransaction()

	// People with various attributes
	people := []struct {
		id     string
		name   string
		age    int64
		city   string
		salary float64
		dept   string
		active bool
	}{
		{"alice", "Alice", 30, "NYC", 75000, "Engineering", true},
		{"bob", "Bob", 25, "LA", 65000, "Engineering", true},
		{"charlie", "Charlie", 35, "NYC", 85000, "Sales", true},
		{"diana", "Diana", 28, "Chicago", 70000, "Engineering", false},
		{"eve", "Eve", 32, "LA", 90000, "Sales", true},
	}

	for _, p := range people {
		e := datalog.NewIdentity(p.id)
		tx.Add(e, PersonName.Keyword(), p.name)
		tx.Add(e, PersonAge.Keyword(), p.age)
		tx.Add(e, PersonCity.Keyword(), p.city)
		tx.Add(e, PersonSalary.Keyword(), p.salary)
		tx.Add(e, PersonDept.Keyword(), p.dept)
		tx.Add(e, PersonActive.Keyword(), p.active)
	}

	if _, err := tx.Commit(); err != nil {
		db.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

// TestIntegration_BasicQuery tests basic query execution
func TestIntegration_BasicQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")

			q := qb.Query().
				Find(name).
				Where(qb.Pat(e, PersonName, name)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 5 {
				t.Errorf("Expected 5 results, got %d", len(results))
			}

			// Verify all names are present
			names := make(map[string]bool)
			for _, tuple := range results {
				if name, ok := tuple[0].(string); ok {
					names[name] = true
				}
			}

			expected := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
			for _, exp := range expected {
				if !names[exp] {
					t.Errorf("Missing expected name: %s", exp)
				}
			}
		})
	}
}

// TestIntegration_JoinQuery tests that same variable creates join
func TestIntegration_JoinQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			// Same `e` variable in both patterns = join on entity
			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")

			q := qb.Query().
				Find(name, age).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 5 {
				t.Errorf("Expected 5 results, got %d", len(results))
			}

			// Verify Alice is 30
			for _, tuple := range results {
				if tuple[0] == "Alice" {
					if age, ok := tuple[1].(int64); ok {
						if age != 30 {
							t.Errorf("Expected Alice age 30, got %d", age)
						}
					}
				}
			}
		})
	}
}

// TestIntegration_PredicateQuery tests queries with predicates
func TestIntegration_PredicateQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")

			// Find people over 30
			q := qb.Query().
				Find(name).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Gt(age, qb.V(int64(30))),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should be Charlie (35) and Eve (32)
			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}

			names := make(map[string]bool)
			for _, tuple := range results {
				if name, ok := tuple[0].(string); ok {
					names[name] = true
				}
			}

			if !names["Charlie"] {
				t.Error("Expected Charlie in results")
			}
			if !names["Eve"] {
				t.Error("Expected Eve in results")
			}
		})
	}
}

// TestIntegration_MultiplePredicates tests queries with multiple predicates
func TestIntegration_MultiplePredicates(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")
			city := qb.NewVar("city")

			// Find people in NYC who are over 25
			q := qb.Query().
				Find(name, age).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Pat(e, PersonCity, city),
					qb.Eq(city, qb.V("NYC")),
					qb.Gt(age, qb.V(int64(25))),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should be Alice (30, NYC) and Charlie (35, NYC)
			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}
		})
	}
}

// TestIntegration_AggregationQuery tests queries with aggregation
func TestIntegration_AggregationQuery(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			dept := qb.NewVar("dept")
			salary := qb.NewVar("salary")

			// Sum salary by department
			q := qb.Query().
				Find(dept, qb.Sum(salary)).
				Where(
					qb.Pat(e, PersonDept, dept),
					qb.Pat(e, PersonSalary, salary),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should have 2 departments
			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}

			// Verify totals
			totals := make(map[string]float64)
			for _, tuple := range results {
				if d, ok := tuple[0].(string); ok {
					if s, ok := tuple[1].(float64); ok {
						totals[d] = s
					}
				}
			}

			// Engineering: Alice (75000) + Bob (65000) + Diana (70000) = 210000
			if totals["Engineering"] != 210000 {
				t.Errorf("Expected Engineering total 210000, got %v", totals["Engineering"])
			}

			// Sales: Charlie (85000) + Eve (90000) = 175000
			if totals["Sales"] != 175000 {
				t.Errorf("Expected Sales total 175000, got %v", totals["Sales"])
			}
		})
	}
}

// TestIntegration_CountAggregation tests count aggregation
func TestIntegration_CountAggregation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			dept := qb.NewVar("dept")
			name := qb.NewVar("name")

			// Count people by department
			q := qb.Query().
				Find(dept, qb.Count(name)).
				Where(
					qb.Pat(e, PersonDept, dept),
					qb.Pat(e, PersonName, name),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			counts := make(map[string]int64)
			for _, tuple := range results {
				if d, ok := tuple[0].(string); ok {
					if c, ok := tuple[1].(int64); ok {
						counts[d] = c
					}
				}
			}

			if counts["Engineering"] != 3 {
				t.Errorf("Expected Engineering count 3, got %v", counts["Engineering"])
			}
			if counts["Sales"] != 2 {
				t.Errorf("Expected Sales count 2, got %v", counts["Sales"])
			}
		})
	}
}

// TestIntegration_InputParameters tests queries with input parameters
func TestIntegration_InputParameters(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			inputName := qb.NewVar("inputName")
			age := qb.NewVar("age")

			// Scalar input used directly in pattern - this is idiomatic Datalog
			// The input variable is bound to the input value, then used to match patterns
			q := qb.Query().
				Find(inputName, age).
				In(qb.DB, qb.Scalar(inputName)).
				Where(
					qb.Pat(e, PersonName, inputName), // inputName bound to "Alice" from input
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			// Find person named "Alice" and return their age
			results, err := executor.CollectTuples(db.Query(q, "Alice"))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 1 {
				t.Errorf("Expected 1 result, got %d", len(results))
			}

			if len(results) > 0 {
				if results[0][0] != "Alice" {
					t.Errorf("Expected name Alice, got %v", results[0][0])
				}
				if results[0][1] != int64(30) {
					t.Errorf("Expected age 30, got %v", results[0][1])
				}
			}
		})
	}
}

// TestIntegration_CollectionInput tests queries with collection input
func TestIntegration_CollectionInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			inputName := qb.NewVar("inputName")
			age := qb.NewVar("age")

			// Collection input - each value is iterated and used to match patterns
			// [?inputName ...] means: for each value in the collection, bind ?inputName
			q := qb.Query().
				Find(inputName, age).
				In(qb.DB, qb.Collection(inputName)).
				Where(
					qb.Pat(e, PersonName, inputName), // inputName bound from collection
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			// Find people named Alice or Bob and their ages
			results, err := executor.CollectTuples(db.Query(q, []string{"Alice", "Bob"}))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should be Alice and Bob
			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}

			names := make(map[string]bool)
			for _, tuple := range results {
				if n, ok := tuple[0].(string); ok {
					names[n] = true
				}
			}
			if !names["Alice"] || !names["Bob"] {
				t.Errorf("Expected Alice and Bob, got %v", names)
			}
		})
	}
}

// TestIntegration_OrderBy tests queries with ordering
func TestIntegration_OrderBy(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")

			q := qb.Query().
				Find(name, age).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
				).
				OrderBy(qb.Desc(age)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 5 {
				t.Fatalf("Expected 5 results, got %d", len(results))
			}

			// First result should be Charlie (35)
			if results[0][0] != "Charlie" {
				t.Errorf("Expected first result to be Charlie, got %v", results[0][0])
			}

			// Last result should be Bob (25)
			if results[4][0] != "Bob" {
				t.Errorf("Expected last result to be Bob, got %v", results[4][0])
			}
		})
	}
}

// TestIntegration_QueryInto tests QueryInto with struct mapping
// Uses positional mapping - no datalog tags needed, fields map by order
func TestIntegration_QueryInto(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			// No datalog tags - fields map positionally to Find() order
			type PersonResult struct {
				Name string
				Age  int64
			}

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")

			q := qb.Query().
				Find(name, age). // Position 0: Name, Position 1: Age
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Gt(age, qb.V(int64(30))),
				).
				OrderBy(qb.Desc(age)).
				MustBuild()

			var results []PersonResult
			err := db.QueryInto(&results, q)
			if err != nil {
				t.Fatalf("QueryInto failed: %v", err)
			}

			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}

			// First should be Charlie (35)
			if results[0].Name != "Charlie" || results[0].Age != 35 {
				t.Errorf("Expected Charlie/35, got %s/%d", results[0].Name, results[0].Age)
			}
		})
	}
}

// TestIntegration_QueryOneInto tests QueryOneInto for single result
// Uses positional mapping - no datalog tags needed
func TestIntegration_QueryOneInto(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			// No datalog tags - fields map positionally
			type PersonResult struct {
				Name string
				Age  int64
			}

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")

			// Query for Alice specifically
			q := qb.Query().
				Find(name, age).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Eq(name, qb.V("Alice")),
				).
				MustBuild()

			var result PersonResult
			found, err := db.QueryOneInto(&result, q)
			if err != nil {
				t.Fatalf("QueryOneInto failed: %v", err)
			}

			if !found {
				t.Error("Expected to find Alice")
			}

			if result.Name != "Alice" || result.Age != 30 {
				t.Errorf("Expected Alice/30, got %s/%d", result.Name, result.Age)
			}
		})
	}
}

// TestIntegration_QueryIntoWithAggregation tests QueryInto with aggregation functions
// Uses positional mapping - no datalog tags needed
func TestIntegration_QueryIntoWithAggregation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			// No datalog tags - fields map positionally to Find() order
			type DeptStats struct {
				Dept  string  // Position 0: dept
				Avg   float64 // Position 1: (avg salary)
				Count int64   // Position 2: (count e)
			}

			e := qb.NewVar("e")
			dept := qb.NewVar("dept")
			salary := qb.NewVar("salary")

			q := qb.Query().
				Find(dept, qb.Avg(salary), qb.Count(e)).
				Where(
					qb.Pat(e, PersonDept, dept),
					qb.Pat(e, PersonSalary, salary),
				).
				OrderBy(qb.Asc(dept)).
				MustBuild()

			var results []DeptStats
			err := db.QueryInto(&results, q)
			if err != nil {
				t.Fatalf("QueryInto failed: %v", err)
			}

			if len(results) != 2 {
				t.Errorf("Expected 2 departments, got %d", len(results))
			}

			// Check Engineering: 3 people (Alice 75000, Bob 65000, Diana 70000) avg = 70000
			// Check Sales: 2 people (Charlie 85000, Eve 90000) avg = 87500
			deptMap := make(map[string]DeptStats)
			for _, r := range results {
				deptMap[r.Dept] = r
			}

			if eng, ok := deptMap["Engineering"]; ok {
				if eng.Count != 3 {
					t.Errorf("Expected Engineering count 3, got %d", eng.Count)
				}
				if eng.Avg != 70000 {
					t.Errorf("Expected Engineering avg 70000, got %f", eng.Avg)
				}
			} else {
				t.Error("Missing Engineering department")
			}

			if sales, ok := deptMap["Sales"]; ok {
				if sales.Count != 2 {
					t.Errorf("Expected Sales count 2, got %d", sales.Count)
				}
				if sales.Avg != 87500 {
					t.Errorf("Expected Sales avg 87500, got %f", sales.Avg)
				}
			} else {
				t.Error("Missing Sales department")
			}
		})
	}
}

// TestIntegration_CompareWithEDN verifies built query produces same results as EDN string
func TestIntegration_CompareWithEDN(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			// EDN query
			ednQuery := `[:find ?name ?age
                  :where
                  [?e :person/name ?name]
                  [?e :person/age ?age]
                  [(> ?age 30)]]`

			// Built query
			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")

			builtQuery := qb.Query().
				Find(name, age).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Gt(age, qb.V(int64(30))),
				).
				MustBuild()

			// Execute both
			ednResults, err := executor.CollectTuples(db.Query(ednQuery))
			if err != nil {
				t.Fatalf("EDN query failed: %v", err)
			}

			builtResults, err := executor.CollectTuples(db.Query(builtQuery))
			if err != nil {
				t.Fatalf("Built query failed: %v", err)
			}

			// Compare results
			if len(ednResults) != len(builtResults) {
				t.Errorf("Result count mismatch: EDN=%d, built=%d", len(ednResults), len(builtResults))
			}

			// Both should have Charlie and Eve
			ednNames := make(map[string]bool)
			builtNames := make(map[string]bool)

			for _, tuple := range ednResults {
				if n, ok := tuple[0].(string); ok {
					ednNames[n] = true
				}
			}
			for _, tuple := range builtResults {
				if n, ok := tuple[0].(string); ok {
					builtNames[n] = true
				}
			}

			if !ednNames["Charlie"] || !ednNames["Eve"] {
				t.Error("EDN query missing expected results")
			}
			if !builtNames["Charlie"] || !builtNames["Eve"] {
				t.Error("Built query missing expected results")
			}
		})
	}
}

// TestIntegration_ConstantValue tests pattern with constant value
func TestIntegration_ConstantValue(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")

			// Find people in NYC (constant value in pattern)
			q := qb.Query().
				Find(name).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonCity, qb.V("NYC")),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should be Alice and Charlie
			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}
		})
	}
}

// TestIntegration_BooleanPredicate tests boolean comparison
func TestIntegration_BooleanPredicate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			active := qb.NewVar("active")

			// Find active people
			q := qb.Query().
				Find(name).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonActive, active),
					qb.Eq(active, qb.V(true)),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should be Alice, Bob, Charlie, Eve (Diana is inactive)
			if len(results) != 4 {
				t.Errorf("Expected 4 active people, got %d", len(results))
			}
		})
	}
}

// TestIntegration_AvgAggregation tests average aggregation
func TestIntegration_AvgAggregation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			age := qb.NewVar("age")

			// Average age of all people
			q := qb.Query().
				Find(qb.Avg(age)).
				Where(
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 result, got %d", len(results))
			}

			// Average of 30, 25, 35, 28, 32 = 150/5 = 30
			if avg, ok := results[0][0].(float64); ok {
				if avg != 30 {
					t.Errorf("Expected average 30, got %v", avg)
				}
			} else {
				t.Errorf("Expected float64, got %T", results[0][0])
			}
		})
	}
}

// TestIntegration_MinMaxAggregation tests min/max aggregation
func TestIntegration_MinMaxAggregation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			salary := qb.NewVar("salary")

			// Min and max salary (separate queries since we can't have two aggregates without grouping)
			minQ := qb.Query().
				Find(qb.Min(salary)).
				Where(qb.Pat(e, PersonSalary, salary)).
				MustBuild()

			maxQ := qb.Query().
				Find(qb.Max(salary)).
				Where(qb.Pat(e, PersonSalary, salary)).
				MustBuild()

			minResults, err := executor.CollectTuples(db.Query(minQ))
			if err != nil {
				t.Fatalf("Min query failed: %v", err)
			}

			maxResults, err := executor.CollectTuples(db.Query(maxQ))
			if err != nil {
				t.Fatalf("Max query failed: %v", err)
			}

			// Min should be Bob's 65000
			if min, ok := minResults[0][0].(float64); ok {
				if min != 65000 {
					t.Errorf("Expected min 65000, got %v", min)
				}
			}

			// Max should be Eve's 90000
			if max, ok := maxResults[0][0].(float64); ok {
				if max != 90000 {
					t.Errorf("Expected max 90000, got %v", max)
				}
			}
		})
	}
}

// TestIntegration_ThreeWayJoin tests joining three patterns
func TestIntegration_ThreeWayJoin(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")
			city := qb.NewVar("city")

			q := qb.Query().
				Find(name, age, city).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Pat(e, PersonCity, city),
				).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 5 {
				t.Errorf("Expected 5 results, got %d", len(results))
			}

			// Verify Alice's data
			for _, tuple := range results {
				if tuple[0] == "Alice" {
					if tuple[1] != int64(30) {
						t.Errorf("Expected Alice age 30, got %v", tuple[1])
					}
					if tuple[2] != "NYC" {
						t.Errorf("Expected Alice city NYC, got %v", tuple[2])
					}
				}
			}
		})
	}
}

// TestIntegration_Explain tests the Explain function with built query
func TestIntegration_Explain(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")

			q := qb.Query().
				Find(name, age).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			plan, err := db.Explain(q)
			if err != nil {
				t.Fatalf("Explain failed: %v", err)
			}

			// Verify plan exists and has content
			planStr := plan.String()
			if len(planStr) == 0 {
				t.Error("Expected non-empty plan string")
			}

			// Plan should mention the attributes
			if !containsAny(planStr, ":person/name", "person/name") {
				t.Error("Plan should mention :person/name")
			}
		})
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if len(s) > 0 && len(sub) > 0 {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// TestIntegration_TupleInput tests queries with tuple input [[?x ?y]]
func TestIntegration_TupleInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			inputName := qb.NewVar("inputName")
			inputAge := qb.NewVar("inputAge")
			city := qb.NewVar("city")

			// Tuple input - binds a single tuple of values
			// [[?inputName ?inputAge]] means: bind these two variables from a single input tuple
			q := qb.Query().
				Find(inputName, inputAge, city).
				In(qb.DB, qb.Tuple(inputName, inputAge)).
				Where(
					qb.Pat(e, PersonName, inputName),
					qb.Pat(e, PersonAge, inputAge),
					qb.Pat(e, PersonCity, city),
				).
				MustBuild()

			// Find person with name "Alice" and age 30
			results, err := executor.CollectTuples(db.Query(q, []interface{}{"Alice", int64(30)}))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 1 {
				t.Errorf("Expected 1 result, got %d", len(results))
			}

			if len(results) > 0 {
				if results[0][0] != "Alice" {
					t.Errorf("Expected name Alice, got %v", results[0][0])
				}
				if results[0][1] != int64(30) {
					t.Errorf("Expected age 30, got %v", results[0][1])
				}
				if results[0][2] != "NYC" {
					t.Errorf("Expected city NYC, got %v", results[0][2])
				}
			}
		})
	}
}

// TestIntegration_RelationInput tests queries with relation input [[?x ?y] ...]
func TestIntegration_RelationInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			inputName := qb.NewVar("inputName")
			inputCity := qb.NewVar("inputCity")
			age := qb.NewVar("age")

			// Relation input - binds multiple tuples, iterating over each
			// [[?inputName ?inputCity] ...] means: for each tuple in the relation, bind both variables
			q := qb.Query().
				Find(inputName, inputCity, age).
				In(qb.DB, qb.Relation(inputName, inputCity)).
				Where(
					qb.Pat(e, PersonName, inputName),
					qb.Pat(e, PersonCity, inputCity),
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			// Find people matching (name, city) pairs
			results, err := executor.CollectTuples(db.Query(q, [][]interface{}{
				{"Alice", "NYC"},
				{"Eve", "LA"},
			}))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}

			// Verify we got Alice and Eve
			names := make(map[string]bool)
			for _, tuple := range results {
				if n, ok := tuple[0].(string); ok {
					names[n] = true
				}
			}
			if !names["Alice"] || !names["Eve"] {
				t.Errorf("Expected Alice and Eve, got %v", names)
			}
		})
	}
}

// TestIntegration_RelationInputNoMatch tests relation input with no matching data
func TestIntegration_RelationInputNoMatch(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			inputName := qb.NewVar("inputName")
			inputCity := qb.NewVar("inputCity")
			age := qb.NewVar("age")

			q := qb.Query().
				Find(inputName, inputCity, age).
				In(qb.DB, qb.Relation(inputName, inputCity)).
				Where(
					qb.Pat(e, PersonName, inputName),
					qb.Pat(e, PersonCity, inputCity),
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			// Alice is in NYC, not LA - should not match
			results, err := executor.CollectTuples(db.Query(q, [][]interface{}{
				{"Alice", "LA"},
				{"Bob", "NYC"},
			}))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Neither should match
			if len(results) != 0 {
				t.Errorf("Expected 0 results, got %d: %v", len(results), results)
			}
		})
	}
}

// TestIntegration_MultipleScalarInputs tests queries with multiple scalar inputs
func TestIntegration_MultipleScalarInputs(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			inputName := qb.NewVar("inputName")
			inputCity := qb.NewVar("inputCity")
			age := qb.NewVar("age")

			// Two scalar inputs
			q := qb.Query().
				Find(inputName, age).
				In(qb.DB, qb.Scalar(inputName), qb.Scalar(inputCity)).
				Where(
					qb.Pat(e, PersonName, inputName),
					qb.Pat(e, PersonCity, inputCity),
					qb.Pat(e, PersonAge, age),
				).
				MustBuild()

			// Find Alice in NYC
			results, err := executor.CollectTuples(db.Query(q, "Alice", "NYC"))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 1 {
				t.Errorf("Expected 1 result, got %d", len(results))
			}

			if len(results) > 0 && results[0][0] != "Alice" {
				t.Errorf("Expected Alice, got %v", results[0][0])
			}
		})
	}
}

// TestIntegration_CompareInputBindingsWithEDN verifies input bindings match EDN behavior
func TestIntegration_CompareInputBindingsWithEDN(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			t.Run("ScalarInput", func(t *testing.T) {
				// EDN query with scalar input
				ednQuery := `[:find ?name ?age
		              :in $ ?name
		              :where
		              [?e :person/name ?name]
		              [?e :person/age ?age]]`

				// Built query
				e := qb.NewVar("e")
				inputName := qb.NewVar("inputName")
				age := qb.NewVar("age")

				builtQuery := qb.Query().
					Find(inputName, age).
					In(qb.DB, qb.Scalar(inputName)).
					Where(
						qb.Pat(e, PersonName, inputName),
						qb.Pat(e, PersonAge, age),
					).
					MustBuild()

				ednResults, err := executor.CollectTuples(db.Query(ednQuery, "Bob"))
				if err != nil {
					t.Fatalf("EDN query failed: %v", err)
				}

				builtResults, err := executor.CollectTuples(db.Query(builtQuery, "Bob"))
				if err != nil {
					t.Fatalf("Built query failed: %v", err)
				}

				if len(ednResults) != len(builtResults) {
					t.Errorf("Result count mismatch: EDN=%d, built=%d", len(ednResults), len(builtResults))
				}

				if len(builtResults) > 0 && builtResults[0][0] != "Bob" {
					t.Errorf("Expected Bob, got %v", builtResults[0][0])
				}
			})

			t.Run("CollectionInput", func(t *testing.T) {
				// EDN query with collection input
				ednQuery := `[:find ?name ?age
		              :in $ [?name ...]
		              :where
		              [?e :person/name ?name]
		              [?e :person/age ?age]]`

				// Built query
				e := qb.NewVar("e")
				inputName := qb.NewVar("inputName")
				age := qb.NewVar("age")

				builtQuery := qb.Query().
					Find(inputName, age).
					In(qb.DB, qb.Collection(inputName)).
					Where(
						qb.Pat(e, PersonName, inputName),
						qb.Pat(e, PersonAge, age),
					).
					MustBuild()

				ednResults, err := executor.CollectTuples(db.Query(ednQuery, []string{"Alice", "Charlie"}))
				if err != nil {
					t.Fatalf("EDN query failed: %v", err)
				}

				builtResults, err := executor.CollectTuples(db.Query(builtQuery, []string{"Alice", "Charlie"}))
				if err != nil {
					t.Fatalf("Built query failed: %v", err)
				}

				if len(ednResults) != len(builtResults) {
					t.Errorf("Result count mismatch: EDN=%d, built=%d", len(ednResults), len(builtResults))
				}

				if len(ednResults) != 2 {
					t.Errorf("Expected 2 results, got %d", len(ednResults))
				}
			})

			t.Run("RelationInput", func(t *testing.T) {
				// EDN query with relation input
				ednQuery := `[:find ?name ?city ?age
		              :in $ [[?name ?city] ...]
		              :where
		              [?e :person/name ?name]
		              [?e :person/city ?city]
		              [?e :person/age ?age]]`

				// Built query
				e := qb.NewVar("e")
				inputName := qb.NewVar("inputName")
				inputCity := qb.NewVar("inputCity")
				age := qb.NewVar("age")

				builtQuery := qb.Query().
					Find(inputName, inputCity, age).
					In(qb.DB, qb.Relation(inputName, inputCity)).
					Where(
						qb.Pat(e, PersonName, inputName),
						qb.Pat(e, PersonCity, inputCity),
						qb.Pat(e, PersonAge, age),
					).
					MustBuild()

				input := [][]interface{}{
					{"Alice", "NYC"},
					{"Bob", "LA"},
				}

				ednResults, err := executor.CollectTuples(db.Query(ednQuery, input))
				if err != nil {
					t.Fatalf("EDN query failed: %v", err)
				}

				builtResults, err := executor.CollectTuples(db.Query(builtQuery, input))
				if err != nil {
					t.Fatalf("Built query failed: %v", err)
				}

				if len(ednResults) != len(builtResults) {
					t.Errorf("Result count mismatch: EDN=%d, built=%d", len(ednResults), len(builtResults))
				}

				if len(ednResults) != 2 {
					t.Errorf("Expected 2 results, got %d", len(ednResults))
				}
			})
		})
	}
}

// ========================================
// Comparison Binding E2E Tests
// ========================================

// TestIntegration_ComparisonBinding tests comparison binding with .As()
func TestIntegration_ComparisonBinding(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")
			isOver30 := qb.NewVar("isOver30")

			// Bind comparison result to variable
			q := qb.Query().
				Find(name, age, isOver30).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Gt(age, qb.V(int64(30))).As(isOver30),
				).
				OrderBy(qb.Asc(name)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Should have all 5 people
			if len(results) != 5 {
				t.Errorf("Expected 5 results, got %d", len(results))
			}

			// Verify specific results
			resultMap := make(map[string]bool)
			for _, tuple := range results {
				name := tuple[0].(string)
				isOver30Val := tuple[2].(bool)
				resultMap[name] = isOver30Val
			}

			// Alice (30) should be false (not strictly > 30)
			if resultMap["Alice"] != false {
				t.Errorf("Alice (age 30) should have isOver30=false")
			}
			// Bob (25) should be false
			if resultMap["Bob"] != false {
				t.Errorf("Bob (age 25) should have isOver30=false")
			}
			// Charlie (35) should be true
			if resultMap["Charlie"] != true {
				t.Errorf("Charlie (age 35) should have isOver30=true")
			}
			// Eve (32) should be true
			if resultMap["Eve"] != true {
				t.Errorf("Eve (age 32) should have isOver30=true")
			}
		})
	}
}

// TestIntegration_ComparisonBindingEDNEquivalence compares qb with EDN query
func TestIntegration_ComparisonBindingEDNEquivalence(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			// EDN query with comparison binding
			ednQuery := `[:find ?name ?age ?flag
	              :where
	              [?e :person/name ?name]
	              [?e :person/age ?age]
	              [(> ?age 30) ?flag]]`

			// Built query
			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")
			flag := qb.NewVar("flag")

			builtQuery := qb.Query().
				Find(name, age, flag).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Gt(age, qb.V(int64(30))).As(flag),
				).
				MustBuild()

			ednResults, err := executor.CollectTuples(db.Query(ednQuery))
			if err != nil {
				t.Fatalf("EDN query failed: %v", err)
			}

			builtResults, err := executor.CollectTuples(db.Query(builtQuery))
			if err != nil {
				t.Fatalf("Built query failed: %v", err)
			}

			if len(ednResults) != len(builtResults) {
				t.Errorf("Result count mismatch: EDN=%d, built=%d", len(ednResults), len(builtResults))
			}

			// Both should have 5 results
			if len(builtResults) != 5 {
				t.Errorf("Expected 5 results, got %d", len(builtResults))
			}
		})
	}
}

// TestIntegration_ChainedComparisonBinding tests chained comparison binding
func TestIntegration_ChainedComparisonBinding(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDB(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			age := qb.NewVar("age")
			inRange := qb.NewVar("inRange")

			// Check if age is in range 26-34 (exclusive)
			q := qb.Query().
				Find(name, age, inRange).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Pat(e, PersonAge, age),
					qb.Range(qb.V(int64(26)), age, qb.V(int64(34))).As(inRange),
				).
				OrderBy(qb.Asc(name)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 5 {
				t.Errorf("Expected 5 results, got %d", len(results))
			}

			// Verify range results
			// Alice (30): 26 < 30 < 34 = true
			// Bob (25): 26 < 25 = false
			// Charlie (35): 35 < 34 = false
			// Diana (28): 26 < 28 < 34 = true
			// Eve (32): 26 < 32 < 34 = true
			resultMap := make(map[string]bool)
			for _, tuple := range results {
				name := tuple[0].(string)
				inRangeVal := tuple[2].(bool)
				resultMap[name] = inRangeVal
			}

			expected := map[string]bool{
				"Alice":   true,  // 30 in (26, 34)
				"Bob":     false, // 25 not in (26, 34)
				"Charlie": false, // 35 not in (26, 34)
				"Diana":   true,  // 28 in (26, 34)
				"Eve":     true,  // 32 in (26, 34)
			}

			for name, exp := range expected {
				if resultMap[name] != exp {
					t.Errorf("%s: expected inRange=%v, got %v", name, exp, resultMap[name])
				}
			}
		})
	}
}

// ========================================
// Database Function E2E Tests
// ========================================

// Additional attributes for database function tests
var (
	PersonNickname = qb.Kw(":person/nickname")
	PersonEmail    = qb.Kw(":person/email")
)

// setupTestDBWithOptionalAttrs creates a test database with some optional
// attributes. popts sets the database's planner options (nil = defaults);
// see setupTestDB for the optimizer mode matrix rationale.
func setupTestDBWithOptionalAttrs(t *testing.T, popts *planner.PlannerOptions) (*storage.Database, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "qb-dbfunc-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
		Path:           dbPath,
		PlannerOptions: popts,
	})
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create database: %v", err)
	}

	// Insert test data with some optional attributes
	tx := db.NewTransaction()

	// Alice: has nickname and email
	alice := datalog.NewIdentity("alice")
	tx.Add(alice, PersonName.Keyword(), "Alice")
	tx.Add(alice, PersonNickname.Keyword(), "Ali")
	tx.Add(alice, PersonEmail.Keyword(), "alice@example.com")

	// Bob: has email but no nickname
	bob := datalog.NewIdentity("bob")
	tx.Add(bob, PersonName.Keyword(), "Bob")
	tx.Add(bob, PersonEmail.Keyword(), "bob@example.com")

	// Charlie: has nickname but no email
	charlie := datalog.NewIdentity("charlie")
	tx.Add(charlie, PersonName.Keyword(), "Charlie")
	tx.Add(charlie, PersonNickname.Keyword(), "Chuck")

	// Diana: has neither nickname nor email
	diana := datalog.NewIdentity("diana")
	tx.Add(diana, PersonName.Keyword(), "Diana")

	if _, err := tx.Commit(); err != nil {
		db.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

// TestIntegration_GetElse tests get-else with default value
func TestIntegration_GetElse(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDBWithOptionalAttrs(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			nickname := qb.NewVar("nickname")

			q := qb.Query().
				Find(name, nickname).
				Where(
					qb.Pat(e, PersonName, name),
					qb.GetElse(e, PersonNickname, "Anonymous").As(nickname),
				).
				OrderBy(qb.Asc(name)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 4 {
				t.Errorf("Expected 4 results, got %d", len(results))
			}

			// Verify results
			resultMap := make(map[string]string)
			for _, tuple := range results {
				name := tuple[0].(string)
				nick := tuple[1].(string)
				resultMap[name] = nick
			}

			// Alice has nickname "Ali"
			if resultMap["Alice"] != "Ali" {
				t.Errorf("Alice: expected nickname 'Ali', got '%s'", resultMap["Alice"])
			}
			// Bob has no nickname, should get default "Anonymous"
			if resultMap["Bob"] != "Anonymous" {
				t.Errorf("Bob: expected nickname 'Anonymous', got '%s'", resultMap["Bob"])
			}
			// Charlie has nickname "Chuck"
			if resultMap["Charlie"] != "Chuck" {
				t.Errorf("Charlie: expected nickname 'Chuck', got '%s'", resultMap["Charlie"])
			}
			// Diana has no nickname, should get default "Anonymous"
			if resultMap["Diana"] != "Anonymous" {
				t.Errorf("Diana: expected nickname 'Anonymous', got '%s'", resultMap["Diana"])
			}
		})
	}
}

// TestIntegration_GetElseEDNEquivalence compares qb GetElse with EDN query
func TestIntegration_GetElseEDNEquivalence(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDBWithOptionalAttrs(t, &popts)
			defer cleanup()

			// EDN query
			ednQuery := `[:find ?name ?nick
	              :where
	              [?e :person/name ?name]
	              [(get-else $ ?e :person/nickname "Anonymous") ?nick]]`

			// Built query
			e := qb.NewVar("e")
			name := qb.NewVar("name")
			nick := qb.NewVar("nick")

			builtQuery := qb.Query().
				Find(name, nick).
				Where(
					qb.Pat(e, PersonName, name),
					qb.GetElse(e, PersonNickname, "Anonymous").As(nick),
				).
				MustBuild()

			ednResults, err := executor.CollectTuples(db.Query(ednQuery))
			if err != nil {
				t.Fatalf("EDN query failed: %v", err)
			}

			builtResults, err := executor.CollectTuples(db.Query(builtQuery))
			if err != nil {
				t.Fatalf("Built query failed: %v", err)
			}

			if len(ednResults) != len(builtResults) {
				t.Errorf("Result count mismatch: EDN=%d, built=%d", len(ednResults), len(builtResults))
			}

			// Both should have 4 results
			if len(builtResults) != 4 {
				t.Errorf("Expected 4 results, got %d", len(builtResults))
			}
		})
	}
}

// TestIntegration_MissingPredicate tests missing? as a filter predicate
func TestIntegration_MissingPredicate(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDBWithOptionalAttrs(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")

			// Find people who don't have email
			q := qb.Query().
				Find(name).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Missing(e, PersonEmail),
				).
				OrderBy(qb.Asc(name)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			// Charlie and Diana don't have email
			if len(results) != 2 {
				t.Errorf("Expected 2 results, got %d", len(results))
			}

			names := make(map[string]bool)
			for _, tuple := range results {
				names[tuple[0].(string)] = true
			}

			if !names["Charlie"] {
				t.Error("Expected Charlie in results (no email)")
			}
			if !names["Diana"] {
				t.Error("Expected Diana in results (no email)")
			}
		})
	}
}

// TestIntegration_MissingPredicateEDNEquivalence compares qb Missing predicate with EDN
func TestIntegration_MissingPredicateEDNEquivalence(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDBWithOptionalAttrs(t, &popts)
			defer cleanup()

			// EDN query
			ednQuery := `[:find ?name
	              :where
	              [?e :person/name ?name]
	              [(missing? $ ?e :person/email)]]`

			// Built query
			e := qb.NewVar("e")
			name := qb.NewVar("name")

			builtQuery := qb.Query().
				Find(name).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Missing(e, PersonEmail),
				).
				MustBuild()

			ednResults, err := executor.CollectTuples(db.Query(ednQuery))
			if err != nil {
				t.Fatalf("EDN query failed: %v", err)
			}

			builtResults, err := executor.CollectTuples(db.Query(builtQuery))
			if err != nil {
				t.Fatalf("Built query failed: %v", err)
			}

			if len(ednResults) != len(builtResults) {
				t.Errorf("Result count mismatch: EDN=%d, built=%d", len(ednResults), len(builtResults))
			}

			// Both should have 2 results (Charlie and Diana)
			if len(builtResults) != 2 {
				t.Errorf("Expected 2 results, got %d", len(builtResults))
			}
		})
	}
}

// TestIntegration_MissingExpression tests missing? as an expression binding bool
func TestIntegration_MissingExpression(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDBWithOptionalAttrs(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			needsEmail := qb.NewVar("needsEmail")

			// Bind missing? result to variable
			q := qb.Query().
				Find(name, needsEmail).
				Where(
					qb.Pat(e, PersonName, name),
					qb.Missing(e, PersonEmail).As(needsEmail),
				).
				OrderBy(qb.Asc(name)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 4 {
				t.Errorf("Expected 4 results, got %d", len(results))
			}

			// Verify results
			resultMap := make(map[string]bool)
			for _, tuple := range results {
				name := tuple[0].(string)
				needsEmailVal := tuple[1].(bool)
				resultMap[name] = needsEmailVal
			}

			// Alice has email -> needsEmail = false
			if resultMap["Alice"] != false {
				t.Errorf("Alice: expected needsEmail=false, got %v", resultMap["Alice"])
			}
			// Bob has email -> needsEmail = false
			if resultMap["Bob"] != false {
				t.Errorf("Bob: expected needsEmail=false, got %v", resultMap["Bob"])
			}
			// Charlie has no email -> needsEmail = true
			if resultMap["Charlie"] != true {
				t.Errorf("Charlie: expected needsEmail=true, got %v", resultMap["Charlie"])
			}
			// Diana has no email -> needsEmail = true
			if resultMap["Diana"] != true {
				t.Errorf("Diana: expected needsEmail=true, got %v", resultMap["Diana"])
			}
		})
	}
}

// TestIntegration_GetSome tests get-some fallback attribute chain
func TestIntegration_GetSome(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			popts := mode.plannerOptions()
			db, cleanup := setupTestDBWithOptionalAttrs(t, &popts)
			defer cleanup()

			e := qb.NewVar("e")
			name := qb.NewVar("name")
			displayName := qb.NewVar("displayName")

			// Get nickname, fallback to name, fallback to email
			q := qb.Query().
				Find(name, displayName).
				Where(
					qb.Pat(e, PersonName, name),
					qb.GetSome(e, PersonNickname, PersonName, PersonEmail).As(displayName),
				).
				OrderBy(qb.Asc(name)).
				MustBuild()

			results, err := executor.CollectTuples(db.Query(q))
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(results) != 4 {
				t.Errorf("Expected 4 results, got %d", len(results))
			}

			// Note: GetSome returns a GetSomeResult struct with Attr and Value
			// The executor should handle this appropriately
			// For now, let's verify the query executes successfully
			t.Logf("GetSome results: %v", results)
		})
	}
}
