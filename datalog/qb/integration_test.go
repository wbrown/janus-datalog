package qb_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
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

// setupTestDB creates a test database with sample data
func setupTestDB(t *testing.T) (*storage.Database, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "qb-integration-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := storage.NewDatabase(dbPath)
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
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()

	q := qb.Query().
		Find(name).
		Where(qb.Pat(e, PersonName, name)).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Verify all names are present
	names := make(map[string]bool)
	for _, row := range results {
		if name, ok := row[0].(string); ok {
			names[name] = true
		}
	}

	expected := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
	for _, exp := range expected {
		if !names[exp] {
			t.Errorf("Missing expected name: %s", exp)
		}
	}
}

// TestIntegration_JoinQuery tests that same variable creates join
func TestIntegration_JoinQuery(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// Same `e` variable in both patterns = join on entity
	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()

	q := qb.Query().
		Find(name, age).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonAge, age),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Verify Alice is 30
	for _, row := range results {
		if row[0] == "Alice" {
			if age, ok := row[1].(int64); ok {
				if age != 30 {
					t.Errorf("Expected Alice age 30, got %d", age)
				}
			}
		}
	}
}

// TestIntegration_PredicateQuery tests queries with predicates
func TestIntegration_PredicateQuery(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()

	// Find people over 30
	q := qb.Query().
		Find(name).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonAge, age),
			qb.Gt(age, qb.V(int64(30))),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should be Charlie (35) and Eve (32)
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	names := make(map[string]bool)
	for _, row := range results {
		if name, ok := row[0].(string); ok {
			names[name] = true
		}
	}

	if !names["Charlie"] {
		t.Error("Expected Charlie in results")
	}
	if !names["Eve"] {
		t.Error("Expected Eve in results")
	}
}

// TestIntegration_MultiplePredicates tests queries with multiple predicates
func TestIntegration_MultiplePredicates(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()
	city := qb.NewVar()

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

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should be Alice (30, NYC) and Charlie (35, NYC)
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TestIntegration_AggregationQuery tests queries with aggregation
func TestIntegration_AggregationQuery(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	dept := qb.NewVar()
	salary := qb.NewVar()

	// Sum salary by department
	q := qb.Query().
		Find(dept, qb.Sum(salary)).
		Where(
			qb.Pat(e, PersonDept, dept),
			qb.Pat(e, PersonSalary, salary),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should have 2 departments
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Verify totals
	totals := make(map[string]float64)
	for _, row := range results {
		if d, ok := row[0].(string); ok {
			if s, ok := row[1].(float64); ok {
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
}

// TestIntegration_CountAggregation tests count aggregation
func TestIntegration_CountAggregation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	dept := qb.NewVar()
	name := qb.NewVar()

	// Count people by department
	q := qb.Query().
		Find(dept, qb.Count(name)).
		Where(
			qb.Pat(e, PersonDept, dept),
			qb.Pat(e, PersonName, name),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	counts := make(map[string]int64)
	for _, row := range results {
		if d, ok := row[0].(string); ok {
			if c, ok := row[1].(int64); ok {
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
}

// TestIntegration_InputParameters tests queries with input parameters
func TestIntegration_InputParameters(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	inputName := qb.NewVar()
	age := qb.NewVar()

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
	results, err := db.ExecuteQueryWithInputs(q, "Alice")
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
}

// TestIntegration_CollectionInput tests queries with collection input
func TestIntegration_CollectionInput(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	inputName := qb.NewVar()
	age := qb.NewVar()

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
	results, err := db.ExecuteQueryWithInputs(q, []string{"Alice", "Bob"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should be Alice and Bob
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	names := make(map[string]bool)
	for _, row := range results {
		if n, ok := row[0].(string); ok {
			names[n] = true
		}
	}
	if !names["Alice"] || !names["Bob"] {
		t.Errorf("Expected Alice and Bob, got %v", names)
	}
}

// TestIntegration_OrderBy tests queries with ordering
func TestIntegration_OrderBy(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()

	q := qb.Query().
		Find(name, age).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonAge, age),
		).
		OrderBy(qb.Desc(age)).
		MustBuild()

	results, err := db.ExecuteQuery(q)
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
}

// TestIntegration_QueryInto tests QueryInto with struct mapping
// Uses positional mapping - no datalog tags needed, fields map by order
func TestIntegration_QueryInto(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// No datalog tags - fields map positionally to Find() order
	type PersonResult struct {
		Name string
		Age  int64
	}

	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()

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
}

// TestIntegration_QueryOneInto tests QueryOneInto for single result
// Uses positional mapping - no datalog tags needed
func TestIntegration_QueryOneInto(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// No datalog tags - fields map positionally
	type PersonResult struct {
		Name string
		Age  int64
	}

	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()

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
}

// TestIntegration_QueryIntoWithAggregation tests QueryInto with aggregation functions
// Uses positional mapping - no datalog tags needed
func TestIntegration_QueryIntoWithAggregation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// No datalog tags - fields map positionally to Find() order
	type DeptStats struct {
		Dept  string  // Position 0: dept
		Avg   float64 // Position 1: (avg salary)
		Count int64   // Position 2: (count e)
	}

	e := qb.NewVar()
	dept := qb.NewVar()
	salary := qb.NewVar()

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
}

// TestIntegration_CompareWithEDN verifies built query produces same results as EDN string
func TestIntegration_CompareWithEDN(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	// EDN query
	ednQuery := `[:find ?name ?age
                  :where
                  [?e :person/name ?name]
                  [?e :person/age ?age]
                  [(> ?age 30)]]`

	// Built query
	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()

	builtQuery := qb.Query().
		Find(name, age).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonAge, age),
			qb.Gt(age, qb.V(int64(30))),
		).
		MustBuild()

	// Execute both
	ednResults, err := db.ExecuteQuery(ednQuery)
	if err != nil {
		t.Fatalf("EDN query failed: %v", err)
	}

	builtResults, err := db.ExecuteQuery(builtQuery)
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

	for _, row := range ednResults {
		if n, ok := row[0].(string); ok {
			ednNames[n] = true
		}
	}
	for _, row := range builtResults {
		if n, ok := row[0].(string); ok {
			builtNames[n] = true
		}
	}

	if !ednNames["Charlie"] || !ednNames["Eve"] {
		t.Error("EDN query missing expected results")
	}
	if !builtNames["Charlie"] || !builtNames["Eve"] {
		t.Error("Built query missing expected results")
	}
}

// TestIntegration_ConstantValue tests pattern with constant value
func TestIntegration_ConstantValue(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()

	// Find people in NYC (constant value in pattern)
	q := qb.Query().
		Find(name).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonCity, qb.V("NYC")),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should be Alice and Charlie
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}

// TestIntegration_BooleanPredicate tests boolean comparison
func TestIntegration_BooleanPredicate(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()
	active := qb.NewVar()

	// Find active people
	q := qb.Query().
		Find(name).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonActive, active),
			qb.Eq(active, qb.V(true)),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should be Alice, Bob, Charlie, Eve (Diana is inactive)
	if len(results) != 4 {
		t.Errorf("Expected 4 active people, got %d", len(results))
	}
}

// TestIntegration_AvgAggregation tests average aggregation
func TestIntegration_AvgAggregation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	age := qb.NewVar()

	// Average age of all people
	q := qb.Query().
		Find(qb.Avg(age)).
		Where(
			qb.Pat(e, PersonAge, age),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
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
}

// TestIntegration_MinMaxAggregation tests min/max aggregation
func TestIntegration_MinMaxAggregation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	salary := qb.NewVar()

	// Min and max salary (separate queries since we can't have two aggregates without grouping)
	minQ := qb.Query().
		Find(qb.Min(salary)).
		Where(qb.Pat(e, PersonSalary, salary)).
		MustBuild()

	maxQ := qb.Query().
		Find(qb.Max(salary)).
		Where(qb.Pat(e, PersonSalary, salary)).
		MustBuild()

	minResults, err := db.ExecuteQuery(minQ)
	if err != nil {
		t.Fatalf("Min query failed: %v", err)
	}

	maxResults, err := db.ExecuteQuery(maxQ)
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
}

// TestIntegration_ThreeWayJoin tests joining three patterns
func TestIntegration_ThreeWayJoin(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()
	city := qb.NewVar()

	q := qb.Query().
		Find(name, age, city).
		Where(
			qb.Pat(e, PersonName, name),
			qb.Pat(e, PersonAge, age),
			qb.Pat(e, PersonCity, city),
		).
		MustBuild()

	results, err := db.ExecuteQuery(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 5 {
		t.Errorf("Expected 5 results, got %d", len(results))
	}

	// Verify Alice's data
	for _, row := range results {
		if row[0] == "Alice" {
			if row[1] != int64(30) {
				t.Errorf("Expected Alice age 30, got %v", row[1])
			}
			if row[2] != "NYC" {
				t.Errorf("Expected Alice city NYC, got %v", row[2])
			}
		}
	}
}

// TestIntegration_Explain tests the Explain function with built query
func TestIntegration_Explain(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	name := qb.NewVar()
	age := qb.NewVar()

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
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	inputName := qb.NewVar()
	inputAge := qb.NewVar()
	city := qb.NewVar()

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
	results, err := db.ExecuteQueryWithInputs(q, []interface{}{"Alice", int64(30)})
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
}

// TestIntegration_RelationInput tests queries with relation input [[?x ?y] ...]
func TestIntegration_RelationInput(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	inputName := qb.NewVar()
	inputCity := qb.NewVar()
	age := qb.NewVar()

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
	results, err := db.ExecuteQueryWithInputs(q, [][]interface{}{
		{"Alice", "NYC"},
		{"Eve", "LA"},
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Verify we got Alice and Eve
	names := make(map[string]bool)
	for _, row := range results {
		if n, ok := row[0].(string); ok {
			names[n] = true
		}
	}
	if !names["Alice"] || !names["Eve"] {
		t.Errorf("Expected Alice and Eve, got %v", names)
	}
}

// TestIntegration_RelationInputNoMatch tests relation input with no matching data
func TestIntegration_RelationInputNoMatch(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	inputName := qb.NewVar()
	inputCity := qb.NewVar()
	age := qb.NewVar()

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
	results, err := db.ExecuteQueryWithInputs(q, [][]interface{}{
		{"Alice", "LA"},
		{"Bob", "NYC"},
	})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Neither should match
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d: %v", len(results), results)
	}
}

// TestIntegration_MultipleScalarInputs tests queries with multiple scalar inputs
func TestIntegration_MultipleScalarInputs(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	e := qb.NewVar()
	inputName := qb.NewVar()
	inputCity := qb.NewVar()
	age := qb.NewVar()

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
	results, err := db.ExecuteQueryWithInputs(q, "Alice", "NYC")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if len(results) > 0 && results[0][0] != "Alice" {
		t.Errorf("Expected Alice, got %v", results[0][0])
	}
}

// TestIntegration_CompareInputBindingsWithEDN verifies input bindings match EDN behavior
func TestIntegration_CompareInputBindingsWithEDN(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	t.Run("ScalarInput", func(t *testing.T) {
		// EDN query with scalar input
		ednQuery := `[:find ?name ?age
		              :in $ ?name
		              :where
		              [?e :person/name ?name]
		              [?e :person/age ?age]]`

		// Built query
		e := qb.NewVar()
		inputName := qb.NewVar()
		age := qb.NewVar()

		builtQuery := qb.Query().
			Find(inputName, age).
			In(qb.DB, qb.Scalar(inputName)).
			Where(
				qb.Pat(e, PersonName, inputName),
				qb.Pat(e, PersonAge, age),
			).
			MustBuild()

		ednResults, err := db.ExecuteQueryWithInputs(ednQuery, "Bob")
		if err != nil {
			t.Fatalf("EDN query failed: %v", err)
		}

		builtResults, err := db.ExecuteQueryWithInputs(builtQuery, "Bob")
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
		e := qb.NewVar()
		inputName := qb.NewVar()
		age := qb.NewVar()

		builtQuery := qb.Query().
			Find(inputName, age).
			In(qb.DB, qb.Collection(inputName)).
			Where(
				qb.Pat(e, PersonName, inputName),
				qb.Pat(e, PersonAge, age),
			).
			MustBuild()

		ednResults, err := db.ExecuteQueryWithInputs(ednQuery, []string{"Alice", "Charlie"})
		if err != nil {
			t.Fatalf("EDN query failed: %v", err)
		}

		builtResults, err := db.ExecuteQueryWithInputs(builtQuery, []string{"Alice", "Charlie"})
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
		e := qb.NewVar()
		inputName := qb.NewVar()
		inputCity := qb.NewVar()
		age := qb.NewVar()

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

		ednResults, err := db.ExecuteQueryWithInputs(ednQuery, input)
		if err != nil {
			t.Fatalf("EDN query failed: %v", err)
		}

		builtResults, err := db.ExecuteQueryWithInputs(builtQuery, input)
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
}
