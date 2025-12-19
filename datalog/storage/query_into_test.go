package storage

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	dlreflect "github.com/wbrown/janus-datalog/datalog/reflect"
)

// Test struct types

type PersonResult struct {
	Name string `datalog:"?name"`
	Age  int64  `datalog:"?age"`
}

type TradeResult struct {
	Symbol string    `datalog:"?symbol"`
	Price  float64   `datalog:"?price"`
	Time   time.Time `datalog:"?time"`
}

type DeptStats struct {
	Dept      string  `datalog:"?dept"`
	TotalPay  float64 `datalog:"(sum ?salary)"`
	HeadCount int64   `datalog:"(count ?emp)"`
}

type PositionalResult struct {
	Name string
	Age  int64
}

type WithPointers struct {
	Name  string  `datalog:"?name"`
	Email *string `datalog:"?email"`
}

// Helper to create a test database with data
func createTestDatabaseWithPeople(t *testing.T) (*Database, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "query-into-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	db, err := NewDatabase(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create database: %v", err)
	}

	// Add test data
	alice := datalog.NewIdentity("person:alice")
	bob := datalog.NewIdentity("person:bob")
	charlie := datalog.NewIdentity("person:charlie")

	tx := db.NewTransaction()
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")
	tx.Add(alice, datalog.NewKeyword(":person/age"), int64(30))
	tx.Add(alice, datalog.NewKeyword(":person/email"), "alice@example.com")

	tx.Add(bob, datalog.NewKeyword(":person/name"), "Bob")
	tx.Add(bob, datalog.NewKeyword(":person/age"), int64(25))
	// Bob has no email

	tx.Add(charlie, datalog.NewKeyword(":person/name"), "Charlie")
	tx.Add(charlie, datalog.NewKeyword(":person/age"), int64(35))
	tx.Add(charlie, datalog.NewKeyword(":person/email"), "charlie@example.com")

	if _, err := tx.Commit(); err != nil {
		db.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to commit: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

func createTestDatabaseWithEmployees(t *testing.T) (*Database, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "query-into-emp-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	db, err := NewDatabase(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create database: %v", err)
	}

	// Add employee data for aggregation tests
	employees := []struct {
		name   string
		dept   string
		salary float64
	}{
		{"Alice", "Engineering", 100000},
		{"Bob", "Engineering", 120000},
		{"Charlie", "Engineering", 110000},
		{"Dave", "Sales", 80000},
		{"Eve", "Sales", 90000},
		{"Frank", "Marketing", 70000},
	}

	tx := db.NewTransaction()
	for _, emp := range employees {
		id := datalog.NewIdentity("emp:" + emp.name)
		tx.Add(id, datalog.NewKeyword(":employee/name"), emp.name)
		tx.Add(id, datalog.NewKeyword(":employee/dept"), emp.dept)
		tx.Add(id, datalog.NewKeyword(":employee/salary"), emp.salary)
	}

	if _, err := tx.Commit(); err != nil {
		db.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to commit: %v", err)
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}

	return db, cleanup
}

func TestQueryInto_BasicQuery(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var results []PersonResult
	err := db.QueryInto(&results, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify we got all people (order may vary)
	names := make(map[string]int64)
	for _, r := range results {
		names[r.Name] = r.Age
	}

	if names["Alice"] != 30 {
		t.Errorf("expected Alice age 30, got %d", names["Alice"])
	}
	if names["Bob"] != 25 {
		t.Errorf("expected Bob age 25, got %d", names["Bob"])
	}
	if names["Charlie"] != 35 {
		t.Errorf("expected Charlie age 35, got %d", names["Charlie"])
	}
}

func TestQueryInto_WithInputs(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Test with a bound value directly in the pattern (this is how inputs typically work)
	var results []PersonResult
	err := db.QueryInto(&results, `
		[:find ?name ?age
		 :in $ ?search-name
		 :where [?e :person/name ?search-name]
		        [?e :person/name ?name]
		        [?e :person/age ?age]]
	`, "Alice")
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Name != "Alice" {
		t.Errorf("expected Name=Alice, got %q", results[0].Name)
	}
	if results[0].Age != 30 {
		t.Errorf("expected Age=30, got %d", results[0].Age)
	}
}

func TestQueryInto_WithAggregates(t *testing.T) {
	db, cleanup := createTestDatabaseWithEmployees(t)
	defer cleanup()

	var results []DeptStats
	err := db.QueryInto(&results, `
		[:find ?dept (sum ?salary) (count ?emp)
		 :where [?emp :employee/dept ?dept]
		        [?emp :employee/salary ?salary]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 departments, got %d", len(results))
	}

	// Verify aggregates
	deptStats := make(map[string]DeptStats)
	for _, r := range results {
		deptStats[r.Dept] = r
	}

	eng := deptStats["Engineering"]
	if eng.TotalPay != 330000 {
		t.Errorf("Engineering total: expected 330000, got %f", eng.TotalPay)
	}
	if eng.HeadCount != 3 {
		t.Errorf("Engineering count: expected 3, got %d", eng.HeadCount)
	}

	sales := deptStats["Sales"]
	if sales.TotalPay != 170000 {
		t.Errorf("Sales total: expected 170000, got %f", sales.TotalPay)
	}
	if sales.HeadCount != 2 {
		t.Errorf("Sales count: expected 2, got %d", sales.HeadCount)
	}

	mkt := deptStats["Marketing"]
	if mkt.TotalPay != 70000 {
		t.Errorf("Marketing total: expected 70000, got %f", mkt.TotalPay)
	}
	if mkt.HeadCount != 1 {
		t.Errorf("Marketing count: expected 1, got %d", mkt.HeadCount)
	}
}

func TestQueryInto_PositionalMapping(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var results []PositionalResult
	err := db.QueryInto(&results, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Verify positional mapping worked
	names := make(map[string]int64)
	for _, r := range results {
		names[r.Name] = r.Age
	}

	if names["Alice"] != 30 {
		t.Errorf("expected Alice age 30, got %d", names["Alice"])
	}
}

func TestQueryInto_EmptyResults(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var results []PersonResult
	err := db.QueryInto(&results, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]
		        [(> ?age 100)]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestQueryOneInto_SingleResult(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var result PersonResult
	found, err := db.QueryOneInto(&result, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]
		        [(= ?name "Alice")]]
	`)
	if err != nil {
		t.Fatalf("QueryOneInto failed: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}

	if result.Name != "Alice" {
		t.Errorf("expected Name=Alice, got %q", result.Name)
	}
	if result.Age != 30 {
		t.Errorf("expected Age=30, got %d", result.Age)
	}
}

func TestQueryOneInto_NoResults(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var result PersonResult
	found, err := db.QueryOneInto(&result, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]
		        [(= ?name "NonExistent")]]
	`)

	// Empty result is NOT an error - it's a valid relational answer
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false for no results")
	}
}

func TestQueryOneInto_MultipleResults(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var result PersonResult
	found, err := db.QueryOneInto(&result, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]]
	`)

	if err == nil {
		t.Fatal("expected error for multiple results")
	}
	if found {
		t.Error("expected found=false when error occurs")
	}
	if !errors.Is(err, dlreflect.ErrMultipleResults) {
		t.Errorf("expected ErrMultipleResults, got %v", err)
	}
}

func TestQueryInto_InvalidDest(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Test non-pointer
	var results []PersonResult
	err := db.QueryInto(results, `[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
	if err == nil {
		t.Error("expected error for non-pointer dest")
	}

	// Test pointer to non-slice
	var single PersonResult
	err = db.QueryInto(&single, `[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
	if err == nil {
		t.Error("expected error for pointer to non-slice")
	}

	// Test pointer to slice of non-structs
	var ints []int
	err = db.QueryInto(&ints, `[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age]]`)
	if err == nil {
		t.Error("expected error for slice of non-structs")
	}
}

func TestQueryOneInto_InvalidDest(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Test non-pointer
	var result PersonResult
	_, err := db.QueryOneInto(result, `[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age] [(= ?name "Alice")]]`)
	if err == nil {
		t.Error("expected error for non-pointer dest")
	}

	// Test pointer to non-struct
	var str string
	_, err = db.QueryOneInto(&str, `[:find ?name ?age :where [?e :person/name ?name] [?e :person/age ?age] [(= ?name "Alice")]]`)
	if err == nil {
		t.Error("expected error for pointer to non-struct")
	}
}

func TestQueryInto_SymbolNotFound(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	type BadStruct struct {
		Name     string `datalog:"?name"`
		NotThere string `datalog:"?notfound"` // This symbol doesn't exist in query
	}

	var results []BadStruct
	err := db.QueryInto(&results, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]]
	`)

	if err == nil {
		t.Fatal("expected error for missing symbol")
	}
	if !errors.Is(err, dlreflect.ErrSymbolNotFound) {
		t.Errorf("expected ErrSymbolNotFound, got %v", err)
	}
}

func TestQueryInto_TypeCoercion(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Test int64 -> int coercion
	type SmallResult struct {
		Name string `datalog:"?name"`
		Age  int    `datalog:"?age"` // int, not int64
	}

	var results []SmallResult
	err := db.QueryInto(&results, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]
		        [(= ?name "Alice")]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Age != 30 {
		t.Errorf("expected Age=30, got %d", results[0].Age)
	}
}

func TestQueryInto_ParseError(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var results []PersonResult
	err := db.QueryInto(&results, `[:find ?name :where [invalid syntax`)

	if err == nil {
		t.Fatal("expected parse error")
	}
}

// Scalar type tests

func TestQueryInto_ScalarString(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var names []string
	err := db.QueryInto(&names, `
		[:find ?name
		 :where [?e :person/name ?name]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}

	// Check that we got the expected names (order may vary)
	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	for _, expected := range []string{"Alice", "Bob", "Charlie"} {
		if !nameSet[expected] {
			t.Errorf("expected name %q not found", expected)
		}
	}
}

func TestQueryInto_ScalarInt64(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var ages []int64
	err := db.QueryInto(&ages, `
		[:find ?age
		 :where [?e :person/age ?age]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(ages) != 3 {
		t.Fatalf("expected 3 ages, got %d", len(ages))
	}
}

func TestQueryInto_ScalarIdentity(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var entities []datalog.Identity
	err := db.QueryInto(&entities, `
		[:find ?e
		 :where [?e :person/name ?name]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(entities) != 3 {
		t.Fatalf("expected 3 entities, got %d", len(entities))
	}

	// Verify these are valid identities
	for i, e := range entities {
		if e.String() == "" {
			t.Errorf("entity %d has empty string representation", i)
		}
	}
}

func TestQueryInto_ScalarMultiColumnError(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var names []string
	err := db.QueryInto(&names, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]]
	`)

	if err == nil {
		t.Fatal("expected error for multi-column query with scalar slice")
	}
}

func TestQueryOneInto_ScalarString(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var name string
	found, err := db.QueryOneInto(&name, `
		[:find ?name
		 :where [?e :person/name ?name]
		        [(= ?name "Alice")]]
	`)
	if err != nil {
		t.Fatalf("QueryOneInto failed: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if name != "Alice" {
		t.Errorf("expected name=Alice, got %q", name)
	}
}

func TestQueryOneInto_ScalarInt64(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var age int64
	found, err := db.QueryOneInto(&age, `
		[:find ?age
		 :where [?e :person/name "Alice"]
		        [?e :person/age ?age]]
	`)
	if err != nil {
		t.Fatalf("QueryOneInto failed: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}
	if age != 30 {
		t.Errorf("expected age=30, got %d", age)
	}
}

func TestQueryOneInto_ScalarNotFound(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var name string
	found, err := db.QueryOneInto(&name, `
		[:find ?name
		 :where [?e :person/name ?name]
		        [(= ?name "NonExistent")]]
	`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found {
		t.Error("expected found=false for no results")
	}
}

func TestQueryOneInto_ScalarMultiColumnError(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var name string
	_, err := db.QueryOneInto(&name, `
		[:find ?name ?age
		 :where [?e :person/name ?name]
		        [?e :person/age ?age]]
	`)

	if err == nil {
		t.Fatal("expected error for multi-column query with scalar destination")
	}
}

func TestQueryOneInto_ScalarMultipleResults(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var name string
	found, err := db.QueryOneInto(&name, `
		[:find ?name
		 :where [?e :person/name ?name]]
	`)

	if err == nil {
		t.Fatal("expected error for multiple results")
	}
	if found {
		t.Error("expected found=false when error occurs")
	}
	if !errors.Is(err, dlreflect.ErrMultipleResults) {
		t.Errorf("expected ErrMultipleResults, got %v", err)
	}
}
