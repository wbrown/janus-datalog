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

// createQueryIntoTestDB creates a test database with data
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

// TestQueryInto_KeywordField tests that Keyword fields in result structs work correctly
// This is a regression test for a bug where keywordType was defined with .Elem()
// which caused "value of type *datalog.keyword is not assignable to type datalog.keyword"
func TestQueryInto_KeywordField(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Add some data with keyword values
	statusKw := datalog.NewKeyword(":person/status")
	activeKw := datalog.NewKeyword(":status/active")

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("alice")
	tx.Add(alice, statusKw, activeKw)
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Query into struct with Keyword field
	type PersonStatus struct {
		Entity datalog.Identity `datalog:"?e"`
		Status datalog.Keyword  `datalog:"?status"`
	}

	var results []PersonStatus
	err := db.QueryInto(&results, `
		[:find ?e ?status
		 :where [?e :person/status ?status]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Status == nil {
		t.Error("expected Status to be non-nil")
	} else if results[0].Status.String() != ":status/active" {
		t.Errorf("expected Status ':status/active', got %q", results[0].Status.String())
	}

	// Verify interning - should be the same pointer
	if results[0].Status != activeKw {
		t.Error("expected Status to be same interned keyword pointer")
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
		t.Fatal("expected error for multi-symbol query with scalar slice")
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
		t.Fatal("expected error for multi-symbol query with scalar destination")
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

// =============================================================================
// Pull Mapping Tests - QueryInto with pull expressions
// =============================================================================

// PullResult maps attributes from a pull expression result
type PullResult struct {
	ID   datalog.Identity `datalog:"db/id"`
	Name string           `datalog:"person/name"`
	Age  int64            `datalog:"person/age"`
}

// MixedModeResult combines query variables with pull attributes
type MixedModeResult struct {
	// Query variable - comes from tuple symbol
	PersonName string `datalog:"?name"`
	// Attribute tags - come from pull result map in tuple
	ID  datalog.Identity `datalog:"db/id"`
	Age int64            `datalog:"person/age"`
}

func TestQueryInto_PullExpression(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Query with pull expression - returns entity attributes as map
	var results []PullResult
	err := db.QueryInto(&results, `
		[:find (pull ?e [:db/id :person/name :person/age])
		 :where [?e :person/name ?name]
		        [(= ?name "Alice")]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	result := results[0]
	if result.Name != "Alice" {
		t.Errorf("expected Name='Alice', got %q", result.Name)
	}
	if result.Age != 30 {
		t.Errorf("expected Age=30, got %d", result.Age)
	}
	if result.ID == nil {
		t.Error("expected ID to be non-nil")
	}
}

func TestQueryInto_PullWildcard(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Query with pull wildcard - returns all entity attributes
	var results []PullResult
	err := db.QueryInto(&results, `
		[:find (pull ?e [*])
		 :where [?e :person/name "Bob"]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	result := results[0]
	if result.Name != "Bob" {
		t.Errorf("expected Name='Bob', got %q", result.Name)
	}
	if result.Age != 25 {
		t.Errorf("expected Age=25, got %d", result.Age)
	}
}

func TestQueryInto_MixedMode(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Mixed mode: query variable + pull expression
	// This tests the full flow: query -> executor -> pull -> struct mapping
	var results []MixedModeResult
	err := db.QueryInto(&results, `
		[:find ?name (pull ?e [:db/id :person/age])
		 :where [?e :person/name ?name]]
	`)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) < 2 {
		t.Fatalf("expected at least 2 results, got %d", len(results))
	}

	// Check that both query variable and pull attributes are mapped
	foundAlice := false
	for _, r := range results {
		if r.PersonName == "Alice" {
			foundAlice = true
			if r.Age != 30 {
				t.Errorf("Alice: expected Age=30, got %d", r.Age)
			}
			if r.ID == nil {
				t.Error("Alice: expected ID to be non-nil")
			}
		}
	}

	if !foundAlice {
		t.Error("expected to find Alice in results")
	}
}

func TestQueryOneInto_PullExpression(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var result PullResult
	found, err := db.QueryOneInto(&result, `
		[:find (pull ?e [*])
		 :where [?e :person/name "Alice"]]
	`)
	if err != nil {
		t.Fatalf("QueryOneInto failed: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}

	if result.Name != "Alice" {
		t.Errorf("expected Name='Alice', got %q", result.Name)
	}
	if result.Age != 30 {
		t.Errorf("expected Age=30, got %d", result.Age)
	}
	if result.ID == nil {
		t.Error("expected ID to be non-nil")
	}
}

func TestQueryOneInto_MixedMode(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	var result MixedModeResult
	found, err := db.QueryOneInto(&result, `
		[:find ?name (pull ?e [:db/id :person/age])
		 :where [?e :person/name ?name]
		        [(= ?name "Bob")]]
	`)
	if err != nil {
		t.Fatalf("QueryOneInto failed: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}

	// Check query variable
	if result.PersonName != "Bob" {
		t.Errorf("expected PersonName='Bob', got %q", result.PersonName)
	}

	// Check pull attributes
	if result.Age != 25 {
		t.Errorf("expected Age=25, got %d", result.Age)
	}
	if result.ID == nil {
		t.Error("expected ID to be non-nil")
	}
}

// =============================================================================
// Any/Interface{} Field Tests - heterogeneous value support
// =============================================================================

// EntityAttr is used for entity enumeration queries where ?v can be any type
type EntityAttr struct {
	Attr  datalog.Keyword `datalog:"?a"`
	Value any             `datalog:"?v"`
}

func TestQueryInto_AnyField(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Get Alice's entity ID for the query
	alice := datalog.NewIdentity("person:alice")

	// Query all attributes of Alice - ?v will contain heterogeneous types
	var attrs []EntityAttr
	err := db.QueryInto(&attrs, `
		[:find ?a ?v
		 :in $ ?e
		 :where [?e ?a ?v]]
	`, alice)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	// Alice has 3 attributes: :person/name (string), :person/age (int64), :person/email (string)
	if len(attrs) != 3 {
		t.Fatalf("expected 3 attributes, got %d", len(attrs))
	}

	// Build a map for easier verification
	attrMap := make(map[string]any)
	for _, a := range attrs {
		attrMap[a.Attr.String()] = a.Value
	}

	// Check string value
	if name, ok := attrMap[":person/name"].(string); !ok {
		t.Errorf("expected :person/name to be string, got %T", attrMap[":person/name"])
	} else if name != "Alice" {
		t.Errorf("expected name='Alice', got %q", name)
	}

	// Check int64 value
	if age, ok := attrMap[":person/age"].(int64); !ok {
		t.Errorf("expected :person/age to be int64, got %T", attrMap[":person/age"])
	} else if age != 30 {
		t.Errorf("expected age=30, got %d", age)
	}

	// Check another string value
	if email, ok := attrMap[":person/email"].(string); !ok {
		t.Errorf("expected :person/email to be string, got %T", attrMap[":person/email"])
	} else if email != "alice@example.com" {
		t.Errorf("expected email='alice@example.com', got %q", email)
	}
}

func TestQueryInto_AnyFieldWithKeywordValue(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	// Add a keyword-valued attribute
	statusKw := datalog.NewKeyword(":person/status")
	activeKw := datalog.NewKeyword(":status/active")

	tx := db.NewTransaction()
	alice := datalog.NewIdentity("person:alice")
	tx.Add(alice, statusKw, activeKw)
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Query the status attribute specifically
	type StatusResult struct {
		Status any `datalog:"?v"`
	}

	var results []StatusResult
	err := db.QueryInto(&results, `
		[:find ?v
		 :in $ ?e
		 :where [?e :person/status ?v]]
	`, alice)
	if err != nil {
		t.Fatalf("QueryInto failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// The value should be a Keyword
	if kw, ok := results[0].Status.(datalog.Keyword); !ok {
		t.Errorf("expected Status to be Keyword, got %T", results[0].Status)
	} else if kw.String() != ":status/active" {
		t.Errorf("expected ':status/active', got %q", kw.String())
	}
}

func TestQueryOneInto_AnyField(t *testing.T) {
	db, cleanup := createTestDatabaseWithPeople(t)
	defer cleanup()

	alice := datalog.NewIdentity("person:alice")

	type SingleAttr struct {
		Value any `datalog:"?v"`
	}

	var result SingleAttr
	found, err := db.QueryOneInto(&result, `
		[:find ?v
		 :in $ ?e
		 :where [?e :person/name ?v]]
	`, alice)
	if err != nil {
		t.Fatalf("QueryOneInto failed: %v", err)
	}
	if !found {
		t.Fatal("expected found=true")
	}

	if name, ok := result.Value.(string); !ok {
		t.Errorf("expected Value to be string, got %T", result.Value)
	} else if name != "Alice" {
		t.Errorf("expected 'Alice', got %q", name)
	}
}
