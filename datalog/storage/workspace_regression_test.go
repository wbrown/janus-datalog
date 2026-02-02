package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// =============================================================================
// Workspace Reuse Regression Tests
//
// These tests isolate each step of the query pipeline to identify where
// tuple corruption from workspace reuse occurs.
//
// Test scenario: 10 people with ages cycling 20, 25, 30
// Query: find ages >= 25 (should return 25, 30)
// =============================================================================

func createWorkspaceTestDB(t *testing.T) (*Database, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "workspace_regression_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create database: %v", err)
	}

	tx := db.NewTransaction()
	age := datalog.NewKeyword(":person/age")

	// Create 10 people with ages: 20, 25, 30, 20, 25, 30, 20, 25, 30, 20
	for i := 0; i < 10; i++ {
		e := datalog.NewIdentity(fmt.Sprintf("person:%d", i))
		ageVal := int64(20 + (i%3)*5)
		tx.Add(e, age, ageVal)
	}

	if _, err := tx.Commit(); err != nil {
		db.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("commit failed: %v", err)
	}

	return db, func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}
}

// Test 1: Raw storage iterator - verify tuples are produced correctly
func TestWorkspaceRegression_RawIterator(t *testing.T) {
	db, cleanup := createWorkspaceTestDB(t)
	defer cleanup()

	// Create pattern to match all ages
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":person/age")},
			query.Variable{Name: datalog.NewSymbol("?age")},
			query.Blank{},
		},
	}

	matcher := db.Matcher().(*BadgerMatcher)
	rel, err := matcher.Match(pattern, nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Collect all tuples and their ages
	var ages []int64
	var tuples []executor.Tuple
	it := rel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		// Copy the tuple to preserve it
		tupleCopy := make(executor.Tuple, len(tuple))
		copy(tupleCopy, tuple)
		tuples = append(tuples, tupleCopy)

		if len(tuple) >= 2 {
			if age, ok := tuple[1].(int64); ok {
				ages = append(ages, age)
			} else {
				t.Errorf("unexpected age type: %T", tuple[1])
			}
		}
	}
	it.Close()

	t.Logf("Raw iterator produced %d tuples", len(tuples))
	t.Logf("Ages: %v", ages)

	// Should have 10 tuples
	if len(tuples) != 10 {
		t.Errorf("expected 10 tuples, got %d", len(tuples))
	}

	// Count age occurrences
	ageCounts := make(map[int64]int)
	for _, age := range ages {
		ageCounts[age]++
	}
	t.Logf("Age counts: %v", ageCounts)

	// Should have: 20 (4 times), 25 (3 times), 30 (3 times)
	if ageCounts[20] != 4 {
		t.Errorf("expected 4 people with age 20, got %d", ageCounts[20])
	}
	if ageCounts[25] != 3 {
		t.Errorf("expected 3 people with age 25, got %d", ageCounts[25])
	}
	if ageCounts[30] != 3 {
		t.Errorf("expected 3 people with age 30, got %d", ageCounts[30])
	}
}

// Test 2: Verify workspace reuse - immediate reads are correct, stored refs share memory
func TestWorkspaceRegression_WorkspaceReuse(t *testing.T) {
	db, cleanup := createWorkspaceTestDB(t)
	defer cleanup()

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":person/age")},
			query.Variable{Name: datalog.NewSymbol("?age")},
			query.Blank{},
		},
	}

	matcher := db.Matcher().(*BadgerMatcher)
	rel, err := matcher.Match(pattern, nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Verify that IMMEDIATE reads produce correct values
	var immediateAges []int64
	it := rel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		if age, ok := tuple[1].(int64); ok {
			immediateAges = append(immediateAges, age)
		}
	}
	it.Close()

	// Should have 10 tuples with correct age distribution
	if len(immediateAges) != 10 {
		t.Fatalf("expected 10 ages, got %d", len(immediateAges))
	}

	ageCounts := make(map[int64]int)
	for _, age := range immediateAges {
		ageCounts[age]++
	}

	if ageCounts[20] != 4 {
		t.Errorf("expected 4 people with age 20, got %d", ageCounts[20])
	}
	if ageCounts[25] != 3 {
		t.Errorf("expected 3 people with age 25, got %d", ageCounts[25])
	}
	if ageCounts[30] != 3 {
		t.Errorf("expected 3 people with age 30, got %d", ageCounts[30])
	}

	// Now verify that stored references share memory (workspace reuse is active)
	rel2, _ := matcher.Match(pattern, nil)
	var storedTuples []executor.Tuple
	it2 := rel2.Iterator()
	for it2.Next() {
		// Store reference without copying
		storedTuples = append(storedTuples, it2.Tuple())
	}
	it2.Close()

	// All stored tuples should point to same backing array (workspace)
	if len(storedTuples) > 1 {
		// Check if first and last tuple share the same backing array
		first := storedTuples[0]
		last := storedTuples[len(storedTuples)-1]

		// They should have the same values because they point to same workspace
		sameValues := true
		for i := range first {
			if first[i] != last[i] {
				sameValues = false
				break
			}
		}

		if !sameValues {
			t.Errorf("workspace reuse not working: stored tuples have different values")
		}
	}
}

// Test 3: Verify predicate filtering works correctly
func TestWorkspaceRegression_PredicateFilter(t *testing.T) {
	db, cleanup := createWorkspaceTestDB(t)
	defer cleanup()

	// Query with predicate
	results, err := db.ExecuteQuery(`
		[:find ?e ?age
		 :where [?e :person/age ?age]
		        [(>= ?age 25)]]
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Predicate filter returned %d results", len(results))

	// Should have 6 results (3 with age 25, 3 with age 30)
	if len(results) != 6 {
		t.Errorf("expected 6 results (ages >= 25), got %d", len(results))
	}

	// Check that all ages are >= 25
	for i, row := range results {
		if len(row) >= 2 {
			age, ok := row[1].(int64)
			if !ok {
				t.Errorf("row %d: unexpected age type %T", i, row[1])
				continue
			}
			if age < 25 {
				t.Errorf("row %d: age %d should not pass filter >= 25", i, age)
			}
			t.Logf("Row %d: age=%d", i, age)
		}
	}
}

// Test 4: Projection to single column
func TestWorkspaceRegression_Projection(t *testing.T) {
	db, cleanup := createWorkspaceTestDB(t)
	defer cleanup()

	// Query projecting only age
	results, err := db.ExecuteQuery(`
		[:find ?age
		 :where [?e :person/age ?age]]
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Projection returned %d results", len(results))

	// With set semantics, should have 3 unique ages
	if len(results) != 3 {
		t.Errorf("expected 3 unique ages, got %d: %v", len(results), results)
	}

	// Collect ages
	ageSet := make(map[int64]bool)
	for _, row := range results {
		if len(row) >= 1 {
			if age, ok := row[0].(int64); ok {
				ageSet[age] = true
				t.Logf("Age: %d", age)
			}
		}
	}

	// Should have 20, 25, 30
	for _, expected := range []int64{20, 25, 30} {
		if !ageSet[expected] {
			t.Errorf("missing expected age %d", expected)
		}
	}
}

// Test 5: Filter + projection (the failing case)
func TestWorkspaceRegression_FilterThenProject(t *testing.T) {
	db, cleanup := createWorkspaceTestDB(t)
	defer cleanup()

	results, err := db.ExecuteQuery(`
		[:find ?age
		 :where [?e :person/age ?age]
		        [(>= ?age 25)]]
	`)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	t.Logf("Filter+Project returned %d results: %v", len(results), results)

	// Should have 2 unique ages: 25 and 30
	if len(results) != 2 {
		t.Errorf("expected 2 unique ages (25, 30), got %d: %v", len(results), results)
	}

	// Verify the ages are correct
	ageSet := make(map[int64]bool)
	for _, row := range results {
		if len(row) >= 1 {
			if age, ok := row[0].(int64); ok {
				ageSet[age] = true
				if age < 25 {
					t.Errorf("age %d should not be in results (filter >= 25)", age)
				}
			}
		}
	}

	if !ageSet[25] {
		t.Errorf("missing expected age 25")
	}
	if !ageSet[30] {
		t.Errorf("missing expected age 30")
	}
	if ageSet[20] {
		t.Errorf("age 20 should not be in results (filter >= 25)")
	}
}

// Test 6: Verify StreamingRelation caching copies tuples
func TestWorkspaceRegression_StreamingRelationCache(t *testing.T) {
	db, cleanup := createWorkspaceTestDB(t)
	defer cleanup()

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":person/age")},
			query.Variable{Name: datalog.NewSymbol("?age")},
			query.Blank{},
		},
	}

	matcher := db.Matcher().(*BadgerMatcher)
	rel, err := matcher.Match(pattern, nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Force caching by calling Materialize
	matRel := rel.Materialize()

	// Iterate twice to verify cache works
	var firstPass []int64
	it1 := matRel.Iterator()
	for it1.Next() {
		tuple := it1.Tuple()
		if age, ok := tuple[1].(int64); ok {
			firstPass = append(firstPass, age)
		}
	}
	it1.Close()

	var secondPass []int64
	it2 := matRel.Iterator()
	for it2.Next() {
		tuple := it2.Tuple()
		if age, ok := tuple[1].(int64); ok {
			secondPass = append(secondPass, age)
		}
	}
	it2.Close()

	t.Logf("First pass: %v", firstPass)
	t.Logf("Second pass: %v", secondPass)

	// Both passes should have same values
	if len(firstPass) != len(secondPass) {
		t.Errorf("pass lengths differ: %d vs %d", len(firstPass), len(secondPass))
	}

	for i := range firstPass {
		if i < len(secondPass) && firstPass[i] != secondPass[i] {
			t.Errorf("value mismatch at %d: %d vs %d", i, firstPass[i], secondPass[i])
		}
	}
}

// Test 7: BufferedIterator correctly copies from workspace-reusing iterator
func TestWorkspaceRegression_BufferedIterator(t *testing.T) {
	db, cleanup := createWorkspaceTestDB(t)
	defer cleanup()

	agePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: datalog.NewKeyword(":person/age")},
			query.Variable{Name: datalog.NewSymbol("?age")},
			query.Blank{},
		},
	}

	matcher := db.Matcher().(*BadgerMatcher)
	rel, _ := matcher.Match(agePattern, nil)

	// Wrap streaming iterator in BufferedIterator
	buffered := executor.NewBufferedIterator(rel.Iterator())

	// First pass - consume and buffer
	var firstPassAges []int64
	for buffered.Next() {
		tuple := buffered.Tuple()
		if age, ok := tuple[1].(int64); ok {
			firstPassAges = append(firstPassAges, age)
		}
	}

	if len(firstPassAges) != 10 {
		t.Fatalf("expected 10 ages on first pass, got %d", len(firstPassAges))
	}

	// Reset and re-iterate
	buffered.Reset()

	var secondPassAges []int64
	for buffered.Next() {
		tuple := buffered.Tuple()
		if age, ok := tuple[1].(int64); ok {
			secondPassAges = append(secondPassAges, age)
		}
	}

	// Both passes should have identical values
	if len(secondPassAges) != len(firstPassAges) {
		t.Fatalf("pass lengths differ: %d vs %d", len(firstPassAges), len(secondPassAges))
	}

	for i := range firstPassAges {
		if firstPassAges[i] != secondPassAges[i] {
			t.Errorf("value mismatch at %d: first=%d second=%d", i, firstPassAges[i], secondPassAges[i])
		}
	}

	// Verify we got distinct ages (not all same due to workspace corruption)
	ageCounts := make(map[int64]int)
	for _, age := range firstPassAges {
		ageCounts[age]++
	}
	if ageCounts[20] != 4 || ageCounts[25] != 3 || ageCounts[30] != 3 {
		t.Errorf("wrong age distribution: %v", ageCounts)
	}
}
