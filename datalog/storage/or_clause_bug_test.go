//go:build !(js && wasm)

package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestOrClauseBug demonstrates a bug where the `or` clause does not filter
// results as expected.
//
// Expected behavior: (or [?t :task/type :type/a] [?t :task/type :type/b])
// should only return tuples where ?t has type :type/a OR :type/b.
//
// Actual behavior: The `or` clause appears to be ignored entirely, returning
// all tuples regardless of whether they match either branch.
func TestOrClauseBug(t *testing.T) {
	dbPath := "/tmp/test-or-clause-bug-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Create three tasks with different types
	task1 := datalog.NewIdentity("task-1")
	task2 := datalog.NewIdentity("task-2")
	task3 := datalog.NewIdentity("task-3")

	typeA := datalog.NewKeyword(":type/a")
	typeB := datalog.NewKeyword(":type/b")
	typeC := datalog.NewKeyword(":type/c")
	statusComplete := datalog.NewKeyword(":status/complete")
	taskType := datalog.NewKeyword(":task/type")
	taskStatus := datalog.NewKeyword(":task/status")

	// Assert task data
	tx := db.NewTransaction()
	assert.NoError(t, tx.Add(task1, taskType, typeA))
	assert.NoError(t, tx.Add(task1, taskStatus, statusComplete))
	assert.NoError(t, tx.Add(task2, taskType, typeB))
	assert.NoError(t, tx.Add(task2, taskStatus, statusComplete))
	assert.NoError(t, tx.Add(task3, taskType, typeC))
	assert.NoError(t, tx.Add(task3, taskStatus, statusComplete))
	_, err = tx.Commit()
	assert.NoError(t, err)

	// Query WITHOUT or clause - filter to type :type/a only
	// Expected: 1 result (task1)
	queryWithoutOr := `[:find ?t ?type
	 :where
	 [?t :task/status :status/complete]
	 [?t :task/type ?type]
	 [?t :task/type :type/a]]`

	results, err := executor.CollectTuples(db.Query(queryWithoutOr))
	assert.NoError(t, err)
	t.Logf("Without or (expect 1 result with :type/a): %d results", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}
	assert.Len(t, results, 1, "Without or: should return only task with :type/a")

	// Query WITH or clause - filter to type :type/a OR :type/b
	// Expected: 2 results (task1 and task2)
	// Actual: 3 results (all tasks, including task3 with :type/c)
	queryWithOr := `[:find ?t ?type
	 :where
	 [?t :task/status :status/complete]
	 [?t :task/type ?type]
	 (or [?t :task/type :type/a]
	     [?t :task/type :type/b])]`

	results, err = executor.CollectTuples(db.Query(queryWithOr))
	assert.NoError(t, err)
	t.Logf("With or (expect 2 results with :type/a or :type/b): %d results", len(results))
	for _, r := range results {
		t.Logf("  %v", r)
	}

	if len(results) != 2 {
		t.Errorf("BUG: Expected 2 results (types a and b only), got %d", len(results))
		t.Error("The `or` clause is not filtering - :type/c should be excluded")
	}
}

// TestOrClauseBugTraced runs the same test with annotation tracing to understand the execution
func TestOrClauseBugTraced(t *testing.T) {
	dbPath := "/tmp/test-or-clause-bug-traced-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Create three tasks with different types
	task1 := datalog.NewIdentity("task-1")
	task2 := datalog.NewIdentity("task-2")
	task3 := datalog.NewIdentity("task-3")

	typeA := datalog.NewKeyword(":type/a")
	typeB := datalog.NewKeyword(":type/b")
	typeC := datalog.NewKeyword(":type/c")
	statusComplete := datalog.NewKeyword(":status/complete")
	taskType := datalog.NewKeyword(":task/type")
	taskStatus := datalog.NewKeyword(":task/status")

	tx := db.NewTransaction()
	assert.NoError(t, tx.Add(task1, taskType, typeA))
	assert.NoError(t, tx.Add(task1, taskStatus, statusComplete))
	assert.NoError(t, tx.Add(task2, taskType, typeB))
	assert.NoError(t, tx.Add(task2, taskStatus, statusComplete))
	assert.NoError(t, tx.Add(task3, taskType, typeC))
	assert.NoError(t, tx.Add(task3, taskStatus, statusComplete))
	_, err = tx.Commit()
	assert.NoError(t, err)

	var events []annotations.Event
	handler := func(event annotations.Event) {
		events = append(events, event)
	}

	// Create executor with tracing - use same options as Database defaults
	baseMatcher := NewBadgerMatcher(db.Store())
	wrappedMatcher := executor.WrapMatcher(baseMatcher, handler)
	opts := DefaultPlannerOptions()
	exec := executor.NewExecutorWithOptions(wrappedMatcher, db, opts)

	// First, test what the OR branches return individually
	t.Log("\n=== Testing OR branch 1: [?t :task/type :type/a] ===")
	q1 := `[:find ?t :where [?t :task/type :type/a]]`
	parsed1, _ := parser.ParseQuery(q1)
	result1, err := exec.Execute(parsed1)
	assert.NoError(t, err)
	t.Logf("Branch 1 returned %d tuples:", result1.Size())
	it1 := result1.Iterator()
	for it1.Next() {
		t.Logf("  %v", it1.Tuple())
	}
	it1.Close()

	t.Log("\n=== Testing OR branch 2: [?t :task/type :type/b] ===")
	q2 := `[:find ?t :where [?t :task/type :type/b]]`
	parsed2, _ := parser.ParseQuery(q2)
	result2, err := exec.Execute(parsed2)
	assert.NoError(t, err)
	t.Logf("Branch 2 returned %d tuples:", result2.Size())
	it2 := result2.Iterator()
	for it2.Next() {
		t.Logf("  %v", it2.Tuple())
	}
	it2.Close()

	t.Log("\n=== Testing simple OR (no prior patterns) ===")
	q3 := `[:find ?t :where (or [?t :task/type :type/a] [?t :task/type :type/b])]`
	parsed3, _ := parser.ParseQuery(q3)
	result3, err := exec.Execute(parsed3)
	if err != nil {
		t.Logf("Simple OR query failed (expected - OR alone doesn't bind symbols): %v", err)
	} else {
		t.Logf("Simple OR returned %d tuples:", result3.Size())
		it3 := result3.Iterator()
		for it3.Next() {
			t.Logf("  %v", it3.Tuple())
		}
		it3.Close()
	}

	t.Log("\n=== Testing full query with OR ===")
	queryWithOr := `[:find ?t ?type
	 :where
	 [?t :task/status :status/complete]
	 [?t :task/type ?type]
	 (or [?t :task/type :type/a]
	     [?t :task/type :type/b])]`

	parsedOr, _ := parser.ParseQuery(queryWithOr)
	t.Logf("Parsed query has %d where clauses:", len(parsedOr.Where))
	for i, clause := range parsedOr.Where {
		t.Logf("  [%d] %T: %s", i, clause, clause)
	}
	resultOr, err := exec.Execute(parsedOr)
	assert.NoError(t, err)
	t.Logf("Full OR query returned %d tuples:", resultOr.Size())
	itOr := resultOr.Iterator()
	for itOr.Next() {
		t.Logf("  %v", itOr.Tuple())
	}
	itOr.Close()

	assert.Equal(t, 2, resultOr.Size(), "Full OR query should return 2 tuples")
	assert.NotEmpty(t, events, "structured annotation handler should receive execution events")
}
