package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestSubqueryBindingNotInFindBug reproduces a bug where a subquery binding variable
// used in a subsequent where clause for filtering, but NOT included in :find, causes
// the filter to be skipped entirely.
//
// Bug: When ?maxTx is not in :find, the planner appears to optimize away or skip
// the final pattern that uses ?maxTx for filtering.
//
// Workaround: Include the subquery binding variable in :find even if not needed.
func TestSubqueryBindingNotInFindBug(t *testing.T) {
	dbPath := "/tmp/test-subquery-binding-not-in-find-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Create test entities
	task1 := datalog.NewIdentity("task1")
	scenario1 := datalog.NewIdentity("scenario1")
	char1 := datalog.NewIdentity("char1")
	completeStatus := datalog.NewKeyword(":status/complete")
	invalidatedStatus := datalog.NewKeyword(":status/invalidated")

	// Transaction 1: Create task with status=:status/complete
	tx1 := db.NewTransaction()
	assert.NoError(t, tx1.Add(scenario1, datalog.NewKeyword(":scenario/name"), "Test Scenario"))
	assert.NoError(t, tx1.Add(char1, datalog.NewKeyword(":character/name"), "Alice"))
	assert.NoError(t, tx1.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	assert.NoError(t, tx1.Add(task1, datalog.NewKeyword(":task/type"), "combat"))
	assert.NoError(t, tx1.Add(task1, datalog.NewKeyword(":task/character"), char1))
	assert.NoError(t, tx1.Add(task1, datalog.NewKeyword(":task/status"), completeStatus))
	_, err = tx1.Commit()
	assert.NoError(t, err)

	// Transaction 2: Update task status to :status/invalidated
	// This creates a second datom for :task/status with a higher tx
	tx2 := db.NewTransaction()
	assert.NoError(t, tx2.Add(task1, datalog.NewKeyword(":task/status"), invalidatedStatus))
	_, err = tx2.Commit()
	assert.NoError(t, err)

	t.Run("BrokenQuery_BindingNotInFind", func(t *testing.T) {
		// This query does NOT include ?maxTx in :find
		// Expected behavior: Should return 0 results because the max tx has status=invalidated
		// Actual behavior (BUG): Returns the task because the filter is skipped
		brokenQuery := `[:find ?charName ?type
		 :in $ ?scenario ?completeStatus
		 :where
		 [?task :task/scenario ?scenario]
		 [?task :task/type ?type]
		 [?task :task/character ?char]
		 [?char :character/name ?charName]
		 [(q [:find (max ?tx) :in $ ?t :where [?t :task/status _ ?tx]] $ ?task) [[?maxTx]]]
		 [?task :task/status ?completeStatus ?maxTx]]`

		q, err := parser.ParseQuery(brokenQuery)
		assert.NoError(t, err)

		// Debug: Show the query plan
		p := planner.NewPlanner(nil, DefaultPlannerOptions())
		plan, err := p.Plan(q)
		assert.NoError(t, err)
		t.Logf("Query Plan for BROKEN query:\n%s", plan.String())

		inputRels, err := db.convertInputsToRelations(q, []interface{}{scenario1, completeStatus})
		assert.NoError(t, err)

		exec := db.NewExecutor()
		result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRels)
		assert.NoError(t, err)

		results := collectResults(result)

		// BUG: This should be 0 results, but due to the bug it returns 1
		// When this test fails with "Expected 0 but got 1", the bug is present
		// When this test passes, the bug is fixed
		if len(results) != 0 {
			t.Logf("BUG CONFIRMED: Query returned %d results when it should return 0", len(results))
			t.Logf("The subquery binding ?maxTx is not in :find, causing the filter to be skipped")
			for i, row := range results {
				t.Logf("  Result %d: %v", i, row)
			}
		}
		assert.Equal(t, 0, len(results), "Should return 0 results - max tx has status=invalidated, not complete")
	})

	t.Run("WorkingQuery_BindingInFind", func(t *testing.T) {
		// This query INCLUDES ?maxTx in :find (workaround)
		// This should correctly return 0 results
		workingQuery := `[:find ?charName ?type ?maxTx
		 :in $ ?scenario ?completeStatus
		 :where
		 [?task :task/scenario ?scenario]
		 [?task :task/type ?type]
		 [?task :task/character ?char]
		 [?char :character/name ?charName]
		 [(q [:find (max ?tx) :in $ ?t :where [?t :task/status _ ?tx]] $ ?task) [[?maxTx]]]
		 [?task :task/status ?completeStatus ?maxTx]]`

		q, err := parser.ParseQuery(workingQuery)
		assert.NoError(t, err)

		// Debug: Show the query plan
		p := planner.NewPlanner(nil, DefaultPlannerOptions())
		plan, err := p.Plan(q)
		assert.NoError(t, err)
		t.Logf("Query Plan for WORKING query:\n%s", plan.String())

		inputRels, err := db.convertInputsToRelations(q, []interface{}{scenario1, completeStatus})
		assert.NoError(t, err)

		exec := db.NewExecutor()
		result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRels)
		assert.NoError(t, err)

		results := collectResults(result)

		// With ?maxTx in :find, this correctly returns 0 results
		assert.Equal(t, 0, len(results), "Should return 0 results - max tx has status=invalidated, not complete")
	})

	t.Run("VerifyDataSetup_LatestStatusIsInvalidated", func(t *testing.T) {
		// Verify our test data is set up correctly:
		// The task should have TWO status values, and the max tx should be for invalidated
		verifyQuery := `[:find ?status ?tx
		 :in $ ?task
		 :where [?task :task/status ?status ?tx]]`

		q, err := parser.ParseQuery(verifyQuery)
		assert.NoError(t, err)

		inputRels, err := db.convertInputsToRelations(q, []interface{}{task1})
		assert.NoError(t, err)

		exec := db.NewExecutor()
		result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRels)
		assert.NoError(t, err)

		results := collectResults(result)

		t.Logf("Task status history:")
		for _, row := range results {
			t.Logf("  status=%v, tx=%v", row[0], row[1])
		}

		assert.Equal(t, 2, len(results), "Task should have 2 status values (complete and invalidated)")
	})

	t.Run("VerifySubqueryReturnsMaxTx", func(t *testing.T) {
		// Verify the subquery correctly returns the max tx
		subqueryTest := `[:find ?maxTx
		 :in $ ?task
		 :where [(q [:find (max ?tx) :in $ ?t :where [?t :task/status _ ?tx]] $ ?task) [[?maxTx]]]]`

		q, err := parser.ParseQuery(subqueryTest)
		assert.NoError(t, err)

		inputRels, err := db.convertInputsToRelations(q, []interface{}{task1})
		assert.NoError(t, err)

		exec := db.NewExecutor()
		result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRels)
		assert.NoError(t, err)

		results := collectResults(result)

		assert.Equal(t, 1, len(results), "Should return 1 result with max tx")
		t.Logf("Max tx from subquery: %v", results[0][0])
	})
}

// collectResults converts a Relation to [][]interface{}
func collectResults(result executor.Relation) [][]interface{} {
	results := make([][]interface{}, 0)
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		tuple := it.Tuple()
		// Make a copy to avoid slice aliasing issues
		row := make([]interface{}, len(tuple))
		copy(row, tuple)
		results = append(results, row)
	}
	return results
}
