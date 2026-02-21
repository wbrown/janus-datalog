package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestOrClauseWithCorrelatedSubquery_E2E reproduces the bug where
// an OR clause with a correlated subquery (SubqueryPattern) fails with
// "no input groups for subquery with variable inputs"
//
// This goes through the full Database.ExecuteQuery path which uses
// the planner - the same path the user's code takes.
func TestOrClauseWithCorrelatedSubquery_E2E(t *testing.T) {
	// Create temporary database with same options as user's code
	dbPath := "/tmp/test-or-subquery-regression-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	// Insert test data
	tx := db.NewTransaction()

	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")

	// Two scenarios
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/name"), "Scenario 1"))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/name"), "Scenario 2"))

	// Tasks for scenario1 (has completed tasks)
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))
	require.NoError(t, tx.Add(task2, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task2, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))

	// No tasks for scenario2 (should fall back to 0)

	_, err = tx.Commit()
	require.NoError(t, err)

	// Query with OR clause containing correlated subquery
	// This is the exact pattern that fails in user's code
	queryStr := `[:find ?scenario ?count
	              :where [?scenario :scenario/name ?name]
	                     (or [(q [:find (count ?t)
	                              :in $ ?s
	                              :where [?t :task/scenario ?s]
	                                     [?t :task/status :status/complete]]
	                             $ ?scenario) [[?count]]]
	                         [(ground 0) ?count])]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	// Collect results
	results := make(map[string]int64)
	for _, row := range rows {
		t.Logf("Result row: %v", row)
		// scenario is an Identity, count is int64
		scenarioID := row[0].(datalog.Identity)
		count := row[1].(int64)
		results[scenarioID.String()] = count
	}

	t.Logf("Results map: %v", results)

	// Verify results
	assert.Len(t, results, 2, "Should have 2 scenarios")
	assert.Equal(t, int64(2), results["scenario:1"], "Scenario 1 should have 2 completed tasks")
	assert.Equal(t, int64(0), results["scenario:2"], "Scenario 2 should fall back to 0")
}

// TestScenarioSummaryQuery_E2E tests the exact query from the user's production code.
// This query uses OR with correlated subqueries, get-else, and aggregations.
func TestScenarioSummaryQuery_E2E(t *testing.T) {
	dbPath := "/tmp/test-scenario-summary-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	// Insert minimal test data
	tx := db.NewTransaction()

	scenario1 := datalog.NewIdentity("scenario:1")
	task1 := datalog.NewIdentity("task:1")

	// Scenario with required attributes
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/created-at"), int64(1000)))

	// A completed task
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/token-count"), int64(100)))

	_, err = tx.Commit()
	require.NoError(t, err)

	// Simplified version of the production query - just the failing OR pattern
	queryStr := `[:find ?scenario ?taskCount
	              :where
	              [?scenario :scenario/id ?id]
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?scenario) [[?taskCount]]]
	                  [(ground 0) ?taskCount])]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}

	require.Len(t, rows, 1, "Should have 1 scenario")
}

// scenarioSummaryQueryFull is the EXACT production query from user's code
const scenarioSummaryQueryFull = `
[:find ?scenario ?id ?title ?createdAt ?intensity ?pov ?genre ?element ?setting
       ?taskCount ?totalTokens ?totalDuration ?cacheCreation ?cacheRead
       ?complete ?lastKey
 :where
 [?scenario :scenario/id ?id]
 [(get-else $ ?scenario :scenario/title "") ?title]
 [?scenario :scenario/created-at ?createdAt]
 [(get-else $ ?scenario :idea/intensity "") ?intensity]
 [(get-else $ ?scenario :scenario/pov "") ?pov]
 [(get-else $ ?scenario :idea/genre "") ?genre]
 [(get-else $ ?scenario :idea/element "") ?element]
 [(get-else $ ?scenario :idea/setting "") ?setting]

 ;; Task stats with OR fallback for scenarios with no completed tasks
 (or [(q [:find (count ?t) (sum ?tok) (sum ?dur) (sum ?cc) (sum ?cr)
          :in $ ?s
          :where [?t :task/scenario ?s]
                 [?t :task/status :status/complete]
                 [(get-else $ ?t :task/token-count 0) ?tok]
                 [(get-else $ ?t :task/duration 0) ?dur]
                 [(get-else $ ?t :task/cache-creation-tokens 0) ?cc]
                 [(get-else $ ?t :task/cache-read-tokens 0) ?cr]]
        $ ?scenario) [[?taskCount ?totalTokens ?totalDuration ?cacheCreation ?cacheRead]]]
     (and [(ground 0) ?taskCount]
          [(ground 0) ?totalTokens]
          [(ground 0) ?totalDuration]
          [(ground 0) ?cacheCreation]
          [(ground 0) ?cacheRead]))

 ;; Opening count: count completed opening tasks
 (or [(q [:find (count ?t)
          :in $ ?s
          :where [?t :task/scenario ?s]
                 [?t :task/key :scenario/opening]
                 [?t :task/status :status/complete]]
        $ ?scenario) [[?openingCount]]]
     [(ground 0) ?openingCount])

 ;; Complete = opening count > 0 (uses comparison binding)
 [(> ?openingCount 0) ?complete]

 ;; Last task key: find the task with max completed-at timestamp
 (or [(q [:find ?key
          :in $ ?s
          :where [?t :task/scenario ?s]
                 [?t :task/status :status/complete]
                 [?t :task/completed-at ?ca]
                 [?t :task/key ?key]
                 [(q [:find (max ?ca2)
                      :in $ ?s2
                      :where [?t2 :task/scenario ?s2]
                             [?t2 :task/status :status/complete]
                             [?t2 :task/completed-at ?ca2]]
                    $ ?s) [[?maxCa]]]
                 [(= ?ca ?maxCa)]]
        $ ?scenario) [[?lastKey]]]
     [(ground :none) ?lastKey])]`

// TestNestedSubqueryInOr_E2E tests OR with NESTED SUBQUERY (subquery inside subquery)
// This is the unique pattern in the production query's third OR clause
func TestNestedSubqueryInOr_E2E(t *testing.T) {
	dbPath := "/tmp/test-nested-subq-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	scenario1 := datalog.NewIdentity("scenario:1")
	task1 := datalog.NewIdentity("task:1")
	task2 := datalog.NewIdentity("task:2")

	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/key"), datalog.NewKeyword(":task/first")))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/completed-at"), int64(100)))
	require.NoError(t, tx.Add(task2, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task2, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))
	require.NoError(t, tx.Add(task2, datalog.NewKeyword(":task/key"), datalog.NewKeyword(":task/second")))
	require.NoError(t, tx.Add(task2, datalog.NewKeyword(":task/completed-at"), int64(200))) // Later = max

	_, err = tx.Commit()
	require.NoError(t, err)

	// NESTED SUBQUERY pattern from production query
	queryStr := `[:find ?scenario ?lastKey
	              :where
	              [?scenario :scenario/id ?id]

	              (or [(q [:find ?key
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]
	                              [?t :task/completed-at ?ca]
	                              [?t :task/key ?key]
	                              [(q [:find (max ?ca2)
	                                   :in $ ?s2
	                                   :where [?t2 :task/scenario ?s2]
	                                          [?t2 :task/status :status/complete]
	                                          [?t2 :task/completed-at ?ca2]]
	                                 $ ?s) [[?maxCa]]]
	                              [(= ?ca ?maxCa)]]
	                      $ ?scenario) [[?lastKey]]]
	                  [(ground :none) ?lastKey])]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}
	require.Len(t, rows, 1)
}

// TestProductionQueryStructure_E2E incrementally builds toward the production query
func TestProductionQueryStructure_E2E(t *testing.T) {
	dbPath := "/tmp/test-prod-structure-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/created-at"), int64(1000)))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/id"), "test-2"))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/created-at"), int64(2000)))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))

	_, err = tx.Commit()
	require.NoError(t, err)

	// Structure matching production query:
	// - Multiple get-else calls
	// - First OR with correlated subquery
	// - Second OR with correlated subquery
	// - Comparison binding
	queryStr := `[:find ?scenario ?id ?title ?createdAt ?taskCount ?openingCount ?complete
	              :where
	              [?scenario :scenario/id ?id]
	              [(get-else $ ?scenario :scenario/title "") ?title]
	              [?scenario :scenario/created-at ?createdAt]
	              [(get-else $ ?scenario :idea/intensity "") ?intensity]
	              [(get-else $ ?scenario :scenario/pov "") ?pov]

	              ;; First OR - task count
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?scenario) [[?taskCount]]]
	                  [(ground 0) ?taskCount])

	              ;; Second OR - opening count
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/key :scenario/opening]
	                              [?t :task/status :status/complete]]
	                      $ ?scenario) [[?openingCount]]]
	                  [(ground 0) ?openingCount])

	              ;; Comparison binding
	              [(> ?openingCount 0) ?complete]]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}
	require.Len(t, rows, 2)
}

// TestGetElseBeforeOrClause_E2E tests get-else BEFORE the OR clause
func TestGetElseBeforeOrClause_E2E(t *testing.T) {
	dbPath := "/tmp/test-getelse-before-or-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/title"), "Test 1"))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/id"), "test-2"))
	// scenario2 has no title - will use get-else default
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))

	_, err = tx.Commit()
	require.NoError(t, err)

	// get-else BEFORE the OR clause (like production query)
	queryStr := `[:find ?scenario ?title ?taskCount
	              :where
	              [?scenario :scenario/id ?id]
	              [(get-else $ ?scenario :scenario/title "") ?title]

	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?scenario) [[?taskCount]]]
	                  [(ground 0) ?taskCount])]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}
	require.Len(t, rows, 2)
}

// TestMultipleSequentialOrClauses_E2E tests MULTIPLE sequential OR clauses
func TestMultipleSequentialOrClauses_E2E(t *testing.T) {
	dbPath := "/tmp/test-multi-or-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/id"), "test-2"))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))

	_, err = tx.Commit()
	require.NoError(t, err)

	// MULTIPLE sequential OR clauses like the production query
	queryStr := `[:find ?scenario ?count1 ?count2
	              :where
	              [?scenario :scenario/id ?id]

	              ;; First OR clause
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?scenario) [[?count1]]]
	                  [(ground 0) ?count1])

	              ;; Second OR clause (same pattern)
	              (or [(q [:find (count ?t)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]]
	                      $ ?scenario) [[?count2]]]
	                  [(ground 0) ?count2])]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}
	require.Len(t, rows, 2)
}

// TestOrWithGetElseInsideSubquery_E2E tests OR with get-else inside the subquery
func TestOrWithGetElseInsideSubquery_E2E(t *testing.T) {
	dbPath := "/tmp/test-or-getelse-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/id"), "test-2"))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))

	_, err = tx.Commit()
	require.NoError(t, err)

	// OR with get-else INSIDE the subquery (like production query)
	queryStr := `[:find ?scenario ?taskCount ?totalTokens
	              :where
	              [?scenario :scenario/id ?id]
	              (or [(q [:find (count ?t) (sum ?tok)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]
	                              [(get-else $ ?t :task/token-count 0) ?tok]]
	                      $ ?scenario) [[?taskCount ?totalTokens]]]
	                  (and [(ground 0) ?taskCount]
	                       [(ground 0) ?totalTokens]))]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}
	require.Len(t, rows, 2)
}

// TestOrWithMultipleAggregations_E2E tests OR with multiple aggregations in subquery
func TestOrWithMultipleAggregations_E2E(t *testing.T) {
	dbPath := "/tmp/test-or-multi-agg-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	tx := db.NewTransaction()
	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/id"), "test-2"))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/token-count"), int64(100)))

	_, err = tx.Commit()
	require.NoError(t, err)

	// OR with MULTIPLE aggregations in subquery (like the production query)
	queryStr := `[:find ?scenario ?taskCount ?totalTokens
	              :where
	              [?scenario :scenario/id ?id]
	              (or [(q [:find (count ?t) (sum ?tok)
	                       :in $ ?s
	                       :where [?t :task/scenario ?s]
	                              [?t :task/status :status/complete]
	                              [?t :task/token-count ?tok]]
	                      $ ?scenario) [[?taskCount ?totalTokens]]]
	                  (and [(ground 0) ?taskCount]
	                       [(ground 0) ?totalTokens]))]`

	rows, err := db.ExecuteQuery(queryStr)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}
	require.Len(t, rows, 2)
}

// TestFullScenarioSummaryQuery_E2E tests the EXACT production query
func TestFullScenarioSummaryQuery_E2E(t *testing.T) {
	dbPath := "/tmp/test-full-scenario-summary-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabaseWithOptions(DatabaseOptions{
		Path:      dbPath,
	})
	require.NoError(t, err)
	defer db.Close()

	// Insert test data
	tx := db.NewTransaction()

	scenario1 := datalog.NewIdentity("scenario:1")
	scenario2 := datalog.NewIdentity("scenario:2")
	task1 := datalog.NewIdentity("task:1")

	// Scenario 1 with required attributes
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/id"), "test-1"))
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/created-at"), int64(1000)))
	require.NoError(t, tx.Add(scenario1, datalog.NewKeyword(":scenario/title"), "Test Scenario"))

	// Scenario 2 - no tasks (tests fallback)
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/id"), "test-2"))
	require.NoError(t, tx.Add(scenario2, datalog.NewKeyword(":scenario/created-at"), int64(2000)))

	// A completed task for scenario 1
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/scenario"), scenario1))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/status"), datalog.NewKeyword(":status/complete")))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/token-count"), int64(100)))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/key"), datalog.NewKeyword(":scenario/idea")))
	require.NoError(t, tx.Add(task1, datalog.NewKeyword(":task/completed-at"), int64(1500)))

	_, err = tx.Commit()
	require.NoError(t, err)

	rows, err := db.ExecuteQuery(scenarioSummaryQueryFull)
	if err != nil {
		t.Fatalf("Query execution failed: %v", err)
	}

	t.Logf("Got %d results", len(rows))
	for _, row := range rows {
		t.Logf("Row: %v", row)
	}

	require.Len(t, rows, 2, "Should have 2 scenarios")
}
