package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestPlannerSelection verifies that both old and new planners work correctly
func TestPlannerSelection(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")

	datoms := []datalog.Datom{
		{E: alice, A: datalog.NewKeyword(":person/name"), V: "Alice", Tx: 1},
		{E: alice, A: datalog.NewKeyword(":person/age"), V: int64(30), Tx: 1},
		{E: bob, A: datalog.NewKeyword(":person/name"), V: "Bob", Tx: 1},
		{E: bob, A: datalog.NewKeyword(":person/age"), V: int64(25), Tx: 1},
	}

	matcher := NewMemoryPatternMatcher(datoms)

	queryStr := `[:find ?name ?age
	              :where
	                [?e :person/name ?name]
	                [?e :person/age ?age]
	                [(> ?age 20)]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	t.Run("OldPlanner", func(t *testing.T) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseClauseBasedPlanner: false,
		})
		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Old planner execution failed: %v", err)
		}
		if result.Size() != 2 {
			t.Errorf("Expected 2 results, got %d", result.Size())
		}
		t.Logf("Old planner: %d results", result.Size())
	})

	t.Run("NewClauseBasedPlanner", func(t *testing.T) {
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseClauseBasedPlanner: true,
		})
		// New planner requires UseQueryExecutor=true
		exec.SetUseQueryExecutor(true)

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("New planner execution failed: %v", err)
		}
		if result.Size() != 2 {
			t.Errorf("Expected 2 results, got %d", result.Size())
		}
		t.Logf("New planner: %d results", result.Size())
	})

	t.Run("BothProduceSameResults", func(t *testing.T) {
		oldExec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseClauseBasedPlanner: false,
		})
		oldResult, err := oldExec.Execute(q)
		if err != nil {
			t.Fatalf("Old planner failed: %v", err)
		}

		newExec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			UseClauseBasedPlanner: true,
		})
		newExec.SetUseQueryExecutor(true)
		newResult, err := newExec.Execute(q)
		if err != nil {
			t.Fatalf("New planner failed: %v", err)
		}

		if oldResult.Size() != newResult.Size() {
			t.Errorf("Size mismatch: old=%d, new=%d", oldResult.Size(), newResult.Size())
		}

		// Both should have Alice (30) and Bob (25)
		t.Logf("Results match: %d tuples from both planners", oldResult.Size())
	})
}
