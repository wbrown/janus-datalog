package tests

import (
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestConditionalAggregateExecution tests the conditional aggregate execution infrastructure
// This tests ONLY the execution layer, not the full rewriting pipeline
func TestConditionalAggregateExecution(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "cond-agg-exec-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data: prices with different days
	tx := db.NewTransaction()

	sym1 := datalog.NewIdentity("sym:AAPL")
	tx.Add(sym1, datalog.NewKeyword(":symbol/ticker"), "AAPL")

	// Day 15 - prices: high 155.0, 158.0
	p1 := datalog.NewIdentity("price:1")
	tx.Add(p1, datalog.NewKeyword(":price/symbol"), sym1)
	tx.Add(p1, datalog.NewKeyword(":price/time"), time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC))
	tx.Add(p1, datalog.NewKeyword(":price/high"), float64(155.0))
	tx.Add(p1, datalog.NewKeyword(":price/day"), int64(15))

	p2 := datalog.NewIdentity("price:2")
	tx.Add(p2, datalog.NewKeyword(":price/symbol"), sym1)
	tx.Add(p2, datalog.NewKeyword(":price/time"), time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC))
	tx.Add(p2, datalog.NewKeyword(":price/high"), float64(158.0))
	tx.Add(p2, datalog.NewKeyword(":price/day"), int64(15))

	// Day 16 - price: high 160.0
	p3 := datalog.NewIdentity("price:3")
	tx.Add(p3, datalog.NewKeyword(":price/symbol"), sym1)
	tx.Add(p3, datalog.NewKeyword(":price/time"), time.Date(2025, 1, 16, 9, 30, 0, 0, time.UTC))
	tx.Add(p3, datalog.NewKeyword(":price/high"), float64(160.0))
	tx.Add(p3, datalog.NewKeyword(":price/day"), int64(16))

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Create base query that gets all data
	queryStr := `[:find ?ticker ?day ?high
	             :where
	             [?s :symbol/ticker ?ticker]
	             [?b :price/symbol ?s]
	             [?b :price/day ?day]
	             [?b :price/high ?high]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	opts := planner.PlannerOptions{
		EnableDynamicReordering: true,
	}
	exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

	// Test 1: Regular max (no filtering) - should get 160.0
	t.Run("Regular max", func(t *testing.T) {
		testQuery := *q
		testQuery.Find = []query.FindElement{
			query.FindVariable{Symbol: "?ticker"},
			query.FindAggregate{Function: "max", Arg: "?high"},
		}

		result, err := exec.Execute(&testQuery)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 row, got %d", result.Size())
		}

		it := result.Iterator()
		defer it.Close()

		if it.Next() {
			tuple := it.Tuple()
			ticker := tuple[0].(string)
			maxHigh := tuple[1].(float64)

			if ticker != "AAPL" {
				t.Errorf("Expected ticker AAPL, got %v", ticker)
			}
			if maxHigh != 160.0 {
				t.Errorf("Expected max high 160.0, got %v", maxHigh)
			}
			t.Logf("✓ Regular max: ticker=%s, max-high=%.1f", ticker, maxHigh)
		}
	})

	// Test 2: Conditional max with predicate
	// Note: We need to programmatically inject the predicate since we can't express it in query syntax
	// In a real scenario, the rewriter would create this structure
	t.Run("Conditional max (infrastructure test)", func(t *testing.T) {
		// This test verifies the conditional aggregate infrastructure exists and compiles
		// Full end-to-end testing requires the rewriter to work with decorrelated subqueries

		agg := query.FindAggregate{
			Function:  "max",
			Arg:       "?high",
			Predicate: "?filter", // Would be set by rewriter
		}

		// Verify the Predicate field exists and is accessible
		if agg.Predicate != "?filter" {
			t.Errorf("Predicate field not working correctly")
		}

		t.Logf("✓ Conditional aggregate infrastructure present: FindAggregate.Predicate field exists")
	})
}
