package tests

import (
	"fmt"
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

// TestConditionalAggregateRewritingEndToEnd verifies that the full rewriting pipeline works
func TestConditionalAggregateRewritingEndToEnd(t *testing.T) {
	t.Skip("Conditional aggregate rewriting is disabled due to 22.5x performance regression. " +
		"Feature is ~95% complete but disabled. See CONDITIONAL_AGGREGATE_REWRITING_STATUS.md for details.")

	// Create temporary database
	dir, err := os.MkdirTemp("", "cond-agg-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data: prices for two days
	tx := db.NewTransaction()

	sym1 := datalog.NewIdentity("sym:AAPL")
	tx.Add(sym1, datalog.NewKeyword(":symbol/ticker"), "AAPL")

	// Day 1 prices (2025-01-15)
	p1 := datalog.NewIdentity("price:1")
	tx.Add(p1, datalog.NewKeyword(":price/symbol"), sym1)
	tx.Add(p1, datalog.NewKeyword(":price/time"), time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC))
	tx.Add(p1, datalog.NewKeyword(":price/open"), float64(150.0))
	tx.Add(p1, datalog.NewKeyword(":price/high"), float64(155.0))
	tx.Add(p1, datalog.NewKeyword(":price/low"), float64(149.0))
	tx.Add(p1, datalog.NewKeyword(":price/close"), float64(154.0))

	p2 := datalog.NewIdentity("price:2")
	tx.Add(p2, datalog.NewKeyword(":price/symbol"), sym1)
	tx.Add(p2, datalog.NewKeyword(":price/time"), time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC))
	tx.Add(p2, datalog.NewKeyword(":price/open"), float64(154.5))
	tx.Add(p2, datalog.NewKeyword(":price/high"), float64(158.0))
	tx.Add(p2, datalog.NewKeyword(":price/low"), float64(153.0))
	tx.Add(p2, datalog.NewKeyword(":price/close"), float64(157.0))

	// Day 2 prices (2025-01-16)
	p3 := datalog.NewIdentity("price:3")
	tx.Add(p3, datalog.NewKeyword(":price/symbol"), sym1)
	tx.Add(p3, datalog.NewKeyword(":price/time"), time.Date(2025, 1, 16, 9, 30, 0, 0, time.UTC))
	tx.Add(p3, datalog.NewKeyword(":price/open"), float64(157.5))
	tx.Add(p3, datalog.NewKeyword(":price/high"), float64(160.0))
	tx.Add(p3, datalog.NewKeyword(":price/low"), float64(156.0))
	tx.Add(p3, datalog.NewKeyword(":price/close"), float64(159.0))

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test query that directly uses conditional aggregate semantics
	// This bypasses the subquery rewriting and directly tests the conditional aggregate execution
	// We manually create what the rewriter would produce in the Find clause
	queryStr := `[:find ?ticker ?max-high
	             :where
	             [?s :symbol/ticker ?ticker]
	             [?b :price/symbol ?s]
	             [?b :price/time ?t]
	             [(day ?t) ?pd]
	             [?b :price/high ?h]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Test 1: Regular aggregation (baseline - no filtering)
	t.Run("Regular aggregation", func(t *testing.T) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: false,
		}
		exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

		// Modify query to use regular max aggregate
		testQuery := *q
		testQuery.Find = []query.FindElement{
			query.FindVariable{Symbol: "?ticker"},
			query.FindAggregate{Function: "max", Arg: "?h"},
		}

		result, err := exec.Execute(&testQuery)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// Without conditional filtering, should get max across ALL bars
		// Expected: 160.0 (from p3 on day 16)
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
				t.Errorf("Expected max high 160.0 (all bars), got %v", maxHigh)
			}
			t.Logf("Result: ticker=%s, max-high=%.1f", ticker, maxHigh)
		}
	})

	// Test 2: Conditional aggregation (with predicate filter)
	t.Run("Conditional aggregation", func(t *testing.T) {
		opts := planner.PlannerOptions{
			EnableDynamicReordering:             true,
			EnableConditionalAggregateRewriting: false, // Not using rewriter, testing execution directly
		}
		exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)

		// Manually create conditional aggregate (what the rewriter would produce)
		testQuery := *q
		testQuery.Find = []query.FindElement{
			query.FindVariable{Symbol: "?ticker"},
			query.FindAggregate{
				Function:  "max",
				Arg:       "?h",
				Predicate: "?cond", // Filter on ?cond (day == 15)
			},
		}

		result, err := exec.Execute(&testQuery)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		// With conditional filtering on day 15, should get max from p1 and p2
		// Expected: 158.0 (p2's high on day 15)
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
			if maxHigh != 158.0 {
				t.Errorf("Expected max high 158.0 (day 15 only), got %v", maxHigh)
			}
			t.Logf("Result: ticker=%s, max-high=%.1f (filtered to day 15)", ticker, maxHigh)
		}
	})

}

// Helper function to collect all rows from a relation
func collectRows(rel executor.Relation) []executor.Tuple {
	var rows []executor.Tuple
	it := rel.Iterator()
	defer it.Close()
	for it.Next() {
		rows = append(rows, it.Tuple())
	}
	return rows
}

// Helper function to compare two tuples
func rowsEqual(a, b executor.Tuple) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !valuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// Helper function to compare two values
func valuesEqual(a, b interface{}) bool {
	// Handle time.Time specially
	if ta, ok := a.(time.Time); ok {
		if tb, ok := b.(time.Time); ok {
			return ta.Equal(tb)
		}
		return false
	}

	// Handle floats with tolerance
	if fa, ok := a.(float64); ok {
		if fb, ok := b.(float64); ok {
			return fmt.Sprintf("%.6f", fa) == fmt.Sprintf("%.6f", fb)
		}
		return false
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
