package tests

import (
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestParameterizedQueryCartesianProduct reproduces the bug where parameterized
// queries fail with "Cartesian product not supported" but fmt.Sprintf works fine.
func TestParameterizedQueryCartesianProduct(t *testing.T) {
	// Create temp directory for database
	dir, err := os.MkdirTemp("", "param-cart-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create database
	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data
	tx := db.NewTransaction()

	// Add symbol entity
	symbolEntity := datalog.NewIdentity("symbol-AAPL")
	tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "AAPL")

	// Add price bars with specific minute-of-day
	for i := 0; i < 5; i++ {
		barEntity := datalog.NewIdentity("bar-" + string(rune(i)))
		tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
		tx.Add(barEntity, datalog.NewKeyword(":price/time"), time.Date(2025, 10, 13, 9, 30+i*5, 0, 0, time.UTC))
		tx.Add(barEntity, datalog.NewKeyword(":price/minute-of-day"), int64(570+i*5)) // 570 = 9:30 AM
	}

	// Add a bar at market close (960 = 4:00 PM)
	closeBarEntity := datalog.NewIdentity("bar-close")
	closeTime := time.Date(2025, 10, 13, 16, 0, 0, 0, time.UTC)
	tx.Add(closeBarEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
	tx.Add(closeBarEntity, datalog.NewKeyword(":price/time"), closeTime)
	tx.Add(closeBarEntity, datalog.NewKeyword(":price/minute-of-day"), int64(960))

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	exec := db.NewExecutor()

	// Test 1: Query with fmt.Sprintf (THIS WORKS)
	t.Run("WithSprintf", func(t *testing.T) {
		queryStr := `[:find (max ?time)
		 :where
		        [?s :symbol/ticker "AAPL"]
		        [?bar :price/symbol ?s]
		        [?bar :price/time ?time]
		        [?bar :price/minute-of-day 960]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		result, err := exec.Execute(q)
		if err != nil {
			t.Fatalf("Query with constant failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}

		it := result.Iterator()
		if it.Next() {
			tuple := it.Tuple()
			if len(tuple) != 1 {
				t.Errorf("Expected tuple with 1 element, got %d", len(tuple))
			}
			maxTime, ok := tuple[0].(time.Time)
			if !ok {
				t.Errorf("Expected time.Time, got %T", tuple[0])
			}
			if !maxTime.Equal(closeTime) {
				t.Errorf("Expected max time %v, got %v", closeTime, maxTime)
			}
		}
		it.Close()
	})

	// Test 2: Query with :in parameter (THIS FAILS WITH BUG)
	t.Run("WithParameter", func(t *testing.T) {
		queryStr := `[:find (max ?time)
		 :in $ ?symbol
		 :where
		        [?s :symbol/ticker ?symbol]
		        [?bar :price/symbol ?s]
		        [?bar :price/time ?time]
		        [?bar :price/minute-of-day 960]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Debug: Check what plan is created
		plan, err := exec.GetPlanner().PlanQuery(q)
		if err != nil {
			t.Fatalf("Failed to create plan: %v", err)
		}

		t.Logf("Plan created %d phases:", len(plan.Phases))
		for i, phase := range plan.Phases {
			t.Logf("  Phase %d:", i)
			t.Logf("    Available: %v", phase.Available)
			t.Logf("    Provides: %v", phase.Provides)
			t.Logf("    Keep: %v", phase.Keep)

			// Count and log data patterns in the phase query
			var patternCount int
			for _, clause := range phase.Query.Where {
				if _, ok := clause.(*query.DataPattern); ok {
					patternCount++
				}
			}
			t.Logf("    Patterns: %d", patternCount)

			// Log each pattern
			patIdx := 0
			for _, clause := range phase.Query.Where {
				if pat, ok := clause.(*query.DataPattern); ok {
					t.Logf("      Pattern %d: %v", patIdx, pat)
					patIdx++
				}
			}
		}

		// Create input relation for ?symbol
		symbolInput := executor.NewMaterializedRelation(
			[]query.Symbol{"?symbol"},
			[]executor.Tuple{{"AAPL"}},
		)

		ctx := executor.NewContext(nil)
		result, err := exec.ExecuteWithRelations(ctx, q, []executor.Relation{symbolInput})
		if err != nil {
			t.Fatalf("Query with parameter failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}

		it := result.Iterator()
		if it.Next() {
			tuple := it.Tuple()
			if len(tuple) != 1 {
				t.Errorf("Expected tuple with 1 element, got %d", len(tuple))
			}
			maxTime, ok := tuple[0].(time.Time)
			if !ok {
				t.Errorf("Expected time.Time, got %T", tuple[0])
			}
			if !maxTime.Equal(closeTime) {
				t.Errorf("Expected max time %v, got %v", closeTime, maxTime)
			}
		}
		it.Close()
	})
}
