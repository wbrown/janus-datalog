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

// TestStringPredicateWithParameter verifies that string predicates can use input parameters
// Reproduces bug: https://github.com/wbrown/janus-datalog/docs/bugs/BUG_STRING_PREDICATES_CANT_USE_PARAMETERS.md
func TestStringPredicateWithParameter(t *testing.T) {
	// Create temp directory for database
	dir, err := os.MkdirTemp("", "string-pred-test-*")
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
	symbolEntity := datalog.NewIdentity("symbol-CRWV")
	tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "CRWV")

	// Add price bars with different times
	times := []time.Time{
		time.Date(2025, 10, 15, 9, 30, 0, 0, time.UTC),
		time.Date(2025, 10, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2025, 11, 3, 9, 30, 0, 0, time.UTC),
		time.Date(2025, 11, 3, 10, 0, 0, 0, time.UTC),
		time.Date(2025, 12, 1, 9, 30, 0, 0, time.UTC),
	}

	for i, tm := range times {
		barEntity := datalog.NewIdentity("bar-" + string(rune('A'+i)))
		tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
		tx.Add(barEntity, datalog.NewKeyword(":price/time"), tm)
		tx.Add(barEntity, datalog.NewKeyword(":price/open"), float64(100.0+float64(i)))
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	exec := db.NewExecutor()

	// Test 1: Query with constant string (baseline - should work)
	t.Run("WithConstant", func(t *testing.T) {
		queryStr := `[:find (count ?p)
		 :in $ ?symbol
		 :where
		        [?s :symbol/ticker ?symbol]
		        [?p :price/symbol ?s]
		        [?p :price/time ?time]
		        [(str ?time) ?timeStr]
		        [(str/starts-with? ?timeStr "2025-10")]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Create input relation for ?symbol
		symbolInput := executor.NewMaterializedRelation(
			[]query.Symbol{"?symbol"},
			[]executor.Tuple{{"CRWV"}},
		)

		ctx := executor.NewContext(nil)
		result, err := exec.ExecuteWithRelations(ctx, q, []executor.Relation{symbolInput})
		if err != nil {
			t.Fatalf("Query with constant failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}

		it := result.Iterator()
		if it.Next() {
			tuple := it.Tuple()
			count := tuple[0].(int64)
			if count != 2 {
				t.Errorf("Expected count of 2 (October bars), got %d", count)
			}
		}
		it.Close()
	})

	// Test 2: Query with input parameter for string (THIS FAILS IN BUG)
	t.Run("WithParameter", func(t *testing.T) {
		queryStr := `[:find (count ?p)
		 :in $ ?symbol ?month
		 :where
		        [?s :symbol/ticker ?symbol]
		        [?p :price/symbol ?s]
		        [?p :price/time ?time]
		        [(str ?time) ?timeStr]
		        [(str/starts-with? ?timeStr ?month)]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Create input relations for ?symbol and ?month
		inputs := []executor.Relation{
			executor.NewMaterializedRelation(
				[]query.Symbol{"?symbol"},
				[]executor.Tuple{{"CRWV"}},
			),
			executor.NewMaterializedRelation(
				[]query.Symbol{"?month"},
				[]executor.Tuple{{"2025-11"}},
			),
		}

		ctx := executor.NewContext(nil)
		result, err := exec.ExecuteWithRelations(ctx, q, inputs)
		if err != nil {
			t.Fatalf("Query with parameter failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}

		it := result.Iterator()
		if it.Next() {
			tuple := it.Tuple()
			count := tuple[0].(int64)
			if count != 2 {
				t.Errorf("Expected count of 2 (November bars), got %d", count)
			}
		}
		it.Close()
	})

	// Test 3: Query with parameter for different month
	t.Run("WithParameterDecember", func(t *testing.T) {
		queryStr := `[:find (count ?p)
		 :in $ ?symbol ?month
		 :where
		        [?s :symbol/ticker ?symbol]
		        [?p :price/symbol ?s]
		        [?p :price/time ?time]
		        [(str ?time) ?timeStr]
		        [(str/starts-with? ?timeStr ?month)]]`

		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Failed to parse query: %v", err)
		}

		// Create input relations for ?symbol and ?month
		inputs := []executor.Relation{
			executor.NewMaterializedRelation(
				[]query.Symbol{"?symbol"},
				[]executor.Tuple{{"CRWV"}},
			),
			executor.NewMaterializedRelation(
				[]query.Symbol{"?month"},
				[]executor.Tuple{{"2025-12"}},
			),
		}

		ctx := executor.NewContext(nil)
		result, err := exec.ExecuteWithRelations(ctx, q, inputs)
		if err != nil {
			t.Fatalf("Query with parameter failed: %v", err)
		}

		if result.Size() != 1 {
			t.Errorf("Expected 1 result, got %d", result.Size())
		}

		it := result.Iterator()
		if it.Next() {
			tuple := it.Tuple()
			count := tuple[0].(int64)
			if count != 1 {
				t.Errorf("Expected count of 1 (December bars), got %d", count)
			}
		}
		it.Close()
	})
}
