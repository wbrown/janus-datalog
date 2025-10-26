package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestGopherStreetDatabase tests whether we can read the existing gopher-street database
// This is a diagnostic test to determine if there's a compatibility issue
func TestGopherStreetDatabase(t *testing.T) {
	dbPath := "/Users/wbrown/go/src/github.com/wbrown/gopher-street/datalog-db"

	// Skip if database doesn't exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("gopher-street database not found at", dbPath)
	}

	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatalf("Failed to open gopher-street database: %v", err)
	}
	defer db.Close()

	// Use the same method as datalog-cli for exact reproduction
	opts := DefaultPlannerOptions()
	exec := db.NewExecutorWithOptions(opts)

	t.Run("UnboundAttributeQuery", func(t *testing.T) {
		// Query: [:find ?e ?a ?v :where [?e ?a ?v]]
		// This should work even if attribute-specific queries are broken
		queryStr := `[:find ?e ?a ?v :where [?e ?a ?v]]`
		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		result, err := exec.Execute(q)
		assert.NoError(t, err)

		// Count results
		it := result.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
			if count >= 10 {
				break
			}
		}

		t.Logf("Unbound query found %d+ datoms", count)
		assert.Greater(t, count, 0, "Should find some datoms with unbound attribute")
	})

	t.Run("BoundAttributeQuery", func(t *testing.T) {
		// Query: [:find ?e ?v :where [?e :price/close ?v]]
		// This fails on gopher-street database but works on fresh databases
		queryStr := `[:find ?e ?v :where [?e :price/close ?v]]`
		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		result, err := exec.Execute(q)
		assert.NoError(t, err)

		// Count results
		it := result.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
			if count >= 10 {
				break
			}
		}

		t.Logf("Bound attribute query found %d results", count)

		// This will fail if there's a compatibility issue
		// If it returns 0, we have a problem
		if count == 0 {
			t.Error("BUG: Bound attribute query returns empty on gopher-street database")
			t.Error("This suggests a database compatibility issue or a regression in attribute matching")
		}
	})

	t.Run("SymbolTickerQuery", func(t *testing.T) {
		// Query: [:find ?ticker :where [?s :symbol/ticker ?ticker]]
		// This is what the OHLC tests try to use
		queryStr := `[:find ?ticker :where [?s :symbol/ticker ?ticker]]`
		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		result, err := exec.Execute(q)
		assert.NoError(t, err)

		// Count results
		it := result.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
			if count >= 10 {
				break
			}
		}

		t.Logf("Symbol ticker query found %d results", count)

		if count == 0 {
			t.Error("BUG: Cannot find :symbol/ticker in gopher-street database")
			t.Error("This explains why all OHLC performance tests return empty")
		}
	})
}
