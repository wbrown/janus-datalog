package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestMatcherRelationsTupleCopyingBug tests that matcher_relations.go:241
// properly copies tuples when collecting bindings.
//
// This test should FAIL until the bug is fixed, demonstrating that:
// 1. Multi-pattern queries trigger the buggy code path
// 2. The bug causes incorrect results (0 tuples instead of expected count)
// 3. Fixing the bug makes the test pass
func TestMatcherRelationsTupleCopyingBug(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data that will trigger matcher_relations.go:241
	// We need multiple entities to ensure binding tuple collection happens
	tx := db.NewTransaction()

	symbolKw := datalog.NewKeyword(":symbol/ticker")
	priceSymbolKw := datalog.NewKeyword(":price/symbol")
	priceOpenKw := datalog.NewKeyword(":price/open")

	// Create 3 symbols with 10 price bars each
	symbols := []string{"AAPL", "GOOG", "MSFT"}
	for _, sym := range symbols {
		symbolEntity := datalog.NewIdentity(sym)
		tx.Add(symbolEntity, symbolKw, sym)

		for i := 0; i < 10; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("%s-bar-%d", sym, i))
			tx.Add(barEntity, priceSymbolKw, symbolEntity)
			tx.Add(barEntity, priceOpenKw, float64(100+i))
		}
	}
	_, err = tx.Commit()
	require.NoError(t, err)

	// Multi-pattern query that forces binding tuple collection
	// Pattern 1: [?s :symbol/ticker ?ticker] -> 3 results
	// Pattern 2: [?b :price/symbol ?s]       -> joins with pattern 1
	// This triggers matcher_relations.go:241 where bindingTuples are collected
	queryStr := `[:find ?ticker ?open
	              :where [?s :symbol/ticker ?ticker]
	                     [?b :price/symbol ?s]
	                     [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.store)
	exec := executor.NewExecutor(matcher)

	result, err := exec.Execute(q)
	require.NoError(t, err)

	// Count results by iterating
	count := 0
	it := result.Iterator()
	defer it.Close()

	for it.Next() {
		count++
	}

	// Should have 30 results (3 symbols × 10 bars)
	// If matcher_relations.go:241 has the bug, this will return 0
	// because all bindingTuples point to same (garbage) memory
	require.Equal(t, 30, count,
		"Expected 30 results but got %d - likely due to tuple copying bug in matcher_relations.go:241", count)
}
