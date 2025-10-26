package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestMatcherTupleCopyBug reproduces the tuple copying bug in matcher_relations.go:241
//
// This test SHOULD FAIL with the current buggy code because:
// 1. Pattern scan finds datoms: [?b :price/symbol ?s] → 15552 datoms
// 2. But relation shows 0 tuples due to buffer reuse in bindingTuples collection
// 3. Query returns 0 results instead of expected results
//
// After fixing matcher_relations.go:241 to copy tuples, this test should PASS.
func TestMatcherTupleCopyBug(t *testing.T) {
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	require.NoError(t, err)
	defer db.Close()

	// Create data matching gopher-street pattern:
	// - 10 symbols (MORE binding tuples to trigger bug)
	// - Many price bars per symbol (to ensure we hit the bug)
	tx := db.NewTransaction()

	symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN", "META", "TSLA", "NVDA", "AMD", "INTC", "QCOM"}
	for _, sym := range symbols {
		symbolEntity := datalog.NewIdentity(sym)
		tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), sym)

		// Add 100 price bars per symbol (1000 total datoms)
		for i := 0; i < 100; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("%s-bar-%d", sym, i))
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
			tx.Add(barEntity, datalog.NewKeyword(":price/open"), float64(100+i))
		}
	}

	_, err = tx.Commit()
	require.NoError(t, err)

	// Multi-pattern query that triggers matcher_relations.go:241
	// Pattern 1: [?s :symbol/ticker ?ticker] → binds ?s
	// Pattern 2: [?b :price/symbol ?s]       → uses ?s from pattern 1
	//
	// This forces matchWithoutIteratorReuse to collect bindingTuples
	queryStr := `[:find ?ticker ?open
	              :where [?s :symbol/ticker ?ticker]
	                     [?b :price/symbol ?s]
	                     [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	// CRITICAL: Use streaming to trigger the buffer reuse bug
	opts := executor.ExecutorOptions{
		EnableTrueStreaming: true,
	}
	matcher := NewBadgerMatcherWithOptions(db.store, opts)
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

	// Expected: 1000 results (10 symbols × 100 bars each)
	// With bug: 0 results or incorrect count (bindingTuples all point to same garbage memory)
	//
	// This assertion will FAIL until matcher_relations.go:241 is fixed
	require.Equal(t, 1000, count,
		"Expected 2000 results but got %d. "+
		"Bug: matcher_relations.go:241 doesn't copy tuples, "+
		"causing all bindingTuples to point to same reused buffer. "+
		"Scan finds datoms but relation shows 0 tuples.", count)
}
