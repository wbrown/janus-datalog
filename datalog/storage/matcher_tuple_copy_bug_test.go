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

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create data shaped to trigger the bug:
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

			_, err := tx.Commit()
			require.NoError(t, err)

			// CRITICAL: Use streaming to trigger the buffer reuse bug
			opts := executor.ExecutorOptions{
				EnableTrueStreaming: true,
			}
			matcher := NewPatternMatcherWithOptions(db.store, opts)
			exec := executor.NewExecutorWithOptions(matcher, db, mode.plannerOptions())

			result, err := exec.Execute(q)
			require.NoError(t, err)

			// Count results by iterating
			count := 0
			it := result.Iterator()
			defer it.Close()

			for it.Next() {
				count++
			}

			// Expected: 1000 results (10 symbols × 100 bars each). Without the
			// copy at matcher_relations.go:241 every bindingTuple points at one
			// reused buffer, and the count comes back 0 or wrong.
			require.Equal(t, 1000, count,
				"Expected 1000 results but got %d. "+
					"matcher_relations.go:241 is not copying tuples, "+
					"so all bindingTuples point at the same reused buffer: "+
					"the scan finds datoms but the relation shows 0 tuples.", count)
		})
	}
}
