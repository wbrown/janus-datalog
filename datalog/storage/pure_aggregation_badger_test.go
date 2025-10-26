package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestPureAggregationWithBadgerDB tests that pure aggregations work with BadgerDB storage
func TestPureAggregationWithBadgerDB(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "badger-agg-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create BadgerDB database
	db, err := NewDatabase(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Insert test data
	tx := db.NewTransaction()

	symbol := datalog.NewIdentity("symbol:CRWV")

	// Add symbol data
	tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), "CRWV")

	// Insert 500 price bars to trigger streaming aggregation (threshold is 100)
	// Values range from 100.0 to 599.0, so max should be 599.0, min should be 100.0
	for i := 0; i < 500; i++ {
		barID := datalog.NewIdentity(fmt.Sprintf("bar:%d", i))
		tx.Add(barID, datalog.NewKeyword(":price/symbol"), symbol)
		tx.Add(barID, datalog.NewKeyword(":price/high"), float64(100+i))
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Create executor with BadgerDB matcher
	// Note: Matcher options must match executor options for proper propagation
	execOpts := executor.ExecutorOptions{
		EnableIteratorComposition:       true,
		EnableTrueStreaming:             true,
		EnableSymmetricHashJoin:         false,
		EnableParallelSubqueries:        true,
		MaxSubqueryWorkers:              0,
		EnableStreamingJoins:            false,
		EnableStreamingAggregation:      true,
		EnableDebugLogging:              false,
		EnableStreamingAggregationDebug: false,
	}
	matcher := NewBadgerMatcherWithOptions(db.Store(), execOpts)
	opts := planner.PlannerOptions{
		EnableDynamicReordering:         true,
		EnablePredicatePushdown:         true,
		EnableSubqueryDecorrelation:     true,
		EnableParallelDecorrelation:     true,
		EnableCSE:                       false,
		MaxPhases:                       10,
		EnableFineGrainedPhases:         true,
		EnableIteratorComposition:       execOpts.EnableIteratorComposition,
		EnableTrueStreaming:             execOpts.EnableTrueStreaming,
		EnableSymmetricHashJoin:         execOpts.EnableSymmetricHashJoin,
		EnableParallelSubqueries:        execOpts.EnableParallelSubqueries,
		MaxSubqueryWorkers:              execOpts.MaxSubqueryWorkers,
		EnableStreamingJoins:            execOpts.EnableStreamingJoins,
		EnableStreamingAggregation:      execOpts.EnableStreamingAggregation,
		EnableDebugLogging:              execOpts.EnableDebugLogging,
	}
	exec := executor.NewExecutorWithOptions(matcher, opts)

	// Test 1: Non-aggregated query (should work)
	t.Run("NonAggregated", func(t *testing.T) {
		queryStr := `[:find ?h
		              :where [?s :symbol/ticker "CRWV"]
		                     [?p :price/symbol ?s]
		                     [?p :price/high ?h]]`

		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		result, err := exec.Execute(q)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 500, result.Size(), "Should find 500 price bars")

		// Verify we got actual values
		for i := 0; i < result.Size(); i++ {
			tuple := result.Get(i)
			assert.NotNil(t, tuple[0], "Price high should not be nil")
		}
	})

	// Test 2: Pure aggregation with max (BROKEN WITH STREAMING AGGREGATION)
	t.Run("PureMaxAggregation", func(t *testing.T) {
		queryStr := `[:find (max ?h)
		              :where [?s :symbol/ticker "CRWV"]
		                     [?p :price/symbol ?s]
		                     [?p :price/high ?h]]`

		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		// Execute with annotation context to verify streaming is used
		// Create a no-op handler just to enable annotation collection
		handler := func(event annotations.Event) {}
		ctx := executor.NewContext(handler)
		result, err := exec.ExecuteWithContext(ctx, q)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// CRITICAL: Verify streaming aggregation was actually used
		// If threshold changes and this test stops using streaming, it will fail here
		if ctx.Collector() != nil {
			events := ctx.Collector().Events()
			var aggregationMode string
			for _, event := range events {
				if event.Name == "aggregation/executed" {
					if mode, ok := event.Data["aggregation_mode"].(string); ok {
						aggregationMode = mode
						break
					}
				}
			}
			if aggregationMode != "streaming" {
				t.Fatalf("TEST CONFIGURATION ERROR: This test must use streaming aggregation to reproduce the bug, but used '%s'. "+
					"Either increase test data size (currently 500 rows) or decrease StreamingAggregationThreshold (currently 100).",
					aggregationMode)
			}
		}

		assert.Equal(t, 1, result.Size(), "Pure aggregation should return 1 row")

		// Check the max value
		tuple := result.Get(0)
		assert.NotNil(t, tuple[0], "CRITICAL BUG: Max value should not be nil when data exists")
		assert.Equal(t, 599.0, tuple[0], "Max should be 599.0 (highest value inserted)")
	})

	// Test 3: Pure aggregation with min (BROKEN WITH STREAMING AGGREGATION)
	t.Run("PureMinAggregation", func(t *testing.T) {
		queryStr := `[:find (min ?h)
		              :where [?s :symbol/ticker "CRWV"]
		                     [?p :price/symbol ?s]
		                     [?p :price/high ?h]]`

		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		// Execute with annotation context to verify streaming is used
		// Create a no-op handler just to enable annotation collection
		handler := func(event annotations.Event) {}
		ctx := executor.NewContext(handler)
		result, err := exec.ExecuteWithContext(ctx, q)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// CRITICAL: Verify streaming aggregation was actually used
		if ctx.Collector() != nil {
			events := ctx.Collector().Events()
			var aggregationMode string
			for _, event := range events {
				if event.Name == "aggregation/executed" {
					if mode, ok := event.Data["aggregation_mode"].(string); ok {
						aggregationMode = mode
						break
					}
				}
			}
			if aggregationMode != "streaming" {
				t.Fatalf("TEST CONFIGURATION ERROR: This test must use streaming aggregation to reproduce the bug, but used '%s'. "+
					"Either increase test data size (currently 500 rows) or decrease StreamingAggregationThreshold (currently 100).",
					aggregationMode)
			}
		}

		assert.Equal(t, 1, result.Size(), "Pure aggregation should return 1 row")

		// Check the min value
		tuple := result.Get(0)
		assert.NotNil(t, tuple[0], "CRITICAL BUG: Min value should not be nil when data exists")
		assert.Equal(t, 100.0, tuple[0], "Min should be 100.0 (lowest value inserted)")
	})

	// Test 4: Count (should work even with streaming)
	t.Run("PureCountAggregation", func(t *testing.T) {
		queryStr := `[:find (count ?p)
		              :where [?s :symbol/ticker "CRWV"]
		                     [?p :price/symbol ?s]]`

		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		result, err := exec.Execute(q)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 1, result.Size(), "Pure aggregation should return 1 row")

		// Check the count
		tuple := result.Get(0)
		assert.Equal(t, int64(500), tuple[0], "Count should be 500")
	})
}
