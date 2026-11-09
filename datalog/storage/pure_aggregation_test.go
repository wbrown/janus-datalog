package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestStorageBackedPureAggregation pins that pure aggregations over a real
// store answer alike on every leg, and that the max and min aggregations run
// on the leg's path: streaming on the legs with streaming aggregation, over
// 500 tuples against a threshold of 100, and batch on the others.
func TestStorageBackedPureAggregation(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Insert test data
			tx := db.NewTransaction()

			symbol := datalog.NewIdentity("symbol:CRWV")

			// Add symbol data
			tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), "CRWV")

			// Insert 500 price bars, above the streaming aggregation threshold of
			// 100. Values range from 100.0 to 599.0, so max should be 599.0, min
			// should be 100.0
			for i := 0; i < 500; i++ {
				barID := datalog.NewIdentity(fmt.Sprintf("bar:%d", i))
				tx.Add(barID, datalog.NewKeyword(":price/symbol"), symbol)
				tx.Add(barID, datalog.NewKeyword(":price/high"), float64(100+i))
			}

			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}

			testStorageBackedPureAggregation(t, db, mode)
		})
	}
}

func testStorageBackedPureAggregation(t *testing.T, db *Database, mode optimizerMode) {
	opts := mode.plannerOptions()
	exec := executor.NewExecutorWithOptions(db.Matcher(), db, opts)
	// path is the aggregation path the leg runs over 500 tuples: streaming with
	// streaming aggregation on, batch with it off.
	path := "batch"
	if mode.aggregation {
		path = "streaming"
	}

	// Test 1: Non-aggregated query (should work)
	t.Run("NonAggregated", func(t *testing.T) {
		queryStr := `[:find ?h
		              :where [?s :symbol/ticker "CRWV"]
		                     [?p :price/symbol ?s]
		                     [?p :price/high ?h]]`

		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		tuples, err := executor.CollectTuples(exec.Execute(q))
		assert.NoError(t, err)
		assert.Len(t, tuples, 500, "Should find 500 price bars")

		// Verify we got actual values
		for _, tuple := range tuples {
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

		// The handler reads the one datum this test asserts on. Registered on the
		// options this executor and its matcher are built with: the aggregation
		// reports through the options of the relation it folds, which the matcher
		// produced.
		var aggregationMode string
		annotated := opts
		annotated.Handler = func(event annotations.Event) {
			if event.Name == annotations.AggregationExecuted {
				if mode, ok := event.Data["aggregation_mode"].(string); ok {
					aggregationMode = mode
				}
			}
		}
		annotatedExec := executor.NewExecutorWithOptions(
			NewPatternMatcherWithOptions(db.Store(), executor.ExecutorOptionsFromPlanner(annotated)),
			db, annotated)

		result, err := annotatedExec.Execute(q)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The aggregation ran on the leg's path: a threshold change that moved an
		// aggregation leg to the batch path fails here.
		if aggregationMode != path {
			t.Fatalf("the aggregation ran as %q on %s, whose path is %q", aggregationMode, mode.name, path)
		}

		assert.Equal(t, 1, result.Size(), "Pure aggregation should return 1 tuple")

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

		// The handler reads the one datum this test asserts on, registered on the
		// options this executor and its matcher are built with.
		var aggregationMode string
		annotated := opts
		annotated.Handler = func(event annotations.Event) {
			if event.Name == annotations.AggregationExecuted {
				if mode, ok := event.Data["aggregation_mode"].(string); ok {
					aggregationMode = mode
				}
			}
		}
		annotatedExec := executor.NewExecutorWithOptions(
			NewPatternMatcherWithOptions(db.Store(), executor.ExecutorOptionsFromPlanner(annotated)),
			db, annotated)

		result, err := annotatedExec.Execute(q)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The aggregation ran on the leg's path.
		if aggregationMode != path {
			t.Fatalf("the aggregation ran as %q on %s, whose path is %q", aggregationMode, mode.name, path)
		}

		assert.Equal(t, 1, result.Size(), "Pure aggregation should return 1 tuple")

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
		assert.Equal(t, 1, result.Size(), "Pure aggregation should return 1 tuple")

		// Check the count
		tuple := result.Get(0)
		assert.Equal(t, int64(500), tuple[0], "Count should be 500")
	})
}
