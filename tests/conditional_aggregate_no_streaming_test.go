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
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestConditionalAggregateWithoutStreaming verifies that conditional aggregate
// rewriting works even when streaming is explicitly disabled
func TestConditionalAggregateWithoutStreaming(t *testing.T) {
	dir, err := os.MkdirTemp("", "cond-agg-no-stream-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create test data
	tx := db.NewTransaction()

	alice := datalog.NewIdentity("person:alice")
	tx.Add(alice, datalog.NewKeyword(":person/name"), "Alice")

	// Create events for day 15 and day 16
	for day := 15; day <= 16; day++ {
		for eventNum := 0; eventNum < 10; eventNum++ {
			e := datalog.NewIdentity(fmt.Sprintf("event:d%d-e%d", day, eventNum))
			tx.Add(e, datalog.NewKeyword(":event/person"), alice)
			tx.Add(e, datalog.NewKeyword(":event/time"), time.Date(2025, 1, day, 10, 0, 0, 0, time.UTC))
			value := int64(day*10 + eventNum)
			tx.Add(e, datalog.NewKeyword(":event/value"), value)
		}
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Query with subquery
	queryStr := `[:find ?name ?day ?max-value
	             :where
	             [?p :person/name ?name]
	             [?e :event/person ?p]
	             [?e :event/time ?time]
	             [(day ?time) ?day]
	             [(q [:find (max ?v)
	                  :in $ ?person ?d
	                  :where
	                  [?ev :event/person ?person]
	                  [?ev :event/time ?t]
	                  [(day ?t) ?pd]
	                  [(= ?pd ?d)]
	                  [?ev :event/value ?v]]
	               $ ?p ?day) [[?max-value]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Test with conditional aggregate rewriting but streaming DISABLED
	opts := planner.PlannerOptions{
		EnableDynamicReordering:             true,
		EnableConditionalAggregateRewriting: true,
		EnableSubqueryDecorrelation:         false,
		// Explicitly disable streaming
		EnableIteratorComposition:  false,
		EnableTrueStreaming:        false,
		EnableStreamingAggregation: false,
	}

	exec := executor.NewExecutorWithOptions(storage.NewBadgerMatcher(db.Store()), opts)
	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Query execution failed with streaming disabled: %v", err)
	}

	// Should return 2 rows (Alice day 15, Alice day 16)
	expectedSize := 2
	if result.Size() != expectedSize {
		t.Errorf("Expected %d rows, got %d", expectedSize, result.Size())
	}

	// Verify the results
	expectedResults := map[int64]int64{
		15: 159, // day 15, event 9: 15*10 + 9 = 159
		16: 169, // day 16, event 9: 16*10 + 9 = 169
	}

	iter := result.Iterator()
	defer iter.Close()

	rowCount := 0
	for iter.Next() {
		tuple := iter.Tuple()
		rowCount++

		name, ok := tuple[0].(string)
		if !ok || name != "Alice" {
			t.Errorf("Row %d: expected name 'Alice', got %v", rowCount, tuple[0])
			continue
		}

		day, ok := tuple[1].(int64)
		if !ok {
			t.Errorf("Row %d: expected int64 day, got %T", rowCount, tuple[1])
			continue
		}

		maxValue, ok := tuple[2].(int64)
		if !ok {
			t.Errorf("Row %d: expected int64 max value, got %T", rowCount, tuple[2])
			continue
		}

		expectedMax, exists := expectedResults[day]
		if !exists {
			t.Errorf("Row %d: unexpected day: %d", rowCount, day)
			continue
		}

		if maxValue != expectedMax {
			t.Errorf("Row %d: day %d: expected max %d, got %d",
				rowCount, day, expectedMax, maxValue)
		}
	}

	t.Logf("✓ Conditional aggregate rewriting works without streaming (%d rows)", rowCount)
}
