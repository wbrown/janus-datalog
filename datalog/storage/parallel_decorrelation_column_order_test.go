package storage

import (
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestParallelDecorrelationColumnOrderBadger tests parallel decorrelation column ordering with BadgerDB
// This reproduces the gopher-street bug where BadgerMatcher + parallel decorrelation
// scrambles column order due to inconsistent transaction snapshots across goroutines
func TestParallelDecorrelationColumnOrderBadger(t *testing.T) {
	// Create temporary BadgerDB
	tmpDir, err := os.MkdirTemp("", "badger-column-order-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := NewDatabase(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create test data - same as OHLC test
	testData := []struct {
		time   time.Time
		open   float64
		high   float64
		low    float64
		close  float64
		volume int64
	}{
		{time: time.Date(2025, 1, 10, 9, 30, 0, 0, time.UTC), open: 100.00, high: 101.50, low: 99.50, close: 101.00, volume: 1000000},
		{time: time.Date(2025, 1, 10, 12, 0, 0, 0, time.UTC), open: 101.00, high: 103.00, low: 100.50, close: 102.00, volume: 950000},
		{time: time.Date(2025, 1, 10, 16, 0, 0, 0, time.UTC), open: 102.00, high: 103.00, low: 101.50, close: 102.50, volume: 1100000},
	}

	// Insert test data
	tx := db.NewTransaction()
	symbol := datalog.NewIdentity("TEST")
	tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), "TEST")

	for _, bar := range testData {
		barID := datalog.NewIdentity("bar-" + bar.time.String())

		// Price bar data
		tx.Add(barID, datalog.NewKeyword(":price/symbol"), symbol)
		tx.Add(barID, datalog.NewKeyword(":price/time"), bar.time)
		tx.Add(barID, datalog.NewKeyword(":price/minute-of-day"), int64(bar.time.Hour()*60+bar.time.Minute()))
		tx.Add(barID, datalog.NewKeyword(":price/open"), bar.open)
		tx.Add(barID, datalog.NewKeyword(":price/high"), bar.high)
		tx.Add(barID, datalog.NewKeyword(":price/low"), bar.low)
		tx.Add(barID, datalog.NewKeyword(":price/close"), bar.close)
		tx.Add(barID, datalog.NewKeyword(":price/volume"), bar.volume)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit test data: %v", err)
	}

	// OHLC query with 4 subqueries
	queryStr := `
[:find ?date ?open-price ?daily-high ?daily-low ?close-price ?total-volume
 :where
   [?s :symbol/ticker "TEST"]

   ; Get date from morning bar
   [?morning-bar :price/symbol ?s]
   [?morning-bar :price/minute-of-day 570]
   [?morning-bar :price/time ?t]
   [(year ?t) ?year]
   [(month ?t) ?month]
   [(day ?t) ?day]
   [(str ?year "-" ?month "-" ?day) ?date]

   ; Subquery 1: Daily high/low (binds ?daily-high, ?daily-low)
   [(q [:find (max ?h) (min ?l)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py] [(= ?py ?y)]
               [(month ?time) ?pm] [(= ?pm ?m)]
               [(day ?time) ?pd] [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 570)] [(<= ?mod 960)]
               [?b :price/high ?h]
               [?b :price/low ?l]]
       $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]

   ; Subquery 2: Open price (binds ?open-price)
   [(q [:find (min ?o)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py] [(= ?py ?y)]
               [(month ?time) ?pm] [(= ?pm ?m)]
               [(day ?time) ?pd] [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 570)] [(<= ?mod 575)]
               [?b :price/open ?o]]
       $ ?s ?year ?month ?day) [[?open-price]]]

   ; Subquery 3: Close price (binds ?close-price)
   [(q [:find (max ?c)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py] [(= ?py ?y)]
               [(month ?time) ?pm] [(= ?pm ?m)]
               [(day ?time) ?pd] [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 955)] [(<= ?mod 960)]
               [?b :price/close ?c]]
       $ ?s ?year ?month ?day) [[?close-price]]]

   ; Subquery 4: Total volume (binds ?total-volume)
   [(q [:find (sum ?v)
        :in $ ?sym ?y ?m ?d
        :where [?b :price/symbol ?sym]
               [?b :price/time ?time]
               [(year ?time) ?py] [(= ?py ?y)]
               [(month ?time) ?pm] [(= ?pm ?m)]
               [(day ?time) ?pd] [(= ?pd ?d)]
               [?b :price/minute-of-day ?mod]
               [(>= ?mod 570)] [(<= ?mod 960)]
               [?b :price/volume ?v]]
       $ ?s ?year ?month ?day) [[?total-volume]]]]
`

	parsedQuery, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	t.Run("ParallelDecorrelation", func(t *testing.T) {
		matcher := db.Matcher()
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: true,
		})

		result, err := exec.Execute(parsedQuery)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if result.Size() != 1 {
			t.Fatalf("Expected 1 result, got %d", result.Size())
		}

		it := result.Iterator()
		defer it.Close()
		if !it.Next() {
			t.Fatal("No results returned")
		}
		tuple := it.Tuple()

		t.Logf("Parallel OHLC result: %v", tuple)
		for i, val := range tuple {
			t.Logf("  [%d] = %v (%T)", i, val, val)
		}

		// Verify correct column ordering: [?date, ?open-price, ?daily-high, ?daily-low, ?close-price, ?total-volume]
		if len(tuple) != 6 {
			t.Fatalf("Expected 6 columns, got %d", len(tuple))
		}

		// Column 1: ?open-price should be 100.00
		if val, ok := tuple[1].(float64); !ok || val != 100.0 {
			t.Errorf("Column 1 (?open-price) should be 100.0, got %v (%T)", tuple[1], tuple[1])
		}

		// Column 2: ?daily-high should be 103.00
		if val, ok := tuple[2].(float64); !ok || val != 103.0 {
			t.Errorf("Column 2 (?daily-high) should be 103.0, got %v (%T)", tuple[2], tuple[2])
		}

		// Column 3: ?daily-low should be 99.50
		if val, ok := tuple[3].(float64); !ok || val != 99.5 {
			t.Errorf("Column 3 (?daily-low) should be 99.5, got %v (%T)", tuple[3], tuple[3])
		}

		// Column 4: ?close-price should be 102.50
		if val, ok := tuple[4].(float64); !ok || val != 102.5 {
			t.Errorf("Column 4 (?close-price) should be 102.5, got %v (%T)", tuple[4], tuple[4])
		}

		// Column 5: ?total-volume should be 3050000
		if val, ok := tuple[5].(float64); !ok || val != 3050000 {
			t.Errorf("Column 5 (?total-volume) should be 3050000, got %v (%T)", tuple[5], tuple[5])
		}
	})

	t.Run("SequentialDecorrelation", func(t *testing.T) {
		matcher := db.Matcher()
		exec := executor.NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: false,
		})

		result, err := exec.Execute(parsedQuery)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if result.Size() != 1 {
			t.Fatalf("Expected 1 result, got %d", result.Size())
		}

		it := result.Iterator()
		defer it.Close()
		if !it.Next() {
			t.Fatal("No results returned")
		}
		tuple := it.Tuple()

		t.Logf("Sequential OHLC result: %v", tuple)
		for i, val := range tuple {
			t.Logf("  [%d] = %v (%T)", i, val, val)
		}

		// Same verification as parallel
		if len(tuple) != 6 {
			t.Fatalf("Expected 6 columns, got %d", len(tuple))
		}

		if val, ok := tuple[1].(float64); !ok || val != 100.0 {
			t.Errorf("Column 1 (?open-price) should be 100.0, got %v (%T)", tuple[1], tuple[1])
		}
		if val, ok := tuple[2].(float64); !ok || val != 103.0 {
			t.Errorf("Column 2 (?daily-high) should be 103.0, got %v (%T)", tuple[2], tuple[2])
		}
		if val, ok := tuple[3].(float64); !ok || val != 99.5 {
			t.Errorf("Column 3 (?daily-low) should be 99.5, got %v (%T)", tuple[3], tuple[3])
		}
		if val, ok := tuple[4].(float64); !ok || val != 102.5 {
			t.Errorf("Column 4 (?close-price) should be 102.5, got %v (%T)", tuple[4], tuple[4])
		}
		if val, ok := tuple[5].(float64); !ok || val != 3050000 {
			t.Errorf("Column 5 (?total-volume) should be 3050000, got %v (%T)", tuple[5], tuple[5])
		}
	})
}
