package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestOHLCColumnOrderBug reproduces the gopher-street column ordering bug
// with a realistic OHLC query pattern
func TestOHLCColumnOrderBug(t *testing.T) {
	// Create realistic OHLC data: 3 bars for one day
	sym1 := datalog.NewIdentity("sym1")
	bar1 := datalog.NewIdentity("bar1")
	bar2 := datalog.NewIdentity("bar2")
	bar3 := datalog.NewIdentity("bar3")

	datoms := []datalog.Datom{
		// Symbol
		{E: sym1, A: datalog.NewKeyword(":symbol/ticker"), V: "TEST", Tx: 1},

		// Bar 1: 09:30 (open of day)
		{E: bar1, A: datalog.NewKeyword(":price/symbol"), V: sym1, Tx: 1},
		{E: bar1, A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 1, 10, 9, 30, 0, 0, time.UTC), Tx: 1},
		{E: bar1, A: datalog.NewKeyword(":price/minute-of-day"), V: int64(570), Tx: 1},
		{E: bar1, A: datalog.NewKeyword(":price/open"), V: 100.00, Tx: 1},
		{E: bar1, A: datalog.NewKeyword(":price/high"), V: 101.50, Tx: 1},
		{E: bar1, A: datalog.NewKeyword(":price/low"), V: 99.50, Tx: 1},
		{E: bar1, A: datalog.NewKeyword(":price/close"), V: 101.00, Tx: 1},
		{E: bar1, A: datalog.NewKeyword(":price/volume"), V: int64(1000000), Tx: 1},

		// Bar 2: 12:00 (midday)
		{E: bar2, A: datalog.NewKeyword(":price/symbol"), V: sym1, Tx: 1},
		{E: bar2, A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 1, 10, 12, 0, 0, 0, time.UTC), Tx: 1},
		{E: bar2, A: datalog.NewKeyword(":price/minute-of-day"), V: int64(720), Tx: 1},
		{E: bar2, A: datalog.NewKeyword(":price/open"), V: 101.00, Tx: 1},
		{E: bar2, A: datalog.NewKeyword(":price/high"), V: 103.00, Tx: 1},
		{E: bar2, A: datalog.NewKeyword(":price/low"), V: 100.50, Tx: 1},
		{E: bar2, A: datalog.NewKeyword(":price/close"), V: 102.00, Tx: 1},
		{E: bar2, A: datalog.NewKeyword(":price/volume"), V: int64(950000), Tx: 1},

		// Bar 3: 16:00 (close of day)
		{E: bar3, A: datalog.NewKeyword(":price/symbol"), V: sym1, Tx: 1},
		{E: bar3, A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 1, 10, 16, 0, 0, 0, time.UTC), Tx: 1},
		{E: bar3, A: datalog.NewKeyword(":price/minute-of-day"), V: int64(960), Tx: 1},
		{E: bar3, A: datalog.NewKeyword(":price/open"), V: 102.00, Tx: 1},
		{E: bar3, A: datalog.NewKeyword(":price/high"), V: 103.00, Tx: 1},
		{E: bar3, A: datalog.NewKeyword(":price/low"), V: 101.50, Tx: 1},
		{E: bar3, A: datalog.NewKeyword(":price/close"), V: 102.50, Tx: 1},
		{E: bar3, A: datalog.NewKeyword(":price/volume"), V: int64(1100000), Tx: 1},
	}

	// OHLC query: date, open, high, low, close, volume
	// This matches the gopher-street pattern exactly
	queryStr := `
	[:find ?date ?open-price ?daily-high ?daily-low ?close-price ?total-volume
	 :where
	   [?s :symbol/ticker "TEST"]

	   ; Get the date from first bar of the day
	   [?morning-bar :price/symbol ?s]
	   [?morning-bar :price/minute-of-day 570]
	   [?morning-bar :price/time ?t]
	   [(year ?t) ?year]
	   [(month ?t) ?month]
	   [(day ?t) ?day]
	   [(str ?year "-" ?month "-" ?day) ?date]

	   ; Subquery 1: Daily high/low (should bind ?daily-high, ?daily-low)
	   [(q [:find (max ?h) (min ?l)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py]
	               [(month ?time) ?pm]
	               [(day ?time) ?pd]
	               [(= ?py ?y)]
	               [(= ?pm ?m)]
	               [(= ?pd ?d)]
	               [?b :price/minute-of-day ?mod]
	               [(>= ?mod 570)]
	               [(<= ?mod 960)]
	               [?b :price/high ?h]
	               [?b :price/low ?l]]
	       $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]

	   ; Subquery 2: Open price (should bind ?open-price)
	   [(q [:find (min ?o)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py]
	               [(month ?time) ?pm]
	               [(day ?time) ?pd]
	               [(= ?py ?y)]
	               [(= ?pm ?m)]
	               [(= ?pd ?d)]
	               [?b :price/minute-of-day ?mod]
	               [(>= ?mod 570)]
	               [(<= ?mod 575)]
	               [?b :price/open ?o]]
	       $ ?s ?year ?month ?day) [[?open-price]]]

	   ; Subquery 3: Close price (should bind ?close-price)
	   [(q [:find (max ?c)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py]
	               [(month ?time) ?pm]
	               [(day ?time) ?pd]
	               [(= ?py ?y)]
	               [(= ?pm ?m)]
	               [(= ?pd ?d)]
	               [?b :price/minute-of-day ?mod]
	               [(>= ?mod 955)]
	               [(<= ?mod 960)]
	               [?b :price/close ?c]]
	       $ ?s ?year ?month ?day) [[?close-price]]]

	   ; Subquery 4: Total volume (should bind ?total-volume)
	   [(q [:find (sum ?v)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py]
	               [(month ?time) ?pm]
	               [(day ?time) ?pd]
	               [(= ?py ?y)]
	               [(= ?pm ?m)]
	               [(= ?pd ?d)]
	               [?b :price/minute-of-day ?mod]
	               [(>= ?mod 570)]
	               [(<= ?mod 960)]
	               [?b :price/volume ?v]]
	       $ ?s ?year ?month ?day) [[?total-volume]]]]
	`

	parsedQuery, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Expected results:
	// ?date = "2025-1-10"
	// ?open-price = 100.00 (min open in 09:30-09:35 range = bar1.open)
	// ?daily-high = 103.00 (max high across all bars)
	// ?daily-low = 99.50 (min low across all bars)
	// ?close-price = 102.50 (max close in 15:55-16:00 range = bar3.close)
	// ?total-volume = 3050000 (sum of all volumes)

	t.Run("ParallelDecorrelation", func(t *testing.T) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: true,
		})

		result, err := exec.Execute(parsedQuery)
		if err != nil {
			t.Fatalf("Parallel execution failed: %v", err)
		}

		if result.Size() == 0 {
			t.Fatal("Expected 1 result, got 0")
		}

		it := result.Iterator()
		if !it.Next() {
			t.Fatal("No result tuple")
		}
		tuple := it.Tuple()
		it.Close()

		t.Logf("Parallel OHLC result: %v", tuple)
		for i, val := range tuple {
			t.Logf("  [%d] = %v (%T)", i, val, val)
		}

		if len(tuple) != 6 {
			t.Fatalf("Expected 6 columns, got %d", len(tuple))
		}

		// Verify :find clause order: ?date ?open-price ?daily-high ?daily-low ?close-price ?total-volume

		// [0] ?date
		if dateStr, ok := tuple[0].(string); !ok {
			t.Errorf("[0] ?date should be string, got %T: %v", tuple[0], tuple[0])
		} else if dateStr != "2025-1-10" {
			t.Errorf("[0] ?date should be '2025-1-10', got '%s'", dateStr)
		}

		// [1] ?open-price = 100.00
		if val, ok := tuple[1].(float64); !ok {
			t.Errorf("[1] ?open-price should be float64, got %T: %v", tuple[1], tuple[1])
		} else if val != 100.00 {
			t.Errorf("[1] ?open-price should be 100.00, got %v", val)
		}

		// [2] ?daily-high = 103.00
		if val, ok := tuple[2].(float64); !ok {
			t.Errorf("[2] ?daily-high should be float64, got %T: %v", tuple[2], tuple[2])
		} else if val != 103.00 {
			t.Errorf("[2] ?daily-high should be 103.00, got %v", val)
		}

		// [3] ?daily-low = 99.50
		if val, ok := tuple[3].(float64); !ok {
			t.Errorf("[3] ?daily-low should be float64, got %T: %v", tuple[3], tuple[3])
		} else if val != 99.50 {
			t.Errorf("[3] ?daily-low should be 99.50, got %v", val)
		}

		// [4] ?close-price = 102.50
		if val, ok := tuple[4].(float64); !ok {
			t.Errorf("[4] ?close-price should be float64, got %T: %v", tuple[4], tuple[4])
		} else if val != 102.50 {
			t.Errorf("[4] ?close-price should be 102.50, got %v", val)
		}

		// [5] ?total-volume = 3050000
		switch val := tuple[5].(type) {
		case int64:
			if val != 3050000 {
				t.Errorf("[5] ?total-volume should be 3050000, got %v", val)
			}
		case float64:
			if val != 3050000.0 {
				t.Errorf("[5] ?total-volume should be 3050000, got %v", val)
			}
		default:
			t.Errorf("[5] ?total-volume should be numeric, got %T: %v", tuple[5], tuple[5])
		}
	})

	t.Run("SequentialDecorrelation", func(t *testing.T) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: false,
		})

		result, err := exec.Execute(parsedQuery)
		if err != nil {
			t.Fatalf("Sequential execution failed: %v", err)
		}

		if result.Size() == 0 {
			t.Fatal("Expected 1 result, got 0")
		}

		it := result.Iterator()
		if !it.Next() {
			t.Fatal("No result tuple")
		}
		tuple := it.Tuple()
		it.Close()

		t.Logf("Sequential OHLC result: %v", tuple)
		for i, val := range tuple {
			t.Logf("  [%d] = %v (%T)", i, val, val)
		}

		// Just verify it returns the same values as parallel (in correct order)
		if len(tuple) != 6 {
			t.Fatalf("Expected 6 columns, got %d", len(tuple))
		}

		// Should match parallel results exactly
		if tuple[1].(float64) != 100.00 {
			t.Errorf("Sequential [1] ?open-price should be 100.00, got %v", tuple[1])
		}
		if tuple[2].(float64) != 103.00 {
			t.Errorf("Sequential [2] ?daily-high should be 103.00, got %v", tuple[2])
		}
		if tuple[3].(float64) != 99.50 {
			t.Errorf("Sequential [3] ?daily-low should be 99.50, got %v", tuple[3])
		}
		if tuple[4].(float64) != 102.50 {
			t.Errorf("Sequential [4] ?close-price should be 102.50, got %v", tuple[4])
		}
	})
}
