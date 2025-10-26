package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestTimeRangeMetadataFlow verifies that time ranges are extracted and passed to merged queries
func TestTimeRangeMetadataFlow(t *testing.T) {
	// Create test data: 3 hours of price bars for CRWV
	var datoms []datalog.Datom

	symCRWV := datalog.NewIdentity("CRWV")

	// Hour 9: 2025-06-20 09:00 to 10:00
	for minute := 0; minute < 60; minute += 5 {
		barTime := time.Date(2025, 6, 20, 9, minute, 0, 0, time.UTC)
		barID := datalog.NewIdentity("bar-" + barTime.Format(time.RFC3339))
		datoms = append(datoms,
			datalog.Datom{E: symCRWV, A: datalog.NewKeyword(":symbol/ticker"), V: "CRWV", Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/symbol"), V: symCRWV, Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/high"), V: 100.0 + float64(minute), Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/low"), V: 99.0 + float64(minute), Tx: 1},
		)
	}

	// Hour 10: 2025-06-20 10:00 to 11:00
	for minute := 0; minute < 60; minute += 5 {
		barTime := time.Date(2025, 6, 20, 10, minute, 0, 0, time.UTC)
		barID := datalog.NewIdentity("bar-" + barTime.Format(time.RFC3339))
		datoms = append(datoms,
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/symbol"), V: symCRWV, Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/high"), V: 110.0 + float64(minute), Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/low"), V: 109.0 + float64(minute), Tx: 1},
		)
	}

	// Hour 11: 2025-06-20 11:00 to 12:00
	for minute := 0; minute < 60; minute += 5 {
		barTime := time.Date(2025, 6, 20, 11, minute, 0, 0, time.UTC)
		barID := datalog.NewIdentity("bar-" + barTime.Format(time.RFC3339))
		datoms = append(datoms,
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/symbol"), V: symCRWV, Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/high"), V: 120.0 + float64(minute), Tx: 1},
			datalog.Datom{E: barID, A: datalog.NewKeyword(":price/low"), V: 119.0 + float64(minute), Tx: 1},
		)
	}

	// Setup matcher and executor
	matcher := NewMemoryPatternMatcher(datoms)

	plannerOpts := planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: false, // Disable for deterministic testing
	}
	exec := NewExecutorWithOptions(matcher, plannerOpts)

	// Query: Hourly OHLC for hours 9 and 10 only (not 11)
	queryStr := `[:find ?hour (max ?h) (min ?l)
	 :where
	   [?s :symbol/ticker "CRWV"]
	   [?b :price/symbol ?s]
	   [?b :price/time ?t]
	   [(year ?t) ?year]
	   [(month ?t) ?month]
	   [(day ?t) ?day]
	   [(hour ?t) ?hour]
	   [(= ?year 2025)]
	   [(= ?month 6)]
	   [(= ?day 20)]
	   [(<= ?hour 10)]  ; Only hours 9 and 10
	   [?b :price/high ?h]
	   [?b :price/low ?l]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Execute without decorrelation (baseline)
	execNoDecorr := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
	})

	baselineResult, err := execNoDecorr.Execute(q)
	if err != nil {
		t.Fatalf("Baseline execution failed: %v", err)
	}

	// Execute with decorrelation (should use time range optimization)
	optimizedResult, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Optimized execution failed: %v", err)
	}

	// Results should be identical
	if baselineResult.Size() != optimizedResult.Size() {
		t.Errorf("Result size mismatch: baseline=%d, optimized=%d",
			baselineResult.Size(), optimizedResult.Size())
	}

	// Should have results for hours 9 and 10 only
	if baselineResult.Size() != 2 {
		t.Errorf("Expected 2 hours (9, 10), got %d", baselineResult.Size())
	}

	// Sort both results by first column (hour) for deterministic comparison
	baselineSorted := baselineResult.Sorted()
	optimizedSorted := optimizedResult.Sorted()

	// Verify actual values match
	for i := 0; i < len(baselineSorted) && i < len(optimizedSorted); i++ {
		baselineTuple := baselineSorted[i]
		optimizedTuple := optimizedSorted[i]

		if len(baselineTuple) != len(optimizedTuple) {
			t.Errorf("Tuple %d length mismatch: baseline=%d, optimized=%d",
				i, len(baselineTuple), len(optimizedTuple))
			continue
		}

		for j := 0; j < len(baselineTuple); j++ {
			if baselineTuple[j] != optimizedTuple[j] {
				t.Errorf("Tuple %d column %d mismatch: baseline=%v, optimized=%v",
					i, j, baselineTuple[j], optimizedTuple[j])
			}
		}
	}
}

// TestTimeRangeOptimizationCorrectness verifies decorrelated subquery with time ranges produces correct results
func TestTimeRangeOptimizationCorrectness(t *testing.T) {
	// Create more realistic test data
	var datoms []datalog.Datom

	symCRWV := datalog.NewIdentity("CRWV")
	datoms = append(datoms,
		datalog.Datom{E: symCRWV, A: datalog.NewKeyword(":symbol/ticker"), V: "CRWV", Tx: 1},
	)

	// Create 5 hours of data: hours 9-13
	for hour := 9; hour <= 13; hour++ {
		for minute := 0; minute < 60; minute += 15 { // 4 bars per hour
			barTime := time.Date(2025, 6, 20, hour, minute, 0, 0, time.UTC)
			barID := datalog.NewIdentity("bar-" + barTime.Format(time.RFC3339))
			high := 100.0 + float64(hour*10) + float64(minute)/10.0
			low := 99.0 + float64(hour*10) + float64(minute)/10.0

			datoms = append(datoms,
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/symbol"), V: symCRWV, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/high"), V: high, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/low"), V: low, Tx: 1},
			)
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	plannerOpts := planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: false,
	}
	exec := NewExecutorWithOptions(matcher, plannerOpts)

	// Query with decorrelated subquery (like OHLC pattern)
	queryStr := `[:find ?hour ?high ?low
	 :where
	   [?s :symbol/ticker "CRWV"]
	   [?first :price/symbol ?s]
	   [?first :price/time ?t]
	   [(year ?t) ?year]
	   [(month ?t) ?month]
	   [(day ?t) ?day]
	   [(hour ?t) ?hour]

	   ; Decorrelated subquery for high/low
	   [(q [:find (max ?h) (min ?l)
	        :in $ ?sym ?y ?m ?d ?hr
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py]
	               [(month ?time) ?pm]
	               [(day ?time) ?pd]
	               [(hour ?time) ?ph]
	               [(= ?py ?y)]
	               [(= ?pm ?m)]
	               [(= ?pd ?d)]
	               [(= ?ph ?hr)]
	               [?b :price/high ?h]
	               [?b :price/low ?l]]
	       $ ?s ?year ?month ?day ?hour) [[?high ?low]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Should have 5 hours of results (9-13)
	if result.Size() != 5 {
		t.Errorf("Expected 5 hours of data, got %d", result.Size())
	}

	// Verify each hour has correct high/low aggregation
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		if len(tuple) < 3 {
			t.Errorf("Tuple too short: %v", tuple)
			continue
		}

		hour, ok := tuple[0].(int64)
		if !ok {
			t.Errorf("Hour is not int64: %v", tuple[0])
			continue
		}

		// For each hour, high should be at the last minute (45)
		// Low should be at the first minute (0)
		expectedMaxHigh := 100.0 + float64(hour*10) + 45.0/10.0
		expectedMinLow := 99.0 + float64(hour*10) + 0.0/10.0

		high, ok := tuple[1].(float64)
		if !ok {
			t.Errorf("High is not float64: %v", tuple[1])
			continue
		}

		low, ok := tuple[2].(float64)
		if !ok {
			t.Errorf("Low is not float64: %v", tuple[2])
			continue
		}

		if high != expectedMaxHigh {
			t.Errorf("Hour %d: expected max high %.1f, got %.1f", hour, expectedMaxHigh, high)
		}

		if low != expectedMinLow {
			t.Errorf("Hour %d: expected min low %.1f, got %.1f", hour, expectedMinLow, low)
		}
	}
}
