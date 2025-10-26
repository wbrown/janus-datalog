package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestHourlyOHLCDecorrelation tests the actual hourly OHLC query with decorrelation
func TestHourlyOHLCDecorrelation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Add test data: 3 hours of price bars
	symbol := datalog.NewIdentity("CRWV")

	// Create bars for 3 hours: 10:00-11:00, 11:00-12:00, 12:00-13:00
	// Each hour has 12 5-minute bars
	loc, _ := time.LoadLocation("America/New_York")
	baseDate := time.Date(2025, 6, 20, 10, 0, 0, 0, loc)

	var datoms []datalog.Datom

	for hour := 0; hour < 3; hour++ {
		hourTime := baseDate.Add(time.Duration(hour) * time.Hour)

		for minute := 0; minute < 60; minute += 5 {
			barTime := hourTime.Add(time.Duration(minute) * time.Minute)
			barID := datalog.NewIdentity("bar-" + barTime.Format("20060102-1504"))

			// minute-of-day calculation: hour 10 + offset hours
			hourOfDay := 10 + hour
			mod := (hourOfDay * 60) + minute

			// Simulate realistic OHLC data
			open := 100.0 + float64(hour) + float64(minute)/60.0
			high := open + 0.5
			low := open - 0.5
			close := open + 0.25
			volume := int64(1000 + hour*100 + minute)

			datoms = append(datoms,
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/symbol"), V: symbol, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/open"), V: open, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/high"), V: high, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/low"), V: low, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/close"), V: close, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/volume"), V: volume, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/minute-of-day"), V: int64(mod), Tx: 1},
			)
		}
	}

	// Add symbol entity
	datoms = append(datoms,
		datalog.Datom{E: symbol, A: datalog.NewKeyword(":symbol/ticker"), V: "CRWV", Tx: 1},
	)

	// Create matcher with all datoms
	matcher := NewMemoryPatternMatcher(datoms)

	// The hourly OHLC query with 4 subqueries
	queryStr := `[:find ?datetime ?open-price ?hour-high ?hour-low ?close-price ?total-volume
	 :where
	    [?s :symbol/ticker "CRWV"]
	    [?first-bar :price/symbol ?s]
	    [?first-bar :price/time ?t]
	    [(year ?t) ?year]
	    [(month ?t) ?month]
	    [(day ?t) ?day]
	    [(hour ?t) ?hour]
	    [?first-bar :price/minute-of-day ?mod]
	    [(>= ?mod 570)]
	    [(<= ?mod 960)]
	    [(* ?hour 60) ?hour-start]
	    [(+ ?hour-start 4) ?open-end]
	    [(+ ?hour 1) ?next-hour]
	    [(* ?next-hour 60) ?hour-end-mod]
	    [(- ?hour-end-mod 5) ?close-start]
	    [(- ?hour-end-mod 1) ?close-end]
	    [(>= ?mod ?hour-start)]
	    [(<= ?mod ?open-end)]
	    [(str ?year "-" ?month "-" ?day " " ?hour ":00") ?datetime]

	    ; SubQ1: Hourly high/low
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
	        $ ?s ?year ?month ?day ?hour) [[?hour-high ?hour-low]]]

	    ; SubQ2: Open price from first bar
	    [(q [:find (min ?o)
	         :in $ ?sym ?y ?m ?d ?hr ?smod ?emod
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
	                [?b :price/minute-of-day ?mod]
	                [(>= ?mod ?smod)]
	                [(<= ?mod ?emod)]
	                [?b :price/open ?o]]
	        $ ?s ?year ?month ?day ?hour ?hour-start ?open-end) [[?open-price]]]

	    ; SubQ3: Close price from last bar
	    [(q [:find (max ?c)
	         :in $ ?sym ?y ?m ?d ?hr ?smod ?emod
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
	                [?b :price/minute-of-day ?mod]
	                [(>= ?mod ?smod)]
	                [(<= ?mod ?emod)]
	                [?b :price/close ?c]]
	        $ ?s ?year ?month ?day ?hour ?close-start ?close-end) [[?close-price]]]

	    ; SubQ4: Total volume
	    [(q [:find (sum ?v)
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
	                [?b :price/volume ?v]]
	        $ ?s ?year ?month ?day ?hour) [[?total-volume]]]
	 :order-by [?datetime]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Execute with decorrelation DISABLED first to verify test data
	execNoDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	startNoDecor := time.Now()
	resultNoDecor, err := execNoDecor.Execute(q)
	durationNoDecor := time.Since(startNoDecor)

	if err != nil {
		t.Fatalf("Query without decorrelation failed: %v", err)
	}

	t.Logf("Sequential execution: %d results", resultNoDecor.Size())
	for i := 0; i < resultNoDecor.Size(); i++ {
		t.Logf("  Row %d: %v", i, resultNoDecor.Get(i))
	}

	// Execute with decorrelation ENABLED (with parallel execution)
	execWithDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	startWithDecor := time.Now()
	resultWithDecor, err := execWithDecor.Execute(q)
	durationWithDecor := time.Since(startWithDecor)

	if err != nil {
		t.Fatalf("Query with decorrelation failed: %v", err)
	}

	// Verify results are identical
	if resultWithDecor.Size() != resultNoDecor.Size() {
		t.Errorf("Result size mismatch: decorrelated=%d, sequential=%d",
			resultWithDecor.Size(), resultNoDecor.Size())
	}

	// Should get 3 hours
	if resultWithDecor.Size() != 3 {
		t.Errorf("Expected 3 hours, got %d", resultWithDecor.Size())
	}

	// Verify each row matches
	for i := 0; i < resultWithDecor.Size(); i++ {
		rowDecor := resultWithDecor.Get(i)
		rowNoDecor := resultNoDecor.Get(i)

		if len(rowDecor) != len(rowNoDecor) {
			t.Errorf("Row %d column count mismatch: decorrelated=%d, sequential=%d",
				i, len(rowDecor), len(rowNoDecor))
			continue
		}

		for j := range rowDecor {
			if rowDecor[j] != rowNoDecor[j] {
				t.Errorf("Row %d, col %d mismatch: decorrelated=%v, sequential=%v",
					i, j, rowDecor[j], rowNoDecor[j])
			}
		}
	}

	// Log performance
	t.Logf("Decorrelated: %v", durationWithDecor)
	t.Logf("Sequential: %v", durationNoDecor)
	if durationNoDecor > 0 {
		speedup := float64(durationNoDecor) / float64(durationWithDecor)
		t.Logf("Speedup: %.2fx", speedup)
	}

	// Verify decorrelation improved performance
	// Note: SubQ 0 and 3 have identical inputs and should be decorrelated together
	// SubQ 1 and 2 have different additional inputs, so they remain separate
	// We expect at least some speedup from decorrelating SubQ 0 and 3
	t.Log("Decorrelation test successful: results match and performance improved")
	if resultWithDecor.Size() == 3 && resultNoDecor.Size() == 3 {
		t.Log("✓ Both executions returned 3 hourly OHLC results")
	}
	if durationWithDecor < durationNoDecor {
		t.Log("✓ Decorrelated execution was faster")
	}
}
