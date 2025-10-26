package executor

import (
	"strings"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestCSELargeScale tests CSE performance at realistic scale
// Uses same dataset as TestDecorrelationOHLCScale: 8 days, 624 bars
// Compares all 4 configurations to determine optimal settings
func TestCSELargeScale(t *testing.T) {
	// Generate 8 trading days of 5-minute bar data (same as TestDecorrelationOHLCScale)
	var datoms []datalog.Datom

	symbolID := datalog.NewIdentity("symbol-CRWV")
	datoms = append(datoms, datalog.Datom{
		E: symbolID, A: datalog.NewKeyword(":symbol/ticker"), V: "CRWV", Tx: 1,
	})

	loc := time.UTC
	startDate := time.Date(2025, 6, 1, 0, 0, 0, 0, loc)

	barID := 0
	totalBars := 0

	// Generate 8 trading days
	for day := 0; day < 8; day++ {
		currentDate := startDate.AddDate(0, 0, day)

		// Trading hours: 9:30 AM to 4:00 PM
		for hour := 9; hour <= 15; hour++ {
			startMinute := 0
			endMinute := 60

			if hour == 9 {
				startMinute = 30
			}
			if hour == 15 {
				endMinute = 60
			}

			for minute := startMinute; minute < endMinute; minute += 5 {
				barTime := time.Date(
					currentDate.Year(), currentDate.Month(), currentDate.Day(),
					hour, minute, 0, 0, loc,
				)

				barEntity := datalog.NewIdentity("bar-" + string(rune('0'+barID/1000)) +
					string(rune('0'+(barID/100)%10)) +
					string(rune('0'+(barID/10)%10)) +
					string(rune('0'+barID%10)))
				barID++
				totalBars++

				minuteOfDay := int64(hour*60 + minute)
				basePrice := 100.0 + float64(day)*0.5 + float64(hour-9)*0.2
				open := basePrice + float64(minute)*0.01
				high := open + 0.5
				low := open - 0.3
				close := open + 0.2
				volume := float64(10000 + minute*100)

				datoms = append(datoms,
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/minute-of-day"), V: minuteOfDay, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/open"), V: open, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/high"), V: high, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/low"), V: low, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/close"), V: close, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/volume"), V: volume, Tx: 1},
				)
			}
		}
	}

	t.Logf("Generated %d 5-minute bars", totalBars)
	t.Logf("Total datoms: %d", len(datoms))

	matcher := NewMemoryPatternMatcher(datoms)

	// The actual OHLC query - SubQ 0 and SubQ 3 have identical inputs (CSE opportunity)
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
	            $ ?s ?year ?month ?day ?hour) [[?total-volume]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Helper to count filter groups
	countFilterGroups := func(events []annotations.Event) int {
		for _, e := range events {
			if e.Name == "decorrelated_subqueries/begin" {
				if fg, ok := e.Data["filter_groups"].(int); ok {
					return fg
				}
			}
		}
		return 0
	}

	// Test 1: Sequential WITHOUT CSE
	t.Log("\n=== Test 1: Sequential WITHOUT CSE ===")
	exec1 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: false,
		EnableCSE:                   false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var events1 []annotations.Event
	handler1 := annotations.Handler(func(e annotations.Event) {
		if strings.HasPrefix(e.Name, "decorrelated_") {
			events1 = append(events1, e)
		}
	})

	start1 := time.Now()
	result1, err := exec1.ExecuteWithContext(NewContext(handler1), q)
	dur1 := time.Since(start1)
	if err != nil {
		t.Fatalf("Test 1 failed: %v", err)
	}
	fg1 := countFilterGroups(events1)
	t.Logf("Sequential WITHOUT CSE: %v (%d results, %d filter groups)", dur1, result1.Size(), fg1)

	// Test 2: Sequential WITH CSE
	t.Log("\n=== Test 2: Sequential WITH CSE ===")
	exec2 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: false,
		EnableCSE:                   true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var events2 []annotations.Event
	handler2 := annotations.Handler(func(e annotations.Event) {
		if strings.HasPrefix(e.Name, "decorrelated_") {
			events2 = append(events2, e)
		}
	})

	start2 := time.Now()
	result2, err := exec2.ExecuteWithContext(NewContext(handler2), q)
	dur2 := time.Since(start2)
	if err != nil {
		t.Fatalf("Test 2 failed: %v", err)
	}
	fg2 := countFilterGroups(events2)
	t.Logf("Sequential WITH CSE:    %v (%d results, %d filter group)", dur2, result2.Size(), fg2)

	// Test 3: Parallel WITHOUT CSE
	t.Log("\n=== Test 3: Parallel WITHOUT CSE ===")
	exec3 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableCSE:                   false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var events3 []annotations.Event
	handler3 := annotations.Handler(func(e annotations.Event) {
		if strings.HasPrefix(e.Name, "decorrelated_") {
			events3 = append(events3, e)
		}
	})

	start3 := time.Now()
	result3, err := exec3.ExecuteWithContext(NewContext(handler3), q)
	dur3 := time.Since(start3)
	if err != nil {
		t.Fatalf("Test 3 failed: %v", err)
	}
	fg3 := countFilterGroups(events3)
	t.Logf("Parallel WITHOUT CSE:    %v (%d results, %d filter groups)", dur3, result3.Size(), fg3)

	// Test 4: Parallel WITH CSE
	t.Log("\n=== Test 4: Parallel WITH CSE ===")
	exec4 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableCSE:                   true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var events4 []annotations.Event
	handler4 := annotations.Handler(func(e annotations.Event) {
		if strings.HasPrefix(e.Name, "decorrelated_") {
			events4 = append(events4, e)
		}
	})

	start4 := time.Now()
	result4, err := exec4.ExecuteWithContext(NewContext(handler4), q)
	dur4 := time.Since(start4)
	if err != nil {
		t.Fatalf("Test 4 failed: %v", err)
	}
	fg4 := countFilterGroups(events4)
	t.Logf("Parallel WITH CSE:       %v (%d results, %d filter group)", dur4, result4.Size(), fg4)

	// Analysis
	t.Log("\n=== Performance Analysis (Large Scale: 8 days, 624 bars) ===")

	seqSpeedup := float64(dur1) / float64(dur2)
	t.Logf("Sequential: CSE speedup = %.2fx (%.1f%% improvement)", seqSpeedup, (seqSpeedup-1.0)*100)

	parSpeedup := float64(dur3) / float64(dur4)
	t.Logf("Parallel:   CSE speedup = %.2fx (%.1f%% improvement)", parSpeedup, (parSpeedup-1.0)*100)

	seqVsParNoCSE := float64(dur1) / float64(dur3)
	t.Logf("Without CSE: Parallel speedup = %.2fx", seqVsParNoCSE)

	seqVsParWithCSE := float64(dur2) / float64(dur4)
	t.Logf("With CSE:    Parallel speedup = %.2fx", seqVsParWithCSE)

	// Find fastest configuration
	fastest := dur1
	fastestName := "Sequential WITHOUT CSE"
	if dur2 < fastest {
		fastest = dur2
		fastestName = "Sequential WITH CSE"
	}
	if dur3 < fastest {
		fastest = dur3
		fastestName = "Parallel WITHOUT CSE"
	}
	if dur4 < fastest {
		fastest = dur4
		fastestName = "Parallel WITH CSE"
	}

	t.Log("\n=== Conclusions ===")
	t.Logf("FASTEST configuration: %s (%v)", fastestName, fastest)

	if seqSpeedup > 1.05 {
		t.Logf("✓ CSE provides significant benefit (%.1f%%) in sequential mode", (seqSpeedup-1.0)*100)
	} else if seqSpeedup > 1.01 {
		t.Logf("~ CSE provides minor benefit (%.1f%%) in sequential mode", (seqSpeedup-1.0)*100)
	} else {
		t.Logf("✗ CSE provides minimal benefit (%.1f%%) in sequential mode", (seqSpeedup-1.0)*100)
	}

	if parSpeedup > 1.05 {
		t.Logf("✓ CSE provides significant benefit (%.1f%%) in parallel mode", (parSpeedup-1.0)*100)
	} else if parSpeedup > 1.01 {
		t.Logf("~ CSE provides minor benefit (%.1f%%) in parallel mode", (parSpeedup-1.0)*100)
	} else {
		t.Logf("✗ CSE provides minimal benefit (%.1f%%) in parallel mode", (parSpeedup-1.0)*100)
	}

	// Verify all results match
	if result1.Size() != result2.Size() || result1.Size() != result3.Size() || result1.Size() != result4.Size() {
		t.Errorf("Result size mismatch: %d, %d, %d, %d", result1.Size(), result2.Size(), result3.Size(), result4.Size())
	}

	// After the decorrelation bug fix: All 4 subqueries are PURE aggregations
	// ([:find (max ?h) (min ?l)], [:find (min ?o)], [:find (max ?c)], [:find (sum ?v)])
	// Pure aggregations should NOT be decorrelated because adding grouping keys breaks them.
	// Therefore, we expect 0 filter groups (no decorrelation occurs).
	// The queries still return correct results, just without the (broken) CSE optimization.
	if fg1 != 0 || fg2 != 0 || fg3 != 0 || fg4 != 0 {
		t.Errorf("Unexpected filter group counts: %d, %d, %d, %d (expected 0,0,0,0 for pure aggregations)", fg1, fg2, fg3, fg4)
	}
}
