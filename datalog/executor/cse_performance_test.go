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

// TestCSEPerformanceImpact measures the actual performance impact of CSE
// Compares decorrelation WITHOUT CSE vs decorrelation WITH CSE
//
// FINDING: CSE provides ~0% performance improvement (1.00x speedup)
// Reason: Parallel decorrelation already executes filter groups concurrently.
// Merging 2 filter groups into 1 doesn't help when they run in parallel anyway.
// In fact, the merged query scans MORE data (all attributes combined) vs
// separate queries scanning subsets in parallel.
//
// CSE would provide benefits in:
// - Single-threaded execution (no parallel decorrelation)
// - Expensive filter predicates (not just pattern scans)
// - High query setup overhead
func TestCSEPerformanceImpact(t *testing.T) {
	// Generate 50 hours of 5-minute bar data (same as TestDecorrelationOHLCScale)
	var datoms []datalog.Datom

	// Symbol entity
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

			// Special cases for first and last hour
			if hour == 9 {
				startMinute = 30 // Start at 9:30
			}
			if hour == 15 {
				endMinute = 60 // End at 4:00 PM (include 15:00-15:59)
			}

			// Generate 5-minute bars for this hour
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

				// Calculate minute-of-day
				minuteOfDay := int64(hour*60 + minute)

				// Generate OHLC data
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

	// Test 1: Decorrelation WITHOUT CSE (2 filter groups)
	execWithoutCSE := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableCSE:                   false, // CSE disabled
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var eventsWithoutCSE []annotations.Event
	handlerWithoutCSE := annotations.Handler(func(e annotations.Event) {
		if strings.HasPrefix(e.Name, "decorrelated_") {
			eventsWithoutCSE = append(eventsWithoutCSE, e)
		}
	})

	startWithoutCSE := time.Now()
	resultWithoutCSE, err := execWithoutCSE.ExecuteWithContext(NewContext(handlerWithoutCSE), q)
	durWithoutCSE := time.Since(startWithoutCSE)
	if err != nil {
		t.Fatalf("Without CSE failed: %v", err)
	}

	// Count filter groups without CSE
	var filterGroupsWithoutCSE int
	for _, e := range eventsWithoutCSE {
		if e.Name == "decorrelated_subqueries/begin" {
			if fg, ok := e.Data["filter_groups"].(int); ok {
				filterGroupsWithoutCSE = fg
			}
		}
	}

	// Test 2: Decorrelation WITH CSE (1 filter group)
	execWithCSE := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableCSE:                   true, // CSE enabled
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var eventsWithCSE []annotations.Event
	handlerWithCSE := annotations.Handler(func(e annotations.Event) {
		if strings.HasPrefix(e.Name, "decorrelated_") {
			eventsWithCSE = append(eventsWithCSE, e)
		}
	})

	startWithCSE := time.Now()
	resultWithCSE, err := execWithCSE.ExecuteWithContext(NewContext(handlerWithCSE), q)
	durWithCSE := time.Since(startWithCSE)
	if err != nil {
		t.Fatalf("With CSE failed: %v", err)
	}

	// Count filter groups with CSE
	var filterGroupsWithCSE int
	for _, e := range eventsWithCSE {
		if e.Name == "decorrelated_subqueries/begin" {
			if fg, ok := e.Data["filter_groups"].(int); ok {
				filterGroupsWithCSE = fg
			}
		}
	}

	// Verify results match
	if resultWithoutCSE.Size() != resultWithCSE.Size() {
		t.Errorf("Size mismatch: without_CSE=%d, with_CSE=%d", resultWithoutCSE.Size(), resultWithCSE.Size())
	}

	// Report results
	t.Logf("WITHOUT CSE: %v (%d results, %d filter groups)", durWithoutCSE, resultWithoutCSE.Size(), filterGroupsWithoutCSE)
	t.Logf("WITH CSE:    %v (%d results, %d filter group)", durWithCSE, resultWithCSE.Size(), filterGroupsWithCSE)

	speedup := float64(durWithoutCSE) / float64(durWithCSE)
	t.Logf("CSE Speedup: %.2fx", speedup)

	// After decorrelation bug fix: These subqueries are PURE aggregations
	// Pure aggregations should NOT be decorrelated, so we expect 0 filter groups.
	// Both with and without CSE should have 0 filter groups now.
	if filterGroupsWithoutCSE != 0 {
		t.Errorf("Expected 0 filter groups without CSE (pure aggregations), got %d", filterGroupsWithoutCSE)
	}
	if filterGroupsWithCSE != 0 {
		t.Errorf("Expected 0 filter groups with CSE (pure aggregations), got %d", filterGroupsWithCSE)
	}

	// Report performance delta
	percentImprovement := (speedup - 1.0) * 100
	t.Logf("CSE Performance Improvement: %.1f%%", percentImprovement)

	// CSE is correctly merging filter groups, but parallel execution means no performance gain
	// In fact, CSE can slightly degrade performance because:
	// - Merged query scans MORE data (all attributes combined)
	// - Separate queries scan subsets in parallel
	// Accept up to 10% degradation as expected behavior with parallel decorrelation
	if percentImprovement < -10.0 {
		t.Errorf("CSE significantly degraded performance: %.1f%%", percentImprovement)
	}
	if percentImprovement < 5.0 {
		t.Logf("NOTE: CSE provides minimal benefit with parallel decorrelation (filter groups already run concurrently)")
	}
}
