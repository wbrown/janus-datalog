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

// TestDecorrelationOHLCScale tests decorrelation on a realistic OHLC query
// with 50 hours of 5-minute bar data (~600 bars total)
func TestDecorrelationOHLCScale(t *testing.T) {
	// Generate 50 hours of 5-minute bar data
	// Trading hours: 9:30 AM (570 minutes) to 4:00 PM (960 minutes) = 6.5 hours/day
	// 50 hours / 6.5 = ~8 trading days
	// Each hour has 12 5-minute bars

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
	t.Logf("Total datoms: %d", len(datoms))

	matcher := NewMemoryPatternMatcher(datoms)

	// The actual OHLC query from gopher-street
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

	// Create planner and look at the plan (WITH decorrelation enabled)
	opts := planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	}
	p := planner.NewPlanner(nil, opts)
	plan, err := p.Plan(q)
	if err != nil {
		t.Fatalf("Planning failed: %v", err)
	}

	t.Logf("Query plan phases: %d", len(plan.Phases))
	for i, phase := range plan.Phases {
		t.Logf("Phase %d: %d subqueries", i, len(phase.Subqueries))
		for j, subq := range phase.Subqueries {
			t.Logf("  SubQ %d: inputs=%v, decorrelated=%v", j, subq.Inputs, subq.Decorrelated)
		}

		// Show decorrelation analysis metadata
		if phase.Metadata != nil {
			if analysis, ok := phase.Metadata["decorrelation_analysis"].(map[string]interface{}); ok {
				t.Logf("  Decorrelation analysis:")
				t.Logf("    Total subqueries: %v", analysis["total_subqueries"])
				t.Logf("    Signature groups: %v", analysis["signature_groups"])
				if sigs, ok := analysis["signatures"].(map[string][]int); ok {
					for sig, indices := range sigs {
						t.Logf("    Signature '%s': subqueries %v (count=%d)", sig, indices, len(indices))
					}
				}
				if errors, ok := analysis["errors"].(map[string]string); ok {
					t.Logf("  Decorrelation errors:")
					for sig, errMsg := range errors {
						t.Logf("    Signature '%s': %s", sig, errMsg)
					}
				}
			}
		}

		t.Logf("  Decorrelated plans: %d", len(phase.DecorrelatedSubqueries))
		for j, decor := range phase.DecorrelatedSubqueries {
			t.Logf("  Decorrelated group %d: signature=%s, subqueries=%v",
				j, decor.SignatureHash, decor.OriginalSubqueries)
		}
	}

	// Test WITHOUT decorrelation
	execNoDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	startNo := time.Now()
	resultNo, err := execNoDecor.Execute(q)
	durNo := time.Since(startNo)
	if err != nil {
		t.Fatalf("No decorrelation failed: %v", err)
	}

	// Test WITH decorrelation (parallel by default)
	execWithDecor := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	// Capture annotations to see what's happening
	var decorEvents []annotations.Event
	handler := annotations.Handler(func(e annotations.Event) {
		if strings.HasPrefix(e.Name, "decorrelated_") {
			decorEvents = append(decorEvents, e)
		}
	})

	startWith := time.Now()
	resultWith, err := execWithDecor.ExecuteWithContext(NewContext(handler), q)
	durWith := time.Since(startWith)
	if err != nil {
		t.Fatalf("With decorrelation failed: %v", err)
	}

	// Log decorrelation events
	if len(decorEvents) > 0 {
		t.Logf("Decorrelation events:")
		for _, event := range decorEvents {
			if cols, ok := event.Data["result_columns"]; ok {
				t.Logf("  %s: size=%v, columns=%v", event.Name, event.Data["result_size"], cols)
			} else if cols, ok := event.Data["combined_columns"]; ok {
				t.Logf("  %s: size=%v, columns=%v", event.Name, event.Data["combined_size"], cols)
			} else if cols, ok := event.Data["joined_columns"]; ok {
				t.Logf("  %s: size=%v, columns=%v", event.Name, event.Data["joined_size"], cols)
			} else {
				t.Logf("  %s: %v", event.Name, event.Data)
			}
		}
	} else {
		t.Logf("WARNING: No decorrelation events detected!")
	}

	// Verify results match
	if resultNo.Size() != resultWith.Size() {
		t.Errorf("Size mismatch: no_decorr=%d, with_decorr=%d", resultNo.Size(), resultWith.Size())
	}

	// Expected: ~50 hours of results
	expectedHours := 50
	if resultNo.Size() < expectedHours-5 || resultNo.Size() > expectedHours+5 {
		t.Logf("Warning: Expected ~%d hours, got %d", expectedHours, resultNo.Size())
	}

	t.Logf("WITHOUT decorrelation: %v (%d results)", durNo, resultNo.Size())
	t.Logf("WITH decorrelation: %v (%d results)", durWith, resultWith.Size())

	speedup := float64(durNo) / float64(durWith)
	t.Logf("Speedup: %.2fx", speedup)

	// With 4 subqueries and 2 decorrelating:
	// - Without: 50 hours × 4 subqueries = 200 sequential executions
	// - With: 50 hours × 2 sequential + 1 merged (computing 2 aggregates) = 101 executions
	// Expected speedup: ~1.5x to 2x (theoretical 200/101 = 1.98x, but overhead reduces it)
	if speedup < 1.3 {
		t.Logf("Warning: Expected at least 1.3x speedup with 2/4 subqueries decorrelated, got %.2fx", speedup)
	}
}
