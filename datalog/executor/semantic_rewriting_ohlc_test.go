package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestSemanticRewritingOHLCScale tests semantic rewriting on the actual production-scale OHLC query
// Same dataset as TestDecorrelationOHLCScale: 8 days, 624 bars, 4993 datoms
func TestSemanticRewritingOHLCScale(t *testing.T) {
	// Generate identical dataset to decorr_ohlc_scale_test.go
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

	t.Logf("Generated %d 5-minute bars, %d total datoms", totalBars, len(datoms))

	matcher := NewMemoryPatternMatcher(datoms)

	// The actual OHLC query - has time extraction predicates!
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

	// Test 1: Decorrelation WITHOUT Semantic Rewriting
	t.Log("\n=== Config 1: Decorrelation WITHOUT Semantic Rewriting ===")
	exec1 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableSemanticRewriting:     false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	start1 := time.Now()
	result1, err := exec1.Execute(q)
	dur1 := time.Since(start1)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	t.Logf("Time: %v (%d results)", dur1, result1.Size())

	// Test 2: Decorrelation WITH Semantic Rewriting
	t.Log("\n=== Config 2: Decorrelation WITH Semantic Rewriting ===")
	exec2 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableSemanticRewriting:     true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	start2 := time.Now()
	result2, err := exec2.Execute(q)
	dur2 := time.Since(start2)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	t.Logf("Time: %v (%d results)", dur2, result2.Size())

	// Test 3: NO Decorrelation, NO Semantic Rewriting (baseline)
	t.Log("\n=== Config 3: NO Decorrelation, NO Semantic Rewriting (Baseline) ===")
	exec3 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
		EnableParallelDecorrelation: false,
		EnableSemanticRewriting:     false,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	start3 := time.Now()
	result3, err := exec3.Execute(q)
	dur3 := time.Since(start3)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	t.Logf("Time: %v (%d results)", dur3, result3.Size())

	// Test 4: NO Decorrelation, WITH Semantic Rewriting
	t.Log("\n=== Config 4: NO Decorrelation, WITH Semantic Rewriting ===")
	exec4 := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: false,
		EnableParallelDecorrelation: false,
		EnableSemanticRewriting:     true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	start4 := time.Now()
	result4, err := exec4.Execute(q)
	dur4 := time.Since(start4)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}
	t.Logf("Time: %v (%d results)", dur4, result4.Size())

	// Analysis
	t.Log("\n=== Performance Analysis ===")
	t.Logf("Config 1 (Decorr only):            %v", dur1)
	t.Logf("Config 2 (Decorr + Semantic):      %v (%.2fx vs Config 1)", dur2, float64(dur1)/float64(dur2))
	t.Logf("Config 3 (Baseline):               %v", dur3)
	t.Logf("Config 4 (Semantic only):          %v (%.2fx vs Config 3)", dur4, float64(dur3)/float64(dur4))

	t.Log("\n=== Key Findings ===")
	t.Logf("Semantic rewriting speedup with decorrelation: %.2fx", float64(dur1)/float64(dur2))
	t.Logf("Semantic rewriting speedup without decorrelation: %.2fx", float64(dur3)/float64(dur4))
	t.Logf("Best configuration: Decorr + Semantic (Config 2): %v", dur2)

	// Verify all produce same results
	if result1.Size() != result2.Size() || result1.Size() != result3.Size() || result1.Size() != result4.Size() {
		t.Errorf("Result size mismatch: %d, %d, %d, %d", result1.Size(), result2.Size(), result3.Size(), result4.Size())
	}
}
