package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestParallelVsSequentialDecorrelation compares parallel vs sequential decorrelation
func TestParallelVsSequentialDecorrelation(t *testing.T) {
	// Create test data: 10 hours of OHLC bars
	symbol := datalog.NewIdentity("CRWV")

	loc, _ := time.LoadLocation("America/New_York")
	baseDate := time.Date(2025, 6, 20, 10, 0, 0, 0, loc)

	var datoms []datalog.Datom

	for hour := 0; hour < 10; hour++ {
		hourTime := baseDate.Add(time.Duration(hour) * time.Hour)

		for minute := 0; minute < 60; minute += 5 {
			barTime := hourTime.Add(time.Duration(minute) * time.Minute)
			barID := datalog.NewIdentity("bar-" + barTime.Format("20060102-1504"))

			hourOfDay := 10 + hour
			mod := (hourOfDay * 60) + minute

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

	datoms = append(datoms,
		datalog.Datom{E: symbol, A: datalog.NewKeyword(":symbol/ticker"), V: "CRWV", Tx: 1},
	)

	matcher := NewMemoryPatternMatcher(datoms)

	// OHLC query with 4 subqueries (decorrelates into 2 filter groups)
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
	        $ ?s ?year ?month ?day ?hour) [[?total-volume]]]
	 :order-by [?datetime]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Test 1: Sequential decorrelation
	t.Run("Sequential", func(t *testing.T) {
		execSeq := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: false, // Sequential
			EnableFineGrainedPhases:     true,
			MaxPhases:                   10,
		})

		start := time.Now()
		result, err := execSeq.Execute(q)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Sequential execution failed: %v", err)
		}

		// 10 hours, but query filters to 570-960 mod (7 hours: 10-16)
		if result.Size() != 7 {
			t.Errorf("Expected 7 hours, got %d", result.Size())
		}

		t.Logf("Sequential decorrelation: %v (%d results)", duration, result.Size())
	})

	// Test 2: Parallel decorrelation
	t.Run("Parallel", func(t *testing.T) {
		execPar := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: true, // Parallel
			EnableFineGrainedPhases:     true,
			MaxPhases:                   10,
		})

		start := time.Now()
		result, err := execPar.Execute(q)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Parallel execution failed: %v", err)
		}

		if result.Size() != 7 {
			t.Errorf("Expected 7 hours, got %d", result.Size())
		}

		t.Logf("Parallel decorrelation: %v (%d results)", duration, result.Size())
	})

	// Test 3: Verify results are identical
	t.Run("ResultsMatch", func(t *testing.T) {
		execSeq := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: false,
			EnableFineGrainedPhases:     true,
			MaxPhases:                   10,
		})

		execPar := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: true,
			EnableFineGrainedPhases:     true,
			MaxPhases:                   10,
		})

		resultSeq, err := execSeq.Execute(q)
		if err != nil {
			t.Fatalf("Sequential execution failed: %v", err)
		}

		resultPar, err := execPar.Execute(q)
		if err != nil {
			t.Fatalf("Parallel execution failed: %v", err)
		}

		if resultSeq.Size() != resultPar.Size() {
			t.Errorf("Size mismatch: sequential=%d, parallel=%d", resultSeq.Size(), resultPar.Size())
		}

		// Compare each row
		for i := 0; i < resultSeq.Size(); i++ {
			rowSeq := resultSeq.Get(i)
			rowPar := resultPar.Get(i)

			if len(rowSeq) != len(rowPar) {
				t.Errorf("Row %d length mismatch: sequential=%d, parallel=%d",
					i, len(rowSeq), len(rowPar))
				continue
			}

			for j := range rowSeq {
				if rowSeq[j] != rowPar[j] {
					t.Errorf("Row %d, col %d mismatch: sequential=%v, parallel=%v",
						i, j, rowSeq[j], rowPar[j])
				}
			}
		}
	})
}
