package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// BenchmarkParallelDecorrelation compares parallel vs sequential merged query execution
func BenchmarkParallelDecorrelation(b *testing.B) {
	// Create test data: 100 categories with 10 products each
	var datoms []datalog.Datom
	for cat := 0; cat < 100; cat++ {
		catID := datalog.NewIdentity("cat-" + string(rune('A'+cat)))
		datoms = append(datoms, datalog.Datom{
			E: catID, A: datalog.NewKeyword(":category/name"), V: string(rune('A' + cat)), Tx: 1,
		})

		for prod := 0; prod < 10; prod++ {
			prodID := datalog.NewIdentity("prod-" + string(rune('A'+cat)) + string(rune('0'+prod)))
			price := float64(100 + cat + prod)
			datoms = append(datoms,
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/category"), V: catID, Tx: 1},
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/price"), V: price, Tx: 1},
			)
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query with 2 subqueries that decorrelate into 2 merged queries
	queryStr := `[:find ?name ?max-price ?count
	             :where
	               [?c :category/name ?name]
	               [(q [:find (max ?p) :in $ ?cat
	                    :where [?prod :product/category ?cat]
	                           [?prod :product/price ?p]]
	                  $ ?c) [[?max-price]]]
	               [(q [:find (count ?prod) :in $ ?cat
	                    :where [?prod :product/category ?cat]]
	                  $ ?c) [[?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		b.Fatalf("Parse error: %v", err)
	}

	// Run benchmark - current implementation uses parallel execution
	exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(q)
		if err != nil {
			b.Fatalf("Execution failed: %v", err)
		}
		if result.Size() != 100 {
			b.Fatalf("Expected 100 results, got %d", result.Size())
		}
	}
}

// BenchmarkParallelOHLCDecorrelation tests parallel execution on OHLC query
func BenchmarkParallelOHLCDecorrelation(b *testing.B) {
	// Create test data: 10 hours of price bars
	// Using simple hour offsets to avoid minute-of-day calculation issues
	symbol := datalog.NewIdentity("CRWV")

	loc, _ := time.LoadLocation("America/New_York")
	baseDate := time.Date(2025, 6, 20, 10, 0, 0, 0, loc)

	var datoms []datalog.Datom

	for hour := 0; hour < 10; hour++ {
		hourTime := baseDate.Add(time.Duration(hour) * time.Hour)

		for minute := 0; minute < 60; minute += 5 {
			barTime := hourTime.Add(time.Duration(minute) * time.Minute)
			barID := datalog.NewIdentity("bar-" + barTime.Format("20060102-1504"))

			// minute-of-day calculation: hour 10 + offset hours
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
		b.Fatalf("Failed to parse query: %v", err)
	}

	exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(q)
		if err != nil {
			b.Fatalf("Query execution failed: %v", err)
		}
		// Query filters to 570-960 minute-of-day (9:30 AM - 4:00 PM)
		// Hours 10-16 are included (7 hours total)
		if result.Size() != 7 {
			b.Fatalf("Expected 7 hours, got %d", result.Size())
		}
	}
}
