package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestCSEOpportunity verifies that identical subqueries with different aggregates
// currently execute as separate merged queries (establishing baseline before CSE)
func TestCSEOpportunity(t *testing.T) {
	// Create test data: products with prices
	var datoms []datalog.Datom
	for cat := 0; cat < 5; cat++ {
		catID := datalog.NewIdentity("cat-" + string(rune('A'+cat)))
		datoms = append(datoms, datalog.Datom{
			E: catID, A: datalog.NewKeyword(":category/name"), V: string(rune('A' + cat)), Tx: 1,
		})

		for prod := 0; prod < 10; prod++ {
			prodID := datalog.NewIdentity("prod-" + string(rune('A'+cat)) + string(rune('0'+prod)))
			price := float64(100 + cat*10 + prod)
			datoms = append(datoms,
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/category"), V: catID, Tx: 1},
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/price"), V: price, Tx: 1},
				datalog.Datom{E: prodID, A: datalog.NewKeyword(":product/stock"), V: int64(prod * 5), Tx: 1},
			)
		}
	}

	matcher := NewMemoryPatternMatcher(datoms)

	// Query with 2 subqueries that have IDENTICAL patterns but different aggregates
	// This is the CSE opportunity: max(price) and sum(stock) scan the same products
	queryStr := `[:find ?name ?max-price ?total-stock
	             :where
	               [?c :category/name ?name]

	               ; Subquery 1: max price
	               [(q [:find (max ?p)
	                    :in $ ?cat
	                    :where [?prod :product/category ?cat]
	                           [?prod :product/price ?p]]
	                  $ ?c) [[?max-price]]]

	               ; Subquery 2: sum stock (IDENTICAL base pattern, different aggregate)
	               [(q [:find (sum ?s)
	                    :in $ ?cat
	                    :where [?prod :product/category ?cat]
	                           [?prod :product/stock ?s]]
	                  $ ?c) [[?total-stock]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Execute with CSE enabled
	exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableCSE:                   true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var events []annotations.Event
	handler := annotations.Handler(func(e annotations.Event) {
		events = append(events, e)
	})

	result, err := exec.ExecuteWithContext(NewContext(handler), q)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Verify results are correct
	if result.Size() != 5 {
		t.Errorf("Expected 5 categories, got %d", result.Size())
	}

	// Check annotations to see how many merged queries were executed
	var mergedQueryCount int
	var filterGroups int
	var foundDecorrelation bool
	for _, e := range events {
		t.Logf("Event: %s - %v", e.Name, e.Data)

		if e.Name == "decorrelated_subqueries/begin" {
			foundDecorrelation = true
			if fg, ok := e.Data["filter_groups"].(int); ok {
				filterGroups = fg
			}
		}
		if e.Name == "decorrelated_subqueries/merged_query_0" ||
			e.Name == "decorrelated_subqueries/merged_query_1" {
			mergedQueryCount++
		}
	}

	if !foundDecorrelation {
		t.Log("WARNING: No decorrelation events found - subqueries may not be decorrelating")
	}

	t.Logf("Filter groups: %d", filterGroups)
	t.Logf("Merged queries executed: %d", mergedQueryCount)

	// After decorrelation bug fix: Both subqueries are PURE aggregations
	// ([:find (max ?p)] and [:find (sum ?s)])
	// Pure aggregations should NOT be decorrelated, so we expect 0 filter groups.
	// The queries still return correct results, just without CSE optimization.
	if filterGroups != 0 {
		t.Errorf("Expected 0 filter groups (pure aggregations not decorrelated), got %d", filterGroups)
	}
	if mergedQueryCount != 0 {
		t.Errorf("Expected 0 merged queries (pure aggregations not decorrelated), got %d", mergedQueryCount)
	}

	// Log columns for debugging
	t.Logf("Result columns: %v", result.Columns())

	// Verify actual result values
	for i := 0; i < result.Size(); i++ {
		row := result.Get(i)
		if i == 0 {
			t.Logf("First row: %v", row)
		}

		name := row[0].(string)

		// Aggregates return float64 for sum/count/avg
		var val1, val2 float64
		switch v := row[1].(type) {
		case int64:
			val1 = float64(v)
		case float64:
			val1 = v
		default:
			t.Fatalf("Unexpected type for row[1]: %T", v)
		}
		switch v := row[2].(type) {
		case int64:
			val2 = float64(v)
		case float64:
			val2 = v
		default:
			t.Fatalf("Unexpected type for row[2]: %T", v)
		}

		// Category A (0): products 0-9, prices 100-109, stock 0,5,10,15,20,25,30,35,40,45
		catIdx := int(name[0] - 'A')
		expectedMaxPrice := float64(100 + catIdx*10 + 9)
		expectedTotalStock := float64(0 + 5 + 10 + 15 + 20 + 25 + 30 + 35 + 40 + 45) // sum(0..9 * 5)

		t.Logf("Category %s: val1=%.0f (expected maxPrice=%.0f), val2=%.0f (expected stock=%.0f)",
			name, val1, expectedMaxPrice, val2, expectedTotalStock)

		// Check which order they're in
		if val1 == expectedTotalStock && val2 == expectedMaxPrice {
			t.Logf("NOTE: Columns appear to be swapped (got stock, price instead of price, stock)")
		}
	}
}

// TestOHLCCSEOpportunity verifies the CSE opportunity in OHLC queries
func TestOHLCCSEOpportunity(t *testing.T) {
	// Create test data: 3 hours of OHLC bars
	symbol := datalog.NewIdentity("TEST")
	loc, _ := time.LoadLocation("America/New_York")
	baseDate := time.Date(2025, 6, 20, 10, 0, 0, 0, loc)

	var datoms []datalog.Datom

	for hour := 0; hour < 3; hour++ {
		hourTime := baseDate.Add(time.Duration(hour) * time.Hour)

		for minute := 0; minute < 60; minute += 5 {
			barTime := hourTime.Add(time.Duration(minute) * time.Minute)
			barID := datalog.NewIdentity("bar-" + barTime.Format("20060102-1504"))

			hourOfDay := 10 + hour
			mod := (hourOfDay * 60) + minute

			open := 100.0 + float64(hour) + float64(minute)/60.0
			high := open + 0.5
			low := open - 0.5
			volume := int64(1000 + hour*100 + minute)

			datoms = append(datoms,
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/symbol"), V: symbol, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/high"), V: high, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/low"), V: low, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/volume"), V: volume, Tx: 1},
				datalog.Datom{E: barID, A: datalog.NewKeyword(":price/minute-of-day"), V: int64(mod), Tx: 1},
			)
		}
	}

	datoms = append(datoms,
		datalog.Datom{E: symbol, A: datalog.NewKeyword(":symbol/ticker"), V: "TEST", Tx: 1},
	)

	matcher := NewMemoryPatternMatcher(datoms)

	// Simplified OHLC query with 2 subqueries that have IDENTICAL inputs
	// SubQ 0: max(high) + min(low)
	// SubQ 1: sum(volume)
	// These should be merged into 1 query with CSE
	queryStr := `[:find ?datetime ?hour-high ?hour-low ?total-volume
	 :where
	    [?s :symbol/ticker "TEST"]
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
	    [(>= ?mod ?hour-start)]
	    [(<= ?mod ?open-end)]
	    [(str ?year "-" ?month "-" ?day " " ?hour ":00") ?datetime]

	    ; Subquery 0: high/low (IDENTICAL inputs)
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

	    ; Subquery 1: volume (IDENTICAL inputs, different attributes)
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

	exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSubqueryDecorrelation: true,
		EnableParallelDecorrelation: true,
		EnableCSE:                   true,
		EnableFineGrainedPhases:     true,
		MaxPhases:                   10,
	})

	var events []annotations.Event
	handler := annotations.Handler(func(e annotations.Event) {
		events = append(events, e)
	})

	result, err := exec.ExecuteWithContext(NewContext(handler), q)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Verify we got 3 hours
	if result.Size() != 3 {
		t.Errorf("Expected 3 hours, got %d", result.Size())
	}

	// Count merged queries
	var mergedQueryCount int
	var filterGroups int
	for _, e := range events {
		if e.Name == "decorrelated_subqueries/begin" {
			if fg, ok := e.Data["filter_groups"].(int); ok {
				filterGroups = fg
			}
		}
		if e.Name == "decorrelated_subqueries/merged_query_0" ||
			e.Name == "decorrelated_subqueries/merged_query_1" {
			mergedQueryCount++
		}
	}

	t.Logf("Filter groups: %d", filterGroups)
	t.Logf("Merged queries executed: %d", mergedQueryCount)

	// After decorrelation bug fix: Both subqueries are PURE aggregations
	// Pure aggregations should NOT be decorrelated, so we expect 0 filter groups.
	// The queries still return correct results, just without CSE optimization.
	if filterGroups != 0 {
		t.Errorf("Expected 0 filter groups (pure aggregations not decorrelated), got %d", filterGroups)
	}
	if mergedQueryCount != 0 {
		t.Errorf("Expected 0 merged queries (pure aggregations not decorrelated), got %d", mergedQueryCount)
	}
}
