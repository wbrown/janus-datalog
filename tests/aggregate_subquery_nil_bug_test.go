package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestMultipleAggregateSubqueriesNilBug reproduces the critical bug where
// multiple aggregate subqueries return nil values except for the last one.
// This test uses real BadgerDB storage to reproduce the production issue.
func TestMultipleAggregateSubqueriesNilBug(t *testing.T) {
	// Create temporary directory for test database
	dir, err := os.MkdirTemp("", "nilbug-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create database
	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add sample price data for a single day
	tx := db.NewTransaction()

	// Create a symbol
	symbol := datalog.NewIdentity("symbol:test")
	if err := tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), "TEST"); err != nil {
		t.Fatalf("Failed to add symbol: %v", err)
	}

	// Add price bars for 2025-01-15 from 9:30-9:34 AM
	baseTime := time.Date(2025, 1, 15, 9, 30, 0, 0, time.UTC)

	bars := []struct {
		minute int64
		open   float64
		high   float64
		low    float64
		close  float64
		volume int64
	}{
		{570, 100.0, 105.0, 99.0, 103.0, 1000},  // 9:30
		{571, 103.0, 110.0, 102.0, 108.0, 1500}, // 9:31
		{572, 108.0, 115.0, 107.0, 112.0, 2000}, // 9:32
		{573, 112.0, 120.0, 111.0, 118.0, 2500}, // 9:33
		{574, 118.0, 125.0, 117.0, 122.0, 3000}, // 9:34
	}

	for i, bar := range bars {
		barID := datalog.NewIdentity(fmt.Sprintf("bar:%d", i))
		barTime := baseTime.Add(time.Duration(bar.minute-570) * time.Minute)

		tx.Add(barID, datalog.NewKeyword(":price/symbol"), symbol)
		tx.Add(barID, datalog.NewKeyword(":price/time"), barTime)
		tx.Add(barID, datalog.NewKeyword(":price/minute-of-day"), bar.minute)
		tx.Add(barID, datalog.NewKeyword(":price/open"), bar.open)
		tx.Add(barID, datalog.NewKeyword(":price/high"), bar.high)
		tx.Add(barID, datalog.NewKeyword(":price/low"), bar.low)
		tx.Add(barID, datalog.NewKeyword(":price/close"), bar.close)
		tx.Add(barID, datalog.NewKeyword(":price/volume"), bar.volume)
	}

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data was written - check for bars with :price/symbol
	verifyQuery := `[:find ?bar :where [?bar :price/symbol ?s]]`
	vq, verifyErr := parser.ParseQuery(verifyQuery)
	if verifyErr != nil {
		t.Fatalf("Failed to parse verify query: %v", verifyErr)
	}
	verifyMatcher := storage.NewBadgerMatcher(db.Store())
	verifyExec := executor.NewExecutor(verifyMatcher)
	vresult, verifyErr := verifyExec.Execute(vq)
	if verifyErr != nil {
		t.Fatalf("Failed to execute verify query: %v", verifyErr)
	}
	t.Logf("Verify query found %d bars with :price/symbol", vresult.Size())
	if vresult.Size() != 5 {
		t.Fatalf("Expected 5 bars with :price/symbol, got %d", vresult.Size())
	}

	// Now verify we can find the symbol and bars together
	symbolQuery := `[:find ?s ?bar :where [?s :symbol/ticker "TEST"] [?bar :price/symbol ?s]]`
	sq, symbolErr := parser.ParseQuery(symbolQuery)
	if symbolErr != nil {
		t.Fatalf("Failed to parse symbol query: %v", symbolErr)
	}
	symbolMatcher := storage.NewBadgerMatcher(db.Store())
	symbolExec := executor.NewExecutor(symbolMatcher)
	sresult, symbolErr := symbolExec.Execute(sq)
	if symbolErr != nil {
		t.Fatalf("Failed to execute symbol query: %v", symbolErr)
	}
	t.Logf("Symbol query found %d results", sresult.Size())
	if sresult.Size() != 5 {
		t.Fatalf("Expected 5 results from symbol query, got %d", sresult.Size())
	}

	// Test with the morning-bar variable name (matching main query)
	morningQuery := `[:find ?s ?morning-bar :where [?s :symbol/ticker "TEST"] [?morning-bar :price/symbol ?s]]`
	mq, morningErr := parser.ParseQuery(morningQuery)
	if morningErr != nil {
		t.Fatalf("Failed to parse morning query: %v", morningErr)
	}
	morningMatcher := storage.NewBadgerMatcher(db.Store())
	morningExec := executor.NewExecutor(morningMatcher)
	mresult, morningErr := morningExec.Execute(mq)
	if morningErr != nil {
		t.Fatalf("Failed to execute morning query: %v", morningErr)
	}
	t.Logf("Morning query found %d results", mresult.Size())
	if mresult.Size() != 5 {
		t.Fatalf("Expected 5 results from morning query, got %d", mresult.Size())
	}

	// First check what minute-of-day values are stored
	checkMinuteQuery := `[:find ?bar ?mod :where [?bar :price/minute-of-day ?mod]]`
	cmq, checkErr := parser.ParseQuery(checkMinuteQuery)
	if checkErr != nil {
		t.Fatalf("Failed to parse check minute query: %v", checkErr)
	}
	checkMatcher := storage.NewBadgerMatcher(db.Store())
	checkExec := executor.NewExecutor(checkMatcher)
	checkResult, checkErr := checkExec.Execute(cmq)
	if checkErr != nil {
		t.Fatalf("Failed to execute check minute query: %v", checkErr)
	}
	t.Logf("Check minute query found %d results", checkResult.Size())
	for i := 0; i < checkResult.Size(); i++ {
		tuple := checkResult.Get(i)
		t.Logf("  Bar %v has minute-of-day %v (type %T)", tuple[0], tuple[1], tuple[1])
	}

	// Check each bar's attributes separately
	checkHighQuery := `[:find ?bar ?h :where [?bar :price/high ?h]]`
	chq, chErr := parser.ParseQuery(checkHighQuery)
	if chErr != nil {
		t.Fatalf("Failed to parse check high query: %v", chErr)
	}
	chMatcher := storage.NewBadgerMatcher(db.Store())
	chExec := executor.NewExecutor(chMatcher)
	chResult, chErr := chExec.Execute(chq)
	if chErr != nil {
		t.Fatalf("Failed to execute check high query: %v", chErr)
	}
	t.Logf("Check high query found %d results", chResult.Size())

	checkLowQuery := `[:find ?bar ?l :where [?bar :price/low ?l]]`
	clq, clErr := parser.ParseQuery(checkLowQuery)
	if clErr != nil {
		t.Fatalf("Failed to parse check low query: %v", clErr)
	}
	clMatcher := storage.NewBadgerMatcher(db.Store())
	clExec := executor.NewExecutor(clMatcher)
	clResult, clErr := clExec.Execute(clq)
	if clErr != nil {
		t.Fatalf("Failed to execute check low query: %v", clErr)
	}
	t.Logf("Check low query found %d results", clResult.Size())
	for i := 0; i < clResult.Size(); i++ {
		tuple := clResult.Get(i)
		t.Logf("  Low bar %d: %v -> %v", i, tuple[0], tuple[1])
	}

	// Test with just the bound minute-of-day value (no joins)
	simpleBoundQuery := `[:find ?bar :where [?bar :price/minute-of-day 570]]`
	sbq, sbErr := parser.ParseQuery(simpleBoundQuery)
	if sbErr != nil {
		t.Fatalf("Failed to parse simple bound query: %v", sbErr)
	}
	sbMatcher := storage.NewBadgerMatcher(db.Store())
	sbExec := executor.NewExecutor(sbMatcher)
	sbResult, sbErr := sbExec.Execute(sbq)
	if sbErr != nil {
		t.Fatalf("Failed to execute simple bound query: %v", sbErr)
	}
	t.Logf("Simple bound query found %d results", sbResult.Size())
	if sbResult.Size() != 1 {
		t.Fatalf("Expected 1 result from simple bound query, got %d", sbResult.Size())
	}

	// Test if entity joins work at all - join two attributes on same entity
	barJoinQuery := `[:find ?bar :where [?bar :price/high ?h] [?bar :price/low ?l]]`
	bjq, bjErr := parser.ParseQuery(barJoinQuery)
	if bjErr != nil {
		t.Fatalf("Failed to parse bar join query: %v", bjErr)
	}
	bjMatcher := storage.NewBadgerMatcher(db.Store())
	bjExec := executor.NewExecutor(bjMatcher)
	bjResult, bjErr := bjExec.Execute(bjq)
	if bjErr != nil {
		t.Fatalf("Failed to execute bar join query: %v", bjErr)
	}
	t.Logf("Bar join query found %d results", bjResult.Size())
	if bjResult.Size() != 5 {
		// Print what we got for debugging
		for i := 0; i < bjResult.Size(); i++ {
			tuple := bjResult.Get(i)
			t.Logf("  Result %d: bar=%v", i, tuple[0])
		}
		t.Fatalf("Expected 5 results from bar join query, got %d", bjResult.Size())
	}

	// Test bar + minute-of-day join (no symbol)
	barMinuteQuery := `[:find ?bar :where [?bar :price/symbol ?s] [?bar :price/minute-of-day 570]]`
	bmq, bmErr := parser.ParseQuery(barMinuteQuery)
	if bmErr != nil {
		t.Fatalf("Failed to parse bar-minute query: %v", bmErr)
	}
	bmMatcher := storage.NewBadgerMatcher(db.Store())
	bmExec := executor.NewExecutor(bmMatcher)
	bmResult, bmErr := bmExec.Execute(bmq)
	if bmErr != nil {
		t.Fatalf("Failed to execute bar-minute query: %v", bmErr)
	}
	t.Logf("Bar-minute query found %d results", bmResult.Size())
	if bmResult.Size() != 1 {
		t.Fatalf("Expected 1 result from bar-minute query, got %d", bmResult.Size())
	}

	// Test with minute-of-day filter
	minuteQuery := `[:find ?s ?morning-bar
	                 :where [?s :symbol/ticker "TEST"]
	                        [?morning-bar :price/symbol ?s]
	                        [?morning-bar :price/minute-of-day 570]]`
	minq, minuteErr := parser.ParseQuery(minuteQuery)
	if minuteErr != nil {
		t.Fatalf("Failed to parse minute query: %v", minuteErr)
	}
	minuteMatcher := storage.NewBadgerMatcher(db.Store())
	minuteExec := executor.NewExecutor(minuteMatcher)
	minresult, minuteErr := minuteExec.Execute(minq)
	if minuteErr != nil {
		t.Fatalf("Failed to execute minute query: %v", minuteErr)
	}
	t.Logf("Minute query found %d results", minresult.Size())
	if minresult.Size() != 1 {
		t.Fatalf("Expected 1 result from minute query, got %d", minresult.Size())
	}

	// Query with multiple aggregate subqueries (reproduces OHLC pattern)
	queryStr := `
	[:find ?date ?daily-high ?daily-low ?open-price ?close-price ?total-volume
	 :where
	   [?s :symbol/ticker "TEST"]
	   [?morning-bar :price/symbol ?s]
	   [?morning-bar :price/minute-of-day 570]
	   [?morning-bar :price/time ?t]
	   [(year ?t) ?year]
	   [(month ?t) ?month]
	   [(day ?t) ?day]
	   [(str ?year "-" ?month "-" ?day) ?date]

	   ; Subquery 1: High/Low (multi-value binding)
	   [(q [:find (max ?h) (min ?l)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py] [(= ?py ?y)]
	               [(month ?time) ?pm] [(= ?pm ?m)]
	               [(day ?time) ?pd] [(= ?pd ?d)]
	               [?b :price/high ?h]
	               [?b :price/low ?l]]
	       $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]

	   ; Subquery 2: Open price (first 5 minutes)
	   [(q [:find (min ?o)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py] [(= ?py ?y)]
	               [(month ?time) ?pm] [(= ?pm ?m)]
	               [(day ?time) ?pd] [(= ?pd ?d)]
	               [?b :price/minute-of-day ?mod]
	               [(>= ?mod 570)] [(<= ?mod 575)]
	               [?b :price/open ?o]]
	       $ ?s ?year ?month ?day) [[?open-price]]]

	   ; Subquery 3: Close price (last 5 minutes - using 570-575 for test)
	   [(q [:find (max ?c)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py] [(= ?py ?y)]
	               [(month ?time) ?pm] [(= ?pm ?m)]
	               [(day ?time) ?pd] [(= ?pd ?d)]
	               [?b :price/minute-of-day ?mod]
	               [(>= ?mod 570)] [(<= ?mod 575)]
	               [?b :price/close ?c]]
	       $ ?s ?year ?month ?day) [[?close-price]]]

	   ; Subquery 4: Total volume
	   [(q [:find (sum ?v)
	        :in $ ?sym ?y ?m ?d
	        :where [?b :price/symbol ?sym]
	               [?b :price/time ?time]
	               [(year ?time) ?py] [(= ?py ?y)]
	               [(month ?time) ?pm] [(= ?pm ?m)]
	               [(day ?time) ?pd] [(= ?pd ?d)]
	               [?b :price/volume ?v]]
	       $ ?s ?year ?month ?day) [[?total-volume]]]]
	`

	// Parse and execute
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	matcher := storage.NewBadgerMatcher(db.Store())
	exec := executor.NewExecutor(matcher)

	// Enable parallel subquery execution (this triggers the bug)
	exec.EnableParallelSubqueries(4)

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Verify results
	if result.Size() != 1 {
		t.Fatalf("Expected 1 result row, got %d", result.Size())
	}

	tuple := result.Get(0)
	if len(tuple) != 6 {
		t.Fatalf("Expected 6 columns, got %d", len(tuple))
	}

	// Extract values
	date := tuple[0]
	dailyHigh := tuple[1]
	dailyLow := tuple[2]
	openPrice := tuple[3]
	closePrice := tuple[4]
	totalVolume := tuple[5]

	// Log actual values
	t.Logf("Results: date=%v, high=%v, low=%v, open=%v, close=%v, volume=%v",
		date, dailyHigh, dailyLow, openPrice, closePrice, totalVolume)

	// Check date
	if date != "2025-1-15" {
		t.Errorf("Expected date '2025-1-15', got %v", date)
	}

	// CRITICAL BUG CHECKS: These should NOT be nil
	if dailyHigh == nil {
		t.Errorf("BUG REPRODUCED: dailyHigh is nil (expected 125.0)")
	} else if high, ok := dailyHigh.(float64); !ok || high != 125.0 {
		t.Errorf("Expected dailyHigh=125.0, got %v (type %T)", dailyHigh, dailyHigh)
	}

	if dailyLow == nil {
		t.Errorf("BUG REPRODUCED: dailyLow is nil (expected 99.0)")
	} else if low, ok := dailyLow.(float64); !ok || low != 99.0 {
		t.Errorf("Expected dailyLow=99.0, got %v (type %T)", dailyLow, dailyLow)
	}

	if openPrice == nil {
		t.Errorf("BUG REPRODUCED: openPrice is nil (expected 100.0)")
	} else if open, ok := openPrice.(float64); !ok || open != 100.0 {
		t.Errorf("Expected openPrice=100.0, got %v (type %T)", openPrice, openPrice)
	}

	if closePrice == nil {
		t.Errorf("BUG REPRODUCED: closePrice is nil (expected 122.0)")
	} else if close, ok := closePrice.(float64); !ok || close != 122.0 {
		t.Errorf("Expected closePrice=122.0, got %v (type %T)", closePrice, closePrice)
	}

	// Volume (last subquery) should work even with the bug
	if totalVolume == nil {
		t.Errorf("totalVolume is nil (expected 10000)")
	} else if vol, ok := totalVolume.(float64); !ok || vol != 10000.0 {
		t.Errorf("Expected totalVolume=10000, got %v (type %T)", totalVolume, totalVolume)
	}
}
