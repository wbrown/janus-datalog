package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// collectTuples collects all tuples from a relation by iterating
func collectTuples(r executor.Relation) []executor.Tuple {
	var tuples []executor.Tuple
	it := r.Iterator()
	for it.Next() {
		tuples = append(tuples, it.Tuple())
	}
	it.Close()
	return tuples
}

// TestMultipleAggregateSubqueriesNilBug reproduces the critical bug where
// multiple aggregate subqueries return nil values except for the last one.
// The reproduction needs storage-backed resolution rather than an in-memory
// relation, so it runs against every backend the build has.
func TestMultipleAggregateSubqueriesNilBug(t *testing.T) {
	eachBackendAndMode(t, testMultipleAggregateSubqueriesNilBug)
}

func testMultipleAggregateSubqueriesNilBug(t *testing.T, db *storage.Database) {
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
	verifyExec := db.NewExecutor()
	vresult, verifyErr := verifyExec.Execute(vq)
	if verifyErr != nil {
		t.Fatalf("Failed to execute verify query: %v", verifyErr)
	}
	vtuples := collectTuples(vresult)
	t.Logf("Verify query found %d bars with :price/symbol", len(vtuples))
	if len(vtuples) != 5 {
		t.Fatalf("Expected 5 bars with :price/symbol, got %d", len(vtuples))
	}

	// Now verify we can find the symbol and bars together
	symbolQuery := `[:find ?s ?bar :where [?s :symbol/ticker "TEST"] [?bar :price/symbol ?s]]`
	sq, symbolErr := parser.ParseQuery(symbolQuery)
	if symbolErr != nil {
		t.Fatalf("Failed to parse symbol query: %v", symbolErr)
	}
	symbolExec := db.NewExecutor()
	sresult, symbolErr := symbolExec.Execute(sq)
	if symbolErr != nil {
		t.Fatalf("Failed to execute symbol query: %v", symbolErr)
	}
	stuples := collectTuples(sresult)
	t.Logf("Symbol query found %d results", len(stuples))
	if len(stuples) != 5 {
		t.Fatalf("Expected 5 results from symbol query, got %d", len(stuples))
	}

	// Test with the morning-bar variable name (matching main query)
	morningQuery := `[:find ?s ?morning-bar :where [?s :symbol/ticker "TEST"] [?morning-bar :price/symbol ?s]]`
	mq, morningErr := parser.ParseQuery(morningQuery)
	if morningErr != nil {
		t.Fatalf("Failed to parse morning query: %v", morningErr)
	}
	morningExec := db.NewExecutor()
	mresult, morningErr := morningExec.Execute(mq)
	if morningErr != nil {
		t.Fatalf("Failed to execute morning query: %v", morningErr)
	}
	mtuples := collectTuples(mresult)
	t.Logf("Morning query found %d results", len(mtuples))
	if len(mtuples) != 5 {
		t.Fatalf("Expected 5 results from morning query, got %d", len(mtuples))
	}

	// First check what minute-of-day values are stored
	checkMinuteQuery := `[:find ?bar ?mod :where [?bar :price/minute-of-day ?mod]]`
	cmq, checkErr := parser.ParseQuery(checkMinuteQuery)
	if checkErr != nil {
		t.Fatalf("Failed to parse check minute query: %v", checkErr)
	}
	checkExec := db.NewExecutor()
	checkResult, checkErr := checkExec.Execute(cmq)
	if checkErr != nil {
		t.Fatalf("Failed to execute check minute query: %v", checkErr)
	}
	checkTuples := collectTuples(checkResult)
	t.Logf("Check minute query found %d results", len(checkTuples))
	for i, tuple := range checkTuples {
		t.Logf("  Bar %v has minute-of-day %v (type %T)", tuple[0], tuple[1], tuple[1])
		_ = i
	}

	// Check each bar's attributes separately
	checkHighQuery := `[:find ?bar ?h :where [?bar :price/high ?h]]`
	chq, chErr := parser.ParseQuery(checkHighQuery)
	if chErr != nil {
		t.Fatalf("Failed to parse check high query: %v", chErr)
	}
	chExec := db.NewExecutor()
	chResult, chErr := chExec.Execute(chq)
	if chErr != nil {
		t.Fatalf("Failed to execute check high query: %v", chErr)
	}
	chTuples := collectTuples(chResult)
	t.Logf("Check high query found %d results", len(chTuples))

	checkLowQuery := `[:find ?bar ?l :where [?bar :price/low ?l]]`
	clq, clErr := parser.ParseQuery(checkLowQuery)
	if clErr != nil {
		t.Fatalf("Failed to parse check low query: %v", clErr)
	}
	clExec := db.NewExecutor()
	clResult, clErr := clExec.Execute(clq)
	if clErr != nil {
		t.Fatalf("Failed to execute check low query: %v", clErr)
	}
	clTuples := collectTuples(clResult)
	t.Logf("Check low query found %d results", len(clTuples))
	for i, tuple := range clTuples {
		t.Logf("  Low bar %d: %v -> %v", i, tuple[0], tuple[1])
	}

	// Test with just the bound minute-of-day value (no joins)
	simpleBoundQuery := `[:find ?bar :where [?bar :price/minute-of-day 570]]`
	sbq, sbErr := parser.ParseQuery(simpleBoundQuery)
	if sbErr != nil {
		t.Fatalf("Failed to parse simple bound query: %v", sbErr)
	}
	sbExec := db.NewExecutor()
	sbResult, sbErr := sbExec.Execute(sbq)
	if sbErr != nil {
		t.Fatalf("Failed to execute simple bound query: %v", sbErr)
	}
	sbTuples := collectTuples(sbResult)
	t.Logf("Simple bound query found %d results", len(sbTuples))
	if len(sbTuples) != 1 {
		t.Fatalf("Expected 1 result from simple bound query, got %d", len(sbTuples))
	}

	// Test if entity joins work at all - join two attributes on same entity
	barJoinQuery := `[:find ?bar :where [?bar :price/high ?h] [?bar :price/low ?l]]`
	bjq, bjErr := parser.ParseQuery(barJoinQuery)
	if bjErr != nil {
		t.Fatalf("Failed to parse bar join query: %v", bjErr)
	}
	bjExec := db.NewExecutor()
	bjResult, bjErr := bjExec.Execute(bjq)
	if bjErr != nil {
		t.Fatalf("Failed to execute bar join query: %v", bjErr)
	}
	bjTuples := collectTuples(bjResult)
	t.Logf("Bar join query found %d results", len(bjTuples))
	if len(bjTuples) != 5 {
		for i, tuple := range bjTuples {
			t.Logf("  Result %d: bar=%v", i, tuple[0])
		}
		t.Fatalf("Expected 5 results from bar join query, got %d", len(bjTuples))
	}

	// Test bar + minute-of-day join (no symbol)
	barMinuteQuery := `[:find ?bar :where [?bar :price/symbol ?s] [?bar :price/minute-of-day 570]]`
	bmq, bmErr := parser.ParseQuery(barMinuteQuery)
	if bmErr != nil {
		t.Fatalf("Failed to parse bar-minute query: %v", bmErr)
	}
	bmExec := db.NewExecutor()
	bmResult, bmErr := bmExec.Execute(bmq)
	if bmErr != nil {
		t.Fatalf("Failed to execute bar-minute query: %v", bmErr)
	}
	bmTuples := collectTuples(bmResult)
	t.Logf("Bar-minute query found %d results", len(bmTuples))
	if len(bmTuples) != 1 {
		t.Fatalf("Expected 1 result from bar-minute query, got %d", len(bmTuples))
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
	minuteExec := db.NewExecutor()
	minresult, minuteErr := minuteExec.Execute(minq)
	if minuteErr != nil {
		t.Fatalf("Failed to execute minute query: %v", minuteErr)
	}
	minTuples := collectTuples(minresult)
	t.Logf("Minute query found %d results", len(minTuples))
	if len(minTuples) != 1 {
		t.Fatalf("Expected 1 result from minute query, got %d", len(minTuples))
	}

	// Main test: query with multiple aggregate subqueries
	mainQuery := `[:find ?s ?morning-bar ?open-price ?high-price ?low-price
			              :where [?s :symbol/ticker "TEST"]
			                     [?morning-bar :price/symbol ?s]
			                     [?morning-bar :price/minute-of-day 570]
			                     [(q [:find (min ?o) :in $ ?bar :where [?bar :price/open ?o]] $ ?morning-bar) [[?open-price]]]
			                     [(q [:find (max ?h) :in $ ?bar :where [?bar :price/high ?h]] $ ?morning-bar) [[?high-price]]]
			                     [(q [:find (min ?l) :in $ ?bar :where [?bar :price/low ?l]] $ ?morning-bar) [[?low-price]]]]`

	q, parseErr := parser.ParseQuery(mainQuery)
	if parseErr != nil {
		t.Fatalf("Failed to parse main query: %v", parseErr)
	}

	exec := db.NewExecutor()
	result, execErr := exec.Execute(q)
	if execErr != nil {
		t.Fatalf("Failed to execute main query: %v", execErr)
	}

	resultTuples := collectTuples(result)
	t.Logf("Main query result: %d tuples", len(resultTuples))

	if len(resultTuples) != 1 {
		t.Fatalf("Expected 1 result tuple, got %d", len(resultTuples))
	}

	tuple := resultTuples[0]
	t.Logf("Result tuple: %v", tuple)

	// Check that all aggregate values are non-nil
	if len(tuple) != 5 {
		t.Fatalf("Expected 5 symbols in result, got %d", len(tuple))
	}

	openPrice := tuple[2]
	highPrice := tuple[3]
	lowPrice := tuple[4]

	t.Logf("Open price: %v (type: %T)", openPrice, openPrice)
	t.Logf("High price: %v (type: %T)", highPrice, highPrice)
	t.Logf("Low price: %v (type: %T)", lowPrice, lowPrice)

	// THE BUG: First aggregate values come back as nil
	if openPrice == nil {
		t.Errorf("BUG: open-price is nil (expected 100.0)")
	}
	if highPrice == nil {
		t.Errorf("BUG: high-price is nil (expected 105.0)")
	}
	if lowPrice == nil {
		t.Errorf("BUG: low-price is nil (expected 99.0)")
	}

	// Verify actual values
	if openPrice != nil {
		if op, ok := openPrice.(float64); !ok || op != 100.0 {
			t.Errorf("Expected open-price=100.0, got %v", openPrice)
		}
	}
	if highPrice != nil {
		if hp, ok := highPrice.(float64); !ok || hp != 105.0 {
			t.Errorf("Expected high-price=105.0, got %v", highPrice)
		}
	}
	if lowPrice != nil {
		if lp, ok := lowPrice.(float64); !ok || lp != 99.0 {
			t.Errorf("Expected low-price=99.0, got %v", lowPrice)
		}
	}
}
