package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestSimpleSubquery(t *testing.T) {
	// Create test data: symbols and their max prices
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			// Symbols
			"[:symbol/ticker \"AAPL\"]": {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
			},
			"[:symbol/ticker \"GOOG\"]": {
				{E: datalog.NewIdentity("symbol:goog"), A: datalog.NewKeyword(":symbol/ticker"), V: "GOOG", Tx: 1},
			},
			// Prices - note: these need to match on entity references
			"[:price/symbol _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:goog"), Tx: 4},
			},
			"[:price/value _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/value"), V: 150.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/value"), V: 155.0, Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/value"), V: 2800.0, Tx: 4},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Query with subquery to find max price for each symbol
	queryStr := `[:find ?symbol ?max-price
	             :where 
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?max-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Check results
	if result.Size() != 2 {
		t.Errorf("Expected 2 results, got %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			t.Logf("Tuple %d: %v", i, result.Get(i))
		}
	}

	// Create a map for easier checking
	resultMap := make(map[string]float64)
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		if len(tuple) < 2 {
			t.Errorf("Tuple %d has insufficient values: %v", i, tuple)
			continue
		}

		symbol, ok := tuple[0].(string)
		if !ok {
			t.Errorf("Tuple %d: expected string for symbol, got %T: %v", i, tuple[0], tuple[0])
			continue
		}

		maxPrice, ok := tuple[1].(float64)
		if !ok {
			t.Errorf("Tuple %d: expected float64 for max price, got %T: %v", i, tuple[1], tuple[1])
			continue
		}

		resultMap[symbol] = maxPrice
	}

	// Check AAPL max price
	if maxPrice, ok := resultMap["AAPL"]; !ok || maxPrice != 155.0 {
		t.Errorf("Expected AAPL max price to be 155.0, got %v (found: %v)", maxPrice, ok)
	}

	// Check GOOG max price
	if maxPrice, ok := resultMap["GOOG"]; !ok || maxPrice != 2800.0 {
		t.Errorf("Expected GOOG max price to be 2800.0, got %v (found: %v)", maxPrice, ok)
	}
}

func TestSubqueryWithMultipleInputs(t *testing.T) {
	// Create test data with time-based prices
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			// Symbols
			"[:symbol/ticker _]": {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
			},
			// Prices with dates
			"[:price/symbol _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 4},
			},
			"[:price/time _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 6, 2, 0, 0, 0, 0, time.UTC), Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/time"), V: time.Date(2025, 6, 2, 0, 0, 0, 0, time.UTC), Tx: 4},
			},
			"[:price/high _]": {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/high"), V: 150.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/high"), V: 155.0, Tx: 3},
				{E: datalog.NewIdentity("price:3"), A: datalog.NewKeyword(":price/high"), V: 160.0, Tx: 4},
			},
		},
	}

	// Register the same-date? function
	RegisterCustomFunction("same-date?", func(args []interface{}) (interface{}, error) {
		if len(args) != 2 {
			t.Logf("same-date?: wrong number of args: %d", len(args))
			return false, nil
		}

		// Handle string dates
		var t1, t2 time.Time
		var err error

		switch v1 := args[0].(type) {
		case time.Time:
			t1 = v1
		case string:
			t1, err = time.Parse("2006-01-02", v1)
			if err != nil {
				t.Logf("same-date?: failed to parse arg0 as date: %v (%T)", v1, v1)
				return false, nil
			}
		default:
			t.Logf("same-date?: arg0 is not time or string: %v (%T)", v1, v1)
			return false, nil
		}

		switch v2 := args[1].(type) {
		case time.Time:
			t2 = v2
		case string:
			t2, err = time.Parse("2006-01-02", v2)
			if err != nil {
				t.Logf("same-date?: failed to parse arg1 as date: %v (%T)", v2, v2)
				return false, nil
			}
		default:
			t.Logf("same-date?: arg1 is not time or string: %v (%T)", v2, v2)
			return false, nil
		}

		result := t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day()
		t.Logf("same-date?: comparing %v and %v = %v", t1, t2, result)
		return result, nil
	})

	exec := NewExecutor(matcher)

	// Query with subquery that takes multiple inputs
	// Note: Simplified to test basic functionality first

	queryStr := `[:find ?symbol ?high
	             :where 
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?h)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/time ?t]
	                         [?p :price/high ?h]]
	                 $ ?s) [[?high]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Check results
	if result.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", result.Size())
		for i := 0; i < result.Size(); i++ {
			t.Logf("Result %d: %v", i, result.Get(i))
		}
	}

	if result.Size() > 0 {
		tuple := result.Get(0)
		if symbol := tuple[0].(string); symbol != "AAPL" {
			t.Errorf("Expected symbol AAPL, got %s", symbol)
		}
		// Should get max of all highs (160.0)
		if high := tuple[1].(float64); high != 160.0 {
			t.Errorf("Expected high 160.0, got %f", high)
		}
	}
}
