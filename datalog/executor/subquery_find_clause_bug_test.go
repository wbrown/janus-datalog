package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestSubqueryFindClauseBug reproduces the bug where input variables
// are incorrectly added to the find clause during decorrelation optimization.
//
// The bug occurs when:
// 1. Multiple subqueries share the same input parameters (triggers decorrelation)
// 2. The decorrelation optimizer adds those inputs as FindVariable elements to :find
// 3. This turns pure aggregation [:find (max ?h)] into grouped aggregation [:find ?sym (max ?h)]
// 4. The grouped aggregation fails to join properly, returning nil values
func TestSubqueryFindClauseBug(t *testing.T) {
	// Create simple test data
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			`[:symbol/ticker "AAPL"]`: {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
			},
			`[:price/symbol _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 3},
			},
			`[:price/value _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/value"), V: 150.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/value"), V: 155.0, Tx: 3},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Query with TWO subqueries that share the same input parameter
	// This triggers decorrelation optimization which incorrectly modifies the find clause
	queryStr := `[:find ?symbol ?max-price ?min-price
	             :where
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?max-price]]]
	             [(q [:find (min ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?min-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Debug: print result structure
	t.Logf("Result columns: %v", result.Columns())
	t.Logf("Result size: %d", result.Size())
	if result.Size() > 0 {
		t.Logf("First tuple: %v", result.Get(0))
	}

	// Check the result is correct
	if result.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", result.Size())
	}

	tuple := result.Get(0)
	symbol := tuple[0]
	maxPrice := tuple[1]
	minPrice := tuple[2]

	t.Logf("symbol=%v (type %T)", symbol, symbol)
	t.Logf("maxPrice=%v (type %T)", maxPrice, maxPrice)
	t.Logf("minPrice=%v (type %T)", minPrice, minPrice)

	if symbol != "AAPL" {
		t.Errorf("Expected symbol 'AAPL', got %v", symbol)
	}

	// THE BUG: Decorrelation incorrectly adds ?sym to find clause
	// This creates grouped aggregation instead of pure aggregation
	// With simple data it still returns values, but the structure is WRONG

	// Check the actual structure via debug output or annotations
	// For now, we document what SHOULD happen vs what DOES happen:
	//
	// EXPECTED find clause: [:find (max ?price) (min ?price)]
	// ACTUAL find clause:   [:find ?sym (max ?price) (min ?price)]
	//
	// The extra ?sym variable causes grouped aggregation.
	// With complex real data (like gopher-street), this leads to nil values.

	if maxPrice == nil || minPrice == nil {
		t.Fatalf("BUG REPRODUCED: Aggregates are nil - decorrelation broke the query")
	}

	// Values are correct with simple data, but structure is wrong (see debug output)
	if price, ok := maxPrice.(float64); !ok || price != 155.0 {
		t.Errorf("Expected maxPrice=155.0, got %v (type %T)", maxPrice, maxPrice)
	}
	if price, ok := minPrice.(float64); !ok || price != 150.0 {
		t.Errorf("Expected minPrice=150.0, got %v (type %T)", minPrice, minPrice)
	}
}

// TestSubqueryFindClauseBugWithAnnotations uses annotations to detect the bug
// This is more robust than checking nil values because it catches the root cause
func TestSubqueryFindClauseBugWithAnnotations(t *testing.T) {
	// Track aggregation events
	var aggregationEvents []annotations.Event
	handler := func(event annotations.Event) {
		if event.Name == annotations.AggregationExecuted {
			aggregationEvents = append(aggregationEvents, event)
		}
	}

	// Create test data
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			`[:symbol/ticker "AAPL"]`: {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},
			},
			`[:price/symbol _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: 3},
			},
			`[:price/value _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/value"), V: 150.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/value"), V: 155.0, Tx: 3},
			},
		},
	}

	// Wrap matcher with annotation support
	annotatedMatcher := WrapMatcher(matcher, handler)
	exec := NewExecutor(annotatedMatcher)

	// Query with TWO subqueries that share the same input parameter
	queryStr := `[:find ?symbol ?max-price ?min-price
	             :where
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?max-price]]]
	             [(q [:find (min ?price)
	                  :in $ ?sym
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/value ?price]]
	                 $ ?s) [[?min-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Create context with handler to capture events
	ctx := NewContext(handler)

	_, err = exec.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	// Check aggregation events for the bug
	if len(aggregationEvents) == 0 {
		t.Skip("No aggregation events captured - annotations not wired up yet")
		return
	}

	// This test MUST FAIL when the bug is present
	bugDetected := false
	for i, event := range aggregationEvents {
		t.Logf("Aggregation event %d: %+v", i, event.Data)

		// Subqueries should have pure aggregations (0 groupby vars)
		// If groupby_count > 0, decorrelation incorrectly added input vars
		if groupByCount, ok := event.Data["groupby_count"].(int); ok {
			if groupByCount > 0 {
				bugDetected = true
				findElems, _ := event.Data["find_elements"].([]string)
				groupByVars, _ := event.Data["groupby_vars"].([]query.Symbol)

				t.Errorf("BUG DETECTED: Decorrelation added input variables to find clause")
				t.Errorf("  Expected: Pure aggregation with 0 groupby vars")
				t.Errorf("  Actual: Grouped aggregation with %d groupby vars: %v", groupByCount, groupByVars)
				t.Errorf("  Find elements: %v", findElems)
			}
		}
	}

	if !bugDetected {
		t.Logf("Bug not detected - decorrelation may be disabled or query structure different")
	}
}

// TestSubqueryMultiValueFindClauseBug tests the bug with multi-value bindings
// This is the pattern from the OHLC queries that's failing
func TestSubqueryMultiValueFindClauseBug(t *testing.T) {
	// Create test data with high/low values AND a day value
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			`[:symbol/ticker "TEST"]`: {
				{E: datalog.NewIdentity("symbol:test"), A: datalog.NewKeyword(":symbol/ticker"), V: "TEST", Tx: 1},
			},
			`[:price/symbol _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:test"), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:test"), Tx: 3},
			},
			`[:price/day _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/day"), V: int64(15), Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/day"), V: int64(15), Tx: 3},
			},
			`[:price/high _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/high"), V: 100.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/high"), V: 110.0, Tx: 3},
			},
			`[:price/low _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/low"), V: 95.0, Tx: 2},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/low"), V: 98.0, Tx: 3},
			},
		},
	}

	exec := NewExecutor(matcher)

	// Query with TWO multi-value binding subqueries that have intermediate variables
	// Both subqueries share the same inputs (?sym, ?d) which triggers decorrelation
	// The subqueries create ?pd as an intermediate variable, then filter on it
	// This mimics the OHLC pattern that's failing
	queryStr := `[:find ?symbol ?high ?low
	             :where
	             [?s :symbol/ticker ?symbol]
	             [(q [:find (max ?h)
	                  :in $ ?sym ?d
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/day ?pd]
	                         [(= ?pd ?d)]
	                         [?p :price/high ?h]]
	                 $ ?s 15) [[?high]]]
	             [(q [:find (min ?l)
	                  :in $ ?sym ?d
	                  :where [?p :price/symbol ?sym]
	                         [?p :price/day ?pd]
	                         [(= ?pd ?d)]
	                         [?p :price/low ?l]]
	                 $ ?s 15) [[?low]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result.Size() != 1 {
		t.Errorf("Expected 1 result, got %d", result.Size())
	}

	tuple := result.Get(0)
	symbol := tuple[0]
	high := tuple[1]
	low := tuple[2]

	if symbol != "TEST" {
		t.Errorf("Expected symbol 'TEST', got %v", symbol)
	}

	// THE BUG: These will be nil if input variable is in find clause
	if high == nil {
		t.Errorf("BUG REPRODUCED: high is nil (expected 110.0)")
		t.Errorf("Input variable ?sym incorrectly added to find clause")
		t.Logf("Result tuple: %v", tuple)
	} else if h, ok := high.(float64); !ok || h != 110.0 {
		t.Errorf("Expected high=110.0, got %v (type %T)", high, high)
	}

	if low == nil {
		t.Errorf("BUG REPRODUCED: low is nil (expected 95.0)")
		t.Errorf("Input variable ?sym incorrectly added to find clause")
		t.Logf("Result tuple: %v", tuple)
	} else if l, ok := low.(float64); !ok || l != 95.0 {
		t.Errorf("Expected low=95.0, got %v (type %T)", low, low)
	}
}
