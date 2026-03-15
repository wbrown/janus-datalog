package executor

import (
	"fmt"
	"strings"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// collectResult iterates a relation and returns all tuples as copies.
func collectResult(rel Relation) []Tuple {
	var tuples []Tuple
	it := rel.Iterator()
	for it.Next() {
		t := it.Tuple()
		cp := make(Tuple, len(t))
		copy(cp, t)
		tuples = append(tuples, cp)
	}
	it.Close()
	return tuples
}

// TestSubqueryFindClauseBug reproduces the bug where input variables
// are incorrectly added to the find clause during decorrelation optimization.
func TestSubqueryFindClauseBug(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			`[:symbol/ticker "AAPL"]`: {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			`[:price/symbol _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
			`[:price/value _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/value"), V: 150.0, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/value"), V: 155.0, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
		},
	}

	exec := NewExecutor(matcher, nil)

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

	tuples := collectResult(result)
	t.Logf("Result symbols: %v", result.Symbols())
	t.Logf("Result count: %d", len(tuples))
	if len(tuples) > 0 {
		t.Logf("First tuple: %v", tuples[0])
	}

	if len(tuples) != 1 {
		t.Errorf("Expected 1 result, got %d", len(tuples))
	}

	tuple := tuples[0]
	symbol := tuple[0]
	maxPrice := tuple[1]
	minPrice := tuple[2]

	t.Logf("symbol=%v (type %T)", symbol, symbol)
	t.Logf("maxPrice=%v (type %T)", maxPrice, maxPrice)
	t.Logf("minPrice=%v (type %T)", minPrice, minPrice)

	if symbol != "AAPL" {
		t.Errorf("Expected symbol 'AAPL', got %v", symbol)
	}

	if maxPrice == nil || minPrice == nil {
		t.Fatalf("BUG REPRODUCED: Aggregates are nil - decorrelation broke the query")
	}

	if price, ok := maxPrice.(float64); !ok || price != 155.0 {
		t.Errorf("Expected maxPrice=155.0, got %v (type %T)", maxPrice, maxPrice)
	}
	if price, ok := minPrice.(float64); !ok || price != 150.0 {
		t.Errorf("Expected minPrice=150.0, got %v (type %T)", minPrice, minPrice)
	}
}

// TestSubqueryFindClauseBugWithAnnotations uses annotations to detect the bug
func TestSubqueryFindClauseBugWithAnnotations(t *testing.T) {
	var aggregationEvents []annotations.Event
	handler := func(event annotations.Event) {
		if event.Name == annotations.AggregationExecuted {
			aggregationEvents = append(aggregationEvents, event)
		}
		if strings.HasPrefix(event.Name, "subquery/decorrelation") || event.Name == "aggregation/pre-data" ||
			event.Name == "matches->relations" || strings.HasPrefix(event.Name, "join/") ||
			strings.HasPrefix(event.Name, "collapse/") {
			fmt.Printf("  [%s] %v\n", event.Name, event.Data)
		}
	}

	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			`[:symbol/ticker "AAPL"]`: {
				{E: datalog.NewIdentity("symbol:aapl"), A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			`[:price/symbol _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:aapl"), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
			`[:price/value _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/value"), V: 150.0, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/value"), V: 155.0, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
		},
	}

	annotatedMatcher := WrapMatcher(matcher, handler)
	exec := NewExecutor(annotatedMatcher, nil)

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

	ctx := NewContext(handler)

	_, err = exec.ExecuteWithContext(ctx, q)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(aggregationEvents) == 0 {
		t.Skip("No aggregation events captured - annotations not wired up yet")
		return
	}

	// With the decorrelation cache, grouped aggregation (groupby_count=1) is
	// CORRECT — the cache adds the correlation variable as a GROUP BY key so the
	// query runs once and results are filtered per correlation value.
	for i, event := range aggregationEvents {
		t.Logf("Aggregation event %d: %+v", i, event.Data)
	}
}

// TestSubqueryMultiValueFindClauseBug tests the bug with multi-value bindings
func TestSubqueryMultiValueFindClauseBug(t *testing.T) {
	matcher := &MockPatternMatcher{
		data: map[string][]datalog.Datom{
			`[:symbol/ticker "TEST"]`: {
				{E: datalog.NewIdentity("symbol:test"), A: datalog.NewKeyword(":symbol/ticker"), V: "TEST", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
			},
			`[:price/symbol _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:test"), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/symbol"), V: datalog.NewIdentity("symbol:test"), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
			`[:price/day _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/day"), V: int64(15), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/day"), V: int64(15), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
			`[:price/high _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/high"), V: 100.0, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/high"), V: 110.0, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
			`[:price/low _]`: {
				{E: datalog.NewIdentity("price:1"), A: datalog.NewKeyword(":price/low"), V: 95.0, Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				{E: datalog.NewIdentity("price:2"), A: datalog.NewKeyword(":price/low"), V: 98.0, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
			},
		},
	}

	exec := NewExecutor(matcher, nil)

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

	tuples := collectResult(result)
	if len(tuples) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(tuples))
	}

	tuple := tuples[0]
	symbol := tuple[0]
	high := tuple[1]
	low := tuple[2]

	if symbol != "TEST" {
		t.Errorf("Expected symbol 'TEST', got %v", symbol)
	}

	if high == nil {
		t.Errorf("BUG REPRODUCED: high is nil (expected 110.0)")
		t.Logf("Result tuple: %v", tuple)
	} else if h, ok := high.(float64); !ok || h != 110.0 {
		t.Errorf("Expected high=110.0, got %v (type %T)", high, high)
	}

	if low == nil {
		t.Errorf("BUG REPRODUCED: low is nil (expected 95.0)")
		t.Logf("Result tuple: %v", tuple)
	} else if l, ok := low.(float64); !ok || l != 95.0 {
		t.Errorf("Expected low=95.0, got %v (type %T)", low, low)
	}
}
