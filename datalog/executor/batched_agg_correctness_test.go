package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestBatchedAggregationCorrectness verifies that batched subquery execution
// produces correct per-group aggregation results
func TestBatchedAggregationCorrectness(t *testing.T) {
	// Create test data: 2 groups with different values
	symbolA := datalog.NewIdentity("symbol:A")
	symbolB := datalog.NewIdentity("symbol:B")

	datoms := []datalog.Datom{
		// Symbol A
		{E: symbolA, A: datalog.NewKeyword(":symbol/ticker"), V: "A", Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		// Symbol B
		{E: symbolB, A: datalog.NewKeyword(":symbol/ticker"), V: "B", Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},

		// Prices for A: 10, 20, 30 (max should be 30)
		{E: datalog.NewIdentity("bar:A:1"), A: datalog.NewKeyword(":price/symbol"), V: symbolA, Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},
		{E: datalog.NewIdentity("bar:A:1"), A: datalog.NewKeyword(":price/value"), V: float64(10), Tx: datalog.ElementID{Lamport: 3, ReplicaID: 1}},

		{E: datalog.NewIdentity("bar:A:2"), A: datalog.NewKeyword(":price/symbol"), V: symbolA, Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},
		{E: datalog.NewIdentity("bar:A:2"), A: datalog.NewKeyword(":price/value"), V: float64(20), Tx: datalog.ElementID{Lamport: 4, ReplicaID: 1}},

		{E: datalog.NewIdentity("bar:A:3"), A: datalog.NewKeyword(":price/symbol"), V: symbolA, Tx: datalog.ElementID{Lamport: 5, ReplicaID: 1}},
		{E: datalog.NewIdentity("bar:A:3"), A: datalog.NewKeyword(":price/value"), V: float64(30), Tx: datalog.ElementID{Lamport: 5, ReplicaID: 1}},

		// Prices for B: 100, 200 (max should be 200)
		{E: datalog.NewIdentity("bar:B:1"), A: datalog.NewKeyword(":price/symbol"), V: symbolB, Tx: datalog.ElementID{Lamport: 6, ReplicaID: 1}},
		{E: datalog.NewIdentity("bar:B:1"), A: datalog.NewKeyword(":price/value"), V: float64(100), Tx: datalog.ElementID{Lamport: 6, ReplicaID: 1}},

		{E: datalog.NewIdentity("bar:B:2"), A: datalog.NewKeyword(":price/symbol"), V: symbolB, Tx: datalog.ElementID{Lamport: 7, ReplicaID: 1}},
		{E: datalog.NewIdentity("bar:B:2"), A: datalog.NewKeyword(":price/value"), V: float64(200), Tx: datalog.ElementID{Lamport: 7, ReplicaID: 1}},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher, nil)

	// Query with RelationInput (uses batched path)
	queryStr := `
		[:find ?ticker ?max-price
		 :where
		 [?s :symbol/ticker ?ticker]
		 [(q [:find (max ?p)
		      :in $ [[?sym] ...]
		      :where
		      [?b :price/symbol ?sym]
		      [?b :price/value ?p]]
		     $ ?s) [[?max-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Build map of results
	results := make(map[string]float64)
	for i := 0; i < result.Size(); i++ {
		tuple := result.Get(i)
		ticker := tuple[0].(string)
		maxPrice := tuple[1].(float64)
		results[ticker] = maxPrice
		t.Logf("Result: %s max = %.0f", ticker, maxPrice)
	}

	// Verify per-group aggregation
	if results["A"] != 30.0 {
		t.Errorf("Expected A max = 30, got %.0f (batched aggregation computing over all groups?)", results["A"])
	}
	if results["B"] != 200.0 {
		t.Errorf("Expected B max = 200, got %.0f", results["B"])
	}
}
