package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestDecorrelationEndToEnd tests the full decorrelation execution path
func TestDecorrelationEndToEnd(t *testing.T) {
	// Create test data - simple price data for testing OHLC-style queries
	prod1 := datalog.NewIdentity("PROD1")

	datoms := []datalog.Datom{
		// Symbol
		{E: prod1, A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1},

		// Prices for hour 9
		{E: datalog.NewIdentity("bar1"), A: datalog.NewKeyword(":price/symbol"), V: prod1, Tx: 1},
		{E: datalog.NewIdentity("bar1"), A: datalog.NewKeyword(":price/hour"), V: int64(9), Tx: 1},
		{E: datalog.NewIdentity("bar1"), A: datalog.NewKeyword(":price/high"), V: 150.0, Tx: 1},
		{E: datalog.NewIdentity("bar1"), A: datalog.NewKeyword(":price/low"), V: 145.0, Tx: 1},

		{E: datalog.NewIdentity("bar2"), A: datalog.NewKeyword(":price/symbol"), V: prod1, Tx: 1},
		{E: datalog.NewIdentity("bar2"), A: datalog.NewKeyword(":price/hour"), V: int64(9), Tx: 1},
		{E: datalog.NewIdentity("bar2"), A: datalog.NewKeyword(":price/high"), V: 155.0, Tx: 1},
		{E: datalog.NewIdentity("bar2"), A: datalog.NewKeyword(":price/low"), V: 148.0, Tx: 1},

		// Prices for hour 10
		{E: datalog.NewIdentity("bar3"), A: datalog.NewKeyword(":price/symbol"), V: prod1, Tx: 1},
		{E: datalog.NewIdentity("bar3"), A: datalog.NewKeyword(":price/hour"), V: int64(10), Tx: 1},
		{E: datalog.NewIdentity("bar3"), A: datalog.NewKeyword(":price/high"), V: 160.0, Tx: 1},
		{E: datalog.NewIdentity("bar3"), A: datalog.NewKeyword(":price/low"), V: 152.0, Tx: 1},

		{E: datalog.NewIdentity("bar4"), A: datalog.NewKeyword(":price/symbol"), V: prod1, Tx: 1},
		{E: datalog.NewIdentity("bar4"), A: datalog.NewKeyword(":price/hour"), V: int64(10), Tx: 1},
		{E: datalog.NewIdentity("bar4"), A: datalog.NewKeyword(":price/high"), V: 165.0, Tx: 1},
		{E: datalog.NewIdentity("bar4"), A: datalog.NewKeyword(":price/low"), V: 158.0, Tx: 1},
	}

	// Create matcher
	matcher := NewMemoryPatternMatcher(datoms)

	// Query with two subqueries that should be decorrelated:
	// - Both have the same correlation signature (?s, ?hour)
	// - Both are grouped aggregations
	queryStr := `[:find ?hour ?high ?low
	              :where
	                [?s :symbol/ticker "AAPL"]
	                [?b :price/symbol ?s]
	                [?b :price/hour ?hour]

	                [(q [:find (max ?h)
	                     :in $ [[?sym ?hr] ...]
	                     :where
	                       [?bar :price/symbol ?sym]
	                       [?bar :price/hour ?hr]
	                       [?bar :price/high ?h]]
	                    $ ?s ?hour) [[?high]]]

	                [(q [:find (min ?l)
	                     :in $ [[?sym ?hr] ...]
	                     :where
	                       [?bar :price/symbol ?sym]
	                       [?bar :price/hour ?hr]
	                       [?bar :price/low ?l]]
	                    $ ?s ?hour) [[?low]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("failed to parse query: %v", err)
	}

	// Create executor with decorrelation enabled
	executor := NewQueryExecutor(matcher, ExecutorOptions{
		EnableSubqueryDecorrelation: true,
	})

	// Execute the query
	ctx := NewContext(nil)
	results, err := executor.Execute(ctx, q, []Relation{})
	if err != nil {
		t.Fatalf("query execution failed: %v", err)
	}

	// Should have one result group
	if len(results) != 1 {
		t.Fatalf("expected 1 result group, got %d", len(results))
	}

	result := results[0]

	// Check columns
	expectedCols := []query.Symbol{"?hour", "?high", "?low"}
	if !columnsEqualTest(result.Columns(), expectedCols) {
		t.Errorf("column mismatch:\n  got=%v\n  want=%v",
			result.Columns(), expectedCols)
	}

	// Check results
	// Hour 9: max high = 155.0, min low = 145.0
	// Hour 10: max high = 165.0, min low = 152.0
	expectedResults := map[int64]struct {
		high float64
		low  float64
	}{
		9:  {high: 155.0, low: 145.0},
		10: {high: 165.0, low: 152.0},
	}

	if result.Size() != 2 {
		t.Errorf("expected 2 result tuples, got %d", result.Size())
		dumpRelationTest(t, result)
		return
	}

	it := result.Iterator()
	defer it.Close()

	found := make(map[int64]bool)
	for it.Next() {
		tuple := it.Tuple()
		if len(tuple) != 3 {
			t.Errorf("expected tuple length 3, got %d: %v", len(tuple), tuple)
			continue
		}

		hour, ok := tuple[0].(int64)
		if !ok {
			t.Errorf("expected hour to be int64, got %T: %v", tuple[0], tuple[0])
			continue
		}

		high, ok := tuple[1].(float64)
		if !ok {
			t.Errorf("expected high to be float64, got %T: %v", tuple[1], tuple[1])
			continue
		}

		low, ok := tuple[2].(float64)
		if !ok {
			t.Errorf("expected low to be float64, got %T: %v", tuple[2], tuple[2])
			continue
		}

		expected, exists := expectedResults[hour]
		if !exists {
			t.Errorf("unexpected hour: %d", hour)
			continue
		}

		if high != expected.high {
			t.Errorf("hour %d: expected high=%f, got %f", hour, expected.high, high)
		}

		if low != expected.low {
			t.Errorf("hour %d: expected low=%f, got %f", hour, expected.low, low)
		}

		found[hour] = true
	}

	// Check all hours were found
	for hour := range expectedResults {
		if !found[hour] {
			t.Errorf("missing results for hour %d", hour)
		}
	}

	t.Logf("✓ Decorrelation test passed: 2 subqueries executed with batched inputs")
}

// Helper functions for testing

func columnsEqualTest(a, b []query.Symbol) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func dumpRelationTest(t *testing.T, rel Relation) {
	t.Logf("Relation columns: %v", rel.Columns())
	it := rel.Iterator()
	defer it.Close()
	count := 0
	for it.Next() {
		t.Logf("  [%d] %v", count, it.Tuple())
		count++
	}
}
