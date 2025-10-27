package executor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestQueryExecutorSubqueryProjection tests that QueryExecutor properly handles
// subquery result columns for projection in the :find clause.
//
// Bug: QueryExecutor fails to preserve subquery result column names, causing
// projection to fail with "cannot project: column ?xxx not found in relation"
//
// This reproduces the gopher-street bug:
// "cannot project: column ?open-price not found in relation"
func TestQueryExecutorSubqueryProjection(t *testing.T) {
	// Create test data
	alice := datalog.NewIdentity("alice")
	bob := datalog.NewIdentity("bob")
	charlie := datalog.NewIdentity("charlie")

	nameKw := datalog.NewKeyword(":person/name")
	ageKw := datalog.NewKeyword(":person/age")

	datoms := []datalog.Datom{
		{E: alice, A: nameKw, V: "Alice", Tx: 1},
		{E: alice, A: ageKw, V: int64(30), Tx: 1},
		{E: bob, A: nameKw, V: "Bob", Tx: 1},
		{E: bob, A: ageKw, V: int64(25), Tx: 1},
		{E: charlie, A: nameKw, V: "Charlie", Tx: 1},
		{E: charlie, A: ageKw, V: int64(35), Tx: 1},
	}

	// Query with subquery that projects a result column
	// The outer query should be able to find ?max-age in its :find clause
	queryStr := `[:find ?name ?max-age
	              :where [?e :person/name ?name]
	                     [(q [:find (max ?a)
	                          :where [?p :person/age ?a]]
	                         $) [[?max-age]]]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	// Test with legacy executor (should work)
	t.Run("LegacyExecutor", func(t *testing.T) {
		matcher := NewIndexedMemoryMatcher(datoms) // Fresh matcher per subtest
		opts := planner.PlannerOptions{
			UseQueryExecutor: false, // Legacy executor
		}
		exec := NewExecutorWithOptions(matcher, opts)
		result, err := exec.Execute(q)
		assert.NoError(t, err)

		// Should have 3 results (one per person), all with max-age=35 (don't check IsEmpty - may consume first tuple)
		assert.Equal(t, 3, result.Size())

		it := result.Iterator()
		defer it.Close()
		for it.Next() {
			tuple := it.Tuple()
			assert.Len(t, tuple, 2)
			assert.IsType(t, "", tuple[0]) // name
			assert.Equal(t, int64(35), tuple[1]) // max-age
		}
	})

	// Test with QueryExecutor (Stage B) - this is the bug
	t.Run("QueryExecutor", func(t *testing.T) {
		matcher := NewIndexedMemoryMatcher(datoms) // Fresh matcher per subtest
		opts := planner.PlannerOptions{
			UseQueryExecutor: true, // QueryExecutor (Stage B)
		}
		exec := NewExecutorWithOptions(matcher, opts)
		result, err := exec.Execute(q)

		// BUG: This should succeed but fails with:
		// "cannot project: column ?max-age not found in relation"
		assert.NoError(t, err, "QueryExecutor should preserve subquery result column names")

		// Collect results (don't check Size() or IsEmpty() - may be streaming and would consume first tuple)
		it := result.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			tuple := it.Tuple()
			assert.Len(t, tuple, 2)
			assert.IsType(t, "", tuple[0]) // name
			assert.Equal(t, int64(35), tuple[1]) // max-age
			count++
		}

		// Should have 3 results (one per person), all with max-age=35
		assert.Equal(t, 3, count)
	})
}

// TestQueryExecutorMultipleSubqueryProjections tests multiple subqueries
// each contributing columns to the final projection
func TestQueryExecutorMultipleSubqueryProjections(t *testing.T) {
	// Create test data - price bars for multiple symbols
	aapl := datalog.NewIdentity("AAPL")
	msft := datalog.NewIdentity("MSFT")

	symbolKw := datalog.NewKeyword(":symbol/ticker")
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceOpen := datalog.NewKeyword(":price/open")
	priceClose := datalog.NewKeyword(":price/close")
	priceHigh := datalog.NewKeyword(":price/high")
	priceLow := datalog.NewKeyword(":price/low")
	priceTime := datalog.NewKeyword(":price/time")

	// Add price bars
	loc, _ := time.LoadLocation("America/New_York")
	t1 := time.Date(2025, 1, 2, 9, 30, 0, 0, loc)
	t2 := time.Date(2025, 1, 2, 9, 35, 0, 0, loc)
	t3 := time.Date(2025, 1, 2, 9, 40, 0, 0, loc)

	bar1 := datalog.NewIdentity("bar1")
	bar2 := datalog.NewIdentity("bar2")
	bar3 := datalog.NewIdentity("bar3")

	datoms := []datalog.Datom{
		{E: aapl, A: symbolKw, V: "AAPL", Tx: 1},
		{E: msft, A: symbolKw, V: "MSFT", Tx: 1},
		// AAPL bars
		{E: bar1, A: priceSymbol, V: aapl, Tx: 1},
		{E: bar1, A: priceTime, V: t1, Tx: 1},
		{E: bar1, A: priceOpen, V: 100.0, Tx: 1},
		{E: bar1, A: priceClose, V: 101.0, Tx: 1},
		{E: bar1, A: priceHigh, V: 102.0, Tx: 1},
		{E: bar1, A: priceLow, V: 99.0, Tx: 1},
		{E: bar2, A: priceSymbol, V: aapl, Tx: 1},
		{E: bar2, A: priceTime, V: t2, Tx: 1},
		{E: bar2, A: priceOpen, V: 101.0, Tx: 1},
		{E: bar2, A: priceClose, V: 103.0, Tx: 1},
		{E: bar2, A: priceHigh, V: 104.0, Tx: 1},
		{E: bar2, A: priceLow, V: 100.0, Tx: 1},
		{E: bar3, A: priceSymbol, V: aapl, Tx: 1},
		{E: bar3, A: priceTime, V: t3, Tx: 1},
		{E: bar3, A: priceOpen, V: 103.0, Tx: 1},
		{E: bar3, A: priceClose, V: 102.0, Tx: 1},
		{E: bar3, A: priceHigh, V: 105.0, Tx: 1},
		{E: bar3, A: priceLow, V: 101.0, Tx: 1},
	}

	// Query with multiple subqueries contributing to projection
	// Simulates gopher-street's OHLC query pattern
	queryStr := `[:find ?symbol ?first-open ?last-close ?max-high ?min-low
	              :where [?s :symbol/ticker ?symbol]
	                     ; Get first open price
	                     [(q [:find (min ?o)
	                          :in $ ?sym
	                          :where [?b :price/symbol ?sym]
	                                 [?b :price/open ?o]]
	                         $ ?s) [[?first-open]]]
	                     ; Get last close price
	                     [(q [:find (max ?c)
	                          :in $ ?sym
	                          :where [?b :price/symbol ?sym]
	                                 [?b :price/close ?c]]
	                         $ ?s) [[?last-close]]]
	                     ; Get max high
	                     [(q [:find (max ?h)
	                          :in $ ?sym
	                          :where [?b :price/symbol ?sym]
	                                 [?b :price/high ?h]]
	                         $ ?s) [[?max-high]]]
	                     ; Get min low
	                     [(q [:find (min ?l)
	                          :in $ ?sym
	                          :where [?b :price/symbol ?sym]
	                                 [?b :price/low ?l]]
	                         $ ?s) [[?min-low]]]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	// Test with QueryExecutor (Stage B) - this is the bug
	t.Run("QueryExecutor", func(t *testing.T) {
		matcher := NewIndexedMemoryMatcher(datoms) // Fresh matcher per subtest
		opts := planner.PlannerOptions{
			UseQueryExecutor: true, // QueryExecutor (Stage B)
		}
		exec := NewExecutorWithOptions(matcher, opts)
		result, err := exec.Execute(q)

		// BUG: This should succeed but fails with:
		// "cannot project: column ?first-open (or other subquery columns) not found in relation"
		assert.NoError(t, err, "QueryExecutor should preserve all subquery result column names")

		// Collect results (don't check Size() - may be streaming)
		it := result.Iterator()
		defer it.Close()
		assert.True(t, it.Next())
		tuple := it.Tuple()
		assert.Len(t, tuple, 5)
		assert.Equal(t, "AAPL", tuple[0])
		assert.Equal(t, 100.0, tuple[1]) // first-open
		assert.Equal(t, 103.0, tuple[2]) // last-close (max of all closes)
		assert.Equal(t, 105.0, tuple[3]) // max-high
		assert.Equal(t, 99.0, tuple[4])  // min-low
		assert.False(t, it.Next()) // Should be only 1 result for AAPL
	})
}
