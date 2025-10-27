package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestQueryExecutorMultipleCorrelatedSubqueries tests the pattern from gopher-street
// where multiple correlated subqueries are used in sequence to build up result columns
func TestQueryExecutorMultipleCorrelatedSubqueries(t *testing.T) {
	// Create test data similar to gopher-street OHLC pattern
	day1 := datalog.NewIdentity("day1")
	day2 := datalog.NewIdentity("day2")

	dateKw := datalog.NewKeyword(":day/date")
	yearKw := datalog.NewKeyword(":day/year")
	monthKw := datalog.NewKeyword(":day/month")
	dayKw := datalog.NewKeyword(":day/day")

	bar1 := datalog.NewIdentity("bar1")
	bar2 := datalog.NewIdentity("bar2")
	bar3 := datalog.NewIdentity("bar3")
	bar4 := datalog.NewIdentity("bar4")

	barDayKw := datalog.NewKeyword(":bar/day")
	barYearKw := datalog.NewKeyword(":bar/year")
	barMonthKw := datalog.NewKeyword(":bar/month")
	barDayNumKw := datalog.NewKeyword(":bar/day-num")
	openKw := datalog.NewKeyword(":bar/open")
	closeKw := datalog.NewKeyword(":bar/close")
	highKw := datalog.NewKeyword(":bar/high")
	lowKw := datalog.NewKeyword(":bar/low")

	datoms := []datalog.Datom{
		// Day 1: 2025-01-01
		{E: day1, A: dateKw, V: "2025-01-01", Tx: 1},
		{E: day1, A: yearKw, V: int64(2025), Tx: 1},
		{E: day1, A: monthKw, V: int64(1), Tx: 1},
		{E: day1, A: dayKw, V: int64(1), Tx: 1},

		// Bars for day 1
		{E: bar1, A: barDayKw, V: day1, Tx: 1},
		{E: bar1, A: barYearKw, V: int64(2025), Tx: 1},
		{E: bar1, A: barMonthKw, V: int64(1), Tx: 1},
		{E: bar1, A: barDayNumKw, V: int64(1), Tx: 1},
		{E: bar1, A: openKw, V: 100.0, Tx: 1},
		{E: bar1, A: closeKw, V: 105.0, Tx: 1},
		{E: bar1, A: highKw, V: 110.0, Tx: 1},
		{E: bar1, A: lowKw, V: 99.0, Tx: 1},

		{E: bar2, A: barDayKw, V: day1, Tx: 1},
		{E: bar2, A: barYearKw, V: int64(2025), Tx: 1},
		{E: bar2, A: barMonthKw, V: int64(1), Tx: 1},
		{E: bar2, A: barDayNumKw, V: int64(1), Tx: 1},
		{E: bar2, A: openKw, V: 105.0, Tx: 1},
		{E: bar2, A: closeKw, V: 108.0, Tx: 1},
		{E: bar2, A: highKw, V: 112.0, Tx: 1},
		{E: bar2, A: lowKw, V: 104.0, Tx: 1},

		// Day 2: 2025-01-02
		{E: day2, A: dateKw, V: "2025-01-02", Tx: 1},
		{E: day2, A: yearKw, V: int64(2025), Tx: 1},
		{E: day2, A: monthKw, V: int64(1), Tx: 1},
		{E: day2, A: dayKw, V: int64(2), Tx: 1},

		// Bars for day 2
		{E: bar3, A: barDayKw, V: day2, Tx: 1},
		{E: bar3, A: barYearKw, V: int64(2025), Tx: 1},
		{E: bar3, A: barMonthKw, V: int64(1), Tx: 1},
		{E: bar3, A: barDayNumKw, V: int64(2), Tx: 1},
		{E: bar3, A: openKw, V: 200.0, Tx: 1},
		{E: bar3, A: closeKw, V: 205.0, Tx: 1},
		{E: bar3, A: highKw, V: 210.0, Tx: 1},
		{E: bar3, A: lowKw, V: 199.0, Tx: 1},

		{E: bar4, A: barDayKw, V: day2, Tx: 1},
		{E: bar4, A: barYearKw, V: int64(2025), Tx: 1},
		{E: bar4, A: barMonthKw, V: int64(1), Tx: 1},
		{E: bar4, A: barDayNumKw, V: int64(2), Tx: 1},
		{E: bar4, A: openKw, V: 205.0, Tx: 1},
		{E: bar4, A: closeKw, V: 208.0, Tx: 1},
		{E: bar4, A: highKw, V: 212.0, Tx: 1},
		{E: bar4, A: lowKw, V: 204.0, Tx: 1},
	}

	// Query similar to gopher-street pattern:
	// Get date, then use multiple correlated subqueries to get open, high, low, close
	queryStr := `[:find ?date ?open-price ?daily-high ?daily-low ?close-price
	              :where [?d :day/date ?date]
	                     [?d :day/year ?year]
	                     [?d :day/month ?month]
	                     [?d :day/day ?day-num]

	                     ; Get open price via subquery
	                     [(q [:find (min ?o)
	                          :in $ ?y ?m ?dn
	                          :where [?b :bar/year ?y]
	                                 [?b :bar/month ?m]
	                                 [?b :bar/day-num ?dn]
	                                 [?b :bar/open ?o]]
	                         $ ?year ?month ?day-num) [[?open-price]]]

	                     ; Get high price via subquery
	                     [(q [:find (max ?h)
	                          :in $ ?y ?m ?dn
	                          :where [?b :bar/year ?y]
	                                 [?b :bar/month ?m]
	                                 [?b :bar/day-num ?dn]
	                                 [?b :bar/high ?h]]
	                         $ ?year ?month ?day-num) [[?daily-high]]]

	                     ; Get low price via subquery
	                     [(q [:find (min ?l)
	                          :in $ ?y ?m ?dn
	                          :where [?b :bar/year ?y]
	                                 [?b :bar/month ?m]
	                                 [?b :bar/day-num ?dn]
	                                 [?b :bar/low ?l]]
	                         $ ?year ?month ?day-num) [[?daily-low]]]

	                     ; Get close price via subquery
	                     [(q [:find (max ?c)
	                          :in $ ?y ?m ?dn
	                          :where [?b :bar/year ?y]
	                                 [?b :bar/month ?m]
	                                 [?b :bar/day-num ?dn]
	                                 [?b :bar/close ?c]]
	                         $ ?year ?month ?day-num) [[?close-price]]]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	// Test with QueryExecutor (Stage B)
	t.Run("QueryExecutor", func(t *testing.T) {
		matcher := NewIndexedMemoryMatcher(datoms)
		opts := planner.PlannerOptions{
			UseQueryExecutor: true,
		}
		exec := NewExecutorWithOptions(matcher, opts)
		result, err := exec.Execute(q)

		if err != nil {
			t.Logf("Error: %v", err)
			t.Logf("Result columns (if any): %v", result.Columns())
		}

		assert.NoError(t, err, "QueryExecutor should handle multiple correlated subqueries")

		// Collect results
		it := result.Iterator()
		defer it.Close()

		results := make(map[string][]float64)
		for it.Next() {
			tuple := it.Tuple()
			assert.Len(t, tuple, 5)
			date := tuple[0].(string)
			open := tuple[1].(float64)
			high := tuple[2].(float64)
			low := tuple[3].(float64)
			close := tuple[4].(float64)
			results[date] = []float64{open, high, low, close}
		}

		// Day 1: open=100, high=112, low=99, close=108
		assert.Equal(t, []float64{100.0, 112.0, 99.0, 108.0}, results["2025-01-01"])

		// Day 2: open=200, high=212, low=199, close=208
		assert.Equal(t, []float64{200.0, 212.0, 199.0, 208.0}, results["2025-01-02"])
	})
}
