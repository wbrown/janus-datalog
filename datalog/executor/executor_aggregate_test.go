package executor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestAggregateSum(t *testing.T) {
	// Create test data
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("order:1"), A: datalog.NewKeyword(":order/total"), V: 100.0, Tx: 1},
		{E: datalog.NewIdentity("order:2"), A: datalog.NewKeyword(":order/total"), V: 200.0, Tx: 2},
		{E: datalog.NewIdentity("order:3"), A: datalog.NewKeyword(":order/total"), V: 300.0, Tx: 3},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	queryStr := `[:find (sum ?total)
	              :where [?o :order/total ?total]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.Size())

	// Check the sum
	tuple := result.Get(0)
	assert.Equal(t, 600.0, tuple[0])
}

func TestAggregateCount(t *testing.T) {
	// Create test data
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("user:1"), A: datalog.NewKeyword(":user/name"), V: "Alice", Tx: 1},
		{E: datalog.NewIdentity("user:2"), A: datalog.NewKeyword(":user/name"), V: "Bob", Tx: 2},
		{E: datalog.NewIdentity("user:3"), A: datalog.NewKeyword(":user/name"), V: "Charlie", Tx: 3},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	queryStr := `[:find (count ?user)
	              :where [?user :user/name ?name]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.Size())

	// Check the count
	tuple := result.Get(0)
	assert.Equal(t, int64(3), tuple[0])
}

func TestAggregateAvg(t *testing.T) {
	// Create test data
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("score:1"), A: datalog.NewKeyword(":score/value"), V: 80.0, Tx: 1},
		{E: datalog.NewIdentity("score:2"), A: datalog.NewKeyword(":score/value"), V: 90.0, Tx: 2},
		{E: datalog.NewIdentity("score:3"), A: datalog.NewKeyword(":score/value"), V: 100.0, Tx: 3},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	queryStr := `[:find (avg ?value)
	              :where [?s :score/value ?value]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.Size())

	// Check the average
	tuple := result.Get(0)
	assert.Equal(t, 90.0, tuple[0])
}

func TestAggregateMinMax(t *testing.T) {
	// Create test data with times
	t1 := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC)
	t3 := time.Date(2024, 1, 3, 10, 0, 0, 0, time.UTC)

	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("event:1"), A: datalog.NewKeyword(":event/time"), V: t1, Tx: 1},
		{E: datalog.NewIdentity("event:2"), A: datalog.NewKeyword(":event/time"), V: t2, Tx: 2},
		{E: datalog.NewIdentity("event:3"), A: datalog.NewKeyword(":event/time"), V: t3, Tx: 3},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	queryStr := `[:find (min ?time) (max ?time)
	              :where [?e :event/time ?time]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.Size())

	// Check min and max
	tuple := result.Get(0)
	assert.Equal(t, t1, tuple[0]) // min
	assert.Equal(t, t3, tuple[1]) // max
}

func TestAggregateGroupBy(t *testing.T) {
	// Create test data - sales by department
	datoms := []datalog.Datom{
		// Engineering department
		{E: datalog.NewIdentity("sale:1"), A: datalog.NewKeyword(":sale/dept"), V: "Engineering", Tx: 1},
		{E: datalog.NewIdentity("sale:1"), A: datalog.NewKeyword(":sale/amount"), V: 1000.0, Tx: 1},
		{E: datalog.NewIdentity("sale:2"), A: datalog.NewKeyword(":sale/dept"), V: "Engineering", Tx: 2},
		{E: datalog.NewIdentity("sale:2"), A: datalog.NewKeyword(":sale/amount"), V: 1500.0, Tx: 2},
		// Sales department
		{E: datalog.NewIdentity("sale:3"), A: datalog.NewKeyword(":sale/dept"), V: "Sales", Tx: 3},
		{E: datalog.NewIdentity("sale:3"), A: datalog.NewKeyword(":sale/amount"), V: 2000.0, Tx: 3},
		{E: datalog.NewIdentity("sale:4"), A: datalog.NewKeyword(":sale/dept"), V: "Sales", Tx: 4},
		{E: datalog.NewIdentity("sale:4"), A: datalog.NewKeyword(":sale/amount"), V: 2500.0, Tx: 4},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	queryStr := `[:find ?dept (sum ?amount)
	              :where [?s :sale/dept ?dept]
	                     [?s :sale/amount ?amount]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 2, result.Size())

	// Check results (order might vary)
	results := make(map[string]float64)
	it := result.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		dept := tuple[0].(string)
		sum := tuple[1].(float64)
		results[dept] = sum
	}
	it.Close()

	assert.Equal(t, 2500.0, results["Engineering"])
	assert.Equal(t, 4500.0, results["Sales"])
}

func TestAggregateMixedWithNonAggregated(t *testing.T) {
	// This test ensures we handle the case where we have both aggregated and non-aggregated values
	datoms := []datalog.Datom{
		{E: datalog.NewIdentity("product:1"), A: datalog.NewKeyword(":product/category"), V: "Electronics", Tx: 1},
		{E: datalog.NewIdentity("product:1"), A: datalog.NewKeyword(":product/price"), V: 100.0, Tx: 1},
		{E: datalog.NewIdentity("product:2"), A: datalog.NewKeyword(":product/category"), V: "Electronics", Tx: 2},
		{E: datalog.NewIdentity("product:2"), A: datalog.NewKeyword(":product/price"), V: 200.0, Tx: 2},
		{E: datalog.NewIdentity("product:3"), A: datalog.NewKeyword(":product/category"), V: "Books", Tx: 3},
		{E: datalog.NewIdentity("product:3"), A: datalog.NewKeyword(":product/price"), V: 30.0, Tx: 3},
	}

	matcher := NewMemoryPatternMatcher(datoms)
	exec := NewExecutor(matcher)

	// Find category and average price
	queryStr := `[:find ?category (avg ?price) (count ?p)
	              :where [?p :product/category ?category]
	                     [?p :product/price ?price]]`

	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	result, err := exec.Execute(q)
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Should have 2 rows (Electronics and Books)
	assert.Equal(t, 2, result.Size())

	// Check results
	results := make(map[string]struct {
		avg   float64
		count int64
	})

	it := result.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		category := tuple[0].(string)
		avgPrice := tuple[1].(float64)
		count := tuple[2].(int64)
		results[category] = struct {
			avg   float64
			count int64
		}{avgPrice, count}
	}
	it.Close()

	// Electronics: (100 + 200) / 2 = 150
	assert.Equal(t, 150.0, results["Electronics"].avg)
	assert.Equal(t, int64(2), results["Electronics"].count)

	// Books: 30 / 1 = 30
	assert.Equal(t, 30.0, results["Books"].avg)
	assert.Equal(t, int64(1), results["Books"].count)
}
