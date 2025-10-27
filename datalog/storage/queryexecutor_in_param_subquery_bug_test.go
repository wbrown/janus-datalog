package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestQueryExecutorInParamWithCorrelatedSubquery reproduces the gopher-street bug:
// Top-level :in parameter combined with correlated subqueries fails with
// "cannot project: column ?open-price not found in relation"
func TestQueryExecutorInParamWithCorrelatedSubquery(t *testing.T) {
	// Create temporary database
	dbPath := "/tmp/test-in-param-subquery-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Insert test data
	tx := db.NewTransaction()

	aapl := datalog.NewIdentity("AAPL")
	bar1 := datalog.NewIdentity("bar1")
	bar2 := datalog.NewIdentity("bar2")
	bar3 := datalog.NewIdentity("bar3")
	bar4 := datalog.NewIdentity("bar4")

	assert.NoError(t, tx.Add(aapl, datalog.NewKeyword(":symbol/ticker"), "AAPL"))

	// Day 1 bars
	assert.NoError(t, tx.Add(bar1, datalog.NewKeyword(":bar/symbol"), aapl))
	assert.NoError(t, tx.Add(bar1, datalog.NewKeyword(":bar/year"), int64(2025)))
	assert.NoError(t, tx.Add(bar1, datalog.NewKeyword(":bar/month"), int64(1)))
	assert.NoError(t, tx.Add(bar1, datalog.NewKeyword(":bar/day"), int64(1)))
	assert.NoError(t, tx.Add(bar1, datalog.NewKeyword(":bar/open"), 100.0))

	assert.NoError(t, tx.Add(bar2, datalog.NewKeyword(":bar/symbol"), aapl))
	assert.NoError(t, tx.Add(bar2, datalog.NewKeyword(":bar/year"), int64(2025)))
	assert.NoError(t, tx.Add(bar2, datalog.NewKeyword(":bar/month"), int64(1)))
	assert.NoError(t, tx.Add(bar2, datalog.NewKeyword(":bar/day"), int64(1)))
	assert.NoError(t, tx.Add(bar2, datalog.NewKeyword(":bar/open"), 105.0))

	// Day 2 bars (so anchor pattern returns multiple rows)
	assert.NoError(t, tx.Add(bar3, datalog.NewKeyword(":bar/symbol"), aapl))
	assert.NoError(t, tx.Add(bar3, datalog.NewKeyword(":bar/year"), int64(2025)))
	assert.NoError(t, tx.Add(bar3, datalog.NewKeyword(":bar/month"), int64(1)))
	assert.NoError(t, tx.Add(bar3, datalog.NewKeyword(":bar/day"), int64(2)))
	assert.NoError(t, tx.Add(bar3, datalog.NewKeyword(":bar/open"), 200.0))

	assert.NoError(t, tx.Add(bar4, datalog.NewKeyword(":bar/symbol"), aapl))
	assert.NoError(t, tx.Add(bar4, datalog.NewKeyword(":bar/year"), int64(2025)))
	assert.NoError(t, tx.Add(bar4, datalog.NewKeyword(":bar/month"), int64(1)))
	assert.NoError(t, tx.Add(bar4, datalog.NewKeyword(":bar/day"), int64(2)))
	assert.NoError(t, tx.Add(bar4, datalog.NewKeyword(":bar/open"), 210.0))

	_, err = tx.Commit()
	assert.NoError(t, err)

	// Query matching gopher-street EXACTLY - RelationBinding with MULTIPLE columns
	// THIS IS THE KEY: [[?daily-high ?daily-low]] returns TWO columns in one binding
	queryStr := `[:find ?date ?daily-high ?daily-low ?open-price
	              :in $ ?ticker
	              :where [?s :symbol/ticker ?ticker]

	                     ; Use anchor bar to get date (like gopher-street uses morning-bar)
	                     [?anchor-bar :bar/symbol ?s]
	                     [?anchor-bar :bar/year ?year]
	                     [?anchor-bar :bar/month ?month]
	                     [?anchor-bar :bar/day ?day]

	                     ; Expression to create date string (like gopher-street)
	                     [(str ?year "-" ?month "-" ?day) ?date]

	                     ; First subquery - HIGH AND LOW in ONE binding (like gopher-street!)
	                     [(q [:find (max ?h) (min ?l)
	                          :in $ ?sym ?y ?m ?d
	                          :where [?bar :bar/symbol ?sym]
	                                 [?bar :bar/year ?y]
	                                 [?bar :bar/month ?m]
	                                 [?bar :bar/day ?d]
	                                 [?bar :bar/open ?h]
	                                 [?bar :bar/open ?l]]
	                         $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]

	                     ; Second subquery - open price
	                     [(q [:find (min ?o)
	                          :in $ ?sym ?y ?m ?d
	                          :where [?bar :bar/symbol ?sym]
	                                 [?bar :bar/year ?y]
	                                 [?bar :bar/month ?m]
	                                 [?bar :bar/day ?d]
	                                 [?bar :bar/open ?o]]
	                         $ ?s ?year ?month ?day) [[?open-price]]]]`

	// Parse query
	q, err := parser.ParseQuery(queryStr)
	assert.NoError(t, err)

	// Convert inputs
	inputRels, err := db.convertInputsToRelations(q, []interface{}{"AAPL"})
	assert.NoError(t, err)

	// Execute with GOPHER-STREET options (EnableFineGrainedPhases = FALSE)
	gopherStreetOpts := DefaultPlannerOptions()
	gopherStreetOpts.EnableFineGrainedPhases = false  // THIS TRIGGERS THE BUG
	gopherStreetOpts.EnableDynamicReordering = false

	// Execute (like ExecuteQueryWithInputs but with custom options)
	exec := db.NewExecutorWithOptions(gopherStreetOpts)
	result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRels)

	if err != nil {
		t.Logf("BUG REPRODUCED! Error: %v", err)
		t.Logf("With EnableFineGrainedPhases=false, QueryExecutor fails with :in + correlated subqueries")
		t.Fatalf("BUG: %v", err)
	}

	// Convert result to [][]interface{}
	results := make([][]interface{}, 0)
	it := result.Iterator()
	defer it.Close()
	for it.Next() {
		results = append(results, it.Tuple())
	}

	// If we get here, the bug is fixed
	t.Logf("Bug NOT reproduced - test PASSES")
	assert.Len(t, results, 2, "Should have 2 results (one per day)")
	for _, row := range results {
		assert.Len(t, row, 4, "Each result should have 4 columns")
		t.Logf("Result: date=%v, high=%v, low=%v, open=%v", row[0], row[1], row[2], row[3])
	}
}
