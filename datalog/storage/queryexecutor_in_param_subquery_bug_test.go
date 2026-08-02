package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestQueryExecutorInParamWithCorrelatedSubquery guards against a bug where a
// top-level :in parameter combined with correlated subqueries fails with
// "cannot project: symbol ?open-price not found in relation"
func TestQueryExecutorInParamWithCorrelatedSubquery(t *testing.T) {
	// The key shape: a RelationBinding with MULTIPLE symbols in one binding —
	// [[?daily-high ?daily-low]] returns two symbols from a single subquery.
	queryStr := `[:find ?date ?daily-high ?daily-low ?open-price
	              :in $ ?ticker
	              :where [?s :symbol/ticker ?ticker]

	                     ; Use an anchor bar to get the date, as the reported query did
	                     [?anchor-bar :bar/symbol ?s]
	                     [?anchor-bar :bar/year ?year]
	                     [?anchor-bar :bar/month ?month]
	                     [?anchor-bar :bar/day ?day]

	                     ; Expression to create date string
	                     [(str ?year "-" ?month "-" ?day) ?date]

	                     ; First subquery - HIGH AND LOW in ONE binding
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

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

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

			// Day 2 bars (so anchor pattern returns multiple tuples)
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

			_, err := tx.Commit()
			assert.NoError(t, err)

			// Convert inputs
			inputRels, err := db.convertInputsToRelations(q, []interface{}{"AAPL"})
			assert.NoError(t, err)

			popts := mode.plannerOptions()

			// Execute (like ExecuteQueryWithInputs but with custom options)
			exec := db.NewExecutorWithOptions(popts)
			result, err := exec.ExecuteWithRelations(executor.NewContext(), q, inputRels)

			if err != nil {
				t.Logf("BUG REPRODUCED! Error: %v", err)
				t.Logf("QueryExecutor fails with :in + correlated subqueries")
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
			for _, tuple := range results {
				assert.Len(t, tuple, 4, "Each result should have 4 symbols")
				t.Logf("Result: date=%v, high=%v, low=%v, open=%v", tuple[0], tuple[1], tuple[2], tuple[3])
			}
		})
	}
}
