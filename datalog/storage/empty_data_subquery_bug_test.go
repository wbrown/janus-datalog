package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestEmptyDataSubqueryBug reproduces the bug where queries with subqueries
// fail with projection errors when there's no matching data
func TestEmptyDataSubqueryBug(t *testing.T) {
	// Create temporary database
	dbPath := "/tmp/test-empty-subquery-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Insert ONLY symbol entity, NO price bars
	tx := db.NewTransaction()
	aapl := datalog.NewIdentity("AAPL")
	assert.NoError(t, tx.Add(aapl, datalog.NewKeyword(":symbol/ticker"), "AAPL"))
	_, err = tx.Commit()
	assert.NoError(t, err)

	// Query with subquery - simplified version of gopher-street OHLC query
	// This should return empty results gracefully, not fail with projection error
	query := `[:find ?date ?open-price
	 :in $ ?symbol
	 :where
	        [?s :symbol/ticker ?symbol]
	        [?morning-bar :price/symbol ?s]
	        [?morning-bar :price/time ?t]
	        [(year ?t) ?year]
	        [(month ?t) ?month]
	        [(day ?t) ?day]
	        [(str ?year "-" ?month "-" ?day) ?date]

	        [(q [:find (min ?o)
	             :in $ ?sym ?y ?m ?d
	             :where [?b :price/symbol ?sym]
	                    [?b :price/open ?o]]
	            $ ?s ?year ?month ?day) [[?open-price]]]]`

	// Execute with default options (EnableFineGrainedPhases=true)
	results, err := db.ExecuteQueryWithInputs(query, "AAPL")

	// Should succeed with empty results, NOT fail with projection error
	if err != nil {
		t.Logf("BUG REPRODUCED! Error: %v", err)
		t.Fatalf("Query with empty data should return empty results, not fail: %v", err)
	}

	assert.Len(t, results, 0, "Should return empty results when no data matches")
	t.Logf("SUCCESS: Query returned %d results (empty as expected)", len(results))
}

// TestEmptyDataSubqueryBug_FullGopherStreetQuery tests with the EXACT gopher-street query
func TestEmptyDataSubqueryBug_FullGopherStreetQuery(t *testing.T) {
	// Create temporary database
	dbPath := "/tmp/test-empty-full-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Insert ONLY symbol entity, NO price bars
	tx := db.NewTransaction()
	aapl := datalog.NewIdentity("AAPL")
	assert.NoError(t, tx.Add(aapl, datalog.NewKeyword(":symbol/ticker"), "AAPL"))
	_, err = tx.Commit()
	assert.NoError(t, err)

	// EXACT gopher-street query with 4 subqueries
	query := `[:find ?date ?open-price ?daily-high ?daily-low ?close-price ?total-volume
	 :in $ ?symbol
	 :where
	        [?s :symbol/ticker ?symbol]
	        [?morning-bar :price/symbol ?s]
	        [?morning-bar :price/minute-of-day 570]
	        [?morning-bar :price/time ?t]
	        [(year ?t) ?year]
	        [(month ?t) ?month]
	        [(day ?t) ?day]
	        [(str ?year "-" ?month "-" ?day) ?date]

	        [(q [:find (max ?h) (min ?l)
	             :in $ ?sym ?y ?m ?d
	             :where [?b :price/symbol ?sym]
	                    [?b :price/time ?time]
	                    [(year ?time) ?py]
	                    [(month ?time) ?pm]
	                    [(day ?time) ?pd]
	                    [(= ?py ?y)]
	                    [(= ?pm ?m)]
	                    [(= ?pd ?d)]
	                    [?b :price/minute-of-day ?mod]
	                    [(>= ?mod 570)]
	                    [(<= ?mod 960)]
	                    [?b :price/high ?h]
	                    [?b :price/low ?l]]
	            $ ?s ?year ?month ?day) [[?daily-high ?daily-low]]]

	        [(q [:find (min ?o)
	             :in $ ?sym ?y ?m ?d
	             :where [?b :price/symbol ?sym]
	                    [?b :price/time ?time]
	                    [(year ?time) ?py]
	                    [(month ?time) ?pm]
	                    [(day ?time) ?pd]
	                    [(= ?py ?y)]
	                    [(= ?pm ?m)]
	                    [(= ?pd ?d)]
	                    [?b :price/minute-of-day ?mod]
	                    [(>= ?mod 570)]
	                    [(<= ?mod 575)]
	                    [?b :price/open ?o]]
	            $ ?s ?year ?month ?day) [[?open-price]]]

	        [(q [:find (max ?c)
	             :in $ ?sym ?y ?m ?d
	             :where [?b :price/symbol ?sym]
	                    [?b :price/time ?time]
	                    [(year ?time) ?py]
	                    [(month ?time) ?pm]
	                    [(day ?time) ?pd]
	                    [(= ?py ?y)]
	                    [(= ?pm ?m)]
	                    [(= ?pd ?d)]
	                    [?b :price/minute-of-day ?mod]
	                    [(>= ?mod 955)]
	                    [(<= ?mod 960)]
	                    [?b :price/close ?c]]
	            $ ?s ?year ?month ?day) [[?close-price]]]

	        [(q [:find (sum ?v)
	             :in $ ?sym ?y ?m ?d
	             :where [?b :price/symbol ?sym]
	                    [?b :price/time ?time]
	                    [(year ?time) ?py]
	                    [(month ?time) ?pm]
	                    [(day ?time) ?pd]
	                    [(= ?py ?y)]
	                    [(= ?pm ?m)]
	                    [(= ?pd ?d)]
	                    [?b :price/minute-of-day ?mod]
	                    [(>= ?mod 570)]
	                    [(<= ?mod 960)]
	                    [?b :price/volume ?v]]
	            $ ?s ?year ?month ?day) [[?total-volume]]]]`

	// Execute with default options (EnableFineGrainedPhases=true)
	results, err := db.ExecuteQueryWithInputs(query, "AAPL")

	if err != nil {
		t.Logf("BUG REPRODUCED! Error: %v", err)
		t.Fatalf("Query with empty data should return empty results, not fail: %v", err)
	}

	assert.Len(t, results, 0, "Should return empty results when no data matches")
	t.Logf("SUCCESS: Full gopher-street query returned %d results (empty as expected)", len(results))
}

// TestEmptyDataSubqueryBug_WithOptions tests with explicit planner options
func TestEmptyDataSubqueryBug_WithOptions(t *testing.T) {
	// Create temporary database
	dbPath := "/tmp/test-empty-subquery-opts-" + t.Name()
	defer os.RemoveAll(dbPath)

	db, err := NewDatabase(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	// Insert ONLY symbol entity, NO price bars
	tx := db.NewTransaction()
	aapl := datalog.NewIdentity("AAPL")
	assert.NoError(t, tx.Add(aapl, datalog.NewKeyword(":symbol/ticker"), "AAPL"))
	_, err = tx.Commit()
	assert.NoError(t, err)

	// Same query as above
	queryStr := `[:find ?date ?open-price
	 :in $ ?symbol
	 :where
	        [?s :symbol/ticker ?symbol]
	        [?morning-bar :price/symbol ?s]
	        [?morning-bar :price/time ?t]
	        [(year ?t) ?year]
	        [(month ?t) ?month]
	        [(day ?t) ?day]
	        [(str ?year "-" ?month "-" ?day) ?date]

	        [(q [:find (min ?o)
	             :in $ ?sym ?y ?m ?d
	             :where [?b :price/symbol ?sym]
	                    [?b :price/open ?o]]
	            $ ?s ?year ?month ?day) [[?open-price]]]]`

	// Test with EnableFineGrainedPhases=true (default)
	t.Run("EnableFineGrainedPhases=true", func(t *testing.T) {
		opts := DefaultPlannerOptions()
		opts.EnableFineGrainedPhases = true

		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		inputRels, err := db.convertInputsToRelations(q, []interface{}{"AAPL"})
		assert.NoError(t, err)

		exec := db.NewExecutorWithOptions(opts)
		result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRels)

		if err != nil {
			t.Logf("BUG: %v", err)
			t.Fatalf("Should succeed with empty results: %v", err)
		}

		// Convert to rows
		it := result.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
		}
		assert.Equal(t, 0, count, "Should have 0 results")
	})

	// Test with EnableFineGrainedPhases=false
	t.Run("EnableFineGrainedPhases=false", func(t *testing.T) {
		opts := DefaultPlannerOptions()
		opts.EnableFineGrainedPhases = false

		q, err := parser.ParseQuery(queryStr)
		assert.NoError(t, err)

		inputRels, err := db.convertInputsToRelations(q, []interface{}{"AAPL"})
		assert.NoError(t, err)

		exec := db.NewExecutorWithOptions(opts)
		result, err := exec.ExecuteWithRelations(executor.NewContext(nil), q, inputRels)

		assert.NoError(t, err, "Should succeed regardless of EnableFineGrainedPhases setting")

		// Convert to rows
		it := result.Iterator()
		defer it.Close()
		count := 0
		for it.Next() {
			count++
		}
		assert.Equal(t, 0, count, "Should have 0 results")
	})
}
