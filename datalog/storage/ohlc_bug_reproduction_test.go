package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestOHLCQueryBug reproduces the exact bug from gopher-street TEST 2
// This test should FAIL (hang or return wrong results) until the tuple copying bug is fixed
func TestOHLCQueryBug(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping slow bug reproduction test")
	}

	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	require.NoError(t, err)
	defer db.Close()

	// Insert realistic OHLC data matching gopher-street schema
	tx := db.NewTransaction()

	// Symbol
	symbol := datalog.NewIdentity("CRWV")
	tx.Add(symbol, datalog.NewKeyword(":symbol/ticker"), "CRWV")

	// Create 100 price bars (enough to trigger the bug but not too slow)
	baseTime := time.Date(2025, 6, 20, 9, 30, 0, 0, time.UTC)

	for i := 0; i < 100; i++ {
		barID := datalog.NewIdentity(fmt.Sprintf("bar-%d", i))
		barTime := baseTime.Add(time.Duration(i) * time.Minute)
		minuteOfDay := int64(570 + i) // 9:30 AM = minute 570

		tx.Add(barID, datalog.NewKeyword(":price/symbol"), symbol)
		tx.Add(barID, datalog.NewKeyword(":price/time"), barTime)
		tx.Add(barID, datalog.NewKeyword(":price/minute-of-day"), minuteOfDay)
		tx.Add(barID, datalog.NewKeyword(":price/open"), float64(100+i))
		tx.Add(barID, datalog.NewKeyword(":price/high"), float64(105+i))
		tx.Add(barID, datalog.NewKeyword(":price/low"), float64(95+i))
		tx.Add(barID, datalog.NewKeyword(":price/close"), float64(102+i))
		tx.Add(barID, datalog.NewKeyword(":price/volume"), int64(1000+i))
	}

	_, err = tx.Commit()
	require.NoError(t, err)

	// The exact query from gopher-street TEST 2 that hangs
	queryStr := `[:find ?year ?month ?day (min ?open) (max ?high) (min ?low) (max ?close) (sum ?volume)
	              :where [?s :symbol/ticker "CRWV"]
	                     [?e :price/symbol ?s]
	                     [?e :price/time ?time]
	                     [(year ?time) ?year]
	                     [(month ?time) ?month]
	                     [(day ?time) ?day]
	                     [?e :price/minute-of-day ?mod]
	                     [(>= ?mod 570)]
	                     [(<= ?mod 960)]
	                     [?e :price/open ?open]
	                     [?e :price/high ?high]
	                     [?e :price/low ?low]
	                     [?e :price/close ?close]
	                     [?e :price/volume ?volume]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	matcher := NewBadgerMatcher(db.store)
	exec := executor.NewExecutor(matcher)

	// Set a timeout channel to prevent hanging forever
	done := make(chan bool)
	var result executor.Relation
	var execErr error

	go func() {
		result, execErr = exec.Execute(q)
		done <- true
	}()

	select {
	case <-done:
		// Query completed
		require.NoError(t, execErr)

		// Count results
		count := 0
		it := result.Iterator()
		for it.Next() {
			count++
		}
		it.Close()

		// Should have 1 row (grouped by year/month/day)
		// If bug exists, might get 0 rows or hang
		require.Greater(t, count, 0, "Query returned 0 results - tuple copying bug in matcher_relations.go:241")

	case <-time.After(30 * time.Second):
		t.Fatal("Query hung for 30 seconds - likely due to tuple copying bug causing infinite loop or massive data")
	}
}
