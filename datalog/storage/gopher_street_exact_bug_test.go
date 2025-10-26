package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestGopherStreetExactBug uses the ACTUAL gopher-street database to reproduce the bug
// This test requires ../gopher-street/datalog-db to exist
func TestGopherStreetExactBug(t *testing.T) {
	dbPath := "/Users/wbrown/go/src/github.com/wbrown/gopher-street/datalog-db"

	// Skip if gopher-street DB doesn't exist
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Skip("gopher-street database not found at", dbPath)
	}

	// Open the ACTUAL gopher-street database
	db, err := NewDatabase(dbPath)
	require.NoError(t, err)
	defer db.Close()

	// Use the EXACT query from gopher-street that shows the bug
	queryStr := `[:find ?ticker :where [?s :symbol/ticker ?ticker] [?b :price/symbol ?s]]`

	q, err := parser.ParseQuery(queryStr)
	require.NoError(t, err)

	// Use streaming options like the CLI does
	opts := executor.ExecutorOptions{
		EnableTrueStreaming: true,
	}
	matcher := NewBadgerMatcherWithOptions(db.store, opts)
	exec := executor.NewExecutor(matcher)

	result, err := exec.Execute(q)
	require.NoError(t, err)

	// Count results
	count := 0
	it := result.Iterator()
	defer it.Close()

	for it.Next() {
		count++
	}

	// gopher-street has 2 symbols (NVDA, CRWV)
	// If bug exists, count will be 0 or wrong
	require.Equal(t, 2, count, "Expected 2 symbols but got %d - tuple copying bug!", count)
}
