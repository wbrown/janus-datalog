package tests

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestBadgerParallelProfile profiles parallel execution to find bottlenecks
func TestBadgerParallelProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping profiling test in short mode")
	}

	// Create temporary directory
	dir, err := os.MkdirTemp("", "badger-profile-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create database with test data
	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create 500 entries
	names := []string{
		"Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack",
	}

	tx := db.NewTransaction()
	idCounter := 0
	for _, name := range names {
		for year := 2020; year <= 2029; year++ {
			for month := 1; month <= 5; month++ {
				entityID := datalog.NewIdentity(fmt.Sprintf("person:%d", idCounter))
				tx.Add(entityID, datalog.NewKeyword(":name"), name)
				tx.Add(entityID, datalog.NewKeyword(":year"), int64(year))
				tx.Add(entityID, datalog.NewKeyword(":month"), int64(month))
				tx.Add(entityID, datalog.NewKeyword(":age"), int64(25+idCounter%15))
				idCounter++
			}
		}
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit test data: %v", err)
	}

	matcher := storage.NewBadgerMatcher(db.Store())
	ctx := executor.NewContext(nil)

	queryStr := `[:find ?n ?y ?m (max ?age)
	              :in $ [[?n ?y ?m] ...]
	              :where [?e :name ?n]
	                     [?e :year ?y]
	                     [?e :month ?m]
	                     [?e :age ?age]]`

	parsed, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Create input relation
	var inputTuples []executor.Tuple
	for _, name := range names {
		for year := 2020; year <= 2029; year++ {
			for month := 1; month <= 5; month++ {
				inputTuples = append(inputTuples, executor.Tuple{name, int64(year), int64(month)})
			}
		}
	}
	inputRel := executor.NewMaterializedRelation([]query.Symbol{"?n", "?y", "?m"}, inputTuples)

	// Profile parallel execution
	f, err := os.Create("badger_parallel.prof")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		t.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	// Run parallel execution
	parExec := executor.NewExecutor(matcher)
	parExec.EnableParallelSubqueries(8)

	start := time.Now()
	result, err := parExec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Parallel execution failed: %v", err)
	}

	t.Logf("Parallel execution: %v (%d results)", duration, result.Size())
	t.Logf("CPU profile written to badger_parallel.prof")
	t.Logf("Analyze with: go tool pprof badger_parallel.prof")
}
