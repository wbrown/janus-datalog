package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

func TestParallelSubqueryWithBadgerDB(t *testing.T) {
	// Create temporary directory for test database
	dir, err := os.MkdirTemp("", "badger-parallel-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create database
	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create 500 entries (10 names × 10 years × 5 months)
	// Balanced to show real parallel benefits without excessive runtime
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

	// Create matcher from database
	matcher := storage.NewBadgerMatcher(db.Store())
	ctx := executor.NewContext(nil)

	// Query to test
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

	// Create input relation (500 tuples)
	var inputTuples []executor.Tuple
	for _, name := range names {
		for year := 2020; year <= 2029; year++ {
			for month := 1; month <= 5; month++ {
				inputTuples = append(inputTuples, executor.Tuple{name, int64(year), int64(month)})
			}
		}
	}
	inputRel := executor.NewMaterializedRelation([]query.Symbol{"?n", "?y", "?m"}, inputTuples)

	t.Run("sequential with BadgerDB", func(t *testing.T) {
		seqExec := executor.NewExecutor(matcher)
		seqExec.DisableParallelSubqueries()

		start := time.Now()
		seqResult, err := seqExec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
		seqDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Sequential query failed: %v", err)
		}

		if seqResult.Size() != 500 {
			t.Errorf("Expected 500 results, got %d", seqResult.Size())
		}

		t.Logf("Sequential (BadgerDB): %v (%d results)", seqDuration, seqResult.Size())
	})

	t.Run("parallel with BadgerDB", func(t *testing.T) {
		parExec := executor.NewExecutor(matcher)
		parExec.EnableParallelSubqueries(8)

		start := time.Now()
		parResult, err := parExec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
		parDuration := time.Since(start)

		if err != nil {
			t.Fatalf("Parallel query failed: %v", err)
		}

		if parResult.Size() != 500 {
			t.Errorf("Expected 500 results, got %d", parResult.Size())
		}

		t.Logf("Parallel (BadgerDB, 8 workers): %v (%d results)", parDuration, parResult.Size())
	})

	t.Run("correctness and performance: sequential vs parallel", func(t *testing.T) {
		// Sequential execution
		seqExec := executor.NewExecutor(matcher)
		seqExec.DisableParallelSubqueries()
		seqStart := time.Now()
		seqResult, err := seqExec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
		seqDuration := time.Since(seqStart)
		if err != nil {
			t.Fatalf("Sequential query failed: %v", err)
		}

		// Parallel execution
		parExec := executor.NewExecutor(matcher)
		parExec.EnableParallelSubqueries(8)
		parStart := time.Now()
		parResult, err := parExec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
		parDuration := time.Since(parStart)
		if err != nil {
			t.Fatalf("Parallel query failed: %v", err)
		}

		// Compare sizes
		if seqResult.Size() != parResult.Size() {
			t.Errorf("Size mismatch: sequential=%d, parallel=%d", seqResult.Size(), parResult.Size())
		}

		// Compare results (collect into maps for order-independent comparison)
		seqMap := make(map[string]int64)
		it := seqResult.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			if len(tuple) == 4 {
				name := tuple[0].(string)
				year := tuple[1].(int64)
				month := tuple[2].(int64)
				maxAge := tuple[3].(int64)
				key := fmt.Sprintf("%s-%d-%d", name, year, month)
				seqMap[key] = maxAge
			}
		}
		it.Close()

		parMap := make(map[string]int64)
		it = parResult.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			if len(tuple) == 4 {
				name := tuple[0].(string)
				year := tuple[1].(int64)
				month := tuple[2].(int64)
				maxAge := tuple[3].(int64)
				key := fmt.Sprintf("%s-%d-%d", name, year, month)
				parMap[key] = maxAge
			}
		}
		it.Close()

		// Verify maps are identical
		if len(seqMap) != len(parMap) {
			t.Errorf("Result count mismatch: sequential=%d, parallel=%d", len(seqMap), len(parMap))
		}

		for key, seqVal := range seqMap {
			if parVal, ok := parMap[key]; !ok {
				t.Errorf("Key %s missing in parallel results", key)
			} else if seqVal != parVal {
				t.Errorf("Value mismatch for %s: sequential=%d, parallel=%d", key, seqVal, parVal)
			}
		}

		for key := range parMap {
			if _, ok := seqMap[key]; !ok {
				t.Errorf("Key %s in parallel results but not in sequential", key)
			}
		}

		// Report performance
		speedup := float64(seqDuration) / float64(parDuration)
		t.Logf("BadgerDB Performance (500 tuples):")
		t.Logf("  Sequential: %v", seqDuration)
		t.Logf("  Parallel (8 workers): %v", parDuration)
		t.Logf("  Speedup: %.2fx", speedup)

		if speedup < 2.0 {
			t.Logf("WARNING: Speedup %.2fx is less than expected (target: >2.5x)", speedup)
		} else {
			t.Logf("SUCCESS: Parallel execution with BadgerDB achieved %.2fx speedup", speedup)
		}
	})
}
