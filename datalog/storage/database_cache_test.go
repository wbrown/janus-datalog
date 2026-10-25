package storage

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

func TestDatabasePlanCache(t *testing.T) {
	// Parse a query
	queryStr := `[:find ?e ?name ?age
	              :where [?e :person/name ?name]
	                     [?e :person/age ?age]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Add some test data
			tx := db.NewTransaction()
			person1 := datalog.NewIdentity("person1")
			person2 := datalog.NewIdentity("person2")

			tx.Add(person1, datalog.NewKeyword(":person/name"), "Alice")
			tx.Add(person2, datalog.NewKeyword(":person/name"), "Bob")
			tx.Add(person1, datalog.NewKeyword(":person/age"), int64(30))
			tx.Add(person2, datalog.NewKeyword(":person/age"), int64(25))

			if _, err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}

			// Test 1: Create multiple executors - they should share the cache
			exec1 := db.NewExecutorWithOptions(mode.plannerOptions())
			exec2 := db.NewExecutorWithOptions(mode.plannerOptions())

			// Debug: Check that cache is not nil
			if db.PlanCache() == nil {
				t.Fatal("Database plan cache is nil")
			}

			// Clear cache to start fresh
			db.ClearPlanCache()

			// First execution should miss the cache: the query's plan and the
			// plan of the scope its second pattern runs over the first's
			// streaming group, each planned under its own text.
			tuples1, err := executor.CollectTuples(exec1.Execute(q))
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			if len(tuples1) != 2 {
				t.Errorf("Expected 2 results, got %d", len(tuples1))
			}

			// Check cache stats - should have 2 misses, the query's and the scope's
			hits, misses, size := db.PlanCache().Stats()
			if misses != 2 {
				t.Errorf("Expected 2 misses, got %d", misses)
			}
			if size != 2 {
				t.Errorf("Expected cache size 2, got %d", size)
			}

			// Second execution from different executor should hit the cache
			tuples2, err := executor.CollectTuples(exec2.Execute(q))
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}
			if len(tuples2) != 2 {
				t.Errorf("Expected 2 results, got %d", len(tuples2))
			}

			// Check cache stats - should now have 2 hits
			hits, misses, size = db.PlanCache().Stats()
			if hits != 2 {
				t.Errorf("Expected 2 hits, got %d", hits)
			}
			if misses != 2 {
				t.Errorf("Expected 2 misses (unchanged), got %d", misses)
			}

			// Test 2: Clearing database cache affects all executors
			db.ClearPlanCache()

			hits, misses, size = db.PlanCache().Stats()
			if hits != 0 || misses != 0 || size != 0 {
				t.Error("Expected cache to be cleared")
			}

			// Next execution should miss again
			_, err = executor.CollectTuples(exec1.Execute(q))
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			hits, misses, size = db.PlanCache().Stats()
			if misses != 2 {
				t.Errorf("Expected 2 misses after clear, got %d", misses)
			}
		})
	}
}

// A query whose input binds a symbol is planned once per database: its second
// execution is served from the cache. A query run with an empty collection
// input and again with a non-empty one holds two entries of its text, one per
// bound set.
func TestDatabasePlanCacheServesAPlanMadeUnderABoundInput(t *testing.T) {
	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})
			tx := db.NewTransaction()
			person := datalog.NewIdentity("person1")
			if err := tx.Add(person, datalog.NewKeyword(":person/name"), "Alice"); err != nil {
				t.Fatal(err)
			}
			if err := tx.Add(person, datalog.NewKeyword(":person/age"), int64(30)); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			scalar := `[:find ?name :in $ ?age :where [?e :person/age ?age] [?e :person/name ?name]]`
			var names []string
			if err := db.QueryInto(&names, scalar, int64(30)); err != nil {
				t.Fatal(err)
			}
			_, missesAfterFirst, sizeAfterFirst := db.PlanCache().Stats()
			names = nil
			if err := db.QueryInto(&names, scalar, int64(30)); err != nil {
				t.Fatal(err)
			}
			if len(names) != 1 || names[0] != "Alice" {
				t.Fatalf("names = %v, want Alice", names)
			}
			hits, misses, size := db.PlanCache().Stats()
			if misses != missesAfterFirst || size != sizeAfterFirst || hits == 0 {
				t.Errorf("stats = %d hits, %d misses, size %d after the second execution; want no new miss, the same size, and a hit: the plan made under the bound input is served again", hits, misses, size)
			}

			collection := `[:find ?name :in $ [?age ...] :where [?e :person/age ?age] [?e :person/name ?name]]`
			names = nil
			if err := db.QueryInto(&names, collection, []int64{}); err != nil {
				t.Fatal(err)
			}
			_, missesAfterEmpty, sizeAfterEmpty := db.PlanCache().Stats()
			names = nil
			if err := db.QueryInto(&names, collection, []int64{30}); err != nil {
				t.Fatal(err)
			}
			_, missesAfterBound, sizeAfterBound := db.PlanCache().Stats()
			if missesAfterBound != missesAfterEmpty+1 || sizeAfterBound != sizeAfterEmpty+1 {
				t.Errorf("misses %d and size %d after the non-empty input, against %d and %d after the empty one; want one more of each: the plan under the bound symbol is another entry of the text", missesAfterBound, sizeAfterBound, missesAfterEmpty, sizeAfterEmpty)
			}
		})
	}
}

func TestDatabaseWithoutCache(t *testing.T) {
	// Parse a query
	queryStr := `[:find ?e :where [?e :person/name _]]`
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Disable caching
			db.SetPlanCache(nil)

			// Create executor - should work without cache
			exec := db.NewExecutorWithOptions(mode.plannerOptions())

			// Execute should work without cache
			_, err := exec.Execute(q)
			if err != nil {
				t.Fatalf("Failed to execute query without cache: %v", err)
			}

			// Plan cache should be nil
			if db.PlanCache() != nil {
				t.Error("Expected plan cache to be nil")
			}
		})
	}
}

// TestExecutorCreationOverhead verifies that creating executors is cheap
func TestExecutorCreationOverhead(t *testing.T) {
	// Basic sanity check that they work
	queryStr := `[:find ?e :where [?e _ _]]`
	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	for _, mode := range optimizerModes {
		t.Run(mode.name, func(t *testing.T) {
			db := createOptimizerModeDB(t, mode, DatabaseOptions{})

			// Create many executors - should be fast and low memory
			executors := make([]*executor.Executor, 1000)
			for i := 0; i < 1000; i++ {
				executors[i] = db.NewExecutorWithOptions(mode.plannerOptions())
			}

			// All should share the same cache (we can't directly compare function pointers,
			// but we can verify they work with the shared cache)

			// First executor execution
			_, err := executors[0].Execute(q)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Last executor should benefit from cache
			_, err = executors[999].Execute(q)
			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Check that cache was used
			hits, _, _ := db.PlanCache().Stats()
			if hits < 1 {
				t.Error("Expected at least 1 cache hit from shared cache")
			}
		})
	}
}
