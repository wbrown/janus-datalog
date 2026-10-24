package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestRelationInputOverStorage: a relation input of five hundred tuples over a
// stored dataset answers one tuple per input tuple, carrying the age the
// fixture gave that tuple's entity, on every backend and both planner paths.
func TestRelationInputOverStorage(t *testing.T) {
	for _, backend := range storage.AvailableBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			for _, mode := range optimizerModes {
				t.Run(mode.name, func(t *testing.T) {
					testRelationInputOverStorage(t, backend, mode)
				})
			}
		})
	}
}

func testRelationInputOverStorage(t *testing.T, backend storage.Backend, mode optimizerMode) {
	db := createBackendModeDB(t, backend, mode, storage.DatabaseOptions{})

	// Create 500 entries (10 names × 10 years × 5 months)
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
	_, err := tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit test data: %v", err)
	}

	// Create matcher from database
	matcher := storage.NewPatternMatcher(db.Store())
	ctx := executor.NewContext()

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

	// Create input relation (500 tuples), and the max age each tuple's
	// entity carries, from the same rule the fixture wrote it by.
	var inputTuples []executor.Tuple
	want := make(map[string]int64, 500)
	i := 0
	for _, name := range names {
		for year := 2020; year <= 2029; year++ {
			for month := 1; month <= 5; month++ {
				inputTuples = append(inputTuples, executor.Tuple{name, int64(year), int64(month)})
				want[fmt.Sprintf("%s-%d-%d", name, year, month)] = int64(25 + i%15)
				i++
			}
		}
	}
	inputRel := executor.NewMaterializedRelation([]query.Symbol{datalog.NewSymbol("?n"), datalog.NewSymbol("?y"), datalog.NewSymbol("?m")}, inputTuples)

	t.Run("five hundred input tuples", func(t *testing.T) {
		exec := executor.NewExecutorWithOptions(matcher, nil, mode.plannerOptions())

		start := time.Now()
		result, err := exec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}

		if result.Size() != 500 {
			t.Errorf("Expected 500 results, got %d", result.Size())
		}

		// Collect the result into a map for order-independent comparison
		got := make(map[string]int64)
		it := result.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			if len(tuple) == 4 {
				name := tuple[0].(string)
				year := tuple[1].(int64)
				month := tuple[2].(int64)
				maxAge := tuple[3].(int64)
				key := fmt.Sprintf("%s-%d-%d", name, year, month)
				got[key] = maxAge
			}
		}
		it.Close()

		if len(got) != len(want) {
			t.Errorf("Result count mismatch: got=%d, want=%d", len(got), len(want))
		}

		for key, wantVal := range want {
			if gotVal, ok := got[key]; !ok {
				t.Errorf("Key %s missing in results", key)
			} else if gotVal != wantVal {
				t.Errorf("Value mismatch for %s: got=%d, want=%d", key, gotVal, wantVal)
			}
		}

		for key := range got {
			if _, ok := want[key]; !ok {
				t.Errorf("Key %s in results but not expected", key)
			}
		}

		t.Logf("500 tuples: %v (%d results)", duration, result.Size())
	})
}
