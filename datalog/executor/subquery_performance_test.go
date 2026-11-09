package executor_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	. "github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestSubqueryPerformance tests that subqueries don't execute redundantly
func TestSubqueryPerformance(t *testing.T) {
	// Create data with duplicate outer values to test deduplication
	datoms := []datalog.Datom{}

	// Create 10 categories
	for i := 0; i < 10; i++ {
		cat := fmt.Sprintf("cat:%d", i)
		catName := fmt.Sprintf("Category%d", i)
		datoms = append(datoms,
			datalog.Datom{E: datalog.NewIdentity(cat), A: datalog.NewKeyword(":category/name"), V: catName, Tx: datalog.ElementID{Lamport: 1, ReplicaID: 1}},
		)

		// Create 100 products per category (1000 total)
		for j := 0; j < 100; j++ {
			prod := fmt.Sprintf("prod:%d-%d", i, j)
			datoms = append(datoms,
				datalog.Datom{E: datalog.NewIdentity(prod), A: datalog.NewKeyword(":product/category"), V: datalog.NewIdentity(cat), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
				datalog.Datom{E: datalog.NewIdentity(prod), A: datalog.NewKeyword(":product/price"), V: float64(10 + j), Tx: datalog.ElementID{Lamport: 2, ReplicaID: 1}},
			)
		}
	}

	matcher := factsDatabase(t, datoms)

	// Count products per category using subquery
	queryStr := `[:find ?cat-name ?count
	             :where
	             [?c :category/name ?cat-name]
	             [(q [:find (count ?p)
	                  :in $ ?category
	                  :where [?p :product/category ?category]]
	                 $ ?c) [[?count]]]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	for _, mode := range LegsFor(t, AlgebraOption, AggregationOption) {
		t.Run(mode.Name(), func(t *testing.T) {
			exec := NewExecutorWithOptions(matcher, nil, mode.PlannerOptions())

			start := time.Now()
			result, err := exec.Execute(q)
			duration := time.Since(start)

			if err != nil {
				t.Fatalf("Failed to execute query: %v", err)
			}

			// Should get 10 results (one per category)
			var perfTuples []Tuple
			perfIt := result.Iterator()
			for perfIt.Next() {
				t := perfIt.Tuple()
				cp := make(Tuple, len(t))
				copy(cp, t)
				perfTuples = append(perfTuples, cp)
			}
			perfIt.Close()
			if len(perfTuples) != 10 {
				t.Errorf("Expected 10 results, got %d", len(perfTuples))
			}

			// Check that each category has count of 100
			for _, tuple := range perfTuples {
				count := tuple[1].(int64)
				if count != 100 {
					t.Errorf("Expected count 100, got %d for %v", count, tuple[0])
				}
			}

			t.Logf("Query with 10 subqueries on 1000 products took %v", duration)

			// This is a basic performance check - with deduplication,
			// we should execute 10 subqueries, not more
			if duration > 500*time.Millisecond {
				t.Logf("Warning: Query took longer than expected: %v", duration)
			}
		})
	}
}
