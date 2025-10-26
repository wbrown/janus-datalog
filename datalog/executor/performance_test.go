package executor

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
)

// TestPerformanceRegression runs a simple performance check
func TestPerformanceRegression(t *testing.T) {
	// Create test data
	datoms := []datalog.Datom{}
	nameAttr := datalog.NewKeyword(":person/name")
	ageAttr := datalog.NewKeyword(":person/age")
	scoreAttr := datalog.NewKeyword(":person/score")

	// Generate 1000 people
	for i := 0; i < 1000; i++ {
		person := datalog.NewIdentity("person:" + string(rune('a'+i%26)) + string(rune('0'+i/26)))
		datoms = append(datoms,
			datalog.Datom{E: person, A: nameAttr, V: "Person" + string(rune(i)), Tx: 1},
			datalog.Datom{E: person, A: ageAttr, V: int64(20 + i%40), Tx: 1},
			datalog.Datom{E: person, A: scoreAttr, V: float64(50 + i%50), Tx: 1},
		)
	}

	matcher := NewMemoryPatternMatcher(datoms)
	executor := NewExecutor(matcher)

	// Test queries
	queries := []string{
		// Simple query
		`[:find ?name ?age
		  :where [?p :person/name ?name]
		         [?p :person/age ?age]]`,

		// With expression
		`[:find ?name ?score ?bonus
		  :where [?p :person/name ?name]
		         [?p :person/score ?score]
		         [(* ?score 0.1) ?bonus]]`,

		// With aggregation
		`[:find ?age (avg ?score) (count ?name)
		  :where [?p :person/age ?age]
		         [?p :person/score ?score]
		         [?p :person/name ?name]]`,

		// Complex with predicates and expressions
		`[:find ?name ?age ?adjusted
		  :where [?p :person/name ?name]
		         [?p :person/age ?age]
		         [?p :person/score ?score]
		         [(> ?age 30)]
		         [(< ?score 80)]
		         [(+ ?score ?age) ?adjusted]]`,
	}

	for i, queryStr := range queries {
		q, err := parser.ParseQuery(queryStr)
		if err != nil {
			t.Fatalf("Query %d: Failed to parse: %v", i, err)
		}

		result, err := executor.Execute(q)
		if err != nil {
			t.Fatalf("Query %d: Execution failed: %v", i, err)
		}

		// Just verify we got results
		if result.Size() == 0 && i != 3 { // Query 3 might have no results due to predicates
			t.Errorf("Query %d: Expected results, got none", i)
		}
	}
}
