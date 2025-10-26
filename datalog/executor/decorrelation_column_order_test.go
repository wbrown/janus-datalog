package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestParallelDecorrelationColumnOrder tests that :find clause order is preserved
// with parallel decorrelation enabled
func TestParallelDecorrelationColumnOrder(t *testing.T) {
	// Create test data
	datoms := []datalog.Datom{
		{
			E:  datalog.NewIdentity("sym1"),
			A:  datalog.NewKeyword(":symbol/ticker"),
			V:  "TEST",
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("bar1"),
			A:  datalog.NewKeyword(":price/symbol"),
			V:  datalog.NewIdentity("sym1"),
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("bar1"),
			A:  datalog.NewKeyword(":price/time"),
			V:  time.Date(2025, 1, 10, 9, 30, 0, 0, time.UTC),
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("bar1"),
			A:  datalog.NewKeyword(":price/open"),
			V:  float64(100.00),
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("bar1"),
			A:  datalog.NewKeyword(":price/high"),
			V:  float64(101.50),
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("bar1"),
			A:  datalog.NewKeyword(":price/low"),
			V:  float64(99.50),
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("bar1"),
			A:  datalog.NewKeyword(":price/close"),
			V:  float64(101.00),
			Tx: 1,
		},
		{
			E:  datalog.NewIdentity("bar1"),
			A:  datalog.NewKeyword(":price/volume"),
			V:  int64(1000000),
			Tx: 1,
		},
	}

	// Query with 3 subqueries: should return [?a, ?b, ?c] in that order
	queryStr := `
	[:find ?date ?first ?second ?third
	 :where
	   [?s :symbol/ticker "TEST"]
	   [?bar :price/symbol ?s]
	   [?bar :price/time ?t]
	   [(str ?t) ?date]

	   ; Subquery 1: high
	   [(q [:find (max ?h)
	        :in $ ?sym
	        :where [?b :price/symbol ?sym]
	               [?b :price/high ?h]]
	       $ ?s) [[?first]]]

	   ; Subquery 2: low
	   [(q [:find (min ?l)
	        :in $ ?sym
	        :where [?b :price/symbol ?sym]
	               [?b :price/low ?l]]
	       $ ?s) [[?second]]]

	   ; Subquery 3: volume
	   [(q [:find (sum ?v)
	        :in $ ?sym
	        :where [?b :price/symbol ?sym]
	               [?b :price/volume ?v]]
	       $ ?s) [[?third]]]]
	`

	parsedQuery, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Test with PARALLEL decorrelation
	t.Run("ParallelDecorrelation", func(t *testing.T) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: true, // PARALLEL
		})

		result, err := exec.Execute(parsedQuery)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if result.Size() == 0 {
			t.Fatal("Expected 1 result, got 0")
		}

		// Get the result tuple
		it := result.Iterator()
		if !it.Next() {
			t.Fatal("No result tuple")
		}
		tuple := it.Tuple()
		it.Close()

		// Verify tuple order matches :find clause
		// Expected: [?date, ?first, ?second, ?third]
		// Which should be: [date_string, 101.50 (max high), 99.50 (min low), 1000000 (sum volume)]

		if len(tuple) != 4 {
			t.Fatalf("Expected 4 columns, got %d: %v", len(tuple), tuple)
		}

		// Log what we actually got
		t.Logf("Parallel result tuple: %v", tuple)
		t.Logf("  [0] = %v (%T)", tuple[0], tuple[0])
		t.Logf("  [1] = %v (%T)", tuple[1], tuple[1])
		t.Logf("  [2] = %v (%T)", tuple[2], tuple[2])
		t.Logf("  [3] = %v (%T)", tuple[3], tuple[3])

		// Position 0: ?date (string)
		if _, ok := tuple[0].(string); !ok {
			t.Errorf("Position 0 (?date) should be string, got %T: %v", tuple[0], tuple[0])
		}

		// Position 1: ?first (max high = 101.50)
		if val, ok := tuple[1].(float64); !ok {
			t.Errorf("Position 1 (?first) should be float64, got %T: %v", tuple[1], tuple[1])
		} else if val != 101.50 {
			t.Errorf("Position 1 (?first) should be 101.50, got %v", val)
		}

		// Position 2: ?second (min low = 99.50)
		if val, ok := tuple[2].(float64); !ok {
			t.Errorf("Position 2 (?second) should be float64, got %T: %v", tuple[2], tuple[2])
		} else if val != 99.50 {
			t.Errorf("Position 2 (?second) should be 99.50, got %v", val)
		}

		// Position 3: ?third (sum volume = 1000000)
		// Note: sum() can return either int64 or float64 depending on aggregation path
		switch val := tuple[3].(type) {
		case int64:
			if val != 1000000 {
				t.Errorf("Position 3 (?third) should be 1000000, got %v", val)
			}
		case float64:
			if val != 1000000.0 {
				t.Errorf("Position 3 (?third) should be 1000000, got %v", val)
			}
		default:
			t.Errorf("Position 3 (?third) should be numeric, got %T: %v", tuple[3], tuple[3])
		}
	})

	// Test with SEQUENTIAL decorrelation (should work)
	t.Run("SequentialDecorrelation", func(t *testing.T) {
		matcher := NewMemoryPatternMatcher(datoms)
		exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
			EnableSubqueryDecorrelation: true,
			EnableParallelDecorrelation: false, // SEQUENTIAL
		})

		result, err := exec.Execute(parsedQuery)
		if err != nil {
			t.Fatalf("Query execution failed: %v", err)
		}

		if result.Size() == 0 {
			t.Fatal("Expected 1 result, got 0")
		}

		// Get the result tuple
		it := result.Iterator()
		if !it.Next() {
			t.Fatal("No result tuple")
		}
		tuple := it.Tuple()
		it.Close()

		// Verify tuple order matches :find clause
		if len(tuple) != 4 {
			t.Fatalf("Expected 4 columns, got %d: %v", len(tuple), tuple)
		}

		// Log what we actually got
		t.Logf("Sequential result tuple: %v", tuple)
		t.Logf("  [0] = %v (%T)", tuple[0], tuple[0])
		t.Logf("  [1] = %v (%T)", tuple[1], tuple[1])
		t.Logf("  [2] = %v (%T)", tuple[2], tuple[2])
		t.Logf("  [3] = %v (%T)", tuple[3], tuple[3])

		// Position 1: ?first (max high = 101.50)
		if val, ok := tuple[1].(float64); !ok {
			t.Errorf("Sequential: Position 1 (?first) should be float64, got %T: %v", tuple[1], tuple[1])
		} else if val != 101.50 {
			t.Errorf("Sequential: Position 1 (?first) should be 101.50, got %v", val)
		}

		// Position 2: ?second (min low = 99.50)
		if val, ok := tuple[2].(float64); !ok {
			t.Errorf("Sequential: Position 2 (?second) should be float64, got %T: %v", tuple[2], tuple[2])
		} else if val != 99.50 {
			t.Errorf("Sequential: Position 2 (?second) should be 99.50, got %v", val)
		}

		// Position 3: ?third (sum volume = 1000000)
		switch val := tuple[3].(type) {
		case int64:
			if val != 1000000 {
				t.Errorf("Sequential: Position 3 (?third) should be 1000000, got %v", val)
			}
		case float64:
			if val != 1000000.0 {
				t.Errorf("Sequential: Position 3 (?third) should be 1000000, got %v", val)
			}
		default:
			t.Errorf("Sequential: Position 3 (?third) should be numeric, got %T: %v", tuple[3], tuple[3])
		}
	})
}
