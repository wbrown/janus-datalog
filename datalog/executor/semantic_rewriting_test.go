package executor

import (
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/planner"
)

// TestSemanticRewritingTimePredicates verifies that time extraction predicates
// are rewritten as efficient time range constraints
func TestSemanticRewritingTimePredicates(t *testing.T) {
	// Create test data: price bars across multiple years
	var datoms []datalog.Datom

	symbolID := datalog.NewIdentity("symbol-NVDA")
	datoms = append(datoms, datalog.Datom{
		E: symbolID, A: datalog.NewKeyword(":symbol/ticker"), V: "NVDA", Tx: 1,
	})

	loc := time.UTC
	barID := 0

	// Generate bars across 3 years: 2023, 2024, 2025
	for year := 2023; year <= 2025; year++ {
		// 10 bars per year for testing
		for i := 0; i < 10; i++ {
			barTime := time.Date(year, 6, 15, 10, i*5, 0, 0, loc)
			barEntity := datalog.NewIdentity("bar-" + string(rune('0'+barID/10)) + string(rune('0'+barID%10)))
			barID++

			open := float64(100 + year - 2023 + i)

			datoms = append(datoms,
				datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: 1},
				datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
				datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/open"), V: open, Tx: 1},
			)
		}
	}

	t.Logf("Generated %d price bars across 3 years", barID)

	matcher := NewMemoryPatternMatcher(datoms)

	// Query that filters by year - this should be rewritten to time range constraint
	queryStr := `[:find ?time ?open
	             :where
	               [?s :symbol/ticker "NVDA"]
	               [?b :price/symbol ?s]
	               [?b :price/time ?time]
	               [(year ?time) ?y]
	               [(= ?y 2024)]
	               [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Execute WITH semantic rewriting
	execWithRewriting := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSemanticRewriting: true,
		EnableFineGrainedPhases: true,
		MaxPhases:               10,
	})

	resultWith, err := execWithRewriting.Execute(q)
	if err != nil {
		t.Fatalf("Execution with rewriting failed: %v", err)
	}

	// Execute WITHOUT semantic rewriting for comparison
	execWithoutRewriting := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSemanticRewriting: false,
		EnableFineGrainedPhases: true,
		MaxPhases:               10,
	})

	resultWithout, err := execWithoutRewriting.Execute(q)
	if err != nil {
		t.Fatalf("Execution without rewriting failed: %v", err)
	}

	// Both should produce identical results
	if resultWith.Size() != resultWithout.Size() {
		t.Errorf("Result size mismatch: with=%d, without=%d", resultWith.Size(), resultWithout.Size())
	}

	// Should only get 2024 bars (10 bars)
	if resultWith.Size() != 10 {
		t.Errorf("Expected 10 bars for year 2024, got %d", resultWith.Size())
	}

	// Verify all results are from 2024
	for i := 0; i < resultWith.Size(); i++ {
		row := resultWith.Get(i)
		timeVal := row[0].(time.Time)
		if timeVal.Year() != 2024 {
			t.Errorf("Row %d: expected year 2024, got %d", i, timeVal.Year())
		}
	}

	t.Logf("✓ Semantic rewriting produced correct results: %d bars from 2024", resultWith.Size())
}

// TestSemanticRewritingMultipleTimeComponents verifies that multiple time
// component predicates are composed into a single constraint
func TestSemanticRewritingMultipleTimeComponents(t *testing.T) {
	// Create test data: bars across multiple days and hours
	var datoms []datalog.Datom

	symbolID := datalog.NewIdentity("symbol-AAPL")
	datoms = append(datoms, datalog.Datom{
		E: symbolID, A: datalog.NewKeyword(":symbol/ticker"), V: "AAPL", Tx: 1,
	})

	loc := time.UTC
	barID := 0

	// Generate bars: 3 days × 3 hours × 3 bars per hour
	for day := 19; day <= 21; day++ {
		for hour := 9; hour <= 11; hour++ {
			for minute := 0; minute < 15; minute += 5 {
				barTime := time.Date(2025, 6, day, hour, minute, 0, 0, loc)
				barEntity := datalog.NewIdentity("bar-" + string(rune('0'+barID/10)) + string(rune('0'+barID%10)))
				barID++

				open := float64(150 + day + hour + minute)

				datoms = append(datoms,
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
					datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/open"), V: open, Tx: 1},
				)
			}
		}
	}

	t.Logf("Generated %d price bars", barID)

	matcher := NewMemoryPatternMatcher(datoms)

	// Query with year, month, day, and hour constraints
	queryStr := `[:find ?time ?open
	             :where
	               [?s :symbol/ticker "AAPL"]
	               [?b :price/symbol ?s]
	               [?b :price/time ?time]
	               [(year ?time) ?y]
	               [(month ?time) ?m]
	               [(day ?time) ?d]
	               [(hour ?time) ?h]
	               [(= ?y 2025)]
	               [(= ?m 6)]
	               [(= ?d 20)]
	               [(= ?h 10)]
	               [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Execute WITH semantic rewriting
	execWithRewriting := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSemanticRewriting: true,
		EnableFineGrainedPhases: true,
		MaxPhases:               10,
	})

	result, err := execWithRewriting.Execute(q)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Should only get bars from 2025-06-20 at 10:xx (3 bars: 10:00, 10:05, 10:10)
	if result.Size() != 3 {
		t.Errorf("Expected 3 bars for 2025-06-20 10:xx, got %d", result.Size())
	}

	// Verify all results match the constraints
	for i := 0; i < result.Size(); i++ {
		row := result.Get(i)
		timeVal := row[0].(time.Time)

		if timeVal.Year() != 2025 {
			t.Errorf("Row %d: expected year 2025, got %d", i, timeVal.Year())
		}
		if timeVal.Month() != 6 {
			t.Errorf("Row %d: expected month 6, got %d", i, int(timeVal.Month()))
		}
		if timeVal.Day() != 20 {
			t.Errorf("Row %d: expected day 20, got %d", i, timeVal.Day())
		}
		if timeVal.Hour() != 10 {
			t.Errorf("Row %d: expected hour 10, got %d", i, timeVal.Hour())
		}
	}

	t.Logf("✓ Multi-component semantic rewriting produced correct results: %d bars", result.Size())
}

// TestSemanticRewritingDisabled verifies that queries work correctly
// when semantic rewriting is disabled
func TestSemanticRewritingDisabled(t *testing.T) {
	// Create test data
	var datoms []datalog.Datom

	symbolID := datalog.NewIdentity("symbol-TSLA")
	datoms = append(datoms, datalog.Datom{
		E: symbolID, A: datalog.NewKeyword(":symbol/ticker"), V: "TSLA", Tx: 1,
	})

	loc := time.UTC

	// Generate 5 bars in 2024
	for i := 0; i < 5; i++ {
		barTime := time.Date(2024, 1, 1, 10, i*5, 0, 0, loc)
		barEntity := datalog.NewIdentity("bar-0" + string(rune('0'+i)))

		datoms = append(datoms,
			datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/symbol"), V: symbolID, Tx: 1},
			datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/time"), V: barTime, Tx: 1},
			datalog.Datom{E: barEntity, A: datalog.NewKeyword(":price/open"), V: float64(200 + i), Tx: 1},
		)
	}

	matcher := NewMemoryPatternMatcher(datoms)

	queryStr := `[:find ?time ?open
	             :where
	               [?s :symbol/ticker "TSLA"]
	               [?b :price/symbol ?s]
	               [?b :price/time ?time]
	               [(year ?time) ?y]
	               [(= ?y 2024)]
	               [?b :price/open ?open]]`

	q, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	// Execute WITHOUT semantic rewriting - should still work via expressions
	exec := NewExecutorWithOptions(matcher, planner.PlannerOptions{
		EnableSemanticRewriting: false,
		EnableFineGrainedPhases: true,
		MaxPhases:               10,
	})

	result, err := exec.Execute(q)
	if err != nil {
		t.Fatalf("Execution without rewriting failed: %v", err)
	}

	// Should get all 5 bars from 2024
	if result.Size() != 5 {
		t.Errorf("Expected 5 bars, got %d", result.Size())
	}

	t.Logf("✓ Query works correctly without semantic rewriting: %d bars", result.Size())
}
