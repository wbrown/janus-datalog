package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/planner"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPredicatePushdownIntegration(t *testing.T) {
	// Create test database
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create a transaction for writing test data
	tx := db.NewTransaction()

	// Generate test data - similar to production case
	// Create a symbol
	symbol := datalog.NewKeyword(":symbol/ticker")
	symbolEntity := datalog.NewIdentity("CRWV")

	// Create datom for symbol
	if err := tx.Add(symbolEntity, symbol, "CRWV"); err != nil {
		t.Fatalf("Failed to write symbol datom: %v", err)
	}

	// Create price datoms for multiple days
	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")
	priceOpen := datalog.NewKeyword(":price/open")

	// Generate data for June 2025
	loc, _ := time.LoadLocation("America/New_York")

	// Day 1: June 20, 2025 - 390 bars
	baseTime := time.Date(2025, 6, 20, 9, 30, 0, 0, loc)
	for i := 0; i < 390; i++ {
		barEntity := datalog.NewIdentity(fmt.Sprintf("bar-20-%d", i))
		barTime := baseTime.Add(time.Duration(i) * time.Minute)

		// Write symbol reference
		if err := tx.Add(barEntity, priceSymbol, symbolEntity); err != nil {
			t.Fatalf("Failed to write price symbol: %v", err)
		}

		// Write time
		if err := tx.Add(barEntity, priceTime, barTime); err != nil {
			t.Fatalf("Failed to write price time: %v", err)
		}

		// Write open price
		if err := tx.Add(barEntity, priceOpen, 100.0+float64(i)*0.01); err != nil {
			t.Fatalf("Failed to write price open: %v", err)
		}
	}

	// Day 2: June 21, 2025 - 390 bars
	baseTime = time.Date(2025, 6, 21, 9, 30, 0, 0, loc)
	for i := 0; i < 390; i++ {
		barEntity := datalog.NewIdentity(fmt.Sprintf("bar-21-%d", i))
		barTime := baseTime.Add(time.Duration(i) * time.Minute)

		if err := tx.Add(barEntity, priceSymbol, symbolEntity); err != nil {
			t.Fatalf("Failed to write price symbol: %v", err)
		}

		if err := tx.Add(barEntity, priceTime, barTime); err != nil {
			t.Fatalf("Failed to write price time: %v", err)
		}

		if err := tx.Add(barEntity, priceOpen, 105.0+float64(i)*0.01); err != nil {
			t.Fatalf("Failed to write price open: %v", err)
		}
	}

	// Total: 780 bars for the symbol

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Now test with and without predicate pushdown

	// Create a pattern that would fetch all bars for the symbol
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Constant{Value: symbolEntity},
		},
	}

	// Test 1: Without predicate pushdown (baseline)
	t.Run("WithoutPredicatePushdown", func(t *testing.T) {
		matcher := NewBadgerMatcher(db.store)

		// Match pattern without constraints
		rel, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Failed to match pattern: %v", err)
		}

		// Should get all 780 bars
		count := 0
		it := rel.Iterator()
		for it.Next() {
			count++
		}

		t.Logf("Without pushdown: fetched %d bars", count)
		if count != 780 {
			t.Errorf("Expected 780 bars, got %d", count)
		}
	})

	// Test 2: With predicate pushdown for day=20
	t.Run("WithPredicatePushdown", func(t *testing.T) {
		matcher := NewBadgerMatcher(db.store)

		// Create a second pattern for time
		timePattern := &query.DataPattern{
			Elements: []query.PatternElement{
				query.Variable{Name: query.Symbol("?b")},
				query.Constant{Value: datalog.NewKeyword(":price/time")},
				query.Variable{Name: query.Symbol("?t")},
			},
		}

		// Create time extraction constraint for day=20
		constraint := &timeExtractionConstraint{
			position:  2, // Value position
			extractFn: "day",
			expected:  int64(20),
		}

		// First match bars for symbol
		symbolRel, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Failed to match symbol pattern: %v", err)
		}

		// Then match with time constraint using symbol results as binding
		timeRel, err := matcher.MatchWithConstraints(
			timePattern,
			executor.Relations{symbolRel},
			[]executor.StorageConstraint{constraint},
		)
		if err != nil {
			t.Fatalf("Failed to match time pattern with constraint: %v", err)
		}

		// Should only get bars for day 20 (390 bars)
		count := 0
		it := timeRel.Iterator()
		for it.Next() {
			count++
		}

		t.Logf("With pushdown (day=20): fetched %d bars", count)
		if count != 390 {
			t.Errorf("Expected 390 bars for day 20, got %d", count)
		}
	})

	// Test 3: Verify predicate pushdown with executor integration
	t.Run("ExecutorIntegration", func(t *testing.T) {
		// This tests the full integration through the executor

		// Create a phase with pattern and predicate
		phase := &planner.Phase{
			Patterns: []planner.PatternPlan{
				{Pattern: pattern},
				{Pattern: &query.DataPattern{
					Elements: []query.PatternElement{
						query.Variable{Name: query.Symbol("?b")},
						query.Constant{Value: datalog.NewKeyword(":price/time")},
						query.Variable{Name: query.Symbol("?t")},
					},
				}},
			},
			Predicates: []planner.PredicatePlan{
				{
					Predicate: &query.FunctionPredicate{
						Fn: "day",
						Args: []query.PatternElement{
							query.Variable{Name: query.Symbol("?t")},
							query.Constant{Value: int64(20)},
						},
					},
				},
			},
			Provides: []query.Symbol{"?b", "?t"},
		}

		// Use the predicate classifier
		classifier := executor.NewPredicateClassifier(
			phase.Patterns[1].Pattern.(*query.DataPattern),
			phase,
		)

		pushable, remaining := classifier.ClassifyAndConvert()

		// Should identify the day predicate as pushable
		if len(pushable) != 1 {
			t.Errorf("Expected 1 pushable predicate, got %d", len(pushable))
		}

		if len(remaining) != 0 {
			t.Errorf("Expected 0 remaining predicates, got %d", len(remaining))
		}

		t.Logf("Successfully classified predicate as pushable: %s", pushable[0].String())
	})
}

// Helper to create time extraction constraint (matches executor's implementation)
type timeExtractionConstraint struct {
	position  int
	extractFn string
	expected  interface{}
}

func (c *timeExtractionConstraint) Evaluate(datom *datalog.Datom) bool {
	if c.position != 2 {
		return false
	}

	t, ok := datom.V.(time.Time)
	if !ok {
		return false
	}

	var extracted interface{}
	switch c.extractFn {
	case "day":
		extracted = int64(t.Day())
	default:
		return false
	}

	return extracted == c.expected
}

func (c *timeExtractionConstraint) String() string {
	return fmt.Sprintf("%s(V) = %v", c.extractFn, c.expected)
}
