package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPredicatePushdownDebug(t *testing.T) {
	// Create small test database
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create just 2 days of data with 10 bars each for easier debugging
	tx := db.NewTransaction()

	symbolEntity := datalog.NewIdentity("TEST")
	tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "TEST")

	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")

	loc, _ := time.LoadLocation("America/New_York")

	// Day 1: 10 bars
	for i := 0; i < 10; i++ {
		barEntity := datalog.NewIdentity(fmt.Sprintf("bar-1-%d", i))
		barTime := time.Date(2025, 6, 1, 10+i, 0, 0, 0, loc)
		tx.Add(barEntity, priceSymbol, symbolEntity)
		tx.Add(barEntity, priceTime, barTime)
	}

	// Day 2: 10 bars
	for i := 0; i < 10; i++ {
		barEntity := datalog.NewIdentity(fmt.Sprintf("bar-2-%d", i))
		barTime := time.Date(2025, 6, 2, 10+i, 0, 0, 0, loc)
		tx.Add(barEntity, priceSymbol, symbolEntity)
		tx.Add(barEntity, priceTime, barTime)
	}

	tx.Commit()

	// Test patterns
	symbolPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: priceSymbol},
			query.Constant{Value: symbolEntity},
		},
	}

	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: priceTime},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	matcher := NewBadgerMatcher(db.store)

	// First get all bars for symbol
	symbolRel, err := matcher.Match(symbolPattern, nil)
	if err != nil {
		t.Fatalf("Failed to match symbol pattern: %v", err)
	}

	// Materialize so we can iterate AND pass to Match() below (which will call Sorted())
	symbolRel = symbolRel.Materialize()

	symbolCount := 0
	it := symbolRel.Iterator()
	for it.Next() {
		symbolCount++
	}
	t.Logf("Symbol pattern matched %d bars", symbolCount)

	// Test without constraint
	t.Run("WithoutConstraint", func(t *testing.T) {
		start := time.Now()

		timeRel, err := matcher.Match(timePattern, executor.Relations{symbolRel})
		if err != nil {
			t.Fatalf("Failed to match time pattern: %v", err)
		}

		count := 0
		it := timeRel.Iterator()
		for it.Next() {
			count++
		}

		elapsed := time.Since(start)
		t.Logf("Without constraint: %d results in %v", count, elapsed)
	})

	// Test with constraint
	t.Run("WithConstraint", func(t *testing.T) {
		constraint := &timeExtractionConstraint{
			position:  2,
			extractFn: "day",
			expected:  int64(1),
		}

		start := time.Now()

		// Check what MatchWithConstraints is doing
		t.Logf("Calling MatchWithConstraints with:")
		t.Logf("  Pattern: %s", timePattern)
		t.Logf("  Binding relation size: %d", symbolRel.Size())
		t.Logf("  Constraint: %s", constraint)

		timeRel, err := matcher.MatchWithConstraints(
			timePattern,
			executor.Relations{symbolRel},
			[]executor.StorageConstraint{constraint},
		)
		if err != nil {
			t.Fatalf("Failed to match with constraint: %v", err)
		}

		count := 0
		it := timeRel.Iterator()
		for it.Next() {
			count++
		}

		elapsed := time.Since(start)
		t.Logf("With constraint: %d results in %v", count, elapsed)
	})

	// Test the actual constraint evaluation
	t.Run("ConstraintEvaluation", func(t *testing.T) {
		constraint := &timeExtractionConstraint{
			position:  2,
			extractFn: "day",
			expected:  int64(1),
		}

		// Create test datoms
		testTime1 := time.Date(2025, 6, 1, 10, 0, 0, 0, loc)
		testTime2 := time.Date(2025, 6, 2, 10, 0, 0, 0, loc)

		datom1 := &datalog.Datom{
			E: datalog.NewIdentity("test"),
			A: priceTime,
			V: testTime1,
		}

		datom2 := &datalog.Datom{
			E: datalog.NewIdentity("test"),
			A: priceTime,
			V: testTime2,
		}

		t.Logf("Constraint evaluation:")
		t.Logf("  Day 1 datom: %v -> %v", testTime1.Day(), constraint.Evaluate(datom1))
		t.Logf("  Day 2 datom: %v -> %v", testTime2.Day(), constraint.Evaluate(datom2))
	})
}
