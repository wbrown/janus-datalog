package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBatchScanTrace(t *testing.T) {
	// Create test with 200 entities (should trigger batch scanning)
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	symbolEntity := datalog.NewIdentity("SYM")
	tx := db.NewTransaction()
	tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "SYM")
	tx.Commit()

	priceSymbol := datalog.NewKeyword(":price/symbol")
	priceTime := datalog.NewKeyword(":price/time")

	loc, _ := time.LoadLocation("America/New_York")

	// Create 200 bars - 100 on day 1, 100 on day 2
	tx = db.NewTransaction()
	for day := 1; day <= 2; day++ {
		for i := 0; i < 100; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%d-%d", day, i))
			barTime := time.Date(2025, 6, day, 9, 30+i, 0, 0, loc)
			tx.Add(barEntity, priceSymbol, symbolEntity)
			tx.Add(barEntity, priceTime, barTime)
		}
	}
	tx.Commit()

	// First pattern: get all bars for symbol
	symbolPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: priceSymbol},
			query.Constant{Value: symbolEntity},
		},
	}

	// Second pattern: get times
	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: priceTime},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	matcher := NewBadgerMatcher(db.store)

	// Get all bars
	symbolRel, err := matcher.Match(symbolPattern, nil)
	if err != nil {
		t.Fatalf("Failed to get symbol bars: %v", err)
	}

	// Materialize so we can iterate multiple times
	symbolRel = symbolRel.Materialize()

	barCount := 0
	it := symbolRel.Iterator()
	for it.Next() {
		barCount++
	}
	t.Logf("Found %d bars for symbol", barCount)

	// Test without bindings first
	t.Logf("Testing pattern without bindings...")
	timeRelDirect, err := matcher.Match(timePattern, nil)
	if err != nil {
		t.Fatalf("Failed direct match: %v", err)
	}

	directCount := 0
	itDirect := timeRelDirect.Iterator()
	for itDirect.Next() {
		directCount++
	}
	t.Logf("Direct match (no bindings): %d results", directCount)

	// Now test with constraint
	constraint := &timeExtractionConstraint{
		position:  2,
		extractFn: "day",
		expected:  int64(1),
	}

	// This should trigger batch scanning (200 > 100 threshold)
	t.Logf("Calling MatchWithConstraints with %d bindings (should use batch scanning)", barCount)

	timeRel, err := matcher.MatchWithConstraints(
		timePattern,
		executor.Relations{symbolRel},
		[]executor.StorageConstraint{constraint},
	)
	if err != nil {
		t.Fatalf("Failed with constraint: %v", err)
	}

	resultCount := 0
	it2 := timeRel.Iterator()
	for it2.Next() {
		resultCount++
		if resultCount <= 3 {
			tuple := it2.Tuple()
			t.Logf("Sample result %d: %v", resultCount, tuple)
		}
	}

	t.Logf("Got %d results for day 1", resultCount)

	if resultCount != 100 {
		t.Errorf("Expected 100 results for day 1, got %d", resultCount)

		// Try without batch scanning to compare
		t.Logf("Trying without constraints to debug...")
		timeRel2, _ := matcher.Match(timePattern, executor.Relations{symbolRel})

		debugCount := 0
		totalDebug := 0
		it3 := timeRel2.Iterator()
		for it3.Next() {
			tuple := it3.Tuple()
			totalDebug++
			// Check the time value
			if len(tuple) >= 2 {
				if tm, ok := tuple[1].(time.Time); ok {
					if totalDebug <= 3 {
						t.Logf("Debug tuple %d: time=%v, day=%d", totalDebug, tm, tm.Day())
					}
					if tm.Day() == 1 {
						debugCount++
					}
				}
			}
		}
		t.Logf("Total tuples: %d, Day 1 count: %d", totalDebug, debugCount)
	}
}
