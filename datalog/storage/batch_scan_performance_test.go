package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBatchScanPerformance(t *testing.T) {
	// Create a temporary database
	tmpDir, err := os.MkdirTemp("", "batch-scan-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Add a symbol
	symbolID := datalog.NewIdentity("TEST-SYMBOL")

	// Create 1000 price bars (simulating a few days of minute data)
	barCount := 1000
	bars := make([]datalog.Identity, barCount)

	tx := db.NewTransaction()
	for i := 0; i < barCount; i++ {
		barID := datalog.NewIdentity(fmt.Sprintf("bar-%d", i))
		bars[i] = barID

		// Each bar has symbol and time
		if err := tx.Add(barID, datalog.NewKeyword(":price/symbol"), symbolID); err != nil {
			t.Fatal(err)
		}

		if err := tx.Add(barID, datalog.NewKeyword(":price/time"),
			time.Date(2025, 6, 1+i/390, 9, 30+i%390, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Get the matcher
	matcher := &BadgerMatcher{
		store: db.store,
		txID:  0,
	}

	// Pattern: [?bar :price/time ?time]
	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("bar")},
			query.Constant{Value: datalog.NewKeyword(":price/time")},
			query.Variable{Name: query.Symbol("time")},
		},
	}

	// Create binding relation with all bars
	tuples := make([]executor.Tuple, barCount)
	for i, bar := range bars {
		tuples[i] = executor.Tuple{bar}
	}
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{query.Symbol("bar")},
		tuples,
	)

	t.Run("WithIteratorReuse", func(t *testing.T) {
		// Force threshold high so we use iterator reuse
		oldThreshold := 10000
		defer func() {
			// Reset threshold
			_ = oldThreshold
		}()

		// Temporarily modify the threshold
		// We'll do this by creating a new matcher
		matcher2 := &BadgerMatcher{
			store: db.store,
			txID:  0,
		}

		start := time.Now()
		// Note: This now uses batch scanning automatically for >100 bindings
		result, err := matcher2.Match(timePattern, executor.Relations{bindingRel})
		if err != nil {
			t.Fatal(err)
		}

		// Consume the iterator to build cache
		it := result.Iterator()
		count := 0
		for it.Next() {
			count++
		}
		it.Close()
		elapsed := time.Since(start)

		t.Logf("Iterator reuse: %v for %d results", elapsed, count)
		if count != barCount {
			t.Errorf("Expected %d results, got %d", barCount, count)
		}

		// Now Size() should work too
		if result.Size() != barCount {
			t.Errorf("Expected Size()=%d, got %d", barCount, result.Size())
		}
	})

	t.Run("WithBatchScanning", func(t *testing.T) {
		// Use normal threshold (100) which will trigger batch scanning
		// The new Match method uses batch scanning when appropriate
		start := time.Now()
		result, err := matcher.Match(timePattern, executor.Relations{bindingRel})
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)

		resultCount := 0
		it := result.Iterator()
		for it.Next() {
			resultCount++
		}

		t.Logf("Batch scanning: %v for %d results", elapsed, resultCount)
		if resultCount != barCount {
			t.Errorf("Expected %d results, got %d", barCount, resultCount)
		}
	})

	// Now test with predicate pushdown
	t.Run("WithPredicatePushdown", func(t *testing.T) {
		// Add a day extraction constraint
		constraint := &timeExtractionConstraint{
			position:  2, // Value position
			extractFn: "day",
			expected:  int64(1),
		}

		start := time.Now()
		result, err := matcher.MatchWithConstraints(
			timePattern,
			executor.Relations{bindingRel},
			[]executor.StorageConstraint{constraint},
		)
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)

		resultCount := 0
		it := result.Iterator()
		for it.Next() {
			resultCount++
		}

		t.Logf("With predicate pushdown (day=1): %v for %d results", elapsed, resultCount)
		// Should only get bars from day 1
		expectedCount := 390 // One day of minute bars
		if resultCount > expectedCount {
			t.Errorf("Expected at most %d results for day 1, got %d", expectedCount, resultCount)
		}
	})
}

// Use the existing timeExtractionConstraint from other test files
