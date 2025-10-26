package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestPredicatePushdownTrace(t *testing.T) {
	// Create test database
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create 3 days of data, 100 bars each
	symbolEntity := datalog.NewIdentity("TEST")

	tx := db.NewTransaction()
	tx.Add(symbolEntity, datalog.NewKeyword(":symbol/ticker"), "TEST")
	tx.Commit()

	loc, _ := time.LoadLocation("America/New_York")
	for day := 1; day <= 3; day++ {
		tx = db.NewTransaction()
		for i := 0; i < 100; i++ {
			barEntity := datalog.NewIdentity(fmt.Sprintf("bar-%d-%d", day, i))
			barTime := time.Date(2025, 6, day, 9, 30+i, 0, 0, loc)
			tx.Add(barEntity, datalog.NewKeyword(":price/symbol"), symbolEntity)
			tx.Add(barEntity, datalog.NewKeyword(":price/time"), barTime)
		}
		tx.Commit()
	}

	// Patterns
	symbolPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: datalog.NewKeyword(":price/symbol")},
			query.Constant{Value: symbolEntity},
		},
	}

	timePattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?b")},
			query.Constant{Value: datalog.NewKeyword(":price/time")},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	matcher := NewBadgerMatcher(db.store)

	// Get all bars (300 total)
	symbolRel, _ := matcher.Match(symbolPattern, nil)
	t.Logf("Symbol relation size: %d", symbolRel.Size())

	// Check what strategy is selected
	strategy := analyzeReuseStrategy(timePattern, symbolRel)
	t.Logf("Reuse strategy: Type=%s, Position=%d, Index=%d",
		strategy.Type.String(), strategy.Position, strategy.Index)

	// Now test with constraint
	constraint := &timeExtractionConstraint{
		position:  2,
		extractFn: "day",
		expected:  int64(2),
	}

	// Measure with constraint
	start := time.Now()
	timeRel, err := matcher.MatchWithConstraints(
		timePattern,
		executor.Relations{symbolRel},
		[]executor.StorageConstraint{constraint},
	)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}

	count := 0
	it := timeRel.Iterator()
	for it.Next() {
		count++
	}
	elapsed := time.Since(start)

	t.Logf("Results: %d in %v", count, elapsed)

	// Check if iterator reuse is actually happening
	if strategy.Type == SinglePositionReuse {
		t.Logf("Iterator reuse SHOULD be used (position %d, index %d)",
			strategy.Position, strategy.Index)
	} else {
		t.Logf("Iterator reuse NOT used - this explains the slowdown!")
	}
}
