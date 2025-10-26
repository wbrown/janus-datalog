package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

func TestBatchScanDebug(t *testing.T) {
	// Create simple test database
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create just 5 entities with times
	tx := db.NewTransaction()

	priceTime := datalog.NewKeyword(":price/time")
	loc, _ := time.LoadLocation("America/New_York")

	for i := 1; i <= 5; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("e%d", i))
		tm := time.Date(2025, 6, 2, 10+i, 0, 0, 0, loc) // All on day 2
		tx.Add(entity, priceTime, tm)
		t.Logf("Added: %s -> day %d", entity.String(), tm.Day())
	}

	tx.Commit()

	// Pattern: [?e :price/time ?t]
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: query.Symbol("?e")},
			query.Constant{Value: priceTime},
			query.Variable{Name: query.Symbol("?t")},
		},
	}

	matcher := NewBadgerMatcher(db.store)

	// First get all entities without constraint
	t.Run("WithoutConstraint", func(t *testing.T) {
		rel, err := matcher.Match(pattern, nil)
		if err != nil {
			t.Fatalf("Failed: %v", err)
		}

		count := 0
		it := rel.Iterator()
		for it.Next() {
			count++
		}
		t.Logf("Without constraint: %d results", count)

		if count != 5 {
			t.Errorf("Expected 5, got %d", count)
		}
	})

	// Now test with constraint for day=2
	t.Run("WithConstraint", func(t *testing.T) {
		// First get entities (this simulates having bindings)
		rel1, _ := matcher.Match(pattern, nil)

		// Create binding relation with just the entity column
		var tuples []executor.Tuple
		it := rel1.Iterator()
		for it.Next() {
			tuple := it.Tuple()
			// Extract just the entity
			tuples = append(tuples, executor.Tuple{tuple[0]})
		}

		bindingRel := executor.NewMaterializedRelation([]query.Symbol{"?e"}, tuples)
		t.Logf("Binding relation has %d tuples", bindingRel.Size())

		// Now match with constraint
		constraint := &timeExtractionConstraint{
			position:  2, // Value position
			extractFn: "day",
			expected:  int64(2),
		}

		rel2, err := matcher.MatchWithConstraints(
			pattern,
			executor.Relations{bindingRel},
			[]executor.StorageConstraint{constraint},
		)
		if err != nil {
			t.Fatalf("Failed with constraint: %v", err)
		}

		count := 0
		it2 := rel2.Iterator()
		for it2.Next() {
			tuple := it2.Tuple()
			t.Logf("Result: %v", tuple)
			count++
		}

		t.Logf("With constraint (day=2): %d results", count)

		if count != 5 {
			t.Errorf("Expected 5 (all are day 2), got %d", count)
		}
	})
}
