package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestAEVTMatcherBug directly tests the storage layer bug
// Calls MatchWithBindings directly to reproduce the issue
func TestAEVTMatcherBug(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "aevt-matcher-bug-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create test data: 10 entities, each with 5 attributes
	// Total: 50 datoms
	tx := db.NewTransaction()
	entities := make([]datalog.Identity, 10)

	for i := 0; i < 10; i++ {
		entityID := datalog.NewIdentity(fmt.Sprintf("entity:%d", i))
		entities[i] = entityID

		// Each entity has 5 different attributes
		tx.Add(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Person%d", i))
		tx.Add(entityID, datalog.NewKeyword(":person/age"), int64(20+i))
		tx.Add(entityID, datalog.NewKeyword(":person/city"), fmt.Sprintf("City%d", i%3))
		tx.Add(entityID, datalog.NewKeyword(":person/active"), true)
		tx.Add(entityID, datalog.NewKeyword(":person/score"), int64(100*i))
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Create annotation handler to track datom scans
	var events []annotations.Event
	handler := func(event annotations.Event) {
		events = append(events, event)
	}
	// Create matcher with annotation tracking using decorator pattern
	baseMatcher := NewBadgerMatcher(db.Store())
	matcher := executor.WrapMatcher(baseMatcher, handler).(executor.PatternMatcher)

	// Create pattern: [?e :person/age ?age]
	// This is the problematic pattern when ?e is bound
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: "?e"},
			query.Constant{Value: datalog.NewKeyword(":person/age")},
			query.Variable{Name: "?age"},
		},
	}

	// Create binding relation with 3 entities
	bindingRel := executor.NewMaterializedRelation(
		[]query.Symbol{"?e"},
		[]executor.Tuple{
			{entities[0]},
			{entities[5]},
			{entities[9]},
		},
	)

	// Call Match with bindings - this triggers the bug
	result, err := matcher.Match(pattern, executor.Relations{bindingRel})
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Consume the iterator to build the cache
	it := result.Iterator()
	var collectedResults []executor.Tuple
	for it.Next() {
		tuple := it.Tuple()
		tupleCopy := make(executor.Tuple, len(tuple))
		copy(tupleCopy, tuple)
		collectedResults = append(collectedResults, tupleCopy)
	}
	it.Close()

	// Verify results
	if len(collectedResults) != 3 {
		t.Errorf("Expected 3 results, got %d", len(collectedResults))
	}

	// Now Size() should work
	if result.Size() != 3 {
		t.Errorf("Expected Size()=3, got %d", result.Size())
	}

	// Check annotations
	t.Logf("Total events: %d", len(events))

	var datomsScanned int
	var indexUsed string

	for i, event := range events {
		t.Logf("Event %d: %s - %+v", i, event.Name, event.Data)

		// Check both iterator-reuse-complete and multi-match events
		if event.Name == "pattern/iterator-reuse-complete" || event.Name == "pattern/multi-match" {
			if scanned, ok := event.Data["datoms.scanned"].(int); ok {
				datomsScanned = scanned
			}
			if idx, ok := event.Data["index"].(string); ok {
				indexUsed = idx
			}
		}
	}

	t.Logf("Index used: %s", indexUsed)
	t.Logf("Datoms scanned: %d", datomsScanned)
	t.Logf("Entities bound: 3")
	t.Logf("Total datoms in DB: 50")

	// Assertions
	if indexUsed != "AEVT" {
		t.Errorf("Expected AEVT index, got %s", indexUsed)
	}

	// CRITICAL TEST: Should scan ~3 datoms (one per bound entity)
	// Bug: Scans entire database or more
	expectedScans := 3
	tolerance := 10 // Allow some overhead, but not 50+

	if datomsScanned > expectedScans+tolerance {
		t.Errorf("🚨 AEVT BUG REPRODUCED: Scanned %d datoms for 3 bound entities (expected ~%d)",
			datomsScanned, expectedScans)
		t.Logf("Performance degradation: %dx too many scans", datomsScanned/expectedScans)
	} else {
		t.Logf("✅ SUCCESS: Scanned %d datoms (expected ~%d)", datomsScanned, expectedScans)
	}
}
