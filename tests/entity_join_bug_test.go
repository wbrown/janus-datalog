package tests

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/storage"
)

// TestEntityJoinBug reproduces the bug where joining two patterns on the same entity
// loses one result
func TestEntityJoinBug(t *testing.T) {
	// Create temporary directory for test database
	dir, err := os.MkdirTemp("", "entity-join-bug-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create database
	db, err := storage.NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Add test data - 5 bars with both high and low values
	tx := db.NewTransaction()

	barIDs := make([]datalog.Identity, 5)
	for i := 0; i < 5; i++ {
		barIDs[i] = datalog.NewIdentity(fmt.Sprintf("bar:%d", i))
		t.Logf("Created bar %d: %s (hash: %x)", i, barIDs[i].L85(), barIDs[i].Hash())
		tx.Add(barIDs[i], datalog.NewKeyword(":price/high"), float64(100+i*10))
		tx.Add(barIDs[i], datalog.NewKeyword(":price/low"), float64(90+i*10))
	}

	if _, err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify individual patterns work
	highQuery := `[:find ?bar :where [?bar :price/high ?h]]`
	hq, _ := parser.ParseQuery(highQuery)
	matcher := storage.NewBadgerMatcher(db.Store())
	exec := executor.NewExecutor(matcher)
	hresult, _ := exec.Execute(hq)
	t.Logf("High query found %d results (type=%T)", hresult.Size(), hresult)
	for i := 0; i < hresult.Size(); i++ {
		t.Logf("  High result %d: %v", i, hresult.Get(i)[0])
	}
	if hresult.Size() != 5 {
		t.Fatalf("Expected 5 results from high query, got %d", hresult.Size())
	}

	// Now test the Relation directly from the matcher (before executor)
	highPattern := hq.Where[0].(*query.DataPattern)
	highRel, _ := matcher.Match(highPattern, nil)
	t.Logf("High pattern Match() returned type=%T, columns=%v", highRel, highRel.Columns())

	// Iterate directly to see all tuples
	hIt := highRel.Iterator()
	hCount := 0
	for hIt.Next() {
		hCount++
		t.Logf("  High pattern tuple %d: %v", hCount, hIt.Tuple())
	}
	hIt.Close()
	t.Logf("High pattern iterator returned %d tuples", hCount)
	if hCount != 5 {
		t.Fatalf("Expected 5 tuples from high pattern iterator, got %d", hCount)
	}

	lowQuery := `[:find ?bar :where [?bar :price/low ?l]]`
	lq, _ := parser.ParseQuery(lowQuery)
	lresult, _ := exec.Execute(lq)
	t.Logf("Low query found %d results (type=%T)", lresult.Size(), lresult)
	for i := 0; i < lresult.Size(); i++ {
		t.Logf("  Low result %d: %v", i, lresult.Get(i)[0])
	}
	if lresult.Size() != 5 {
		t.Fatalf("Expected 5 results from low query, got %d", lresult.Size())
	}

	// Now test the low pattern directly
	lowPattern := lq.Where[0].(*query.DataPattern)
	lowRel, _ := matcher.Match(lowPattern, nil)
	t.Logf("Low pattern Match() returned type=%T, columns=%v", lowRel, lowRel.Columns())

	// Check Size() before iterating - this triggers materialization
	lowSize := lowRel.Size()
	t.Logf("Low pattern Size()=%d (type after Size(): %T)", lowSize, lowRel)

	// Iterate directly to see all tuples
	lIt := lowRel.Iterator()
	lCount := 0
	for lIt.Next() {
		lCount++
		t.Logf("  Low pattern tuple %d: %v", lCount, lIt.Tuple())
	}
	lIt.Close()
	t.Logf("Low pattern iterator returned %d tuples", lCount)
	if lCount != 5 {
		t.Fatalf("Expected 5 tuples from low pattern iterator, got %d", lCount)
	}

	// Test join with annotations - this should return 5 results but returns 4
	joinQuery := `[:find ?bar :where [?bar :price/high ?h] [?bar :price/low ?l]]`
	jq, _ := parser.ParseQuery(joinQuery)

	// Create executor with annotations
	opts := executor.ExecutorOptions{
		EnableDebugLogging: true,
	}
	annotatedMatcher := storage.NewBadgerMatcherWithOptions(db.Store(), opts)
	annotatedExec := executor.NewExecutor(annotatedMatcher)

	jresult, _ := annotatedExec.Execute(jq)
	t.Logf("Join query found %d results", jresult.Size())
	if jresult.Size() != 5 {
		t.Errorf("BUG REPRODUCED: Join query expected 5 results, got %d", jresult.Size())
		// Print which bars we got
		for i := 0; i < jresult.Size(); i++ {
			t.Logf("  Got bar: %v", jresult.Get(i)[0])
		}
	}
}
