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
	exec := executor.NewExecutor(matcher, nil)
	hresult, _ := exec.Execute(hq)

	// Collect results by iterating
	var htuples []executor.Tuple
	hIt := hresult.Iterator()
	for hIt.Next() {
		htuples = append(htuples, hIt.Tuple())
	}
	hIt.Close()

	t.Logf("High query found %d results (type=%T)", len(htuples), hresult)
	for i, tuple := range htuples {
		t.Logf("  High result %d: %v", i, tuple[0])
	}
	if len(htuples) != 5 {
		t.Fatalf("Expected 5 results from high query, got %d", len(htuples))
	}

	// Now test the Relation directly from the matcher (before executor)
	highPattern := hq.Where[0].(*query.DataPattern)
	highRel, _ := matcher.Match(query.PatternQuery(highPattern), nil)
	t.Logf("High pattern Match() returned type=%T, columns=%v", highRel, highRel.Symbols())

	// Iterate directly to see all tuples
	hPatIt := highRel.Iterator()
	hCount := 0
	for hPatIt.Next() {
		hCount++
		t.Logf("  High pattern tuple %d: %v", hCount, hPatIt.Tuple())
	}
	hPatIt.Close()
	t.Logf("High pattern iterator returned %d tuples", hCount)
	if hCount != 5 {
		t.Fatalf("Expected 5 tuples from high pattern iterator, got %d", hCount)
	}

	lowQuery := `[:find ?bar :where [?bar :price/low ?l]]`
	lq, _ := parser.ParseQuery(lowQuery)
	lresult, _ := exec.Execute(lq)

	// Collect results by iterating
	var ltuples []executor.Tuple
	lIt := lresult.Iterator()
	for lIt.Next() {
		ltuples = append(ltuples, lIt.Tuple())
	}
	lIt.Close()

	t.Logf("Low query found %d results (type=%T)", len(ltuples), lresult)
	for i, tuple := range ltuples {
		t.Logf("  Low result %d: %v", i, tuple[0])
	}
	if len(ltuples) != 5 {
		t.Fatalf("Expected 5 results from low query, got %d", len(ltuples))
	}

	// Now test the low pattern directly
	lowPattern := lq.Where[0].(*query.DataPattern)
	lowRel, _ := matcher.Match(query.PatternQuery(lowPattern), nil)
	t.Logf("Low pattern Match() returned type=%T, columns=%v", lowRel, lowRel.Symbols())

	// Iterate directly to see all tuples
	lPatIt := lowRel.Iterator()
	lCount := 0
	for lPatIt.Next() {
		lCount++
		t.Logf("  Low pattern tuple %d: %v", lCount, lPatIt.Tuple())
	}
	lPatIt.Close()
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
	annotatedExec := executor.NewExecutor(annotatedMatcher, nil)

	jresult, _ := annotatedExec.Execute(jq)

	// Collect results by iterating
	var jtuples []executor.Tuple
	jIt := jresult.Iterator()
	for jIt.Next() {
		jtuples = append(jtuples, jIt.Tuple())
	}
	jIt.Close()

	t.Logf("Join query found %d results", len(jtuples))
	if len(jtuples) != 5 {
		t.Errorf("BUG REPRODUCED: Join query expected 5 results, got %d", len(jtuples))
		// Print which bars we got
		for i, tuple := range jtuples {
			t.Logf("  Got bar: %v", tuple[0])
			_ = i
		}
	}
}
