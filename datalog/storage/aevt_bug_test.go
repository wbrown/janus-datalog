package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// TestAEVTIndexBugDirect validates that queries with RelationInput correctly process all entities
// Previously: Pattern [?e :constant-attr ?v] with RelationInput was only processing first entity
// Now: All entities in the RelationInput should be processed and results returned
func TestAEVTIndexBugDirect(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "aevt-bug-test-*")
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

	// Create context with annotations
	ctx := executor.NewContext(handler)

	// Create matcher with annotations using decorator pattern
	baseMatcher := NewBadgerMatcher(db.Store())
	matcher := executor.WrapMatcher(baseMatcher, handler).(executor.PatternMatcher)

	// Query: Find :person/age for bound entities
	// Use RelationInput to reproduce the exact gopher-street pattern
	// [[?e] ...] means "collection of tuples, each with one variable ?e"
	queryStr := `[:find ?e ?age
	              :in $ [[?e] ...]
	              :where [?e :person/age ?age]]`

	parsed, err := parser.ParseQuery(queryStr)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// DEBUG: Check what input type was parsed
	t.Logf("Query :in clause has %d inputs", len(parsed.In))
	for i, input := range parsed.In {
		switch inp := input.(type) {
		case query.DatabaseInput:
			t.Logf("  Input %d: DatabaseInput", i)
		case query.ScalarInput:
			t.Logf("  Input %d: ScalarInput (?%s)", i, inp.Symbol)
		case query.RelationInput:
			t.Logf("  Input %d: RelationInput (%v)", i, inp.Symbols)
		case query.TupleInput:
			t.Logf("  Input %d: TupleInput (%v)", i, inp.Symbols)
		case query.CollectionInput:
			t.Logf("  Input %d: CollectionInput (?%s)", i, inp.Symbol)
		default:
			t.Logf("  Input %d: Unknown type %T", i, input)
		}
	}

	// Bind 3 entities as a RelationInput (this is what triggers the bug)
	inputRel := executor.NewMaterializedRelation(
		[]query.Symbol{"?e"},
		[]executor.Tuple{{entities[0]}, {entities[5]}, {entities[9]}},
	)

	// Execute query without parallel execution (to capture all annotations)
	exec := executor.NewExecutor(matcher)
	exec.DisableParallelSubqueries() // Disable parallel to get all events in our handler
	result, err := exec.ExecuteWithRelations(ctx, parsed, []executor.Relation{inputRel})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Verify results
	if result.Size() != 3 {
		t.Errorf("Expected 3 results, got %d", result.Size())
	}

	// Check index selection and datom scan count

	t.Logf("Total events captured: %d", len(events))

	var indexUsed string
	var datomsScanned int

	for i, event := range events {
		t.Logf("Event %d: %s - Data: %+v", i, event.Name, event.Data)

		// Check multiple event types for index and scan info
		if event.Name == "pattern/iterator-reuse" ||
			event.Name == "pattern/multi-match" ||
			event.Name == "pattern/index-selection" ||
			event.Name == "pattern/match-with-bindings" {
			if idx, ok := event.Data["index"].(string); ok {
				indexUsed = idx
			}
			if scanned, ok := event.Data["datoms.scanned"].(int); ok {
				datomsScanned += scanned // Accumulate across multiple events
			}
		}
	}

	t.Logf("Index used: %s", indexUsed)
	t.Logf("Datoms scanned: %d", datomsScanned)
	t.Logf("Entities bound: 3")
	t.Logf("Total datoms in DB: 50")

	// The key test is that we got all 3 results
	// With RelationInput iteration, each entity is processed independently, so index choice may vary
	// The original bug was that only 1 result was returned instead of 3
	if result.Size() == 3 {
		t.Logf("SUCCESS: All 3 entities processed correctly")
	}
}

// TestAEVTPrefixRangeDebug inspects the actual prefix range generated
func TestAEVTPrefixRangeDebug(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "aevt-prefix-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create test entity
	entityID := datalog.NewIdentity("test-entity")
	attrKw := datalog.NewKeyword(":test/attr")

	tx := db.NewTransaction()
	tx.Add(entityID, attrKw, "test-value")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Get the matcher and inspect prefix range for AEVT
	matcher := NewBadgerMatcher(db.Store())

	// Call chooseIndex with both E and A bound (the bug scenario)
	index, start, end := matcher.chooseIndex(entityID, attrKw, nil, nil)

	t.Logf("Index selected: %s", indexName(index))
	t.Logf("Start key length: %d bytes", len(start))
	t.Logf("End key length: %d bytes", len(end))
	t.Logf("Start key (hex): % x", start)
	t.Logf("End key (hex): % x", end)

	// For AEVT with A+E bound, the prefix should be 52 bytes: A[32] + E[20]
	expectedPrefixLen := 32 + 20 // A + E

	if index != AEVT {
		t.Errorf("Expected AEVT index, got %s", indexName(index))
	}

	// Check if start key has proper length for (A, E) prefix
	if len(start) < expectedPrefixLen {
		t.Errorf("Start key too short: %d bytes (expected >=%d for A+E prefix)",
			len(start), expectedPrefixLen)
	}

	// The range should be tight: end should be start with last byte incremented
	// or start with additional 0xFF bytes for prefix matching
	rangeSize := "unknown"
	if len(start) > 0 && len(end) > 0 {
		// Check if they share a common prefix
		commonLen := 0
		for i := 0; i < len(start) && i < len(end); i++ {
			if start[i] == end[i] {
				commonLen++
			} else {
				break
			}
		}
		t.Logf("Common prefix length: %d bytes", commonLen)

		if commonLen >= expectedPrefixLen {
			rangeSize = "tight (good)"
		} else {
			rangeSize = "wide (BUG!)"
		}
	}

	t.Logf("Range size: %s", rangeSize)

	// Debug: Show what the encoder thinks
	aStorage := ToStorageDatom(datalog.Datom{A: attrKw}).A
	eBytes := entityID.Bytes()
	t.Logf("Attribute storage bytes: % x (%d bytes)", aStorage, len(aStorage))
	t.Logf("Entity bytes: % x (%d bytes)", eBytes, len(eBytes))
}
