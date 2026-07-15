//go:build !(js && wasm)

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestCardinalityOneCurrentValue verifies that cardinality-one queries return
// only the highest ElementID (current value) in CRDT mode.
func TestCardinalityOneCurrentValue(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "crdt-one-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create schema with cardinality-one attribute
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")

	// Write multiple versions using Set (CRDT append-only)
	for i := 0; i < 5; i++ {
		tx := db.NewTransaction()
		err := tx.Set(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// Query should return only the current (latest) value
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Should have exactly 1 result (the current value)
	resultCount := 0
	iter := results.Iterator()
	var lastName string
	for iter.Next() {
		resultCount++
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			if name, ok := tuple[0].(string); ok {
				lastName = name
			}
		}
	}

	if resultCount != 1 {
		t.Errorf("Expected 1 result (current value), got %d", resultCount)
	}

	// The current value should be the last one written
	if lastName != "Name4" {
		t.Errorf("Expected current value 'Name4', got '%s'", lastName)
	}
}

// TestCardinalityOneHistory verifies that MatchWithHistory returns all versions
func TestCardinalityOneHistory(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "crdt-one-history-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	entityID := datalog.NewIdentity("test-entity")

	// Write multiple versions
	for i := 0; i < 5; i++ {
		tx := db.NewTransaction()
		tx.Add(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// Query with history should return all versions
	matcher := NewBadgerMatcher(db.Store())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Blank{},
		},
	}

	results, err := matcher.MatchWithHistory(pattern)
	if err != nil {
		t.Fatalf("MatchWithHistory failed: %v", err)
	}

	// Should have all 5 versions
	if len(results) != 5 {
		t.Errorf("Expected 5 historical results, got %d", len(results))
	}

	// Results should be in descending Tx order (newest first)
	var prevLamport uint64 = ^uint64(0) // Max uint64
	for i, d := range results {
		if d.Tx.Lamport > prevLamport {
			t.Errorf("Results not in descending order: result %d has Lamport %d > previous %d",
				i, d.Tx.Lamport, prevLamport)
		}
		prevLamport = d.Tx.Lamport
	}
}

// TestCardinalityOneAsOf verifies that MatchAsOf returns the correct historical value
func TestCardinalityOneAsOf(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "crdt-one-asof-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	entityID := datalog.NewIdentity("test-entity")

	// Write multiple versions and track their transaction IDs
	var txIDs []datalog.ElementID
	for i := 0; i < 5; i++ {
		tx := db.NewTransaction()
		tx.Add(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		txID, err := tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		txIDs = append(txIDs, txID)
	}

	matcher := NewBadgerMatcher(db.Store())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Blank{},
		},
	}

	// Query as-of each transaction should return the value from that transaction
	for i, targetTx := range txIDs {
		results, err := matcher.MatchAsOf(pattern, targetTx)
		if err != nil {
			t.Fatalf("MatchAsOf failed: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("MatchAsOf(Tx=%v): expected 1 result, got %d", targetTx, len(results))
			continue
		}

		expectedName := fmt.Sprintf("Name%d", i)
		if results[0].V.(string) != expectedName {
			t.Errorf("MatchAsOf(Tx=%v): expected '%s', got '%s'",
				targetTx, expectedName, results[0].V)
		}
	}

	// Query as-of zero ElementID should return no results (before any writes)
	results, err := matcher.MatchAsOf(pattern, datalog.ElementID{})
	if err != nil {
		t.Fatalf("MatchAsOf(0) failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("MatchAsOf(0): expected 0 results, got %d", len(results))
	}
}

// TestCardinalityOneNoRead verifies that Set() doesn't read existing value
// (append-only CRDT semantics)
func TestCardinalityOneNoRead(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "crdt-one-noread-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create schema
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")

	// Use Set() which should be append-only (no read-modify-write)
	for i := 0; i < 3; i++ {
		tx := db.NewTransaction()
		// Set should NOT read existing value - just append
		err := tx.Set(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// All 3 values should be stored (CRDT keeps history)
	matcher := NewBadgerMatcher(db.Store())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Blank{},
		},
	}

	// MatchWithHistory should return all 3 versions
	results, err := matcher.MatchWithHistory(pattern)
	if err != nil {
		t.Fatalf("MatchWithHistory failed: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 historical values (append-only), got %d", len(results))
	}
}

// TestSetCardinalityValidation verifies that Set() rejects non-cardinality-one attributes
func TestSetCardinalityValidation(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "crdt-set-validation-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create schema with cardinality-many attribute
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")

	// Set() should fail for cardinality-many
	tx := db.NewTransaction()
	err = tx.Set(entityID, datalog.NewKeyword(":person/tags"), "tag1")
	if err == nil {
		t.Error("Expected Set() to fail for cardinality-many attribute")
	} else {
		t.Logf("Set correctly rejected for cardinality-many: %v", err)
	}
}

// TestCardinalityOneConcurrentWrites verifies that ReplicaID tiebreaker works.
// When two writes have the same Lamport value but different ReplicaIDs,
// the higher ReplicaID should win (LWW semantics).
func TestCardinalityOneConcurrentWrites(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "crdt-one-concurrent-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create schema with cardinality-one attribute
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":person/name")

	// Construct datoms with the same Lamport but different ReplicaIDs
	// This simulates two replicas writing concurrently
	sameLamport := uint64(1000)
	lowerReplicaID := uint64(100)
	higherReplicaID := uint64(200)

	// Create datoms - the one with higher ReplicaID should win
	datomWithLowerReplica := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "ValueFromLowerReplica",
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: lowerReplicaID},
	}

	datomWithHigherReplica := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "ValueFromHigherReplica",
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: higherReplicaID},
	}

	// Assert both datoms directly to storage (simulating merge from replicas)
	// Order shouldn't matter - higher ReplicaID should win
	err = db.Store().Assert([]datalog.Datom{datomWithLowerReplica, datomWithHigherReplica})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Query should return only the value from higher ReplicaID
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Should have exactly 1 result (the current value)
	resultCount := 0
	var resultName string
	iter := results.Iterator()
	for iter.Next() {
		resultCount++
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			if name, ok := tuple[0].(string); ok {
				resultName = name
			}
		}
	}

	if resultCount != 1 {
		t.Errorf("Expected 1 result, got %d", resultCount)
	}

	// Higher ReplicaID should win
	if resultName != "ValueFromHigherReplica" {
		t.Errorf("Expected 'ValueFromHigherReplica' (higher ReplicaID wins), got '%s'", resultName)
	}

	// Verify history contains both values
	history, err := matcher.MatchWithHistory(pattern)
	if err != nil {
		t.Fatalf("MatchWithHistory failed: %v", err)
	}

	if len(history) != 2 {
		t.Errorf("Expected 2 historical values, got %d", len(history))
	}

	// Verify the order: highest (Lamport, ReplicaID) first
	if len(history) >= 2 {
		// First should be higher ReplicaID (current value)
		if history[0].V.(string) != "ValueFromHigherReplica" {
			t.Errorf("First historical value should be 'ValueFromHigherReplica', got '%s'", history[0].V)
		}
		// Second should be lower ReplicaID (superseded value)
		if history[1].V.(string) != "ValueFromLowerReplica" {
			t.Errorf("Second historical value should be 'ValueFromLowerReplica', got '%s'", history[1].V)
		}
	}
}

// TestSchemalessDefaultsToCardinalityOne verifies that schemaless attributes
// default to cardinality-one behavior
func TestSchemalessDefaultsToCardinalityOne(t *testing.T) {
	// Create temporary database
	dir, err := os.MkdirTemp("", "crdt-schemaless-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// No schema set - schemaless mode
	entityID := datalog.NewIdentity("test-entity")

	// Write multiple versions using Add (works in schemaless mode)
	for i := 0; i < 5; i++ {
		tx := db.NewTransaction()
		tx.Add(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// Query should return only current value (schemaless defaults to cardinality-one)
	matcher := NewBadgerMatcher(db.Store())
	// Note: no SetSchema() call - schemaless mode

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Should have exactly 1 result (schemaless defaults to cardinality-one)
	resultCount := 0
	iter := results.Iterator()
	for iter.Next() {
		resultCount++
	}

	if resultCount != 1 {
		t.Errorf("Expected 1 result (schemaless defaults to cardinality-one), got %d", resultCount)
	}
}

// TestHistoryMode verifies that db.History() returns raw datoms without CRDT
// resolution. For cardinality-one (LWW), this means ALL historical values are
// returned, not just the winner.
func TestHistoryMode(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-history-mode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create schema with cardinality-one attribute
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")

	// Write 3 versions of the same attribute
	var txIDs []datalog.ElementID
	for i := 0; i < 3; i++ {
		tx := db.NewTransaction()
		err := tx.Set(entityID, datalog.NewKeyword(":person/name"), fmt.Sprintf("Name%d", i))
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		txID, err := tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		txIDs = append(txIDs, txID)
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: datalog.NewKeyword(":person/name")},
			query.Variable{Name: datalog.NewSymbol("?name")},
			query.Blank{},
		},
	}

	// Latest mode: should return only 1 result (LWW winner)
	latestMatcher := db.Matcher()
	latestResults, err := latestMatcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Latest Match failed: %v", err)
	}
	latestCount := 0
	latestIter := latestResults.Iterator()
	for latestIter.Next() {
		latestCount++
	}
	if latestCount != 1 {
		t.Errorf("Latest mode: expected 1 result, got %d", latestCount)
	}

	// History mode: should return ALL 3 raw datoms
	historyMatcher := db.History()
	historyResults, err := historyMatcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("History Match failed: %v", err)
	}
	historyCount := 0
	historyIter := historyResults.Iterator()
	var historyNames []string
	for historyIter.Next() {
		historyCount++
		tuple := historyIter.Tuple()
		if len(tuple) > 0 {
			if name, ok := tuple[0].(string); ok {
				historyNames = append(historyNames, name)
			}
		}
	}
	if historyCount != 3 {
		t.Errorf("History mode: expected 3 results (all versions), got %d", historyCount)
	}

	// As-of mode: should return 1 result (LWW winner at that point)
	asOfMatcher := db.AsOf(txIDs[1])
	asOfResults, err := asOfMatcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("AsOf Match failed: %v", err)
	}
	asOfCount := 0
	asOfIter := asOfResults.Iterator()
	var asOfName string
	for asOfIter.Next() {
		asOfCount++
		tuple := asOfIter.Tuple()
		if len(tuple) > 0 {
			if name, ok := tuple[0].(string); ok {
				asOfName = name
			}
		}
	}
	if asOfCount != 1 {
		t.Errorf("AsOf mode: expected 1 result, got %d", asOfCount)
	}
	if asOfName != "Name1" {
		t.Errorf("AsOf mode: expected 'Name1', got '%s'", asOfName)
	}
}
