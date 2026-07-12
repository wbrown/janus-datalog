package storage

// op_field_test.go - Tests for Op field implementation
//
// CRITICAL: These tests verify that the Op field on Datom works correctly
// for CRDT cardinality-many semantics. The Op field replaced the old
// SetEntry wrapper approach.
//
// Key changes tested:
// 1. Op field is always the last byte of every key
// 2. V stores raw values (not SetEntry bytes) - enables AVET lookups
// 3. Add-wins resolution reads Op from datom.Op, not from SetEntry
// 4. AVET index can now match raw value types for cardinality-many

import (
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestOpFieldInKeyEncoding verifies Op is correctly encoded/decoded in keys
func TestOpFieldInKeyEncoding(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	// Create datom with OpCRDTAdd
	datom := &datalog.Datom{
		E:  datalog.NewIdentity("test-entity"),
		A:  datalog.NewKeyword(":test/attr"),
		V:  "test-value",
		Tx: datalog.ElementID{Lamport: 1000, ReplicaID: 100},
		Op: datalog.OpCRDTAdd,
	}

	// Encode and decode for each index type
	for _, idx := range []IndexType{EAVT, EATV, AEVT, AVET, VAET, TAEV} {
		key := encoder.EncodeKey(idx, datom)
		_, _, _, _, op, _, err := encoder.DecodeKey(idx, key)
		if err != nil {
			t.Errorf("%v: DecodeKey error: %v", idx, err)
			continue
		}
		if op != byte(datalog.OpCRDTAdd) {
			t.Errorf("%v: Op mismatch: expected %d, got %d", idx, datalog.OpCRDTAdd, op)
		}
	}

	// Test OpCRDTRemove
	datom.Op = datalog.OpCRDTRemove
	for _, idx := range []IndexType{EAVT, AVET} {
		key := encoder.EncodeKey(idx, datom)
		_, _, _, _, op, _, err := encoder.DecodeKey(idx, key)
		if err != nil {
			t.Errorf("%v: DecodeKey error: %v", idx, err)
			continue
		}
		if op != byte(datalog.OpCRDTRemove) {
			t.Errorf("%v: Op mismatch: expected %d, got %d", idx, datalog.OpCRDTRemove, op)
		}
	}
}

// TestOpFieldPreservesRawValueType verifies V stores raw type, not TypeBytes
func TestOpFieldPreservesRawValueType(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	// Create datom with string value
	datom := &datalog.Datom{
		E:  datalog.NewIdentity("test-entity"),
		A:  datalog.NewKeyword(":test/attr"),
		V:  "warrior", // Raw string value
		Tx: datalog.ElementID{Lamport: 1000, ReplicaID: 100},
		Op: datalog.OpCRDTAdd,
	}

	// Encode for AVET (value lookup index)
	key := encoder.EncodeKey(AVET, datom)
	_, _, vBytes, _, _, _, err := encoder.DecodeKey(AVET, key)
	if err != nil {
		t.Fatalf("DecodeKey error: %v", err)
	}

	// First byte should be TypeString, not TypeBytes
	if len(vBytes) < 1 {
		t.Fatal("vBytes too short")
	}
	if datalog.ValueType(vBytes[0]) != datalog.TypeString {
		t.Errorf("Value type: expected TypeString (%d), got %d", datalog.TypeString, vBytes[0])
	}

	// Rest should be the string "warrior"
	if string(vBytes[1:]) != "warrior" {
		t.Errorf("Value data: expected 'warrior', got '%s'", string(vBytes[1:]))
	}
}

// TestAddMethodUsesOpField verifies Transaction.Add() sets Op correctly
func TestAddMethodUsesOpField(t *testing.T) {
	dir, err := os.MkdirTemp("", "op-field-add-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":person/tags")

	// Use Add() API
	tx := db.NewTransaction()
	err = tx.Add(entityID, attr, "warrior")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query should find the value
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	count := 0
	iter := results.Iterator()
	for iter.Next() {
		count++
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			if tag, ok := tuple[0].(string); ok {
				if tag != "warrior" {
					t.Errorf("Expected 'warrior', got '%s'", tag)
				}
			}
		}
	}

	if count != 1 {
		t.Errorf("Expected 1 result, got %d", count)
	}
}

// TestRemoveMethodUsesOpField verifies Transaction.Remove() sets Op correctly
func TestRemoveMethodUsesOpField(t *testing.T) {
	dir, err := os.MkdirTemp("", "op-field-remove-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":person/tags")

	// Add then Remove
	tx := db.NewTransaction()
	err = tx.Add(entityID, attr, "warrior")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx2 := db.NewTransaction()
	err = tx2.Remove(entityID, attr, "warrior")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query should return empty (remove has higher Lamport)
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	count := 0
	iter := results.Iterator()
	for iter.Next() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 results after remove, got %d", count)
	}
}

// TestAVETLookupWithOpField verifies AVET index works with raw values
// This was broken with the old SetEntry approach (TypeBytes pollution)
func TestAVETLookupWithOpField(t *testing.T) {
	dir, err := os.MkdirTemp("", "op-field-avet-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	// Add tags to multiple entities
	entity1 := datalog.NewIdentity("entity1")
	entity2 := datalog.NewIdentity("entity2")
	attr := datalog.NewKeyword(":person/tags")

	tx := db.NewTransaction()
	tx.Add(entity1, attr, "warrior")
	tx.Add(entity1, attr, "veteran")
	tx.Add(entity2, attr, "warrior") // Same tag on different entity
	tx.Add(entity2, attr, "mage")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query: find all entities with "warrior" tag
	// This requires AVET index to work with raw string values
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "warrior"},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	entities := make(map[string]bool)
	iter := results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			if e, ok := tuple[0].(datalog.Identity); ok {
				entities[e.String()] = true
			}
		}
	}

	if len(entities) != 2 {
		t.Errorf("Expected 2 entities with 'warrior' tag, got %d: %v", len(entities), entities)
	}
}

// TestBadgerIteratorDecodesOp verifies BadgerIterator.Datom() sets Op from key
func TestBadgerIteratorDecodesOp(t *testing.T) {
	dir, err := os.MkdirTemp("", "op-field-iterator-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":person/tags")

	// Add a value
	tx := db.NewTransaction()
	tx.Add(entityID, attr, "warrior")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Scan EAVT and verify Op is decoded
	var eBytes [20]byte
	var aBytes [32]byte
	copy(eBytes[:], entityID.Bytes())
	copy(aBytes[:], attr.String())

	prefix := make([]byte, 1+20+32)
	prefix[0] = byte(EAVT)
	copy(prefix[1:21], eBytes[:])
	copy(prefix[21:53], aBytes[:])

	iter, err := db.Store().Scan(EAVT, prefix, prefixEnd(prefix))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	defer iter.Close()

	found := false
	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			t.Fatalf("Datom() failed: %v", err)
		}

		// Op should be OpCRDTAdd
		if datom.Op != datalog.OpCRDTAdd {
			t.Errorf("Expected Op=OpCRDTAdd (%d), got %d", datalog.OpCRDTAdd, datom.Op)
		}

		// V should be raw string
		if v, ok := datom.V.(string); !ok || v != "warrior" {
			t.Errorf("Expected V='warrior', got %v", datom.V)
		}

		found = true
	}

	if !found {
		t.Error("No datoms found in scan")
	}
}

// TestSetMethodGeneratesCorrectOps verifies Set() for cardinality-many
func TestSetMethodGeneratesCorrectOps(t *testing.T) {
	dir, err := os.MkdirTemp("", "op-field-set-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":person/tags")

	// Initial set
	tx := db.NewTransaction()
	err = tx.Set(entityID, attr, []interface{}{"a", "b", "c"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify initial set
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
			query.Blank{},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	tags := make(map[string]bool)
	iter := results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			if tag, ok := tuple[0].(string); ok {
				tags[tag] = true
			}
		}
	}

	if len(tags) != 3 || !tags["a"] || !tags["b"] || !tags["c"] {
		t.Errorf("Expected {a, b, c}, got %v", tags)
	}

	// Replace with new set
	tx2 := db.NewTransaction()
	err = tx2.Set(entityID, attr, []interface{}{"x", "y"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify replacement
	results2, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	tags2 := make(map[string]bool)
	iter2 := results2.Iterator()
	for iter2.Next() {
		tuple := iter2.Tuple()
		if len(tuple) > 0 {
			if tag, ok := tuple[0].(string); ok {
				tags2[tag] = true
			}
		}
	}

	if len(tags2) != 2 || !tags2["x"] || !tags2["y"] {
		t.Errorf("Expected {x, y}, got %v", tags2)
	}
	if tags2["a"] || tags2["b"] || tags2["c"] {
		t.Errorf("Old values should be removed, got %v", tags2)
	}
}
