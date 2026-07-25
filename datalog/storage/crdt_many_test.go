package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestSetEntryEncodeDecode verifies SetEntry encoding and decoding round-trips correctly
func TestSetEntryEncodeDecode(t *testing.T) {
	testCases := []struct {
		name  string
		value interface{}
		op    uint8
	}{
		{"string_add", "warrior", OpAdd},
		{"string_remove", "warrior", OpRemove},
		{"int_add", int64(42), OpAdd},
		{"int_remove", int64(42), OpRemove},
		{"float_add", 3.14, OpAdd},
		{"bool_add", true, OpAdd},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			entry := SetEntry{Value: tc.value, Op: tc.op}
			encoded := EncodeSetEntry(entry)

			decoded, err := DecodeSetEntry(encoded)
			if err != nil {
				t.Fatalf("DecodeSetEntry failed: %v", err)
			}

			if decoded.Op != tc.op {
				t.Errorf("Op mismatch: expected %d, got %d", tc.op, decoded.Op)
			}

			// Compare values (handle type differences for numbers)
			switch expected := tc.value.(type) {
			case string:
				if got, ok := decoded.Value.(string); !ok || got != expected {
					t.Errorf("Value mismatch: expected %q, got %v", expected, decoded.Value)
				}
			case int64:
				if got, ok := decoded.Value.(int64); !ok || got != expected {
					t.Errorf("Value mismatch: expected %d, got %v", expected, decoded.Value)
				}
			case float64:
				if got, ok := decoded.Value.(float64); !ok || got != expected {
					t.Errorf("Value mismatch: expected %f, got %v", expected, decoded.Value)
				}
			case bool:
				if got, ok := decoded.Value.(bool); !ok || got != expected {
					t.Errorf("Value mismatch: expected %v, got %v", expected, decoded.Value)
				}
			}
		})
	}
}

// TestSetEntryStorageRoundTrip verifies SetEntry values survive storage and retrieval
func TestSetEntryStorageRoundTrip(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-set-roundtrip-*")
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

	// Create and encode a SetEntry
	originalEntry := SetEntry{Value: "warrior", Op: OpAdd}
	encoded := EncodeSetEntry(originalEntry)

	// Store via Assert with explicit Tx
	datom := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  encoded,
		Tx: datalog.ElementID{Lamport: 1000, ReplicaID: 100},
	}
	err = db.Store().Assert([]datalog.Datom{datom})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Build scan prefix for EAVT
	var eBytes [20]byte
	var aBytes [32]byte
	copy(eBytes[:], entityID.Bytes())
	copy(aBytes[:], attr.String())

	prefix := make([]byte, 1+20+32)
	prefix[0] = byte(EAVT)
	copy(prefix[1:21], eBytes[:])
	copy(prefix[21:53], aBytes[:])

	// Scan EAVT to retrieve the datom
	iter, err := db.Store().Scan(EAVT, prefix, prefixEnd(prefix))
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	defer iter.Close()

	foundCount := 0
	for iter.Next() {
		foundCount++
		d, err := iter.Datom()
		if err != nil {
			t.Errorf("Datom() error: %v", err)
			continue
		}

		// Verify the value is []byte
		vBytes, ok := d.V.([]byte)
		if !ok {
			t.Errorf("Expected V to be []byte, got %T: %v", d.V, d.V)
			continue
		}

		// Decode as SetEntry
		decoded, err := DecodeSetEntry(vBytes)
		if err != nil {
			t.Errorf("DecodeSetEntry failed: %v", err)
			continue
		}

		// Verify the decoded entry matches
		if decoded.Op != originalEntry.Op {
			t.Errorf("Op mismatch: expected %d, got %d", originalEntry.Op, decoded.Op)
		}
		if decoded.Value != originalEntry.Value {
			t.Errorf("Value mismatch: expected %v, got %v", originalEntry.Value, decoded.Value)
		}

		// Verify Tx
		if d.Tx.Lamport != 1000 || d.Tx.ReplicaID != 100 {
			t.Errorf("Tx mismatch: expected {1000, 100}, got %v", d.Tx)
		}
	}

	if foundCount == 0 {
		t.Error("No datoms found in EAVT scan - storage round-trip failed")
	} else if foundCount > 1 {
		t.Errorf("Expected 1 datom, found %d", foundCount)
	}
}

// TestResolveAddWinsSetDirect tests the resolveAddWinsSet function directly
func TestResolveAddWinsSetDirect(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-resolve-direct-*")
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

	// Store two datoms with Op field: one add, one remove for same value
	// Add at Lamport 1000, Remove at Lamport 900 (Add is newer, should be in set)
	datoms := []datalog.Datom{
		{E: entityID, A: attr, V: "warrior", Tx: datalog.ElementID{Lamport: 1000, ReplicaID: 100}, Op: datalog.OpCRDTAdd},
		{E: entityID, A: attr, V: "warrior", Tx: datalog.ElementID{Lamport: 900, ReplicaID: 100}, Op: datalog.OpCRDTRemove},
	}
	err = db.Store().Assert(datoms)
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Create matcher and call resolveAddWinsSet directly
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)

	var eBytes [20]byte
	var aBytes [32]byte
	copy(eBytes[:], entityID.Bytes())
	copy(aBytes[:], attr.String())

	result, err := matcher.resolveAddWinsSet(eBytes[:], aBytes[:])
	if err != nil {
		t.Fatalf("resolveAddWinsSet failed: %v", err)
	}

	t.Logf("Result: Members=%v, MaxElementID=%v", result.Members, result.MaxElementID)

	if len(result.Members) != 1 {
		t.Errorf("Expected 1 member (add is newer), got %d: %v", len(result.Members), result.Members)
	} else if _, ok := result.Members["warrior"]; !ok {
		t.Errorf("Expected 'warrior' to be in set, got %v", result.Members)
	}
}

// TestResolveAddWinsSameLamport tests add-wins when Add and Remove have same Lamport
func TestResolveAddWinsSameLamport(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-resolve-samelamp-*")
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

	// Store two datoms with Op field: add and remove with SAME Lamport
	sameLamport := uint64(1000)
	datoms := []datalog.Datom{
		{E: entityID, A: attr, V: "warrior", Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: 100}, Op: datalog.OpCRDTAdd},
		{E: entityID, A: attr, V: "warrior", Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: 200}, Op: datalog.OpCRDTRemove},
	}
	err = db.Store().Assert(datoms)
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Create matcher and call resolveAddWinsSet directly
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)

	var eBytes [20]byte
	var aBytes [32]byte
	copy(eBytes[:], entityID.Bytes())
	copy(aBytes[:], attr.String())

	result, err := matcher.resolveAddWinsSet(eBytes[:], aBytes[:])
	if err != nil {
		t.Fatalf("resolveAddWinsSet failed: %v", err)
	}

	t.Logf("Result: Members=%v, MaxElementID=%v", result.Members, result.MaxElementID)

	// At same Lamport, add should win
	if len(result.Members) != 1 {
		t.Errorf("Expected 1 member (add-wins at same Lamport), got %d: %v", len(result.Members), result.Members)
	} else if _, ok := result.Members["warrior"]; !ok {
		t.Errorf("Expected 'warrior' to be in set, got %v", result.Members)
	}
}

// TestCheckSetMembershipDirect tests the checkSetMembership function directly
func TestCheckSetMembershipDirect(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-membership-direct-*")
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

	// Add a value using the Transaction API
	tx := db.NewTransaction()
	tx.Add(entityID, attr, "present")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Create matcher
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)

	var eBytes [20]byte
	var aBytes [32]byte
	copy(eBytes[:], entityID.Bytes())
	copy(aBytes[:], attr.String())

	// Check membership for "present" (should be true)
	isMember, err := matcher.checkSetMembership(eBytes[:], aBytes[:], "present")
	if err != nil {
		t.Fatalf("checkSetMembership failed: %v", err)
	}
	t.Logf("Membership of 'present': %v", isMember)
	if !isMember {
		t.Error("Expected 'present' to be a member")
	}

	// Check membership for "absent" (should be false)
	isMember2, err := matcher.checkSetMembership(eBytes[:], aBytes[:], "absent")
	if err != nil {
		t.Fatalf("checkSetMembership failed: %v", err)
	}
	t.Logf("Membership of 'absent': %v", isMember2)
	if isMember2 {
		t.Error("Expected 'absent' to NOT be a member")
	}
}

// TestCardinalityManyAddRemove verifies basic add/remove operations work
func TestCardinalityManyAddRemove(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-basic-*")
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
	attr := datalog.NewKeyword(":person/tags")

	// Add some values
	tx := db.NewTransaction()
	err = tx.Add(entityID, attr, "warrior")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	err = tx.Add(entityID, attr, "veteran")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query should return both values
	matcher := NewPatternMatcher(db.Store())
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

	// Collect results
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

	if len(tags) != 2 {
		t.Errorf("Expected 2 tags, got %d: %v", len(tags), tags)
	}
	if !tags["warrior"] {
		t.Error("Expected 'warrior' tag")
	}
	if !tags["veteran"] {
		t.Error("Expected 'veteran' tag")
	}

	// Now remove one value
	tx2 := db.NewTransaction()
	err = tx2.Remove(entityID, attr, "warrior")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query should return only one value now
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

	if len(tags2) != 1 {
		t.Errorf("Expected 1 tag after remove, got %d: %v", len(tags2), tags2)
	}
	if !tags2["veteran"] {
		t.Error("Expected 'veteran' tag to remain")
	}
	if tags2["warrior"] {
		t.Error("Expected 'warrior' tag to be removed")
	}
}

// TestCardinalityManyAddWins verifies add-wins semantics at same Lamport
func TestCardinalityManyAddWins(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-addwins-*")
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

	// Create datoms with same Lamport but different operations
	// When Lamport is equal, Add should win
	sameLamport := uint64(1000)
	replicaA := uint64(100)
	replicaB := uint64(200)

	// Create datoms - Add from replica A, Remove from replica B, same Lamport
	// NEW FORMAT: Op is a field on Datom, V is the raw value
	datomAdd := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "warrior",
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaA},
		Op: datalog.OpCRDTAdd,
	}
	datomRemove := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "warrior",
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaB},
		Op: datalog.OpCRDTRemove,
	}

	// Assert both directly
	err = db.Store().Assert([]datalog.Datom{datomAdd, datomRemove})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Query should return the value (add-wins at same Lamport)
	matcher := NewPatternMatcher(db.Store())
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
	var foundTag string
	iter := results.Iterator()
	for iter.Next() {
		count++
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			if tag, ok := tuple[0].(string); ok {
				foundTag = tag
			}
		}
	}

	if count != 1 {
		t.Errorf("Expected 1 result (add-wins), got %d", count)
	}
	if foundTag != "warrior" {
		t.Errorf("Expected 'warrior' (add-wins), got '%s'", foundTag)
	}
}

// TestCardinalityManyReplicaIDTiebreaker verifies ReplicaID tiebreaker for add-wins
func TestCardinalityManyReplicaIDTiebreaker(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-replicaid-*")
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

	// Two adds with same Lamport, different ReplicaID
	// Both should be in the set (not a conflict - both are adds)
	sameLamport := uint64(1000)
	replicaA := uint64(100)
	replicaB := uint64(200)

	// NEW FORMAT: Op is a field on Datom, V is the raw value
	datomA := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "tagA",
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaA},
		Op: datalog.OpCRDTAdd,
	}
	datomB := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "tagB",
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaB},
		Op: datalog.OpCRDTAdd,
	}

	err = db.Store().Assert([]datalog.Datom{datomA, datomB})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewPatternMatcher(db.Store())
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

	if len(tags) != 2 {
		t.Errorf("Expected 2 tags (both adds preserved), got %d: %v", len(tags), tags)
	}
}

// TestCardinalityManyAddThenRemove verifies: add at T1, remove at T2 (T2 > T1) → not in set
func TestCardinalityManyAddThenRemove(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-add-then-remove-*")
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

	// Add first, then remove later
	addEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpAdd})
	removeEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpRemove})

	datomAdd := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  addEntry,
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, // Earlier
	}
	datomRemove := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  removeEntry,
		Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}, // Later
	}

	err = db.Store().Assert([]datalog.Datom{datomAdd, datomRemove})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewPatternMatcher(db.Store())
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
		t.Errorf("Expected 0 results (remove after add), got %d", count)
	}
}

// TestCardinalityManyRemoveThenAdd verifies: remove at T1, add at T2 (T2 > T1) → in set
func TestCardinalityManyRemoveThenAdd(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-remove-then-add-*")
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

	// Remove first, then add later (re-add)
	// NEW FORMAT: Op is a field on Datom, V is the raw value
	datomRemove := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "warrior",
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, // Earlier
		Op: datalog.OpCRDTRemove,
	}
	datomAdd := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  "warrior",
		Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}, // Later
		Op: datalog.OpCRDTAdd,
	}

	err = db.Store().Assert([]datalog.Datom{datomRemove, datomAdd})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewPatternMatcher(db.Store())
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
	var foundTag string
	iter := results.Iterator()
	for iter.Next() {
		count++
		tuple := iter.Tuple()
		if len(tuple) > 0 {
			if tag, ok := tuple[0].(string); ok {
				foundTag = tag
			}
		}
	}

	if count != 1 {
		t.Errorf("Expected 1 result (add after remove), got %d", count)
	}
	if foundTag != "warrior" {
		t.Errorf("Expected 'warrior', got '%s'", foundTag)
	}
}

// TestCardinalityManyMultipleValues verifies independent add/remove of different values
func TestCardinalityManyMultipleValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-multiple-*")
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

	// Add multiple values, remove some
	tx := db.NewTransaction()
	tx.Add(entityID, attr, "tag1")
	tx.Add(entityID, attr, "tag2")
	tx.Add(entityID, attr, "tag3")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Remove tag2
	tx2 := db.NewTransaction()
	tx2.Remove(entityID, attr, "tag2")
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	matcher := NewPatternMatcher(db.Store())
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

	if len(tags) != 2 {
		t.Errorf("Expected 2 tags (tag1, tag3), got %d: %v", len(tags), tags)
	}
	if !tags["tag1"] || !tags["tag3"] {
		t.Errorf("Expected tag1 and tag3, got: %v", tags)
	}
	if tags["tag2"] {
		t.Error("tag2 should have been removed")
	}
}

// TestCardinalityManyEmptySet verifies that removing all values results in empty set
func TestCardinalityManyEmptySet(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-empty-*")
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

	// Add then remove all
	tx := db.NewTransaction()
	tx.Add(entityID, attr, "tag1")
	tx.Add(entityID, attr, "tag2")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx2 := db.NewTransaction()
	tx2.Remove(entityID, attr, "tag1")
	tx2.Remove(entityID, attr, "tag2")
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	matcher := NewPatternMatcher(db.Store())
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
		t.Errorf("Expected empty set, got %d items", count)
	}
}

// TestCardinalityManyHistory verifies all operations are preserved in history
func TestCardinalityManyHistory(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-history-*")
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

	// Multiple operations
	tx := db.NewTransaction()
	tx.Add(entityID, attr, "tag1")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx2 := db.NewTransaction()
	tx2.Add(entityID, attr, "tag2")
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx3 := db.NewTransaction()
	tx3.Remove(entityID, attr, "tag1")
	_, err = tx3.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Use a history-mode match to get all operations. Bind ?tx so the add
	// and the remove of the same value stay distinct tuples in the relation.
	matcher := NewPatternMatcher(db.Store())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
			query.Variable{Name: datalog.NewSymbol("?tx")},
		},
	}

	rel, err := matcher.History().Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("history match failed: %v", err)
	}
	entries := 0
	iter := rel.Iterator()
	for iter.Next() {
		entries++
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("history scan failed: %v", err)
	}
	iter.Close()

	// Should have 3 entries (2 adds + 1 remove)
	if entries != 3 {
		t.Errorf("Expected 3 historical entries, got %d", entries)
	}
}

// TestCardinalityManyMembership verifies querying specific value membership
func TestCardinalityManyMembership(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-membership-*")
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

	tx := db.NewTransaction()
	tx.Add(entityID, attr, "present")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)

	// Query for specific value that IS in the set
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Constant{Value: "present"},
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

	if count != 1 {
		t.Errorf("Expected 1 result for existing value, got %d", count)
	}

	// Query for value NOT in set
	pattern2 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Constant{Value: "absent"},
			query.Blank{},
		},
	}

	results2, err := matcher.Match(query.PatternQuery(pattern2), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	count2 := 0
	iter2 := results2.Iterator()
	for iter2.Next() {
		count2++
	}

	if count2 != 0 {
		t.Errorf("Expected 0 results for non-existing value, got %d", count2)
	}
}

// TestCardinalityManyReplaceSet verifies Set() replaces entire set
func TestCardinalityManyReplaceSet(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-replace-set-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDatabase(dir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Set up schema with cardinality-many
	s := schema.NewSchema()
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":person/tags")

	// Step 1: Add initial values using Add()
	tx1 := db.NewTransaction()
	if err := tx1.Add(entityID, attr, "warrior"); err != nil {
		t.Fatalf("Add warrior failed: %v", err)
	}
	if err := tx1.Add(entityID, attr, "veteran"); err != nil {
		t.Fatalf("Add veteran failed: %v", err)
	}
	if err := tx1.Add(entityID, attr, "leader"); err != nil {
		t.Fatalf("Add leader failed: %v", err)
	}
	if _, err := tx1.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify initial state
	matcher := NewPatternMatcher(db.Store())
	var eBytes [20]byte
	var aBytes [32]byte
	copy(eBytes[:], entityID.Bytes())
	copy(aBytes[:], attr.String())

	result1, err := matcher.resolveAddWinsSet(eBytes[:], aBytes[:])
	if err != nil {
		t.Fatalf("resolveAddWinsSet failed: %v", err)
	}
	if len(result1.Members) != 3 {
		t.Errorf("Expected 3 initial members, got %d", len(result1.Members))
	}
	_, hasW := result1.Members["warrior"]
	_, hasV := result1.Members["veteran"]
	_, hasL := result1.Members["leader"]
	if !hasW || !hasV || !hasL {
		t.Errorf("Initial members incorrect: %v", result1.Members)
	}

	// Step 2: Replace the set with a new slice using Set()
	// New set: ["mage", "warrior"] - removes "veteran", "leader"; keeps "warrior"; adds "mage"
	tx2 := db.NewTransaction()
	if err := tx2.Set(entityID, attr, []interface{}{"mage", "warrior"}); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Step 3: Verify the set now contains only the new values
	result2, err := matcher.resolveAddWinsSet(eBytes[:], aBytes[:])
	if err != nil {
		t.Fatalf("resolveAddWinsSet failed: %v", err)
	}
	if len(result2.Members) != 2 {
		t.Errorf("Expected 2 members after Set, got %d: %v", len(result2.Members), result2.Members)
	}
	_, hasMage := result2.Members["mage"]
	_, hasWarrior := result2.Members["warrior"]
	_, hasVeteran := result2.Members["veteran"]
	_, hasLeader := result2.Members["leader"]
	if !hasMage {
		t.Error("Expected 'mage' to be in set")
	}
	if !hasWarrior {
		t.Error("Expected 'warrior' to be in set")
	}
	if hasVeteran {
		t.Error("'veteran' should have been removed")
	}
	if hasLeader {
		t.Error("'leader' should have been removed")
	}
}

// TestAddSchemaAware verifies Add() is schema-aware and works for all cardinalities
// Add() uses LWW semantics for cardinality-one, add-wins for cardinality-many
func TestAddSchemaAware(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-add-schema-aware-*")
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
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	s.Add(&schema.AttributeDefinition{
		Ident:       datalog.NewKeyword(":person/tags"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")

	// Add() should work for cardinality-one (uses LWW internally)
	tx := db.NewTransaction()
	err = tx.Add(entityID, datalog.NewKeyword(":person/name"), "Alice")
	if err != nil {
		t.Errorf("Add() should work for cardinality-one: %v", err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Add() should work for cardinality-many (uses add-wins internally)
	tx2 := db.NewTransaction()
	err = tx2.Add(entityID, datalog.NewKeyword(":person/tags"), "developer")
	if err != nil {
		t.Errorf("Add() should work for cardinality-many: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify LWW behavior: second Add() to cardinality-one updates value
	tx3 := db.NewTransaction()
	err = tx3.Add(entityID, datalog.NewKeyword(":person/name"), "Bob")
	if err != nil {
		t.Errorf("Second Add() should work for cardinality-one: %v", err)
	}
	_, err = tx3.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query should return the latest value (LWW)
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	result, found := requireAttributeLookup(
		t,
		matcher,
		entityID,
		datalog.NewKeyword(":person/name"),
	)
	if !found {
		t.Fatal("LookupAttribute returned not found")
	}
	if result != "Bob" {
		t.Errorf("Expected 'Bob' (LWW), got %v", result)
	}
}

// TestRemoveCardinalityValidation verifies Remove() works for all cardinalities.
// Remove writes OpCRDTRemove regardless of cardinality.
func TestRemoveCardinalityValidation(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-remove-validation-*")
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
		Ident:       datalog.NewKeyword(":person/name"),
		ValueType:   schema.TypeString,
		Cardinality: schema.CardinalityOne,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("test-entity")

	// Remove() on CardinalityOne should succeed (writes OpCRDTRemove)
	tx := db.NewTransaction()
	err = tx.Remove(entityID, datalog.NewKeyword(":person/name"), "Alice")
	if err != nil {
		t.Errorf("Remove() should succeed for cardinality-one attribute: %v", err)
	} else {
		t.Log("Remove correctly accepted for cardinality-one (writes OpCRDTRemove)")
	}

	// Remove() on unknown/schemaless attributes should succeed (CardinalityOne default)
	tx2 := db.NewTransaction()
	err = tx2.Remove(entityID, datalog.NewKeyword(":unknown/attr"), "value")
	if err != nil {
		t.Errorf("Remove() should succeed for unknown attribute: %v", err)
	} else {
		t.Log("Remove correctly accepted for unknown attribute (CardinalityOne default)")
	}
}

// TestCardinalityManySetSeesPendingOps verifies that Set() sees Add/Remove ops
// from earlier in the same transaction (Bug #3 fix)
func TestCardinalityManySetSeesPendingOps(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-pending-*")
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

	// Test 1: Add() then Set() in same transaction
	// Add("foo"), then Set({"bar"}) should result in {"bar"}, not {"foo", "bar"}
	tx1 := db.NewTransaction()
	err = tx1.Add(entityID, attr, "foo")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	err = tx1.Set(entityID, attr, []interface{}{"bar"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = tx1.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query to verify result
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
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
	iter.Close()

	// Should only have "bar", not "foo"
	if len(tags) != 1 {
		t.Errorf("Expected 1 tag, got %d: %v", len(tags), tags)
	}
	if !tags["bar"] {
		t.Error("Expected 'bar' tag to be present")
	}
	if tags["foo"] {
		t.Error("'foo' should have been removed by Set()")
	}
}

// TestCardinalityManySetSeesCommittedAndPending verifies that Set() correctly
// handles both committed state and pending ops
func TestCardinalityManySetSeesCommittedAndPending(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-mixed-*")
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

	// First transaction: commit some values
	tx1 := db.NewTransaction()
	err = tx1.Add(entityID, attr, "committed1")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	err = tx1.Add(entityID, attr, "committed2")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	_, err = tx1.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Second transaction: Add pending, then Set() should see both committed and pending
	tx2 := db.NewTransaction()
	err = tx2.Add(entityID, attr, "pending1")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	// Set to only keep "committed1" - should remove "committed2" and "pending1"
	err = tx2.Set(entityID, attr, []interface{}{"committed1"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query to verify result
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
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
	iter.Close()

	// Should only have "committed1"
	if len(tags) != 1 {
		t.Errorf("Expected 1 tag, got %d: %v", len(tags), tags)
	}
	if !tags["committed1"] {
		t.Error("Expected 'committed1' tag to be present")
	}
	if tags["committed2"] {
		t.Error("'committed2' should have been removed by Set()")
	}
	if tags["pending1"] {
		t.Error("'pending1' should have been removed by Set()")
	}
}

// TestCardinalityManyUnboundEWithValue tests [?e :attr "value"] patterns
// where E is unbound and V is bound - finds all entities with specific value in set
func TestCardinalityManyUnboundEWithValue(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-unbound-e-value-*")
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

	attr := datalog.NewKeyword(":person/tags")

	// Create multiple entities with different tag combinations
	entity1 := datalog.NewIdentity("person-1")
	entity2 := datalog.NewIdentity("person-2")
	entity3 := datalog.NewIdentity("person-3")

	tx := db.NewTransaction()
	// Entity 1: has "warrior" and "veteran"
	tx.Add(entity1, attr, "warrior")
	tx.Add(entity1, attr, "veteran")
	// Entity 2: has "warrior" only
	tx.Add(entity2, attr, "warrior")
	// Entity 3: has "mage" only (no "warrior")
	tx.Add(entity3, attr, "mage")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query: [?e :person/tags "warrior"] - should find entity1 and entity2
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "warrior"},
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
			if ident, ok := tuple[0].(datalog.Identity); ok {
				entities[ident.String()] = true
			}
		}
	}
	iter.Close()

	// Should find exactly entity1 and entity2
	if len(entities) != 2 {
		t.Errorf("Expected 2 entities with 'warrior' tag, got %d: %v", len(entities), entities)
	}
	if !entities[entity1.String()] {
		t.Error("Expected entity1 to have 'warrior' tag")
	}
	if !entities[entity2.String()] {
		t.Error("Expected entity2 to have 'warrior' tag")
	}
	if entities[entity3.String()] {
		t.Error("entity3 should NOT have 'warrior' tag")
	}
}

// TestCardinalityManyUnboundEUnboundV tests [?e :attr ?v] patterns
// where both E and V are unbound - returns all entity/value pairs
func TestCardinalityManyUnboundEUnboundV(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-unbound-ev-*")
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

	attr := datalog.NewKeyword(":person/tags")

	// Create multiple entities with different tag combinations
	entity1 := datalog.NewIdentity("person-1")
	entity2 := datalog.NewIdentity("person-2")

	tx := db.NewTransaction()
	tx.Add(entity1, attr, "warrior")
	tx.Add(entity1, attr, "veteran")
	tx.Add(entity2, attr, "mage")
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query: [?e :person/tags ?v] - should return all entity/tag pairs
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	// Collect all (entity, value) pairs
	type pair struct {
		entity string
		value  string
	}
	pairs := make(map[pair]bool)
	iter := results.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if len(tuple) >= 2 {
			var e string
			var v string
			if ident, ok := tuple[0].(datalog.Identity); ok {
				e = ident.String()
			}
			if tag, ok := tuple[1].(string); ok {
				v = tag
			}
			if e != "" && v != "" {
				pairs[pair{e, v}] = true
			}
		}
	}
	iter.Close()

	// Should have 3 pairs: (entity1, warrior), (entity1, veteran), (entity2, mage)
	if len(pairs) != 3 {
		t.Errorf("Expected 3 entity/tag pairs, got %d: %v", len(pairs), pairs)
	}
	if !pairs[pair{entity1.String(), "warrior"}] {
		t.Error("Expected (entity1, warrior) pair")
	}
	if !pairs[pair{entity1.String(), "veteran"}] {
		t.Error("Expected (entity1, veteran) pair")
	}
	if !pairs[pair{entity2.String(), "mage"}] {
		t.Error("Expected (entity2, mage) pair")
	}
}

// TestCardinalityManySetWithDuplicates tests Set() with duplicate values in slice
func TestCardinalityManySetWithDuplicates(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-set-dups-*")
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

	// Set with duplicates in the slice
	tx := db.NewTransaction()
	err = tx.Set(entityID, attr, []interface{}{"a", "b", "a", "c", "b", "a"})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query to verify - should have exactly {a, b, c}
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
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
	iter.Close()

	// Should have exactly 3 unique values
	if len(tags) != 3 {
		t.Errorf("Expected 3 unique tags, got %d: %v", len(tags), tags)
	}
	if !tags["a"] || !tags["b"] || !tags["c"] {
		t.Error("Expected tags {a, b, c}")
	}
}

// TestCardinalityManyUnboundEWithRemovedValue tests that removed values
// don't appear in [?e :attr "value"] query results
func TestCardinalityManyUnboundEWithRemovedValue(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-unbound-removed-*")
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

	attr := datalog.NewKeyword(":person/tags")

	entity1 := datalog.NewIdentity("person-1")
	entity2 := datalog.NewIdentity("person-2")

	// Add "warrior" to both entities
	tx1 := db.NewTransaction()
	tx1.Add(entity1, attr, "warrior")
	tx1.Add(entity2, attr, "warrior")
	_, err = tx1.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Remove "warrior" from entity2
	tx2 := db.NewTransaction()
	tx2.Remove(entity2, attr, "warrior")
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Query: [?e :person/tags "warrior"] - should only find entity1
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "warrior"},
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
			if ident, ok := tuple[0].(datalog.Identity); ok {
				entities[ident.String()] = true
			}
		}
	}
	iter.Close()

	// Should find only entity1
	if len(entities) != 1 {
		t.Errorf("Expected 1 entity with 'warrior' tag, got %d: %v", len(entities), entities)
	}
	if !entities[entity1.String()] {
		t.Error("Expected entity1 to have 'warrior' tag")
	}
	if entities[entity2.String()] {
		t.Error("entity2 should NOT have 'warrior' tag (it was removed)")
	}
}

// TestAVETOptimizationForCardinalityMany tests that [?e :attr "value"] queries
// use the AVET index for O(k) performance where k = datoms with value,
// instead of O(n) where n = all entities with attribute.
func TestAVETOptimizationForCardinalityMany(t *testing.T) {
	dir, err := os.MkdirTemp("", "avet-optimization-test-*")
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

	attr := datalog.NewKeyword(":person/tags")

	// Create many entities with various tags
	// 10 entities with "warrior", 90 entities with other tags
	for i := 0; i < 100; i++ {
		entity := datalog.NewIdentity(fmt.Sprintf("person-%d", i))
		tx := db.NewTransaction()
		if i < 10 {
			tx.Add(entity, attr, "warrior")
		}
		tx.Add(entity, attr, "citizen")               // All have this
		tx.Add(entity, attr, fmt.Sprintf("id-%d", i)) // Unique tag
		_, err = tx.Commit()
		if err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
	}

	// Query: [?e :person/tags "warrior"] - should find exactly 10 entities
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "warrior"},
		},
	}

	results, err := matcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	var count int
	iter := results.Iterator()
	for iter.Next() {
		count++
	}
	iter.Close()

	if count != 10 {
		t.Errorf("Expected 10 entities with 'warrior' tag, got %d", count)
	}

	// Query for "citizen" - should find all 100 entities
	pattern2 := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "citizen"},
		},
	}

	results2, err := matcher.Match(query.PatternQuery(pattern2), nil)
	if err != nil {
		t.Fatalf("Match failed: %v", err)
	}

	count = 0
	iter2 := results2.Iterator()
	for iter2.Next() {
		count++
	}
	iter2.Close()

	if count != 100 {
		t.Errorf("Expected 100 entities with 'citizen' tag, got %d", count)
	}
}

// TestAVETAddWinsResolution tests that the AVET-based iterator correctly
// applies add-wins resolution per entity.
func TestAVETAddWinsResolution(t *testing.T) {
	dir, err := os.MkdirTemp("", "avet-addwins-test-*")
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

	attr := datalog.NewKeyword(":person/tags")

	entity1 := datalog.NewIdentity("person-1")
	entity2 := datalog.NewIdentity("person-2")
	entity3 := datalog.NewIdentity("person-3")

	// entity1: Add only
	tx1 := db.NewTransaction()
	tx1.Add(entity1, attr, "warrior")
	_, _ = tx1.Commit()

	// entity2: Add then Remove (should NOT be in set)
	tx2 := db.NewTransaction()
	tx2.Add(entity2, attr, "warrior")
	_, _ = tx2.Commit()

	tx3 := db.NewTransaction()
	tx3.Remove(entity2, attr, "warrior")
	_, _ = tx3.Commit()

	// entity3: Remove then Add (should be in set)
	tx4 := db.NewTransaction()
	tx4.Remove(entity3, attr, "warrior") // Tombstone before any add - unusual but valid
	_, _ = tx4.Commit()

	tx5 := db.NewTransaction()
	tx5.Add(entity3, attr, "warrior")
	_, _ = tx5.Commit()

	// Query: [?e :person/tags "warrior"]
	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "warrior"},
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
			if ident, ok := tuple[0].(datalog.Identity); ok {
				entities[ident.String()] = true
			}
		}
	}
	iter.Close()

	// Should find entity1 and entity3, not entity2
	if len(entities) != 2 {
		t.Errorf("Expected 2 entities, got %d: %v", len(entities), entities)
	}
	if !entities[entity1.String()] {
		t.Error("entity1 should have 'warrior' tag (add only)")
	}
	if entities[entity2.String()] {
		t.Error("entity2 should NOT have 'warrior' tag (add then remove)")
	}
	if !entities[entity3.String()] {
		t.Error("entity3 should have 'warrior' tag (remove then add)")
	}
}

// TestHistoryModeCardinalityMany verifies that db.History() returns raw
// datoms including retract operations, bypassing add-wins CRDT resolution.
func TestHistoryModeCardinalityMany(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-history-mode-*")
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

	// Add two tags
	tx1 := db.NewTransaction()
	err = tx1.Add(entityID, attr, "warrior")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	err = tx1.Add(entityID, attr, "veteran")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	_, err = tx1.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Remove one tag
	tx2 := db.NewTransaction()
	err = tx2.Remove(entityID, attr, "warrior")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	_, err = tx2.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
			query.Blank{},
		},
	}

	// Latest mode: add-wins resolution should show only "veteran"
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
		t.Errorf("Latest mode: expected 1 result (add-wins resolved), got %d", latestCount)
	}

	// History mode with Tx projected away: the add and the remove of
	// "warrior" are distinct operation records, but both project to the same
	// ("warrior") tuple — and a relation is a set, so the ?tag projection
	// carries two bindings. Observing every raw operation requires binding
	// ?tx, which distinguishes the records (each datom has its own ElementID).
	historyMatcher := db.History()
	historyResults, err := historyMatcher.Match(query.PatternQuery(pattern), nil)
	if err != nil {
		t.Fatalf("History Match failed: %v", err)
	}
	historyCount := 0
	historyIter := historyResults.Iterator()
	for historyIter.Next() {
		historyCount++
	}
	if historyCount != 2 {
		t.Errorf("History mode ?tag projection: expected 2 distinct tag bindings, got %d", historyCount)
	}

	// History mode with Tx bound: all raw operations visible
	// (2 adds + 1 remove = 3 datoms).
	txPattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
			query.Variable{Name: datalog.NewSymbol("?tx")},
		},
	}
	historyTxResults, err := historyMatcher.Match(query.PatternQuery(txPattern), nil)
	if err != nil {
		t.Fatalf("History Match with ?tx failed: %v", err)
	}
	historyTxCount := 0
	historyTxIter := historyTxResults.Iterator()
	for historyTxIter.Next() {
		historyTxCount++
	}
	if historyTxCount != 3 {
		t.Errorf("History mode ?tag ?tx projection: expected 3 operation records (2 adds + 1 remove), got %d", historyTxCount)
	}
}

// TestLookupAttributeManyBytesMembers exercises the storage-scan fallback of
// LookupAttribute (matcher without a cache) for a cardinality-many bytes
// attribute. []byte members must resolve through add-wins like any other
// value type.
func TestLookupAttributeManyBytesMembers(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-many-bytes-lookup-*")
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
		Ident:       datalog.NewKeyword(":doc/chunks"),
		ValueType:   schema.TypeBytes,
		Cardinality: schema.CardinalityMany,
	})
	db.SetSchema(s)

	entityID := datalog.NewIdentity("bytes-entity")
	attr := datalog.NewKeyword(":doc/chunks")

	tx1 := db.NewTransaction()
	if err := tx1.Add(entityID, attr, []byte("alpha")); err != nil {
		t.Fatalf("Add alpha failed: %v", err)
	}
	if err := tx1.Add(entityID, attr, []byte("beta")); err != nil {
		t.Fatalf("Add beta failed: %v", err)
	}
	if _, err := tx1.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	tx2 := db.NewTransaction()
	if err := tx2.Remove(entityID, attr, []byte("alpha")); err != nil {
		t.Fatalf("Remove alpha failed: %v", err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	matcher := NewPatternMatcher(db.Store())
	matcher.SetSchema(s)
	value, found := requireAttributeLookup(t, matcher, entityID, attr)
	if !found {
		t.Fatal("LookupAttribute returned not found")
	}
	members, ok := value.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", value)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 member after remove, got %d: %v", len(members), members)
	}
	b, ok := members[0].([]byte)
	if !ok {
		t.Fatalf("expected []byte member, got %T", members[0])
	}
	if string(b) != "beta" {
		t.Errorf("expected beta to survive, got %q", b)
	}
}
