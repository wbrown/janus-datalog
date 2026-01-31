package storage

import (
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

	// Store two SetEntry values: one add, one remove for same value
	addEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpAdd})
	removeEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpRemove})

	// Add at Lamport 1000, Remove at Lamport 900 (Add is newer, should be in set)
	datoms := []datalog.Datom{
		{E: entityID, A: attr, V: addEntry, Tx: datalog.ElementID{Lamport: 1000, ReplicaID: 100}},
		{E: entityID, A: attr, V: removeEntry, Tx: datalog.ElementID{Lamport: 900, ReplicaID: 100}},
	}
	err = db.Store().Assert(datoms)
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Create matcher and call resolveAddWinsSet directly
	matcher := NewBadgerMatcher(db.Store())
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
	} else if !result.Members["warrior"] {
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

	// Store two SetEntry values: add and remove with SAME Lamport
	addEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpAdd})
	removeEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpRemove})

	sameLamport := uint64(1000)
	datoms := []datalog.Datom{
		{E: entityID, A: attr, V: addEntry, Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: 100}},
		{E: entityID, A: attr, V: removeEntry, Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: 200}},
	}
	err = db.Store().Assert(datoms)
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Create matcher and call resolveAddWinsSet directly
	matcher := NewBadgerMatcher(db.Store())
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
	} else if !result.Members["warrior"] {
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
	matcher := NewBadgerMatcher(db.Store())
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

	results, err := matcher.Match(pattern, nil)
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
	results2, err := matcher.Match(pattern, nil)
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

	// Encode the value as SetEntry
	addEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpAdd})
	removeEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpRemove})

	// Create datoms - Add from replica A, Remove from replica B, same Lamport
	datomAdd := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  addEntry,
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaA},
	}
	datomRemove := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  removeEntry,
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaB},
	}

	// Assert both directly
	err = db.Store().Assert([]datalog.Datom{datomAdd, datomRemove})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	// Query should return the value (add-wins at same Lamport)
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

	results, err := matcher.Match(pattern, nil)
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

	addEntryA := EncodeSetEntry(SetEntry{Value: "tagA", Op: OpAdd})
	addEntryB := EncodeSetEntry(SetEntry{Value: "tagB", Op: OpAdd})

	datomA := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  addEntryA,
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaA},
	}
	datomB := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  addEntryB,
		Tx: datalog.ElementID{Lamport: sameLamport, ReplicaID: replicaB},
	}

	err = db.Store().Assert([]datalog.Datom{datomA, datomB})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

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

	results, err := matcher.Match(pattern, nil)
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

	results, err := matcher.Match(pattern, nil)
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
	removeEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpRemove})
	addEntry := EncodeSetEntry(SetEntry{Value: "warrior", Op: OpAdd})

	datomRemove := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  removeEntry,
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, // Earlier
	}
	datomAdd := datalog.Datom{
		E:  entityID,
		A:  attr,
		V:  addEntry,
		Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}, // Later
	}

	err = db.Store().Assert([]datalog.Datom{datomRemove, datomAdd})
	if err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

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

	results, err := matcher.Match(pattern, nil)
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

	results, err := matcher.Match(pattern, nil)
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

	results, err := matcher.Match(pattern, nil)
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

	// Use MatchWithHistory to get all operations
	matcher := NewBadgerMatcher(db.Store())

	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
			query.Blank{},
		},
	}

	results, err := matcher.MatchWithHistory(pattern)
	if err != nil {
		t.Fatalf("MatchWithHistory failed: %v", err)
	}

	// Should have 3 entries (2 adds + 1 remove)
	if len(results) != 3 {
		t.Errorf("Expected 3 historical entries, got %d", len(results))
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

	matcher := NewBadgerMatcher(db.Store())
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

	results, err := matcher.Match(pattern, nil)
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

	results2, err := matcher.Match(pattern2, nil)
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
	matcher := NewBadgerMatcher(db.Store())
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
	if !result1.Members["warrior"] || !result1.Members["veteran"] || !result1.Members["leader"] {
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
	if !result2.Members["mage"] {
		t.Error("Expected 'mage' to be in set")
	}
	if !result2.Members["warrior"] {
		t.Error("Expected 'warrior' to be in set")
	}
	if result2.Members["veteran"] {
		t.Error("'veteran' should have been removed")
	}
	if result2.Members["leader"] {
		t.Error("'leader' should have been removed")
	}
}

// TestAddCardinalityValidation verifies Add() rejects cardinality-one attributes
func TestAddCardinalityValidation(t *testing.T) {
	dir, err := os.MkdirTemp("", "crdt-add-validation-*")
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

	tx := db.NewTransaction()
	err = tx.Add(entityID, datalog.NewKeyword(":person/name"), "Alice")
	if err == nil {
		t.Error("Expected Add() to fail for cardinality-one attribute")
	} else {
		t.Logf("Add correctly rejected for cardinality-one: %v", err)
	}
}

// TestRemoveCardinalityValidation verifies Remove() rejects non-cardinality-many attributes
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

	tx := db.NewTransaction()
	err = tx.Remove(entityID, datalog.NewKeyword(":person/name"), "Alice")
	if err == nil {
		t.Error("Expected Remove() to fail for cardinality-one attribute")
	} else {
		t.Logf("Remove correctly rejected for cardinality-one: %v", err)
	}

	// Also test that Remove() requires schema
	tx2 := db.NewTransaction()
	err = tx2.Remove(entityID, datalog.NewKeyword(":unknown/attr"), "value")
	if err == nil {
		t.Error("Expected Remove() to fail for unknown attribute (no schema)")
	} else {
		t.Logf("Remove correctly rejected for unknown attribute: %v", err)
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
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
		},
	}

	results, err := matcher.Match(pattern, nil)
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
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
		},
	}

	results, err := matcher.Match(pattern, nil)
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
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "warrior"},
		},
	}

	results, err := matcher.Match(pattern, nil)
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
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?v")},
		},
	}

	results, err := matcher.Match(pattern, nil)
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
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Constant{Value: entityID},
			query.Constant{Value: attr},
			query.Variable{Name: datalog.NewSymbol("?tag")},
		},
	}

	results, err := matcher.Match(pattern, nil)
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
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)
	pattern := &query.DataPattern{
		Elements: []query.PatternElement{
			query.Variable{Name: datalog.NewSymbol("?e")},
			query.Constant{Value: attr},
			query.Constant{Value: "warrior"},
		},
	}

	results, err := matcher.Match(pattern, nil)
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
