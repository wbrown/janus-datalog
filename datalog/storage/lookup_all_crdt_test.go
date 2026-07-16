//go:build !(js && wasm)

package storage

import (
	"os"
	"sort"
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
)

// Tests for LookupAllAttributes CRDT resolution without cache or schema.
// These verify that the fallback path infers cardinality from ops and
// resolves correctly, rather than returning raw datoms including tombstones.

// --- Cardinality-One (LWW): OpNone datoms ---

func TestLookupAllAttributes_NoCacheNoSchema_LWW_LatestValue(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("lww-entity")
	attr := datalog.NewKeyword(":person/name")

	// Two LWW writes: older value then newer value
	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "Alice", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, Op: datalog.OpNone},
		{E: entity, A: attr, V: "Bob", Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}, Op: datalog.OpNone},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	// LWW: should return only the latest value
	if len(vals) != 1 {
		t.Errorf("expected 1 value (latest LWW), got %d: %v", len(vals), vals)
	} else if vals[0] != "Bob" {
		t.Errorf("expected latest value 'Bob', got %v", vals[0])
	}
}

func TestLookupAllAttributes_NoCacheNoSchema_LWW_SingleValue(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("lww-single")
	attr := datalog.NewKeyword(":person/name")

	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "Alice", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, Op: datalog.OpNone},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	if len(vals) != 1 {
		t.Errorf("expected 1 value, got %d: %v", len(vals), vals)
	} else if vals[0] != "Alice" {
		t.Errorf("expected 'Alice', got %v", vals[0])
	}
}

// --- Cardinality-Many (Add-Wins Set): OpCRDTAdd / OpCRDTRemove datoms ---

func TestLookupAllAttributes_NoCacheNoSchema_AddWins_Basic(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("set-entity")
	attr := datalog.NewKeyword(":person/tags")

	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "fantasy", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		{E: entity, A: attr, V: "epic", Tx: datalog.ElementID{Lamport: 101, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 102, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	got := toStringSlice(t, vals)
	sort.Strings(got)
	expected := []string{"adventure", "epic", "fantasy"}
	assertStringSlicesEqual(t, "basic add-wins", got, expected)
}

func TestLookupAllAttributes_NoCacheNoSchema_AddWins_AfterRemoval(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("set-removal")
	attr := datalog.NewKeyword(":person/tags")

	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "fantasy", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 101, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		{E: entity, A: attr, V: "epic", Tx: datalog.ElementID{Lamport: 102, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		// Remove "adventure" at higher Lamport
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}, Op: datalog.OpCRDTRemove},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	got := toStringSlice(t, vals)
	sort.Strings(got)
	expected := []string{"epic", "fantasy"}
	assertStringSlicesEqual(t, "after removal", got, expected)
}

func TestLookupAllAttributes_NoCacheNoSchema_AddWins_ReAdd(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("set-readd")
	attr := datalog.NewKeyword(":person/tags")

	datoms := []datalog.Datom{
		// Initial add
		{E: entity, A: attr, V: "fantasy", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 101, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		{E: entity, A: attr, V: "epic", Tx: datalog.ElementID{Lamport: 102, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		// Remove "adventure"
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1}, Op: datalog.OpCRDTRemove},
		// Re-add "adventure" at even higher Lamport
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 300, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	got := toStringSlice(t, vals)
	sort.Strings(got)
	expected := []string{"adventure", "epic", "fantasy"}
	assertStringSlicesEqual(t, "after re-add", got, expected)
}

func TestLookupAllAttributes_NoCacheNoSchema_AddWins_ConcurrentAddWins(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("set-concurrent")
	attr := datalog.NewKeyword(":person/tags")

	// Same Lamport for add and remove — add should win
	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, Op: datalog.OpCRDTAdd},
		{E: entity, A: attr, V: "adventure", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 2}, Op: datalog.OpCRDTRemove},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	got := toStringSlice(t, vals)
	expected := []string{"adventure"}
	assertStringSlicesEqual(t, "concurrent add wins", got, expected)
}

func TestLookupAllAttributes_NoCacheNoSchema_AddWins_OnlyRemoves(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("set-only-removes")
	attr := datalog.NewKeyword(":person/tags")

	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "ghost", Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1}, Op: datalog.OpCRDTRemove},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	if len(vals) != 0 {
		t.Errorf("expected 0 values for remove-only, got %d: %v", len(vals), vals)
	}
}

// --- Cardinality-Vector (RGA): OpRGAInsert / OpRGATombstone datoms ---

func TestLookupAllAttributes_NoCacheNoSchema_RGA_Basic(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("vec-entity")
	attr := datalog.NewKeyword(":doc/content")

	// Three inserts forming a chain: HEAD -> elem1 -> elem2 -> elem3
	elem1 := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	elem2 := datalog.ElementID{Lamport: 101, ReplicaID: 1}
	elem3 := datalog.ElementID{Lamport: 102, ReplicaID: 1}

	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "line one", Tx: elem1, Op: datalog.OpRGAInsert, AfterRef: datalog.ElementID{}}, // after HEAD
		{E: entity, A: attr, V: "line two", Tx: elem2, Op: datalog.OpRGAInsert, AfterRef: elem1},               // after elem1
		{E: entity, A: attr, V: "line three", Tx: elem3, Op: datalog.OpRGAInsert, AfterRef: elem2},             // after elem2
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	// RGA should preserve insertion order
	expected := []interface{}{"line one", "line two", "line three"}
	assertInterfaceSlicesEqual(t, "basic RGA", vals, expected)
}

func TestLookupAllAttributes_NoCacheNoSchema_RGA_WithTombstone(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("vec-tombstone")
	attr := datalog.NewKeyword(":doc/content")

	elem1 := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	elem2 := datalog.ElementID{Lamport: 101, ReplicaID: 1}
	elem3 := datalog.ElementID{Lamport: 102, ReplicaID: 1}
	tombstone := datalog.ElementID{Lamport: 200, ReplicaID: 1}

	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "line one", Tx: elem1, Op: datalog.OpRGAInsert, AfterRef: datalog.ElementID{}},
		{E: entity, A: attr, V: "line two", Tx: elem2, Op: datalog.OpRGAInsert, AfterRef: elem1},
		{E: entity, A: attr, V: "line three", Tx: elem3, Op: datalog.OpRGAInsert, AfterRef: elem2},
		// Tombstone elem2 ("line two")
		{E: entity, A: attr, V: "line two", Tx: tombstone, Op: datalog.OpRGATombstone, AfterRef: elem2},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	// elem2 is tombstoned, but elem3 (its child) should still appear
	expected := []interface{}{"line one", "line three"}
	assertInterfaceSlicesEqual(t, "RGA with tombstone", vals, expected)
}

func TestLookupAllAttributes_NoCacheNoSchema_RGA_ConcurrentInserts(t *testing.T) {
	db, cleanup := createLookupTestDB(t)
	defer cleanup()

	entity := datalog.NewIdentity("vec-concurrent")
	attr := datalog.NewKeyword(":doc/content")

	// Two concurrent inserts after HEAD — lower ReplicaID first
	elemA := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	elemB := datalog.ElementID{Lamport: 100, ReplicaID: 2}

	datoms := []datalog.Datom{
		{E: entity, A: attr, V: "from replica 1", Tx: elemA, Op: datalog.OpRGAInsert, AfterRef: datalog.ElementID{}},
		{E: entity, A: attr, V: "from replica 2", Tx: elemB, Op: datalog.OpRGAInsert, AfterRef: datalog.ElementID{}},
	}
	if err := db.Store().Assert(datoms); err != nil {
		t.Fatalf("Assert failed: %v", err)
	}

	matcher := NewBadgerMatcher(db.Store())
	vals, err := matcher.LookupAllAttributes(entity, attr)
	if err != nil {
		t.Fatalf("LookupAllAttributes: %v", err)
	}

	// Both preserved, ordered by ElementID (lower ReplicaID first)
	expected := []interface{}{"from replica 1", "from replica 2"}
	assertInterfaceSlicesEqual(t, "concurrent RGA inserts", vals, expected)
}

// --- Test utilities ---

func createLookupTestDB(t *testing.T) (*Database, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "lookup-all-crdt-*")
	if err != nil {
		t.Fatal(err)
	}
	db, err := NewDatabase(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return db, func() {
		db.Close()
		os.RemoveAll(dir)
	}
}

func toStringSlice(t *testing.T, vals []interface{}) []string {
	t.Helper()
	result := make([]string, len(vals))
	for i, v := range vals {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("expected string at index %d, got %T: %v", i, v, v)
		}
		result[i] = s
	}
	return result
}

func assertStringSlicesEqual(t *testing.T, context string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: expected %d values %v, got %d values %v", context, len(want), want, len(got), got)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s: value[%d] expected %q, got %q", context, i, want[i], got[i])
		}
	}
}

func assertInterfaceSlicesEqual(t *testing.T, context string, got, want []interface{}) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: expected %d values %v, got %d values %v", context, len(want), want, len(got), got)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s: value[%d] expected %v, got %v", context, i, want[i], got[i])
		}
	}
}
