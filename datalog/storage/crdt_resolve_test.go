package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wbrown/janus-datalog/datalog"
)

// Helper to create ElementID with specific Lamport clock
func elemID(lamport uint64, replica uint64) datalog.ElementID {
	return datalog.ElementID{Lamport: lamport, ReplicaID: replica}
}

// Helper to create a datom for testing
func testDatom(tx datalog.ElementID, value any, op datalog.CRDTOp) datalog.Datom {
	return datalog.Datom{
		Tx: tx,
		V:  value,
		Op: op,
	}
}

// Helper to create RGA datom with AfterRef
func rgaDatom(tx datalog.ElementID, value any, op datalog.CRDTOp, afterRef datalog.ElementID) datalog.Datom {
	return datalog.Datom{
		Tx:       tx,
		V:        value,
		Op:       op,
		AfterRef: afterRef,
	}
}

// =============================================================================
// LWW Resolution Tests
// =============================================================================

func TestResolveLWWFromDatoms_Empty(t *testing.T) {
	val, maxID := ResolveLWWFromDatoms(nil)
	assert.Nil(t, val)
	assert.Equal(t, datalog.ElementID{}, maxID)
}

func TestResolveLWWFromDatoms_Single(t *testing.T) {
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "Alice", datalog.OpNone),
	}

	val, maxID := ResolveLWWFromDatoms(datoms)
	assert.Equal(t, "Alice", val)
	assert.Equal(t, elemID(100, 1), maxID)
}

func TestResolveLWWFromDatoms_HighestWins(t *testing.T) {
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "Alice", datalog.OpNone),
		testDatom(elemID(200, 1), "Bob", datalog.OpNone),
		testDatom(elemID(150, 1), "Charlie", datalog.OpNone),
	}

	val, maxID := ResolveLWWFromDatoms(datoms)
	assert.Equal(t, "Bob", val) // Highest Lamport wins
	assert.Equal(t, elemID(200, 1), maxID)
}

func TestResolveLWWFromDatoms_SameLamportDifferentReplica(t *testing.T) {
	// When Lamport is same, higher ReplicaID wins (ElementID.Compare behavior)
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "Alice", datalog.OpNone),
		testDatom(elemID(100, 2), "Bob", datalog.OpNone),
	}

	val, maxID := ResolveLWWFromDatoms(datoms)
	assert.Equal(t, "Bob", val) // Higher ReplicaID wins
	assert.Equal(t, elemID(100, 2), maxID)
}

// =============================================================================
// Add-Wins Set Resolution Tests
// =============================================================================

func TestResolveAddWinsFromDatoms_Empty(t *testing.T) {
	members, maxID := ResolveAddWinsFromDatoms(nil)
	assert.Empty(t, members)
	assert.Equal(t, datalog.ElementID{}, maxID)
}

func TestResolveAddWinsFromDatoms_OnlyAdds(t *testing.T) {
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "tag1", datalog.OpCRDTAdd),
		testDatom(elemID(101, 1), "tag2", datalog.OpCRDTAdd),
	}

	members, maxID := ResolveAddWinsFromDatoms(datoms)
	assert.True(t, members["tag1"])
	assert.True(t, members["tag2"])
	assert.Equal(t, 2, len(members))
	assert.Equal(t, elemID(101, 1), maxID)
}

func TestResolveAddWinsFromDatoms_AddThenRemove_RemoveWins(t *testing.T) {
	// Remove has higher Lamport -> not in set
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "tag1", datalog.OpCRDTAdd),
		testDatom(elemID(200, 1), "tag1", datalog.OpCRDTRemove),
	}

	members, _ := ResolveAddWinsFromDatoms(datoms)
	assert.False(t, members["tag1"])
	assert.Equal(t, 0, len(members))
}

func TestResolveAddWinsFromDatoms_RemoveThenAdd_AddWins(t *testing.T) {
	// Add has higher Lamport -> in set
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "tag1", datalog.OpCRDTRemove),
		testDatom(elemID(200, 1), "tag1", datalog.OpCRDTAdd),
	}

	members, _ := ResolveAddWinsFromDatoms(datoms)
	assert.True(t, members["tag1"])
}

func TestResolveAddWinsFromDatoms_Concurrent_AddWins(t *testing.T) {
	// Same Lamport (concurrent) -> add wins regardless of ReplicaID
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "tag1", datalog.OpCRDTAdd),
		testDatom(elemID(100, 2), "tag1", datalog.OpCRDTRemove), // Same Lamport, higher replica
	}

	members, _ := ResolveAddWinsFromDatoms(datoms)
	assert.True(t, members["tag1"]) // Add wins at same Lamport
}

func TestResolveAddWinsFromDatoms_MultipleValues(t *testing.T) {
	datoms := []datalog.Datom{
		// tag1: added then removed (remove wins)
		testDatom(elemID(100, 1), "tag1", datalog.OpCRDTAdd),
		testDatom(elemID(200, 1), "tag1", datalog.OpCRDTRemove),
		// tag2: only added (in set)
		testDatom(elemID(150, 1), "tag2", datalog.OpCRDTAdd),
		// tag3: removed then re-added (add wins)
		testDatom(elemID(100, 1), "tag3", datalog.OpCRDTAdd),
		testDatom(elemID(200, 1), "tag3", datalog.OpCRDTRemove),
		testDatom(elemID(300, 1), "tag3", datalog.OpCRDTAdd),
	}

	members, maxID := ResolveAddWinsFromDatoms(datoms)
	assert.False(t, members["tag1"]) // Removed
	assert.True(t, members["tag2"])  // Only added
	assert.True(t, members["tag3"])  // Re-added
	assert.Equal(t, 2, len(members))
	assert.Equal(t, elemID(300, 1), maxID)
}

func TestResolveAddWinsFromDatoms_OnlyRemoves(t *testing.T) {
	// Remove without prior add -> not in set
	datoms := []datalog.Datom{
		testDatom(elemID(100, 1), "tag1", datalog.OpCRDTRemove),
	}

	members, _ := ResolveAddWinsFromDatoms(datoms)
	assert.Empty(t, members)
}

// =============================================================================
// RGA Vector Resolution Tests
// =============================================================================

func TestResolveRGAFromDatoms_Empty(t *testing.T) {
	values, positions, maxID := ResolveRGAFromDatoms(nil)
	assert.Nil(t, values)
	assert.Nil(t, positions)
	assert.Equal(t, datalog.ElementID{}, maxID)
}

func TestResolveRGAFromDatoms_SingleInsert(t *testing.T) {
	id1 := elemID(100, 1)
	datoms := []datalog.Datom{
		rgaDatom(id1, "first", datalog.OpRGAInsert, datalog.ElementID{}), // After root
	}

	values, positions, maxID := ResolveRGAFromDatoms(datoms)
	assert.Equal(t, []any{"first"}, values)
	assert.Equal(t, []datalog.ElementID{id1}, positions)
	assert.Equal(t, id1, maxID)
}

func TestResolveRGAFromDatoms_OrderedInserts(t *testing.T) {
	id1 := elemID(100, 1)
	id2 := elemID(200, 1)
	id3 := elemID(300, 1)

	datoms := []datalog.Datom{
		rgaDatom(id1, "first", datalog.OpRGAInsert, datalog.ElementID{}), // After root
		rgaDatom(id2, "second", datalog.OpRGAInsert, id1),                // After first
		rgaDatom(id3, "third", datalog.OpRGAInsert, id2),                 // After second
	}

	values, positions, maxID := ResolveRGAFromDatoms(datoms)
	assert.Equal(t, []any{"first", "second", "third"}, values)
	assert.Equal(t, []datalog.ElementID{id1, id2, id3}, positions)
	assert.Equal(t, id3, maxID)
}

func TestResolveRGAFromDatoms_InsertInMiddle(t *testing.T) {
	id1 := elemID(100, 1)
	id2 := elemID(200, 1)
	id3 := elemID(300, 1)

	datoms := []datalog.Datom{
		rgaDatom(id1, "first", datalog.OpRGAInsert, datalog.ElementID{}),
		rgaDatom(id2, "third", datalog.OpRGAInsert, id1),  // After first
		rgaDatom(id3, "second", datalog.OpRGAInsert, id1), // Also after first
	}

	values, _, _ := ResolveRGAFromDatoms(datoms)
	// RGA: when two elements have same AfterRef, lower ElementID comes first
	// So id2 (200, "third") comes before id3 (300, "second")
	assert.Equal(t, []any{"first", "third", "second"}, values)
}

func TestResolveRGAFromDatoms_WithTombstone(t *testing.T) {
	id1 := elemID(100, 1)
	id2 := elemID(200, 1)
	id3 := elemID(300, 1)
	tombstoneID := elemID(400, 1)

	datoms := []datalog.Datom{
		rgaDatom(id1, "first", datalog.OpRGAInsert, datalog.ElementID{}),
		rgaDatom(id2, "second", datalog.OpRGAInsert, id1),
		rgaDatom(id3, "third", datalog.OpRGAInsert, id2),
		rgaDatom(tombstoneID, "second", datalog.OpRGATombstone, id2), // Delete "second"
	}

	values, positions, _ := ResolveRGAFromDatoms(datoms)
	assert.Equal(t, []any{"first", "third"}, values)
	assert.Equal(t, []datalog.ElementID{id1, id3}, positions)
}

func TestResolveRGAFromDatoms_OutOfOrderInput(t *testing.T) {
	// Datoms arrive in arbitrary order - resolution should still work
	id1 := elemID(100, 1)
	id2 := elemID(200, 1)
	id3 := elemID(300, 1)

	datoms := []datalog.Datom{
		rgaDatom(id3, "third", datalog.OpRGAInsert, id2),                 // Third insert, but first in input
		rgaDatom(id1, "first", datalog.OpRGAInsert, datalog.ElementID{}), // First insert
		rgaDatom(id2, "second", datalog.OpRGAInsert, id1),                // Second insert
	}

	values, _, _ := ResolveRGAFromDatoms(datoms)
	assert.Equal(t, []any{"first", "second", "third"}, values)
}

func TestResolveRGAFromDatoms_IgnoresNonRGAOps(t *testing.T) {
	id1 := elemID(100, 1)
	datoms := []datalog.Datom{
		rgaDatom(id1, "first", datalog.OpRGAInsert, datalog.ElementID{}),
		testDatom(elemID(200, 1), "ignored", datalog.OpNone),     // Not RGA op
		testDatom(elemID(300, 1), "ignored", datalog.OpCRDTAdd),    // Not RGA op
	}

	values, _, _ := ResolveRGAFromDatoms(datoms)
	assert.Equal(t, []any{"first"}, values)
}
