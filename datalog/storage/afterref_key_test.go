package storage

// AfterRef Key Encoding Tests
//
// These tests verify that AfterRef is correctly encoded/decoded in index keys
// when Op is OpRGAInsert or OpRGATombstone.
//
// Key format: [...][V][Tx↓][Op][AfterRef?]
// AfterRef? = 16 bytes present only when Op.HasAfterRef() is true

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

// TestAfterRefKeyEncodingEAVT verifies AfterRef is correctly encoded/decoded in EAVT index
func TestAfterRefKeyEncodingEAVT(t *testing.T) {
	encoder := NewKeyEncoder(BinaryStrategy)

	elemID := datalog.ElementID{Lamport: 1000, ReplicaID: 100}
	afterRef := datalog.ElementID{Lamport: 500, ReplicaID: 100}

	datom := &datalog.Datom{
		E:        datalog.NewIdentity("test-entity"),
		A:        datalog.NewKeyword(":character/skills"),
		V:        "stealth",
		Tx:       elemID,
		Op:       datalog.OpRGAInsert,
		AfterRef: afterRef,
	}

	// Encode
	key := encoder.EncodeKey(EAVT, datom)

	// Verify key is longer than without AfterRef (16 extra bytes)
	datomNoAfterRef := &datalog.Datom{
		E:  datom.E,
		A:  datom.A,
		V:  datom.V,
		Tx: datom.Tx,
		Op: datalog.OpNone, // No AfterRef
	}
	keyNoAfterRef := encoder.EncodeKey(EAVT, datomNoAfterRef)
	assert.Equal(t, len(keyNoAfterRef)+16, len(key), "key with AfterRef should be 16 bytes longer")

	// Decode
	e, a, v, tx, op, decodedAfterRef, err := encoder.DecodeKey(EAVT, key)
	require.NoError(t, err)

	// Verify all components
	assert.Equal(t, datom.E.Hash(), e, "entity should match")
	assert.Equal(t, []byte(datom.A.String()), a[:len(datom.A.String())], "attribute should match")
	assert.Equal(t, byte(datalog.TypeString), v[0], "value type should be string")
	assert.Equal(t, "stealth", string(v[1:]), "value should match")
	assert.Equal(t, byte(datalog.OpRGAInsert), op, "op should be OpRGAInsert")

	// Verify Tx decodes correctly (with bitwise NOT reversal)
	decodedTx := Tx(tx).ToElementID()
	assert.Equal(t, elemID.Lamport, decodedTx.Lamport, "Tx Lamport should match")
	assert.Equal(t, elemID.ReplicaID, decodedTx.ReplicaID, "Tx ReplicaID should match")

	// Verify AfterRef decodes correctly
	decodedAfterRefElem := Tx(decodedAfterRef).ToElementID()
	assert.Equal(t, afterRef.Lamport, decodedAfterRefElem.Lamport, "AfterRef Lamport should match")
	assert.Equal(t, afterRef.ReplicaID, decodedAfterRefElem.ReplicaID, "AfterRef ReplicaID should match")
}

// TestAfterRefKeyEncodingAllIndices verifies AfterRef works in all 6 indices
func TestAfterRefKeyEncodingAllIndices(t *testing.T) {
	encoder := NewKeyEncoder(BinaryStrategy)

	elemID := datalog.ElementID{Lamport: 2000, ReplicaID: 200}
	afterRef := datalog.ElementID{Lamport: 1000, ReplicaID: 200}

	datom := &datalog.Datom{
		E:        datalog.NewIdentity("multi-index-entity"),
		A:        datalog.NewKeyword(":test/vector"),
		V:        "value",
		Tx:       elemID,
		Op:       datalog.OpRGAInsert,
		AfterRef: afterRef,
	}

	indices := []IndexType{EAVT, EATV, AEVT, AVET, VAET, TAEV}

	indexNames := []string{"EAVT", "EATV", "AEVT", "AVET", "VAET", "TAEV"}

	for i, idx := range indices {
		t.Run(indexNames[i], func(t *testing.T) {
			// Encode
			key := encoder.EncodeKey(idx, datom)

			// Decode
			_, _, _, tx, op, decodedAfterRef, err := encoder.DecodeKey(idx, key)
			require.NoError(t, err, "DecodeKey should succeed for %v", idx)

			// Verify Op
			assert.Equal(t, byte(datalog.OpRGAInsert), op, "op should be OpRGAInsert for %v", idx)

			// Verify Tx
			decodedTx := Tx(tx).ToElementID()
			assert.Equal(t, elemID.Lamport, decodedTx.Lamport, "Tx Lamport should match for %v", idx)

			// Verify AfterRef
			decodedAfterRefElem := Tx(decodedAfterRef).ToElementID()
			assert.Equal(t, afterRef.Lamport, decodedAfterRefElem.Lamport, "AfterRef Lamport should match for %v", idx)
			assert.Equal(t, afterRef.ReplicaID, decodedAfterRefElem.ReplicaID, "AfterRef ReplicaID should match for %v", idx)
		})
	}
}

// TestOpRGAInsertKeyEncoding verifies OpRGAInsert triggers AfterRef encoding
func TestOpRGAInsertKeyEncoding(t *testing.T) {
	encoder := NewKeyEncoder(BinaryStrategy)

	elemID := datalog.ElementID{Lamport: 100, ReplicaID: 1}
	afterRef := datalog.ElementID{Lamport: 50, ReplicaID: 1}

	// With OpRGAInsert - should have AfterRef
	datomWithAfterRef := &datalog.Datom{
		E:        datalog.NewIdentity("entity"),
		A:        datalog.NewKeyword(":attr"),
		V:        "val",
		Tx:       elemID,
		Op:       datalog.OpRGAInsert,
		AfterRef: afterRef,
	}

	// With OpCRDTAdd - should NOT have AfterRef
	datomWithoutAfterRef := &datalog.Datom{
		E:        datalog.NewIdentity("entity"),
		A:        datalog.NewKeyword(":attr"),
		V:        "val",
		Tx:       elemID,
		Op:       datalog.OpCRDTAdd,
		AfterRef: afterRef, // Set but should be ignored
	}

	keyWith := encoder.EncodeKey(EAVT, datomWithAfterRef)
	keyWithout := encoder.EncodeKey(EAVT, datomWithoutAfterRef)

	// Key with AfterRef should be 16 bytes longer
	assert.Equal(t, len(keyWithout)+16, len(keyWith),
		"OpRGAInsert key should be 16 bytes longer than OpCRDTAdd key")

	// Decode and verify AfterRef is present for OpRGAInsert
	_, _, _, _, op, decodedAfterRef, err := encoder.DecodeKey(EAVT, keyWith)
	require.NoError(t, err)
	assert.Equal(t, byte(datalog.OpRGAInsert), op)

	decodedAfterRefElem := Tx(decodedAfterRef).ToElementID()
	assert.Equal(t, afterRef.Lamport, decodedAfterRefElem.Lamport)

	// Decode key without AfterRef - should have zero AfterRef
	_, _, _, _, op2, decodedAfterRef2, err := encoder.DecodeKey(EAVT, keyWithout)
	require.NoError(t, err)
	assert.Equal(t, byte(datalog.OpCRDTAdd), op2)

	decodedAfterRefElem2 := Tx(decodedAfterRef2).ToElementID()
	assert.Equal(t, uint64(0), decodedAfterRefElem2.Lamport, "AfterRef should be zero for non-RGA ops")
	assert.Equal(t, uint64(0), decodedAfterRefElem2.ReplicaID, "AfterRef should be zero for non-RGA ops")
}

// TestOpRGATombstoneKeyEncoding verifies OpRGATombstone triggers AfterRef encoding
func TestOpRGATombstoneKeyEncoding(t *testing.T) {
	encoder := NewKeyEncoder(BinaryStrategy)

	tombstoneID := datalog.ElementID{Lamport: 200, ReplicaID: 1}
	targetElemID := datalog.ElementID{Lamport: 100, ReplicaID: 1} // Element being deleted

	datom := &datalog.Datom{
		E:        datalog.NewIdentity("entity"),
		A:        datalog.NewKeyword(":attr"),
		V:        "tombstoned-value",
		Tx:       tombstoneID,
		Op:       datalog.OpRGATombstone,
		AfterRef: targetElemID, // ID of element being tombstoned
	}

	key := encoder.EncodeKey(EAVT, datom)

	// Decode
	_, _, _, tx, op, decodedAfterRef, err := encoder.DecodeKey(EAVT, key)
	require.NoError(t, err)

	// Verify Op
	assert.Equal(t, byte(datalog.OpRGATombstone), op, "op should be OpRGATombstone")

	// Verify Tx (tombstone ID)
	decodedTx := Tx(tx).ToElementID()
	assert.Equal(t, tombstoneID.Lamport, decodedTx.Lamport)

	// Verify AfterRef (target element ID)
	decodedAfterRefElem := Tx(decodedAfterRef).ToElementID()
	assert.Equal(t, targetElemID.Lamport, decodedAfterRefElem.Lamport,
		"AfterRef should contain target element ID for tombstone")
	assert.Equal(t, targetElemID.ReplicaID, decodedAfterRefElem.ReplicaID)
}

// TestAfterRefHEADSentinel verifies HEAD (zero ElementID) is correctly encoded
func TestAfterRefHEADSentinel(t *testing.T) {
	encoder := NewKeyEncoder(BinaryStrategy)

	// HEAD is zero ElementID - first element in vector
	head := datalog.ElementID{Lamport: 0, ReplicaID: 0}
	elemID := datalog.ElementID{Lamport: 1, ReplicaID: 1}

	datom := &datalog.Datom{
		E:        datalog.NewIdentity("entity"),
		A:        datalog.NewKeyword(":attr"),
		V:        "first-element",
		Tx:       elemID,
		Op:       datalog.OpRGAInsert,
		AfterRef: head, // Inserted after HEAD (beginning)
	}

	key := encoder.EncodeKey(EAVT, datom)

	_, _, _, _, _, decodedAfterRef, err := encoder.DecodeKey(EAVT, key)
	require.NoError(t, err)

	decodedAfterRefElem := Tx(decodedAfterRef).ToElementID()
	assert.Equal(t, uint64(0), decodedAfterRefElem.Lamport, "HEAD Lamport should be 0")
	assert.Equal(t, uint64(0), decodedAfterRefElem.ReplicaID, "HEAD ReplicaID should be 0")
}

// TestAfterRefSortOrder verifies keys with same prefix sort by Tx then AfterRef
func TestAfterRefSortOrder(t *testing.T) {
	encoder := NewKeyEncoder(BinaryStrategy)

	entity := datalog.NewIdentity("entity")
	attr := datalog.NewKeyword(":attr")

	// Same E, A, V, different Tx and AfterRef
	datom1 := &datalog.Datom{
		E:        entity,
		A:        attr,
		V:        "value",
		Tx:       datalog.ElementID{Lamport: 100, ReplicaID: 1},
		Op:       datalog.OpRGAInsert,
		AfterRef: datalog.ElementID{Lamport: 50, ReplicaID: 1},
	}

	datom2 := &datalog.Datom{
		E:        entity,
		A:        attr,
		V:        "value",
		Tx:       datalog.ElementID{Lamport: 200, ReplicaID: 1}, // Higher Tx
		Op:       datalog.OpRGAInsert,
		AfterRef: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}

	key1 := encoder.EncodeKey(EAVT, datom1)
	key2 := encoder.EncodeKey(EAVT, datom2)

	// With bitwise NOT encoding, higher Tx sorts FIRST (lower byte value)
	// So key2 (Tx=200) should sort BEFORE key1 (Tx=100)
	assert.True(t, bytes.Compare(key2, key1) < 0,
		"higher Tx should sort first due to bitwise NOT encoding")
}

// TestAfterRefWithDifferentValueTypes verifies AfterRef works with various value types
func TestAfterRefWithDifferentValueTypes(t *testing.T) {
	encoder := NewKeyEncoder(BinaryStrategy)

	testCases := []struct {
		name  string
		value any
	}{
		{"string", "test-string"},
		{"int64", int64(42)},
		{"float64", float64(3.14)},
		{"bool", true},
		{"reference", datalog.Reference(datalog.NewIdentity("ref-target"))},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			elemID := datalog.ElementID{Lamport: 100, ReplicaID: 1}
			afterRef := datalog.ElementID{Lamport: 50, ReplicaID: 1}

			datom := &datalog.Datom{
				E:        datalog.NewIdentity("entity"),
				A:        datalog.NewKeyword(":attr"),
				V:        tc.value,
				Tx:       elemID,
				Op:       datalog.OpRGAInsert,
				AfterRef: afterRef,
			}

			key := encoder.EncodeKey(EAVT, datom)

			_, _, _, _, op, decodedAfterRef, err := encoder.DecodeKey(EAVT, key)
			require.NoError(t, err, "should decode key for value type %s", tc.name)

			assert.Equal(t, byte(datalog.OpRGAInsert), op)

			decodedAfterRefElem := Tx(decodedAfterRef).ToElementID()
			assert.Equal(t, afterRef.Lamport, decodedAfterRefElem.Lamport,
				"AfterRef should be preserved for value type %s", tc.name)
		})
	}
}

// TestHasAfterRefMethod verifies the HasAfterRef() method works correctly
func TestHasAfterRefMethod(t *testing.T) {
	testCases := []struct {
		name     string
		op       datalog.CRDTOp
		expected bool
	}{
		{"OpNone", datalog.OpNone, false},
		{"OpCRDTAdd", datalog.OpCRDTAdd, false},
		{"OpCRDTRemove", datalog.OpCRDTRemove, false},
		{"OpRGAInsert", datalog.OpRGAInsert, true},
		{"OpRGATombstone", datalog.OpRGATombstone, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.op.HasAfterRef(),
				"HasAfterRef() should return %v for %s", tc.expected, tc.name)
		})
	}
}
