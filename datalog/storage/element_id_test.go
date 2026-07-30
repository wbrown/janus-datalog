package storage

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
)

func TestElementIDOrdering(t *testing.T) {
	tests := []struct {
		name     string
		a, b     ElementID
		wantLess bool
	}{
		{
			name:     "same values are equal",
			a:        ElementID{Lamport: 100, ReplicaID: 5},
			b:        ElementID{Lamport: 100, ReplicaID: 5},
			wantLess: false,
		},
		{
			name:     "lower Lamport is less",
			a:        ElementID{Lamport: 99, ReplicaID: 5},
			b:        ElementID{Lamport: 100, ReplicaID: 5},
			wantLess: true,
		},
		{
			name:     "higher Lamport is greater",
			a:        ElementID{Lamport: 101, ReplicaID: 5},
			b:        ElementID{Lamport: 100, ReplicaID: 5},
			wantLess: false,
		},
		{
			name:     "same Lamport, lower ReplicaID is less",
			a:        ElementID{Lamport: 100, ReplicaID: 4},
			b:        ElementID{Lamport: 100, ReplicaID: 5},
			wantLess: true,
		},
		{
			name:     "same Lamport, higher ReplicaID is greater",
			a:        ElementID{Lamport: 100, ReplicaID: 6},
			b:        ElementID{Lamport: 100, ReplicaID: 5},
			wantLess: false,
		},
		{
			name:     "HEAD is less than any non-zero",
			a:        HEAD,
			b:        ElementID{Lamport: 1, ReplicaID: 0},
			wantLess: true,
		},
		{
			name:     "zero ReplicaID still orders by Lamport",
			a:        ElementID{Lamport: 50, ReplicaID: 0},
			b:        ElementID{Lamport: 100, ReplicaID: 0},
			wantLess: true,
		},
		{
			name:     "max uint64 values",
			a:        ElementID{Lamport: ^uint64(0), ReplicaID: ^uint64(0) - 1},
			b:        ElementID{Lamport: ^uint64(0), ReplicaID: ^uint64(0)},
			wantLess: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.Less(tt.b)
			assert.Equal(t, tt.wantLess, got, "Less() mismatch")

			// Verify antisymmetry: if a < b, then b is not < a
			if tt.wantLess {
				assert.False(t, tt.b.Less(tt.a), "antisymmetry violated")
			}
		})
	}
}

func TestElementIDEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		id   ElementID
	}{
		{"zero/HEAD", HEAD},
		{"small values", ElementID{Lamport: 1, ReplicaID: 1}},
		{"medium values", ElementID{Lamport: 1000, ReplicaID: 500}},
		{"large Lamport", ElementID{Lamport: ^uint64(0), ReplicaID: 0}},
		{"large ReplicaID", ElementID{Lamport: 0, ReplicaID: ^uint64(0)}},
		{"both max", ElementID{Lamport: ^uint64(0), ReplicaID: ^uint64(0)}},
		{"realistic values", ElementID{Lamport: 1706745600000000000, ReplicaID: 12345678901234567}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeElementIDForKey(tt.id)
			require.Len(t, encoded, ElementIDSize, "encoded size should be 16 bytes")

			decoded := DecodeElementID(encoded)
			assert.Equal(t, tt.id, decoded, "round-trip failed")
		})
	}
}

func TestElementIDEncodeInto(t *testing.T) {
	id := ElementID{Lamport: 1234, ReplicaID: 5678}
	buf := make([]byte, 32)

	// Encode at offset 8
	EncodeElementIDInto(id, buf, 8)

	// Decode from offset 8
	decoded := DecodeElementID(buf[8:24])
	assert.Equal(t, id, decoded, "EncodeInto/DecodeElementID round-trip failed")

	// Verify EncodeElementIDForKey and EncodeElementIDInto produce same bytes
	directEncode := EncodeElementIDForKey(id)
	assert.Equal(t, directEncode, buf[8:24], "EncodeElementIDInto should match EncodeElementIDForKey")
}

func TestElementIDSortOrderPreservation(t *testing.T) {
	// This is the CRITICAL test: encoded bytes must sort so that
	// HIGHEST ElementID comes FIRST (descending order for O(1) current value lookup)

	ids := []ElementID{
		{Lamport: 100, ReplicaID: 1}, // Should be first after encoding (highest Lamport)
		{Lamport: 50, ReplicaID: 1},  // Should be second
		{Lamport: 50, ReplicaID: 0},  // Should be third (same Lamport, lower ReplicaID)
		{Lamport: 1, ReplicaID: 100}, // Should be fourth
		HEAD,                         // Should be last (lowest)
	}

	// Encode all
	type encodedID struct {
		original ElementID
		encoded  []byte
	}
	encoded := make([]encodedID, len(ids))
	for i, id := range ids {
		encoded[i] = encodedID{id, EncodeElementIDForKey(id)}
	}

	// Sort by encoded bytes (lexicographic, ascending)
	sort.Slice(encoded, func(i, j int) bool {
		return bytes.Compare(encoded[i].encoded, encoded[j].encoded) < 0
	})

	// After sorting encoded bytes lexicographically ascending,
	// the HIGHEST ElementID should come FIRST (because we use bitwise NOT)
	//
	// With NOT encoding:
	// - Higher Lamport → smaller encoded bytes → sorts first (ascending)
	// - Same Lamport, higher ReplicaID → smaller encoded ReplicaID → sorts first
	// So {50, 1} should come BEFORE {50, 0} in encoded sort order

	expectedOrder := []ElementID{
		{Lamport: 100, ReplicaID: 1}, // highest Lamport
		{Lamport: 50, ReplicaID: 1},  // same Lamport group, higher ReplicaID → smaller encoded → first
		{Lamport: 50, ReplicaID: 0},  // same Lamport group, lower ReplicaID → larger encoded → second
		{Lamport: 1, ReplicaID: 100}, // lower Lamport
		HEAD,                         // zero = lowest, encodes to all 0xFF → largest → last
	}

	for i, enc := range encoded {
		assert.Equal(t, expectedOrder[i], enc.original,
			"position %d: expected %v, got %v", i, expectedOrder[i], enc.original)
	}
}

func TestElementIDSortOrderDescending(t *testing.T) {
	// More explicit test: verify that forward iteration through encoded keys
	// yields ElementIDs in DESCENDING order (highest first)

	// Create a sequence of ElementIDs in ascending order
	ascending := []ElementID{
		{Lamport: 1, ReplicaID: 0},
		{Lamport: 2, ReplicaID: 0},
		{Lamport: 3, ReplicaID: 0},
		{Lamport: 4, ReplicaID: 0},
		{Lamport: 5, ReplicaID: 0},
	}

	// Encode and sort
	type pair struct {
		id      ElementID
		encoded []byte
	}
	pairs := make([]pair, len(ascending))
	for i, id := range ascending {
		pairs[i] = pair{id, EncodeElementIDForKey(id)}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].encoded, pairs[j].encoded) < 0
	})

	// After sorting by encoded bytes, we should get DESCENDING Lamport order
	for i := 0; i < len(pairs)-1; i++ {
		current := pairs[i].id
		next := pairs[i+1].id
		assert.True(t, current.Lamport > next.Lamport ||
			(current.Lamport == next.Lamport && current.ReplicaID > next.ReplicaID),
			"encoded sort should yield descending order: %v should be > %v", current, next)
	}
}

func TestElementIDZero(t *testing.T) {
	assert.True(t, HEAD.IsZero(), "HEAD should be zero")
	assert.True(t, ElementID{}.IsZero(), "default ElementID should be zero")
	assert.False(t, (ElementID{Lamport: 1, ReplicaID: 0}).IsZero(), "non-zero Lamport should not be zero")
	assert.False(t, (ElementID{Lamport: 0, ReplicaID: 1}).IsZero(), "non-zero ReplicaID should not be zero")
}

func TestElementIDString(t *testing.T) {
	tests := []struct {
		id   ElementID
		want string
	}{
		{HEAD, "HEAD"},
		{ElementID{}, "HEAD"},
		{ElementID{Lamport: 1234, ReplicaID: 5678}, "L1234@R5678"},
		{ElementID{Lamport: 0, ReplicaID: 1}, "L0@R1"}, // Non-zero ReplicaID, zero Lamport
		{ElementID{Lamport: 1, ReplicaID: 0}, "L1@R0"}, // Zero ReplicaID, non-zero Lamport
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.id.String())
		})
	}
}

func TestElementIDEqual(t *testing.T) {
	a := ElementID{Lamport: 100, ReplicaID: 5}
	b := ElementID{Lamport: 100, ReplicaID: 5}
	c := ElementID{Lamport: 100, ReplicaID: 6}
	d := ElementID{Lamport: 101, ReplicaID: 5}

	assert.True(t, a.Equal(b), "identical IDs should be equal")
	assert.True(t, b.Equal(a), "equality should be symmetric")
	assert.False(t, a.Equal(c), "different ReplicaID should not be equal")
	assert.False(t, a.Equal(d), "different Lamport should not be equal")
}

func TestElementIDCompare(t *testing.T) {
	a := ElementID{Lamport: 100, ReplicaID: 5}
	b := ElementID{Lamport: 100, ReplicaID: 5}
	c := ElementID{Lamport: 50, ReplicaID: 5}
	d := ElementID{Lamport: 150, ReplicaID: 5}

	assert.Equal(t, 0, a.Compare(b), "equal IDs should compare as 0")
	assert.Equal(t, 1, a.Compare(c), "a > c should return 1")
	assert.Equal(t, -1, a.Compare(d), "a < d should return -1")
}

func TestElementIDMaxMin(t *testing.T) {
	a := ElementID{Lamport: 100, ReplicaID: 5}
	b := ElementID{Lamport: 50, ReplicaID: 10}
	c := ElementID{Lamport: 100, ReplicaID: 3}

	assert.Equal(t, a, Max(a, b), "Max should return higher Lamport")
	assert.Equal(t, b, Min(a, b), "Min should return lower Lamport")
	assert.Equal(t, a, Max(a, c), "Max with same Lamport should return higher ReplicaID")
	assert.Equal(t, c, Min(a, c), "Min with same Lamport should return lower ReplicaID")
}

func TestElementIDEncodeSizeConsistency(t *testing.T) {
	// All ElementIDs should encode to exactly 16 bytes
	testCases := []ElementID{
		HEAD,
		{Lamport: 1, ReplicaID: 1},
		{Lamport: ^uint64(0), ReplicaID: ^uint64(0)},
		{Lamport: 1706745600000000000, ReplicaID: 12345678901234567},
	}

	for _, id := range testCases {
		encoded := EncodeElementIDForKey(id)
		assert.Len(t, encoded, ElementIDSize, "ElementID %v should encode to %d bytes", id, ElementIDSize)
	}
}

func TestElementIDOrderingTransitivity(t *testing.T) {
	// Total order requires transitivity: if a < b and b < c, then a < c
	// Test with various combinations

	testCases := []struct {
		a, b, c ElementID
	}{
		// Strictly increasing Lamport
		{
			ElementID{Lamport: 1, ReplicaID: 0},
			ElementID{Lamport: 2, ReplicaID: 0},
			ElementID{Lamport: 3, ReplicaID: 0},
		},
		// Same Lamport, increasing ReplicaID
		{
			ElementID{Lamport: 100, ReplicaID: 1},
			ElementID{Lamport: 100, ReplicaID: 2},
			ElementID{Lamport: 100, ReplicaID: 3},
		},
		// Mixed: Lamport takes precedence
		{
			ElementID{Lamport: 1, ReplicaID: 100},
			ElementID{Lamport: 2, ReplicaID: 1},
			ElementID{Lamport: 3, ReplicaID: 50},
		},
		// Including HEAD (zero)
		{
			HEAD,
			ElementID{Lamport: 1, ReplicaID: 0},
			ElementID{Lamport: 1, ReplicaID: 1},
		},
		// Near max values
		{
			ElementID{Lamport: ^uint64(0) - 2, ReplicaID: 0},
			ElementID{Lamport: ^uint64(0) - 1, ReplicaID: 0},
			ElementID{Lamport: ^uint64(0), ReplicaID: 0},
		},
	}

	for i, tc := range testCases {
		// Verify a < b
		assert.True(t, tc.a.Less(tc.b), "case %d: a should be less than b", i)
		// Verify b < c
		assert.True(t, tc.b.Less(tc.c), "case %d: b should be less than c", i)
		// Transitivity: a < c
		assert.True(t, tc.a.Less(tc.c), "case %d: transitivity violated - a should be less than c", i)

		// Also verify via Compare
		assert.Equal(t, -1, tc.a.Compare(tc.b), "case %d: Compare(a,b) should be -1", i)
		assert.Equal(t, -1, tc.b.Compare(tc.c), "case %d: Compare(b,c) should be -1", i)
		assert.Equal(t, -1, tc.a.Compare(tc.c), "case %d: Compare(a,c) should be -1", i)
	}
}

func TestMaxElementID(t *testing.T) {
	// Test that MaxElementID returns the highest ElementID from the database
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	require.NoError(t, err)
	defer db.Close()

	// Initially should be zero (empty database)
	maxID, err := db.store.MaxElementID()
	require.NoError(t, err)
	t.Logf("Initial MaxElementID: %v", maxID)
	assert.True(t, maxID.IsZero(), "Empty database should have zero MaxElementID")

	// Add some datoms
	tx := db.NewTransaction()
	entity1 := datalog.NewIdentity("test:entity1")
	attr := datalog.NewKeyword(":test/name")
	tx.Add(entity1, attr, "value1")
	_, err = tx.Commit()
	require.NoError(t, err)

	// MaxElementID should now be greater
	maxID1, err := db.store.MaxElementID()
	require.NoError(t, err)
	t.Logf("After first tx MaxElementID: %v", maxID1)
	assert.Greater(t, maxID1.Lamport, uint64(0), "MaxElementID should be non-zero after write")

	// Add more datoms
	tx2 := db.NewTransaction()
	entity2 := datalog.NewIdentity("test:entity2")
	tx2.Add(entity2, attr, "value2")
	tx2.Add(entity2, attr, "value3")
	_, err = tx2.Commit()
	require.NoError(t, err)

	// MaxElementID should be higher
	maxID2, err := db.store.MaxElementID()
	require.NoError(t, err)
	t.Logf("After second tx MaxElementID: %v", maxID2)
	assert.True(t, maxID1.Less(maxID2), "MaxElementID should increase after more writes")
}

// TestDatabaseClockRestoration lives in persistence_reopen_test.go — its
// write/close/reopen premise needs a durable backend.

func TestElementIDKeyEncodingWithBadgerDB(t *testing.T) {
	// Integration test: verify that encoded keys sort correctly in actual BadgerDB
	tempDir := t.TempDir()
	db, err := NewDatabase(tempDir)
	require.NoError(t, err)
	defer db.Close()

	// Write datoms with specific known Lamport values
	// We'll use different transactions to get different ElementIDs
	attr := datalog.NewKeyword(":test/attr")
	var entities []datalog.Identity

	for i := 0; i < 5; i++ {
		tx := db.NewTransaction()
		entity := datalog.NewIdentity(fmt.Sprintf("test:e%d", i))
		entities = append(entities, entity)
		tx.Add(entity, attr, fmt.Sprintf("value%d", i))
		_, err = tx.Commit()
		require.NoError(t, err)
	}

	// Scan TAEV index (Tx first) - should return entries in descending Tx order
	// because of bitwise NOT encoding
	iter, err := db.store.Scan(ScanBound{Index: TAEV})
	require.NoError(t, err)
	defer iter.Close()

	var foundTx []datalog.ElementID
	for iter.Next() {
		datom, err := iter.Datom()
		require.NoError(t, err)
		foundTx = append(foundTx, datom.Tx)
	}

	// Verify descending order (highest Tx first)
	for i := 0; i < len(foundTx)-1; i++ {
		assert.GreaterOrEqual(t, foundTx[i].Lamport, foundTx[i+1].Lamport,
			"TAEV scan should return entries in descending Tx order (highest first)")
	}

	t.Logf("Found Tx values in scan order: %v", foundTx)
}
