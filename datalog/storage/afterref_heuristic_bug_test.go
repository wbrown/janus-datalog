package storage

// Tests for the AfterRef heuristic decode bug (BUG_SHARED_DB_DATOM_LOSS).
//
// The bug: DecodeKey's AfterRef detection heuristic reads a byte from
// inside value data and misinterprets it as the Op byte. For reference-
// valued datoms (TypeReference, 21 value bytes), the key is long enough
// to trigger the heuristic. When the 5th byte of the SHA1 hash equals 3
// (OpRGAInsert) or 4 (OpRGATombstone), the decoder enters the wrong path,
// truncates the value, and DatomFromKey returns an error — silently
// dropping the datom.
//
// P(failure) = 2/256 ≈ 0.78% per reference-valued datom.

import (
	"crypto/sha1"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// findHashWithByte4 finds an identity string whose SHA1 hash has a
// specific byte value at position 4 (the position the heuristic reads).
func findHashWithByte4(targetByte byte) (string, [20]byte) {
	for i := 0; ; i++ {
		s := fmt.Sprintf("ref-%d", i)
		h := sha1.Sum([]byte(s))
		if h[4] == targetByte {
			return s, h
		}
	}
}

// TestAfterRefHeuristicBug_Unit demonstrates the decode bug at the
// key encoder level. For a reference-valued datom (21 value bytes),
// the key is 90 bytes (after prefix). The heuristic always reads
// key[73]. Which component that lands on depends on the index layout:
//
//	EATV, AETV, TAEV: key[73] = hash[4]  (value data)
//	EAVT, AEVT, AVET, VAET: key[73] = Tx↓[0]
//
// We craft inputs so key[73] == 3 for EVERY index, proving all 7 are broken.
func TestAfterRefHeuristicBug_Unit(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	// For EATV/AETV/TAEV: key[73] lands in value data (hash[4]).
	// Find a ref whose SHA1 hash has byte[4] == 3.
	triggerStr, _ := findHashWithByte4(3)
	triggerRef := datalog.NewIdentity(triggerStr)
	normalTx := datalog.ElementID{Lamport: 100, ReplicaID: 1}

	// For EAVT/AEVT/AVET/VAET: key[73] lands on Tx↓[0].
	// Tx↓[0] == 3 requires Tx[0] == ^3 == 0xFC.
	// Lamport is big-endian uint64, so Lamport = 0xFC00000000000000.
	txTrigger := datalog.ElementID{Lamport: 0xFC00000000000000, ReplicaID: 1}
	// Use a safe ref (hash[4] != 3 or 4) so only the Tx triggers.
	safeStr, _ := findHashWithByte4(0x20)
	safeRef := datalog.NewIdentity(safeStr)

	entity := datalog.NewIdentity("test-entity")
	attr := datalog.NewKeyword(":test/ref-attr")

	indices := []struct {
		name string
		idx  IndexType
		ref  datalog.Identity // value that puts 3 at key[73]
		tx   datalog.ElementID
	}{
		// Value-at-73 group: trigger via hash[4]==3, normal Tx
		{"EATV", EATV, triggerRef, normalTx},
		{"AETV", AETV, triggerRef, normalTx},
		{"TAEV", TAEV, triggerRef, normalTx},
		// Tx-at-73 group: trigger via Tx↓[0]==3, safe ref
		{"EAVT", EAVT, safeRef, txTrigger},
		{"AEVT", AEVT, safeRef, txTrigger},
		{"AVET", AVET, safeRef, txTrigger},
		{"VAET", VAET, safeRef, txTrigger},
	}

	for _, tc := range indices {
		t.Run(tc.name, func(t *testing.T) {
			datom := &datalog.Datom{
				E: entity, A: attr, V: tc.ref, Tx: tc.tx, Op: datalog.OpNone,
			}
			key := encoder.EncodeKey(tc.idx, datom)

			// Verify key[73] is actually 3 (the trigger byte)
			k := key[1:] // skip prefix
			require.Equal(t, byte(3), k[73],
				"%s: key[73] must be 3 to trigger heuristic", tc.name)

			// Decode must round-trip correctly
			decoded, err := DatomFromKey(tc.idx, key, encoder, nil)
			require.NoError(t, err, "%s: decode must succeed", tc.name)

			decodedRef, ok := decoded.V.(datalog.Identity)
			require.True(t, ok, "%s: value should be Identity, got %T", tc.name, decoded.V)
			assert.True(t, decodedRef.Equal(tc.ref),
				"%s: decoded reference must match original", tc.name)
			assert.Equal(t, byte(datalog.OpNone), byte(decoded.Op),
				"%s: decoded Op must be OpNone, not %d", tc.name, decoded.Op)
		})
	}
}

// TestAfterRefHeuristicBug_KeyLayout prints the exact key layout for a
// reference-valued EATV datom to show where the heuristic reads from.
func TestAfterRefHeuristicBug_KeyLayout(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	triggerStr, triggerHash := findHashWithByte4(3)
	triggerRef := datalog.NewIdentity(triggerStr)

	datom := &datalog.Datom{
		E:  datalog.NewIdentity("test-entity"),
		A:  datalog.NewKeyword(":test/ref-attr"),
		V:  triggerRef,
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1},
		Op: datalog.OpNone,
	}

	key := encoder.EncodeKey(EATV, datom)
	k := key[1:] // skip prefix

	t.Logf("EATV key layout (after prefix):")
	t.Logf("  Total length: %d bytes", len(k))
	t.Logf("  [0:20]   E:         %x", k[0:20])
	t.Logf("  [20:52]  A:         %x", k[20:52])
	t.Logf("  [52:68]  Tx↓:       %x", k[52:68])
	t.Logf("  [68]     ValueType: %d (TypeReference=%d)", k[68], datalog.TypeReference)
	t.Logf("  [69:89]  ValueData: %x", k[69:89])
	t.Logf("  [89]     Op:        %d (actual)", k[89])

	// Show what the heuristic reads
	heuristicPos := len(k) - 16 - 1 // afterRefSize=16, opSize=1
	t.Logf("")
	t.Logf("  Heuristic reads k[%d] = %d (this is hash[%d])", heuristicPos, k[heuristicPos], heuristicPos-69)
	t.Logf("  Actual Op at k[%d] = %d", 89, k[89])
	t.Logf("")
	t.Logf("  hash[4] = %d matches OpRGAInsert(3)? %v", triggerHash[4], triggerHash[4] == 3)

	// Verify the positions
	assert.Equal(t, 90, len(k), "EATV ref key should be 90 bytes (after prefix)")
	assert.Equal(t, 73, heuristicPos, "heuristic should read from position 73")
	assert.Equal(t, byte(3), k[heuristicPos], "heuristic position should contain hash[4]=3")
	assert.Equal(t, byte(0), k[89], "actual Op should be 0 (OpNone)")
}

// TestAfterRefHeuristicBug_Integration tests the full write+read path
// through BadgerDB, demonstrating that datoms are silently lost.
func TestAfterRefHeuristicBug_Integration(t *testing.T) {
	db, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	// Install schema via builder
	refAttr := datalog.NewKeyword(":test/ref")
	s, err := schema.NewBuilder().
		Attribute(":test/ref").Type(schema.TypeRef).Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	// Find identities that trigger and don't trigger the bug
	triggerStr, _ := findHashWithByte4(3)
	safeStr, _ := findHashWithByte4(0x20)

	triggerRef := datalog.NewIdentity(triggerStr)
	safeRef := datalog.NewIdentity(safeStr)

	// Write both datoms
	entity := datalog.NewIdentity("test-entity")
	writeTx := db.NewTransaction()
	writeTx.Add(entity, refAttr, triggerRef)
	_, err = writeTx.Commit()
	require.NoError(t, err)

	entity2 := datalog.NewIdentity("test-entity-2")
	writeTx2 := db.NewTransaction()
	writeTx2.Add(entity2, refAttr, safeRef)
	_, err = writeTx2.Commit()
	require.NoError(t, err)

	// Read back via EATV scan (used by LookupAttribute / ResolveLWW)
	store := db.Store()
	encoder := store.encoder

	// Build EATV prefix for entity + refAttr
	eBytes := entity.Hash()
	aStr := refAttr.String()
	var aBytes [32]byte
	copy(aBytes[:], []byte(aStr))

	prefix := encoder.EncodePrefix(EATV, eBytes[:], aBytes[:])
	end := incrementLastByte(prefix)
	it, err := store.ScanKeysOnly(EATV, prefix, end)
	require.NoError(t, err)
	defer it.Close()

	found := false
	for it.Next() {
		d, derr := it.Datom()
		if derr != nil {
			t.Logf("BUG: decode error during EATV scan: %v", derr)
			continue
		}
		if d.E.Equal(entity) {
			found = true
		}
	}

	if !found {
		t.Error("BUG CONFIRMED: trigger reference datom lost in EATV scan")
	}

	// Same for the safe entity — should always be found
	eBytes2 := entity2.Hash()
	prefix2 := encoder.EncodePrefix(EATV, eBytes2[:], aBytes[:])
	end2 := incrementLastByte(prefix2)
	it2, err := store.ScanKeysOnly(EATV, prefix2, end2)
	require.NoError(t, err)
	defer it2.Close()

	found2 := false
	for it2.Next() {
		d, derr := it2.Datom()
		if derr != nil {
			t.Logf("decode error during safe EATV scan: %v", derr)
			continue
		}
		if d.E.Equal(entity2) {
			found2 = true
		}
	}
	require.True(t, found2, "safe reference datom should always be found")
}

// TestAfterRefHeuristicBug_Statistical writes many random reference-valued
// datoms and measures the decode failure rate, confirming the ~0.78% prediction.
func TestAfterRefHeuristicBug_Statistical(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("stat-entity")
	attr := datalog.NewKeyword(":test/ref-attr")
	tx := datalog.ElementID{Lamport: 100, ReplicaID: 1}

	const N = 10000
	failures := 0
	failedBytes := make(map[byte]int)

	for i := 0; i < N; i++ {
		ref := datalog.NewIdentity(fmt.Sprintf("ref-value-%d", i))
		datom := &datalog.Datom{
			E: entity, A: attr, V: ref, Tx: tx, Op: datalog.OpNone,
		}

		// Test EATV (the primary CRDT index used for reads)
		key := encoder.EncodeKey(EATV, datom)
		_, err := DatomFromKey(EATV, key, encoder, nil)
		if err != nil {
			failures++
			k := key[1:] // skip prefix
			heuristicPos := len(k) - 16 - 1
			if heuristicPos >= 0 && heuristicPos < len(k) {
				failedBytes[k[heuristicPos]]++
			}
		}
	}

	rate := float64(failures) / float64(N) * 100
	t.Logf("Decode failures: %d/%d = %.2f%% (expected ~0.78%%)", failures, N, rate)
	t.Logf("Failed byte values: %v", failedBytes)

	// The observed rate should be in the ballpark of 0.78%
	// With N=10000, expected ~78 failures. Allow wide margin for this test.
	assert.Equal(t, 0, failures,
		"all reference-valued datoms must decode correctly; got %d/%d failures (%.2f%%)", failures, N, rate)
}

// TestAfterRefHeuristicBug_NonRefValues verifies that non-reference value
// types are NOT affected (keyword, string, int64 all have different sizes
// or byte patterns that don't trigger the heuristic).
func TestAfterRefHeuristicBug_NonRefValues(t *testing.T) {
	encoder := &BinaryKeyEncoder{}

	entity := datalog.NewIdentity("test-entity")
	tx := datalog.ElementID{Lamport: 100, ReplicaID: 1}

	testCases := []struct {
		name string
		attr datalog.Keyword
		val  any
	}{
		// Keyword values: ASCII bytes > 0x20, never hit 3 or 4
		{"keyword-short", datalog.NewKeyword(":test/kw"), datalog.NewKeyword(":entity.type/crawl")},
		// String values: ASCII bytes > 0x20
		{"string-short", datalog.NewKeyword(":test/str"), "hello"},
		// Int64: 9 bytes (1 type + 8 data), total key < 85, heuristic won't trigger
		{"int64", datalog.NewKeyword(":test/int"), int64(42)},
		// Bool: 2 bytes (1 type + 1 data), total key < 85
		{"bool", datalog.NewKeyword(":test/bool"), true},
		// Float64: 9 bytes (1 type + 8 data), total key < 85
		{"float64", datalog.NewKeyword(":test/float"), float64(3.14)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			datom := &datalog.Datom{
				E: entity, A: tc.attr, V: tc.val, Tx: tx, Op: datalog.OpNone,
			}
			for _, idx := range Indices {
				key := encoder.EncodeKey(idx, datom)
				_, err := DatomFromKey(idx, key, encoder, nil)
				assert.NoError(t, err, "non-reference value should always decode in %v", idx)
			}
		})
	}
}

// TestAfterRefHeuristicBug_FullDBRoundTrip writes N entities each with 3
// reference fields, reads them back, and counts missing fields.
func TestAfterRefHeuristicBug_FullDBRoundTrip(t *testing.T) {
	db, err := NewDatabase(t.TempDir())
	require.NoError(t, err)
	defer db.Close()

	// Install schema via builder
	s, err := schema.NewBuilder().
		Attribute(":test/ref-a").Type(schema.TypeRef).Add().
		Attribute(":test/ref-b").Type(schema.TypeRef).Add().
		Attribute(":test/ref-c").Type(schema.TypeRef).Add().
		Attribute(":test/type").Type(schema.TypeKeyword).Add().
		Build()
	require.NoError(t, err)
	db.SetSchema(s)

	refAttrs := []datalog.Keyword{
		datalog.NewKeyword(":test/ref-a"),
		datalog.NewKeyword(":test/ref-b"),
		datalog.NewKeyword(":test/ref-c"),
	}
	kwAttr := datalog.NewKeyword(":test/type")

	const N = 500
	type record struct {
		entity datalog.Identity
		refs   [3]datalog.Identity
	}
	records := make([]record, N)

	// Write N entities, each with 3 ref fields + 1 keyword field
	for i := 0; i < N; i++ {
		entityID := datalog.NewIdentity(fmt.Sprintf("entity-%d", i))
		refA := datalog.NewIdentity(fmt.Sprintf("ref-a-%d", i))
		refB := datalog.NewIdentity(fmt.Sprintf("ref-b-%d", i))
		refC := datalog.NewIdentity(fmt.Sprintf("ref-c-%d", i))
		records[i] = record{entity: entityID, refs: [3]datalog.Identity{refA, refB, refC}}

		writeTx := db.NewTransaction()
		writeTx.Add(entityID, kwAttr, datalog.NewKeyword(":entity.type/test"))
		writeTx.Add(entityID, refAttrs[0], refA)
		writeTx.Add(entityID, refAttrs[1], refB)
		writeTx.Add(entityID, refAttrs[2], refC)
		_, err = writeTx.Commit()
		require.NoError(t, err)
	}

	// Read back using LookupAttribute (same path as PullInto)
	matcher := NewBadgerMatcher(db.Store())
	matcher.SetSchema(s)

	fieldLosses := 0
	entityLosses := 0

	for i := 0; i < N; i++ {
		rec := records[i]
		anyMissing := false

		for j, attr := range refAttrs {
			val, found := requireAttributeLookup(t, matcher, rec.entity, attr)
			if !found || val == nil {
				fieldLosses++
				anyMissing = true
				t.Logf("LOSS: entity=%d attr=%s ref hash[4]=%d",
					i, attr.String(), rec.refs[j].Hash()[4])
			}
		}

		// Keyword field should never be lost
		kwVal, kwFound := requireAttributeLookup(t, matcher, rec.entity, kwAttr)
		require.True(t, kwFound, "keyword field should always be found (entity %d)", i)
		require.NotNil(t, kwVal, "keyword value should not be nil (entity %d)", i)

		if anyMissing {
			entityLosses++
		}
	}

	rate := float64(entityLosses) / float64(N) * 100
	t.Logf("Entity loss: %d/%d = %.1f%% (expected ~2.3%%)", entityLosses, N, rate)
	t.Logf("Field loss: %d/%d total ref fields", fieldLosses, N*3)

	assert.Equal(t, 0, fieldLosses,
		"all reference fields must be readable; got %d/%d losses", fieldLosses, N*3)
}
