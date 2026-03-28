package storage

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
)

func compressedEncoder() *BinaryKeyEncoder {
	return &BinaryKeyEncoder{CompressionThreshold: 256}
}

func rawEncoder() *BinaryKeyEncoder {
	return &BinaryKeyEncoder{} // threshold 0 = disabled
}

func makeCompressDatom(value interface{}) *datalog.Datom {
	return &datalog.Datom{
		E:  datalog.NewIdentity("test-entity"),
		A:  datalog.NewKeyword(":test/content"),
		V:  value,
		Tx: datalog.ElementID{Lamport: 100, ReplicaID: 1},
	}
}

// ---- Key Round-Trip Tests (All Indexes) ----

func TestCompressedKey_RoundTrip_AllIndexes(t *testing.T) {
	enc := compressedEncoder()
	longStr := strings.Repeat("This is compressible content. ", 25) // ~750 bytes

	d := makeCompressDatom(longStr)

	indexes := []IndexType{EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV}
	for _, idx := range indexes {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			key := enc.EncodeKey(idx, d)
			require.NotEmpty(t, key)

			decoded, err := DatomFromKey(idx, key, enc)
			require.NoError(t, err)

			assert.Equal(t, longStr, decoded.V.(string),
				"value round-trip failed for %s", idx)
			assert.Equal(t, d.E.L85(), decoded.E.L85(),
				"entity round-trip failed for %s", idx)
			assert.Equal(t, d.A.String(), decoded.A.String(),
				"attribute round-trip failed for %s", idx)
		})
	}
}

func TestCompressedKey_RoundTrip_Bytes(t *testing.T) {
	enc := compressedEncoder()
	longBytes := []byte(strings.Repeat("compressible bytes data. ", 25))

	d := makeCompressDatom(longBytes)

	for _, idx := range []IndexType{EAVT, AEVT, AVET} {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			key := enc.EncodeKey(idx, d)
			decoded, err := DatomFromKey(idx, key, enc)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(longBytes, decoded.V.([]byte)))
		})
	}
}

func TestCompressedKey_TypeTagPosition(t *testing.T) {
	enc := compressedEncoder()
	longStr := strings.Repeat("compressed string ", 30) // > 256 bytes

	d := makeCompressDatom(longStr)
	key := enc.EncodeKey(EAVT, d)

	// In EAVT: [prefix:1][E:20][A:32][V:...][Tx:16][Op:1]
	// V starts at position 53, first byte is type tag
	require.Greater(t, len(key), 53)
	assert.Equal(t, byte(datalog.TypeCompressedString), key[53],
		"expected TypeCompressedString at position 53")
}

func TestCompressedKey_SmallValueStaysRaw(t *testing.T) {
	enc := compressedEncoder()
	shortStr := "hello" // well below 256 threshold

	d := makeCompressDatom(shortStr)
	key := enc.EncodeKey(EAVT, d)

	// Value type should be TypeString (raw)
	require.Greater(t, len(key), 53)
	assert.Equal(t, byte(datalog.TypeString), key[53],
		"short string should use TypeString, not compressed")

	// Round-trip
	decoded, err := DatomFromKey(EAVT, key, enc)
	require.NoError(t, err)
	assert.Equal(t, shortStr, decoded.V.(string))
}

// ---- AVET Lookup Correctness ----

func TestCompressedKey_AVET_SameValueSamePrefix(t *testing.T) {
	enc := compressedEncoder()
	longStr := strings.Repeat("The AVET lookup test string. ", 20)

	d := makeCompressDatom(longStr)

	// Encode the datom key
	datomKey := enc.EncodeKey(AVET, d)

	// Encode a search prefix for the same value
	vType, vData, _ := datalog.EncodeValue(longStr, 256)
	searchBytes := append([]byte{byte(vType)}, vData...)
	attrBytes := ToStorageDatom(*d).A
	start, end := enc.EncodePrefixRange(AVET, attrBytes[:], searchBytes)

	// The datom key should fall within the search range
	assert.True(t, bytes.Compare(datomKey, start) >= 0,
		"datom key should be >= start prefix")
	assert.True(t, bytes.Compare(datomKey, end) < 0,
		"datom key should be < end prefix")
}

func TestCompressedKey_AVET_DifferentValues(t *testing.T) {
	enc := compressedEncoder()

	str1 := strings.Repeat("value one for AVET test. ", 20)
	str2 := strings.Repeat("value two for AVET test. ", 20)

	d1 := makeCompressDatom(str1)
	d2 := &datalog.Datom{
		E:  datalog.NewIdentity("entity-2"),
		A:  datalog.NewKeyword(":test/content"),
		V:  str2,
		Tx: datalog.ElementID{Lamport: 200, ReplicaID: 1},
	}

	key1 := enc.EncodeKey(AVET, d1)
	key2 := enc.EncodeKey(AVET, d2)

	// Keys should be different (different values)
	assert.False(t, bytes.Equal(key1, key2), "different values should produce different AVET keys")

	// Search for str1 should not match str2
	vType1, vData1, _ := datalog.EncodeValue(str1, 256)
	searchBytes1 := append([]byte{byte(vType1)}, vData1...)
	attrBytes := ToStorageDatom(*d1).A
	start1, end1 := enc.EncodePrefixRange(AVET, attrBytes[:], searchBytes1)

	assert.True(t, bytes.Compare(key1, start1) >= 0 && bytes.Compare(key1, end1) < 0,
		"key1 should be in range of search for str1")
	assert.False(t, bytes.Compare(key2, start1) >= 0 && bytes.Compare(key2, end1) < 0,
		"key2 should NOT be in range of search for str1")
}

// ---- Mixed Tier Tests ----

func TestCompressedKey_MixedTiers(t *testing.T) {
	enc := compressedEncoder()

	shortStr := "short" // Tier 1: raw
	longStr := strings.Repeat("This is a longer compressible string. ", 20) // Tier 2: compressed

	d1 := makeCompressDatom(shortStr)
	d2 := makeCompressDatom(longStr)

	// Both should round-trip through EAVT
	key1 := enc.EncodeKey(EAVT, d1)
	key2 := enc.EncodeKey(EAVT, d2)

	decoded1, err := DatomFromKey(EAVT, key1, enc)
	require.NoError(t, err)
	assert.Equal(t, shortStr, decoded1.V.(string))

	decoded2, err := DatomFromKey(EAVT, key2, enc)
	require.NoError(t, err)
	assert.Equal(t, longStr, decoded2.V.(string))
}

// ---- Backward Compatibility ----

func TestCompressedKey_BackwardCompat(t *testing.T) {
	// Write with compression disabled, read with compression enabled
	rawEnc := rawEncoder()
	compEnc := compressedEncoder()

	longStr := strings.Repeat("backward compat test ", 30)
	d := makeCompressDatom(longStr)

	// Encode without compression
	key := rawEnc.EncodeKey(EAVT, d)
	require.Greater(t, len(key), 53)
	assert.Equal(t, byte(datalog.TypeString), key[53], "raw encoder should use TypeString")

	// Decode with compression-enabled encoder — should still work
	decoded, err := DatomFromKey(EAVT, key, compEnc)
	require.NoError(t, err)
	assert.Equal(t, longStr, decoded.V.(string),
		"compression-enabled decoder should read uncompressed keys")
}

// ---- Compression Disabled ----

func TestCompressedKey_DisabledThreshold(t *testing.T) {
	enc := rawEncoder() // threshold = 0
	longStr := strings.Repeat("should not be compressed ", 30)

	d := makeCompressDatom(longStr)
	key := enc.EncodeKey(EAVT, d)

	// Should use TypeString (raw)
	require.Greater(t, len(key), 53)
	assert.Equal(t, byte(datalog.TypeString), key[53])

	// Compressed key should be larger (has the full string in it)
	compEnc := compressedEncoder()
	compKey := compEnc.EncodeKey(EAVT, d)
	assert.Less(t, len(compKey), len(key),
		"compressed key should be smaller than raw key for long strings")
}

// ---- Determinism ----

func TestCompressedKey_Determinism(t *testing.T) {
	enc := compressedEncoder()
	longStr := strings.Repeat("determinism key test ", 30)
	d := makeCompressDatom(longStr)

	golden := enc.EncodeKey(EAVT, d)
	for i := 0; i < 100; i++ {
		key := enc.EncodeKey(EAVT, d)
		assert.True(t, bytes.Equal(golden, key), "iteration %d: key differs", i)
	}
}
