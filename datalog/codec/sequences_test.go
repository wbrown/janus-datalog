package codec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Literal Length Encoding Tests ----

func TestSequenceEncode_LitLen_Small(t *testing.T) {
	// Values 0-15 are direct codes, 0 extra bits
	for val := 0; val <= 15; val++ {
		code, extra, nbits := encodeLitLen(val)
		assert.Equal(t, byte(val), code, "val=%d", val)
		assert.Equal(t, uint32(0), extra, "val=%d", val)
		assert.Equal(t, 0, nbits, "val=%d", val)

		decoded := decodeLitLen(code, extra)
		assert.Equal(t, val, decoded, "val=%d round-trip", val)
	}
}

func TestSequenceEncode_LitLen_Large(t *testing.T) {
	// Test values in the exponential range
	testValues := []int{16, 17, 18, 19, 20, 23, 24, 31, 32, 47, 48, 100, 255, 500, 1000, 5000, 10000, 100000}
	for _, val := range testValues {
		code, extra, nbits := encodeLitLen(val)
		decoded := decodeLitLen(code, extra)
		assert.Equal(t, val, decoded, "litLen=%d code=%d extra=%d nbits=%d", val, code, extra, nbits)
	}
}

func TestSequenceEncode_LitLen_Boundaries(t *testing.T) {
	// Test at each table boundary
	for i := 16; i < len(litLenTable); i++ {
		maxVal := litLenTable[i].maxVal
		// Test the boundary value
		code, extra, nbits := encodeLitLen(maxVal)
		decoded := decodeLitLen(code, extra)
		assert.Equal(t, maxVal, decoded, "boundary litLen=%d", maxVal)
		assert.Equal(t, byte(i), code, "boundary litLen=%d should use code %d", maxVal, i)
		_ = nbits

		// Test one above boundary (should use next code)
		if i+1 < len(litLenTable) {
			code2, extra2, _ := encodeLitLen(maxVal + 1)
			decoded2 := decodeLitLen(code2, extra2)
			assert.Equal(t, maxVal+1, decoded2, "above boundary litLen=%d", maxVal+1)
			assert.Equal(t, byte(i+1), code2, "above boundary should use code %d", i+1)
		}
	}
}

// ---- Match Length Encoding Tests ----

func TestSequenceEncode_MatchLen_Small(t *testing.T) {
	for val := 0; val <= 15; val++ {
		code, extra, nbits := encodeMatchLen(val)
		assert.Equal(t, byte(val), code, "val=%d", val)
		assert.Equal(t, uint32(0), extra, "val=%d", val)
		assert.Equal(t, 0, nbits, "val=%d", val)

		decoded := decodeMatchLen(code, extra)
		assert.Equal(t, val, decoded, "val=%d round-trip", val)
	}
}

func TestSequenceEncode_MatchLen_Large(t *testing.T) {
	testValues := []int{16, 17, 20, 31, 47, 100, 255, 500, 1000, 4000}
	for _, val := range testValues {
		code, extra, nbits := encodeMatchLen(val)
		decoded := decodeMatchLen(code, extra)
		assert.Equal(t, val, decoded, "matchLen=%d code=%d extra=%d nbits=%d", val, code, extra, nbits)
	}
}

func TestSequenceEncode_MatchLen_Boundaries(t *testing.T) {
	for i := 16; i < len(matchLenTable); i++ {
		maxVal := matchLenTable[i].maxVal
		code, extra, _ := encodeMatchLen(maxVal)
		decoded := decodeMatchLen(code, extra)
		assert.Equal(t, maxVal, decoded, "boundary matchLen=%d", maxVal)
		assert.Equal(t, byte(i), code, "boundary matchLen=%d should use code %d", maxVal, i)
	}
}

// ---- Offset Encoding Tests ----

func TestSequenceEncode_Offset_Small(t *testing.T) {
	// Offset 1 → code 0, 0 extra bits
	code, extra, nbits := encodeOffset(1)
	assert.Equal(t, byte(0), code)
	assert.Equal(t, uint32(0), extra)
	assert.Equal(t, 0, nbits)
	assert.Equal(t, 1, decodeOffset(code, extra))
}

func TestSequenceEncode_Offset_PowersOf2(t *testing.T) {
	// Offset 2^N should give code N
	for n := 1; n <= 20; n++ {
		offset := 1 << n
		code, extra, nbits := encodeOffset(offset)
		assert.Equal(t, byte(n), code, "offset=%d", offset)
		assert.Equal(t, uint32(0), extra, "offset=%d (exact power of 2, no extra)", offset)
		assert.Equal(t, n, nbits, "offset=%d", offset)

		decoded := decodeOffset(code, extra)
		assert.Equal(t, offset, decoded, "offset=%d round-trip", offset)
	}
}

func TestSequenceEncode_Offset_Range(t *testing.T) {
	// Test a range of offsets
	offsets := []int{1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 100, 255, 1000, 10000, 100000, 1000000}
	for _, off := range offsets {
		code, extra, nbits := encodeOffset(off)
		decoded := decodeOffset(code, extra)
		assert.Equal(t, off, decoded, "offset=%d code=%d extra=%d nbits=%d", off, code, extra, nbits)
	}
}

// ---- Stream Encoding/Decoding Tests ----

func TestSequenceEncode_Streams_Simple(t *testing.T) {
	seqs := []Sequence{
		{LitLen: 5, Offset: 10, MatchLen: 7},
		{LitLen: 0, Offset: 3, MatchLen: 4},
		{LitLen: 12, Offset: 100, MatchLen: 20},
	}

	es := EncodeSequences(seqs)

	// Should have 3 litLen codes (one per sequence)
	require.Len(t, es.LitLenCodes, 3)
	// Should have 3 match/offset codes (all sequences have matches)
	require.Len(t, es.MatchLenCodes, 3)
	require.Len(t, es.OffsetCodes, 3)

	// Decode back
	decoded := DecodeSequences(es)
	require.Len(t, decoded, 3)

	assert.Equal(t, 5, decoded[0].LitLen)
	assert.Equal(t, 10, decoded[0].Offset)
	assert.Equal(t, 7, decoded[0].MatchLen)

	assert.Equal(t, 0, decoded[1].LitLen)
	assert.Equal(t, 3, decoded[1].Offset)
	assert.Equal(t, 4, decoded[1].MatchLen)

	assert.Equal(t, 12, decoded[2].LitLen)
	assert.Equal(t, 100, decoded[2].Offset)
	assert.Equal(t, 20, decoded[2].MatchLen)
}

func TestSequenceEncode_Streams_WithTrailingLiterals(t *testing.T) {
	seqs := []Sequence{
		{LitLen: 5, Offset: 10, MatchLen: 7},
		{LitLen: 20}, // trailing literals, no match
	}

	es := EncodeSequences(seqs)

	// 2 litLen codes
	require.Len(t, es.LitLenCodes, 2)
	// Only 1 match/offset (trailing literals have no match)
	require.Len(t, es.MatchLenCodes, 1)
	require.Len(t, es.OffsetCodes, 1)

	decoded := DecodeSequences(es)
	require.Len(t, decoded, 2)

	assert.Equal(t, 5, decoded[0].LitLen)
	assert.Equal(t, 10, decoded[0].Offset)
	assert.Equal(t, 7, decoded[0].MatchLen)

	assert.Equal(t, 20, decoded[1].LitLen)
	// Second sequence should have no match since we ran out of match codes
	assert.Equal(t, 0, decoded[1].Offset)
	assert.Equal(t, 0, decoded[1].MatchLen)
}

func TestSequenceEncode_Streams_LargeValues(t *testing.T) {
	seqs := []Sequence{
		{LitLen: 10000, Offset: 500000, MatchLen: 1000},
	}

	es := EncodeSequences(seqs)
	decoded := DecodeSequences(es)
	require.Len(t, decoded, 1)

	assert.Equal(t, 10000, decoded[0].LitLen)
	assert.Equal(t, 500000, decoded[0].Offset)
	assert.Equal(t, 1000, decoded[0].MatchLen)
}

func TestSequenceEncode_Streams_ManySequences(t *testing.T) {
	// Generate many sequences with varying values
	seqs := make([]Sequence, 1000)
	for i := range seqs {
		seqs[i] = Sequence{
			LitLen:   i % 50,
			Offset:   (i%100 + 1) * 10,
			MatchLen: (i%20 + 3),
		}
	}

	es := EncodeSequences(seqs)
	decoded := DecodeSequences(es)
	require.Len(t, decoded, 1000)

	for i, seq := range seqs {
		assert.Equal(t, seq.LitLen, decoded[i].LitLen, "seq %d litLen", i)
		assert.Equal(t, seq.Offset, decoded[i].Offset, "seq %d offset", i)
		assert.Equal(t, seq.MatchLen, decoded[i].MatchLen, "seq %d matchLen", i)
	}
}

func TestSequenceEncode_Streams_Empty(t *testing.T) {
	es := EncodeSequences(nil)
	assert.Empty(t, es.LitLenCodes)
	assert.Empty(t, es.MatchLenCodes)
	assert.Empty(t, es.OffsetCodes)

	decoded := DecodeSequences(es)
	assert.Empty(t, decoded)
}
