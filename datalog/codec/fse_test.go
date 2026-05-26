package codec

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Bit Reader/Writer Tests ----

func TestBitWriter_SingleBits(t *testing.T) {
	w := &bitWriter{}
	// Write bits: 1,0,1,1,0,1,0,0 (LSB first → byte 0x2D = 00101101)
	// Actually LSB first: bit 0=1, bit 1=0, bit 2=1, bit 3=1, bit 4=0, bit 5=1, bit 6=0, bit 7=0
	// = 0b00101101 = 0x2D
	for _, b := range []uint32{1, 0, 1, 1, 0, 1, 0, 0} {
		w.writeBits(b, 1)
	}
	out := w.flush()
	require.Len(t, out, 1)
	assert.Equal(t, byte(0x2D), out[0])

	// Read back
	r := newBitReader(out)
	for _, expected := range []uint32{1, 0, 1, 1, 0, 1, 0, 0} {
		got := r.readBits(1)
		assert.Equal(t, expected, got)
	}
}

func TestBitWriter_MultiBit(t *testing.T) {
	w := &bitWriter{}
	// Write 3 bits (value 5 = 101), then 5 bits (value 19 = 10011)
	w.writeBits(5, 3)  // bits 0-2: 101
	w.writeBits(19, 5) // bits 3-7: 10011
	// Combined: 10011_101 = 0x9D? No, LSB first:
	// bits 0-2: 101 (value 5)
	// bits 3-7: 10011 (value 19)
	// byte = 10011_101 → reading right to left: bit7..bit0 = 1,0,0,1,1,1,0,1 = 0x9D
	out := w.flush()
	require.Len(t, out, 1)
	assert.Equal(t, byte(0x9D), out[0])

	// Read back
	r := newBitReader(out)
	assert.Equal(t, uint32(5), r.readBits(3))
	assert.Equal(t, uint32(19), r.readBits(5))
}

func TestBitWriter_CrossByte(t *testing.T) {
	w := &bitWriter{}
	// Write 6 bits, then 6 bits = 12 bits total = straddles 2 bytes
	w.writeBits(0x3F, 6) // 111111
	w.writeBits(0x15, 6) // 010101
	out := w.flush()
	require.Len(t, out, 2)

	// Read back
	r := newBitReader(out)
	assert.Equal(t, uint32(0x3F), r.readBits(6))
	assert.Equal(t, uint32(0x15), r.readBits(6))
}

func TestBitWriter_LargeValues(t *testing.T) {
	w := &bitWriter{}
	w.writeBits(0xABCD, 16)
	w.writeBits(0x1234, 16)
	out := w.flush()
	require.Len(t, out, 4)

	r := newBitReader(out)
	assert.Equal(t, uint32(0xABCD), r.readBits(16))
	assert.Equal(t, uint32(0x1234), r.readBits(16))
}

func TestBitWriter_WidthRange(t *testing.T) {
	for nbits := 1; nbits <= 25; nbits++ {
		w := &bitWriter{}
		val := uint32((1 << uint(nbits)) - 1) // all ones
		w.writeBits(val, nbits)
		out := w.flush()

		r := newBitReader(out)
		got := r.readBits(nbits)
		assert.Equal(t, val, got, "nbits=%d", nbits)
	}
}

func TestBitWriter_Empty(t *testing.T) {
	w := &bitWriter{}
	out := w.flush()
	assert.Empty(t, out)
}

func TestBitWriter_FlushPadding(t *testing.T) {
	w := &bitWriter{}
	w.writeBits(5, 3) // 3 bits → flush produces 1 byte with 5 zero-padded bits
	out := w.flush()
	require.Len(t, out, 1)
	// Low 3 bits = 101 (5), high 5 bits = 0
	assert.Equal(t, byte(0x05), out[0])

	r := newBitReader(out)
	assert.Equal(t, uint32(5), r.readBits(3))
}

func TestBitWriter_ManySmallWrites(t *testing.T) {
	// Write 100 values of 3 bits each = 300 bits = 37.5 bytes → 38 bytes
	w := &bitWriter{}
	values := make([]uint32, 100)
	rng := rand.New(rand.NewSource(42))
	for i := range values {
		values[i] = uint32(rng.Intn(8)) // 0-7 in 3 bits
		w.writeBits(values[i], 3)
	}
	out := w.flush()
	require.True(t, len(out) >= 37 && len(out) <= 38)

	r := newBitReader(out)
	for i, expected := range values {
		got := r.readBits(3)
		assert.Equal(t, expected, got, "index %d", i)
	}
}

// ---- Count Normalization Tests ----

func TestNormalize_Uniform(t *testing.T) {
	counts := make([]int, 4)
	for i := range counts {
		counts[i] = 10
	}
	norm, maxSym, err := normalizeCount(counts, 6) // tableSize=64
	require.NoError(t, err)
	assert.Equal(t, 3, maxSym)

	sum := 0
	for _, c := range norm {
		sum += int(c)
	}
	assert.Equal(t, 64, sum)
	// Each should be 16 (64/4)
	for i := 0; i < 4; i++ {
		assert.Equal(t, int16(16), norm[i])
	}
}

func TestNormalize_SingleSymbol(t *testing.T) {
	counts := make([]int, 256)
	counts[42] = 100
	norm, maxSym, err := normalizeCount(counts, 8) // tableSize=256
	require.NoError(t, err)
	assert.Equal(t, 42, maxSym)
	assert.Equal(t, int16(256), norm[42])
}

func TestNormalize_HighlySkewed(t *testing.T) {
	counts := []int{990, 5, 3, 1, 1}
	norm, maxSym, err := normalizeCount(counts, 8) // tableSize=256
	require.NoError(t, err)
	assert.Equal(t, 4, maxSym)

	sum := 0
	for _, c := range norm {
		sum += int(c)
	}
	assert.Equal(t, 256, sum)

	// Every non-zero symbol must have count >= 1
	for i := 0; i < 5; i++ {
		assert.GreaterOrEqual(t, norm[i], int16(1), "symbol %d", i)
	}
	// Dominant symbol should have most of the count
	assert.GreaterOrEqual(t, norm[0], int16(240))
}

func TestNormalize_AllByteValues(t *testing.T) {
	counts := make([]int, 256)
	for i := range counts {
		counts[i] = i + 1 // triangular distribution
	}
	norm, maxSym, err := normalizeCount(counts, 12) // tableSize=4096
	require.NoError(t, err)
	assert.Equal(t, 255, maxSym)

	sum := 0
	for _, c := range norm {
		sum += int(c)
	}
	assert.Equal(t, 4096, sum)

	// Every symbol must have count >= 1
	for i := 0; i < 256; i++ {
		assert.GreaterOrEqual(t, norm[i], int16(1), "symbol %d", i)
	}
	// Higher symbols should generally have higher counts
	assert.Greater(t, norm[255], norm[0])
}

func TestNormalize_TwoSymbols(t *testing.T) {
	counts := []int{75, 25}
	norm, _, err := normalizeCount(counts, 8) // tableSize=256
	require.NoError(t, err)

	sum := int(norm[0]) + int(norm[1])
	assert.Equal(t, 256, sum)
	// Ratio should be roughly 3:1
	assert.InDelta(t, 192, int(norm[0]), 5)
	assert.InDelta(t, 64, int(norm[1]), 5)
}

func TestNormalize_ManyZeros(t *testing.T) {
	counts := make([]int, 256)
	// Only 6 non-zero
	counts[10] = 100
	counts[50] = 80
	counts[100] = 60
	counts[150] = 40
	counts[200] = 15
	counts[250] = 5

	norm, maxSym, err := normalizeCount(counts, 8) // tableSize=256
	require.NoError(t, err)
	assert.Equal(t, 250, maxSym)

	sum := 0
	nonZeroCount := 0
	for _, c := range norm {
		sum += int(c)
		if c > 0 {
			nonZeroCount++
		}
	}
	assert.Equal(t, 256, sum)
	assert.Equal(t, 6, nonZeroCount)
}

func TestNormalize_MinimumGuarantee(t *testing.T) {
	counts := []int{10000, 1, 1, 1, 1, 1}
	norm, _, err := normalizeCount(counts, 6) // tableSize=64
	require.NoError(t, err)

	sum := 0
	for _, c := range norm {
		sum += int(c)
	}
	assert.Equal(t, 64, sum)

	// Each rare symbol must have >= 1
	for i := 1; i < 6; i++ {
		assert.GreaterOrEqual(t, norm[i], int16(1), "symbol %d", i)
	}
	// Dominant symbol gets the rest
	assert.Equal(t, int16(64-5), norm[0])
}

func TestNormalize_NoSymbols(t *testing.T) {
	counts := make([]int, 10)
	_, _, err := normalizeCount(counts, 8)
	assert.Error(t, err)
}

// ---- State Table Construction Tests ----

func TestTableBuild_Simple(t *testing.T) {
	// 2 symbols: counts [24, 8], tableLog=5, tableSize=32
	normCounts := []int16{24, 8}
	tbl, err := buildFSETable(normCounts, 1, 5)
	require.NoError(t, err)

	// Verify decode table has correct symbol distribution
	symbolCounts := make(map[byte]int)
	for _, entry := range tbl.decodeTable {
		symbolCounts[entry.symbol]++
	}
	assert.Equal(t, 24, symbolCounts[0])
	assert.Equal(t, 8, symbolCounts[1])
}

func TestTableBuild_Uniform4(t *testing.T) {
	normCounts := []int16{8, 8, 8, 8}
	tbl, err := buildFSETable(normCounts, 3, 5) // tableSize=32
	require.NoError(t, err)

	symbolCounts := make(map[byte]int)
	for _, entry := range tbl.decodeTable {
		symbolCounts[entry.symbol]++
	}
	for s := byte(0); s < 4; s++ {
		assert.Equal(t, 8, symbolCounts[s], "symbol %d", s)
	}
}

func TestTableBuild_Skewed(t *testing.T) {
	normCounts := []int16{252, 2, 1, 1}
	tbl, err := buildFSETable(normCounts, 3, 8) // tableSize=256
	require.NoError(t, err)

	symbolCounts := make(map[byte]int)
	for _, entry := range tbl.decodeTable {
		symbolCounts[entry.symbol]++
	}
	assert.Equal(t, 252, symbolCounts[0])
	assert.Equal(t, 2, symbolCounts[1])
	assert.Equal(t, 1, symbolCounts[2])
	assert.Equal(t, 1, symbolCounts[3])
}

func TestTableBuild_InverseProperty(t *testing.T) {
	// The fundamental correctness property: encode then decode returns the same symbol
	distributions := []struct {
		name   string
		counts []int16
		tLog   int
	}{
		{"uniform4", []int16{8, 8, 8, 8}, 5},
		{"skewed", []int16{28, 2, 1, 1}, 5},
		{"binary", []int16{200, 56}, 8},
	}

	for _, dist := range distributions {
		t.Run(dist.name, func(t *testing.T) {
			maxSym := len(dist.counts) - 1
			tbl, err := buildFSETable(dist.counts, maxSym, dist.tLog)
			require.NoError(t, err)

			tableSize := 1 << dist.tLog

			// For each symbol, test encode→decode cycle
			for s := 0; s <= maxSym; s++ {
				if dist.counts[s] == 0 {
					continue
				}

				// Try encoding from various valid states
				for state := tableSize; state < 2*tableSize; state++ {
					// Encode
					nbBitsOut := int((uint32(state) + tbl.symbolTT[s].deltaNbBits) >> 16)
					bitsValue := uint32(state) & ((1 << uint(nbBitsOut)) - 1)
					reducedState := state >> uint(nbBitsOut)
					newState := int(tbl.stateTable[int32(reducedState)+tbl.symbolTT[s].deltaFindState])

					// newState should be in [tableSize, 2*tableSize)
					assert.GreaterOrEqual(t, newState, tableSize,
						"symbol=%d state=%d", s, state)
					assert.Less(t, newState, 2*tableSize,
						"symbol=%d state=%d", s, state)

					// Decode from newState
					decState := newState - tableSize
					entry := tbl.decodeTable[decState]
					assert.Equal(t, byte(s), entry.symbol,
						"symbol=%d state=%d → decState=%d", s, state, decState)

					// Reconstruct original state from bits
					reconstructed := int(entry.newState) + int(bitsValue)
					// The reconstructed decode state should map back to a valid state
					// (it may not equal the original state exactly due to the many-to-one
					// nature of the encode mapping, but the round-trip through the full
					// compress/decompress pipeline will be exact)
					assert.GreaterOrEqual(t, reconstructed, 0)
					assert.Less(t, reconstructed, tableSize)
				}
			}
		})
	}
}

// ---- Table Serialization Tests ----

func TestTableSerialize_RoundTrip(t *testing.T) {
	normCounts := []int16{128, 64, 32, 16, 8, 4, 2, 2}
	tbl, err := buildFSETable(normCounts, 7, 8)
	require.NoError(t, err)

	serialized := serializeTable(tbl)

	tbl2, consumed, err := deserializeTable(serialized)
	require.NoError(t, err)
	assert.Equal(t, len(serialized), consumed)
	assert.Equal(t, tbl.tableLog, tbl2.tableLog)
	assert.Equal(t, tbl.maxSymbol, tbl2.maxSymbol)

	for i := 0; i <= tbl.maxSymbol; i++ {
		assert.Equal(t, tbl.normCounts[i], tbl2.normCounts[i], "symbol %d", i)
	}

	// Decode tables should be identical
	for i := range tbl.decodeTable {
		assert.Equal(t, tbl.decodeTable[i], tbl2.decodeTable[i], "decode entry %d", i)
	}
}

func TestTableSerialize_Determinism(t *testing.T) {
	normCounts := []int16{128, 64, 32, 16, 8, 4, 2, 2}
	tbl, err := buildFSETable(normCounts, 7, 8)
	require.NoError(t, err)

	golden := serializeTable(tbl)
	for i := 0; i < 100; i++ {
		got := serializeTable(tbl)
		assert.True(t, bytes.Equal(golden, got), "iteration %d", i)
	}
}

func TestTableSerialize_AllDistributions(t *testing.T) {
	distributions := []struct {
		name   string
		counts []int16
		tLog   int
	}{
		{"uniform-small", []int16{8, 8, 8, 8}, 5},
		{"skewed-3", []int16{28, 2, 1, 1}, 5},
		{"binary", []int16{200, 56}, 8},
		{"triangular-8", []int16{1, 2, 3, 4, 5, 6, 7, 4}, 5},
	}

	for _, dist := range distributions {
		t.Run(dist.name, func(t *testing.T) {
			maxSym := len(dist.counts) - 1
			tbl, err := buildFSETable(dist.counts, maxSym, dist.tLog)
			require.NoError(t, err)

			ser := serializeTable(tbl)
			tbl2, _, err := deserializeTable(ser)
			require.NoError(t, err)

			assert.Equal(t, tbl.tableLog, tbl2.tableLog)
			assert.Equal(t, tbl.maxSymbol, tbl2.maxSymbol)
			for i := 0; i <= tbl.maxSymbol; i++ {
				assert.Equal(t, tbl.normCounts[i], tbl2.normCounts[i])
			}
		})
	}
}

func TestTableSerialize_TooShort(t *testing.T) {
	_, _, err := deserializeTable([]byte{8})
	assert.Error(t, err)
}

func TestTableSerialize_InvalidTableLog(t *testing.T) {
	_, _, err := deserializeTable([]byte{20, 1, 0, 0, 0, 4}) // tableLog=20 > max
	assert.Error(t, err)
}

// ---- FSE Compress/Decompress Round-Trip Tests ----

func TestFSE_RoundTrip_Empty(t *testing.T) {
	compressed, err := FSECompress(nil)
	require.NoError(t, err)
	assert.Nil(t, compressed)
}

func TestFSE_RoundTrip_SingleByte(t *testing.T) {
	// Single byte may not compress (overhead > savings)
	compressed, err := FSECompress([]byte{0x42})
	require.NoError(t, err)
	if compressed != nil {
		out, err := FSEDecompress(compressed, 1)
		require.NoError(t, err)
		assert.Equal(t, []byte{0x42}, out)
	}
}

func TestFSE_RoundTrip_AllSame(t *testing.T) {
	input := bytes.Repeat([]byte{0x41}, 1000)
	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed, "1000 identical bytes should compress")
	assert.Less(t, len(compressed), 100, "expected excellent compression for identical bytes")

	out, err := FSEDecompress(compressed, 1000)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, out))
}

func TestFSE_RoundTrip_UniformRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	input := make([]byte, 1000)
	rng.Read(input)

	compressed, err := FSECompress(input)
	require.NoError(t, err)
	// Random data may not compress (that's OK, safety net returns nil)
	if compressed != nil {
		out, err := FSEDecompress(compressed, 1000)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(input, out))
	}
}

func TestFSE_RoundTrip_EnglishText(t *testing.T) {
	input := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 20))
	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed, "English text should compress")
	assert.Less(t, len(compressed), len(input), "compressed should be smaller")

	out, err := FSEDecompress(compressed, len(input))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, out))
}

func TestFSE_RoundTrip_BinaryRamp(t *testing.T) {
	input := make([]byte, 512)
	for i := range input {
		input[i] = byte(i % 256)
	}
	compressed, err := FSECompress(input)
	require.NoError(t, err)
	if compressed != nil {
		out, err := FSEDecompress(compressed, len(input))
		require.NoError(t, err)
		assert.True(t, bytes.Equal(input, out))
	}
}

func TestFSE_RoundTrip_SizeSweep(t *testing.T) {
	sizes := []int{1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 100, 255, 256, 512, 1000, 4096, 10000}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(size)))
			input := make([]byte, size)
			rng.Read(input)

			compressed, err := FSECompress(input)
			require.NoError(t, err)
			if compressed != nil {
				out, err := FSEDecompress(compressed, size)
				require.NoError(t, err)
				assert.True(t, bytes.Equal(input, out), "size=%d", size)
			}
		})
	}
}

func TestFSE_RoundTrip_HighlySkewed(t *testing.T) {
	// Build input: 950 'a', 30 'b', 15 'c', 5 'd' — shuffled deterministically
	var input []byte
	input = append(input, bytes.Repeat([]byte{'a'}, 950)...)
	input = append(input, bytes.Repeat([]byte{'b'}, 30)...)
	input = append(input, bytes.Repeat([]byte{'c'}, 15)...)
	input = append(input, bytes.Repeat([]byte{'d'}, 5)...)

	rng := rand.New(rand.NewSource(99))
	rng.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})

	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed, "skewed data should compress")
	assert.Less(t, len(compressed), len(input)/2, "expected significant compression")

	out, err := FSEDecompress(compressed, len(input))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, out))
}

func TestFSE_RoundTrip_TwoAlternating(t *testing.T) {
	input := bytes.Repeat([]byte{'a', 'b'}, 500) // 1000 bytes
	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	out, err := FSEDecompress(compressed, len(input))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, out))
}

func TestFSE_RoundTrip_AllByteValues(t *testing.T) {
	// Every byte value appears at least once
	input := make([]byte, 512)
	for i := range input {
		input[i] = byte(i % 256)
	}
	// Add extra copies of some values for non-uniform distribution
	input = append(input, bytes.Repeat([]byte{0}, 100)...)
	input = append(input, bytes.Repeat([]byte{255}, 50)...)

	compressed, err := FSECompress(input)
	require.NoError(t, err)
	if compressed != nil {
		out, err := FSEDecompress(compressed, len(input))
		require.NoError(t, err)
		assert.True(t, bytes.Equal(input, out))
	}
}

func TestFSE_RoundTrip_RepetitivePhrases(t *testing.T) {
	// Simulates structured data with repeated patterns
	input := []byte(strings.Repeat("[#identity \"abc\" :test/name \"value\" [1 1] :op/none]\n", 50))
	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed)
	assert.Less(t, len(compressed), len(input))

	out, err := FSEDecompress(compressed, len(input))
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, out))
}

// ---- FSE Determinism Tests ----

func TestFSE_Determinism(t *testing.T) {
	input := []byte(strings.Repeat("determinism test payload ", 40))
	golden, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, golden)

	for i := 0; i < 100; i++ {
		compressed, err := FSECompress(input)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(golden, compressed), "iteration %d: output differs", i)
	}
}

func TestFSE_Determinism_Concurrent(t *testing.T) {
	input := []byte(strings.Repeat("concurrent determinism ", 40))
	golden, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, golden)

	var wg sync.WaitGroup
	errs := make(chan string, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			compressed, err := FSECompress(input)
			if err != nil {
				errs <- fmt.Sprintf("goroutine %d: error: %v", idx, err)
				return
			}
			if !bytes.Equal(golden, compressed) {
				errs <- fmt.Sprintf("goroutine %d: output differs (len %d vs %d)",
					idx, len(compressed), len(golden))
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for msg := range errs {
		t.Error(msg)
	}
}

func TestFSE_Determinism_DifferentSizes(t *testing.T) {
	// Verify determinism across a range of sizes
	sizes := []int{50, 100, 256, 500, 1000, 5000}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(size + 7)))
			input := make([]byte, size)
			// Use text-like data (biased distribution)
			for i := range input {
				input[i] = byte(rng.Intn(26)) + 'a'
			}

			golden, err := FSECompress(input)
			require.NoError(t, err)
			if golden == nil {
				return // safety net, skip
			}

			for j := 0; j < 50; j++ {
				compressed, err := FSECompress(input)
				require.NoError(t, err)
				assert.True(t, bytes.Equal(golden, compressed),
					"size=%d iter=%d: output differs", size, j)
			}
		})
	}
}

// ---- Fuzz-style Round-Trip Tests ----

func TestFSE_Fuzz_RandomInputs(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 500; i++ {
		size := rng.Intn(5000) + 10 // 10 to 5010
		input := make([]byte, size)
		rng.Read(input)

		compressed, err := FSECompress(input)
		require.NoError(t, err, "input %d (size %d)", i, size)
		if compressed == nil {
			continue // safety net
		}

		out, err := FSEDecompress(compressed, size)
		require.NoError(t, err, "input %d (size %d)", i, size)
		assert.True(t, bytes.Equal(input, out),
			"input %d (size %d): round-trip failed", i, size)
	}
}

func TestFSE_Fuzz_TextLikeInputs(t *testing.T) {
	words := []string{"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
		"and", "then", "sat", "down", "by", "river", "to", "rest", "a", "in", "on"}
	rng := rand.New(rand.NewSource(43))

	for i := 0; i < 200; i++ {
		// Generate random sentence
		numWords := rng.Intn(200) + 20
		var sb strings.Builder
		for j := 0; j < numWords; j++ {
			if j > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(words[rng.Intn(len(words))])
		}
		input := []byte(sb.String())

		compressed, err := FSECompress(input)
		require.NoError(t, err, "sentence %d", i)
		if compressed == nil {
			continue
		}

		out, err := FSEDecompress(compressed, len(input))
		require.NoError(t, err, "sentence %d", i)
		assert.True(t, bytes.Equal(input, out),
			"sentence %d: round-trip failed (len %d)", i, len(input))
	}
}

func TestFSE_Fuzz_SkewedDistributions(t *testing.T) {
	rng := rand.New(rand.NewSource(44))

	for i := 0; i < 100; i++ {
		// Generate input with varying skew
		numSymbols := rng.Intn(10) + 2 // 2-11 symbols
		size := rng.Intn(3000) + 100

		input := make([]byte, size)
		for j := range input {
			// Zipf-like: lower symbols more likely
			sym := rng.Intn(numSymbols * numSymbols)
			sym = int(float64(numSymbols) - float64(numSymbols)/float64(sym/numSymbols+1))
			if sym >= numSymbols {
				sym = numSymbols - 1
			}
			input[j] = byte(sym)
		}

		compressed, err := FSECompress(input)
		require.NoError(t, err, "skewed %d", i)
		if compressed == nil {
			continue
		}

		out, err := FSEDecompress(compressed, size)
		require.NoError(t, err, "skewed %d", i)
		assert.True(t, bytes.Equal(input, out),
			"skewed %d: round-trip failed (len %d, symbols %d)", i, size, numSymbols)
	}
}

// ---- Compression Ratio Tests (Informational) ----

func TestFSE_Ratio_HighlySkewed(t *testing.T) {
	// 95% one symbol, 5% another
	input := make([]byte, 10000)
	for i := range input {
		if i%20 == 0 {
			input[i] = 'b'
		} else {
			input[i] = 'a'
		}
	}
	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	ratio := float64(len(input)) / float64(len(compressed))
	t.Logf("Highly skewed (95/5): %.2fx compression (10000 → %d bytes)", ratio, len(compressed))
	assert.Greater(t, ratio, 2.0, "expected at least 2x on highly skewed data")
}

func TestFSE_Ratio_EnglishText(t *testing.T) {
	input := []byte(strings.Repeat(
		"To be or not to be, that is the question. Whether tis nobler in the mind "+
			"to suffer the slings and arrows of outrageous fortune, or to take arms "+
			"against a sea of troubles, and by opposing end them. ", 10))

	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	ratio := float64(len(input)) / float64(len(compressed))
	t.Logf("English text: %.2fx compression (%d → %d bytes)", ratio, len(input), len(compressed))
	// FSE alone (no LZ77) on English text: expect ~1.5-2x
	assert.Greater(t, ratio, 1.2, "expected some compression on English text")
}

// ---- Error Handling Tests ----

func TestFSE_Decompress_Empty(t *testing.T) {
	_, err := FSEDecompress(nil, 10)
	assert.Error(t, err)

	_, err = FSEDecompress([]byte{}, 10)
	assert.Error(t, err)
}

func TestFSE_Decompress_Truncated(t *testing.T) {
	input := []byte(strings.Repeat("test data ", 100))
	compressed, err := FSECompress(input)
	require.NoError(t, err)
	require.NotNil(t, compressed)

	// Truncate at various points
	for _, cutoff := range []int{1, 2, 5, len(compressed) / 2} {
		_, err := FSEDecompress(compressed[:cutoff], len(input))
		assert.Error(t, err, "cutoff=%d should error", cutoff)
	}
}
