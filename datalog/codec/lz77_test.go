package codec

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Match Finding Tests ----

func TestLZ77_NoMatches(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	input := make([]byte, 500)
	rng.Read(input)

	sb := FindMatches(input)
	// All output should be literals (random data has no repeats)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed), "random data round-trip failed")

	// All sequences should be literals-only or have short matches
	totalLits := 0
	for _, seq := range sb.Sequences {
		totalLits += seq.LitLen
	}
	// Most bytes should be literals for random data
	assert.Greater(t, totalLits, 400, "expected mostly literals for random data")
}

func TestLZ77_SimpleRepeat(t *testing.T) {
	input := []byte("abcdefgh abcdefgh")
	sb := FindMatches(input)

	// Must round-trip
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))

	// Should find at least one match
	hasMatch := false
	for _, seq := range sb.Sequences {
		if seq.MatchLen >= 3 {
			hasMatch = true
			break
		}
	}
	assert.True(t, hasMatch, "expected a match for repeated 'abcdefgh'")
}

func TestLZ77_OverlappingMatch(t *testing.T) {
	input := []byte("aaaaaaaaaaaa") // 12 a's — match overlaps with itself
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))

	// Should find a match (offset=1, length up to 11)
	hasMatch := false
	for _, seq := range sb.Sequences {
		if seq.MatchLen >= 3 && seq.Offset == 1 {
			hasMatch = true
			break
		}
	}
	assert.True(t, hasMatch, "expected overlapping match at offset 1")
}

func TestLZ77_MinMatchLength(t *testing.T) {
	input := []byte("ab ab") // "ab" repeats but length=2 < minimum=3
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))

	// Should NOT find a match (2-byte repeat below minimum)
	for _, seq := range sb.Sequences {
		if seq.MatchLen > 0 {
			assert.GreaterOrEqual(t, seq.MatchLen, lz77MinMatch,
				"match length %d below minimum %d", seq.MatchLen, lz77MinMatch)
		}
	}
}

func TestLZ77_ExactMinMatch(t *testing.T) {
	input := []byte("abc abc") // "abc" repeats, length=3 == minimum
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

func TestLZ77_WindowBoundary(t *testing.T) {
	// Create input where a pattern repeats beyond the window
	// Use a small window for testing
	pattern := []byte("PATTERN!")
	gap := make([]byte, 200)
	for i := range gap {
		gap[i] = byte('0' + i%10)
	}

	// Pattern appears at position 0 and position 208
	input := make([]byte, 0, 300)
	input = append(input, pattern...)
	input = append(input, gap...)
	input = append(input, pattern...)

	// With default window (1MB), match should be found
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

func TestLZ77_LazyMatch(t *testing.T) {
	// Position 10: "abcd" matches (length 4)
	// Position 11: "bcdefgh" could match longer if we skip one literal
	input := []byte("___abcdefgh___abcdefgh")
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))

	// Should find a match of length >= 7 (lazy matching finds the longer one)
	maxMatch := 0
	for _, seq := range sb.Sequences {
		if seq.MatchLen > maxMatch {
			maxMatch = seq.MatchLen
		}
	}
	assert.GreaterOrEqual(t, maxMatch, 7, "lazy matching should find longer match")
}

func TestLZ77_MultipleMatches(t *testing.T) {
	input := []byte("the cat sat on the mat and the cat sat on the mat")
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))

	matchCount := 0
	for _, seq := range sb.Sequences {
		if seq.MatchLen >= lz77MinMatch {
			matchCount++
		}
	}
	assert.Greater(t, matchCount, 1, "expected multiple matches in repeated text")
}

func TestLZ77_Empty(t *testing.T) {
	sb := FindMatches(nil)
	reconstructed := Reconstruct(sb)
	assert.Empty(t, reconstructed)
}

func TestLZ77_OneByte(t *testing.T) {
	input := []byte{0x42}
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

func TestLZ77_TwoBytes(t *testing.T) {
	input := []byte{0x42, 0x43}
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

func TestLZ77_ThreeBytes(t *testing.T) {
	input := []byte("abc")
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

// ---- Reconstruction Property (Critical) ----

func TestLZ77_ReconstructProperty(t *testing.T) {
	inputs := []struct {
		name  string
		input []byte
	}{
		{"english", []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog.")},
		{"json", []byte(`{"name":"Alice","age":30,"tags":["admin","user"],"name":"Alice","age":30}`)},
		{"binary_allsame", bytes.Repeat([]byte{0xAA}, 500)},
		{"binary_alternating", bytes.Repeat([]byte{0xAA, 0xBB}, 250)},
		{"code", []byte("func main() {\n\tfmt.Println(\"hello\")\n\tfmt.Println(\"world\")\n}\n")},
		{"empty", nil},
		{"one_byte", []byte{0x42}},
		{"three_bytes", []byte("abc")},
		{"edn", []byte(strings.Repeat("[#identity \"abc\" :test/name \"value\" [1 1] :op/none]\n", 20))},
		{"long_prose", []byte(strings.Repeat(
			"To be or not to be, that is the question. Whether tis nobler in the mind "+
				"to suffer the slings and arrows of outrageous fortune. ", 20))},
	}

	for _, tc := range inputs {
		t.Run(tc.name, func(t *testing.T) {
			sb := FindMatches(tc.input)
			reconstructed := Reconstruct(sb)
			assert.True(t, bytes.Equal(tc.input, reconstructed),
				"round-trip failed: len(input)=%d len(output)=%d", len(tc.input), len(reconstructed))
		})
	}
}

func TestLZ77_ReconstructProperty_RandomSizes(t *testing.T) {
	rng := rand.New(rand.NewSource(55))
	for i := 0; i < 200; i++ {
		size := rng.Intn(5000) + 1
		input := make([]byte, size)
		// Mix of text-like and random data
		if i%2 == 0 {
			for j := range input {
				input[j] = byte(rng.Intn(26)) + 'a'
			}
		} else {
			rng.Read(input)
		}

		sb := FindMatches(input)
		reconstructed := Reconstruct(sb)
		require.True(t, bytes.Equal(input, reconstructed),
			"round-trip failed: iteration=%d size=%d", i, size)
	}
}

// ---- Repeat Offset Tests ----

func TestRepeatOffset_Basic(t *testing.T) {
	ro := NewRepeatOffsets()
	assert.Equal(t, RepeatOffsets{1, 4, 8}, ro)

	// Encode offset 10 (new) → code = 10+3 = 13, ring becomes [10, 1, 4]
	code := ro.EncodeOffset(10)
	assert.Equal(t, 13, code)
	assert.Equal(t, RepeatOffsets{10, 1, 4}, ro)

	// Encode offset 10 again (repeat 0) → code = 1, ring unchanged
	code = ro.EncodeOffset(10)
	assert.Equal(t, 1, code)
	assert.Equal(t, RepeatOffsets{10, 1, 4}, ro)

	// Encode offset 1 (repeat 1) → code = 2, swap [0] and [1]
	code = ro.EncodeOffset(1)
	assert.Equal(t, 2, code)
	assert.Equal(t, RepeatOffsets{1, 10, 4}, ro)

	// Encode offset 4 (repeat 2) → code = 3, rotate to front
	code = ro.EncodeOffset(4)
	assert.Equal(t, 3, code)
	assert.Equal(t, RepeatOffsets{4, 1, 10}, ro)
}

func TestRepeatOffset_RoundTrip(t *testing.T) {
	offsets := []int{10, 20, 30, 10, 20, 30, 5, 5, 10, 30, 20, 5}

	encRO := NewRepeatOffsets()
	decRO := NewRepeatOffsets()

	codes := make([]int, len(offsets))
	for i, off := range offsets {
		codes[i] = encRO.EncodeOffset(off)
	}

	for i, code := range codes {
		decoded := decRO.DecodeOffset(code)
		assert.Equal(t, offsets[i], decoded, "offset %d: code=%d decoded=%d", i, code, decoded)
	}
}

func TestRepeatOffset_ThreeDistinct(t *testing.T) {
	ro := NewRepeatOffsets()

	// Push three distinct offsets
	ro.EncodeOffset(100) // ring: [100, 1, 4]
	ro.EncodeOffset(200) // ring: [200, 100, 1]
	ro.EncodeOffset(300) // ring: [300, 200, 100]

	// Now 100 is at position 2
	code := ro.EncodeOffset(100)
	assert.Equal(t, 3, code) // repeat offset 2 (third most recent)
	assert.Equal(t, RepeatOffsets{100, 300, 200}, ro)
}

func TestRepeatOffset_Ring(t *testing.T) {
	// Pattern: A, B, C, A, B, C — after first three, all should be repeats
	offsets := []int{10, 20, 30, 10, 20, 30, 10, 20, 30}

	ro := NewRepeatOffsets()
	codes := make([]int, len(offsets))
	for i, off := range offsets {
		codes[i] = ro.EncodeOffset(off)
	}

	// First three are new offsets (codes > 3)
	for i := 0; i < 3; i++ {
		assert.Greater(t, codes[i], 3, "offset %d should be new", i)
	}
	// Remaining should be repeat codes (1, 2, or 3)
	for i := 3; i < len(codes); i++ {
		assert.LessOrEqual(t, codes[i], 3, "offset %d should be a repeat", i)
	}

	// Verify round-trip
	decRO := NewRepeatOffsets()
	for i, code := range codes {
		decoded := decRO.DecodeOffset(code)
		assert.Equal(t, offsets[i], decoded, "offset %d", i)
	}
}

func TestRepeatOffset_StructuredData(t *testing.T) {
	// Simulate structured data where same field spacing recurs
	// Offsets: 50, 50, 50, 50 — after the first, all are repeat 0
	ro := NewRepeatOffsets()
	codes := make([]int, 4)
	for i := 0; i < 4; i++ {
		codes[i] = ro.EncodeOffset(50)
	}
	assert.Equal(t, 53, codes[0])   // new: 50 + 3
	assert.Equal(t, 1, codes[1])    // repeat 0
	assert.Equal(t, 1, codes[2])    // repeat 0
	assert.Equal(t, 1, codes[3])    // repeat 0
}

// ---- Compression Effectiveness Tests ----

func TestLZ77_CompressesRepetitiveData(t *testing.T) {
	input := []byte(strings.Repeat("hello world ", 100)) // 1200 bytes
	sb := FindMatches(input)

	// Count total match bytes
	matchBytes := 0
	for _, seq := range sb.Sequences {
		matchBytes += seq.MatchLen
	}
	// Repetitive data should have lots of matches
	assert.Greater(t, matchBytes, len(input)/2,
		"expected >50%% of data to be matches for repetitive input")

	// Literals should be much less than input
	assert.Less(t, len(sb.Literals), len(input)/2,
		"expected <50%% literals for repetitive input")

	// Round-trip
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

func TestLZ77_RandomDataMostlyLiterals(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	input := make([]byte, 1000)
	rng.Read(input)

	sb := FindMatches(input)

	// Random data should have very few matches
	assert.Greater(t, len(sb.Literals), 900,
		"expected >90%% literals for random data")

	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

// ---- LZ77 Benchmark ----

func BenchmarkFindMatches_1KB(b *testing.B) {
	base := []byte(strings.Repeat("The quick brown fox jumps. ", 40))
	input := base[:1024]
	b.SetBytes(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FindMatches(input)
	}
}

func BenchmarkFindMatches_4KB(b *testing.B) {
	base := []byte(strings.Repeat("The quick brown fox jumps. ", 160))
	input := base[:4096]
	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FindMatches(input)
	}
}

func BenchmarkReconstruct_1KB(b *testing.B) {
	base := []byte(strings.Repeat("The quick brown fox jumps. ", 40))
	input := base[:1024]
	sb := FindMatches(input)
	b.SetBytes(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Reconstruct(sb)
	}
}

// Helper to keep fmt import used
var _ = fmt.Sprintf
