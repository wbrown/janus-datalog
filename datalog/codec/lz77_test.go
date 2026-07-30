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

func TestLZ77_RoundTripRepeatAfterGap(t *testing.T) {
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

	// 208 back is well inside the 1MB window, so the second occurrence encodes as
	// a back-reference and the round trip covers a match, not only literals.
	sb := FindMatches(input)
	reconstructed := Reconstruct(sb)
	assert.True(t, bytes.Equal(input, reconstructed))
}

// TestLZ77_MatchAtWindowBoundary pins the two offsets on either side of the
// match window. findBestMatch admits a candidate when cpos >= pos-window, so an
// offset of exactly lz77WindowSize is the last one that matches and
// lz77WindowSize+1 is the first that does not.
//
// Round-tripping cannot see this on its own: a repeat the encoder declines to
// match still reconstructs correctly, as literals. The assertions are therefore
// on the emitted offsets.
func TestLZ77_MatchAtWindowBoundary(t *testing.T) {
	pattern := []byte("LZ77-WINDOW-BOUNDARY-SENTINEL!!!")

	// The filler is one repeated byte absent from the pattern, which keeps every
	// interior 4-byte window in a single hash bucket. The pattern's own bucket
	// then holds just its two occurrences, so the lz77MaxChain walk reaches the
	// far one instead of being crowded out by collisions.
	repeatAtOffset := func(offset int) []byte {
		input := make([]byte, 0, offset+len(pattern))
		input = append(input, pattern...)
		for len(input) < offset {
			input = append(input, 'q')
		}
		return append(input, pattern...)
	}

	t.Run("offset equal to the window matches", func(t *testing.T) {
		input := repeatAtOffset(lz77WindowSize)
		sb := FindMatches(input)

		var found bool
		for _, seq := range sb.Sequences {
			if seq.Offset == lz77WindowSize {
				found = true
				require.GreaterOrEqual(t, seq.MatchLen, len(pattern),
					"the boundary match must cover the whole repeated pattern")
			}
		}
		require.True(t, found,
			"a repeat exactly lz77WindowSize back is inside the window and must be matched")
		assert.True(t, bytes.Equal(input, Reconstruct(sb)))
	})

	t.Run("offset one past the window does not match", func(t *testing.T) {
		input := repeatAtOffset(lz77WindowSize + 1)
		sb := FindMatches(input)

		for _, seq := range sb.Sequences {
			require.LessOrEqual(t, seq.Offset, lz77WindowSize,
				"no back-reference may reach further than lz77WindowSize")
		}
		assert.True(t, bytes.Equal(input, Reconstruct(sb)),
			"the declined repeat still reconstructs, as literals")
	})
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

// keep fmt import used
var _ = fmt.Sprintf
