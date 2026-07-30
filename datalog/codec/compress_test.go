package codec

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	mathrand "math/rand"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Round-Trip Tests ----

func TestCompress_RoundTrip_EnglishText(t *testing.T) {
	input := []byte(strings.Repeat(
		"To be or not to be, that is the question. Whether tis nobler in the mind "+
			"to suffer the slings and arrows of outrageous fortune, or to take arms "+
			"against a sea of troubles, and by opposing end them. ", 5))

	compressed := Compress(input)
	require.NotNil(t, compressed, "English text should compress")
	t.Logf("English text: %d → %d bytes (%.1fx)", len(input), len(compressed),
		float64(len(input))/float64(len(compressed)))

	decompressed, err := Decompress(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, decompressed))
}

func TestCompress_RoundTrip_EDN(t *testing.T) {
	input := []byte(strings.Repeat(
		"[#identity \"abc123\" :task/content \"The butler did it.\" [100 1] :op/none]\n", 30))

	compressed := Compress(input)
	require.NotNil(t, compressed)
	t.Logf("EDN: %d → %d bytes (%.1fx)", len(input), len(compressed),
		float64(len(input))/float64(len(compressed)))

	decompressed, err := Decompress(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, decompressed))
}

func TestCompress_RoundTrip_Code(t *testing.T) {
	input := []byte(strings.Repeat(
		"func main() {\n\tfmt.Println(\"hello world\")\n\tfor i := 0; i < 10; i++ {\n\t\tfmt.Println(i)\n\t}\n}\n", 20))

	compressed := Compress(input)
	require.NotNil(t, compressed)
	t.Logf("Code: %d → %d bytes (%.1fx)", len(input), len(compressed),
		float64(len(input))/float64(len(compressed)))

	decompressed, err := Decompress(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, decompressed))
}

func TestCompress_RoundTrip_AllSame(t *testing.T) {
	input := bytes.Repeat([]byte{'a'}, 1000)
	compressed := Compress(input)
	require.NotNil(t, compressed)
	t.Logf("All same: %d → %d bytes (%.1fx)", len(input), len(compressed),
		float64(len(input))/float64(len(compressed)))

	decompressed, err := Decompress(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, decompressed))
}

func TestCompress_RoundTrip_Repetitive(t *testing.T) {
	input := bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 250)
	compressed := Compress(input)
	require.NotNil(t, compressed)
	t.Logf("Repetitive: %d → %d bytes (%.1fx)", len(input), len(compressed),
		float64(len(input))/float64(len(compressed)))

	decompressed, err := Decompress(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, decompressed))
}

func TestCompress_RoundTrip_TwoAlternating(t *testing.T) {
	input := bytes.Repeat([]byte{'a', 'b'}, 500)
	compressed := Compress(input)
	require.NotNil(t, compressed)

	decompressed, err := Decompress(compressed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(input, decompressed))
}

func TestCompress_RoundTrip_BinaryRamp(t *testing.T) {
	input := make([]byte, 512)
	for i := range input {
		input[i] = byte(i % 256)
	}
	compressed := Compress(input)
	if compressed != nil {
		decompressed, err := Decompress(compressed)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(input, decompressed))
	}
}

func TestCompress_RoundTrip_SizeSweep(t *testing.T) {
	sizes := []int{256, 300, 500, 1000, 2000, 4096, 8000, 16000, 50000}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%d", size), func(t *testing.T) {
			base := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", size/45+2))
			input := base[:size]

			compressed := Compress(input)
			require.NotNil(t, compressed, "size=%d should compress", size)

			decompressed, err := Decompress(compressed)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(input, decompressed),
				"size=%d: round-trip failed", size)

			ratio := float64(size) / float64(len(compressed))
			t.Logf("size=%d: %d → %d bytes (%.1fx)", size, size, len(compressed), ratio)
		})
	}
}

// ---- Safety Net Tests ----

func TestCompress_SafetyNet_HighEntropy(t *testing.T) {
	input := make([]byte, 1000)
	rand.Read(input)
	compressed := Compress(input)
	assert.Nil(t, compressed, "truly random data should not compress")
}

func TestCompress_SafetyNet_Empty(t *testing.T) {
	compressed := Compress(nil)
	assert.Nil(t, compressed)
}

func TestCompress_SafetyNet_TooSmall(t *testing.T) {
	// Very small inputs have too much header overhead
	compressed := Compress([]byte("hi"))
	assert.Nil(t, compressed, "2 bytes should not compress (header overhead)")
}

// ---- Header Tests ----

func TestCompress_Header_Parse(t *testing.T) {
	input := []byte(strings.Repeat("header test content ", 50))
	compressed := Compress(input)
	require.NotNil(t, compressed)

	version, origLen, err := CompressedHeader(compressed)
	require.NoError(t, err)
	assert.Equal(t, CompressionVersion, version)
	assert.Equal(t, len(input), origLen)
}

func TestCompress_Header_Truncated(t *testing.T) {
	_, _, err := CompressedHeader([]byte{0x01, 0x00})
	assert.Error(t, err)
}

func TestCompress_Header_WrongVersion(t *testing.T) {
	data := make([]byte, 20)
	data[0] = 0x03 // unsupported version (1 and 2 are valid)
	binary.BigEndian.PutUint32(data[1:], 100)
	_, err := Decompress(data)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported version")
}

func TestCompress_Header_Corrupted(t *testing.T) {
	input := []byte(strings.Repeat("valid input for corruption test ", 50))
	compressed := Compress(input)
	require.NotNil(t, compressed)

	// Flip a byte in the middle of the compressed data
	corrupted := make([]byte, len(compressed))
	copy(corrupted, compressed)
	corrupted[len(corrupted)/2] ^= 0xFF

	_, err := Decompress(corrupted)
	// Should fail — either FSE decompress error or data mismatch
	assert.Error(t, err, "corrupted data should fail decompression")
}

func TestCompress_Header_VersionByte(t *testing.T) {
	input := []byte(strings.Repeat("version check ", 50))
	compressed := Compress(input)
	require.NotNil(t, compressed)
	assert.Equal(t, CompressionVersion, compressed[0])
}

// ---- Determinism Stress Tests ----

func TestCompress_Determinism_Repeated(t *testing.T) {
	input := []byte(strings.Repeat("This is a determinism test. ", 100))
	golden := Compress(input)
	require.NotNil(t, golden)

	for i := 0; i < 1000; i++ {
		compressed := Compress(input)
		require.True(t, bytes.Equal(golden, compressed), "iteration %d: output differs", i)
	}
}

func TestCompress_Determinism_LargeRepeated(t *testing.T) {
	base := []byte(strings.Repeat(
		"To be or not to be, that is the question. Whether tis nobler in the mind to suffer. ", 600))
	input := base[:50000]
	golden := Compress(input)
	require.NotNil(t, golden)

	for i := 0; i < 100; i++ {
		compressed := Compress(input)
		require.True(t, bytes.Equal(golden, compressed), "iteration %d: output differs", i)
	}
}

func TestCompress_Determinism_Concurrent(t *testing.T) {
	input := []byte(strings.Repeat("concurrent test payload ", 100))
	golden := Compress(input)
	require.NotNil(t, golden)

	var wg sync.WaitGroup
	errs := make(chan string, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			compressed := Compress(input)
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

// ---- Fuzz Round-Trip Tests ----

func TestCompress_Fuzz_RandomInputs(t *testing.T) {
	rng := mathrand.New(mathrand.NewSource(42))
	for i := 0; i < 500; i++ {
		size := rng.Intn(10000) + 100
		input := make([]byte, size)
		rng.Read(input)

		compressed := Compress(input)
		if compressed == nil {
			continue // safety net
		}

		decompressed, err := Decompress(compressed)
		require.NoError(t, err, "input %d (size %d)", i, size)
		require.True(t, bytes.Equal(input, decompressed),
			"input %d (size %d): round-trip failed", i, size)
	}
}

func TestCompress_Fuzz_TextLike(t *testing.T) {
	words := []string{"the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
		"and", "then", "sat", "down", "by", "river", "to", "rest", "a", "in", "on"}
	rng := mathrand.New(mathrand.NewSource(43))

	for i := 0; i < 200; i++ {
		numWords := rng.Intn(500) + 50
		var sb strings.Builder
		for j := 0; j < numWords; j++ {
			if j > 0 {
				sb.WriteByte(' ')
			}
			sb.WriteString(words[rng.Intn(len(words))])
		}
		input := []byte(sb.String())

		compressed := Compress(input)
		if compressed == nil {
			continue
		}

		decompressed, err := Decompress(compressed)
		require.NoError(t, err, "sentence %d (len %d)", i, len(input))
		require.True(t, bytes.Equal(input, decompressed),
			"sentence %d: round-trip failed (len %d)", i, len(input))
	}
}

func TestCompress_Fuzz_StructuredData(t *testing.T) {
	rng := mathrand.New(mathrand.NewSource(44))

	for i := 0; i < 100; i++ {
		// Generate EDN-like lines
		var sb strings.Builder
		numLines := rng.Intn(20) + 5
		for j := 0; j < numLines; j++ {
			fmt.Fprintf(&sb, "[#identity \"%06d\" :attr/name-%d \"value-%d\" [%d 1] :op/none]\n",
				rng.Intn(999999), rng.Intn(10), rng.Intn(1000), rng.Intn(10000))
		}
		input := []byte(sb.String())

		compressed := Compress(input)
		if compressed == nil {
			continue
		}

		decompressed, err := Decompress(compressed)
		require.NoError(t, err, "edn %d", i)
		require.True(t, bytes.Equal(input, decompressed),
			"edn %d: round-trip failed (len %d)", i, len(input))
	}
}

// ---- Compression Ratio Tests ----

func makeTestInput(pattern string, targetSize int) []byte {
	reps := targetSize/len(pattern) + 2
	base := []byte(strings.Repeat(pattern, reps))
	return base[:targetSize]
}

func TestCompress_Ratio_EnglishProse(t *testing.T) {
	input := makeTestInput(
		"To be or not to be, that is the question. Whether tis nobler in the mind "+
			"to suffer the slings and arrows of outrageous fortune, or to take arms "+
			"against a sea of troubles, and by opposing end them. ", 10000)

	compressed := Compress(input)
	require.NotNil(t, compressed)
	ratio := float64(len(input)) / float64(len(compressed))
	t.Logf("English prose 10KB: %.2fx (%d → %d bytes)", ratio, len(input), len(compressed))
	assert.Greater(t, ratio, 2.5, "expected at least 2.5x on English prose")
}

func TestCompress_Ratio_StructuredEDN(t *testing.T) {
	input := makeTestInput(
		"[#identity \"abc123\" :task/content \"The butler did it in the library.\" [100 1] :op/none]\n", 10000)

	compressed := Compress(input)
	require.NotNil(t, compressed)
	ratio := float64(len(input)) / float64(len(compressed))
	t.Logf("EDN 10KB: %.2fx (%d → %d bytes)", ratio, len(input), len(compressed))
	assert.Greater(t, ratio, 2.0, "expected at least 2x on structured EDN")
}

func TestCompress_Ratio_SourceCode(t *testing.T) {
	input := makeTestInput(
		"func processItem(ctx context.Context, item *Item) error {\n"+
			"\tif item == nil {\n\t\treturn fmt.Errorf(\"nil item\")\n\t}\n"+
			"\tresult, err := db.Query(ctx, item.ID)\n"+
			"\tif err != nil {\n\t\treturn err\n\t}\n"+
			"\treturn result.Apply(item)\n}\n\n", 10000)

	compressed := Compress(input)
	require.NotNil(t, compressed)
	ratio := float64(len(input)) / float64(len(compressed))
	t.Logf("Source code 10KB: %.2fx (%d → %d bytes)", ratio, len(input), len(compressed))
	assert.Greater(t, ratio, 2.0, "expected at least 2x on source code")
}

// ---- Golden Tests ----
// These record the exact compressed output for specific inputs.
// If ANY golden test fails after a code change, the change MUST be rejected.
// The golden outputs are as immutable as the L85 alphabet.
//
// Golden outputs are recorded by running the test once with -update-golden
// (or manually). Once recorded, they are frozen forever.

// goldenTest defines a golden test case.
type goldenTest struct {
	name        string
	input       []byte
	expectedHex string // hex-encoded expected v2 compressed output (empty = not yet recorded)
	v1Hex       string // captured v1 (uint16-header) blob; dual-read back-compat fixture
	minRatio    float64
}

// Golden tests: frozen compressed outputs. If ANY of these change, the codec
// determinism guarantee is broken and the change MUST be rejected.
// expectedHex holds the CompressionVersion 0x02 (uint32 counts) golden output.
// v1Hex holds real captured v1 (0x01, uint16 counts) blobs, proving
// Decompress still reads legacy v1 data.
var goldenTests = []goldenTest{
	{
		name:        "paragraph_500",
		input:       []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 11))[:500],
		minRatio:    2.0,
		expectedHex: "02000001f400000003000000030000002e0000002e010000002e54686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e2000000000030100000003140001000000030100000003171701000000030100000003050500000000040100000004fd6ead06",
		v1Hex:       "01000001f4000300030000002e0000002e010000002e54686520717569636b2062726f776e20666f78206a756d7073206f76657220746865206c617a7920646f672e2000000000030100000003140001000000030100000003171701000000030100000003050500000000040100000004fd6ead06",
	},
	{
		name:        "prose_1kb",
		input:       []byte(strings.Repeat("To be or not to be, that is the question. Whether tis nobler in the mind to suffer the slings and arrows. ", 10))[:1024],
		minRatio:    2.0,
		expectedHex: "0200000400000000080000000800000059000000590100000059546f206265206f72206e6f7420742c207468617420697320746865207175657374696f6e2e205768657468657220746973206e6f626c657220696e6d696e6473756666686520736c696e677320616e64206172726f77732e200000000801000000080e14040412000000000000080100000008010201011717171600000008010000000803050505060606060000000a010000000a6dc2836ff5adbed55e05",
		v1Hex:       "01000004000008000800000059000000590100000059546f206265206f72206e6f7420742c207468617420697320746865207175657374696f6e2e205768657468657220746973206e6f626c657220696e6d696e6473756666686520736c696e677320616e64206172726f77732e200000000801000000080e14040412000000000000080100000008010201011717171600000008010000000803050505060606060000000a010000000a6dc2836ff5adbed55e05",
	},
	{
		name:        "edn_1kb",
		input:       []byte(strings.Repeat("[#identity \"hash25\" :test/attr \"some value here\" [42 1] :op/none]\n", 16))[:1024],
		minRatio:    2.0,
		expectedHex: "02000004000000000400000004000000420000004201000000425b236964656e74697479202268617368323522203a746573742f617474722022736f6d652076616c7565206865726522205b343220315d203a6f702f6e6f6e655d0a000000040100000004150000000000000401000000041717171700000004010000000406060606000000080100000008f22dbc85b7501200",
		v1Hex:       "010000040000040004000000420000004201000000425b236964656e74697479202268617368323522203a746573742f617474722022736f6d652076616c7565206865726522205b343220315d203a6f702f6e6f6e655d0a000000040100000004150000000000000401000000041717171700000004010000000406060606000000080100000008f22dbc85b7501200",
	},
	{
		name:        "repetitive_1kb",
		input:       bytes.Repeat([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 256),
		minRatio:    5.0,
		expectedHex: "0200000400000000040000000400000004000000040100000004deadbeef0000000401000000040400000000000004010000000417171717000000040100000004020202020000000501000000056fdebc1903",
		v1Hex:       "01000004000004000400000004000000040100000004deadbeef0000000401000000040400000000000004010000000417171717000000040100000004020202020000000501000000056fdebc1903",
	},
	{
		name:        "all_same_1kb",
		input:       bytes.Repeat([]byte{'a'}, 1024),
		minRatio:    10.0,
		expectedHex: "020000040000000004000000040000000100000001010000000161000000040100000004010000000000000401000000041717171700000004010000000400000000000000040100000004eff7db0c",
		v1Hex:       "0100000400000400040000000100000001010000000161000000040100000004010000000000000401000000041717171700000004010000000400000000000000040100000004eff7db0c",
	},
}

func TestCompress_Golden_Record(t *testing.T) {
	// This test records golden outputs. Run once to capture, then
	// copy the hex strings into goldenTests above and freeze them.
	for _, gt := range goldenTests {
		t.Run(gt.name, func(t *testing.T) {
			compressed := Compress(gt.input)
			require.NotNil(t, compressed, "%s should compress", gt.name)

			// Verify round-trip
			decompressed, err := Decompress(compressed)
			require.NoError(t, err)
			require.True(t, bytes.Equal(gt.input, decompressed), "round-trip failed")

			ratio := float64(len(gt.input)) / float64(len(compressed))
			t.Logf("%s: %d → %d bytes (%.1fx)", gt.name, len(gt.input), len(compressed), ratio)
			assert.GreaterOrEqual(t, ratio, gt.minRatio,
				"%s: ratio %.1f below minimum %.1f", gt.name, ratio, gt.minRatio)

			// Print hex for recording
			t.Logf("GOLDEN %s: %s", gt.name, hex.EncodeToString(compressed))

			// If expectedHex is set, verify it matches
			if gt.expectedHex != "" {
				expected, err := hex.DecodeString(gt.expectedHex)
				require.NoError(t, err)
				assert.True(t, bytes.Equal(compressed, expected),
					"%s: compressed output changed!\nGot:    %s\nExpect: %s",
					gt.name, hex.EncodeToString(compressed), gt.expectedHex)
			}
		})
	}
}

func TestCompress_Determinism_Golden(t *testing.T) {
	// Each golden input compressed 100 times must produce identical output
	for _, gt := range goldenTests {
		t.Run(gt.name, func(t *testing.T) {
			golden := Compress(gt.input)
			require.NotNil(t, golden)

			for i := 0; i < 100; i++ {
				compressed := Compress(gt.input)
				require.True(t, bytes.Equal(golden, compressed),
					"%s: iteration %d differs", gt.name, i)
			}
		})
	}
}

// ---- v1 → v2 back-compat (dual read) ----

// TestDecompress_V1BlobStillReadable proves the v2 decoder still reads real
// captured v1 (uint16-header) blobs. The fixtures are the original golden
// outputs recorded under CompressionVersion 0x01 — genuine legacy bytes, not
// synthesized — so this guards the dual-read path against regression.
func TestDecompress_V1BlobStillReadable(t *testing.T) {
	for _, gt := range goldenTests {
		t.Run(gt.name, func(t *testing.T) {
			v1, err := hex.DecodeString(gt.v1Hex)
			require.NoError(t, err)
			require.Equal(t, byte(0x01), v1[0], "fixture must be a v1 blob")

			got, err := Decompress(v1)
			require.NoError(t, err, "v1 blob must still decode under the v2 decoder")
			assert.True(t, bytes.Equal(gt.input, got),
				"v1 blob did not round-trip to its original input")
		})
	}
}

// ---- uint16 sequence-count overflow regression (data-loss fix) ----

// jsonish builds the densest realistic content: a JSON-like array of small
// uniform records. Sequence count scales with the number of distinct short
// fields, so this crosses 65535 sequences (the old uint16 ceiling) well before
// a few MB.
func jsonish(n int) []byte {
	var b bytes.Buffer
	b.WriteByte('[')
	for i := 0; b.Len() < n; i++ {
		fmt.Fprintf(&b, `{"id":%d,"name":"alice","active":true,"score":%d},`,
			100000+i, i%97)
	}
	return b.Bytes()[:n]
}

// adversarialDenseSequences emits a stream of recurring 4-byte tokens, each
// followed by a pseudo-random separator byte. The tokens recur (so the 4-byte
// match hash hits and a sequence is emitted) but the separator prevents match
// extension and keeps the stream aperiodic, maximizing sequence count per byte.
func adversarialDenseSequences(n int) []byte {
	r := mathrand.New(mathrand.NewSource(1))
	const poolSize = 256
	pool := make([][4]byte, poolSize)
	for i := range pool {
		binary.BigEndian.PutUint32(pool[i][:], uint32(i)*2654435761)
	}
	out := make([]byte, 0, n+8)
	for len(out) < n {
		tok := pool[r.Intn(poolSize)]
		out = append(out, tok[:]...)
		out = append(out, byte(r.Intn(256)))
	}
	return out[:n]
}

// TestCompress_JSONLikeValueAboveUint16Sequences_RoundTrips is the core
// regression: a ~2 MB densely-structured value produces more than 65535 LZ77
// sequences. Under v1 the count truncated to uint16 and the blob became
// permanently unreadable; under v2 it must round-trip exactly.
func TestCompress_JSONLikeValueAboveUint16Sequences_RoundTrips(t *testing.T) {
	input := jsonish(2 * 1024 * 1024)

	seqs := len(FindMatches(input).Sequences)
	require.Greater(t, seqs, 0xFFFF,
		"test input must exceed the old uint16 cap (got %d sequences)", seqs)
	t.Logf("jsonish 2 MB → %d sequences", seqs)

	compressed := Compress(input)
	require.NotNil(t, compressed, "structured value should compress")
	require.Equal(t, CompressionVersion, compressed[0], "must be written as v2")

	out, err := Decompress(compressed)
	require.NoError(t, err, "value with >uint16 sequences must decompress")
	assert.True(t, bytes.Equal(input, out), "round trip must be lossless")
}

// TestCompress_AdversarialDenseSequences_RoundTrips drives the sequence count
// past the old cap with a much smaller (~512 KB) adversarial value.
func TestCompress_AdversarialDenseSequences_RoundTrips(t *testing.T) {
	input := adversarialDenseSequences(512 * 1024)

	seqs := len(FindMatches(input).Sequences)
	require.Greater(t, seqs, 0xFFFF,
		"adversarial input must exceed the old uint16 cap (got %d sequences)", seqs)
	t.Logf("adversarial 512 KB → %d sequences", seqs)

	compressed := Compress(input)
	if compressed == nil {
		// Safety net engaged: stored raw, which is also safe (never unreadable).
		t.Skip("adversarial value did not compress below original; stored raw (safe)")
	}
	require.Equal(t, CompressionVersion, compressed[0], "must be written as v2")

	out, err := Decompress(compressed)
	require.NoError(t, err, "value with >uint16 sequences must decompress")
	assert.True(t, bytes.Equal(input, out), "round trip must be lossless")
}

// TestCompress_SequenceCountOverflow_DoesNotProduceUnreadableBlob asserts the
// invariant directly: for inputs whose sequence count exceeds the old uint16
// ceiling, Compress must EITHER produce a blob that decompresses correctly
// (widened v2 header) OR decline (return nil ⇒ stored raw). It must never
// produce a blob that Decompress rejects.
func TestCompress_SequenceCountOverflow_DoesNotProduceUnreadableBlob(t *testing.T) {
	inputs := map[string][]byte{
		"jsonish_2mb":     jsonish(2 * 1024 * 1024),
		"adversarial_512": adversarialDenseSequences(512 * 1024),
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			require.Greater(t, len(FindMatches(input).Sequences), 0xFFFF,
				"input must exceed the old uint16 cap to exercise the fix")

			compressed := Compress(input)
			if compressed == nil {
				return // declined → stored raw → safe by construction
			}
			out, err := Decompress(compressed)
			require.NoError(t, err, "compressed blob must never be unreadable")
			assert.True(t, bytes.Equal(input, out))
		})
	}
}

// ---- Benchmarks ----

func BenchmarkCompress(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"256B", 256}, {"1KB", 1024}, {"4KB", 4096}, {"16KB", 16384},
	}
	for _, s := range sizes {
		base := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", s.size/45+2))
		input := base[:s.size]
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(int64(s.size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Compress(input)
			}
		})
	}
}

func BenchmarkDecompress(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"256B", 256}, {"1KB", 1024}, {"4KB", 4096}, {"16KB", 16384},
	}
	for _, s := range sizes {
		base := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", s.size/45+2))
		input := base[:s.size]
		compressed := Compress(input)
		if compressed == nil {
			continue
		}
		b.Run(s.name, func(b *testing.B) {
			b.SetBytes(int64(s.size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				Decompress(compressed)
			}
		})
	}
}
