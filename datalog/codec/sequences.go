package codec

// Sequence encoding: converts LZ77 (litLen, offset, matchLen) triples into
// three parallel streams suitable for FSE compression. Each value is encoded
// as a code (for FSE) plus extra bits (written directly to the bitstream).

// ---- Baseline + Extra Bits Tables ----
//
// These tables map variable-range values into (code, extraBits, baseline) triples.
// The code is what FSE compresses. The extra bits refine the value within the
// code's range: value = baseline + readBits(extraBits).
//
// The tables are frozen (determinism requirement). Do not modify.

// litLenTable: literal length codes (0-35)
// code 0-15: literal values 0-15 (0 extra bits)
// code 16+: exponentially increasing ranges
var litLenTable = []struct {
	maxVal    int
	extraBits int
}{
	{0, 0}, {1, 0}, {2, 0}, {3, 0},     // codes 0-3
	{4, 0}, {5, 0}, {6, 0}, {7, 0},     // codes 4-7
	{8, 0}, {9, 0}, {10, 0}, {11, 0},   // codes 8-11
	{12, 0}, {13, 0}, {14, 0}, {15, 0}, // codes 12-15
	{17, 1}, {19, 1}, {23, 2}, {31, 3}, // codes 16-19
	{47, 4}, {79, 5}, {143, 6}, {271, 7},       // codes 20-23
	{527, 8}, {1039, 9}, {2063, 10}, {4111, 11}, // codes 24-27
	{8207, 12}, {16399, 13}, {32783, 14}, {65551, 15}, // codes 28-31
	{131087, 16}, {262159, 17}, {524303, 18}, {1048591, 19}, // codes 32-35
}

// matchLenTable: match length codes (0-52)
// Match lengths are stored as matchLen - lz77MinMatch (so 0 = length 3)
var matchLenTable = []struct {
	maxVal    int
	extraBits int
}{
	{0, 0}, {1, 0}, {2, 0}, {3, 0},     // codes 0-3 (lengths 3-6)
	{4, 0}, {5, 0}, {6, 0}, {7, 0},     // codes 4-7 (lengths 7-10)
	{8, 0}, {9, 0}, {10, 0}, {11, 0},   // codes 8-11 (lengths 11-14)
	{12, 0}, {13, 0}, {14, 0}, {15, 0}, // codes 12-15 (lengths 15-18)
	{17, 1}, {19, 1}, {23, 2}, {31, 3}, // codes 16-19
	{47, 4}, {79, 5}, {143, 6}, {271, 7}, // codes 20-23
	{527, 8}, {1039, 9}, {2063, 10}, {4111, 11}, // codes 24-27
}

// offsetTable: offset codes (0-31)
// Offsets use pure exponential coding: code N covers [2^N, 2^(N+1))
// Code 0 is special: covers value 1 (offset of 1)

// EncodedStreams holds the three parallel streams produced from sequences,
// ready for FSE compression.
type EncodedStreams struct {
	LitLenCodes  []byte   // FSE-compressible codes for literal lengths
	LitLenExtra  []uint32 // extra bits values for literal lengths
	LitLenBits   []int    // number of extra bits per literal length

	MatchLenCodes []byte   // FSE-compressible codes for match lengths
	MatchLenExtra []uint32 // extra bits values for match lengths
	MatchLenBits  []int    // number of extra bits per match length

	OffsetCodes  []byte   // FSE-compressible codes for offsets
	OffsetExtra  []uint32 // extra bits values for offsets
	OffsetBits   []int    // number of extra bits per offset
}

// EncodeSequences converts a slice of Sequences into three parallel streams.
// Sequences with Offset == 0 (trailing literals) are skipped for match/offset streams.
func EncodeSequences(seqs []Sequence) *EncodedStreams {
	es := &EncodedStreams{}

	for _, seq := range seqs {
		// Encode literal length
		code, extra, nbits := encodeLitLen(seq.LitLen)
		es.LitLenCodes = append(es.LitLenCodes, code)
		es.LitLenExtra = append(es.LitLenExtra, extra)
		es.LitLenBits = append(es.LitLenBits, nbits)

		// Encode match (only if this sequence has a match)
		if seq.Offset > 0 {
			// Match length (stored as matchLen - minMatch)
			mlCode, mlExtra, mlBits := encodeMatchLen(seq.MatchLen - lz77MinMatch)
			es.MatchLenCodes = append(es.MatchLenCodes, mlCode)
			es.MatchLenExtra = append(es.MatchLenExtra, mlExtra)
			es.MatchLenBits = append(es.MatchLenBits, mlBits)

			// Offset (already coded by repeat offset logic or raw)
			offCode, offExtra, offBits := encodeOffset(seq.Offset)
			es.OffsetCodes = append(es.OffsetCodes, offCode)
			es.OffsetExtra = append(es.OffsetExtra, offExtra)
			es.OffsetBits = append(es.OffsetBits, offBits)
		}
	}

	return es
}

// DecodeSequences reconstructs Sequences from encoded streams.
func DecodeSequences(es *EncodedStreams) []Sequence {
	seqs := make([]Sequence, len(es.LitLenCodes))
	matchIdx := 0

	for i := range es.LitLenCodes {
		seqs[i].LitLen = decodeLitLen(es.LitLenCodes[i], es.LitLenExtra[i])

		if matchIdx < len(es.MatchLenCodes) {
			seqs[i].MatchLen = decodeMatchLen(es.MatchLenCodes[matchIdx], es.MatchLenExtra[matchIdx]) + lz77MinMatch
			seqs[i].Offset = decodeOffset(es.OffsetCodes[matchIdx], es.OffsetExtra[matchIdx])
			matchIdx++
		}
	}

	return seqs
}

// ---- Literal Length Encoding ----

func encodeLitLen(val int) (code byte, extra uint32, nbits int) {
	if val <= 15 {
		return byte(val), 0, 0
	}
	baseline := 0
	for i := 16; i < len(litLenTable); i++ {
		prevMax := litLenTable[i-1].maxVal
		baseline = prevMax + 1
		if val <= litLenTable[i].maxVal {
			return byte(i), uint32(val - baseline), litLenTable[i].extraBits
		}
	}
	// Overflow: use the last code
	last := len(litLenTable) - 1
	prevMax := litLenTable[last-1].maxVal
	return byte(last), uint32(val - prevMax - 1), litLenTable[last].extraBits
}

func decodeLitLen(code byte, extra uint32) int {
	c := int(code)
	if c <= 15 {
		return c
	}
	if c >= len(litLenTable) {
		c = len(litLenTable) - 1
	}
	baseline := litLenTable[c-1].maxVal + 1
	return baseline + int(extra)
}

// ---- Match Length Encoding ----

func encodeMatchLen(val int) (code byte, extra uint32, nbits int) {
	if val <= 15 {
		return byte(val), 0, 0
	}
	baseline := 0
	for i := 16; i < len(matchLenTable); i++ {
		prevMax := matchLenTable[i-1].maxVal
		baseline = prevMax + 1
		if val <= matchLenTable[i].maxVal {
			return byte(i), uint32(val - baseline), matchLenTable[i].extraBits
		}
	}
	last := len(matchLenTable) - 1
	prevMax := matchLenTable[last-1].maxVal
	return byte(last), uint32(val - prevMax - 1), matchLenTable[last].extraBits
}

func decodeMatchLen(code byte, extra uint32) int {
	c := int(code)
	if c <= 15 {
		return c
	}
	if c >= len(matchLenTable) {
		c = len(matchLenTable) - 1
	}
	baseline := matchLenTable[c-1].maxVal + 1
	return baseline + int(extra)
}

// ---- Offset Encoding ----
// Offsets use exponential coding: code = floor(log2(offset)), extra = offset - 2^code
// Code 0 = offset 1 (no extra bits)
// Code 1 = offsets 2-3 (1 extra bit)
// Code N = offsets [2^N, 2^(N+1)) (N extra bits)

func encodeOffset(offset int) (code byte, extra uint32, nbits int) {
	if offset <= 0 {
		return 0, 0, 0
	}
	if offset == 1 {
		return 0, 0, 0
	}
	// code = floor(log2(offset))
	c := 0
	v := offset
	for v > 1 {
		v >>= 1
		c++
	}
	baseline := 1 << c
	return byte(c), uint32(offset - baseline), c
}

func decodeOffset(code byte, extra uint32) int {
	c := int(code)
	if c == 0 {
		return 1
	}
	baseline := 1 << c
	return baseline + int(extra)
}
