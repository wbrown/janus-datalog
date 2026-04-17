package codec

// LZ77 match finder with hash-chain and lazy matching.
// Produces Sequence blocks that the FSE entropy coder compresses.

const (
	lz77HashBits   = 15
	lz77HashSize   = 1 << lz77HashBits
	lz77HashMask   = lz77HashSize - 1
	lz77MinMatch   = 3
	lz77MaxMatch   = 258
	lz77MaxChain   = 64        // limit chain traversal to avoid O(n²)
	lz77WindowSize = 1 << 20   // 1MB default window
	lz77NoPos      = int32(-1) // sentinel for empty hash slot
)

// Sequence represents one LZ77 output unit: a run of literal bytes
// followed by an optional back-reference match.
type Sequence struct {
	LitLen   int // number of literal bytes before this match
	Offset   int // back-reference distance (0 = no match, literals only)
	MatchLen int // match length (>= lz77MinMatch when Offset > 0)
}

// SequenceBlock holds the output of LZ77 compression: literal bytes
// and the sequence of (litLen, offset, matchLen) triples.
type SequenceBlock struct {
	Literals  []byte     // all literal bytes concatenated
	Sequences []Sequence // each sequence consumes LitLen literals then copies MatchLen from Offset
}

// lz77Encoder holds the working state for LZ77 match finding.
type lz77Encoder struct {
	hashTable [lz77HashSize]int32 // hash → most recent position
	chain     []int32             // chain[pos] = previous position with same hash
	window    int
}

// hash4 computes a 4-byte hash for match finding.
func hash4(b []byte) uint32 {
	v := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return (v * 2654435761) >> (32 - lz77HashBits)
}

// FindMatches runs LZ77 match finding on input and returns a SequenceBlock.
func FindMatches(input []byte) *SequenceBlock {
	if len(input) < lz77MinMatch {
		// Too short for any matches
		return &SequenceBlock{
			Literals:  append([]byte(nil), input...),
			Sequences: []Sequence{{LitLen: len(input)}},
		}
	}

	enc := &lz77Encoder{
		chain:  make([]int32, len(input)),
		window: lz77WindowSize,
	}
	// Initialize hash table to no-position
	for i := range enc.hashTable {
		enc.hashTable[i] = lz77NoPos
	}

	sb := &SequenceBlock{
		Literals:  make([]byte, 0, len(input)),
		Sequences: make([]Sequence, 0, len(input)/16),
	}

	litStart := 0 // start of current literal run
	pos := 0
	end := len(input) - 3 // need at least 4 bytes for hash4

	for pos < end {
		// Find best match at current position
		bestLen, bestOff := enc.findBestMatch(input, pos)

		// Lazy matching: check if next position has a better match
		if bestLen >= lz77MinMatch && pos+1 < end {
			nextLen, nextOff := enc.findBestMatch(input, pos+1)
			if nextLen > bestLen+1 {
				// Next position is significantly better — emit current as literal
				enc.updateHash(input, pos)
				sb.Literals = append(sb.Literals, input[pos])
				pos++
				bestLen = nextLen
				bestOff = nextOff
			}
		}

		if bestLen >= lz77MinMatch {
			// Emit sequence: literals + match
			litLen := pos - litStart
			sb.Sequences = append(sb.Sequences, Sequence{
				LitLen:   litLen,
				Offset:   bestOff,
				MatchLen: bestLen,
			})

			// Update hash table for all positions in the match
			for i := 0; i < bestLen; i++ {
				if pos+i < end {
					enc.updateHash(input, pos+i)
				}
			}
			pos += bestLen
			litStart = pos
		} else {
			// No match — advance one byte
			enc.updateHash(input, pos)
			sb.Literals = append(sb.Literals, input[pos])
			pos++
		}
	}

	// Emit remaining bytes as literals
	for pos < len(input) {
		sb.Literals = append(sb.Literals, input[pos])
		pos++
	}

	// Final sequence for trailing literals (if any)
	trailingLits := len(input) - litStart
	if trailingLits > 0 {
		if len(sb.Sequences) == 0 || sb.Sequences[len(sb.Sequences)-1].Offset > 0 {
			// Need a new sequence for trailing literals
			sb.Sequences = append(sb.Sequences, Sequence{LitLen: trailingLits})
		} else {
			// Last sequence already has no match — add to it
			sb.Sequences[len(sb.Sequences)-1].LitLen += trailingLits
		}
	}

	return sb
}

// findBestMatch finds the longest match at pos using the hash chain.
func (enc *lz77Encoder) findBestMatch(input []byte, pos int) (bestLen, bestOff int) {
	if pos+4 > len(input) {
		return 0, 0
	}

	h := hash4(input[pos:])
	candidate := enc.hashTable[h]

	chainLen := 0
	minPos := pos - enc.window
	if minPos < 0 {
		minPos = 0
	}

	for candidate != lz77NoPos && int(candidate) >= minPos && chainLen < lz77MaxChain {
		cpos := int(candidate)

		// Verify match and measure length
		matchLen := matchLength(input, cpos, pos)
		if matchLen >= lz77MinMatch && matchLen > bestLen {
			bestLen = matchLen
			bestOff = pos - cpos
			if bestLen >= lz77MaxMatch {
				break // can't do better
			}
		}

		candidate = enc.chain[cpos]
		chainLen++
	}

	return bestLen, bestOff
}

// matchLength measures how many bytes match starting at positions a and b.
func matchLength(input []byte, a, b int) int {
	maxLen := len(input) - b
	if maxLen > lz77MaxMatch {
		maxLen = lz77MaxMatch
	}
	n := 0
	for n < maxLen && input[a+n] == input[b+n] {
		n++
	}
	return n
}

// updateHash inserts pos into the hash chain.
func (enc *lz77Encoder) updateHash(input []byte, pos int) {
	if pos+4 > len(input) {
		return
	}
	h := hash4(input[pos:])
	enc.chain[pos] = enc.hashTable[h]
	enc.hashTable[h] = int32(pos)
}

// Reconstruct rebuilds the original input from a SequenceBlock.
// This is the decode side of LZ77 — used by the decompressor.
func Reconstruct(sb *SequenceBlock) []byte {
	// Estimate output size from literals + match lengths
	size := len(sb.Literals)
	for _, seq := range sb.Sequences {
		size += seq.MatchLen
	}
	out := make([]byte, 0, size)
	litPos := 0

	for _, seq := range sb.Sequences {
		// Copy literal bytes
		if seq.LitLen > 0 {
			out = append(out, sb.Literals[litPos:litPos+seq.LitLen]...)
			litPos += seq.LitLen
		}

		// Copy match (back-reference)
		if seq.MatchLen > 0 && seq.Offset > 0 {
			matchStart := len(out) - seq.Offset
			// Byte-by-byte copy handles overlapping matches (e.g., offset=1 repeats last byte)
			for i := 0; i < seq.MatchLen; i++ {
				out = append(out, out[matchStart+i])
			}
		}
	}

	return out
}

