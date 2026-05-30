package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Compression version. This byte is embedded in every compressed output.
// If the algorithm changes, increment this. The decompressor rejects
// unknown versions. The format is frozen per version — same input must
// always produce same output for a given version.
//
// v1 (0x01): sequence/match counts are uint16 — overflowed and silently
//   truncated above 65535 sequences, producing unreadable blobs for large,
//   compressible, densely-structured values. Read-only now; never written.
// v2 (0x02): sequence/match counts widened to uint32. Decompress reads both
//   versions; Compress always writes v2.
const CompressionVersion byte = 0x02

// maxSequenceCount is the largest sequence/match count the v2 header can encode
// (uint32). Compress declines (returns nil → value stored raw) above this rather
// than truncating the count and writing a blob that later fails to decompress.
// Unreachable for any real value — each sequence needs ≥3 bytes, so exceeding
// this implies a >12 GB input — it is a belt-and-suspenders invariant against a
// future header-width regression, not a limit reached in practice.
const maxSequenceCount = 0xFFFFFFFF

// Compress compresses input using LZ77 + FSE.
// Returns compressed bytes with header, or nil if compression provides
// no size benefit (safety net).
func Compress(input []byte) []byte {
	if len(input) == 0 {
		return nil
	}

	// LZ77 match finding
	sb := FindMatches(input)

	// Encode sequences into parallel streams
	es := EncodeSequences(sb.Sequences)

	// Count matches (sequences with Offset > 0)
	numMatches := 0
	for _, seq := range sb.Sequences {
		if seq.Offset > 0 {
			numMatches++
		}
	}

	// Correctness guard: the v2 header encodes both counts as uint32. If a value
	// ever produced more sequences/matches than that, the count would truncate on
	// write and the blob would be unrecoverable on read. Decline instead so the
	// caller stores the value raw (Compress == nil ⇒ uncompressed). Unreachable in
	// practice; see maxSequenceCount.
	if len(sb.Sequences) > maxSequenceCount || numMatches > maxSequenceCount {
		return nil
	}

	// Build extra bits bitstream
	extraBW := &bitWriter{}
	matchIdx := 0
	for i, seq := range sb.Sequences {
		if es.LitLenBits[i] > 0 {
			extraBW.writeBits(es.LitLenExtra[i], es.LitLenBits[i])
		}
		if seq.Offset > 0 && matchIdx < len(es.MatchLenBits) {
			if es.MatchLenBits[matchIdx] > 0 {
				extraBW.writeBits(es.MatchLenExtra[matchIdx], es.MatchLenBits[matchIdx])
			}
			if es.OffsetBits[matchIdx] > 0 {
				extraBW.writeBits(es.OffsetExtra[matchIdx], es.OffsetBits[matchIdx])
			}
			matchIdx++
		}
	}
	extraBits := extraBW.flush()

	// FSE compress each stream (falls back to raw if no benefit)
	litBlock := compressOrRaw(sb.Literals)
	litLenBlock := compressOrRaw(es.LitLenCodes)
	matchLenBlock := compressOrRaw(es.MatchLenCodes)
	offsetBlock := compressOrRaw(es.OffsetCodes)
	extraBlock := rawBlock(extraBits)

	// Assemble output
	// Header (v2): [version:1][originalLen:4 BE][numSequences:4][numMatches:4][numLiterals:4]
	result := make([]byte, 0, len(input))
	result = append(result, CompressionVersion)
	result = binary.BigEndian.AppendUint32(result, uint32(len(input)))
	result = binary.BigEndian.AppendUint32(result, uint32(len(sb.Sequences)))
	result = binary.BigEndian.AppendUint32(result, uint32(numMatches))
	result = binary.BigEndian.AppendUint32(result, uint32(len(sb.Literals)))

	// Blocks
	result = append(result, litBlock...)
	result = append(result, litLenBlock...)
	result = append(result, matchLenBlock...)
	result = append(result, offsetBlock...)
	result = append(result, extraBlock...)

	// Safety net: compressed must be strictly smaller
	if len(result) >= len(input) {
		return nil
	}

	return result
}

// Decompress decompresses data produced by Compress. It reads both header
// formats: v1 (uint16 sequence/match counts, 13-byte header) and v2 (uint32
// counts, 17-byte header). The version-dispatch below runs once per value,
// outside the per-byte decode loop, so the dual-read path costs nothing in the
// hot path. Compress only ever writes v2.
func Decompress(compressed []byte) ([]byte, error) {
	if len(compressed) < 1 {
		return nil, errors.New("compress: data too short")
	}

	// Parse header — field widths and offsets depend on the version byte.
	version := compressed[0]
	var originalLen, numSequences, numMatches, numLiterals, pos int
	switch version {
	case 1:
		if len(compressed) < 13 { // minimum v1 header
			return nil, errors.New("compress: data too short")
		}
		originalLen = int(binary.BigEndian.Uint32(compressed[1:5]))
		numSequences = int(binary.BigEndian.Uint16(compressed[5:7]))
		numMatches = int(binary.BigEndian.Uint16(compressed[7:9]))
		numLiterals = int(binary.BigEndian.Uint32(compressed[9:13]))
		pos = 13
	case 2:
		if len(compressed) < 17 { // minimum v2 header
			return nil, errors.New("compress: data too short")
		}
		originalLen = int(binary.BigEndian.Uint32(compressed[1:5]))
		numSequences = int(binary.BigEndian.Uint32(compressed[5:9]))
		numMatches = int(binary.BigEndian.Uint32(compressed[9:13]))
		numLiterals = int(binary.BigEndian.Uint32(compressed[13:17]))
		pos = 17
	default:
		return nil, fmt.Errorf("compress: unsupported version %d (expected 1 or 2)", version)
	}

	// Read and decompress blocks
	literals, n, err := readDecompressBlock(compressed, pos, numLiterals)
	if err != nil {
		return nil, fmt.Errorf("compress: literals: %w", err)
	}
	pos += n

	litLenCodes, n, err := readDecompressBlock(compressed, pos, numSequences)
	if err != nil {
		return nil, fmt.Errorf("compress: litLen codes: %w", err)
	}
	pos += n

	matchLenCodes, n, err := readDecompressBlock(compressed, pos, numMatches)
	if err != nil {
		return nil, fmt.Errorf("compress: matchLen codes: %w", err)
	}
	pos += n

	offsetCodes, n, err := readDecompressBlock(compressed, pos, numMatches)
	if err != nil {
		return nil, fmt.Errorf("compress: offset codes: %w", err)
	}
	pos += n

	extraBitsData, n, err := readRawBlock(compressed, pos)
	if err != nil {
		return nil, fmt.Errorf("compress: extra bits: %w", err)
	}
	_ = n

	// Validate stream lengths
	if len(litLenCodes) != numSequences {
		return nil, fmt.Errorf("compress: litLen count %d != numSequences %d", len(litLenCodes), numSequences)
	}
	if len(matchLenCodes) != numMatches {
		return nil, fmt.Errorf("compress: matchLen count %d != numMatches %d", len(matchLenCodes), numMatches)
	}
	if len(offsetCodes) != numMatches {
		return nil, fmt.Errorf("compress: offset count %d != numMatches %d", len(offsetCodes), numMatches)
	}

	// Decode sequences from codes + extra bits
	extraBR := newBitReader(extraBitsData)
	matchIdx := 0

	sequences := make([]Sequence, numSequences)
	for i := 0; i < numSequences; i++ {
		// Decode litLen
		llCode := litLenCodes[i]
		_, _, llNBits := encodeLitLen(decodeLitLen(llCode, 0))
		llExtra := uint32(0)
		if llNBits > 0 {
			llExtra = extraBR.readBits(llNBits)
		}
		sequences[i].LitLen = decodeLitLen(llCode, llExtra)

		// Decode match (if this sequence has one)
		if matchIdx < numMatches {
			mlCode := matchLenCodes[matchIdx]
			_, _, mlNBits := encodeMatchLen(decodeMatchLen(mlCode, 0))
			mlExtra := uint32(0)
			if mlNBits > 0 {
				mlExtra = extraBR.readBits(mlNBits)
			}
			sequences[i].MatchLen = decodeMatchLen(mlCode, mlExtra) + lz77MinMatch

			offCode := offsetCodes[matchIdx]
			offNBits := int(offCode) // offset code N has N extra bits
			offExtra := uint32(0)
			if offNBits > 0 {
				offExtra = extraBR.readBits(offNBits)
			}
			sequences[i].Offset = decodeOffset(offCode, offExtra)
			matchIdx++
		}
	}

	// Reconstruct via LZ77
	result := Reconstruct(&SequenceBlock{
		Literals:  literals,
		Sequences: sequences,
	})

	if len(result) != originalLen {
		return nil, fmt.Errorf("compress: output length %d != expected %d", len(result), originalLen)
	}

	return result, nil
}

// CompressedHeader parses just the header without decompressing.
// Returns version and original length.
func CompressedHeader(data []byte) (version byte, originalLen int, err error) {
	if len(data) < 5 {
		return 0, 0, errors.New("compress: header too short")
	}
	return data[0], int(binary.BigEndian.Uint32(data[1:5])), nil
}

// ---- Block format ----
//
// Each block:
//   [originalLen:4 BE]     -- decompressed byte count
//   [flag:1]               -- 0x00 = FSE compressed, 0x01 = raw
//   [dataLen:4 BE]         -- bytes of data following
//   [data:dataLen]         -- FSE compressed bytes or raw bytes
//
// Total overhead per block: 9 bytes + data

// compressOrRaw FSE-compresses data, or returns a raw block if FSE doesn't help.
func compressOrRaw(data []byte) []byte {
	if len(data) == 0 {
		return rawBlock(nil)
	}

	compressed, err := FSECompress(data)
	if err != nil || compressed == nil {
		return rawBlock(data)
	}

	// FSE block
	buf := make([]byte, 0, 9+len(compressed))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
	buf = append(buf, 0x00) // FSE flag
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(compressed)))
	buf = append(buf, compressed...)
	return buf
}

// rawBlock creates a raw (uncompressed) block.
func rawBlock(data []byte) []byte {
	buf := make([]byte, 0, 9+len(data))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
	buf = append(buf, 0x01) // raw flag
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(data)))
	buf = append(buf, data...)
	return buf
}

// readDecompressBlock reads a block, FSE-decompresses if needed, and returns
// the decompressed bytes. expectedLen is the expected decompressed length.
func readDecompressBlock(data []byte, pos int, expectedLen int) ([]byte, int, error) {
	if pos+9 > len(data) {
		return nil, 0, errors.New("block header truncated")
	}

	origLen := int(binary.BigEndian.Uint32(data[pos:]))
	flag := data[pos+4]
	dataLen := int(binary.BigEndian.Uint32(data[pos+5:]))
	consumed := 9 + dataLen

	if pos+consumed > len(data) {
		return nil, 0, errors.New("block data truncated")
	}

	blockData := data[pos+9 : pos+9+dataLen]

	if flag == 0x01 {
		// Raw block — return data as-is
		if origLen != dataLen {
			return nil, 0, fmt.Errorf("raw block size mismatch: origLen=%d dataLen=%d", origLen, dataLen)
		}
		result := make([]byte, dataLen)
		copy(result, blockData)
		return result, consumed, nil
	}

	if flag != 0x00 {
		return nil, 0, fmt.Errorf("unknown block flag 0x%02x", flag)
	}

	// FSE compressed — decompress
	decompressed, err := FSEDecompress(blockData, origLen)
	if err != nil {
		return nil, 0, fmt.Errorf("FSE decompress: %w", err)
	}
	if len(decompressed) != expectedLen {
		return nil, 0, fmt.Errorf("decompressed length %d != expected %d", len(decompressed), expectedLen)
	}

	return decompressed, consumed, nil
}

// readRawBlock reads a block without FSE decompression (for extra bits).
func readRawBlock(data []byte, pos int) ([]byte, int, error) {
	if pos+9 > len(data) {
		return nil, 0, errors.New("block header truncated")
	}

	_ = binary.BigEndian.Uint32(data[pos:]) // origLen (same as dataLen for raw)
	flag := data[pos+4]
	dataLen := int(binary.BigEndian.Uint32(data[pos+5:]))
	consumed := 9 + dataLen

	if pos+consumed > len(data) {
		return nil, 0, errors.New("block data truncated")
	}

	_ = flag // extra bits block is always raw
	if dataLen == 0 {
		return nil, consumed, nil
	}
	result := make([]byte, dataLen)
	copy(result, data[pos+9:pos+9+dataLen])
	return result, consumed, nil
}
