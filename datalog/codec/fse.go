// Package codec provides encoding utilities for janus-datalog storage.
//
// FSE (Finite State Entropy) implements tANS (table-based Asymmetric Numeral
// Systems) for near-optimal entropy coding. This is a custom, owned
// implementation for deterministic compression in storage keys.
//
// The implementation is frozen: same input must always produce same output,
// forever. Do not change the algorithm without updating the compression
// version byte.
package codec

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sync"
)

// FSE constants
const (
	fseMaxTableLog     = 12
	fseMinTableLog     = 5
	fseDefaultTableLog = 8 // good default for byte-alphabet data
	fseMaxSymbolValue  = 255
)

// ---- Bit I/O ----

// bitWriter accumulates bits LSB-first and flushes to a byte slice.
type bitWriter struct {
	buf   uint64
	nbits int // valid bits in buf (0-63)
	out   []byte
}

// writeBits writes the low nbits of value to the stream, LSB first.
func (w *bitWriter) writeBits(value uint32, nbits int) {
	w.buf |= uint64(value) << uint(w.nbits)
	w.nbits += nbits
	for w.nbits >= 8 {
		w.out = append(w.out, byte(w.buf))
		w.buf >>= 8
		w.nbits -= 8
	}
}

// flush writes any remaining partial byte and returns the output.
func (w *bitWriter) flush() []byte {
	if w.nbits > 0 {
		w.out = append(w.out, byte(w.buf))
	}
	return w.out
}

// bitReader reads bits LSB-first from a byte slice.
type bitReader struct {
	data  []byte
	buf   uint64
	nbits int // valid bits in buf
	pos   int // next byte to read from data
}

func newBitReader(data []byte) *bitReader {
	r := &bitReader{data: data}
	r.refill()
	return r
}

// refill loads more bytes into the buffer.
func (r *bitReader) refill() {
	for r.nbits <= 56 && r.pos < len(r.data) {
		r.buf |= uint64(r.data[r.pos]) << uint(r.nbits)
		r.nbits += 8
		r.pos++
	}
}

// readBits reads nbits from the stream, LSB first.
func (r *bitReader) readBits(nbits int) uint32 {
	if nbits == 0 {
		return 0
	}
	val := uint32(r.buf) & ((1 << uint(nbits)) - 1)
	r.buf >>= uint(nbits)
	r.nbits -= nbits
	r.refill()
	return val
}

// bitsRemaining returns the number of unread bits.
func (r *bitReader) bitsRemaining() int {
	return r.nbits + (len(r.data)-r.pos)*8
}

// ---- Count Normalization ----

// normalizeCount converts raw symbol frequencies to normalized counts that
// sum to exactly tableSize = 1 << tableLog. Every symbol with non-zero
// raw count gets at least 1 in the normalized output.
func normalizeCount(counts []int, tableLog int) ([]int16, int, error) {
	tableSize := 1 << tableLog

	// Find total and max symbol
	total := 0
	maxSymbol := 0
	nonZero := 0
	for i, c := range counts {
		if c > 0 {
			total += c
			maxSymbol = i
			nonZero++
		}
	}
	if total == 0 {
		return nil, 0, errors.New("fse: no symbols")
	}
	if nonZero == 1 {
		// Single symbol: special case
		norm := make([]int16, maxSymbol+1)
		norm[maxSymbol] = int16(tableSize)
		return norm, maxSymbol, nil
	}
	if nonZero > tableSize {
		return nil, 0, fmt.Errorf("fse: too many symbols (%d) for tableLog %d", nonZero, tableLog)
	}

	norm := make([]int16, maxSymbol+1)

	// Two-pass allocation:
	// Pass 1: Give every non-zero symbol max(1, floor(count * tableSize / total)).
	// Pass 2: Distribute remaining capacity to symbols proportionally, largest first.
	assigned := 0
	largestSymbol := -1
	largestCount := 0

	for i := 0; i <= maxSymbol; i++ {
		if counts[i] == 0 {
			continue
		}
		n := counts[i] * tableSize / total // floor division
		if n < 1 {
			n = 1
		}
		norm[i] = int16(n)
		assigned += n

		if counts[i] > largestCount {
			largestCount = counts[i]
			largestSymbol = i
		}
	}

	// Distribute remaining slots (or reclaim excess) via the largest symbol
	diff := tableSize - assigned
	norm[largestSymbol] += int16(diff)

	// If the largest symbol was driven below 1 (too many symbols forced to 1),
	// steal from the next-largest symbols to fix it.
	if norm[largestSymbol] < 1 {
		// Sort non-zero symbols by their norm descending, steal from them
		type symCount struct {
			sym   int
			count int16
		}
		var syms []symCount
		for i := 0; i <= maxSymbol; i++ {
			if norm[i] > 1 && i != largestSymbol {
				syms = append(syms, symCount{i, norm[i]})
			}
		}
		// Sort by count descending so we steal from the richest first
		for i := 0; i < len(syms)-1; i++ {
			for j := i + 1; j < len(syms); j++ {
				if syms[j].count > syms[i].count {
					syms[i], syms[j] = syms[j], syms[i]
				}
			}
		}
		for _, sc := range syms {
			if norm[largestSymbol] >= 1 {
				break
			}
			steal := int16(1) // steal 1 at a time
			if norm[sc.sym]-steal < 1 {
				continue
			}
			norm[sc.sym] -= steal
			norm[largestSymbol] += steal
		}
		if norm[largestSymbol] < 1 {
			return nil, 0, errors.New("fse: normalization failed - cannot fit all symbols")
		}
	}

	// Verify sum
	sum := 0
	for i := 0; i <= maxSymbol; i++ {
		sum += int(norm[i])
		if norm[i] < 0 {
			return nil, 0, fmt.Errorf("fse: negative normalized count for symbol %d", i)
		}
	}
	if sum != tableSize {
		return nil, 0, fmt.Errorf("fse: normalized sum %d != tableSize %d", sum, tableSize)
	}

	return norm, maxSymbol, nil
}

// ---- Table Construction ----

// fseDecodeEntry is one entry in the FSE decode table.
type fseDecodeEntry struct {
	newState uint16
	symbol   uint8
	nbBits   uint8
}

// symbolTransform holds precomputed encoding parameters for one symbol.
type symbolTransform struct {
	deltaNbBits    uint32 // (maxBitsOut << 16) - minStatePlus
	deltaFindState int32  // offset into stateTable
}

// fseTable holds both encode and decode tables for FSE.
type fseTable struct {
	tableLog    int
	maxSymbol   int
	normCounts  []int16           // normalized counts per symbol
	decodeTable []fseDecodeEntry  // indexed by state [0, tableSize)
	stateTable  []uint16          // encode: maps reduced state + offset → full state
	symbolTT    []symbolTransform // encode: per-symbol transform parameters
}

// buildFSETable builds both encode and decode tables from normalized counts.
func buildFSETable(normCounts []int16, maxSymbol int, tableLog int) (*fseTable, error) {
	tableSize := 1 << tableLog

	// Verify counts sum to tableSize
	sum := 0
	for i := 0; i <= maxSymbol; i++ {
		sum += int(normCounts[i])
	}
	if sum != tableSize {
		return nil, fmt.Errorf("fse: counts sum %d != tableSize %d", sum, tableSize)
	}

	t := &fseTable{
		tableLog:   tableLog,
		maxSymbol:  maxSymbol,
		normCounts: normCounts,
	}

	// Step 1: Build spread table
	spread := make([]byte, tableSize)
	step := (tableSize >> 1) + (tableSize >> 3) + 3
	mask := tableSize - 1
	pos := 0
	for s := 0; s <= maxSymbol; s++ {
		for i := 0; i < int(normCounts[s]); i++ {
			spread[pos] = byte(s)
			pos = (pos + step) & mask
		}
	}

	// Step 2: Build decode table
	t.decodeTable = make([]fseDecodeEntry, tableSize)
	symbolCounter := make([]int, maxSymbol+1)
	for s := 0; s <= maxSymbol; s++ {
		symbolCounter[s] = int(normCounts[s])
	}

	for x := 0; x < tableSize; x++ {
		s := spread[x]
		nextState := symbolCounter[s]
		symbolCounter[s]++

		// highBit of nextState
		highBit := bits.Len(uint(nextState)) - 1
		nbBits := tableLog - highBit
		newState := uint16((nextState << uint(nbBits)) - tableSize)

		t.decodeTable[x] = fseDecodeEntry{
			newState: newState,
			symbol:   s,
			nbBits:   uint8(nbBits),
		}
	}

	// Step 3: Build encode state table
	// For each position u in spread, stateTable[cumul[s]++] = tableSize + u
	t.stateTable = make([]uint16, tableSize)
	cumul := make([]int, maxSymbol+2)
	for s := 0; s <= maxSymbol; s++ {
		cumul[s+1] = cumul[s] + int(normCounts[s])
	}
	// working copy of cumulative counters
	cumulCopy := make([]int, maxSymbol+2)
	copy(cumulCopy, cumul)

	for u := 0; u < tableSize; u++ {
		s := spread[u]
		t.stateTable[cumulCopy[s]] = uint16(tableSize + u)
		cumulCopy[s]++
	}

	// Step 4: Build symbolTT (encode parameters)
	t.symbolTT = make([]symbolTransform, maxSymbol+1)
	cumulRunning := 0
	for s := 0; s <= maxSymbol; s++ {
		count := int(normCounts[s])
		if count == 0 {
			// Unused: deltaNbBits won't be used, but set to safe value
			t.symbolTT[s].deltaNbBits = (uint32(tableLog) << 16) + uint32(tableSize)
			t.symbolTT[s].deltaFindState = int32(-cumulRunning)
			continue
		}

		if count == 1 {
			t.symbolTT[s].deltaNbBits = (uint32(tableLog) << 16) - uint32(tableSize)
			t.symbolTT[s].deltaFindState = int32(cumulRunning - 1)
		} else {
			maxBitsOut := tableLog - (bits.Len(uint(count-1)) - 1)
			minStatePlus := count << uint(maxBitsOut)
			t.symbolTT[s].deltaNbBits = (uint32(maxBitsOut) << 16) - uint32(minStatePlus)
			t.symbolTT[s].deltaFindState = int32(cumulRunning - count)
		}
		cumulRunning += count
	}

	return t, nil
}

// ---- Table Cache ----

// fseTableCache caches decoded FSE tables by their serialized header bytes.
// The decode path hits this on every FSEDecompress call, avoiding full table
// reconstruction for repeated table descriptions (common when many values
// share similar byte distributions).
var fseTableCache sync.Map // map[string]*fseTable

// getCachedTable looks up or builds an FSE table from serialized header bytes.
func getCachedTable(headerBytes []byte) (*fseTable, error) {
	key := string(headerBytes) // safe: used as map key only

	if cached, ok := fseTableCache.Load(key); ok {
		return cached.(*fseTable), nil
	}

	// Build fresh table
	t, _, err := deserializeTableDirect(headerBytes)
	if err != nil {
		return nil, err
	}

	// Cache it (Store is idempotent — concurrent builds are fine)
	fseTableCache.Store(key, t)
	return t, nil
}

// ---- Encode Working Memory Pool ----

type encodeWork struct {
	pairs []struct {
		value uint32
		nbits int
	}
}

var encodeWorkPool = sync.Pool{
	New: func() interface{} {
		return &encodeWork{
			pairs: make([]struct {
				value uint32
				nbits int
			}, 0, 1024),
		}
	},
}

// ---- Table Serialization ----

// serializeTable writes the FSE table description to bytes.
// Format: [tableLog:1][maxSymbol:1][count0:2][count1:2]...[countN:2]
func serializeTable(t *fseTable) []byte {
	buf := make([]byte, 2+2*(t.maxSymbol+1))
	buf[0] = byte(t.tableLog)
	buf[1] = byte(t.maxSymbol)
	for i := 0; i <= t.maxSymbol; i++ {
		binary.LittleEndian.PutUint16(buf[2+i*2:], uint16(t.normCounts[i]))
	}
	return buf
}

// fseHeaderSize returns the header size for the given data, or an error.
func fseHeaderSize(data []byte) (int, error) {
	if len(data) < 2 {
		return 0, errors.New("fse: table header too short")
	}
	tableLog := int(data[0])
	maxSymbol := int(data[1])
	if tableLog < fseMinTableLog || tableLog > fseMaxTableLog {
		return 0, fmt.Errorf("fse: invalid tableLog %d", tableLog)
	}
	if maxSymbol > fseMaxSymbolValue {
		return 0, fmt.Errorf("fse: invalid maxSymbol %d", maxSymbol)
	}
	headerSize := 2 + 2*(maxSymbol+1)
	if len(data) < headerSize {
		return 0, errors.New("fse: table data too short")
	}
	return headerSize, nil
}

// deserializeTableDirect builds an fseTable from serialized header bytes.
// This is the uncached path — use getCachedTable for the hot path.
func deserializeTableDirect(data []byte) (*fseTable, int, error) {
	headerSize, err := fseHeaderSize(data)
	if err != nil {
		return nil, 0, err
	}
	tableLog := int(data[0])
	maxSymbol := int(data[1])

	normCounts := make([]int16, maxSymbol+1)
	for i := 0; i <= maxSymbol; i++ {
		normCounts[i] = int16(binary.LittleEndian.Uint16(data[2+i*2:]))
	}

	t, err := buildFSETable(normCounts, maxSymbol, tableLog)
	if err != nil {
		return nil, 0, err
	}
	return t, headerSize, nil
}

// deserializeTable reads an FSE table description from bytes (cached).
func deserializeTable(data []byte) (*fseTable, int, error) {
	headerSize, err := fseHeaderSize(data)
	if err != nil {
		return nil, 0, err
	}

	t, err := getCachedTable(data[:headerSize])
	if err != nil {
		return nil, 0, err
	}
	return t, headerSize, nil
}

// ---- Compress ----

// FSECompress compresses a byte slice using Finite State Entropy coding.
// Returns compressed bytes including the table description, or nil if
// compression provides no size benefit.
func FSECompress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}

	// Count symbol frequencies
	var counts [256]int
	for _, b := range src {
		counts[b]++
	}

	// Choose tableLog based on input size and symbol count
	tableLog := chooseTableLog(len(src), &counts)

	// Normalize counts
	normCounts, maxSymbol, err := normalizeCount(counts[:], tableLog)
	if err != nil {
		return nil, err
	}

	// Check for single symbol (RLE case)
	nonZero := 0
	for _, c := range normCounts {
		if c > 0 {
			nonZero++
		}
	}
	if nonZero == 1 {
		// RLE: compact encoding [0xFF marker][symbol:1][length:4] = 6 bytes
		var rleSymbol byte
		for i, c := range normCounts {
			if c > 0 {
				rleSymbol = byte(i)
				break
			}
		}
		result := make([]byte, 6)
		result[0] = 0xFF // RLE marker (invalid as tableLog, signals RLE)
		result[1] = rleSymbol
		binary.LittleEndian.PutUint32(result[2:], uint32(len(src)))
		if len(result) >= len(src) {
			return nil, nil // no benefit
		}
		return result, nil
	}

	// Build tables
	t, err := buildFSETable(normCounts, maxSymbol, tableLog)
	if err != nil {
		return nil, err
	}

	// Encode symbols in reverse order, collecting bit pairs
	tableSize := 1 << tableLog
	state := tableSize // initial encoder state

	work := encodeWorkPool.Get().(*encodeWork)
	work.pairs = work.pairs[:0] // reset length, keep capacity

	for i := len(src) - 1; i >= 0; i-- {
		s := src[i]

		// Compute number of bits to output
		nbBitsOut := int((uint32(state) + t.symbolTT[s].deltaNbBits) >> 16)

		// Record bits to output
		bitsValue := uint32(state) & ((1 << uint(nbBitsOut)) - 1)
		work.pairs = append(work.pairs, struct {
			value uint32
			nbits int
		}{bitsValue, nbBitsOut})

		// Transition state
		reducedState := state >> uint(nbBitsOut)
		state = int(t.stateTable[int32(reducedState)+t.symbolTT[s].deltaFindState])
	}

	// Build output: [tableHeader][initialState:tableLog bits][bits in decoder order][sentinel]
	header := serializeTable(t)

	bw := &bitWriter{out: make([]byte, 0, len(header)+len(src))}
	// Pre-write header so we assemble in one buffer
	bw.out = append(bw.out, header...)

	// Write initial state for decoder (encoder's final state, minus tableSize)
	bw.writeBits(uint32(state-tableSize), tableLog)

	// Write bit pairs in REVERSE order (so decoder reads them forward)
	pairs := work.pairs
	for i := len(pairs) - 1; i >= 0; i-- {
		if pairs[i].nbits > 0 {
			bw.writeBits(pairs[i].value, pairs[i].nbits)
		}
	}

	// Return pooled work memory
	encodeWorkPool.Put(work)

	// Write sentinel bit (1 followed by zero padding to byte boundary)
	// Decoder finds the highest set bit in the last byte to determine data end.
	bw.writeBits(1, 1)
	result := bw.flush()

	if len(result) >= len(src) {
		return nil, nil // no compression benefit
	}

	return result, nil
}

// chooseTableLog picks an appropriate tableLog for the given input.
func chooseTableLog(srcLen int, counts *[256]int) int {
	// Count non-zero symbols
	nonZero := 0
	for _, c := range counts {
		if c > 0 {
			nonZero++
		}
	}

	// tableLog should be at least log2(nonZero) + 2
	minLog := bits.Len(uint(nonZero-1)) + 2
	if minLog < fseMinTableLog {
		minLog = fseMinTableLog
	}

	// For small inputs, use smaller tableLog
	maxLog := fseDefaultTableLog
	if srcLen < 256 {
		maxLog = 7
	}
	if srcLen < 64 {
		maxLog = 6
	}

	if minLog > maxLog {
		return minLog
	}
	return maxLog
}

// ---- Decompress ----

// FSEDecompress decompresses FSE-compressed data.
// originalLen is the expected output length.
func FSEDecompress(compressed []byte, originalLen int) ([]byte, error) {
	if originalLen == 0 {
		return nil, nil
	}
	if len(compressed) == 0 {
		return nil, errors.New("fse: empty compressed data")
	}

	// Check for RLE marker (0xFF in first byte)
	if compressed[0] == 0xFF {
		if len(compressed) < 6 {
			return nil, errors.New("fse: truncated RLE data")
		}
		rleSymbol := compressed[1]
		rleLen := int(binary.LittleEndian.Uint32(compressed[2:]))
		if rleLen != originalLen {
			return nil, fmt.Errorf("fse: RLE length mismatch: %d vs %d", rleLen, originalLen)
		}
		out := make([]byte, originalLen)
		for i := range out {
			out[i] = rleSymbol
		}
		return out, nil
	}

	// Deserialize table
	t, headerSize, err := deserializeTable(compressed)
	if err != nil {
		return nil, fmt.Errorf("fse: %w", err)
	}

	// Read bitstream (after header)
	bitData := compressed[headerSize:]
	if len(bitData) == 0 {
		return nil, errors.New("fse: no bitstream data")
	}

	// Find sentinel bit: scan from the end of the last byte
	lastByte := bitData[len(bitData)-1]
	if lastByte == 0 {
		return nil, errors.New("fse: invalid bitstream (last byte is 0, no sentinel)")
	}
	sentinelPos := bits.Len(uint(lastByte)) - 1 // position of highest set bit

	// Calculate total valid bits: all bytes except last provide 8 bits,
	// last byte provides sentinelPos bits (the sentinel itself is not data)
	totalBits := (len(bitData)-1)*8 + sentinelPos

	// Create a bit reader for the valid bits
	// We need to read exactly totalBits of data
	br := newBitReader(bitData)

	// Read initial state
	tableLog := t.tableLog
	state := int(br.readBits(tableLog))

	// Decode symbols
	out := make([]byte, originalLen)
	bitsRead := tableLog

	for i := 0; i < originalLen; i++ {
		// Decode symbol from current state
		entry := t.decodeTable[state]
		out[i] = entry.symbol

		if i == originalLen-1 {
			break // last symbol, no need to transition
		}

		// Read bits and transition to next state
		nbBits := int(entry.nbBits)
		addBits := int(br.readBits(nbBits))
		state = int(entry.newState) + addBits
		bitsRead += nbBits
	}

	// Verify we consumed the right amount of bits
	if bitsRead > totalBits {
		return nil, fmt.Errorf("fse: consumed %d bits but only %d available", bitsRead, totalBits)
	}

	return out, nil
}
