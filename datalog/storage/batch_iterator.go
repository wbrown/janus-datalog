package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// batchScanIterator processes multiple binding tuples in batches to minimize seeks
type batchScanIterator struct {
	matcher     *BadgerMatcher
	pattern     *query.DataPattern
	bindingRel  executor.Relation
	tuples      []executor.Tuple
	position    int       // Which position is changing (0=E, 1=A, 2=V)
	index       IndexType // Which index to use
	columns     []query.Symbol
	constraints []executor.StorageConstraint

	// Batch state
	batchSize    int              // Max tuples per batch
	currentBatch []executor.Tuple // Current batch of binding tuples
	batchStart   int              // Start index in tuples
	batchEnd     int              // End index in tuples

	// Scan state
	storageIter    Iterator         // Current storage iterator
	pendingMatches []executor.Tuple // Buffered matches from current batch
	matchIndex     int              // Current position in pendingMatches

	// Stats
	totalSeeks    int
	totalScans    int
	datomsScanned int
	datomsMatched int

	// Optimized tuple builder
	tupleBuilder *query.OptimizedTupleBuilder
}

// RangeGroup represents a group of binding tuples that can be scanned together
type RangeGroup struct {
	startIdx int
	endIdx   int
	startKey []byte
	endKey   []byte
}

// newBatchScanIterator creates a new batch scanning iterator
func newBatchScanIterator(
	matcher *BadgerMatcher,
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	tuples []executor.Tuple,
	position int,
	index IndexType,
	columns []query.Symbol,
	constraints []executor.StorageConstraint,
) *batchScanIterator {
	return &batchScanIterator{
		matcher:     matcher,
		pattern:     pattern,
		bindingRel:  bindingRel,
		tuples:      tuples,
		position:    position,
		index:       index,
		columns:     columns,
		constraints: constraints,
		batchSize:   100, // Default batch size - tune based on performance
		batchStart:  0,
		matchIndex:  -1,
	}
}

func (it *batchScanIterator) Next() bool {
	// If we have pending matches from the current batch, return them
	if it.matchIndex >= 0 && it.matchIndex < len(it.pendingMatches)-1 {
		it.matchIndex++
		return true
	}

	// Need to process next batch
	for it.batchStart < len(it.tuples) {
		// Load next batch
		if !it.loadNextBatch() {
			it.batchStart = it.batchEnd
			continue
		}

		_ = fmt.Sprintf("[BATCH] Processing batch %d-%d of %d tuples\n", it.batchStart, it.batchEnd, len(it.tuples))

		// Process the batch
		it.processBatch()

		_ = fmt.Sprintf("[BATCH] Found %d matches in batch\n", len(it.pendingMatches))

		// If we found matches, start returning them
		if len(it.pendingMatches) > 0 {
			it.matchIndex = 0
			return true
		}

		// No matches in this batch, try next
		it.batchStart = it.batchEnd
	}

	// No more batches
	if it.storageIter != nil {
		it.storageIter.Close()
		it.storageIter = nil
	}

	return false
}

func (it *batchScanIterator) loadNextBatch() bool {
	if it.batchStart >= len(it.tuples) {
		return false
	}

	// Determine batch end
	it.batchEnd = it.batchStart + it.batchSize
	if it.batchEnd > len(it.tuples) {
		it.batchEnd = len(it.tuples)
	}

	it.currentBatch = it.tuples[it.batchStart:it.batchEnd]

	// Debug: Show what's in the batch
	if it.batchStart == 0 && len(it.currentBatch) > 0 {
		_ = fmt.Sprintf("[LOAD] First batch tuple[0]: %v (len=%d)\n", it.currentBatch[0], len(it.currentBatch[0]))
		_ = fmt.Sprintf("[LOAD] Position %d should contain entity ID\n", it.position)
		if it.position < len(it.currentBatch[0]) {
			_ = fmt.Sprintf("[LOAD] Value at position %d: %v\n", it.position, it.currentBatch[0][it.position])
		}
	}

	return len(it.currentBatch) > 0
}

func (it *batchScanIterator) processBatch() {
	// Clear pending matches
	it.pendingMatches = it.pendingMatches[:0]

	// Group the batch into ranges based on key proximity
	ranges := it.groupIntoRanges(it.currentBatch)

	for _, rg := range ranges {
		it.scanRange(rg)
	}
}

// groupIntoRanges groups binding tuples into ranges that can be scanned together
func (it *batchScanIterator) groupIntoRanges(batch []executor.Tuple) []RangeGroup {
	if len(batch) == 0 {
		return nil
	}

	var ranges []RangeGroup
	currentRange := RangeGroup{startIdx: 0}

	for i := 0; i < len(batch); i++ {
		if i > 0 {
			// Check if this tuple is "close" to the previous one
			// For now, use a simple heuristic: if keys share a common prefix
			prevKey := it.calculateKey(batch[i-1])
			currKey := it.calculateKey(batch[i])

			// If keys are not adjacent (differ by more than threshold), start new range
			if !it.areKeysClose(prevKey, currKey) {
				// Finish current range
				currentRange.endIdx = i
				currentRange.startKey = it.calculateKey(batch[currentRange.startIdx])
				currentRange.endKey = it.calculateRangeEnd(batch[i-1])
				ranges = append(ranges, currentRange)

				// Start new range
				currentRange = RangeGroup{startIdx: i}
			}
		}
	}

	// Finish last range
	currentRange.endIdx = len(batch)
	currentRange.startKey = it.calculateKey(batch[currentRange.startIdx])
	currentRange.endKey = it.calculateRangeEnd(batch[len(batch)-1])
	ranges = append(ranges, currentRange)

	return ranges
}

// areKeysClose determines if two keys are close enough to scan together
func (it *batchScanIterator) areKeysClose(key1, key2 []byte) bool {
	// For EAVT index with entity bound, keys will differ in the entity hash
	// We should group them all together since we're scanning by entity
	// For now, always group consecutive tuples together
	// TODO: Smarter grouping based on actual key distance
	return true
}

// calculateKey calculates the seek key for a binding tuple
func (it *batchScanIterator) calculateKey(tuple executor.Tuple) []byte {
	// Extract the binding value based on position
	if it.position >= len(tuple) {
		fmt.Printf("[KEY] Position %d >= tuple length %d\n", it.position, len(tuple))
		return nil
	}

	value := tuple[it.position]
	fmt.Printf("[KEY] Value at position %d: %v (type: %T)\n", it.position, value, value)

	// Build key based on index type and binding position
	switch it.index {
	case 0: // EAVT
		fmt.Printf("[KEY] Using EAVT index\n")
		if e, ok := value.(datalog.Identity); ok {
			// Also need to include the attribute if it's constant
			var aBytes []byte
			if c, ok := it.pattern.GetA().(query.Constant); ok {
				if kw, ok := c.Value.(datalog.Keyword); ok {
					aBytes = encodeKeyword(kw)
				}
			}
			key := buildEAVTKey(e, aBytes, nil, 0)
			fmt.Printf("[KEY] Built EAVT key: %x\n", key)
			return key
		}

	case 1: // AEVT
		if kw, ok := value.(datalog.Keyword); ok {
			return buildAEVTKey(kw, nil, nil, 0)
		}

	case 3: // VAET
		// Value can be any type
		vBytes := encodeValue(value)
		return buildVAETKey(vBytes, nil, nil, 0)

	case 4: // TAEV
		if tx, ok := value.(uint64); ok {
			return buildTAEVKey(tx, nil, nil, nil)
		}
	}

	return nil
}

// calculateRangeEnd calculates the end key for a range
func (it *batchScanIterator) calculateRangeEnd(tuple executor.Tuple) []byte {
	key := it.calculateKey(tuple)
	if key == nil {
		return nil
	}

	// Extend the key to include all possible suffixes
	return append(key, 0xFF, 0xFF, 0xFF, 0xFF)
}

// scanRange scans a range and collects matches
func (it *batchScanIterator) scanRange(rg RangeGroup) {
	// Close previous iterator if any
	if it.storageIter != nil {
		it.storageIter.Close()
	}

	_ = fmt.Sprintf("[RANGE] Scanning range %d-%d, startKey=%x, endKey=%x\n",
		rg.startIdx, rg.endIdx, rg.startKey[:20], rg.endKey[:20])

	// Open scan for this range using key-only scanning
	fmt.Printf("[SCAN] Opening scan with index %d from %x to %x\n", it.index, rg.startKey, rg.endKey)
	var err error
	it.storageIter, err = it.matcher.store.ScanKeysOnly(it.index, rg.startKey, rg.endKey)
	if err != nil {
		fmt.Printf("[SCAN] Error opening scan: %v\n", err)
		return
	}
	it.totalScans++
	fmt.Printf("[SCAN] Scan opened successfully\n")

	// Build a map of binding values for quick lookup
	bindingValues := make(map[string]executor.Tuple)
	fmt.Printf("[SCAN] Building binding map for %d tuples\n", rg.endIdx-rg.startIdx)
	for i := rg.startIdx; i < rg.endIdx; i++ {
		tuple := it.currentBatch[i]
		if it.position < len(tuple) {
			// Use a string representation of the value as key
			key := valueToString(tuple[it.position])
			bindingValues[key] = tuple
			if i < rg.startIdx+3 {
				fmt.Printf("[MAP] Binding %d: key=%x\n", i, []byte(key)[:8])
			}
		}
	}

	// Scan and match
	datomCount := 0
	for it.storageIter.Next() {
		datom, err := it.storageIter.Datom()
		if err != nil {
			continue
		}

		it.datomsScanned++
		datomCount++

		if datomCount <= 3 {
			_ = fmt.Sprintf("[SCAN] Datom %d: E=%s, A=%s\n", datomCount, datom.E.String(), datom.A.String())
		}

		// Check transaction validity
		if it.matcher.txID > 0 && datom.Tx > it.matcher.txID {
			continue
		}

		// Extract the value at the binding position from the datom
		var datomValue interface{}
		switch it.position {
		case 0:
			datomValue = datom.E
			if datomCount <= 3 {
				_ = fmt.Sprintf("[EXTRACT] Entity from datom: %v (type: %T)\n", datomValue, datomValue)
			}
		case 1:
			datomValue = datom.A
		case 2:
			datomValue = datom.V
		case 3:
			datomValue = datom.Tx
		}

		// Check if this datom matches any binding in our batch
		datomKey := valueToString(datomValue)
		bindingTuple, found := bindingValues[datomKey]
		if !found {
			if datomCount <= 3 {
				fmt.Printf("[MATCH] Datom key %x not in binding map\n", []byte(datomKey)[:8])
			}
			continue
		}
		fmt.Printf("[MATCH] Found binding match for key %x\n", []byte(datomKey)[:8])

		// Check if datom matches the full pattern with this binding
		if !it.matchesPattern(datom, bindingTuple) {
			continue
		}

		// Apply constraints
		satisfiesAll := true
		for _, constraint := range it.constraints {
			if !constraint.Evaluate(datom) {
				satisfiesAll = false
				break
			}
		}

		if satisfiesAll {
			// Convert to tuple and add to results
			resultTuple := query.DatomToTuple(*datom, it.pattern, it.columns)
			if resultTuple != nil {
				it.pendingMatches = append(it.pendingMatches, resultTuple)
				it.datomsMatched++
			}
		}
	}
}

// matchesPattern checks if a datom matches the pattern with the given binding
func (it *batchScanIterator) matchesPattern(datom *datalog.Datom, bindingTuple executor.Tuple) bool {
	// Build the bound pattern from the binding tuple
	boundPattern := it.matcher.bindPattern(it.pattern, bindingTuple, it.bindingRel)

	// Extract expected values
	var e, a, v interface{}
	var tx *uint64

	if elem := boundPattern.GetE(); elem != nil {
		e = it.matcher.extractValue(elem)
	}
	if elem := boundPattern.GetA(); elem != nil {
		a = it.matcher.extractValue(elem)
	}
	if elem := boundPattern.GetV(); elem != nil {
		v = it.matcher.extractValue(elem)
	}
	if elem := boundPattern.GetT(); elem != nil {
		if val := it.matcher.extractValue(elem); val != nil {
			if txVal, ok := val.(uint64); ok {
				tx = &txVal
			}
		}
	}

	return it.matcher.matchesDatom(datom, e, a, v, tx)
}

func (it *batchScanIterator) Tuple() executor.Tuple {
	if it.matchIndex >= 0 && it.matchIndex < len(it.pendingMatches) {
		return it.pendingMatches[it.matchIndex]
	}
	return nil
}

func (it *batchScanIterator) Close() error {
	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}

// Helper function to convert a value to string for map keys
// For Identity types, we use the hash as the key to ensure proper comparison
func valueToString(v interface{}) string {
	switch val := v.(type) {
	case datalog.Identity:
		// Use the hash bytes as the key for consistent comparison
		hash := val.Hash()
		return string(hash[:])
	case datalog.Keyword:
		return val.String()
	case string:
		return val
	case uint64:
		return fmt.Sprintf("%d", val)
	default:
		// Use a generic string representation
		return fmt.Sprintf("%v", v)
	}
}

// buildEAVTKey builds a key for the EAVT index
func buildEAVTKey(e datalog.Identity, aBytes []byte, vBytes []byte, tx uint64) []byte {
	key := make([]byte, 0, 100)
	hash := e.Hash()
	key = append(key, hash[:]...)
	if aBytes != nil {
		key = append(key, aBytes...)
	}
	if vBytes != nil {
		key = append(key, vBytes...)
	}
	if tx > 0 {
		key = append(key, encodeUint64(tx)...)
	}
	return key
}

// buildAEVTKey builds a key for the AEVT index
func buildAEVTKey(a datalog.Keyword, eBytes []byte, vBytes []byte, tx uint64) []byte {
	key := make([]byte, 0, 100)
	key = append(key, encodeKeyword(a)...)
	if eBytes != nil {
		key = append(key, eBytes...)
	}
	if vBytes != nil {
		key = append(key, vBytes...)
	}
	if tx > 0 {
		key = append(key, encodeUint64(tx)...)
	}
	return key
}

// buildVAETKey builds a key for the VAET index
func buildVAETKey(vBytes []byte, aBytes []byte, eBytes []byte, tx uint64) []byte {
	key := make([]byte, 0, 100)
	key = append(key, vBytes...)
	if aBytes != nil {
		key = append(key, aBytes...)
	}
	if eBytes != nil {
		key = append(key, eBytes...)
	}
	if tx > 0 {
		key = append(key, encodeUint64(tx)...)
	}
	return key
}

// buildTAEVKey builds a key for the TAEV index
func buildTAEVKey(tx uint64, aBytes []byte, eBytes []byte, vBytes []byte) []byte {
	key := make([]byte, 0, 100)
	key = append(key, encodeUint64(tx)...)
	if aBytes != nil {
		key = append(key, aBytes...)
	}
	if eBytes != nil {
		key = append(key, eBytes...)
	}
	if vBytes != nil {
		key = append(key, vBytes...)
	}
	return key
}

// encodeKeyword encodes a keyword to bytes
func encodeKeyword(kw datalog.Keyword) []byte {
	// Use the keyword's string representation
	// In production, this should use the same encoding as the storage layer
	return []byte(kw.String())
}

// encodeValue encodes any value to bytes
func encodeValue(v interface{}) []byte {
	// This should match the storage layer's value encoding
	// For now, use a simple string representation
	switch val := v.(type) {
	case datalog.Identity:
		hash := val.Hash()
		return hash[:]
	case datalog.Keyword:
		return []byte(val.String())
	case string:
		return []byte(val)
	case []byte:
		return val
	default:
		return []byte(fmt.Sprintf("%v", v))
	}
}

// encodeUint64 encodes a uint64 to bytes
func encodeUint64(n uint64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(n)
		n >>= 8
	}
	return b
}
