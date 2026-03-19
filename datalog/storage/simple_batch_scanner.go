package storage

import (
	"bytes"
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// simpleBatchScanner implements a simpler approach to batch scanning
// Instead of complex range grouping, it does a single scan and filters in memory
type simpleBatchScanner struct {
	matcher     *BadgerMatcher
	pattern     *query.DataPattern
	bindingRel  executor.Relation
	position    int // Which position has bindings (0=E, 1=A, 2=V, 3=T)
	index       IndexType
	symbols     []query.Symbol
	constraints []executor.StorageConstraint

	// Results
	results     []executor.Tuple
	resultIndex int

	// Optimized tuple builder
	tupleBuilder *query.InternedTupleBuilder

	err error // First error from storage operations during scan
}

// newSimpleBatchScanner creates a new simple batch scanner
func newSimpleBatchScanner(
	matcher *BadgerMatcher,
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	position int,
	index IndexType,
	symbols []query.Symbol,
	constraints []executor.StorageConstraint,
) *simpleBatchScanner {
	return &simpleBatchScanner{
		matcher:      matcher,
		pattern:      pattern,
		bindingRel:   bindingRel,
		position:     position,
		index:        index,
		symbols:      symbols,
		constraints:  constraints,
		resultIndex:  -1,
		tupleBuilder: matcher.getTupleBuilder(pattern, symbols),
	}
}

// Scan performs the batch scan and collects all results
func (s *simpleBatchScanner) Scan() error {
	// Step 1: Build a set of binding values for fast lookup
	bindingSet := s.buildBindingSet()
	if len(bindingSet) == 0 {
		return nil
	}

	// Step 2: Calculate scan range that encompasses all bindings
	startKey, endKey := s.calculateScanRange(bindingSet)
	if startKey == nil || endKey == nil {
		return fmt.Errorf("failed to calculate scan range")
	}

	// Step 3: Open a single scan for the entire range using key-only scanning
	rawIter, err := s.matcher.store.ScanKeysOnly(s.index, startKey, endKey)
	if err != nil {
		return fmt.Errorf("failed to open scan: %w", err)
	}

	// Wrap with CRDT resolution unless in history mode
	var iter Iterator
	if s.matcher.isHistoryMode() {
		iter = rawIter
	} else {
		iter = NewCRDTResolvingIterator(rawIter, s.matcher.schema, s.matcher.crdtTxID())
	}
	defer iter.Close()

	// Step 4: Scan and filter
	s.results = s.scanAndFilter(iter, bindingSet)

	return nil
}

// buildBindingSet creates a map of binding values for O(1) lookup
func (s *simpleBatchScanner) buildBindingSet() map[string]executor.Tuple {
	bindingSet := make(map[string]executor.Tuple)

	// Get all tuples from the binding relation
	it := s.bindingRel.Iterator()
	defer it.Close()
	for it.Next() {
		tuple := it.Tuple()
		if s.position < len(tuple) {
			// Use hash-based key for Identity types
			key := s.valueToKey(tuple[s.position])
			if key != "" {
				bindingSet[key] = tuple
			}
		}
	}

	return bindingSet
}

// valueToKey converts a value to a string key for the binding set
func (s *simpleBatchScanner) valueToKey(v interface{}) string {
	// Handle pointers by dereferencing first
	// Note: Identity is always a pointer type now, no dereferencing needed
	if ptr, ok := v.(datalog.Keyword); ok {
		v = *ptr
	} else if ptr, ok := v.(*uint64); ok {
		v = *ptr
	} else if eid, ok := datalog.DerefElementID(v); ok {
		v = eid
	}

	switch val := v.(type) {
	case datalog.Identity:
		// Use hash for consistent comparison
		if val == nil {
			return ""
		}
		hash := val.Hash()
		return string(hash[:])
	case datalog.Keyword:
		return val.String()
	case string:
		return val
	case datalog.ElementID:
		return val.String()
	case uint64:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// calculateScanRange determines the min and max keys for scanning
func (s *simpleBatchScanner) calculateScanRange(bindingSet map[string]executor.Tuple) ([]byte, []byte) {
	var minKey, maxKey []byte

	// Get constant parts of the pattern for key construction
	var constA []byte
	if c, ok := s.pattern.GetA().(query.Constant); ok {
		if kw, ok := c.Value.(datalog.Keyword); ok {
			// Convert keyword to 32-byte storage format
			var attr Attribute
			copy(attr[:], kw.String())
			constA = attr[:]
		}
	}

	// Find min and max keys from binding values
	for _, tuple := range bindingSet {
		if s.position >= len(tuple) {
			continue
		}

		key := s.buildKey(tuple[s.position], constA)
		if key == nil {
			continue
		}

		if minKey == nil || bytes.Compare(key, minKey) < 0 {
			minKey = key
		}
		if maxKey == nil || bytes.Compare(key, maxKey) > 0 {
			maxKey = key
		}
	}

	// Extend max key to include all possible suffixes
	if maxKey != nil {
		maxKey = append(maxKey, 0xFF, 0xFF, 0xFF, 0xFF)
	}

	return minKey, maxKey
}

// buildKey builds a storage key for a binding value
func (s *simpleBatchScanner) buildKey(value interface{}, constA []byte) []byte {
	// Handle pointers by dereferencing first
	// Note: Identity is always a pointer type now, no dereferencing needed
	if ptr, ok := value.(datalog.Keyword); ok {
		value = *ptr
	} else if ptr, ok := value.(*uint64); ok {
		value = *ptr
	}

	// Use the store's encoder to build proper keys
	encoder := s.matcher.store.encoder

	switch s.index {
	case 0: // EAVT
		if e, ok := value.(datalog.Identity); ok {
			// Get the entity hash directly
			hash := e.Hash()

			parts := [][]byte{hash[:]}
			if constA != nil {
				parts = append(parts, constA)
			}
			return encoder.EncodePrefix(s.index, parts...)
		}
	case 1: // AEVT
		// AEVT index order: A + E + V + Tx
		// Position 0 = E bound, Position 1 = A bound
		if s.position == 0 {
			// Entity bound, attribute is constant
			if e, ok := value.(datalog.Identity); ok {
				hash := e.Hash()
				// AEVT: A is first, E is second
				if constA != nil {
					parts := [][]byte{constA, hash[:]}
					return encoder.EncodePrefix(s.index, parts...)
				}
			}
		} else if s.position == 1 {
			// Attribute bound (A varies)
			if kw, ok := value.(datalog.Keyword); ok {
				var attr Attribute
				copy(attr[:], kw.String())
				parts := [][]byte{attr[:]}
				return encoder.EncodePrefix(s.index, parts...)
			}
		}
	case 3: // VAET
		// For VAET, the value is the first component
		// Need to handle different value types appropriately
		var valueBytes []byte
		switch v := value.(type) {
		case datalog.Identity:
			// References are stored as 20-byte hashes
			hash := v.Hash()
			valueBytes = hash[:]
		case datalog.Keyword:
			var attr Attribute
			copy(attr[:], v.String())
			valueBytes = attr[:]
		case string:
			valueBytes = []byte(v)
		case []byte:
			valueBytes = v
		default:
			// For other types, encode as bytes
			valueBytes = []byte(fmt.Sprintf("%v", v))
		}
		parts := [][]byte{valueBytes}
		if constA != nil {
			parts = append(parts, constA)
		}
		return encoder.EncodePrefix(s.index, parts...)
	case 4: // TAEV
		// Tx must be encoded with bitwise-NOT for descending sort order
		if eid, ok := datalog.DerefElementID(value); ok {
			txBytes := NewTxFromElementID(eid)
			encTx := encoder.EncodeTxForPrefix(txBytes)
			parts := [][]byte{encTx}
			return encoder.EncodePrefix(s.index, parts...)
		} else if tx, ok := value.(uint64); ok {
			txBytes := NewTxFromUint(tx)
			encTx := encoder.EncodeTxForPrefix(txBytes)
			parts := [][]byte{encTx}
			return encoder.EncodePrefix(s.index, parts...)
		}
	}
	return nil
}

// scanAndFilter scans the iterator and filters by bindings and constraints
func (s *simpleBatchScanner) scanAndFilter(iter Iterator, bindingSet map[string]executor.Tuple) []executor.Tuple {
	var results []executor.Tuple
	datomCount := 0

	for iter.Next() {
		datom, err := iter.Datom()
		if err != nil {
			s.err = err
			break
		}
		datomCount++

		// Check transaction validity
		if s.matcher.shouldFilterTx(datom.Tx) {
			continue
		}

		// Extract value at binding position
		var datomValue interface{}
		switch s.position {
		case 0:
			datomValue = datom.E
		case 1:
			datomValue = datom.A
		case 2:
			datomValue = datom.V
		case 3:
			datomValue = datom.Tx
		}

		// Check if this datom matches any binding
		datomKey := s.valueToKey(datomValue)
		bindingTuple, found := bindingSet[datomKey]
		if !found {
			continue
		}

		// Check if datom matches the full pattern with this binding
		if !s.matchesPattern(datom, bindingTuple) {
			continue
		}

		// Apply constraints
		satisfiesAll := true
		for _, constraint := range s.constraints {
			if !constraint.Evaluate(datom) {
				satisfiesAll = false
				break
			}
		}

		if satisfiesAll {
			// Convert to result tuple
			resultTuple := s.tupleBuilder.BuildTupleInterned(datom)
			results = append(results, resultTuple)
		}
	}

	return results
}

// matchesPattern checks if a datom matches the pattern with the given binding
func (s *simpleBatchScanner) matchesPattern(datom *datalog.Datom, bindingTuple executor.Tuple) bool {
	// For EAVT index with entity bound, we need to check if the attribute matches
	// The pattern is [?bar :price/time ?time] where ?bar is bound

	// Check attribute if it's constant in pattern
	if c, ok := s.pattern.GetA().(query.Constant); ok {
		if kw, ok := c.Value.(datalog.Keyword); ok {
			if datom.A.String() != kw.String() {
				return false
			}
		}
	}

	// For this simple case, if we're here the entity matches (we found it in bindingSet)
	// and if the attribute matches (checked above), we're good
	return true
}

// Iterator interface implementation
func (s *simpleBatchScanner) Next() bool {
	s.resultIndex++
	return s.resultIndex < len(s.results)
}

func (s *simpleBatchScanner) Tuple() executor.Tuple {
	if s.resultIndex >= 0 && s.resultIndex < len(s.results) {
		return s.results[s.resultIndex]
	}
	return nil
}

func (s *simpleBatchScanner) Close() error {
	// Results are already materialized, nothing to close
	return nil
}

func (s *simpleBatchScanner) Error() error { return s.err }
