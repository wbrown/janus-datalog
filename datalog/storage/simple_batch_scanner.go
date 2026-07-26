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
	matcher     *PatternMatcher
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
	matcher *PatternMatcher,
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
	// Step 1: Build a set of binding values for fast lookup. A failed
	// bindings scan must not read as "no bindings".
	bindingSet := s.buildBindingSet()
	if s.err != nil {
		return s.err
	}
	if len(bindingSet) == 0 {
		return nil
	}

	// Step 2: Calculate the run that encompasses all bindings
	bound, err := s.calculateScanBound(bindingSet)
	if err != nil {
		return err
	}

	// Step 3: Open a single scan for the entire run using key-only scanning
	rawIter, err := s.matcher.reader.ScanKeysOnly(bound)
	if err != nil {
		return fmt.Errorf("failed to open scan: %w", err)
	}

	// Wrap with CRDT resolution unless in history mode
	var iter Iterator
	if s.matcher.isHistoryMode() {
		iter = rawIter
	} else {
		iter = NewCRDTResolvingIterator(rawIter, s.matcher.schema, s.matcher.crdtTxID(), s.matcher)
	}

	// Step 4: Scan and filter. Surface any captured error — a failed scan
	// must not present as a completed (possibly empty) result.
	s.results = s.scanAndFilter(iter, bindingSet)
	if closeErr := iter.Close(); closeErr != nil && s.err == nil {
		s.err = closeErr
	}
	return s.err
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
	if err := it.Error(); err != nil && s.err == nil {
		s.err = err
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

// calculateScanBound determines the one run covering every binding: it starts
// at the lowest binding's prefix and reaches through the highest's, and the
// filter phase discards whatever falls between them. Ordering is a property of
// the encoded keys, so each candidate prefix is encoded to find the two
// endpoints; only those two are kept.
func (s *simpleBatchScanner) calculateScanBound(bindingSet map[string]executor.Tuple) (ScanBound, error) {
	// Get constant parts of the pattern for prefix construction
	var constA datalog.Keyword
	if c, ok := s.pattern.GetA().(query.Constant); ok {
		if kw, ok := c.Value.(datalog.Keyword); ok {
			constA = kw
		}
	}

	// Same for the Tx slot — ATEV's [A][Tx↓][E][V] layout needs constT
	// after constA to produce a tight prefix. Tx is always an ElementID.
	var constT *datalog.ElementID
	if c, ok := s.pattern.GetT().(query.Constant); ok {
		eid, ok := datalog.DerefElementID(c.Value)
		if !ok {
			panic(fmt.Sprintf("Tx constant must be ElementID, got %T", c.Value))
		}
		constT = &eid
	}

	var lowest, highest []datalog.Value
	var lowestKey, highestKey []byte

	for _, tuple := range bindingSet {
		if s.position >= len(tuple) {
			continue
		}

		prefix, ok := s.bindingPrefix(tuple[s.position], constA, constT)
		if !ok {
			continue
		}

		key, _, err := s.matcher.encoder.EncodeScanBound(ScanBound{Index: s.index, Prefix: prefix})
		if err != nil {
			return ScanBound{}, err
		}

		if lowestKey == nil || bytes.Compare(key, lowestKey) < 0 {
			lowest, lowestKey = prefix, key
		}
		if highestKey == nil || bytes.Compare(key, highestKey) > 0 {
			highest, highestKey = prefix, key
		}
	}

	if lowestKey == nil {
		return ScanBound{}, fmt.Errorf("failed to calculate scan range")
	}
	return ScanBound{Index: s.index, Prefix: lowest, Through: highest}, nil
}

// bindingPrefix names the run one binding value occupies on s.index: the
// leading components of that index's order, as far as the binding and the
// pattern's constants can bind them. Reports false when this binding cannot
// address a run on this index, in which case it contributes no endpoint.
//
// Value type invariants:
//   - datalog.Identity and datalog.Keyword are pointer type aliases
//     (*identity / *keyword). They are kept as pointers throughout —
//     interning depends on pointer identity. Do NOT dereference them;
//     doing so produces unexported struct values that subsequent
//     type assertions against the pointer alias type cannot match,
//     which used to silently route keyword-bound scans to the
//     "return nil" fall-through.
//   - *uint64 is a non-interned boxed scalar sometimes used for
//     comparable-value workarounds; dereference it for uniform
//     handling.
func (s *simpleBatchScanner) bindingPrefix(
	value interface{},
	constA datalog.Keyword,
	constT *datalog.ElementID,
) ([]datalog.Value, bool) {
	if ptr, ok := value.(*uint64); ok {
		value = *ptr
	}

	// Use named IndexType constants. The earlier implementation switched
	// on integer literals (0=EAVT, 1=AEVT, 3=VAET, 4=TAEV), which was
	// correct for the 5-index enum but silently broke when the enum
	// expanded to 7 values: now 1=EATV (not AEVT), 3=AETV (not VAET),
	// 4=AVET (not TAEV). Callers passing AETV (a common choice for
	// A-bound CardinalityOne under CRDT resolution) fell into the VAET
	// branch and produced wrong-shaped keys, causing silent
	// under-counting on bindingRel.Size() > 100.
	switch s.index {
	case EAVT, EATV:
		// Both order E then A, so an E-bound + A-constant batch scan
		// addresses the same run on either.
		if e, ok := value.(datalog.Identity); ok {
			prefix := []datalog.Value{e}
			if constA != nil {
				prefix = append(prefix, constA)
			}
			return prefix, true
		}
	case AEVT, AETV:
		// Both order A then E. Position 0 = E bound, position 1 = A bound.
		if s.position == 0 {
			if e, ok := value.(datalog.Identity); ok && constA != nil {
				return []datalog.Value{constA, e}, true
			}
		} else if s.position == 1 {
			if kw, ok := value.(datalog.Keyword); ok {
				return []datalog.Value{kw}, true
			}
		}
	case ATEV:
		// ATEV: [A][Tx↓][E][V] — chosen when both A and Tx are constants.
		// Position 0 (E varies):  [A][Tx][E] — fully tightened.
		// Position 2 (V varies):  [A][Tx] — E sits between Tx and V and is
		//                         unbound, so we can't include V in the prefix.
		if constA == nil || constT == nil {
			return nil, false
		}
		switch s.position {
		case 0:
			if e, ok := value.(datalog.Identity); ok {
				return []datalog.Value{constA, *constT, e}, true
			}
		case 2:
			return []datalog.Value{constA, *constT}, true
		}
	case AVET:
		// AVET: [A][V][E][Tx]. Useful prefixes:
		//   position=1 (A varies):  [A] prefix from the binding keyword
		//   position=2 (V varies):  [A][V] prefix; requires constA
		//   position=0 (E varies):  [A] prefix only — E comes after V,
		//                           so we cannot tighten without V.
		//                           The scanner's filter phase narrows
		//                           by E in memory.
		switch s.position {
		case 1:
			if kw, ok := value.(datalog.Keyword); ok {
				return []datalog.Value{kw}, true
			}
		case 2:
			if constA != nil {
				return []datalog.Value{constA, value}, true
			}
		case 0:
			if constA != nil {
				return []datalog.Value{constA}, true
			}
		}
	case VAET:
		// VAET: [V][A][E][Tx] — the value is the first component.
		prefix := []datalog.Value{value}
		if constA != nil {
			prefix = append(prefix, constA)
		}
		return prefix, true
	case TAEV:
		// TAEV: [Tx][A][E][V]. The binding value is always an ElementID.
		eid, ok := datalog.DerefElementID(value)
		if !ok {
			panic(fmt.Sprintf("Tx binding must be ElementID, got %T", value))
		}
		return []datalog.Value{eid}, true
	}
	return nil, false
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
	if err := iter.Error(); err != nil && s.err == nil {
		s.err = err
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
			if datom.A != kw {
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
