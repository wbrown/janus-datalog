package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// JoinStrategy represents different join strategies for pattern matching with bindings
type JoinStrategy int

const (
	// IndexNestedLoop uses Seek() per binding value (good for small sets or high selectivity)
	IndexNestedLoop JoinStrategy = iota

	// HashJoinScan builds hash set and does single scan (good for medium selectivity 1-50%)
	HashJoinScan

	// MergeJoin merges sorted streams (future: good for large sets >50% selectivity)
	MergeJoin
)

func (js JoinStrategy) String() string {
	switch js {
	case IndexNestedLoop:
		return "index-nested-loop"
	case HashJoinScan:
		return "hash-join-scan"
	case MergeJoin:
		return "merge-join"
	default:
		return "unknown"
	}
}

// chooseJoinStrategy selects the optimal join strategy based on selectivity
func (m *BadgerMatcher) chooseJoinStrategy(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	position int,
) JoinStrategy {
	bindingSize := bindingRel.Size()

	// Estimate pattern cardinality (total datoms that match the constant parts)
	patternCard := m.estimatePatternCardinality(pattern)

	// Calculate selectivity: what % of pattern matches are in the binding set?
	selectivity := float64(bindingSize) / float64(patternCard)

	// Strategy selection based on selectivity and absolute size
	if bindingSize <= 10 {
		// Very small binding sets: index nested loop is fine
		// Overhead of hash table not worth it
		return IndexNestedLoop
	}

	// For medium-sized binding sets (11-1000), always use hash join
	// Iterator reuse has proven to be unreliable and scans excessive datoms
	if bindingSize <= 1000 {
		return HashJoinScan
	}

	// For large binding sets (>1000), consider selectivity
	if selectivity < 0.50 {
		// Medium selectivity (<50%): hash join scan
		// Single scan + hash probe is optimal
		return HashJoinScan
	}

	// High selectivity (>50%): merge join is optimal
	// Both binding relation and pattern scan are sorted, so we can merge them
	return MergeJoin
}

// estimatePatternCardinality estimates total datoms matching pattern's constant parts
func (m *BadgerMatcher) estimatePatternCardinality(pattern *query.DataPattern) int {
	// TODO: Implement proper cardinality estimation using statistics
	// For now, use simple heuristics based on what's bound

	// Check if attribute is constant (most common case)
	if c, ok := pattern.GetA().(query.Constant); ok {
		if kw, ok := c.Value.(datalog.Keyword); ok {
			// Estimate based on attribute
			// For now, return a conservative estimate
			// TODO: Track actual counts per attribute in BadgerStore
			_ = kw       // Future: m.store.GetAttributeCardinality(kw)
			return 10000 // Conservative estimate: 10K datoms per attribute
		}
	}

	// If no attribute bound, could be any datom - very large estimate
	return 1000000 // 1M datoms as default estimate
}

// matchWithHashJoin performs a hash join between binding relation and pattern
func (m *BadgerMatcher) matchWithHashJoin(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	position int,
	index IndexType,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// PHASE 1: Build hash set from binding relation
	hashSet := m.buildHashSet(bindingRel, position)

	if len(hashSet) == 0 {
		// No bindings - return empty result
		return executor.NewMaterializedRelationNoDedupeWithOptions(columns, nil, m.options), nil
	}

	// PHASE 2: Determine scan range for the pattern
	scanRange := m.calculatePatternScanRange(pattern, index)

	// PHASE 3: Create storage iterator
	storageIter, err := m.store.ScanKeysOnly(index, scanRange.start, scanRange.end)
	if err != nil {
		return nil, fmt.Errorf("hash join scan failed: %w", err)
	}

	// PHASE 4: Create streaming hash join iterator
	iter := &hashJoinIterator{
		matcher:      m,
		pattern:      pattern,
		bindingRel:   bindingRel,
		columns:      columns,
		position:     position,
		index:        index,
		constraints:  constraints,
		hashSet:      hashSet,
		iter:         storageIter,
		tupleBuilder: m.getTupleBuilder(pattern, columns),
	}

	// Return streaming relation
	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}

// scanRange holds start and end keys for a storage scan
type scanRange struct {
	start []byte
	end   []byte
}

// calculatePatternScanRange determines the scan range for a pattern
func (m *BadgerMatcher) calculatePatternScanRange(pattern *query.DataPattern, index IndexType) scanRange {
	// Extract constant parts of pattern
	var e, a, v, tx interface{}

	if c, ok := pattern.GetE().(query.Constant); ok {
		e = c.Value
	}
	if c, ok := pattern.GetA().(query.Constant); ok {
		a = c.Value
	}
	if c, ok := pattern.GetV().(query.Constant); ok {
		v = c.Value
	}
	if c, ok := pattern.GetT().(query.Constant); ok {
		tx = c.Value
	}

	// Use existing chooseIndex logic to determine range
	// But we already know which index to use, so just compute the range
	_, start, end := m.chooseIndexForValues(index, e, a, v, tx)

	return scanRange{start: start, end: end}
}

// chooseIndexForValues computes scan range for a specific index
func (m *BadgerMatcher) chooseIndexForValues(index IndexType, e, a, v, tx interface{}) (IndexType, []byte, []byte) {
	// Use the provided index and compute range based on bound values
	var startParts, endParts [][]byte

	encoder := m.store.encoder

	switch index {
	case EAVT: // 0
		if e != nil {
			if entity, ok := e.(datalog.Identity); ok {
				hash := entity.Hash()
				startParts = append(startParts, hash[:])
				endParts = append(endParts, hash[:])

				if a != nil {
					if kw, ok := a.(datalog.Keyword); ok {
						attr := NewAttribute(kw.String())
						startParts = append(startParts, attr[:])
						endParts = append(endParts, attr[:])
					}
				}
			}
		}

	case AEVT: // 1
		if a != nil {
			if kw, ok := a.(datalog.Keyword); ok {
				attr := NewAttribute(kw.String())
				startParts = append(startParts, attr[:])
				endParts = append(endParts, attr[:])

				if e != nil {
					if entity, ok := e.(datalog.Identity); ok {
						hash := entity.Hash()
						startParts = append(startParts, hash[:])
						endParts = append(endParts, hash[:])
					}
				}
			}
		}
	}

	start := encoder.EncodePrefix(index, startParts...)
	end := encoder.EncodePrefix(index, endParts...)

	// Extend end key to include all suffixes
	if len(end) > 0 {
		end = append(end, 0xFF, 0xFF, 0xFF, 0xFF)
	}

	return index, start, end
}

// buildHashSet creates a hash set from binding relation for O(1) lookup
func (m *BadgerMatcher) buildHashSet(bindingRel executor.Relation, position int) map[string]executor.Tuple {
	hashSet := make(map[string]executor.Tuple)

	iter := bindingRel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if position >= len(tuple) {
			continue
		}

		// Extract value at position and convert to hash key
		value := tuple[position]
		key := valueToHashKey(value)

		if key != "" {
			hashSet[key] = tuple
		}
	}
	iter.Close()

	return hashSet
}

// extractProbeKey extracts the value from datom at the specified position
func extractProbeKey(datom *datalog.Datom, position int) interface{} {
	switch position {
	case 0:
		return datom.E
	case 1:
		return datom.A
	case 2:
		return datom.V
	case 3:
		return datom.Tx
	default:
		return nil
	}
}

// valueToHashKey converts a value to a string key for hashing
func valueToHashKey(v interface{}) string {
	// Handle pointers by dereferencing
	if ptr, ok := v.(*datalog.Identity); ok {
		v = *ptr
	} else if ptr, ok := v.(*datalog.Keyword); ok {
		v = *ptr
	} else if ptr, ok := v.(*uint64); ok {
		v = *ptr
	}

	switch val := v.(type) {
	case datalog.Identity:
		// Use hash for consistent comparison
		hash := val.Hash()
		return string(hash[:])
	case datalog.Keyword:
		return val.String()
	case string:
		return val
	case uint64:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// matchesWithBindingTuple checks if datom matches pattern with the given binding tuple
func (m *BadgerMatcher) matchesWithBindingTuple(
	datom *datalog.Datom,
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	bindingTuple executor.Tuple,
) bool {
	// Build column index for binding relation
	columns := bindingRel.Columns()
	colIndex := make(map[query.Symbol]int)
	for i, col := range columns {
		colIndex[col] = i
	}

	// Extract bound values for E, A, V, T
	var e, a, v, tx interface{}

	// E
	if c, ok := pattern.GetE().(query.Constant); ok {
		e = c.Value
	} else if sym, ok := pattern.GetE().(query.Variable); ok {
		if idx, found := colIndex[sym.Name]; found && idx < len(bindingTuple) {
			e = bindingTuple[idx]
		}
	}

	// A
	if c, ok := pattern.GetA().(query.Constant); ok {
		a = c.Value
	} else if sym, ok := pattern.GetA().(query.Variable); ok {
		if idx, found := colIndex[sym.Name]; found && idx < len(bindingTuple) {
			a = bindingTuple[idx]
		}
	}

	// V
	if c, ok := pattern.GetV().(query.Constant); ok {
		v = c.Value
	} else if sym, ok := pattern.GetV().(query.Variable); ok {
		if idx, found := colIndex[sym.Name]; found && idx < len(bindingTuple) {
			v = bindingTuple[idx]
		}
	}

	// T
	if len(pattern.Elements) > 3 {
		if c, ok := pattern.GetT().(query.Constant); ok {
			tx = c.Value
		} else if sym, ok := pattern.GetT().(query.Variable); ok {
			if idx, found := colIndex[sym.Name]; found && idx < len(bindingTuple) {
				tx = bindingTuple[idx]
			}
		}
	}

	// Use existing matchesDatom logic
	return m.matchesDatom(datom, e, a, v, tx)
}

// matchWithMergeJoin performs a merge join between sorted binding relation and sorted scan
// This is optimal for high selectivity (>50%) with large binding sets (>1000 entities)
// Complexity: O(n + m) where n = binding size, m = datoms scanned
func (m *BadgerMatcher) matchWithMergeJoin(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	position int,
	index IndexType,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// PHASE 1: Sort binding relation by join key
	// Sorted() will auto-materialize if needed
	sortedTuples := bindingRel.Sorted()

	if len(sortedTuples) == 0 {
		// No bindings - return empty result
		return executor.NewMaterializedRelationNoDedupeWithOptions(columns, nil, m.options), nil
	}

	// PHASE 2: Determine scan range for the pattern
	scanRange := m.calculatePatternScanRange(pattern, index)

	// PHASE 3: Create storage iterator
	storageIter, err := m.store.ScanKeysOnly(index, scanRange.start, scanRange.end)
	if err != nil {
		return nil, fmt.Errorf("merge join scan failed: %w", err)
	}

	// PHASE 4: Create streaming merge join iterator
	iter := &mergeJoinIterator{
		matcher:      m,
		pattern:      pattern,
		bindingRel:   bindingRel,
		columns:      columns,
		position:     position,
		index:        index,
		constraints:  constraints,
		sortedTuples: sortedTuples,
		bindingIdx:   0,
		iter:         storageIter,
		tupleBuilder: m.getTupleBuilder(pattern, columns),
	}

	// Return streaming relation
	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}

// extractBindingKey extracts the join key from a binding tuple at the specified position
func extractBindingKey(tuple executor.Tuple, position int) interface{} {
	if position >= len(tuple) {
		return nil
	}
	return tuple[position]
}

// compareJoinKeys compares two join keys for ordering in merge join
// Returns: -1 if a < b, 0 if a == b, 1 if a > b
func compareJoinKeys(a, b interface{}) int {
	// Handle nil cases
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Handle pointers by dereferencing
	if ptr, ok := a.(*datalog.Identity); ok {
		a = *ptr
	}
	if ptr, ok := b.(*datalog.Identity); ok {
		b = *ptr
	}

	// Type-specific comparisons using sort order
	switch aVal := a.(type) {
	case datalog.Identity:
		if bVal, ok := b.(datalog.Identity); ok {
			// Compare by hash (lexicographic order)
			aHash := aVal.Hash()
			bHash := bVal.Hash()
			for i := 0; i < len(aHash) && i < len(bHash); i++ {
				if aHash[i] < bHash[i] {
					return -1
				}
				if aHash[i] > bHash[i] {
					return 1
				}
			}
			return 0
		}
	case datalog.Keyword:
		if bVal, ok := b.(datalog.Keyword); ok {
			aStr := aVal.String()
			bStr := bVal.String()
			if aStr < bStr {
				return -1
			}
			if aStr > bStr {
				return 1
			}
			return 0
		}
	case string:
		if bVal, ok := b.(string); ok {
			if aVal < bVal {
				return -1
			}
			if aVal > bVal {
				return 1
			}
			return 0
		}
	case uint64:
		if bVal, ok := b.(uint64); ok {
			if aVal < bVal {
				return -1
			}
			if aVal > bVal {
				return 1
			}
			return 0
		}
	case int64:
		if bVal, ok := b.(int64); ok {
			if aVal < bVal {
				return -1
			}
			if aVal > bVal {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison for unknown types
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

// hashJoinIterator performs lazy hash join iteration
type hashJoinIterator struct {
	matcher      *BadgerMatcher
	pattern      *query.DataPattern
	bindingRel   executor.Relation
	columns      []query.Symbol
	position     int
	index        IndexType
	constraints  []executor.StorageConstraint
	hashSet      map[string]executor.Tuple // Built upfront
	iter         Iterator                  // Storage iterator
	tupleBuilder *query.InternedTupleBuilder
	current      executor.Tuple
}

func (it *hashJoinIterator) Next() bool {
	for it.iter.Next() {
		datom, err := it.iter.Datom()
		if err != nil {
			continue
		}

		// Check transaction validity
		if it.matcher.txID > 0 && datom.Tx > it.matcher.txID {
			continue
		}

		// Extract probe key based on position
		probeKey := extractProbeKey(datom, it.position)
		probeKeyStr := valueToHashKey(probeKey)

		// Probe hash set (O(1) lookup)
		if bindingTuple, found := it.hashSet[probeKeyStr]; found {
			// Verify full pattern match
			if it.matcher.matchesWithBindingTuple(datom, it.pattern, it.bindingRel, bindingTuple) {
				// Apply storage constraints
				satisfiesAll := true
				for _, constraint := range it.constraints {
					if !constraint.Evaluate(datom) {
						satisfiesAll = false
						break
					}
				}

				if satisfiesAll {
					tuple := it.tupleBuilder.BuildTupleInterned(datom)
					if tuple != nil {
						it.current = tuple
						return true
					}
				}
			}
		}
	}
	return false
}

func (it *hashJoinIterator) Tuple() executor.Tuple {
	return it.current
}

func (it *hashJoinIterator) Close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

// mergeJoinIterator performs lazy merge join iteration
type mergeJoinIterator struct {
	matcher       *BadgerMatcher
	pattern       *query.DataPattern
	bindingRel    executor.Relation
	columns       []query.Symbol
	position      int
	index         IndexType
	constraints   []executor.StorageConstraint
	sortedTuples  []executor.Tuple // Sorted binding tuples
	bindingIdx    int              // Current position in sorted tuples
	iter          Iterator         // Storage iterator
	tupleBuilder  *query.InternedTupleBuilder
	current       executor.Tuple
}

func (it *mergeJoinIterator) Next() bool {
	for it.iter.Next() {
		datom, err := it.iter.Datom()
		if err != nil {
			continue
		}

		// Check transaction validity
		if it.matcher.txID > 0 && datom.Tx > it.matcher.txID {
			continue
		}

		// Extract datom key
		probeKey := extractProbeKey(datom, it.position)

		// Advance binding index while binding < datom
		for it.bindingIdx < len(it.sortedTuples) {
			bindingKey := extractBindingKey(it.sortedTuples[it.bindingIdx], it.position)
			cmp := compareJoinKeys(bindingKey, probeKey)

			if cmp < 0 {
				// Binding < datom: advance binding
				it.bindingIdx++
			} else {
				// Binding >= datom: stop advancing
				break
			}
		}

		if it.bindingIdx >= len(it.sortedTuples) {
			// No more bindings
			return false
		}

		// Check if binding == datom
		bindingKey := extractBindingKey(it.sortedTuples[it.bindingIdx], it.position)
		cmp := compareJoinKeys(bindingKey, probeKey)

		if cmp == 0 {
			// Keys match! Check full pattern match
			if it.matcher.matchesWithBindingTuple(datom, it.pattern, it.bindingRel, it.sortedTuples[it.bindingIdx]) {
				// Apply storage constraints
				satisfiesAll := true
				for _, constraint := range it.constraints {
					if !constraint.Evaluate(datom) {
						satisfiesAll = false
						break
					}
				}

				if satisfiesAll {
					tuple := it.tupleBuilder.BuildTupleInterned(datom)
					if tuple != nil {
						it.current = tuple
						return true
					}
				}
			}
		}
		// If cmp > 0, datom is less than current binding, skip it
	}
	return false
}

func (it *mergeJoinIterator) Tuple() executor.Tuple {
	return it.current
}

func (it *mergeJoinIterator) Close() error {
	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}
