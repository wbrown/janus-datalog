package storage

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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
	// Check for forced strategy override (for testing)
	if m.forceJoinStrategy != nil {
		return *m.forceJoinStrategy
	}

	bindingSize := bindingRel.Size()

	// Estimate pattern cardinality (total datoms that match the constant parts)
	patternCard := m.estimatePatternCardinality(pattern)

	// Calculate selectivity: what % of pattern matches are in the binding set?
	selectivity := float64(bindingSize) / float64(patternCard)

	// Strategy selection based on selectivity and absolute size
	//
	// NOTE: IndexNestedLoop was originally thought to be better for small binding sets,
	// but comprehensive benchmarking revealed that the Sorted() call in matchWithIteratorReuse()
	// (which materializes AND sorts) adds so much overhead that HashJoinScan is faster
	// even for single bindings:
	//
	//   Size 1:  IndexNestedLoop 821µs vs HashJoinScan 204µs (4.0× speedup)
	//   Size 2:  IndexNestedLoop 1522µs vs HashJoinScan 203µs (7.5× speedup)
	//   Size 3:  IndexNestedLoop 2298µs vs HashJoinScan 203µs (11.3× speedup)
	//   Size 10: IndexNestedLoop 7660µs vs HashJoinScan 206µs (37× speedup)
	//
	// The sorting overhead dominates seek cost at all tested binding sizes.
	// See: datalog/storage/join_strategy_threshold_bench_test.go

	// Check if IndexNestedLoop is preferred for small binding sets (configurable via options)
	threshold := m.options.IndexNestedLoopThreshold

	// CRITICAL: Size() returns -1 for streaming relations with unknown size
	// Don't use IndexNestedLoop for unknown sizes (-1 <= 0 would be true!)
	// Default to HashJoinScan for streaming relations
	if bindingSize >= 0 && bindingSize <= threshold {
		return IndexNestedLoop
	}

	// For small to medium-sized binding sets (1-1000), use hash join
	if bindingSize <= 1000 {
		return HashJoinScan
	}

	// For large binding sets (>1000), consider selectivity
	if selectivity < 0.50 {
		// Medium selectivity (<50%): hash join scan
		// Single scan + hash probe is optimal
		return HashJoinScan
	}

	// High selectivity (>50%): merge join, but only for entity-position keys.
	// The merge advance is correct only when three orders agree: the binding
	// sort (CompareValues via Sorted()), the advance comparator (CompareValues),
	// and the storage scan order of the probe stream. That agreement is provable
	// for E-position keys — probe datoms arrive in hash-byte order, which is
	// exactly Identity's CompareValues order, and every binding key is an
	// interned Identity (the typed-position filter drops the rest), so cmp==0
	// coincides with ValuesEqual. Multi-column bindings may repeat a key;
	// mergeJoinIterator pairs each datom with its whole key group. Value-position
	// scan order is the on-disk type-tag order, which deliberately differs from
	// CompareValues' rank order (see datalog/compare.go), so those joins use the
	// order-free hash join instead.
	if position == 0 {
		return MergeJoin
	}
	return HashJoinScan
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
	symbols []query.Symbol,
	position int, // Datom position (0=E, 1=A, 2=V, 3=T)
	index IndexType,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// PHASE 1: Build hash set from binding relation
	// Find which variable is at the datom position, then find its symbol index
	var joinSymbol query.Symbol
	switch position {
	case 0:
		if v, ok := pattern.GetE().(query.Variable); ok {
			joinSymbol = v.Name
		}
	case 1:
		if v, ok := pattern.GetA().(query.Variable); ok {
			joinSymbol = v.Name
		}
	case 2:
		if v, ok := pattern.GetV().(query.Variable); ok {
			joinSymbol = v.Name
		}
	case 3:
		if len(pattern.Elements) > 3 {
			if v, ok := pattern.GetT().(query.Variable); ok {
				joinSymbol = v.Name
			}
		}
	}

	// Find the symbol index of joinSymbol in bindingRel
	symbolIndex := query.SymbolIndex(bindingRel.Symbols(), joinSymbol)

	if symbolIndex == -1 {
		// Variable not in binding relation - shouldn't happen if strategy is correct
		return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), nil
	}

	// Build hash set using symbol index (not datom position)
	hashSet, keyCount, boundValue, err := m.buildHashSet(
		bindingRel, symbolIndex, typedPositionBindingCheck(pattern, bindingRel.Symbols()))
	if err != nil {
		return nil, fmt.Errorf("hash join bindings failed: %w", err)
	}

	if keyCount == 0 {
		// No bindings - return empty result
		return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), nil
	}

	// PHASE 2: Determine scan range for the pattern
	// For single bindings, use the bound value to narrow the scan range
	if keyCount != 1 {
		boundValue = nil
	}
	scanRange := m.calculatePatternScanRangeWithBinding(pattern, index, position, boundValue)

	// PHASE 3: Create storage iterator
	storageIter, err := m.store.ScanKeysOnly(index, scanRange.start, scanRange.end)
	if err != nil {
		return nil, fmt.Errorf("hash join scan failed: %w", err)
	}

	// PHASE 3.5: Wrap with CRDT resolution unless in history mode
	var resolvedIter Iterator
	if m.isHistoryMode() {
		resolvedIter = storageIter
	} else {
		resolvedIter = NewCRDTResolvingIterator(storageIter, m.schema, m.crdtTxID(), m)
	}

	// PHASE 4: Create streaming hash join iterator
	iter := &hashJoinIterator{
		matcher:         m,
		patternString:   pattern.String(),
		matchPlan:       compileBindingMatchPlan(pattern, bindingRel.Symbols()),
		symbols:         symbols,
		position:        position,
		index:           index,
		constraints:     constraints,
		hashSet:         hashSet,
		bindingKeyCount: keyCount,
		iter:            resolvedIter,
		workspace:       make(executor.Tuple, len(symbols)),
		tupleBuilder:    m.getTupleBuilder(pattern, symbols),
	}

	// Return streaming relation
	return executor.NewStreamingRelationWithOptions(symbols, iter, m.options), nil
}

// scanRange holds start and end keys for a storage scan
type scanRange struct {
	start []byte
	end   []byte
}

// calculatePatternScanRange determines the scan range for a pattern
func (m *BadgerMatcher) calculatePatternScanRange(pattern *query.DataPattern, index IndexType) scanRange {
	return m.calculatePatternScanRangeWithBinding(pattern, index, -1, nil)
}

// calculatePatternScanRangeWithBinding determines the scan range for a pattern,
// optionally using a bound value for a specific position to narrow the range
func (m *BadgerMatcher) calculatePatternScanRangeWithBinding(
	pattern *query.DataPattern,
	index IndexType,
	boundPosition int, // -1 means no bound position, 0=E, 1=A, 2=V, 3=T
	boundValue interface{}, // The bound value for that position (nil if no bound)
) scanRange {
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

	// Use bound value if provided for the bound position
	if boundValue != nil {
		switch boundPosition {
		case 0:
			if e == nil {
				e = boundValue
			}
		case 1:
			if a == nil {
				a = boundValue
			}
		case 2:
			if v == nil {
				v = boundValue
			}
		case 3:
			if tx == nil {
				tx = boundValue
			}
		}
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

	encoder := m.encoder

	switch index {
	case EAVT, EATV: // E-primary indices: E first, then A
		if e != nil {
			if entity, ok := e.(datalog.Identity); ok {
				hash := entity.Hash()
				startParts = append(startParts, hash[:])
				endParts = append(endParts, hash[:])

				if a != nil {
					if kw, ok := a.(datalog.Keyword); ok {
						var attr Attribute
						copy(attr[:], kw.String())
						startParts = append(startParts, attr[:])
						endParts = append(endParts, attr[:])
					}
				}
			}
		}

	case AEVT:
		if a != nil {
			if kw, ok := a.(datalog.Keyword); ok {
				var attr Attribute
				copy(attr[:], kw.String())
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

	case AETV: // A-primary CRDT index (A → E → Tx↓ → V)
		// Same prefix structure as AEVT: A first, then E
		if a != nil {
			if kw, ok := a.(datalog.Keyword); ok {
				var attr Attribute
				copy(attr[:], kw.String())
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

	case ATEV: // A → Tx↓ → E → V — chosen by chooseIndex when both A and Tx
		// are bound. Contract: a is a Keyword, tx is an ElementID,
		// e (if set) is an Identity. Type assertions surface a planner bug
		// loudly rather than degrading to a full scan.
		var attr Attribute
		copy(attr[:], a.(datalog.Keyword).String())
		startParts = append(startParts, attr[:])
		endParts = append(endParts, attr[:])

		eid, ok := datalog.DerefElementID(tx)
		if !ok {
			panic(fmt.Sprintf("Tx must be ElementID, got %T", tx))
		}
		encTx := encoder.EncodeTxForPrefix(NewTxFromElementID(eid))
		startParts = append(startParts, encTx)
		endParts = append(endParts, encTx)

		if e != nil {
			hash := e.(datalog.Identity).Hash()
			startParts = append(startParts, hash[:])
			endParts = append(endParts, hash[:])
		}

	case AVET:
		if a != nil {
			if kw, ok := a.(datalog.Keyword); ok {
				var attr Attribute
				copy(attr[:], kw.String())
				startParts = append(startParts, attr[:])
				endParts = append(endParts, attr[:])

				if v != nil {
					// Encode value for the prefix
					// Values in AVET keys are encoded as: [type byte][value data]
					// For Identity/Reference values: [TypeReference][20-byte hash]
					if entity, ok := v.(datalog.Identity); ok && entity != nil {
						hash := entity.Hash()
						vBytes := append([]byte{byte(datalog.TypeReference)}, hash[:]...)
						startParts = append(startParts, vBytes)
						endParts = append(endParts, vBytes)
					}
					// For other value types, we could add similar handling
					// but fall back to attribute-only prefix for now
				}
			}
		}

	case VAET:
		// VAET: Value-Attribute-Entity-Transaction
		// Key format: [index][V][A][E][Op][Tx] (Op before Tx, not between V and A)
		// Values in VAET are encoded with type prefix
		if v != nil {
			// For Identity/Reference values
			if entity, ok := v.(datalog.Identity); ok && entity != nil {
				hash := entity.Hash()
				vBytes := append([]byte{byte(datalog.TypeReference)}, hash[:]...)
				startParts = append(startParts, vBytes)
				endParts = append(endParts, vBytes)

				if a != nil {
					if kw, ok := a.(datalog.Keyword); ok {
						var attr Attribute
						copy(attr[:], kw.String())
						startParts = append(startParts, attr[:])
						endParts = append(endParts, attr[:])
					}
				}
			}
		}

	case TAEV:
		// TAEV: Transaction-Attribute-Entity-Value.
		// Tx is always an ElementID by contract; encode with bitwise-NOT for
		// descending sort order.
		if tx != nil {
			eid, ok := datalog.DerefElementID(tx)
			if !ok {
				panic(fmt.Sprintf("Tx must be ElementID, got %T", tx))
			}
			encTx := encoder.EncodeTxForPrefix(NewTxFromElementID(eid))
			startParts = append(startParts, encTx)
			endParts = append(endParts, encTx)
		}
	}

	start := encoder.EncodePrefix(index, startParts...)
	// Use incrementLastByte for proper prefix range scan
	// This creates an exclusive upper bound that includes all suffixes
	end := incrementLastByte(start)

	return index, start, end
}

// buildHashSet creates a typed hash set from the binding relation. It returns
// all tuples per key plus the distinct-key count and sole key value, when one
// exists, so a single binding can narrow the storage scan. Tuples failing the
// typed-position check (non-Identity entity, non-Keyword attribute — see
// filterTypedPositionBindings) are dropped here: they are typed non-matches,
// and dropping them at construction keeps them away from the probe's
// full-tuple verification.
func (m *BadgerMatcher) buildHashSet(
	bindingRel executor.Relation,
	position int,
	typed func(executor.Tuple) bool,
) (*executor.TupleKeyMap, int, interface{}, error) {
	capacity := bindingRel.Size()
	if capacity < 0 {
		capacity = 0
	}
	hashSet := executor.NewTupleKeyMapWithCapacity(capacity)
	keyCount := 0
	var soleValue interface{}

	iter := bindingRel.Iterator()
	for iter.Next() {
		tuple := iter.Tuple()
		if position >= len(tuple) {
			continue
		}
		if typed != nil && !typed(tuple) {
			// Typed non-match: this tuple's entity/attribute-position value
			// names nothing, so it joins zero rows by definition. Dropping it
			// here is the same result the probe would compute, without ever
			// presenting a mistyped binding to matchesDatom.
			continue
		}

		value := tuple[position]
		if existing, found := hashSet.GetValue(value); found {
			hashSet.PutValue(value, append(existing.([]executor.Tuple), tuple))
		} else {
			// No copy needed: binding relations are materialized
			// (materializeRelationsForPattern), and sliceIterator returns
			// distinct slice references without reusing storage.
			hashSet.PutValue(value, []executor.Tuple{tuple})
			keyCount++
			soleValue = value
		}
	}
	iterErr := iter.Error()
	if closeErr := iter.Close(); iterErr == nil {
		iterErr = closeErr
	}
	if iterErr != nil {
		return nil, 0, nil, iterErr
	}
	return hashSet, keyCount, soleValue, nil
}

// extractProbeKey extracts the value from datom at the specified position.
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
		// Positions are constructor-controlled 0..3; anything else is a bug.
		panic(fmt.Sprintf("BUG: datom position %d out of range", position))
	}
}

type bindingMatchSlot struct {
	constant     interface{}
	hasConstant  bool
	bindingIndex int
}

type bindingMatchPlan struct {
	slots [4]bindingMatchSlot
}

func compileBindingMatchPlan(
	pattern *query.DataPattern,
	bindingSymbols []query.Symbol,
) bindingMatchPlan {
	symbolIndex := make(map[query.Symbol]int, len(bindingSymbols))
	for i, symbol := range bindingSymbols {
		symbolIndex[symbol] = i
	}

	var plan bindingMatchPlan
	for i := range plan.slots {
		plan.slots[i].bindingIndex = -1
	}
	elements := []query.PatternElement{
		pattern.GetE(),
		pattern.GetA(),
		pattern.GetV(),
		pattern.GetT(),
	}
	for i, element := range elements {
		switch value := element.(type) {
		case query.Constant:
			plan.slots[i].constant = value.Value
			plan.slots[i].hasConstant = true
		case query.Variable:
			if index, found := symbolIndex[value.Name]; found {
				plan.slots[i].bindingIndex = index
			}
		}
	}
	return plan
}

func (p bindingMatchPlan) matches(
	matcher *BadgerMatcher,
	datom *datalog.Datom,
	bindingTuple executor.Tuple,
) bool {
	var values [4]interface{}
	for i, slot := range p.slots {
		if slot.hasConstant {
			values[i] = slot.constant
		} else if slot.bindingIndex >= 0 {
			values[i] = bindingTuple[slot.bindingIndex]
		}
	}
	return matcher.matchesDatom(datom, values[0], values[1], values[2], values[3])
}

// matchesWithBindingTuple checks if datom matches pattern with the given binding tuple
func (m *BadgerMatcher) matchesWithBindingTuple(
	datom *datalog.Datom,
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	bindingTuple executor.Tuple,
) bool {
	// Build symbol index for binding relation
	symbols := bindingRel.Symbols()
	symIndex := make(map[query.Symbol]int)
	for i, sym := range symbols {
		symIndex[sym] = i
	}

	// Extract bound values for E, A, V, T
	var e, a, v, tx interface{}

	// E
	if c, ok := pattern.GetE().(query.Constant); ok {
		e = c.Value
	} else if sym, ok := pattern.GetE().(query.Variable); ok {
		if idx, found := symIndex[sym.Name]; found && idx < len(bindingTuple) {
			e = bindingTuple[idx]
		}
	}

	// A
	if c, ok := pattern.GetA().(query.Constant); ok {
		a = c.Value
	} else if sym, ok := pattern.GetA().(query.Variable); ok {
		if idx, found := symIndex[sym.Name]; found && idx < len(bindingTuple) {
			a = bindingTuple[idx]
		}
	}

	// V
	if c, ok := pattern.GetV().(query.Constant); ok {
		v = c.Value
	} else if sym, ok := pattern.GetV().(query.Variable); ok {
		if idx, found := symIndex[sym.Name]; found && idx < len(bindingTuple) {
			v = bindingTuple[idx]
		}
	}

	// T
	if len(pattern.Elements) > 3 {
		if c, ok := pattern.GetT().(query.Constant); ok {
			tx = c.Value
		} else if sym, ok := pattern.GetT().(query.Variable); ok {
			if idx, found := symIndex[sym.Name]; found && idx < len(bindingTuple) {
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
	symbols []query.Symbol,
	position int,
	index IndexType,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// PHASE 1: Sort binding relation by join key
	// Sorted() will auto-materialize if needed
	sortedTuples, err := bindingRel.Sorted()
	if err != nil {
		return nil, err
	}
	// Typed non-matches join zero rows by definition; drop them before
	// merging, exactly as the seek paths do. Filtering preserves the sorted
	// order.
	sortedTuples = filterTypedPositionBindings(pattern, bindingRel.Symbols(), sortedTuples)

	if len(sortedTuples) == 0 {
		// No bindings - return empty result
		return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), nil
	}

	// PHASE 2: Determine scan range for the pattern
	scanRange := m.calculatePatternScanRange(pattern, index)

	// PHASE 3: Create storage iterator
	storageIter, err := m.store.ScanKeysOnly(index, scanRange.start, scanRange.end)
	if err != nil {
		return nil, fmt.Errorf("merge join scan failed: %w", err)
	}

	// PHASE 3.5: Wrap with CRDT resolution unless in history mode
	var resolvedIterMerge Iterator
	if m.isHistoryMode() {
		resolvedIterMerge = storageIter
	} else {
		resolvedIterMerge = NewCRDTResolvingIterator(storageIter, m.schema, m.crdtTxID(), m)
	}

	// PHASE 4: Create streaming merge join iterator
	iter := &mergeJoinIterator{
		matcher:      m,
		pattern:      pattern,
		bindingRel:   bindingRel,
		symbols:      symbols,
		position:     position,
		index:        index,
		constraints:  constraints,
		sortedTuples: sortedTuples,
		bindingIdx:   0,
		iter:         resolvedIterMerge,
		workspace:    make(executor.Tuple, len(symbols)),
		tupleBuilder: m.getTupleBuilder(pattern, symbols),
	}

	// Return streaming relation
	return executor.NewStreamingRelationWithOptions(symbols, iter, m.options), nil
}

// extractBindingKey extracts the join key from a binding tuple at the specified position
func extractBindingKey(tuple executor.Tuple, position int) interface{} {
	if position >= len(tuple) {
		return nil
	}
	return tuple[position]
}

// hashJoinIterator performs lazy hash join iteration
type hashJoinIterator struct {
	matcher         *BadgerMatcher
	patternString   string
	matchPlan       bindingMatchPlan
	symbols         []query.Symbol
	position        int
	index           IndexType
	constraints     []executor.StorageConstraint
	hashSet         *executor.TupleKeyMap // Built upfront - maps key to all matching tuples
	bindingKeyCount int
	iter            Iterator // Storage iterator
	tupleBuilder    *query.InternedTupleBuilder
	current         executor.Tuple
	workspace       executor.Tuple // Reusable workspace for tuple building
	datomsScanned   int            // Track number of datoms scanned for event reporting
	matchesFound    int            // Track number of matches for event reporting
	err             error          // First error from storage operations
}

func (it *hashJoinIterator) Next() bool {
	for it.iter.Next() {
		datom, err := it.iter.Datom()
		if err != nil {
			it.err = err
			return false
		}

		// Count every datom scanned for performance monitoring
		it.datomsScanned++

		// Check transaction validity
		if it.matcher.shouldFilterTx(datom.Tx) {
			continue
		}

		// Extract probe key based on position
		probeKey := extractProbeKey(datom, it.position)

		// Probe hash set (O(1) lookup)
		if bindingTuplesValue, found := it.hashSet.GetValue(probeKey); found {
			bindingTuples := bindingTuplesValue.([]executor.Tuple)
			// Check against ALL binding tuples with this key
			// (for multi-symbol bindings, there may be multiple tuples per key)
			for _, bindingTuple := range bindingTuples {
				// Verify full pattern match
				if it.matchPlan.matches(it.matcher, datom, bindingTuple) {
					// Apply storage constraints
					satisfiesAll := true
					for _, constraint := range it.constraints {
						if !constraint.Evaluate(datom) {
							satisfiesAll = false
							break
						}
					}

					if satisfiesAll {
						it.tupleBuilder.BuildTupleInternedInto(datom, it.workspace)
						it.current = it.workspace
						it.matchesFound++
						return true
					}
				}
			}
		}
	}
	// Inner iterator exhausted — propagate any deferred error (e.g. a
	// CRDTResolvingIterator decode/blob failure that made Next() return false
	// rather than erroring in Datom()).
	if srcErr := it.iter.Error(); srcErr != nil && it.err == nil {
		it.err = srcErr
	}
	return false
}

func (it *hashJoinIterator) Tuple() executor.Tuple {
	return it.current
}

func (it *hashJoinIterator) Close() error {
	// Emit event with scan statistics for performance monitoring
	// ONLY emit if we actually scanned datoms (avoid emitting on unused iterators)
	if it.matcher.handler != nil && it.datomsScanned > 0 {
		it.matcher.handler(annotations.Event{
			Name: "pattern/hash-join-complete",
			Data: map[string]interface{}{
				"pattern":        it.patternString,
				"index":          indexName(it.index),
				"binding.size":   it.bindingKeyCount,
				"datoms.scanned": it.datomsScanned,
				"matches.found":  it.matchesFound,
			},
		})
	}

	if it.iter != nil {
		return it.iter.Close()
	}
	return nil
}

func (it *hashJoinIterator) Error() error { return it.err }

// mergeJoinIterator performs lazy merge join iteration
type mergeJoinIterator struct {
	matcher      *BadgerMatcher
	pattern      *query.DataPattern
	bindingRel   executor.Relation
	symbols      []query.Symbol
	position     int
	index        IndexType
	constraints  []executor.StorageConstraint
	sortedTuples []executor.Tuple // Sorted binding tuples
	bindingIdx   int              // Start of the current key group in sortedTuples
	groupDatom   *datalog.Datom   // Datom being paired with its key group; nil = pull the next datom
	groupOffset  int              // Next tuple within the key group to try against groupDatom
	iter         Iterator         // Storage iterator
	tupleBuilder *query.InternedTupleBuilder
	current      executor.Tuple
	workspace    executor.Tuple // Reusable workspace for tuple building
	err          error          // First error from storage operations
}

// Next advances to the next joined row. Binding tuples are sorted by join
// key, so tuples sharing a key form a consecutive group, and every datom is
// paired with each tuple of its key group — checking only the group's first
// tuple loses rows (it can fail full-pattern verification while a later
// tuple passes, and distinct datoms sharing a key can each match different
// tuples). Group state persists across Next() calls so rows are emitted one
// at a time; groupDatom stays valid between calls because the storage
// iterator only advances after the group is exhausted (see the Iterator
// workspace contract in store.go).
func (it *mergeJoinIterator) Next() bool {
	for {
		// Pair the current datom with the remaining tuples of its key group.
		if it.groupDatom != nil {
			probeKey := extractProbeKey(it.groupDatom, it.position)
			for it.bindingIdx+it.groupOffset < len(it.sortedTuples) {
				tuple := it.sortedTuples[it.bindingIdx+it.groupOffset]
				if datalog.CompareValues(extractBindingKey(tuple, it.position), probeKey) != 0 {
					break // past the key group
				}
				it.groupOffset++
				if it.matcher.matchesWithBindingTuple(it.groupDatom, it.pattern, it.bindingRel, tuple) {
					it.tupleBuilder.BuildTupleInternedInto(it.groupDatom, it.workspace)
					it.current = it.workspace
					return true
				}
			}
			// Group exhausted for this datom. bindingIdx stays at the group
			// start: the next datom may carry the same key.
			it.groupDatom = nil
			it.groupOffset = 0
		}

		if !it.iter.Next() {
			break
		}
		datom, err := it.iter.Datom()
		if err != nil {
			it.err = err
			return false
		}

		// Check transaction validity
		if it.matcher.shouldFilterTx(datom.Tx) {
			continue
		}

		probeKey := extractProbeKey(datom, it.position)

		// Advance past binding key groups below the datom's key.
		for it.bindingIdx < len(it.sortedTuples) &&
			datalog.CompareValues(extractBindingKey(it.sortedTuples[it.bindingIdx], it.position), probeKey) < 0 {
			it.bindingIdx++
		}
		if it.bindingIdx >= len(it.sortedTuples) {
			// No bindings remain; the rest of the scan cannot join.
			break
		}

		if datalog.CompareValues(extractBindingKey(it.sortedTuples[it.bindingIdx], it.position), probeKey) == 0 {
			// Constraints depend only on the datom; evaluate once before
			// pairing it with the group.
			satisfiesAll := true
			for _, constraint := range it.constraints {
				if !constraint.Evaluate(datom) {
					satisfiesAll = false
					break
				}
			}
			if satisfiesAll {
				it.groupDatom = datom
				it.groupOffset = 0
			}
		}
		// cmp > 0: datom below the current binding group; pull the next datom.
	}
	// Scan ended — surface any deferred error rather than presenting a
	// failed scan as an empty one.
	if srcErr := it.iter.Error(); srcErr != nil && it.err == nil {
		it.err = srcErr
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

func (it *mergeJoinIterator) Error() error { return it.err }
