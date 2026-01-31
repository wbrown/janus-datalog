package storage

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Pattern matching implementation is split across multiple files:
//   - matcher_relations.go: Main Match() logic and strategy dispatch
//   - matcher_strategy.go: ReuseStrategy analysis and decision logic
//   - matcher_iterator_reusing.go: Optimized iterator reuse with Seek()
//   - matcher_iterator_nonreusing.go: Simple per-tuple iteration
//   - matcher_iterator_unbound.go: Full scans without bindings
//
// Start with Match() and MatchWithConstraints() in this file.

// DEBUG: Global counter for iterator opens
var globalIteratorOpens int

// ResetIteratorOpenCount resets the global iterator open counter for testing
func ResetIteratorOpenCount() {
	globalIteratorOpens = 0
}

// GetIteratorOpenCount returns the current iterator open count for testing
func GetIteratorOpenCount() int {
	return globalIteratorOpens
}

// Ensure BadgerMatcher implements PatternMatcher
var _ executor.PatternMatcher = (*BadgerMatcher)(nil)

// Ensure BadgerMatcher implements executor.PredicateAwareMatcher
var _ executor.PredicateAwareMatcher = (*BadgerMatcher)(nil)

// Match implements PatternMatcher.Match - returns a Relation directly
func (m *BadgerMatcher) Match(pattern *query.DataPattern, bindings executor.Relations) (executor.Relation, error) {
	// Default implementation with no constraints
	return m.MatchWithConstraints(pattern, bindings, nil)
}

// MatchWithConstraints implements predicate-aware matching with storage-level filtering
func (m *BadgerMatcher) MatchWithConstraints(
	pattern *query.DataPattern,
	bindings executor.Relations,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// Determine pattern columns
	columns := pattern.ExtractColumns()

	if bindings == nil || len(bindings) == 0 {
		// Simple case - no bindings
		return m.matchUnboundAsRelation(pattern, columns, constraints)
	}

	// Find best binding relation for this pattern
	bindingRel := bindings.FindBestForPattern(pattern)
	if bindingRel == nil {
		// No relation binds any pattern variables
		return m.matchUnboundAsRelation(pattern, columns, constraints)
	}

	// CRITICAL FIX: Don't call IsEmpty() on StreamingRelations - it consumes first tuple!
	// See docs/bugs/BUG_ENTITY_JOIN_LOSES_FIRST_TUPLE.md
	// If relation is empty, subsequent iteration will discover that naturally.
	if _, isStreaming := bindingRel.(*executor.StreamingRelation); !isStreaming {
		if bindingRel.IsEmpty() {
			return m.matchUnboundAsRelation(pattern, columns, constraints)
		}
	}

	// Project the binding relation to only include columns used in the pattern
	bindingRel = bindingRel.ProjectFromPattern(pattern)

	// Analyze if we can use iterator reuse
	// For multi-position cases, the relation may be materialized to allow cardinality counting
	strategy, bindingRel := analyzeReuseStrategy(pattern, bindingRel)

	// Emit strategy selection event
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  "storage/reuse-strategy",
			Start: time.Now(),
			Data: map[string]interface{}{
				"pattern":       pattern.String(),
				"strategy_type": strategy.Type.String(),
				"index":         indexName(strategy.Index),
				"position":      strategy.Position,
			},
		})
	}

	// Use appropriate matching strategy
	switch strategy.Type {
	case SinglePositionReuse:
		// Choose join strategy based on selectivity
		joinStrategy := m.chooseJoinStrategy(pattern, bindingRel, strategy.Position)

		// Emit join strategy selection event
		if m.handler != nil {
			m.handler(annotations.Event{
				Name:  "storage/join-strategy",
				Start: time.Now(),
				Data: map[string]interface{}{
					"pattern":       pattern.String(),
					"join_strategy": joinStrategy.String(),
					"position":      strategy.Position,
					"index":         indexName(strategy.Index),
				},
			})
		}

		switch joinStrategy {
		case HashJoinScan:
			// Use hash join for medium selectivity (1-50%)
			return m.matchWithHashJoin(pattern, bindingRel, columns, strategy.Position, strategy.Index, constraints)

		case MergeJoin:
			// Use merge join for high selectivity (>50%) with large binding sets
			return m.matchWithMergeJoin(pattern, bindingRel, columns, strategy.Position, strategy.Index, constraints)

		case IndexNestedLoop:
			// Use iterator reuse for small sets or high selectivity
			return m.matchWithIteratorReuse(pattern, bindingRel, columns, strategy, constraints)

		default:
			// Fall back to iterator reuse
			return m.matchWithIteratorReuse(pattern, bindingRel, columns, strategy, constraints)
		}

	case NoReuse:
		fallthrough
	default:
		// Fall back to opening/closing iterator for each tuple
		return m.matchWithoutIteratorReuse(pattern, bindingRel, columns, constraints)
	}
}

// matchUnboundAsRelation matches a pattern without bindings and returns a Relation
func (m *BadgerMatcher) matchUnboundAsRelation(pattern *query.DataPattern, columns []query.Symbol, constraints []executor.StorageConstraint) (executor.Relation, error) {
	// Extract constant values from pattern
	var e, a, v, tx interface{}

	if elem := pattern.GetE(); elem != nil {
		e = m.extractValue(elem)
	}
	if elem := pattern.GetA(); elem != nil {
		a = m.extractValue(elem)
	}
	if elem := pattern.GetV(); elem != nil {
		v = m.extractValue(elem)
	}
	if elem := pattern.GetT(); elem != nil {
		tx = m.extractValue(elem)
	}

	// Determine cardinality for CRDT resolution
	// For cardinality-one with E+A bound, V unbound: return only current value (first result)
	// For cardinality-many with E+A bound, V unbound: use add-wins resolution
	// For cardinality-many with E+A+V all bound: membership check
	// For cardinality-many with A bound, E unbound: scan all entities with add-wins
	returnOnlyFirst := false
	useAddWinsResolution := false
	useMembershipCheck := false
	useAddWinsScanAllEntities := false
	useAddWinsScanAllEntitiesWithValue := false
	var card schema.Cardinality = schema.CardinalityOne // Default for schemaless

	// Determine cardinality when A is bound (regardless of E)
	if a != nil {
		if m.schema != nil {
			if aKw, ok := a.(datalog.Keyword); ok {
				if def := m.schema.GetAttribute(aKw); def != nil {
					card = def.Cardinality
				}
			}
		}
	}

	// Check resolution strategy based on bound components and cardinality
	if e != nil && a != nil {
		// E and A both bound
		if v == nil {
			// V is unbound
			switch card {
			case schema.CardinalityOne, schema.CardinalityVector:
				// Return only the first (= current) value
				returnOnlyFirst = true
			case schema.CardinalityMany:
				// Use add-wins resolution for set semantics
				useAddWinsResolution = true
			}
		} else {
			// V is bound - for cardinality-many, this is a membership check
			if card == schema.CardinalityMany {
				useMembershipCheck = true
			}
		}
	} else if e == nil && a != nil && card == schema.CardinalityMany {
		// E is unbound, A is bound, cardinality-many
		// Need to scan all entities with this attribute and apply add-wins resolution
		if v == nil {
			// V unbound: return all (entity, value) pairs
			useAddWinsScanAllEntities = true
		} else {
			// V bound: find all entities where this value is in the set
			useAddWinsScanAllEntitiesWithValue = true
		}
	}

	// For cardinality-many with E+A bound, V unbound: use add-wins resolution
	if useAddWinsResolution {
		return m.matchCardinalityManyAsRelation(pattern, columns, e, a)
	}

	// For cardinality-many with E+A+V bound: use membership check
	if useMembershipCheck {
		return m.matchCardinalityManyMembership(pattern, columns, e, a, v)
	}

	// For cardinality-many with A bound, E unbound, V unbound: scan all entities
	if useAddWinsScanAllEntities {
		return m.matchCardinalityManyScanAllEntities(pattern, columns, a)
	}

	// For cardinality-many with A bound, E unbound, V bound: find entities with value
	if useAddWinsScanAllEntitiesWithValue {
		return m.matchCardinalityManyFindEntitiesWithValue(pattern, columns, a, v)
	}

	// Choose index and create scan range
	index, start, end := m.chooseIndex(e, a, v, tx)

	// Emit index selection event if handler is available
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  "pattern/index-selection",
			Start: time.Now(),
			Data: map[string]interface{}{
				"pattern": pattern.String(),
				"index":   indexName(index),
			},
		})
	}

	// Always try to convert constraints to key masks first for efficient filtering
	// The TryConvertConstraintsToMasks function will safely return nil if it can't optimize
	var keyMask *KeyMaskConstraint
	if len(constraints) > 0 {
		// Try for any index, not just AEVT - the function will check compatibility
		keyMask = TryConvertConstraintsToMasks(constraints, index)

		// If we got a mask but don't have the required bounds, clear it
		if keyMask != nil && keyMask.IndexType == AEVT && a == nil {
			keyMask = nil // Can't use AEVT mask without attribute bound
		}
	}

	// Create streaming iterator
	var iter interface {
		Next() bool
		Tuple() executor.Tuple
		Close() error
	}

	if keyMask != nil {
		// Use key mask iterator for efficient filtering
		maskIter := &unboundMaskIterator{
			matcher:         m,
			index:           index,
			start:           start,
			end:             end,
			pattern:         pattern,
			columns:         columns,
			e:               e,
			a:               a,
			v:               v,
			tx:              tx,
			keyMask:         keyMask,
			constraints:     constraints, // Still need for non-mask constraints
			tupleBuilder:    m.getTupleBuilder(pattern, columns),
			returnOnlyFirst: returnOnlyFirst, // CRDT cardinality-one support
		}

		// Initialize the key mask iterator using the optimized method
		storageIter, err := m.store.ScanKeysOnlyWithMask(index, start, end, keyMask)
		if err != nil {
			return nil, fmt.Errorf("key mask scan failed: %w", err)
		}
		maskIter.storageIter = storageIter
		iter = maskIter
	} else {
		// Use regular iterator
		regularIter := &unboundIterator{
			matcher:         m,
			index:           index,
			start:           start,
			end:             end,
			pattern:         pattern,
			columns:         columns,
			e:               e,
			a:               a,
			v:               v,
			tx:              tx,
			constraints:     constraints,
			tupleBuilder:    m.getTupleBuilder(pattern, columns),
			returnOnlyFirst: returnOnlyFirst, // CRDT cardinality-one support
		}

		// Initialize the storage iterator using key-only scanning
		storageIter, err := m.store.ScanKeysOnly(index, start, end)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		regularIter.storageIter = storageIter
		iter = regularIter
	}

	// Return streaming relation with lazy materialization
	// The iterator will be consumed and cached on first call to Iterator(),
	// eliminating the 6.3 GB of upfront allocations while maintaining correctness
	rel := executor.NewStreamingRelationWithOptions(columns, iter, m.options)
	return rel, nil
}

// matchWithoutIteratorReuse uses separate scan for each binding tuple
func (m *BadgerMatcher) matchWithoutIteratorReuse(pattern *query.DataPattern, bindingRel executor.Relation, columns []query.Symbol, constraints []executor.StorageConstraint) (executor.Relation, error) {
	// Emit no-reuse path event
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  "storage/no-reuse-path",
			Start: time.Now(),
			Data: map[string]interface{}{
				"pattern":       pattern.String(),
				"relation_type": fmt.Sprintf("%T", bindingRel),
			},
		})
	}

	// We need to materialize the binding relation to iterate multiple times
	// CRITICAL: Must copy tuples because iterator reuses buffer
	var bindingTuples []executor.Tuple
	it := bindingRel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		tupleCopy := make(executor.Tuple, len(tuple))
		copy(tupleCopy, tuple)
		bindingTuples = append(bindingTuples, tupleCopy)
	}
	it.Close()

	// Create iterator that will scan for each binding tuple
	iter := &nonReusingIterator{
		matcher:          m,
		pattern:          pattern,
		bindingRel:       bindingRel,
		bindingTuples:    bindingTuples,
		columns:          columns,
		constraints:      constraints,
		currentIdx:       -1,
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Columns()),
		tupleBuilder:     m.getTupleBuilder(pattern, columns),
	}

	// Return streaming relation with the iterator
	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}

// matchWithIteratorReuse implements the optimized iterator reuse strategy
func (m *BadgerMatcher) matchWithIteratorReuse(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	strategy ReuseStrategy,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// Get sorted tuples - THIS IS CRITICAL!
	// Without sorted tuples, we cannot use Seek() to jump forward in the iterator
	// Sorted() will auto-materialize if needed
	sortedTuples := bindingRel.Sorted()

	// Create streaming iterator that will reuse storage iterator
	iter := &reusingIterator{
		matcher:          m,
		pattern:          pattern,
		bindingRel:       bindingRel,
		tuples:           sortedTuples,
		position:         strategy.Position,
		index:            strategy.Index,
		columns:          columns,
		constraints:      constraints,
		currentIdx:       -1,
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Columns()),
		tupleBuilder:     m.getTupleBuilder(pattern, columns),
	}

	// Return streaming relation with the iterator
	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}

// matchWithSimpleBatchScanning uses simplified batch scanning to process large binding sets efficiently
func (m *BadgerMatcher) matchWithSimpleBatchScanning(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	strategy ReuseStrategy,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// Determine which index and position to use
	index := strategy.Index
	position := strategy.Position

	// Create simple batch scanner
	scanner := newSimpleBatchScanner(
		m,
		pattern,
		bindingRel,
		position,
		IndexType(index),
		columns,
		constraints,
	)

	// Perform the batch scan
	if err := scanner.Scan(); err != nil {
		return nil, fmt.Errorf("batch scan failed: %w", err)
	}

	// Return streaming relation wrapping the scanner
	// Note: scanner materializes internally but we avoid secondary materialization
	return executor.NewStreamingRelationWithOptions(columns, scanner, m.options), nil
}

// matchWithBatchScanning uses batch scanning to process large binding sets efficiently
func (m *BadgerMatcher) matchWithBatchScanning(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	strategy ReuseStrategy,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// Use the simplified batch scanner
	scanner := newSimpleBatchScanner(
		m,
		pattern,
		bindingRel,
		strategy.Position,
		strategy.Index,
		columns,
		constraints,
	)

	// Perform the scan
	if err := scanner.Scan(); err != nil {
		return nil, err
	}

	// Return streaming relation wrapping the scanner
	// Note: scanner materializes internally but we avoid secondary materialization
	return executor.NewStreamingRelationWithOptions(columns, scanner, m.options), nil
}

// matchCardinalityManyAsRelation handles cardinality-many patterns using add-wins resolution
func (m *BadgerMatcher) matchCardinalityManyAsRelation(
	pattern *query.DataPattern,
	columns []query.Symbol,
	e, a interface{},
) (executor.Relation, error) {
	// Get entity and attribute bytes
	var eBytes [20]byte
	var aBytes [32]byte

	if ident, ok := e.(datalog.Identity); ok {
		copy(eBytes[:], ident.Bytes())
	}
	if kw, ok := a.(datalog.Keyword); ok {
		copy(aBytes[:], kw.String())
	}

	// Resolve the set using add-wins semantics
	result, err := m.resolveAddWinsSet(eBytes[:], aBytes[:])
	if err != nil {
		return nil, fmt.Errorf("add-wins resolution failed: %w", err)
	}

	// Build tuples from resolved members
	// We need to map the values to the correct column positions
	tuples := make([]executor.Tuple, 0, len(result.Members))

	for member := range result.Members {
		tuple := make(executor.Tuple, len(columns))
		for i, col := range columns {
			switch {
			case pattern.GetE() != nil && pattern.GetE().IsVariable() &&
				pattern.GetE().(query.Variable).Name == col:
				tuple[i] = e
			case pattern.GetA() != nil && pattern.GetA().IsVariable() &&
				pattern.GetA().(query.Variable).Name == col:
				tuple[i] = a
			case pattern.GetV() != nil && pattern.GetV().IsVariable() &&
				pattern.GetV().(query.Variable).Name == col:
				tuple[i] = member
			case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
				pattern.GetT().(query.Variable).Name == col:
				// For cardinality-many, we don't have a single Tx
				// Use the max ElementID as the "current" transaction
				tuple[i] = result.MaxElementID
			}
		}
		tuples = append(tuples, tuple)
	}

	// Return materialized relation with the resolved set members
	return executor.NewMaterializedRelation(columns, tuples), nil
}

// matchCardinalityManyMembership checks if a specific value is in a cardinality-many set
func (m *BadgerMatcher) matchCardinalityManyMembership(
	pattern *query.DataPattern,
	columns []query.Symbol,
	e, a, v interface{},
) (executor.Relation, error) {
	// Get entity and attribute bytes
	var eBytes [20]byte
	var aBytes [32]byte

	if ident, ok := e.(datalog.Identity); ok {
		copy(eBytes[:], ident.Bytes())
	}
	if kw, ok := a.(datalog.Keyword); ok {
		copy(aBytes[:], kw.String())
	}

	// Check if the value is in the set using add-wins semantics
	isMember, err := m.checkSetMembership(eBytes[:], aBytes[:], v)
	if err != nil {
		return nil, fmt.Errorf("membership check failed: %w", err)
	}

	// If not a member, return empty relation
	if !isMember {
		return executor.NewMaterializedRelation(columns, nil), nil
	}

	// Value is in set - build result tuple
	tuple := make(executor.Tuple, len(columns))
	for i, col := range columns {
		switch {
		case pattern.GetE() != nil && pattern.GetE().IsVariable() &&
			pattern.GetE().(query.Variable).Name == col:
			tuple[i] = e
		case pattern.GetA() != nil && pattern.GetA().IsVariable() &&
			pattern.GetA().(query.Variable).Name == col:
			tuple[i] = a
		case pattern.GetV() != nil && pattern.GetV().IsVariable() &&
			pattern.GetV().(query.Variable).Name == col:
			tuple[i] = v
		case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
			pattern.GetT().(query.Variable).Name == col:
			// For membership queries, we don't have a specific Tx
			tuple[i] = datalog.ElementID{}
		}
	}

	return executor.NewMaterializedRelation(columns, []executor.Tuple{tuple}), nil
}

// cardinalityManyScanAllEntitiesIterator streams results for [?e :attr ?v] patterns
// where E is unbound and cardinality is many. It iterates through entities at the
// entity level - for each entity, it resolves the add-wins set and yields members one by one.
type cardinalityManyScanAllEntitiesIterator struct {
	matcher      *BadgerMatcher
	pattern      *query.DataPattern
	columns      []query.Symbol
	a            interface{}
	aBytes       [32]byte
	storageIter  Iterator
	seenEntities map[[20]byte]bool

	// Current entity state
	currentEntity         datalog.Identity
	currentSetMembers     []interface{} // Resolved set members for current entity
	currentMaxElementID   datalog.ElementID
	currentMemberIdx      int
	currentTuple          executor.Tuple
}

func (it *cardinalityManyScanAllEntitiesIterator) Next() bool {
	for {
		// If we have members remaining for current entity, yield next
		if it.currentMemberIdx < len(it.currentSetMembers) {
			member := it.currentSetMembers[it.currentMemberIdx]
			it.currentMemberIdx++

			// Build tuple
			tuple := make(executor.Tuple, len(it.columns))
			for i, col := range it.columns {
				switch {
				case it.pattern.GetE() != nil && it.pattern.GetE().IsVariable() &&
					it.pattern.GetE().(query.Variable).Name == col:
					tuple[i] = it.currentEntity
				case it.pattern.GetA() != nil && it.pattern.GetA().IsVariable() &&
					it.pattern.GetA().(query.Variable).Name == col:
					tuple[i] = it.a
				case it.pattern.GetV() != nil && it.pattern.GetV().IsVariable() &&
					it.pattern.GetV().(query.Variable).Name == col:
					tuple[i] = member
				case it.pattern.GetT() != nil && it.pattern.GetT().IsVariable() &&
					it.pattern.GetT().(query.Variable).Name == col:
					tuple[i] = it.currentMaxElementID
				}
			}
			it.currentTuple = tuple
			return true
		}

		// Need to find next entity with non-empty set
		for it.storageIter.Next() {
			datom, err := it.storageIter.Datom()
			if err != nil {
				continue
			}

			// Get entity bytes
			eBytes := datom.E.Hash()

			// Skip if we've already processed this entity
			if it.seenEntities[eBytes] {
				continue
			}
			it.seenEntities[eBytes] = true

			// Resolve the set for this entity using add-wins semantics
			result, err := it.matcher.resolveAddWinsSet(eBytes[:], it.aBytes[:])
			if err != nil {
				continue
			}

			// If set has members, set up iteration
			if len(result.Members) > 0 {
				it.currentEntity = datom.E
				it.currentSetMembers = make([]interface{}, 0, len(result.Members))
				for member := range result.Members {
					it.currentSetMembers = append(it.currentSetMembers, member)
				}
				it.currentMaxElementID = result.MaxElementID
				it.currentMemberIdx = 0
				break // Go back to top of outer loop to yield first member
			}
		}

		// If no more entities or we just found one with members
		if it.currentMemberIdx < len(it.currentSetMembers) {
			continue // Yield first member
		}

		// No more entities
		return false
	}
}

func (it *cardinalityManyScanAllEntitiesIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *cardinalityManyScanAllEntitiesIterator) Close() error {
	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}

// matchCardinalityManyScanAllEntities handles [?e :attr ?v] where E is unbound
// Scans all entities with the attribute and resolves each set using add-wins
func (m *BadgerMatcher) matchCardinalityManyScanAllEntities(
	pattern *query.DataPattern,
	columns []query.Symbol,
	a interface{},
) (executor.Relation, error) {
	// Get attribute bytes
	var aBytes [32]byte
	if kw, ok := a.(datalog.Keyword); ok {
		copy(aBytes[:], kw.String())
	}

	// Scan AEVT to find all entities with this attribute
	// AEVT key format: [prefix:1][A:32][E:20][V:var][Tx:16]
	prefix := make([]byte, 1+32)
	prefix[0] = byte(AEVT)
	copy(prefix[1:33], aBytes[:])

	storageIter, err := m.store.Scan(AEVT, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, fmt.Errorf("AEVT scan failed: %w", err)
	}

	// Create streaming iterator
	iter := &cardinalityManyScanAllEntitiesIterator{
		matcher:      m,
		pattern:      pattern,
		columns:      columns,
		a:            a,
		aBytes:       aBytes,
		storageIter:  storageIter,
		seenEntities: make(map[[20]byte]bool),
	}

	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}

// cardinalityManyFindEntitiesWithValueIterator streams results for [?e :attr "value"] patterns
// where E is unbound and cardinality is many. It iterates through entities and checks
// membership for each, yielding one tuple per entity where the value is in the set.
type cardinalityManyFindEntitiesWithValueIterator struct {
	matcher      *BadgerMatcher
	pattern      *query.DataPattern
	columns      []query.Symbol
	a, v         interface{}
	aBytes       [32]byte
	storageIter  Iterator
	seenEntities map[[20]byte]bool
	currentTuple executor.Tuple
}

func (it *cardinalityManyFindEntitiesWithValueIterator) Next() bool {
	for it.storageIter.Next() {
		datom, err := it.storageIter.Datom()
		if err != nil {
			continue
		}

		// Get entity bytes
		eBytes := datom.E.Hash()

		// Skip if we've already processed this entity
		if it.seenEntities[eBytes] {
			continue
		}
		it.seenEntities[eBytes] = true

		// Check if the specific value is in this entity's set
		isMember, err := it.matcher.checkSetMembership(eBytes[:], it.aBytes[:], it.v)
		if err != nil {
			continue
		}

		if !isMember {
			continue
		}

		// Value is in the set - build tuple
		tuple := make(executor.Tuple, len(it.columns))
		for i, col := range it.columns {
			switch {
			case it.pattern.GetE() != nil && it.pattern.GetE().IsVariable() &&
				it.pattern.GetE().(query.Variable).Name == col:
				tuple[i] = datom.E
			case it.pattern.GetA() != nil && it.pattern.GetA().IsVariable() &&
				it.pattern.GetA().(query.Variable).Name == col:
				tuple[i] = it.a
			case it.pattern.GetV() != nil && it.pattern.GetV().IsVariable() &&
				it.pattern.GetV().(query.Variable).Name == col:
				tuple[i] = it.v
			case it.pattern.GetT() != nil && it.pattern.GetT().IsVariable() &&
				it.pattern.GetT().(query.Variable).Name == col:
				tuple[i] = datalog.ElementID{}
			}
		}
		it.currentTuple = tuple
		return true
	}
	return false
}

func (it *cardinalityManyFindEntitiesWithValueIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *cardinalityManyFindEntitiesWithValueIterator) Close() error {
	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}

// matchCardinalityManyFindEntitiesWithValue handles [?e :attr "value"] where E is unbound
// Finds all entities where the specific value is in the set
func (m *BadgerMatcher) matchCardinalityManyFindEntitiesWithValue(
	pattern *query.DataPattern,
	columns []query.Symbol,
	a, v interface{},
) (executor.Relation, error) {
	// Get attribute bytes
	var aBytes [32]byte
	if kw, ok := a.(datalog.Keyword); ok {
		copy(aBytes[:], kw.String())
	}

	// Scan AEVT to find all entities with this attribute
	prefix := make([]byte, 1+32)
	prefix[0] = byte(AEVT)
	copy(prefix[1:33], aBytes[:])

	storageIter, err := m.store.Scan(AEVT, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, fmt.Errorf("AEVT scan failed: %w", err)
	}

	// Create streaming iterator
	iter := &cardinalityManyFindEntitiesWithValueIterator{
		matcher:      m,
		pattern:      pattern,
		columns:      columns,
		a:            a,
		v:            v,
		aBytes:       aBytes,
		storageIter:  storageIter,
		seenEntities: make(map[[20]byte]bool),
	}

	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}
