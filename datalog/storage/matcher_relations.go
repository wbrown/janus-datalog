package storage

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/codec"
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

	// Check for vector cardinality - requires special handling with bound E from bindings
	// This intercepts BEFORE normal join paths since vectors need RGA resolution
	var aResolved interface{}
	aResolved = m.extractValue(pattern.GetA())
	if aResolved == nil {
		if aVar, ok := pattern.GetA().(query.Variable); ok {
			if kw, found := resolveKeywordFromBindings(aVar, bindings); found {
				aResolved = kw
			}
		}
	}
	if aResolved != nil {
		if m.schema != nil {
			if kw, ok := aResolved.(datalog.Keyword); ok {
				if attr := m.schema.GetAttribute(kw); attr != nil {
					if attr.Cardinality == schema.CardinalityVector {
						return m.matchVectorWithBindings(pattern, bindingRel, columns, kw)
					}
				}
			}
		}
	}

	// CACHE OPTIMIZATION: When A is known (from pattern constant or bindings),
	// use the cache for each E value instead of storage scans.
	if m.cache != nil && m.txID == 0 {
		if aResolved != nil {
			// Phase 1: A has a single known value (from constant or single-row binding)
			if aKw, ok := aResolved.(datalog.Keyword); ok {
				cacheResult, handled := m.matchWithBindingsFromCache(pattern, bindingRel, columns, aKw, -1)
				if handled {
					return cacheResult, nil
				}
			}
		} else if aVar, ok := pattern.GetA().(query.Variable); ok {
			// Phase 2: A varies per row in bindingRel (e.g., from join results)
			aIdx := findVariableColumn(aVar.Name, bindingRel)
			eVar, eIsVar := pattern.GetE().(query.Variable)
			if aIdx >= 0 && eIsVar {
				eIdx := findVariableColumn(eVar.Name, bindingRel)
				if eIdx >= 0 {
					cacheResult, handled := m.matchWithBindingsFromCache(
						pattern, bindingRel, columns, nil, aIdx)
					if handled {
						return cacheResult, nil
					}
				}
			}
		}
	}

	// Analyze if we can use iterator reuse
	// For multi-position cases, the relation may be materialized to allow cardinality counting
	strategy, bindingRel := analyzeReuseStrategy(pattern, bindingRel)

	// For V-bound queries with NeedsValidation, check cardinality
	// Only validate when EXPLICITLY CardinalityOne
	// CardinalityMany uses add-wins, CardinalityUnknown/schemaless emits all
	// See docs/reference/INDEX_SELECTION_PROOF.md Theorem 3b
	if strategy.NeedsValidation && strategy.BoundA != nil {
		// Default: no validation (schemaless = Datascript-style = emit all)
		strategy.NeedsValidation = false

		if m.schema != nil {
			if aKw, ok := strategy.BoundA.(datalog.Keyword); ok {
				if def := m.schema.GetAttribute(aKw); def != nil {
					if def.Cardinality == schema.CardinalityOne {
						// Only explicit CardinalityOne needs validation
						strategy.NeedsValidation = true
					}
				}
			}
		}
	}

	// Emit strategy selection event
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  "storage/reuse-strategy",
			Start: time.Now(),
			Data: map[string]interface{}{
				"pattern":          pattern.String(),
				"strategy_type":    strategy.Type.String(),
				"index":            indexName(strategy.Index),
				"position":         strategy.Position,
				"needs_validation": strategy.NeedsValidation,
				"bound_a":          fmt.Sprintf("%v", strategy.BoundA),
			},
		})
	}

	// Use appropriate matching strategy
	switch strategy.Type {
	case SinglePositionReuse:
		// For V-bound cardinality-one queries, use candidate + validate pattern
		// See docs/reference/INDEX_SELECTION_PROOF.md Theorem 4
		if strategy.NeedsValidation {
			return m.matchWithVValidation(pattern, bindingRel, columns, strategy, constraints)
		}

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
	// For cardinality-vector with E+A bound: use vector resolution
	returnOnlyFirst := false
	useAddWinsResolution := false
	useMembershipCheck := false
	useAddWinsScanAllEntities := false
	useAddWinsScanAllEntitiesWithValue := false
	useVectorResolution := false
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

	// CACHE OPTIMIZATION: When E and A are bound and we're querying latest state,
	// use the cache for O(1) access instead of storage scans.
	if m.cache != nil && m.txID == 0 && e != nil && a != nil {
		if eIdent, ok := e.(datalog.Identity); ok {
			if aKw, ok := a.(datalog.Keyword); ok {
				cacheResult, handled := m.matchFromCache(pattern, columns, eIdent, aKw, v, card)
				if handled {
					return cacheResult, nil
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
			case schema.CardinalityOne:
				// Return only the first (= current) value
				returnOnlyFirst = true
			case schema.CardinalityVector:
				// Use vector resolution (RGA reconstruction)
				useVectorResolution = true
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
	} else if e == nil && a != nil && v != nil && card == schema.CardinalityOne {
		// E is unbound, A is bound, V is constant, cardinality-one
		// V-bound scan (AVET) only sees datoms with this V, but the LWW winner
		// may have a different V. Use candidate + validate pattern.
		// See docs/reference/INDEX_SELECTION_PROOF.md Theorem 2 & 5.
		dummyCol := datalog.NewSymbol("__v_bound_dummy__")
		bindingRel := executor.NewMaterializedRelation(
			[]query.Symbol{dummyCol},
			[]executor.Tuple{{v}},
		)
		strategy := ReuseStrategy{
			Type:            SinglePositionReuse,
			NeedsValidation: true,
			Index:           AVET,
			ValidationIndex: EATV,
			BoundA:          a,
			BoundV:          v,
		}
		return m.matchWithVValidation(pattern, bindingRel, columns, strategy, nil)
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

	// For cardinality-vector with E+A bound: use vector resolution
	if useVectorResolution {
		return m.matchCardinalityVectorAsRelation(pattern, columns, e, a)
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
				"pattern":    pattern.String(),
				"index":      indexName(index),
				"scan.start": codec.EncodeL85(start),
				"scan.end":   codec.EncodeL85(end),
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
			workspace:       make(executor.Tuple, len(columns)),
			tupleBuilder:    m.getTupleBuilder(pattern, columns),
			returnOnlyFirst: returnOnlyFirst, // CRDT cardinality-one support
		}

		// Initialize the key mask iterator using the optimized method
		rawStorageIter, err := m.store.ScanKeysOnlyWithMask(index, start, end, keyMask)
		if err != nil {
			return nil, fmt.Errorf("key mask scan failed: %w", err)
		}

		// Wrap with CRDT resolution for per-(E, A) group resolution
		// Always applied: CRDTResolvingIterator handles nil schema correctly
		// (defaults to CardinalityUnknown → add-wins semantics)
		maskIter.storageIter = NewCRDTResolvingIterator(rawStorageIter, m.schema, m.txID)
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
			workspace:       make(executor.Tuple, len(columns)),
			tupleBuilder:    m.getTupleBuilder(pattern, columns),
			returnOnlyFirst: returnOnlyFirst, // CRDT cardinality-one support
		}

		// Initialize the storage iterator using key-only scanning
		rawStorageIter, err := m.store.ScanKeysOnly(index, start, end)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		// Wrap with CRDT resolution for per-(E, A) group resolution
		// Always applied: CRDTResolvingIterator handles nil schema correctly
		// (defaults to CardinalityUnknown → add-wins semantics)
		regularIter.storageIter = NewCRDTResolvingIterator(rawStorageIter, m.schema, m.txID)
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
		workspace:        make(executor.Tuple, len(columns)),
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
		workspace:        make(executor.Tuple, len(columns)),
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Columns()),
		tupleBuilder:     m.getTupleBuilder(pattern, columns),
	}

	// Return streaming relation with the iterator
	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}

// matchWithVValidation implements the candidate + validate pattern for V-bound
// cardinality-one queries. See docs/reference/INDEX_SELECTION_PROOF.md Theorem 4.
//
// Algorithm:
// 1. Scan V-primary index (AVET/VAET) for candidate entities
// 2. For each candidate E, point-lookup EATV to get CRDT winner
// 3. If winner.V == boundV, emit; otherwise skip (stale candidate)
func (m *BadgerMatcher) matchWithVValidation(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	strategy ReuseStrategy,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  "v-validation/entry",
			Start: time.Now(),
			Data: map[string]any{
				"pattern":     pattern.String(),
				"index":       indexName(strategy.Index),
				"bound_a":     fmt.Sprintf("%v", strategy.BoundA),
				"binding_rel": bindingRel.Columns(),
			},
		})
	}
	// Get sorted tuples for efficient iteration
	sortedTuples := bindingRel.Sorted()

	// Create validating iterator
	iter := &validatingVBoundIterator{
		matcher:          m,
		pattern:          pattern,
		bindingRel:       bindingRel,
		tuples:           sortedTuples,
		candidateIndex:   strategy.Index,           // AVET or VAET for candidates
		validationIndex:  strategy.ValidationIndex, // EATV for validation
		boundA:           strategy.BoundA,          // Constant A value if any
		columns:          columns,
		constraints:      constraints,
		currentTupleIdx:  -1,
		workspace:        make(executor.Tuple, len(columns)),
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Columns()),
		tupleBuilder:     m.getTupleBuilder(pattern, columns),
	}

	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}

// validatingVBoundIterator implements the semi-join pattern for V-bound queries.
// It wraps a VAET scan with CRDTResolvingIterator for correct CRDT semantics,
// then post-validates CardinalityOne emissions with EATV point lookup.
//
// Architecture (per proof doc):
//  1. VAET scan wrapped with CRDTResolvingIterator
//  2. CRDTResolvingIterator handles: group deduplication (O(1) space),
//     add-wins with same-Tx tiebreaking, RGA for vectors
//  3. Only CardinalityOne needs post-validation (LWW winner may have different V)
type validatingVBoundIterator struct {
	matcher     *BadgerMatcher
	pattern     *query.DataPattern
	bindingRel  executor.Relation
	tuples      []executor.Tuple
	columns     []query.Symbol
	constraints []executor.StorageConstraint

	// Index configuration
	candidateIndex  IndexType // AVET or VAET
	validationIndex IndexType // EATV
	boundA          any       // Constant A value (nil if A is variable)

	// Iterator state - CRDTResolvingIterator wraps the raw scan
	crdtIter        *CRDTResolvingIterator // Wraps VAET/AVET scan
	rawIter         Iterator               // Underlying raw iterator (for Close)
	currentTupleIdx int                    // Current binding tuple index
	currentTuple    executor.Tuple         // Current result tuple
	workspace       executor.Tuple

	// Pattern extraction
	patternExtractor *query.PatternExtractor
	tupleBuilder     *query.InternedTupleBuilder

	// Current V value being searched
	currentBoundV any
}

func (it *validatingVBoundIterator) Next() bool {
	for {
		// Try to get next CRDT-resolved result from current scan
		if it.crdtIter != nil {
			for it.crdtIter.Next() {
				datom, err := it.crdtIter.Datom()
				if err != nil {
					continue
				}

				// Emit annotation for CRDT-resolved candidate
				if it.matcher.handler != nil {
					it.matcher.handler(annotations.Event{
						Name:  "v-validation/candidate",
						Start: time.Now(),
						Data: map[string]any{
							"e":     datom.E.String(),
							"a":     datom.A.String(),
							"v":     fmt.Sprintf("%v", datom.V),
							"tx":    datom.Tx.String(),
							"op":    fmt.Sprintf("%d", datom.Op),
							"bound": fmt.Sprintf("%v", it.currentBoundV),
						},
					})
				}

				// CRDTResolvingIterator already handled:
				// - CardinalityMany: add-wins with same-Tx tiebreaking
				// - CardinalityVector: RGA resolution
				// - CardinalityUnknown: add-wins (same as CardinalityMany)
				//
				// Only CardinalityOne needs post-validation because the
				// LWW winner may have a different V than our bound V.
				card := it.getCardinalityEnum(datom.A)
				if card == schema.CardinalityOne {
					if !it.validateCandidate(datom.E, datom.A) {
						// Stale candidate - LWW winner has different V
						continue
					}
				}

				// Build result tuple
				it.currentTuple = it.buildTuple(datom)
				return true
			}
			it.crdtIter.Close()
			it.crdtIter = nil
			it.rawIter = nil
		}

		// Move to next binding tuple
		it.currentTupleIdx++
		if it.currentTupleIdx >= len(it.tuples) {
			return false
		}

		// Get V value from binding tuple
		tuple := it.tuples[it.currentTupleIdx]
		it.currentBoundV = it.patternExtractor.ExtractV(tuple)
		if it.currentBoundV == nil {
			continue // V not bound in this tuple
		}

		// Open CRDT-resolving scan on V-primary index
		var err error
		it.crdtIter, it.rawIter, err = it.openCRDTScan()
		if err != nil {
			continue
		}
	}
}

// validateCandidate checks if the current value of (E, A) matches boundV
func (it *validatingVBoundIterator) validateCandidate(e datalog.Identity, a datalog.Keyword) bool {
	encoder := it.matcher.store.encoder

	// Convert E to storage bytes
	eBytes := ToStorageDatom(datalog.Datom{E: e}).E

	// Convert A to storage bytes
	aPtr := datalog.NewKeyword(a.String())
	aStorage := ToStorageDatom(datalog.Datom{A: aPtr}).A

	// Point lookup on EATV: scan (E, A) prefix, first result is CRDT winner
	start, end := encoder.EncodePrefixRange(it.validationIndex, eBytes[:], aStorage[:])
	rawIter, err := it.matcher.store.ScanKeysOnly(it.validationIndex, start, end)
	if err != nil {
		return false
	}
	defer rawIter.Close()

	// First result is the CRDT winner (Tx descending in EATV)
	if !rawIter.Next() {
		if it.matcher.handler != nil {
			it.matcher.handler(annotations.Event{
				Name:  "v-validation/no-winner",
				Start: time.Now(),
				Data: map[string]any{
					"e":     e.String(),
					"a":     a.String(),
					"bound": fmt.Sprintf("%v", it.currentBoundV),
				},
			})
		}
		return false // No current value
	}

	winner, err := rawIter.Datom()
	if err != nil {
		return false
	}

	// Check Op: if the latest operation is a tombstone, the attribute doesn't exist.
	// No V can match a tombstoned attribute.
	if winner.Op == datalog.OpCRDTRemove {
		return false
	}

	// Check if winner's V matches our bound V
	matches := winner.V == it.currentBoundV

	if it.matcher.handler != nil {
		it.matcher.handler(annotations.Event{
			Name:  "v-validation/result",
			Start: time.Now(),
			Data: map[string]any{
				"e":           e.String(),
				"a":           a.String(),
				"bound_v":     fmt.Sprintf("%v", it.currentBoundV),
				"winner_v":    fmt.Sprintf("%v", winner.V),
				"winner_tx":   winner.Tx.String(),
				"winner_op":   fmt.Sprintf("%d", winner.Op),
				"matches":     matches,
				"will_emit":   matches,
				"cardinality": it.getCardinality(a),
			},
		})
	}

	return matches
}

// getCardinality looks up the cardinality for an attribute (for annotations)
func (it *validatingVBoundIterator) getCardinality(a datalog.Keyword) string {
	if it.matcher.schema == nil {
		return "unknown"
	}
	def := it.matcher.schema.GetAttribute(a)
	if def == nil {
		return "unknown"
	}
	switch def.Cardinality {
	case schema.CardinalityOne:
		return "one"
	case schema.CardinalityMany:
		return "many"
	case schema.CardinalityVector:
		return "vector"
	default:
		return "unknown"
	}
}

// getCardinalityEnum looks up the cardinality enum for an attribute
// Returns CardinalityUnknown (0) for schemaless or undefined attributes
func (it *validatingVBoundIterator) getCardinalityEnum(a datalog.Keyword) schema.Cardinality {
	if it.matcher.schema == nil {
		return schema.CardinalityUnknown
	}
	def := it.matcher.schema.GetAttribute(a)
	if def == nil {
		return schema.CardinalityUnknown
	}
	return def.Cardinality
}

// openCRDTScan opens a CRDT-resolving scan on the V-primary index for current bound V.
// Returns both the CRDTResolvingIterator wrapper and the raw iterator (for proper Close).
func (it *validatingVBoundIterator) openCRDTScan() (*CRDTResolvingIterator, Iterator, error) {
	encoder := it.matcher.store.encoder
	var start, end []byte

	if it.matcher.handler != nil {
		it.matcher.handler(annotations.Event{
			Name:  "v-validation/open-scan",
			Start: time.Now(),
			Data: map[string]any{
				"bound_v": fmt.Sprintf("%v", it.currentBoundV),
				"bound_a": fmt.Sprintf("%v", it.boundA),
				"index":   indexName(it.candidateIndex),
			},
		})
	}

	if it.boundA != nil {
		// A is constant: use AVET with (A, V) prefix
		aKw, ok := it.boundA.(datalog.Keyword)
		if !ok {
			return nil, nil, fmt.Errorf("boundA is not a Keyword")
		}

		// Convert A to storage bytes
		aPtr := datalog.NewKeyword(aKw.String())
		aStorage := ToStorageDatom(datalog.Datom{A: aPtr}).A

		// Encode V with type prefix
		valueBytes := it.encodeValue(it.currentBoundV)

		start, end = encoder.EncodePrefixRange(it.candidateIndex, aStorage[:], valueBytes)
	} else {
		// A is variable: use VAET with V prefix
		valueBytes := it.encodeValue(it.currentBoundV)
		start, end = encoder.EncodePrefixRange(it.candidateIndex, valueBytes)

		if it.matcher.handler != nil {
			it.matcher.handler(annotations.Event{
				Name:  "v-validation/scan-range",
				Start: time.Now(),
				Data: map[string]any{
					"value_bytes": fmt.Sprintf("%x", valueBytes),
					"start":       fmt.Sprintf("%x", start),
					"end":         fmt.Sprintf("%x", end),
				},
			})
		}
	}

	// Raw scan on V-primary index
	rawIter, err := it.matcher.store.ScanKeysOnly(it.candidateIndex, start, end)
	if err != nil {
		return nil, nil, err
	}

	// Wrap with CRDTResolvingIterator for correct CRDT semantics
	// CRDTResolvingIterator handles:
	// - Group deduplication via contiguous comparison (O(1) space)
	// - Add-wins with same-Tx tiebreaking for CardinalityMany
	// - RGA for CardinalityVector
	// - Add-wins for CardinalityUnknown (same as CardinalityMany)
	crdtIter := NewCRDTResolvingIterator(rawIter, it.matcher.schema, 0)

	if it.matcher.handler != nil {
		it.matcher.handler(annotations.Event{
			Name:  "v-validation/scan-opened",
			Start: time.Now(),
			Data: map[string]any{
				"index":        indexName(it.candidateIndex),
				"crdt_wrapped": true,
			},
		})
	}

	return crdtIter, rawIter, nil
}

// encodeValue converts a value to bytes for index prefix
func (it *validatingVBoundIterator) encodeValue(v any) []byte {
	encoder := it.matcher.store.encoder

	// Create dummy datom for encoding
	dummyDatom := &datalog.Datom{
		E:  datalog.NewIdentity(""),
		A:  datalog.NewKeyword(":dummy"),
		V:  v,
		Tx: datalog.ElementID{},
	}
	sDatom := ToStorageDatom(*dummyDatom)
	vType := byte(datalog.Type(sDatom.V))

	// Check if we're using L85 encoding and have a reference value
	if _, isL85 := encoder.(*L85KeyEncoder); isL85 && vType == byte(datalog.TypeReference) {
		var vArr [20]byte
		copy(vArr[:], datalog.ValueBytes(sDatom.V))
		return append([]byte{vType}, []byte(codec.EncodeFixed20(vArr))...)
	}

	// Binary encoder or non-reference values: type + raw bytes
	vData := datalog.ValueBytes(sDatom.V)
	return append([]byte{vType}, vData...)
}

// buildTuple creates a result tuple from a validated datom
func (it *validatingVBoundIterator) buildTuple(datom *datalog.Datom) executor.Tuple {
	tuple := make(executor.Tuple, len(it.columns))
	for i, col := range it.columns {
		// Check each pattern element - might be Variable or Constant
		if v, ok := it.pattern.GetE().(query.Variable); ok && col == v.Name {
			tuple[i] = datom.E
			continue
		}
		if v, ok := it.pattern.GetA().(query.Variable); ok && col == v.Name {
			tuple[i] = datom.A
			continue
		}
		if v, ok := it.pattern.GetV().(query.Variable); ok && col == v.Name {
			tuple[i] = datom.V
			continue
		}
		if len(it.pattern.Elements) > 3 {
			if v, ok := it.pattern.GetT().(query.Variable); ok && col == v.Name {
				tuple[i] = datom.Tx
			}
		}
	}
	return tuple
}

func (it *validatingVBoundIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *validatingVBoundIterator) Close() error {
	if it.crdtIter != nil {
		it.crdtIter.Close()
		it.crdtIter = nil
	}
	if it.rawIter != nil {
		it.rawIter.Close()
		it.rawIter = nil
	}
	return nil
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

// matchFromCache attempts to resolve a pattern using the cache.
// Returns (relation, true) if cache was used, (nil, false) if fallback to storage is needed.
// This provides O(1) access for patterns with E and A bound when querying latest state.
func (m *BadgerMatcher) matchFromCache(
	pattern *query.DataPattern,
	columns []query.Symbol,
	e datalog.Identity,
	a datalog.Keyword,
	v interface{}, // nil if V is unbound
	card schema.Cardinality,
) (executor.Relation, bool) {
	// Build cache key
	eBytes := Entity(e.Hash())
	aStorage := ToStorageDatom(datalog.Datom{A: a}).A
	var aAttr Attribute
	copy(aAttr[:], aStorage[:])
	key := CacheKey{E: eBytes, A: aAttr}

	// Get or resolve from cache
	entry := m.cache.GetOrResolve(key, m)
	if entry == nil {
		return nil, false // Fallback to storage
	}

	// Get tuple builder for building tuples
	tupleBuilder := m.getTupleBuilder(pattern, columns)

	// Pre-allocate datom buffer with constant E and A
	var datomBuf datalog.Datom
	datomBuf.E = e
	datomBuf.A = a

	// Helper to build tuple from datom (reuses buffer)
	buildTuple := func(val interface{}, tx datalog.ElementID) executor.Tuple {
		datomBuf.V = val
		datomBuf.Tx = tx
		return tupleBuilder.BuildTupleInterned(&datomBuf)
	}

	switch card {
	case schema.CardinalityOne:
		val := entry.OneValue()
		if val == nil {
			// No value - return empty relation
			return executor.NewMaterializedRelationWithOptions(columns, nil, m.options), true
		}
		if v != nil {
			// V is bound - check if it matches
			if !valuesEqual(val, v) {
				return executor.NewMaterializedRelationWithOptions(columns, nil, m.options), true
			}
		}
		// Build tuple with the cached value
		tuple := buildTuple(val, entry.Version())
		return executor.NewMaterializedRelationWithOptions(columns, []executor.Tuple{tuple}, m.options), true

	case schema.CardinalityMany:
		set := entry.ManySet()
		if len(set) == 0 {
			return executor.NewMaterializedRelationWithOptions(columns, nil, m.options), true
		}
		if v != nil {
			// V is bound - membership check
			if set[v] {
				tuple := buildTuple(v, entry.Version())
				return executor.NewMaterializedRelationWithOptions(columns, []executor.Tuple{tuple}, m.options), true
			}
			return executor.NewMaterializedRelationWithOptions(columns, nil, m.options), true
		}
		// V is unbound - return all set members
		tuples := make([]executor.Tuple, 0, len(set))
		for val := range set {
			tuple := buildTuple(val, entry.Version())
			tuples = append(tuples, tuple)
		}
		return executor.NewMaterializedRelationWithOptions(columns, tuples, m.options), true

	case schema.CardinalityVector:
		list := entry.VectorList()
		if len(list) == 0 {
			return executor.NewMaterializedRelationWithOptions(columns, nil, m.options), true
		}
		if v != nil {
			// V is bound - check if vector equals (for vector, V is the whole list)
			// This is an edge case - usually V is unbound for vectors
			return nil, false // Fallback to storage for this case
		}
		// V is unbound - return the vector as a single value
		tuple := buildTuple(list, entry.Version())
		return executor.NewMaterializedRelationWithOptions(columns, []executor.Tuple{tuple}, m.options), true
	}

	return nil, false // Unknown cardinality, fallback to storage
}

// findVariableColumn returns the column index for a variable in a relation, or -1.
func findVariableColumn(sym query.Symbol, rel executor.Relation) int {
	for i, col := range rel.Columns() {
		if col == sym {
			return i
		}
	}
	return -1
}

// resolveKeywordFromBindings searches all binding relations for a single-row relation
// containing the given variable and returns its value as a Keyword. This covers scalar
// inputs and single-row tuple inputs where A has exactly one known value.
func resolveKeywordFromBindings(aVar query.Variable, bindings executor.Relations) (datalog.Keyword, bool) {
	for _, rel := range bindings {
		idx := findVariableColumn(aVar.Name, rel)
		if idx < 0 {
			continue
		}
		if rel.Size() != 1 {
			return nil, false // multi-row — can't extract single value
		}
		iter := rel.Iterator()
		defer iter.Close()
		if iter.Next() {
			if kw, ok := iter.Tuple()[idx].(datalog.Keyword); ok {
				return kw, true
			}
		}
		return nil, false
	}
	return nil, false
}

// matchWithBindingsFromCache handles patterns where E comes from bindings and A is known.
// Uses cache for O(1) lookup per bound E value instead of storage scans.
// Returns (relation, true) if cache was used, (nil, false) if fallback to storage is needed.
//
// When aColIdx >= 0, A is extracted per-row from bindingRel[aColIdx] instead of using
// the fixed `a` parameter. This handles the case where both E and A are columns in the
// binding relation (e.g., from join results with varying attributes per row).
func (m *BadgerMatcher) matchWithBindingsFromCache(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	a datalog.Keyword,
	aColIdx int,
) (executor.Relation, bool) {
	// Find which column in the binding relation has E
	eVar, isVar := pattern.GetE().(query.Variable)
	if !isVar {
		return nil, false // E is not a variable, can't get it from bindings
	}

	bindingColumns := bindingRel.Columns()
	eColIdx := -1
	for i, col := range bindingColumns {
		if col == eVar.Name {
			eColIdx = i
			break
		}
	}
	if eColIdx < 0 {
		return nil, false // E variable not in bindings
	}

	// Pre-compute fixed-A values when A is constant across all rows
	var fixedCard schema.Cardinality
	var fixedAAttr Attribute
	if aColIdx < 0 {
		fixedCard = schema.CardinalityOne
		if m.schema != nil {
			if def := m.schema.GetAttribute(a); def != nil {
				fixedCard = def.Cardinality
			}
		}
		aStorage := ToStorageDatom(datalog.Datom{A: a}).A
		copy(fixedAAttr[:], aStorage[:])
	}

	// Extract V if bound
	var v interface{}
	if elem := pattern.GetV(); elem != nil {
		v = m.extractValue(elem)
	}

	// Get tuple builder
	tupleBuilder := m.getTupleBuilder(pattern, columns)

	// Pre-allocate datom buffer
	var datomBuf datalog.Datom
	if aColIdx < 0 {
		datomBuf.A = a // constant A across all rows
	}

	buildTuple := func(e datalog.Identity, val interface{}, tx datalog.ElementID) executor.Tuple {
		datomBuf.E = e
		datomBuf.V = val
		datomBuf.Tx = tx
		return tupleBuilder.BuildTupleInterned(&datomBuf)
	}

	// Iterate bindings and collect results from cache
	var resultTuples []executor.Tuple
	iter := bindingRel.Iterator()
	defer iter.Close()

	for iter.Next() {
		tuple := iter.Tuple()
		if eColIdx >= len(tuple) {
			continue
		}

		// Get E value from binding
		eVal := tuple[eColIdx]
		eIdent, ok := eVal.(datalog.Identity)
		if !ok {
			// Try to convert if it's a different type
			if id, ok := eVal.(*datalog.Identity); ok {
				eIdent = *id
			} else {
				continue // Can't use cache for non-Identity E
			}
		}

		// Determine A and cardinality for this row
		var rowCard schema.Cardinality
		var rowAAttr Attribute
		if aColIdx >= 0 {
			// Per-row A: extract from binding tuple
			if aColIdx >= len(tuple) {
				continue
			}
			aKw, ok := tuple[aColIdx].(datalog.Keyword)
			if !ok {
				continue
			}
			rowCard = schema.CardinalityOne
			if m.schema != nil {
				if def := m.schema.GetAttribute(aKw); def != nil {
					rowCard = def.Cardinality
				}
			}
			aStorage := ToStorageDatom(datalog.Datom{A: aKw}).A
			copy(rowAAttr[:], aStorage[:])
			datomBuf.A = aKw
		} else {
			rowCard = fixedCard
			rowAAttr = fixedAAttr
		}

		// Build cache key
		eBytes := Entity(eIdent.Hash())
		key := CacheKey{E: eBytes, A: rowAAttr}

		// Get from cache
		entry := m.cache.GetOrResolve(key, m)
		if entry == nil {
			// Cache miss - fallback to storage for entire query
			return nil, false
		}

		// Process based on cardinality
		switch rowCard {
		case schema.CardinalityOne:
			val := entry.OneValue()
			if val == nil {
				continue // No value for this E
			}
			if v != nil && !valuesEqual(val, v) {
				continue // Value doesn't match bound V
			}
			resultTuples = append(resultTuples, buildTuple(eIdent, val, entry.Version()))

		case schema.CardinalityMany:
			set := entry.ManySet()
			if len(set) == 0 {
				continue
			}
			if v != nil {
				// Membership check
				if set[v] {
					resultTuples = append(resultTuples, buildTuple(eIdent, v, entry.Version()))
				}
			} else {
				// Return all set members
				for val := range set {
					resultTuples = append(resultTuples, buildTuple(eIdent, val, entry.Version()))
				}
			}

		case schema.CardinalityVector:
			list := entry.VectorList()
			if len(list) == 0 {
				continue
			}
			if v != nil {
				// Can't efficiently check vector membership
				return nil, false
			}
			resultTuples = append(resultTuples, buildTuple(eIdent, list, entry.Version()))
		}
	}

	return executor.NewMaterializedRelationWithOptions(columns, resultTuples, m.options), true
}

// valuesEqual compares two values for equality
func valuesEqual(a, b interface{}) bool {
	// Handle common types directly for performance
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return av == bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	case datalog.Identity:
		if bv, ok := b.(datalog.Identity); ok {
			return av.Hash() == bv.Hash()
		}
	case datalog.Keyword:
		if bv, ok := b.(datalog.Keyword); ok {
			return av == bv
		}
	}
	// Fallback to reflect-based comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
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

// matchCardinalityVectorAsRelation handles cardinality-vector patterns using RGA resolution.
// Returns the entire reconstructed vector as a single value bound to the V variable.
func (m *BadgerMatcher) matchCardinalityVectorAsRelation(
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

	// Resolve the vector using RGA reconstruction
	result, err := m.resolveVector(eBytes[:], aBytes[:])
	if err != nil {
		return nil, fmt.Errorf("vector resolution failed: %w", err)
	}

	// If empty vector, return empty relation
	if len(result.Elements) == 0 {
		return executor.NewMaterializedRelation(columns, nil), nil
	}

	// Build a single tuple with the entire vector as the V value
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
			// Return the entire vector as a []any
			tuple[i] = result.Elements
		case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
			pattern.GetT().(query.Variable).Name == col:
			// Use the max ElementID as the "current" transaction
			tuple[i] = result.MaxElementID
		}
	}

	// Return single-row relation with the vector
	return executor.NewMaterializedRelation(columns, []executor.Tuple{tuple}), nil
}

// matchVectorWithBindings handles vector patterns when E is bound via join bindings.
// For each entity in the bindings, it resolves the vector using RGA reconstruction
// and returns tuples with the reconstructed vector as the V value.
func (m *BadgerMatcher) matchVectorWithBindings(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	columns []query.Symbol,
	attr datalog.Keyword,
) (executor.Relation, error) {
	// Find which column in bindings provides the entity
	var eColIdx int = -1
	bindingCols := bindingRel.Columns()

	// Find the entity variable in the pattern and match it to binding columns
	if pattern.GetE() != nil && pattern.GetE().IsVariable() {
		eVar := pattern.GetE().(query.Variable).Name
		for i, col := range bindingCols {
			if col == eVar {
				eColIdx = i
				break
			}
		}
	}

	if eColIdx == -1 {
		// E is not bound from bindings - fall back to normal path
		// This shouldn't happen if we got here, but be safe
		return executor.NewMaterializedRelation(columns, nil), nil
	}

	// Get attribute bytes once
	var aBytes [32]byte
	copy(aBytes[:], attr.String())

	// Iterate through bindings and resolve vector for each entity
	var tuples []executor.Tuple
	it := bindingRel.Iterator()
	defer it.Close()

	for it.Next() {
		bindingTuple := it.Tuple()
		eVal := bindingTuple[eColIdx]

		// Get entity bytes
		var eBytes [20]byte
		if ident, ok := eVal.(datalog.Identity); ok {
			copy(eBytes[:], ident.Bytes())
		} else {
			continue // Skip non-identity values
		}

		// Resolve the vector using RGA reconstruction
		result, err := m.resolveVector(eBytes[:], aBytes[:])
		if err != nil {
			return nil, fmt.Errorf("vector resolution failed for entity: %w", err)
		}

		// Skip entities with empty vectors
		if len(result.Elements) == 0 {
			continue
		}

		// Build output tuple
		tuple := make(executor.Tuple, len(columns))
		for i, col := range columns {
			switch {
			case pattern.GetE() != nil && pattern.GetE().IsVariable() &&
				pattern.GetE().(query.Variable).Name == col:
				tuple[i] = eVal
			case pattern.GetA() != nil && pattern.GetA().IsVariable() &&
				pattern.GetA().(query.Variable).Name == col:
				tuple[i] = attr
			case pattern.GetV() != nil && pattern.GetV().IsVariable() &&
				pattern.GetV().(query.Variable).Name == col:
				// Return the entire vector as []any
				tuple[i] = result.Elements
			case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
				pattern.GetT().(query.Variable).Name == col:
				tuple[i] = result.MaxElementID
			default:
				// Check if this column comes from bindings (pass through)
				for j, bindCol := range bindingCols {
					if bindCol == col && j < len(bindingTuple) {
						tuple[i] = bindingTuple[j]
						break
					}
				}
			}
		}

		tuples = append(tuples, tuple)
	}

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
	currentEntity       datalog.Identity
	currentSetMembers   []interface{} // Resolved set members for current entity
	currentMaxElementID datalog.ElementID
	currentMemberIdx    int
	currentTuple        executor.Tuple
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

// cardinalityManyAVETValueIterator streams results for [?e :attr "value"] patterns
// using the AVET index for O(k) lookup. It scans entries with [A][V] prefix and applies
// add-wins resolution per entity to determine if the value is currently in each entity's set.
//
// AVET key order: [A][V][E][Op][Tx↓]
// - Same A and V (from prefix)
// - Entities grouped together
// - Within entity: Op=0 (Add) before Op=1 (Remove), then by Tx descending
type cardinalityManyAVETValueIterator struct {
	matcher     *BadgerMatcher
	pattern     *query.DataPattern
	columns     []query.Symbol
	a, v        interface{}
	aBytes      [32]byte
	storageIter Iterator

	// Current entity being processed
	currentEntity     datalog.Identity
	currentEntityHash [20]byte
	hasCurrentEntity  bool

	// Add-wins state for current entity
	highestAddLamport    uint64
	highestRemoveLamport uint64
	hasAdd               bool
	hasRemove            bool

	// Result
	currentTuple executor.Tuple
	done         bool
}

func (it *cardinalityManyAVETValueIterator) Next() bool {
	if it.done {
		return false
	}

	for {
		if !it.storageIter.Next() {
			// End of scan - emit final entity if it's a member
			if it.hasCurrentEntity && it.isCurrentEntityMember() {
				it.buildTuple()
				it.done = true
				return true
			}
			return false
		}

		datom, err := it.storageIter.Datom()
		if err != nil {
			continue
		}

		eBytes := datom.E.Hash()

		// Check if this is a new entity
		if !it.hasCurrentEntity {
			// First entity
			it.startNewEntity(datom.E, eBytes, datom.Op, datom.Tx.Lamport)
			continue
		}

		if eBytes != it.currentEntityHash {
			// Moving to new entity - check if previous entity is a member
			if it.isCurrentEntityMember() {
				it.buildTuple()
				// Start tracking new entity for next iteration
				it.startNewEntity(datom.E, eBytes, datom.Op, datom.Tx.Lamport)
				return true
			}
			// Previous entity not a member, start tracking new entity
			it.startNewEntity(datom.E, eBytes, datom.Op, datom.Tx.Lamport)
			continue
		}

		// Same entity - update add-wins state
		it.updateAddWinsState(datom.Op, datom.Tx.Lamport)
	}
}

func (it *cardinalityManyAVETValueIterator) startNewEntity(e datalog.Identity, eHash [20]byte, op datalog.CRDTOp, lamport uint64) {
	it.currentEntity = e
	it.currentEntityHash = eHash
	it.hasCurrentEntity = true
	it.highestAddLamport = 0
	it.highestRemoveLamport = 0
	it.hasAdd = false
	it.hasRemove = false
	it.updateAddWinsState(op, lamport)
}

func (it *cardinalityManyAVETValueIterator) updateAddWinsState(op datalog.CRDTOp, lamport uint64) {
	if op == datalog.OpCRDTAdd {
		if !it.hasAdd || lamport > it.highestAddLamport {
			it.highestAddLamport = lamport
			it.hasAdd = true
		}
	} else if op == datalog.OpCRDTRemove {
		if !it.hasRemove || lamport > it.highestRemoveLamport {
			it.highestRemoveLamport = lamport
			it.hasRemove = true
		}
	}
}

func (it *cardinalityManyAVETValueIterator) isCurrentEntityMember() bool {
	if !it.hasAdd {
		return false // No adds means not in set
	}
	if !it.hasRemove {
		return true // Only adds, no removes
	}
	// Both add and remove exist - compare Lamport timestamps
	// Add wins at same Lamport (add-wins semantics)
	return it.highestAddLamport >= it.highestRemoveLamport
}

func (it *cardinalityManyAVETValueIterator) buildTuple() {
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
			tuple[i] = it.v
		case it.pattern.GetT() != nil && it.pattern.GetT().IsVariable() &&
			it.pattern.GetT().(query.Variable).Name == col:
			tuple[i] = datalog.ElementID{}
		}
	}
	it.currentTuple = tuple
}

func (it *cardinalityManyAVETValueIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *cardinalityManyAVETValueIterator) Close() error {
	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}

// cardinalityManyFindEntitiesWithValueIterator streams results for [?e :attr "value"] patterns
// where E is unbound and cardinality is many. It iterates through entities and checks
// membership for each, yielding one tuple per entity where the value is in the set.
//
// DEPRECATED: Use cardinalityManyAVETValueIterator instead for O(k) performance.
// This iterator uses AEVT which is O(n) where n = all entities with attribute.
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
// Finds all entities where the specific value is in the set.
//
// Uses AVET index with [A][V] prefix for O(k) lookup where k = datoms with this value,
// instead of O(n) where n = all entities with the attribute.
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

	// Encode value with type prefix (same as key encoding)
	vType := byte(datalog.Type(v))
	vData := datalog.ValueBytes(v)
	vBytes := append([]byte{vType}, vData...)

	// Build AVET prefix: [index][A][type+value]
	// This directly seeks to entries with this specific value - O(k) not O(n)
	prefix := make([]byte, 1+32+len(vBytes))
	prefix[0] = byte(AVET)
	copy(prefix[1:33], aBytes[:])
	copy(prefix[33:], vBytes)

	storageIter, err := m.store.Scan(AVET, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, fmt.Errorf("AVET scan failed: %w", err)
	}

	// Create streaming iterator that applies add-wins resolution per entity
	iter := &cardinalityManyAVETValueIterator{
		matcher:     m,
		pattern:     pattern,
		columns:     columns,
		a:           a,
		v:           v,
		aBytes:      aBytes,
		storageIter: storageIter,
	}

	return executor.NewStreamingRelationWithOptions(columns, iter, m.options), nil
}
