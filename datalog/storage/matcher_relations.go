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

// Ensure BadgerMatcher implements PatternMatcher
var _ executor.PatternMatcher = (*BadgerMatcher)(nil)

// Ensure BadgerMatcher implements executor.PredicateAwareMatcher
var _ executor.PredicateAwareMatcher = (*BadgerMatcher)(nil)

// Match implements PatternMatcher.Match - returns a Relation directly
func (m *BadgerMatcher) Match(q *query.Query, bindings executor.Relations) (executor.Relation, error) {
	// Default implementation with no constraints
	return m.MatchWithConstraints(q, bindings, nil)
}

// MatchWithConstraints implements predicate-aware matching with storage-level filtering
func (m *BadgerMatcher) MatchWithConstraints(
	q *query.Query,
	bindings executor.Relations,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	pattern, err := q.SingleDataPattern()
	if err != nil {
		return nil, err
	}
	if err := query.ValidateEntityBinding(m.extractValue(pattern.GetE())); err != nil {
		return nil, err
	}
	if err := query.ValidateAttributeBinding(m.extractValue(pattern.GetA())); err != nil {
		return nil, err
	}
	// Determine pattern symbols
	symbols := pattern.Symbols()

	if bindings == nil || len(bindings) == 0 {
		// Simple case - no bindings
		return m.matchUnboundAsRelation(q, pattern, symbols, constraints)
	}

	// Find best binding relation for this pattern
	bindingRel := bindings.FindBestForPattern(pattern)
	if bindingRel == nil {
		// No relation binds any pattern variables
		return m.matchUnboundAsRelation(q, pattern, symbols, constraints)
	}

	// Size() declines with -1 on streaming relations rather than consuming
	// a tuple to answer, so the empty-binding shortcut applies exactly when
	// the count is already free; an empty stream is discovered naturally by
	// iteration.
	if bindingRel.Size() == 0 {
		// An errored relation that materialized empty is not an empty
		// binding: its zero rows mean the upstream scan failed. Falling
		// back to an unbound scan here would launder that failure into
		// a silent result.
		if err := executor.EmptyRelationError(bindingRel); err != nil {
			return nil, err
		}
		return m.matchUnboundAsRelation(q, pattern, symbols, constraints)
	}

	// Project the binding relation to only include symbols used in the pattern
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
						vBound := m.extractValue(pattern.GetV())
						return m.matchVectorWithBindings(pattern, bindingRel, symbols, kw, vBound, attr.ValueType)
					}
				}
			}
		}
	}

	// CACHE OPTIMIZATION: When A is known (from pattern constant or bindings),
	// use the cache for each E value instead of storage scans.
	if m.handler != nil {
		m.handler(annotations.Event{
			Name: "cache/check",
			Data: map[string]interface{}{
				"pattern":      pattern.String(),
				"a_resolved":   fmt.Sprintf("%v (%T)", aResolved, aResolved),
				"cache_nil":    m.cache == nil,
				"txID_nil":     m.txID == nil,
				"has_bindings": bindings != nil && len(bindings) > 0,
			},
		})
	}
	if m.cache != nil && !m.isHistoryMode() {
		if aResolved != nil {
			// Phase 1: A has a single known value (from constant or single-tuple binding)
			if aKw, ok := aResolved.(datalog.Keyword); ok {
				cacheResult, handled, err := m.matchWithBindingsFromCache(pattern, bindingRel, symbols, aKw, -1)
				if err != nil {
					return nil, err
				}
				if handled {
					if m.handler != nil {
						m.handler(annotations.Event{
							Name: "cache/match-handled",
							Data: map[string]interface{}{
								"pattern": pattern.String(),
								"results": cacheResult.Size(),
							},
						})
					}
					return cacheResult, nil
				}
			}
		} else if aVar, ok := pattern.GetA().(query.Variable); ok {
			// Phase 2: A varies per tuple in bindingRel (e.g., from join results)
			aIdx := query.SymbolIndex(bindingRel.Symbols(), aVar.Name)
			eVar, eIsVar := pattern.GetE().(query.Variable)
			if aIdx >= 0 && eIsVar {
				eIdx := query.SymbolIndex(bindingRel.Symbols(), eVar.Name)
				if eIdx >= 0 {
					cacheResult, handled, err := m.matchWithBindingsFromCache(
						pattern, bindingRel, symbols, nil, aIdx)
					if err != nil {
						return nil, err
					}
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
			return m.matchWithVValidation(pattern, bindingRel, symbols, strategy, constraints)
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
			return m.matchWithHashJoin(pattern, bindingRel, symbols, strategy.Position, strategy.Index, constraints)

		case MergeJoin:
			// Use merge join for high selectivity (>50%) with large binding sets
			return m.matchWithMergeJoin(pattern, bindingRel, symbols, strategy.Position, strategy.Index, constraints)

		case IndexNestedLoop:
			// Use iterator reuse for small sets or high selectivity
			return m.matchWithIteratorReuse(pattern, bindingRel, symbols, strategy, constraints)

		default:
			// Fall back to iterator reuse
			return m.matchWithIteratorReuse(pattern, bindingRel, symbols, strategy, constraints)
		}

	case NoReuse:
		fallthrough
	default:
		// Fall back to opening/closing iterator for each tuple
		return m.matchWithoutIteratorReuse(pattern, bindingRel, symbols, constraints)
	}
}

// matchUnboundAsRelation matches a pattern without bindings and returns a Relation
func (m *BadgerMatcher) matchUnboundAsRelation(
	q *query.Query,
	pattern *query.DataPattern,
	symbols []query.Symbol,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
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
	var valueType schema.ValueType

	// Determine cardinality and value type when A is bound (regardless of E)
	if a != nil {
		if m.schema != nil {
			if aKw, ok := a.(datalog.Keyword); ok {
				if def := m.schema.GetAttribute(aKw); def != nil {
					card = def.Cardinality
					valueType = def.ValueType
				}
			}
		}
	}

	// CACHE OPTIMIZATION: When E and A are bound, use the cache for O(1)
	// access instead of storage scans. Concrete AsOf matchers use
	// transaction-scoped cache keys; History matchers bypass cache.
	if m.cache != nil && !m.isHistoryMode() && e != nil && a != nil {
		if eIdent, ok := e.(datalog.Identity); ok {
			if aKw, ok := a.(datalog.Keyword); ok {
				cacheResult, handled := m.matchFromCache(pattern, symbols, eIdent, aKw, v, card, valueType)
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
			// V is bound
			switch card {
			case schema.CardinalityMany:
				useMembershipCheck = true
			case schema.CardinalityVector:
				// Vector literal: resolve RGA then compare against bound V
				useVectorResolution = true
			}
		}
	} else if e == nil && a != nil && v != nil && card == schema.CardinalityOne {
		// E is unbound, A is bound, V is constant, cardinality-one
		// V-bound scan (AVET) only sees datoms with this V, but the LWW winner
		// may have a different V. Use candidate + validate pattern.
		// See docs/reference/INDEX_SELECTION_PROOF.md Theorem 2 & 5.
		dummySym := datalog.NewSymbol("__v_bound_dummy__")
		bindingRel := executor.NewMaterializedRelation(
			[]query.Symbol{dummySym},
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
		return m.matchWithVValidation(pattern, bindingRel, symbols, strategy, nil)
	} else if e == nil && a != nil && card == schema.CardinalityVector {
		// E unbound, A bound, cardinality-vector
		// Scan all entities with this attribute, resolve each vector, compare against V.
		// Pass v (nil for unbound, []interface{} for bound literal).
		return m.matchVectorScanAllEntities(pattern, symbols, a, v, valueType)
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

	// In history mode, disable all CRDT resolution flags — return raw datoms
	if m.isHistoryMode() {
		returnOnlyFirst = false
		useAddWinsResolution = false
		useMembershipCheck = false
		useAddWinsScanAllEntities = false
		useAddWinsScanAllEntitiesWithValue = false
		useVectorResolution = false
	}

	// For cardinality-vector with E+A bound: use vector resolution
	if useVectorResolution {
		return m.matchCardinalityVectorAsRelation(pattern, symbols, e, a, v, valueType)
	}

	// For cardinality-many with E+A bound, V unbound: use add-wins resolution
	if useAddWinsResolution {
		return m.matchCardinalityManyAsRelation(pattern, symbols, e, a)
	}

	// For cardinality-many with E+A+V bound: use membership check
	if useMembershipCheck {
		return m.matchCardinalityManyMembership(pattern, symbols, e, a, v)
	}

	// For cardinality-many with A bound, E unbound, V unbound: scan all entities
	if useAddWinsScanAllEntities {
		return m.matchCardinalityManyScanAllEntities(pattern, symbols, a)
	}

	// For cardinality-many with A bound, E unbound, V bound: find entities with value
	if useAddWinsScanAllEntitiesWithValue {
		return m.matchCardinalityManyFindEntitiesWithValue(pattern, symbols, a, v)
	}

	// Choose index and create scan range
	index, start, end := m.chooseIndex(e, a, v, tx)
	properties := unboundScanProperties(pattern, index, card, m.isHistoryMode())
	if orderedProperties, ok := historyTAEVProperties(q, pattern, m.isHistoryMode()); ok {
		index = TAEV
		start, end = m.encoder.EncodePrefixRange(TAEV)
		properties = orderedProperties
	} else if orderedProperties, ok := historyAETVProperties(q, pattern, m.isHistoryMode()); ok {
		if keyword, keywordOK := a.(datalog.Keyword); keywordOK {
			attr := ToStorageDatom(datalog.Datom{A: keyword}).A
			index = AETV
			start, end = m.encoder.EncodePrefixRange(AETV, attr[:])
			properties = orderedProperties
		}
	} else if orderedProperties, ok := historyEATVProperties(q, pattern, m.isHistoryMode()); ok {
		if identity, identityOK := e.(datalog.Identity); identityOK {
			entity := ToStorageDatom(datalog.Datom{E: identity}).E
			index = EATV
			start, end = m.encoder.EncodePrefixRange(EATV, entity[:])
			properties = orderedProperties
		}
	} else if orderedProperties, ok := historyATEVProperties(q, pattern, m.isHistoryMode()); ok {
		if keyword, keywordOK := a.(datalog.Keyword); keywordOK {
			attr := ToStorageDatom(datalog.Datom{A: keyword}).A
			index = ATEV
			start, end = m.encoder.EncodePrefixRange(ATEV, attr[:])
			properties = orderedProperties
		}
	}

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

	// Streaming iterator: key-only scan wrapped with CRDT resolution in
	// current/as-of mode, or raw scan in history mode.
	regularIter := &unboundIterator{
		matcher:         m,
		index:           index,
		start:           start,
		end:             end,
		pattern:         pattern,
		symbols:         symbols,
		e:               e,
		a:               a,
		v:               v,
		tx:              tx,
		constraints:     constraints,
		workspace:       make(executor.Tuple, len(symbols)),
		tupleBuilder:    m.getTupleBuilder(pattern, symbols),
		returnOnlyFirst: returnOnlyFirst, // CRDT cardinality-one support
	}

	rawStorageIter, err := m.reader.ScanKeysOnly(index, start, end)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	if m.isHistoryMode() {
		regularIter.storageIter = rawStorageIter
	} else {
		regularIter.storageIter = NewCRDTResolvingIterator(rawStorageIter, m.schema, m.crdtTxID(), m)
	}

	// Return streaming relation with lazy materialization
	// The iterator will be consumed and cached on first call to Iterator(),
	// eliminating the 6.3 GB of upfront allocations while maintaining correctness
	rel := executor.NewStreamingRelationWithProperties(
		symbols,
		regularIter,
		m.options,
		properties,
	)
	return rel, nil
}

// matchWithoutIteratorReuse uses separate scan for each binding tuple
func (m *BadgerMatcher) matchWithoutIteratorReuse(pattern *query.DataPattern, bindingRel executor.Relation, symbols []query.Symbol, constraints []executor.StorageConstraint) (executor.Relation, error) {
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

	// We need to materialize the binding relation to iterate multiple times.
	// ForEach drives the iterator per the storage.Iterator contract: a deferred
	// binding-relation failure (Tier-3 blob decode, CRDT unique-walk) surfaces
	// only through Error()/Close() after Next() returns false, and must abort
	// the match rather than be laundered into a clean partial result.
	// CRITICAL: Must copy tuples because the iterator reuses its buffer.
	var bindingTuples []executor.Tuple
	if err := executor.ForEach(bindingRel, func(tuple executor.Tuple) error {
		tupleCopy := make(executor.Tuple, len(tuple))
		copy(tupleCopy, tuple)
		bindingTuples = append(bindingTuples, tupleCopy)
		return nil
	}); err != nil {
		return nil, err
	}
	bindingTuples = filterTypedPositionBindings(pattern, bindingRel.Symbols(), bindingTuples)

	// Create iterator that will scan for each binding tuple
	iter := &nonReusingIterator{
		matcher:          m,
		pattern:          pattern,
		bindingRel:       bindingRel,
		bindingTuples:    bindingTuples,
		symbols:          symbols,
		constraints:      constraints,
		currentIdx:       -1,
		workspace:        make(executor.Tuple, len(symbols)),
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Symbols()),
		tupleBuilder:     m.getTupleBuilder(pattern, symbols),
	}

	// Return streaming relation with the iterator
	return executor.NewStreamingRelationWithOptions(symbols, iter, m.options), nil
}

// matchWithIteratorReuse implements the optimized iterator reuse strategy
func (m *BadgerMatcher) matchWithIteratorReuse(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
	strategy ReuseStrategy,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	// Get sorted tuples - THIS IS CRITICAL!
	// Without sorted tuples, we cannot use Seek() to jump forward in the iterator
	// Sorted() will auto-materialize if needed
	sortedTuples, err := bindingRel.Sorted()
	if err != nil {
		return nil, err
	}
	sortedTuples = filterTypedPositionBindings(pattern, bindingRel.Symbols(), sortedTuples)

	// Create streaming iterator that will reuse storage iterator
	iter := &reusingIterator{
		matcher:          m,
		pattern:          pattern,
		bindingRel:       bindingRel,
		tuples:           sortedTuples,
		position:         strategy.Position,
		index:            strategy.Index,
		symbols:          symbols,
		constraints:      constraints,
		currentIdx:       -1,
		workspace:        make(executor.Tuple, len(symbols)),
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Symbols()),
		tupleBuilder:     m.getTupleBuilder(pattern, symbols),
	}

	// Return streaming relation with the iterator
	return executor.NewStreamingRelationWithOptions(symbols, iter, m.options), nil
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
	symbols []query.Symbol,
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
				"binding_rel": bindingRel.Symbols(),
			},
		})
	}
	// Get sorted tuples for efficient iteration
	sortedTuples, err := bindingRel.Sorted()
	if err != nil {
		return nil, err
	}

	// Create validating iterator
	iter := &validatingVBoundIterator{
		matcher:          m,
		pattern:          pattern,
		bindingRel:       bindingRel,
		tuples:           sortedTuples,
		candidateIndex:   strategy.Index,           // AVET or VAET for candidates
		validationIndex:  strategy.ValidationIndex, // EATV for validation
		boundA:           strategy.BoundA,          // Constant A value if any
		symbols:          symbols,
		constraints:      constraints,
		currentTupleIdx:  -1,
		workspace:        make(executor.Tuple, len(symbols)),
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Symbols()),
		tupleBuilder:     m.getTupleBuilder(pattern, symbols),
	}

	return executor.NewStreamingRelationWithProperties(
		symbols,
		iter,
		m.options,
		validatedVBoundProperties(pattern, strategy),
	), nil
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
	symbols     []query.Symbol
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

	err error // First error from storage operations
}

func (it *validatingVBoundIterator) Next() bool {
	for {
		// Try to get next CRDT-resolved result from current scan
		if it.crdtIter != nil {
			for it.crdtIter.Next() {
				datom, err := it.crdtIter.Datom()
				if err != nil {
					it.err = err
					return false
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
				// Schemaless attributes are treated as cardinality-one (LWW)
				// everywhere else in the engine (planner routing, cache OneValue),
				// so they must validate the candidate against the current winner
				// too. Without this, the V-prefix candidate scan leaks over-matches:
				// an empty bound value matches every value of the type, and a
				// non-empty value prefix-matches. Validation enforces exact equality.
				if card == schema.CardinalityOne || card == schema.CardinalityUnknown {
					ok, err := it.validateCandidate(datom.E, datom.A)
					if err != nil {
						it.err = err
						return false
					}
					if !ok {
						// Stale candidate - LWW winner has different V
						continue
					}
				}

				// Build result tuple
				it.currentTuple = it.tupleBuilder.BuildTupleInterned(datom)
				return true
			}
			// Inner iterator exhausted — propagate any deferred error
			// so Error() surfaces failures (e.g., unique-walk sub-scan
			// errors that caused the scan to abort rather than return
			// a datom).
			if srcErr := it.crdtIter.Error(); srcErr != nil && it.err == nil {
				it.err = srcErr
			}
			if closeErr := it.crdtIter.Close(); closeErr != nil && it.err == nil {
				it.err = closeErr
			}
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

		// Unique-attribute short-circuit: resolve the (A, V)-LWW winner
		// once via a dedicated primitive and emit that one entity (or
		// nothing) instead of streaming all validated candidates. This
		// makes V-bound queries on unique attributes return exactly the
		// canonical owner, satisfying the symmetry between the value view
		// and the entity view under the CRDT-unique semantics.
		if emitted, err := it.tryEmitUniqueWinner(); err != nil {
			it.err = err
			return false
		} else if emitted {
			return true
		} else if it.isBoundAUniqueAttr() {
			// Attribute is unique but no valid claimant — skip this binding.
			continue
		}

		// Open CRDT-resolving scan on V-primary index
		var err error
		it.crdtIter, it.rawIter, err = it.openCRDTScan()
		if err != nil {
			it.err = err
			return false
		}
	}
}

// isBoundAUniqueAttr reports whether the bound attribute (if any) is
// declared unique in the schema. Returns false if A is a variable (not
// constant), if no schema is set, or if the attribute is not unique.
func (it *validatingVBoundIterator) isBoundAUniqueAttr() bool {
	if it.boundA == nil {
		return false
	}
	aKw, ok := it.boundA.(datalog.Keyword)
	if !ok {
		return false
	}
	if it.matcher.schema == nil {
		return false
	}
	def := it.matcher.schema.GetAttribute(aKw)
	if def == nil {
		return false
	}
	return def.Unique != ""
}

// tryEmitUniqueWinner resolves the (A, V)-LWW winner for the current
// binding and, if a valid claimant exists, sets it.currentTuple and
// returns (true, nil). If the attribute is not unique, returns
// (false, nil) and the caller falls through to the normal scan path.
// If the attribute is unique but no entity currently claims the bound
// value, returns (false, nil) with a short-circuit indicator (see caller).
func (it *validatingVBoundIterator) tryEmitUniqueWinner() (bool, error) {
	if !it.isBoundAUniqueAttr() {
		return false, nil
	}

	aKw := it.boundA.(datalog.Keyword)
	var aStorage Attribute
	copy(aStorage[:], aKw.String())
	vBytes := encodeValueForSearch(it.currentBoundV, it.matcher.encoder)

	owner, ownerTx, err := it.matcher.resolveAVLWW(aStorage, vBytes, it.currentBoundV)
	if err != nil {
		return false, err
	}
	if owner == nil {
		return false, nil // No claimant; caller treats this as skip-binding.
	}

	// Build a synthetic winner datom for tuple construction.
	winner := &datalog.Datom{
		E:  owner,
		A:  aKw,
		V:  it.currentBoundV,
		Tx: ownerTx,
	}
	it.currentTuple = it.tupleBuilder.BuildTupleInterned(winner)
	return true, nil
}

// validateCandidate checks if the current value of (E, A) matches boundV.
// A storage or decode failure surfaces as an error — it must never collapse
// to "candidate doesn't match", which would silently drop valid results.
func (it *validatingVBoundIterator) validateCandidate(e datalog.Identity, a datalog.Keyword) (bool, error) {
	encoder := it.matcher.encoder

	// Convert E to storage bytes
	eBytes := ToStorageDatom(datalog.Datom{E: e}).E

	// Convert A to storage bytes; the keyword is already interned
	aStorage := ToStorageDatom(datalog.Datom{A: a}).A

	// Latest-mode CardinalityOne fast path. The EA cache resolves the current
	// (E, A) value with the same EATV-first-entry + tombstone semantics this
	// scan performs, but memoized — turning a per-candidate Badger seek into an
	// O(1) cache lookup. For a set-once attribute (e.g. a type tag), every
	// candidate after the first is a permanent cache hit.
	//
	// Equivalence preconditions (all required):
	//   - cache != nil: cache exists (not DisableCache).
	//   - txID == nil: latest mode. The scan below reads the absolute-latest
	//     entry with NO shouldFilterTx, so it equals ResolveLWW only when no
	//     as-of/history filter applies. Concrete-AsOf and history keep the scan
	//     path untouched — this change is a strict no-op in those modes.
	//   - non-unique: ResolveLWW walks for unique attributes (CRDT-unique
	//     fallback), which differs from this scan's naive first-entry. Unique
	//     attributes already short-circuit before validation via
	//     tryEmitUniqueWinner; this guard makes the precondition self-evident.
	if it.matcher.cache != nil && it.matcher.txID == nil && !it.attrIsUnique(a) {
		var eEnt Entity
		var aAttr Attribute
		copy(eEnt[:], eBytes[:])
		copy(aAttr[:], aStorage[:])
		entry := it.matcher.cache.GetOrResolve(CacheKey{E: eEnt, A: aAttr}, it.matcher, it.matcher.cacheBound())
		if entry != nil && entry.Cardinality() == schema.CardinalityOne {
			// oneValue is nil for a tombstoned or never-set (E, A); ValuesEqual
			// against the (always non-nil) bound V yields false, matching the
			// scan path's tombstone and no-winner handling exactly.
			matches := datalog.ValuesEqual(entry.OneValue(), it.currentBoundV)
			if it.matcher.handler != nil {
				it.matcher.handler(annotations.Event{
					Name:  "v-validation/cache-resolved",
					Start: time.Now(),
					Data: map[string]any{
						"e":        e.String(),
						"a":        a.String(),
						"bound_v":  fmt.Sprintf("%v", it.currentBoundV),
						"cached_v": fmt.Sprintf("%v", entry.OneValue()),
						"matches":  matches,
					},
				})
			}
			return matches, nil
		}
	}

	// Point lookup on EATV: scan (E, A) prefix, first result is CRDT winner
	start, end := encoder.EncodePrefixRange(it.validationIndex, eBytes[:], aStorage[:])
	rawIter, err := it.matcher.reader.ScanKeysOnly(it.validationIndex, start, end)
	if err != nil {
		return false, err
	}
	defer rawIter.Close()

	// Resolve the (E, A) winner visible in this matcher's mode: the highest-Tx
	// entry not filtered out by the as-of target (EATV sorts Tx descending). In
	// latest and history mode shouldFilterTx is always false, so this is the
	// first entry — identical to the prior behavior. In concrete as-of mode it
	// skips entries newer than the snapshot, mirroring ResolveLWW, so a V-bound
	// query as-of T validates against the value as of T rather than
	// absolute-latest. Without this skip, a value overwritten or tombstoned
	// after T leaked into validation and wrongly rejected (or admitted)
	// candidates under as-of.
	for rawIter.Next() {
		winner, err := rawIter.Datom()
		if err != nil {
			return false, err
		}
		if it.matcher.shouldFilterTx(winner.Tx) {
			continue
		}

		// Check Op: if the latest visible operation is a tombstone, the
		// attribute doesn't exist. No V can match a tombstoned attribute.
		if winner.Op == datalog.OpCRDTRemove {
			return false, nil
		}

		// Check if winner's V matches our bound V. Use ValuesEqual (not raw ==)
		// so byte-slice values compare by content instead of panicking on the
		// uncomparable []byte dynamic type.
		matches := datalog.ValuesEqual(winner.V, it.currentBoundV)

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

		return matches, nil
	}
	// A failed scan is not "no winner" — surface it.
	if err := rawIter.Error(); err != nil {
		return false, err
	}

	// No entry visible under this snapshot — the attribute has no value as-of T.
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
	return false, nil
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

// attrIsUnique reports whether attribute a is declared unique in the schema.
// Keeps the validateCandidate cache fast-path off unique attributes, whose
// ResolveLWW uses walk-based resolution that differs from the naive
// first-entry scan the fast path is proven equivalent to.
func (it *validatingVBoundIterator) attrIsUnique(a datalog.Keyword) bool {
	if it.matcher.schema == nil {
		return false
	}
	def := it.matcher.schema.GetAttribute(a)
	return def != nil && def.Unique != ""
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
	encoder := it.matcher.encoder
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

		// Convert A to storage bytes; the keyword is already interned
		aStorage := ToStorageDatom(datalog.Datom{A: aKw}).A

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
	rawIter, err := it.matcher.reader.ScanKeysOnly(it.candidateIndex, start, end)
	if err != nil {
		return nil, nil, err
	}

	// Wrap with CRDTResolvingIterator for correct CRDT semantics
	// CRDTResolvingIterator handles:
	// - Group deduplication via contiguous comparison (O(1) space)
	// - Add-wins with same-Tx tiebreaking for CardinalityMany
	// - RGA for CardinalityVector
	// - Add-wins for CardinalityUnknown (same as CardinalityMany)
	//
	// Pass the matcher's as-of target (crdtTxID) so candidate resolution
	// respects the snapshot. In latest and history mode this is the zero
	// ElementID (no filter); in concrete as-of mode it skips post-snapshot
	// entries, so a value tombstoned or overwritten after T does not drop or
	// alter a candidate that existed as of T. Every other CRDTResolvingIterator
	// call site already threads crdtTxID(); this one previously hardcoded zero.
	crdtIter := NewCRDTResolvingIterator(rawIter, it.matcher.schema, it.matcher.crdtTxID(), it.matcher)

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
	return encodeValueForSearch(v, it.matcher.encoder)
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

func (it *validatingVBoundIterator) Error() error { return it.err }

// matchWithSimpleBatchScanning uses simplified batch scanning to process large binding sets efficiently
func (m *BadgerMatcher) matchWithSimpleBatchScanning(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
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
		symbols,
		constraints,
	)

	// Perform the batch scan
	if err := scanner.Scan(); err != nil {
		return nil, fmt.Errorf("batch scan failed: %w", err)
	}

	// Return streaming relation wrapping the scanner
	// Note: scanner materializes internally but we avoid secondary materialization
	return executor.NewStreamingRelationWithOptions(symbols, scanner, m.options), nil
}

// matchWithBatchScanning uses batch scanning to process large binding sets efficiently
func (m *BadgerMatcher) matchWithBatchScanning(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
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
		symbols,
		constraints,
	)

	// Perform the scan
	if err := scanner.Scan(); err != nil {
		return nil, err
	}

	// Return streaming relation wrapping the scanner
	// Note: scanner materializes internally but we avoid secondary materialization
	return executor.NewStreamingRelationWithOptions(symbols, scanner, m.options), nil
}

// matchFromCache attempts to resolve a pattern using the cache.
// Returns (relation, true) if cache was used, (nil, false) if fallback to storage is needed.
// This provides O(1) access for patterns with E and A bound when querying latest state.
func (m *BadgerMatcher) matchFromCache(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	e datalog.Identity,
	a datalog.Keyword,
	v interface{}, // nil if V is unbound
	card schema.Cardinality,
	valueType schema.ValueType,
) (executor.Relation, bool) {
	// Build cache key
	eBytes := Entity(e.Hash())
	aStorage := ToStorageDatom(datalog.Datom{A: a}).A
	var aAttr Attribute
	copy(aAttr[:], aStorage[:])
	key, ok := m.cacheKey(eBytes, aAttr)
	if !ok {
		return nil, false
	}

	// Get or resolve from cache
	entry := m.cache.GetOrResolve(key, m, m.cacheBound())
	if entry == nil {
		return nil, false // Fallback to storage
	}

	// Get tuple builder for building tuples
	tupleBuilder := m.getTupleBuilder(pattern, symbols)

	// Pre-allocate datom buffer with constant E and A
	var datomBuf datalog.Datom
	datomBuf.E = e
	datomBuf.A = a

	// buildTupleFromDatom builds a tuple from datom (reuses buffer)
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
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true
		}
		if v != nil {
			// V is bound - check if it matches
			if !datalog.ValuesEqual(val, v) {
				return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true
			}
		}
		// Build tuple with the cached value
		tuple := buildTuple(val, entry.Version())
		return executor.NewMaterializedRelationWithOptions(symbols, []executor.Tuple{tuple}, m.options), true

	case schema.CardinalityMany:
		set := entry.ManySet()
		if len(set) == 0 {
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true
		}
		if v != nil {
			// V is bound - membership check
			// []byte keys are stored as string(bytes) for hashability
			lookupKey := v
			if b, ok := lookupKey.([]byte); ok {
				lookupKey = string(b)
			}
			if _, exists := set[lookupKey]; exists {
				tuple := buildTuple(v, entry.Version())
				return executor.NewMaterializedRelationWithOptions(symbols, []executor.Tuple{tuple}, m.options), true
			}
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true
		}
		// V is unbound - return all set members
		tuples := make([]executor.Tuple, 0, len(set))
		for _, val := range set {
			tuple := buildTuple(val, entry.Version())
			tuples = append(tuples, tuple)
		}
		return executor.NewMaterializedRelationWithOptions(symbols, tuples, m.options), true

	case schema.CardinalityVector:
		list := entry.VectorList()
		resolved := typedVector(list, valueType)
		if v != nil {
			// V is bound - compare resolved vector against bound value
			if !datalog.ValuesEqual(resolved, v) {
				return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true
			}
			// Matched — fall through to build tuple
		}
		if len(list) == 0 && v == nil {
			// Empty vector with unbound V — no tuples
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true
		}
		tuple := buildTuple(resolved, entry.Version())
		return executor.NewMaterializedRelationWithOptions(symbols, []executor.Tuple{tuple}, m.options), true
	}

	return nil, false // Unknown cardinality, fallback to storage
}

// resolveKeywordFromBindings searches all binding relations for a single-tuple relation
// containing the given variable and returns its value as a Keyword. This covers scalar
// inputs and single-tuple tuple inputs where A has exactly one known value.
func resolveKeywordFromBindings(aVar query.Variable, bindings executor.Relations) (datalog.Keyword, bool) {
	for _, rel := range bindings {
		idx := query.SymbolIndex(rel.Symbols(), aVar.Name)
		if idx < 0 {
			continue
		}
		if rel.Size() != 1 {
			return nil, false // multi-tuple — can't extract single value
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
// When aSymIdx >= 0, A is extracted per-tuple from bindingRel[aSymIdx] instead of using
// the fixed `a` parameter. This handles the case where both E and A are symbols in the
// binding relation (e.g., from join results with varying attributes per tuple).
func (m *BadgerMatcher) matchWithBindingsFromCache(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
	a datalog.Keyword,
	aSymIdx int,
) (executor.Relation, bool, error) {
	// Find which symbol in the binding relation has E
	eVar, isVar := pattern.GetE().(query.Variable)
	if !isVar {
		return nil, false, nil // E is not a variable, can't get it from bindings
	}

	eSymIdx := query.SymbolIndex(bindingRel.Symbols(), eVar.Name)
	if eSymIdx < 0 {
		return nil, false, nil // E variable not in bindings
	}

	// Pre-compute fixed-A values when A is constant across all tuples
	var fixedCard schema.Cardinality
	var fixedValueType schema.ValueType
	var fixedAAttr Attribute
	if aSymIdx < 0 {
		fixedCard = schema.CardinalityOne
		if m.schema != nil {
			if def := m.schema.GetAttribute(a); def != nil {
				fixedCard = def.Cardinality
				fixedValueType = def.ValueType
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
	tupleBuilder := m.getTupleBuilder(pattern, symbols)

	// Pre-allocate datom buffer
	var datomBuf datalog.Datom
	if aSymIdx < 0 {
		datomBuf.A = a // constant A across all tuples
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
		if eSymIdx >= len(tuple) {
			continue
		}

		// Get E value from binding
		eVal := tuple[eSymIdx]
		eIdent, ok := eVal.(datalog.Identity)
		if !ok {
			// Try to convert if it's a different type
			if id, ok := eVal.(*datalog.Identity); ok {
				eIdent = *id
			} else {
				continue // Can't use cache for non-Identity E
			}
		}

		// Determine A and cardinality for this tuple
		var rowCard schema.Cardinality
		var rowValueType schema.ValueType
		var rowAAttr Attribute
		if aSymIdx >= 0 {
			// Per-tuple A: extract from binding tuple
			if aSymIdx >= len(tuple) {
				continue
			}
			aKw, ok := tuple[aSymIdx].(datalog.Keyword)
			if !ok {
				continue
			}
			rowCard = schema.CardinalityOne
			if m.schema != nil {
				if def := m.schema.GetAttribute(aKw); def != nil {
					rowCard = def.Cardinality
					rowValueType = def.ValueType
				}
			}
			aStorage := ToStorageDatom(datalog.Datom{A: aKw}).A
			copy(rowAAttr[:], aStorage[:])
			datomBuf.A = aKw
		} else {
			rowCard = fixedCard
			rowValueType = fixedValueType
			rowAAttr = fixedAAttr
		}

		// Build cache key
		eBytes := Entity(eIdent.Hash())
		key, ok := m.cacheKey(eBytes, rowAAttr)
		if !ok {
			return nil, false, nil
		}

		// Get from cache
		entry := m.cache.GetOrResolve(key, m, m.cacheBound())
		if entry == nil {
			// Cache miss - fallback to storage for entire query
			return nil, false, nil
		}

		// Process based on cardinality
		switch rowCard {
		case schema.CardinalityOne:
			val := entry.OneValue()
			if val == nil {
				continue // No value for this E
			}
			if v != nil && !datalog.ValuesEqual(val, v) {
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
				lookupKey := v
				if b, ok := lookupKey.([]byte); ok {
					lookupKey = string(b)
				}
				if _, exists := set[lookupKey]; exists {
					resultTuples = append(resultTuples, buildTuple(eIdent, v, entry.Version()))
				}
			} else {
				// Return all set members
				for _, val := range set {
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
				return nil, false, nil
			}
			resultTuples = append(resultTuples, buildTuple(eIdent, typedVector(list, rowValueType), entry.Version()))
		}
	}
	// A failed bindings scan is not an exhausted one — surface it rather
	// than answering from a truncated binding set.
	if err := iter.Error(); err != nil {
		return nil, false, err
	}

	return executor.NewMaterializedRelationWithOptions(symbols, resultTuples, m.options), true, nil
}

// matchCardinalityManyAsRelation handles cardinality-many patterns using add-wins resolution
func (m *BadgerMatcher) matchCardinalityManyAsRelation(
	pattern *query.DataPattern,
	symbols []query.Symbol,
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
	// We need to map the values to the correct symbol positions
	tuples := make([]executor.Tuple, 0, len(result.Members))

	for _, member := range result.Members {
		tuple := make(executor.Tuple, len(symbols))
		for i, sym := range symbols {
			switch {
			case pattern.GetE() != nil && pattern.GetE().IsVariable() &&
				pattern.GetE().(query.Variable).Name == sym:
				tuple[i] = e
			case pattern.GetA() != nil && pattern.GetA().IsVariable() &&
				pattern.GetA().(query.Variable).Name == sym:
				tuple[i] = a
			case pattern.GetV() != nil && pattern.GetV().IsVariable() &&
				pattern.GetV().(query.Variable).Name == sym:
				tuple[i] = member
			case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
				pattern.GetT().(query.Variable).Name == sym:
				// For cardinality-many, we don't have a single Tx
				// Use the max ElementID as the "current" transaction
				tuple[i] = result.MaxElementID
			}
		}
		tuples = append(tuples, tuple)
	}

	// Return materialized relation with the resolved set members
	return executor.NewMaterializedRelation(symbols, tuples), nil
}

// matchCardinalityVectorAsRelation handles cardinality-vector patterns using RGA resolution.
// Returns the entire reconstructed vector as a single value bound to the V variable.
func (m *BadgerMatcher) matchCardinalityVectorAsRelation(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	e, a, v interface{},
	valueType schema.ValueType,
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

	// No datoms at all → attribute was never set
	if result.Stats.TotalElements == 0 {
		return executor.NewMaterializedRelation(symbols, nil), nil
	}

	// Convert resolved elements to a typed vector
	resolved := typedVector(result.Elements, valueType)

	// If V is bound, compare the resolved vector against the bound value
	if v != nil {
		if !datalog.ValuesEqual(resolved, v) {
			return executor.NewMaterializedRelation(symbols, nil), nil
		}
		// Matched — fall through to build the tuple
	}

	// If empty vector (and V was nil/matched), return empty relation
	// Empty vectors produce no tuples for unbound V queries.
	if len(result.Elements) == 0 && v == nil {
		return executor.NewMaterializedRelation(symbols, nil), nil
	}

	// Build a single tuple with the entire vector as the V value
	tuple := make(executor.Tuple, len(symbols))
	for i, sym := range symbols {
		switch {
		case pattern.GetE() != nil && pattern.GetE().IsVariable() &&
			pattern.GetE().(query.Variable).Name == sym:
			tuple[i] = e
		case pattern.GetA() != nil && pattern.GetA().IsVariable() &&
			pattern.GetA().(query.Variable).Name == sym:
			tuple[i] = a
		case pattern.GetV() != nil && pattern.GetV().IsVariable() &&
			pattern.GetV().(query.Variable).Name == sym:
			tuple[i] = resolved
		case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
			pattern.GetT().(query.Variable).Name == sym:
			// Use the max ElementID as the "current" transaction
			tuple[i] = result.MaxElementID
		}
	}

	// Return single-tuple relation with the vector
	return executor.NewMaterializedRelation(symbols, []executor.Tuple{tuple}), nil
}

// matchVectorWithBindings handles vector patterns when E is bound via join bindings.
// For each entity in the bindings, it resolves the vector using RGA reconstruction
// and returns tuples with the reconstructed vector as the V value.
func (m *BadgerMatcher) matchVectorWithBindings(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
	attr datalog.Keyword,
	v interface{},
	valueType schema.ValueType,
) (executor.Relation, error) {
	// Find which symbol in bindings provides the entity
	var eSymIdx int = -1
	bindingSyms := bindingRel.Symbols()

	// Find the entity variable in the pattern and match it to binding symbols
	if pattern.GetE() != nil && pattern.GetE().IsVariable() {
		eSymIdx = query.SymbolIndex(bindingSyms, pattern.GetE().(query.Variable).Name)
	}

	if eSymIdx == -1 {
		// E is not bound from bindings - fall back to normal path
		// This shouldn't happen if we got here, but be safe
		return executor.NewMaterializedRelation(symbols, nil), nil
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
		eVal := bindingTuple[eSymIdx]

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

		resolved := typedVector(result.Elements, valueType)

		neverSet := result.Stats.TotalElements == 0

		// Entity has no datoms at all for this attribute — skip always
		if neverSet {
			continue
		}

		// If V is bound, compare the resolved vector against bound value
		if v != nil {
			if !datalog.ValuesEqual(resolved, v) {
				continue
			}
		} else if len(result.Elements) == 0 {
			// Unbound V: skip entities with empty vectors
			continue
		}

		// Build output tuple
		tuple := make(executor.Tuple, len(symbols))
		for i, sym := range symbols {
			switch {
			case pattern.GetE() != nil && pattern.GetE().IsVariable() &&
				pattern.GetE().(query.Variable).Name == sym:
				tuple[i] = eVal
			case pattern.GetA() != nil && pattern.GetA().IsVariable() &&
				pattern.GetA().(query.Variable).Name == sym:
				tuple[i] = attr
			case pattern.GetV() != nil && pattern.GetV().IsVariable() &&
				pattern.GetV().(query.Variable).Name == sym:
				tuple[i] = resolved
			case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
				pattern.GetT().(query.Variable).Name == sym:
				tuple[i] = result.MaxElementID
			default:
				// Check if this symbol comes from bindings (pass through)
				for j, bindSym := range bindingSyms {
					if bindSym == sym && j < len(bindingTuple) {
						tuple[i] = bindingTuple[j]
						break
					}
				}
			}
		}

		tuples = append(tuples, tuple)
	}
	// A failed bindings scan is not an exhausted one — surface it.
	if err := it.Error(); err != nil {
		return nil, err
	}

	return executor.NewMaterializedRelation(symbols, tuples), nil
}

// matchCardinalityManyMembership checks if a specific value is in a cardinality-many set
func (m *BadgerMatcher) matchCardinalityManyMembership(
	pattern *query.DataPattern,
	symbols []query.Symbol,
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
		return executor.NewMaterializedRelation(symbols, nil), nil
	}

	// Value is in set - build result tuple
	tuple := make(executor.Tuple, len(symbols))
	for i, sym := range symbols {
		switch {
		case pattern.GetE() != nil && pattern.GetE().IsVariable() &&
			pattern.GetE().(query.Variable).Name == sym:
			tuple[i] = e
		case pattern.GetA() != nil && pattern.GetA().IsVariable() &&
			pattern.GetA().(query.Variable).Name == sym:
			tuple[i] = a
		case pattern.GetV() != nil && pattern.GetV().IsVariable() &&
			pattern.GetV().(query.Variable).Name == sym:
			tuple[i] = v
		case pattern.GetT() != nil && pattern.GetT().IsVariable() &&
			pattern.GetT().(query.Variable).Name == sym:
			// For membership queries, we don't have a specific Tx
			tuple[i] = datalog.ElementID{}
		}
	}

	return executor.NewMaterializedRelation(symbols, []executor.Tuple{tuple}), nil
}

// cardinalityManyScanAllEntitiesIterator streams results for [?e :attr ?v] patterns
// where E is unbound and cardinality is many. It iterates through entities at the
// entity level - for each entity, it resolves the add-wins set and yields members one by one.
type cardinalityManyScanAllEntitiesIterator struct {
	matcher      *BadgerMatcher
	pattern      *query.DataPattern
	symbols      []query.Symbol
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
	err                 error // First error from storage operations
}

func (it *cardinalityManyScanAllEntitiesIterator) Next() bool {
	for {
		// If we have members remaining for current entity, yield next
		if it.currentMemberIdx < len(it.currentSetMembers) {
			member := it.currentSetMembers[it.currentMemberIdx]
			it.currentMemberIdx++

			// Build tuple
			tuple := make(executor.Tuple, len(it.symbols))
			for i, sym := range it.symbols {
				switch {
				case it.pattern.GetE() != nil && it.pattern.GetE().IsVariable() &&
					it.pattern.GetE().(query.Variable).Name == sym:
					tuple[i] = it.currentEntity
				case it.pattern.GetA() != nil && it.pattern.GetA().IsVariable() &&
					it.pattern.GetA().(query.Variable).Name == sym:
					tuple[i] = it.a
				case it.pattern.GetV() != nil && it.pattern.GetV().IsVariable() &&
					it.pattern.GetV().(query.Variable).Name == sym:
					tuple[i] = member
				case it.pattern.GetT() != nil && it.pattern.GetT().IsVariable() &&
					it.pattern.GetT().(query.Variable).Name == sym:
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
				it.err = err
				return false
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
				it.err = err
				return false
			}

			// If set has members, set up iteration
			if len(result.Members) > 0 {
				it.currentEntity = datom.E
				it.currentSetMembers = make([]interface{}, 0, len(result.Members))
				for _, member := range result.Members {
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

		// No more entities — propagate any deferred error from the
		// inner storage iterator so Error() surfaces deep failures.
		if srcErr := it.storageIter.Error(); srcErr != nil && it.err == nil {
			it.err = srcErr
		}
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

func (it *cardinalityManyScanAllEntitiesIterator) Error() error { return it.err }

// matchVectorScanAllEntities handles [?e :attr <vector-literal>] where E is unbound.
// Scans all entities with the attribute and resolves each vector using RGA.
// If v is non-nil, only entities whose resolved vector equals v are returned.
// If v is nil, all entities with non-empty vectors are returned.
func (m *BadgerMatcher) matchVectorScanAllEntities(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	a, v interface{},
	valueType schema.ValueType,
) (executor.Relation, error) {
	var aBytes [32]byte
	if kw, ok := a.(datalog.Keyword); ok {
		copy(aBytes[:], kw.String())
	}

	// Scan AEVT to find all entities with this attribute
	prefix := make([]byte, 1+32)
	prefix[0] = byte(AEVT)
	copy(prefix[1:33], aBytes[:])

	storageIter, err := m.reader.Scan(AEVT, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, fmt.Errorf("AEVT scan failed: %w", err)
	}

	iter := &vectorScanAllEntitiesIterator{
		matcher:      m,
		pattern:      pattern,
		symbols:      symbols,
		a:            a,
		v:            v,
		aBytes:       aBytes,
		valueType:    valueType,
		storageIter:  storageIter,
		seenEntities: make(map[[20]byte]bool),
	}

	return executor.NewStreamingRelationWithOptions(symbols, iter, m.options), nil
}

// vectorScanAllEntitiesIterator streams results for vector patterns with E unbound.
// For each unique entity, resolves the RGA vector and yields a tuple if it matches.
type vectorScanAllEntitiesIterator struct {
	matcher      *BadgerMatcher
	pattern      *query.DataPattern
	symbols      []query.Symbol
	a, v         interface{}
	aBytes       [32]byte
	valueType    schema.ValueType
	storageIter  Iterator
	seenEntities map[[20]byte]bool

	currentTuple executor.Tuple
	err          error // First error from storage operations
}

func (it *vectorScanAllEntitiesIterator) Next() bool {
	for it.storageIter.Next() {
		datom, err := it.storageIter.Datom()
		if err != nil {
			it.err = err
			return false
		}

		eBytes := datom.E.Hash()
		if it.seenEntities[eBytes] {
			continue
		}
		it.seenEntities[eBytes] = true

		// Resolve vector for this entity
		result, err := it.matcher.resolveVector(eBytes[:], it.aBytes[:])
		if err != nil {
			it.err = err
			return false
		}

		// Never set — skip
		if result.Stats.TotalElements == 0 {
			continue
		}

		resolved := typedVector(result.Elements, it.valueType)

		if it.v != nil {
			// V is bound — compare
			if !datalog.ValuesEqual(resolved, it.v) {
				continue
			}
		} else if len(result.Elements) == 0 {
			// Unbound V: skip empty vectors
			continue
		}

		// Build tuple
		tuple := make(executor.Tuple, len(it.symbols))
		for i, sym := range it.symbols {
			switch {
			case it.pattern.GetE() != nil && it.pattern.GetE().IsVariable() &&
				it.pattern.GetE().(query.Variable).Name == sym:
				tuple[i] = datom.E
			case it.pattern.GetA() != nil && it.pattern.GetA().IsVariable() &&
				it.pattern.GetA().(query.Variable).Name == sym:
				tuple[i] = it.a
			case it.pattern.GetV() != nil && it.pattern.GetV().IsVariable() &&
				it.pattern.GetV().(query.Variable).Name == sym:
				tuple[i] = resolved
			case it.pattern.GetT() != nil && it.pattern.GetT().IsVariable() &&
				it.pattern.GetT().(query.Variable).Name == sym:
				tuple[i] = result.MaxElementID
			}
		}
		it.currentTuple = tuple
		return true
	}
	// Propagate any deferred error from the inner storage iterator.
	if srcErr := it.storageIter.Error(); srcErr != nil && it.err == nil {
		it.err = srcErr
	}
	return false
}

func (it *vectorScanAllEntitiesIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

func (it *vectorScanAllEntitiesIterator) Close() error {
	if it.storageIter != nil {
		return it.storageIter.Close()
	}
	return nil
}

func (it *vectorScanAllEntitiesIterator) Error() error { return it.err }

// matchCardinalityManyScanAllEntities handles [?e :attr ?v] where E is unbound
// Scans all entities with the attribute and resolves each set using add-wins
func (m *BadgerMatcher) matchCardinalityManyScanAllEntities(
	pattern *query.DataPattern,
	symbols []query.Symbol,
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

	storageIter, err := m.reader.Scan(AEVT, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, fmt.Errorf("AEVT scan failed: %w", err)
	}

	// Create streaming iterator
	iter := &cardinalityManyScanAllEntitiesIterator{
		matcher:      m,
		pattern:      pattern,
		symbols:      symbols,
		a:            a,
		aBytes:       aBytes,
		storageIter:  storageIter,
		seenEntities: make(map[[20]byte]bool),
	}

	return executor.NewStreamingRelationWithOptions(symbols, iter, m.options), nil
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
	symbols     []query.Symbol
	indexer     *query.TupleIndexer
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
	err          error // First error from storage operations
}

func (it *cardinalityManyAVETValueIterator) Next() bool {
	if it.done {
		return false
	}

	for {
		if !it.storageIter.Next() {
			// End of scan - emit final entity if it's a member.
			// Also surface any deferred error from the inner iterator
			// so Error() reflects deep failures.
			if srcErr := it.storageIter.Error(); srcErr != nil && it.err == nil {
				it.err = srcErr
			}
			if it.hasCurrentEntity && it.isCurrentEntityMember() {
				it.buildTuple()
				it.done = true
				return true
			}
			return false
		}

		datom, err := it.storageIter.Datom()
		if err != nil {
			it.err = err
			return false
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
	// Positions precomputed once at construction (query.TupleIndexer); this
	// iterator fills from add-wins state rather than a datom, so the indexer
	// applies directly instead of an InternedTupleBuilder.
	tuple := make(executor.Tuple, len(it.symbols))
	if it.indexer.EIndex >= 0 {
		tuple[it.indexer.EIndex] = it.currentEntity
	}
	if it.indexer.AIndex >= 0 {
		tuple[it.indexer.AIndex] = it.a
	}
	if it.indexer.VIndex >= 0 {
		tuple[it.indexer.VIndex] = it.v
	}
	if it.indexer.TIndex >= 0 {
		tuple[it.indexer.TIndex] = datalog.ElementID{}
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

func (it *cardinalityManyAVETValueIterator) Error() error { return it.err }

// cardinalityManyFindEntitiesWithValueIterator streams results for [?e :attr "value"] patterns
// where E is unbound and cardinality is many. It iterates through entities and checks
// membership for each, yielding one tuple per entity where the value is in the set.
//
// DEPRECATED: Use cardinalityManyAVETValueIterator instead for O(k) performance.
// This iterator uses AEVT which is O(n) where n = all entities with attribute.
type cardinalityManyFindEntitiesWithValueIterator struct {
	matcher      *BadgerMatcher
	pattern      *query.DataPattern
	symbols      []query.Symbol
	a, v         interface{}
	aBytes       [32]byte
	storageIter  Iterator
	seenEntities map[[20]byte]bool
	currentTuple executor.Tuple
	err          error // First error from storage operations
}

func (it *cardinalityManyFindEntitiesWithValueIterator) Next() bool {
	for it.storageIter.Next() {
		datom, err := it.storageIter.Datom()
		if err != nil {
			if it.err == nil {
				it.err = err
			}
			return false
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
			if it.err == nil {
				it.err = err
			}
			return false
		}

		if !isMember {
			continue
		}

		// Value is in the set - build tuple
		tuple := make(executor.Tuple, len(it.symbols))
		for i, sym := range it.symbols {
			switch {
			case it.pattern.GetE() != nil && it.pattern.GetE().IsVariable() &&
				it.pattern.GetE().(query.Variable).Name == sym:
				tuple[i] = datom.E
			case it.pattern.GetA() != nil && it.pattern.GetA().IsVariable() &&
				it.pattern.GetA().(query.Variable).Name == sym:
				tuple[i] = it.a
			case it.pattern.GetV() != nil && it.pattern.GetV().IsVariable() &&
				it.pattern.GetV().(query.Variable).Name == sym:
				tuple[i] = it.v
			case it.pattern.GetT() != nil && it.pattern.GetT().IsVariable() &&
				it.pattern.GetT().(query.Variable).Name == sym:
				tuple[i] = datalog.ElementID{}
			}
		}
		it.currentTuple = tuple
		return true
	}
	// Propagate any deferred error from the inner storage iterator.
	if srcErr := it.storageIter.Error(); srcErr != nil && it.err == nil {
		it.err = srcErr
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

func (it *cardinalityManyFindEntitiesWithValueIterator) Error() error { return it.err }

// matchCardinalityManyFindEntitiesWithValue handles [?e :attr "value"] where E is unbound
// Finds all entities where the specific value is in the set.
//
// Uses AVET index with [A][V] prefix for O(k) lookup where k = datoms with this value,
// instead of O(n) where n = all entities with the attribute.
func (m *BadgerMatcher) matchCardinalityManyFindEntitiesWithValue(
	pattern *query.DataPattern,
	symbols []query.Symbol,
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

	storageIter, err := m.reader.Scan(AVET, prefix, prefixEnd(prefix))
	if err != nil {
		return nil, fmt.Errorf("AVET scan failed: %w", err)
	}

	// Create streaming iterator that applies add-wins resolution per entity
	iter := &cardinalityManyAVETValueIterator{
		matcher:     m,
		pattern:     pattern,
		symbols:     symbols,
		indexer:     query.NewTupleIndexer(pattern, symbols),
		a:           a,
		v:           v,
		aBytes:      aBytes,
		storageIter: storageIter,
	}

	return executor.NewStreamingRelationWithOptions(symbols, iter, m.options), nil
}
