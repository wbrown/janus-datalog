package storage

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/internal/errjoin"
	"github.com/wbrown/janus-datalog/datalog/query"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// Pattern matching implementation is split across multiple files:
//   - matcher_relations.go: Main Match() logic and strategy dispatch
//   - matcher_strategy.go: ReuseStrategy analysis and decision logic
//   - matcher_iterator_nonreusing.go: Simple per-tuple iteration
//   - matcher_iterator_unbound.go: Full scans without bindings
//   - hash_join_matcher.go: Binding-driven scans (hash join, merge join)
//
// Start with Match() and MatchWithConstraints() in this file.

const (
	keyBoundV   = "bound_v"
	keyBoundT   = "bound_t"
	keyBoundA   = "bound_a"
	keyPosition = "position"
	keyMatches  = "matches"
)

// Ensure PatternMatcher implements PatternMatcher
var _ executor.PatternMatcher = (*PatternMatcher)(nil)

// Ensure PatternMatcher implements executor.PredicateAwareMatcher
var _ executor.PredicateAwareMatcher = (*PatternMatcher)(nil)

// Match implements PatternMatcher.Match - returns a Relation directly
func (m *PatternMatcher) Match(q *query.Query, bindings executor.Relations) (executor.Relation, error) {
	// Default implementation with no constraints
	return m.MatchWithConstraints(q, bindings, nil)
}

// matchAsAssertions matches a pattern of five elements in a view that
// resolves, the latest or one as of a target. Every tuple such a view answers
// is a resolved value, an assertion, so the fifth position, whether the datom
// asserts, holds true for each. The constant true and a blank answer what the
// pattern's first four positions answer. A variable is bound to true beside
// each of their tuples. Any other constant, false among them, answers nothing.
func (m *PatternMatcher) matchAsAssertions(
	pattern *query.DataPattern,
	bindings executor.Relations,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	four := &query.DataPattern{Source: pattern.Source, Elements: pattern.Elements[:query.PatternPositions-1]}
	switch added := pattern.GetAdded().(type) {
	case query.Blank:
		return m.MatchWithConstraints(query.PatternQuery(four), bindings, constraints)
	case query.Variable:
		rel, err := m.MatchWithConstraints(query.PatternQuery(four), bindings, constraints)
		if err != nil {
			return nil, err
		}
		asserted := executor.NewMaterializedRelationWithOptions([]query.Symbol{added.Name}, []executor.Tuple{{true}}, m.options)
		return executor.NewProductRelation([]executor.Relation{rel, asserted}), nil
	case query.Constant:
		if added.Value != true {
			return executor.NewMaterializedRelationWithOptions(pattern.Symbols(), nil, m.options), nil
		}
		return m.MatchWithConstraints(query.PatternQuery(four), bindings, constraints)
	case query.VectorConstant:
		return executor.NewMaterializedRelationWithOptions(pattern.Symbols(), nil, m.options), nil
	default:
		panic(fmt.Sprintf("BUG: unknown pattern element %T at the fifth position", added))
	}
}

// MatchWithConstraints implements predicate-aware matching with storage-level filtering
func (m *PatternMatcher) MatchWithConstraints(
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
	if m.mode != historyMode && len(pattern.Elements) == query.PatternPositions {
		return m.matchAsAssertions(pattern, bindings, constraints)
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
		// binding: its zero tuples mean the upstream scan failed. Falling
		// back to an unbound scan here would launder that failure into
		// a silent result.
		if err := executor.EmptyRelationError(bindingRel); err != nil {
			return nil, err
		}
		return m.matchUnboundAsRelation(q, pattern, symbols, constraints)
	}

	// Project the binding relation to only include symbols used in the pattern
	bindingRel = bindingRel.ProjectFromPattern(pattern)

	// A declared vector attribute's value is the collection its element datoms
	// resolve to (RGA), so a pattern on one is intercepted before the join
	// paths, which match single datoms. Under History the element datoms are
	// the answer, read as written, and the joins below read them.
	var aResolved interface{}
	aResolved = m.extractValue(pattern.GetA())
	if aResolved == nil {
		if aVar, ok := pattern.GetA().(query.Variable); ok {
			kw, found, err := resolveKeywordFromBindings(aVar, bindings)
			if err != nil {
				return nil, err
			}
			if found {
				aResolved = kw
			}
		}
	}
	if aResolved != nil && m.mode != historyMode {
		if m.schema != nil {
			if kw, ok := aResolved.(datalog.Keyword); ok {
				if attr := m.schema.GetAttribute(kw); attr != nil {
					if attr.Cardinality == schema.CardinalityVector {
						vBound := m.extractValue(pattern.GetV())
						rel, err := m.matchVectorWithBindings(pattern, bindingRel, symbols, kw, vBound, attr.ValueType)
						if err != nil {
							return nil, err
						}
						return m.restoreScanSetSemantics(rel, pattern, symbols), nil
					}
				}
			}
		}
	}

	// A binding tuple that names no entity, no attribute or no datom's value
	// at the pattern's positions is the equality join's typed non-match: it
	// contributes no tuple and drives no seek (typedPositionBindingCheck). The
	// bindings are selected once here, after the vector arm, which compares a
	// vector against a declared vector attribute's whole collection, so the
	// cache arm, the strategy chooser's counting and every join strategy read
	// typed bindings alone. The selection reads the relation once into a
	// materialized one carrying its properties and its deferred error, which
	// every path below reads whole in any case.
	if typed := typedPositionBindingCheck(pattern, bindingRel.Symbols()); typed != nil {
		bindingRel = executor.Select(bindingRel, typed)
	}

	// CACHE OPTIMIZATION: When A is known (from pattern constant or bindings),
	// use the cache for each E value instead of storage scans.
	if m.handler != nil {
		m.handler(annotations.Event{
			Name: annotations.CacheCheck,
			Data: map[string]interface{}{
				annotations.KeyPattern: pattern,
				"a_resolved":           aResolved,
				"cache_nil":            m.cache == nil,
				"mode":                 m.mode,
				"has_bindings":         bindings != nil && len(bindings) > 0,
			},
		})
	}
	if m.cache != nil && m.mode != historyMode {
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
							Name: annotations.CacheMatchHandled,
							Data: map[string]interface{}{
								annotations.KeyPattern: pattern,
								"results":              cacheResult.Size(),
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
	strategy, bindingRel, err := m.analyzeReuseStrategy(pattern, bindingRel)
	if err != nil {
		return nil, err
	}

	// Emit strategy selection event
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  annotations.StorageReuseStrategy,
			Start: time.Now(),
			Data: map[string]interface{}{
				annotations.KeyPattern: pattern,
				annotations.KeyIndex:   strategy.Index,
				"strategy_type":        strategy.Type,
				keyPosition:            strategy.Position,
				"needs_validation":     strategy.NeedsValidation,
				keyBoundA:              strategy.BoundA,
			},
		})
	}

	// Use appropriate matching strategy. Every binding-driven scan flows
	// through the single exit below so the relation is restored to set
	// semantics at birth when the pattern's projection drops part of the
	// emitted stream's candidate key.
	var rel executor.Relation
	switch strategy.Type {
	case SinglePositionReuse:
		// A binding narrowing a run to its value, on an attribute resolving as
		// cardinality one, or to its transaction is served by candidates
		// validated per key, Theorem 5 of
		// docs/reference/INDEX_SELECTION_PROOF.md and T6 of
		// docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, at or below the
		// boundary. Above it, on a constant attribute, the binding is probed
		// against one resolved scan of the attribute under AETV, whose first
		// entry per entity is the current value, which answers the same
		// entities (A_VALUE_BINDING_ABOVE_THE_BOUNDARY_SCANS_ONCE_PROOF.md);
		// a transaction binding on an open attribute is probed against one
		// resolved scan of EATV, which holds every group whole.
		if strategy.NeedsValidation {
			large := bindingRel.Size() > executor.SmallBindingBoundary
			switch {
			case large && strategy.BoundA != nil:
				rel, err = m.matchWithHashJoin(pattern, bindingRel, symbols, strategy.Position, AETV, constraints)
			case large && strategy.Position == 3:
				rel, err = m.matchWithHashJoin(pattern, bindingRel, symbols, strategy.Position, EATV, constraints)
			default:
				rel, err = m.matchWithVValidation(pattern, bindingRel, symbols, strategy, constraints)
			}
			break
		}

		// Choose join strategy based on selectivity
		joinStrategy := m.chooseJoinStrategy(pattern, bindingRel, strategy.Position)

		// Emit join strategy selection event
		if m.handler != nil {
			m.handler(annotations.Event{
				Name:  annotations.StorageJoinStrategy,
				Start: time.Now(),
				Data: map[string]interface{}{
					annotations.KeyPattern: pattern,
					annotations.KeyIndex:   strategy.Index,
					"join_strategy":        joinStrategy,
					keyPosition:            strategy.Position,
				},
			})
		}

		switch joinStrategy {
		case HashJoinScan:
			// Use hash join for medium selectivity (1-50%)
			rel, err = m.matchWithHashJoin(pattern, bindingRel, symbols, strategy.Position, strategy.Index, constraints)

		case MergeJoin:
			// Use merge join for high selectivity (>50%) with large binding sets
			rel, err = m.matchWithMergeJoin(pattern, bindingRel, symbols, strategy.Position, strategy.Index, constraints)

		default:
			panic(fmt.Sprintf("unhandled join strategy %v", joinStrategy))
		}

	case NoReuse:
		fallthrough
	default:
		// Fall back to opening/closing iterator for each tuple
		rel, err = m.matchWithoutIteratorReuse(pattern, bindingRel, symbols, constraints)
	}
	if err != nil {
		return nil, err
	}
	return m.restoreScanSetSemantics(rel, pattern, symbols), nil
}

// restoreScanSetSemantics returns rel unchanged when the pattern scan
// provably emits a set: a held result is a set by its constructor, the
// deduplicating one or the proven-set one its producer chose (matchFromCache,
// matchWithBindingsFromCache), and streaming scans whose projection covers a
// candidate key of the emitted stream are injective
// (scanProjectionPreservesSet). Every other streaming scan wraps a
// deduplicating pass so the relation is a set at birth. Scan iterators reuse
// workspace tuples, so the dedup's seen-keys copy. The pass keeps the scan's
// values, so it carries the scan's identity symbols.
func (m *PatternMatcher) restoreScanSetSemantics(rel executor.Relation, pattern *query.DataPattern, symbols []query.Symbol) executor.Relation {
	if rel == nil {
		return nil
	}
	if _, ok := rel.(*executor.MaterializedRelation); ok {
		return rel
	}
	if scanProjectionPreservesSet(pattern, m.schema, m.mode == historyMode) {
		return rel
	}
	return executor.NewStreamingRelationWithProperties(
		symbols,
		executor.NewDedupIterator(rel.Iterator(), 0, true),
		m.options,
		executor.RelationProperties{Identities: rel.Properties().Identities},
	)
}

// matchUnboundAsRelation matches a pattern without bindings and returns a
// Relation, restored to set semantics at birth when the pattern's projection
// drops part of the emitted stream's candidate key.
func (m *PatternMatcher) matchUnboundAsRelation(
	q *query.Query,
	pattern *query.DataPattern,
	symbols []query.Symbol,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	rel, err := m.matchUnboundScan(q, pattern, symbols, constraints)
	if err != nil {
		return nil, err
	}
	return m.restoreScanSetSemantics(rel, pattern, symbols), nil
}

func (m *PatternMatcher) matchUnboundScan(
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
	tx = m.extractValue(pattern.GetT())

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
	var card datalog.Keyword = schema.CardinalityOne // Default for schemaless
	var valueType datalog.Keyword
	var def *schema.AttributeDefinition

	// Determine cardinality and value type when A is bound (regardless of E)
	if a != nil {
		if m.schema != nil {
			if aKw, ok := a.(datalog.Keyword); ok {
				def = m.schema.GetAttribute(aKw)
				if def != nil {
					card = def.Cardinality
					valueType = def.ValueType
				}
			}
		}
	}

	// A datom's value is a scalar: a declared vector attribute is stored as one
	// datom per element, and the collection is what resolution reconstructs
	// from them (resolveVector). A vector at the value position names that
	// collection, which the vector arms below compare under a constant vector
	// attribute, and matchVectorUnderOpenAttribute compares under every
	// declared vector attribute when the attribute is open, in the current and
	// as-of views; under any other constant attribute, and under History, where
	// every datom is read as written and nothing resolves, it equals no datom,
	// and the pattern answers the empty relation.
	if datalog.IsVector(v) {
		if m.mode == historyMode || (a != nil && card != schema.CardinalityVector) {
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), nil
		}
		if a == nil {
			return m.matchVectorUnderOpenAttribute(pattern, symbols, v)
		}
	}

	// CACHE OPTIMIZATION: When E and A are bound, use the cache for O(1)
	// access instead of storage scans. An as-of matcher reads its view's cache
	// (Cache.ForView); History matchers bypass cache.
	if m.cache != nil && m.mode != historyMode && e != nil && a != nil {
		if eIdent, ok := e.(datalog.Identity); ok {
			if aKw, ok := a.(datalog.Keyword); ok {
				cacheResult, handled, err := m.matchFromCache(pattern, symbols, eIdent, aKw, v, tx, def, valueType)
				if err != nil {
					return nil, err
				}
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
	} else if txEid, txNarrows := datalog.DerefElementID(tx); e == nil && m.mode != historyMode && (txNarrows || (v != nil && (a == nil || card == schema.CardinalityOne))) {
		// E is unbound, in the current or an as-of view, and the pattern
		// binds T to an ElementID or V to a constant under an attribute open
		// or resolving as cardinality one. A run narrowed to T holds one
		// operation's datoms and a run narrowed to V the datoms carrying the
		// value, and neither holds a pair's group whole, so each datom the
		// run emits is a candidate validated against the pair's resolved
		// datom (docs/reference/INDEX_SELECTION_PROOF.md Theorem 2 & 5,
		// docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md T6). With T
		// bound the run is the T-run, the narrower of the two (T7): ATEV
		// under [A][T] with a bound A, TAEV under [T] with an open one. With
		// T free it is AVET under [A][V] or VAET under [V], and each
		// candidate's own attribute decides its validation, as on the binding
		// route (analyzeReuseStrategy). Under History nothing resolves and
		// there is no resolved datom: the raw scan below reads every datom
		// carrying the bound positions, which is the answer. A T bound to a
		// Lamport integer narrows no run and is compared on the resolved
		// datom by the V-run or by the whole-group scan below.
		strategy := ReuseStrategy{
			Type:            SinglePositionReuse,
			NeedsValidation: true,
			BoundA:          a,
		}
		var narrowing datalog.Value
		if txNarrows {
			strategy.Position, strategy.Index, narrowing = 3, TAEV, txEid
			if a != nil {
				strategy.Index = ATEV
			}
		} else {
			strategy.Position, strategy.Index, narrowing = 2, VAET, v
			if a != nil {
				strategy.Index = AVET
			}
		}
		dummySym := datalog.NewSymbol("__bound_dummy__")
		bindingRel := executor.NewMaterializedRelation(
			[]query.Symbol{dummySym},
			[]executor.Tuple{{narrowing}},
		)
		return m.matchWithVValidation(pattern, bindingRel, symbols, strategy, nil)
	} else if e == nil && a != nil && card == schema.CardinalityVector && m.mode != historyMode {
		// E unbound, A bound, cardinality-vector, in the current or an as-of
		// view: scan all entities with this attribute, resolve each vector from
		// its element datoms and compare it against V, nil for unbound or the
		// bound literal. Under History nothing resolves: the raw scan below
		// reads each element datom as written.
		return m.matchVectorScanAllEntities(pattern, symbols, a.(datalog.Keyword), v, valueType)
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

	// Under History nothing resolves: every flag is cleared, and the raw scan
	// below reads each datom as written.
	if m.mode == historyMode {
		returnOnlyFirst = false
		useAddWinsResolution = false
		useMembershipCheck = false
		useAddWinsScanAllEntities = false
		useAddWinsScanAllEntitiesWithValue = false
		useVectorResolution = false
	}

	// For cardinality-vector with E+A bound: use vector resolution
	if useVectorResolution {
		return m.matchCardinalityVectorAsRelation(pattern, symbols, e.(datalog.Identity), a.(datalog.Keyword), v, valueType)
	}

	// For cardinality-many with E+A bound, V unbound: use add-wins resolution
	if useAddWinsResolution {
		return m.matchCardinalityManyAsRelation(pattern, symbols, e.(datalog.Identity), a.(datalog.Keyword), tx)
	}

	// For cardinality-many with E+A+V bound: use membership check
	if useMembershipCheck {
		return m.matchCardinalityManyMembership(pattern, symbols, e.(datalog.Identity), a.(datalog.Keyword), v, tx)
	}

	// For cardinality-many with A bound, E unbound, V unbound: scan all entities
	if useAddWinsScanAllEntities {
		return m.matchCardinalityManyScanAllEntities(pattern, symbols, a.(datalog.Keyword), def, tx)
	}

	// For cardinality-many with A bound, E unbound, V bound: find entities with value
	if useAddWinsScanAllEntitiesWithValue {
		return m.matchCardinalityManyFindEntitiesWithValue(pattern, symbols, a.(datalog.Keyword), v, tx)
	}

	// Choose index and create scan bound
	bound := m.chooseIndex(e, a, v, tx)
	properties := unboundScanProperties(pattern, bound.Index, card, m.mode == historyMode)
	if orderedProperties, ok := historyTAEVProperties(q, pattern, m.mode == historyMode); ok {
		bound = ScanBound{Index: TAEV}
		properties = orderedProperties
	} else if orderedProperties, ok := historyAETVProperties(q, pattern, m.mode == historyMode); ok {
		if keyword, keywordOK := a.(datalog.Keyword); keywordOK {
			bound = ScanBound{Index: AETV, Prefix: []datalog.Value{keyword}}
			properties = orderedProperties
		}
	} else if orderedProperties, ok := historyEATVProperties(q, pattern, m.mode == historyMode); ok {
		if identity, identityOK := e.(datalog.Identity); identityOK {
			bound = ScanBound{Index: EATV, Prefix: []datalog.Value{identity}}
			properties = orderedProperties
		}
	} else if orderedProperties, ok := historyATEVProperties(q, pattern, m.mode == historyMode); ok {
		if keyword, keywordOK := a.(datalog.Keyword); keywordOK {
			bound = ScanBound{Index: ATEV, Prefix: []datalog.Value{keyword}}
			properties = orderedProperties
		}
	}

	var report *scanReport
	if m.handler != nil {
		m.emitIndexSelection(pattern, bound)
		report = &scanReport{
			handler:  m.handler,
			opened:   time.Now(),
			strategy: annotations.ScanDirect,
			cause:    map[string]interface{}{annotations.KeyPattern: pattern},
			run:      &bound,
		}
	}

	// Streaming iterator: key-only scan wrapped with CRDT resolution in
	// current/as-of mode, or raw scan in history mode.
	regularIter := &unboundIterator{
		matcher:         m,
		report:          report,
		pattern:         pattern,
		symbols:         symbols,
		e:               e,
		a:               a,
		v:               v,
		tx:              tx,
		added:           m.extractValue(pattern.GetAdded()),
		constraints:     constraints,
		workspace:       make(executor.Tuple, len(symbols)),
		tupleBuilder:    m.getTupleBuilder(pattern, symbols),
		returnOnlyFirst: returnOnlyFirst, // CRDT cardinality-one support
	}

	rawStorageIter, err := OpenKeyScan(m.reader, report, bound)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	if m.mode == historyMode {
		regularIter.storageIter = rawStorageIter
	} else {
		regularIter.storageIter = NewCRDTResolvingIterator(rawStorageIter, m.schema, m, report)
	}

	// Return streaming relation with lazy materialization
	// The iterator will be consumed and cached on first call to Iterator()
	properties.Identities = executor.AnswerIdentities(pattern)
	rel := executor.NewStreamingRelationWithProperties(
		symbols,
		regularIter,
		m.options,
		properties,
	)
	return rel, nil
}

// matchWithoutIteratorReuse uses separate scan for each binding tuple
func (m *PatternMatcher) matchWithoutIteratorReuse(pattern *query.DataPattern, bindingRel executor.Relation, symbols []query.Symbol, constraints []executor.StorageConstraint) (executor.Relation, error) {
	// Emit no-reuse path event
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  annotations.StorageNoReusePath,
			Start: time.Now(),
			Data: map[string]interface{}{
				annotations.KeyPattern: pattern,
				"relation_type":        fmt.Sprintf("%T", bindingRel),
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

	report := DiscardIntake
	if m.handler != nil {
		report = &scanReport{
			handler:  m.handler,
			opened:   time.Now(),
			strategy: annotations.ScanPerBinding,
			cause: map[string]interface{}{
				annotations.KeyPattern:     pattern,
				annotations.KeyBindingSize: len(bindingTuples),
			},
		}
	}

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
		report:           report,
	}

	// Return streaming relation with the iterator
	return executor.NewStreamingRelationWithProperties(symbols, iter, m.options,
		executor.RelationProperties{Identities: executor.AnswerIdentities(pattern)}), nil
}

// matchWithVValidation implements the candidate + validate pattern for a
// pattern whose binding narrows a run to its V or to its T. See
// docs/reference/INDEX_SELECTION_PROOF.md Theorem 5 and
// docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md T6.
//
// Algorithm, per binding:
//  1. Scan the run narrowed to the binding: AVET or VAET under its V, ATEV
//     or TAEV under its T (strategy.Position), for candidate datoms
//  2. For each candidate (E, A), resolve the pair's datom under A's
//     definition: the current value and its Tx through the EA cache or
//     ResolveLWW for cardinality one, the member's newest assertion for
//     many, the collection for a vector
//  3. If matchesDatom holds on the resolved datom with the bound V and T,
//     emit it; otherwise skip (the candidate is not the pair's datom)
func (m *PatternMatcher) matchWithVValidation(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
	strategy ReuseStrategy,
	constraints []executor.StorageConstraint,
) (executor.Relation, error) {
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  annotations.VValidationEntry,
			Start: time.Now(),
			Data: map[string]any{
				annotations.KeyPattern: pattern,
				annotations.KeyIndex:   strategy.Index,
				keyBoundA:              strategy.BoundA,
				"binding_rel":          bindingRel.Symbols(),
			},
		})
	}
	// Get sorted tuples for efficient iteration
	sortedTuples, err := bindingRel.Sorted()
	if err != nil {
		return nil, err
	}

	report := DiscardIntake
	if m.handler != nil {
		report = &scanReport{
			handler: m.handler,
			// Taken here rather than at the first candidate scan, because the
			// unique short-circuit resolves a claimant and emits without
			// opening one. Started at construction, the completion's latency
			// spans the whole per-binding sequence on either branch.
			opened:   time.Now(),
			strategy: annotations.ScanVValidation,
			cause: map[string]interface{}{
				annotations.KeyPattern:     pattern,
				annotations.KeyBindingSize: len(sortedTuples),
			},
		}
	}

	var boundDef *schema.AttributeDefinition
	if a, ok := strategy.BoundA.(datalog.Keyword); ok && m.schema != nil {
		boundDef = m.schema.GetAttribute(a)
	}

	// Create validating iterator
	iter := &validatingVBoundIterator{
		matcher:           m,
		pattern:           pattern,
		bindingRel:        bindingRel,
		tuples:            sortedTuples,
		candidateIndex:    strategy.Index,    // AVET or VAET under V, ATEV or TAEV under T
		candidatePosition: strategy.Position, // 2 for a run narrowed to V, 3 for one narrowed to T
		boundA:            strategy.BoundA,   // Constant A value if any
		boundDef:          boundDef,
		symbols:          symbols,
		constraints:      constraints,
		currentTupleIdx:  -1,
		workspace:        make(executor.Tuple, len(symbols)),
		patternExtractor: query.NewPatternExtractor(pattern, bindingRel.Symbols()),
		tupleBuilder:     m.getTupleBuilder(pattern, symbols),
		report:           report,
	}

	properties := validatedVBoundProperties(pattern, strategy)
	properties.Identities = executor.AnswerIdentities(pattern)
	return executor.NewStreamingRelationWithProperties(
		symbols,
		iter,
		m.options,
		properties,
	), nil
}

// validatingVBoundIterator is the candidate-and-validate route of
// docs/reference/INDEX_SELECTION_PROOF.md Theorem 5 and
// docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md T6. Per binding it opens
// a run narrowed to the binding's V (AVET, VAET) or to its T (ATEV, TAEV),
// which holds no pair's group whole, so each datom the resolving iterator
// emits from it is a candidate. The candidate's pair is resolved under its
// attribute's definition, and the pattern admits the pair's resolved datom
// when matchesDatom holds on it with the bound V and T. The resolved datom's
// tuple is what the iterator emits.
//
// Per cardinality the resolved datom is: for one, the pair's current value
// and its Tx through the EA cache or ResolveLWW (resolvedDatom); for many, the
// member's newest assertion, the candidate itself from a V-narrowed run
// (INDEX Theorem 3) and the first datom of the member's own EAVT run from a
// T-narrowed one (resolvedMemberDatom); for a vector, the collection and the
// group's highest ElementID (resolvedVectorDatom), from a T-narrowed run alone.
type validatingVBoundIterator struct {
	matcher     *PatternMatcher
	pattern     *query.DataPattern
	bindingRel  executor.Relation
	tuples      []executor.Tuple
	symbols     []query.Symbol
	constraints []executor.StorageConstraint

	// Index configuration
	candidateIndex IndexType // AVET or VAET narrowed to V; ATEV or TAEV narrowed to T
	// candidatePosition is the position the candidate run is narrowed to, 2
	// for V and 3 for T, the strategy's Position.
	candidatePosition int
	boundA            any // Constant A value (nil if A is variable)
	// boundDef is boundA's definition, nil when A is a variable or the schema
	// declares no definition of it. Every candidate of a bound attribute
	// carries it, so it is read once, when the iterator is built.
	boundDef *schema.AttributeDefinition

	// Iterator state - CRDTResolvingIterator wraps the raw scan
	crdtIter        *CRDTResolvingIterator // Wraps the candidate scan
	currentTupleIdx int                    // Current binding tuple index
	currentTuple    executor.Tuple         // Current result tuple
	workspace       executor.Tuple

	// Pattern extraction
	patternExtractor *query.PatternExtractor
	tupleBuilder     *query.InternedTupleBuilder

	// The binding's V and T: the pattern's constant at the position, the
	// tuple's value when the binding binds the position's variable, and nil
	// when the position is free.
	currentBoundV any
	currentBoundT any
	// currentNarrowing is the value the current binding's candidate run is
	// narrowed to: currentBoundV at position 2, and at position 3 the
	// ElementID currentBoundT holds.
	currentNarrowing any

	// vectorAnswers is the current binding's answer when its value is a
	// vector, drained before the next binding: every entity whose collection
	// under some declared vector attribute equals it
	// (matchVectorUnderOpenAttribute). Nil between such bindings.
	vectorAnswers executor.Iterator

	// report accounts for this arm. Its run is nil: this path opens a candidate
	// scan per binding, and those are peers with no one of them describing the
	// whole. Everything it drives acquires through the same report — the
	// per-binding candidate scans, the unique-ownership walk, and the cache
	// resolution validateCandidate performs per candidate, which reads through
	// no iterator this one holds — so intake survives each scan being dropped
	// for the next.
	report *scanReport

	err error // First error from storage operations
}

func (it *validatingVBoundIterator) Next() bool {
	for {
		// A vector-valued binding answers from the vector arms, never from a
		// candidate scan.
		if it.vectorAnswers != nil {
			if it.vectorAnswers.Next() {
				it.currentTuple = it.vectorAnswers.Tuple()
				if it.report != nil {
					it.report.matched++
				}
				return true
			}
			if err := it.vectorAnswers.Error(); err != nil {
				it.err = err
				return false
			}
			if err := it.vectorAnswers.Close(); err != nil {
				it.err = err
				return false
			}
			it.vectorAnswers = nil
		}

		// Try to get next CRDT-resolved result from current scan
		if it.crdtIter != nil {
			for it.crdtIter.Next() {
				datom, err := it.crdtIter.Datom()
				if err != nil {
					it.err = err
					return false
				}
				if it.matcher.handler != nil {
					// What CRDT resolution produced, before validation narrows
					// it. The gap between this and matched below is what the
					// candidate-then-validate design costs: candidates found
					// in the narrowed run whose pair resolves to another datom.
					//
					// Counted under the same condition that decides whether
					// Close will ever read it, so a run with nothing listening
					// does not tally per candidate for nobody.
					it.report.resolved++

					it.matcher.handler(annotations.Event{
						Name:  annotations.VValidationCandidate,
						Start: time.Now(),
						Data: map[string]any{
							annotations.KeyEntity:    datom.E,
							annotations.KeyAttribute: datom.A,
							"v":                      datom.V,
							"tx":                     datom.Tx,
							"op":                     datom.Op,
							keyBoundV:                it.currentBoundV,
							keyBoundT:                it.currentBoundT,
						},
					})
				}

				// The candidate run is narrowed to the binding's V or T under
				// the attribute when one is bound, and carries every entity's
				// datoms in that run; the pattern's entity, when it names one,
				// answers alone.
				if named, ok := it.pattern.GetE().(query.Constant); ok && !datom.E.Equal(named.Value.(datalog.Identity)) {
					continue
				}

				// A bound attribute's candidates all carry it; an open one's carry
				// their own.
				def := it.boundDef
				if it.boundA == nil && it.matcher.schema != nil {
					def = it.matcher.schema.GetAttribute(datom.A)
				}

				// The pair's resolved datom under its cardinality. The run holds
				// no pair's group whole, so the candidate decides nothing by
				// itself: the pair resolves to a datom the run may not hold, and
				// the pattern admits that datom or nothing.
				var resolved *datalog.Datom
				switch resolvedCardinality(def) {
				case schema.CardinalityOne:
					// An undeclared attribute resolves as cardinality one at every
					// layer (INDEX Theorem 3b), so its candidates resolve here too.
					var viaCache bool
					resolved, viaCache, err = it.resolvedDatom(datom.E, datom.A, def)
					if err != nil {
						it.err = err
						return false
					}
					matches := resolved != nil && it.matcher.matchesDatom(resolved, nil, nil, it.currentBoundV, it.currentBoundT, nil)
					if it.matcher.handler != nil {
						// The validation's outcome, under the arm that answered:
						// the pair's cache entry, the resolver finding the pair
						// holding no datom, or the resolver's datom.
						data := map[string]any{
							annotations.KeyEntity:    datom.E,
							annotations.KeyAttribute: datom.A,
							keyBoundV:                it.currentBoundV,
							keyBoundT:                it.currentBoundT,
							keyMatches:               matches,
						}
						name := annotations.VValidationResult
						switch {
						case viaCache:
							name = annotations.VValidationCacheResolved
							if resolved != nil {
								data["cached_v"] = resolved.V
								data["cached_tx"] = resolved.Tx
							}
						case resolved == nil:
							name = annotations.VValidationNoWinner
						default:
							data["winner_v"] = resolved.V
							data["winner_tx"] = resolved.Tx
							data["will_emit"] = matches
							data[annotations.KeyCardinality] = resolvedCardinality(def)
						}
						it.matcher.handler(annotations.Event{Name: name, Start: time.Now(), Data: data})
					}
					if !matches {
						continue
					}
				case schema.CardinalityMany:
					if it.candidatePosition == 2 {
						// A run narrowed to V holds the whole add-wins decision
						// of (E, A, V) (INDEX Theorem 3), so the emission is the
						// member's resolved datom.
						resolved = datom
					} else {
						// A run narrowed to T holds one operation's datom of the
						// member, so the member resolves over its own EAVT run.
						resolved, err = it.matcher.resolvedMemberDatom(ScanBound{Index: EAVT, Prefix: []datalog.Value{datom.E, datom.A, datom.V}}, it.report)
						if err != nil {
							it.err = err
							return false
						}
					}
					if resolved == nil || !it.matcher.matchesDatom(resolved, nil, nil, it.currentBoundV, it.currentBoundT, nil) {
						continue
					}
				case schema.CardinalityVector:
					if it.candidatePosition == 2 {
						// A vector attribute's value is its whole collection, and
						// the bound V here is a scalar, which equals no collection:
						// the candidate is one element of it.
						continue
					}
					resolved, err = it.resolvedVectorDatom(datom.E, datom.A, def)
					if err != nil {
						it.err = err
						return false
					}
					if resolved == nil || !it.matcher.matchesDatom(resolved, nil, nil, it.currentBoundV, it.currentBoundT, nil) {
						continue
					}
				}

				// The resolved datom's tuple, whose V and Tx are the pair's.
				it.currentTuple = it.tupleBuilder.BuildTupleInterned(resolved)
				if it.report != nil {
					it.report.matched++
				}
				return true
			}
			// Inner iterator exhausted — propagate any deferred error
			// so Error() surfaces failures (e.g., unique-walk sub-scan
			// errors that caused the scan to abort rather than return
			// a datom).
			it.err = errjoin.Join(it.err, it.crdtIter.Error())
			it.err = errjoin.Join(it.err, it.crdtIter.Close())
			it.crdtIter = nil
		}

		// Move to next binding tuple
		it.currentTupleIdx++
		if it.currentTupleIdx >= len(it.tuples) {
			return false
		}

		// The binding's V and T: the pattern's constants, or the tuple's
		// values at the variables it binds.
		tuple := it.tuples[it.currentTupleIdx]
		it.currentBoundV = it.patternExtractor.ExtractV(tuple)
		it.currentBoundT = it.patternExtractor.ExtractT(tuple)
		if it.candidatePosition == 3 {
			// A T-narrowed run is addressed by an ElementID; a binding that
			// holds none at the position narrows no run.
			eid, ok := datalog.DerefElementID(it.currentBoundT)
			if !ok {
				continue
			}
			it.currentNarrowing = eid
		} else {
			if it.currentBoundV == nil {
				continue // V not bound in this tuple
			}
			it.currentNarrowing = it.currentBoundV

			// A vector names a declared vector attribute's whole collection,
			// which no candidate datom carries: the index holds a collection's
			// elements, each its own datom. The attribute is open on this
			// route, since a vector under a constant attribute is selected
			// out at the match's entry (typedPositionBindingCheck), so the
			// binding's answer is each declared vector attribute under which
			// the pattern's entity, or every entity when it names none,
			// carries a collection equal to it, drained before the next
			// binding.
			if datalog.IsVector(it.currentBoundV) {
				answers, err := it.matcher.matchVectorUnderOpenAttribute(it.pattern, it.symbols, it.currentBoundV)
				if err != nil {
					it.err = err
					return false
				}
				it.vectorAnswers = answers.Iterator()
				continue
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
				// Not counted here. This branch emits exactly when resolveAVLWW
				// found the claimant's current value to be the bound one, and the
				// resolver counts that against the same report — counting it again
				// would report one emission as two.
				return true
			} else if it.boundDef.HasUniqueConstraint() {
				// Attribute is unique but no valid claimant — skip this binding.
				continue
			}
		}

		// Open the resolving scan over the run narrowed to this binding.
		var err error
		it.crdtIter, err = it.openCRDTScan()
		if err != nil {
			it.err = err
			return false
		}
	}
}

// tryEmitUniqueWinner resolves the (A, V)-LWW winner for the current
// binding and, if a valid claimant exists whose assertion the bound T
// admits, sets it.currentTuple and returns (true, nil). If the attribute is
// not unique, returns (false, nil) and the caller falls through to the
// normal scan path. If the attribute is unique but no entity currently
// claims the bound value, or the claimant's assertion is not the bound
// transaction's, returns (false, nil) with a short-circuit indicator (see
// caller).
func (it *validatingVBoundIterator) tryEmitUniqueWinner() (bool, error) {
	if !it.boundDef.HasUniqueConstraint() {
		return false, nil
	}

	aKw := it.boundA.(datalog.Keyword)

	// Nested resolution contributes to the enclosing pattern's counts rather
	// than emitting an event of its own. Intake and what the walk resolved both
	// travel back on its funnel; matched stays this iterator's to report, from
	// what it actually emits.
	owner, ownerTx, err := it.matcher.resolveAVLWW(aKw, it.currentBoundV, it.report)
	if err != nil {
		return false, err
	}
	if owner == nil {
		return false, nil // No claimant; caller treats this as skip-binding.
	}
	if named, ok := it.pattern.GetE().(query.Constant); ok && !owner.Equal(named.Value.(datalog.Identity)) {
		return false, nil // The claimant is another entity than the pattern names.
	}

	// The claimant's resolved datom: its own assertion of the value, whose Tx
	// resolveAVLWW answers. The bound T admits it or nothing.
	winner := &datalog.Datom{
		E:  owner,
		A:  aKw,
		V:  it.currentBoundV,
		Tx: ownerTx,
	}
	if !it.matcher.matchesDatom(winner, nil, nil, nil, it.currentBoundT, nil) {
		return false, nil
	}
	it.currentTuple = it.tupleBuilder.BuildTupleInterned(winner)
	return true, nil
}

// resolvedDatom is the datom a cardinality-one pair resolves to in the
// matcher's view, def being A's definition, nil for an attribute the schema
// does not declare: the current value and the Tx of the assertion it was read
// from, as the matcher's lookups answer them for the pair under def. It reads
// the EA cache's entry under the view's read point when the cache is on, and
// ResolveLWW when it is off or holds no entry. For a definition declaring a
// unique the value is the walk's, the entity's newest assertion no later
// withdrawal cancels and no other entity's later claim supersedes, with that
// assertion's own Tx; for any other it is the newest visible entry. A pair
// holding no value, withdrawn or never asserted, resolves to no datom, which
// no bound position matches. viaCache reports which arm answered. A storage
// or decode failure surfaces as an error — it must never collapse to "no
// datom", which would silently drop valid results.
func (it *validatingVBoundIterator) resolvedDatom(e datalog.Identity, a datalog.Keyword, def *schema.AttributeDefinition) (resolved *datalog.Datom, viaCache bool, err error) {
	// An as-of matcher reads its view's own cache (Cache.ForView). History
	// resolves nothing and never takes this route.
	if it.matcher.cache != nil && it.matcher.mode != historyMode {
		entry, err := it.matcher.cache.GetOrResolve(CacheKey{E: e, A: a}, def, it.matcher, it.matcher.cacheBound(), it.matcher.handler, it.report)
		if err != nil {
			return nil, false, err
		}
		if entry != nil && entry.Cardinality() == schema.CardinalityOne {
			if entry.OneValue() == nil {
				return nil, true, nil
			}
			return &datalog.Datom{E: e, A: a, V: entry.OneValue(), Tx: entry.OneTx()}, true, nil
		}
	}

	// The cache is off, or holds no entry for the pair: resolve the pair as
	// the cache's rebuild does. The resolver's scans accrue into the arm's
	// report.
	v, tx, _, present, err := it.matcher.ResolveLWW(e, a, def, it.report)
	if err != nil {
		return nil, false, err
	}
	if !present || v == nil {
		return nil, false, nil
	}
	return &datalog.Datom{E: e, A: a, V: v, Tx: tx}, false, nil
}

// resolvedVectorDatom is the datom a vector pair resolves to in the matcher's
// view: the collection the RGA rule reconstructs, typed by the attribute's
// value type, under the group's highest ElementID, as the vector arms and the
// resolving iterator answer it. It reads the EA cache's entry when the cache
// is on and resolveVector when it is off or holds no entry. A pair with no
// element datom resolves to no datom. A storage or decode failure surfaces as
// an error.
func (it *validatingVBoundIterator) resolvedVectorDatom(e datalog.Identity, a datalog.Keyword, def *schema.AttributeDefinition) (*datalog.Datom, error) {
	if it.matcher.cache != nil && it.matcher.mode != historyMode {
		entry, err := it.matcher.cache.GetOrResolve(CacheKey{E: e, A: a}, def, it.matcher, it.matcher.cacheBound(), it.matcher.handler, it.report)
		if err != nil {
			return nil, err
		}
		if entry != nil && entry.Cardinality() == schema.CardinalityVector {
			return &datalog.Datom{E: e, A: a, V: typedVector(entry.VectorList(), def.ValueType), Tx: entry.Version()}, nil
		}
	}
	result, err := it.matcher.resolveVector(e, a, it.report)
	if err != nil {
		return nil, err
	}
	if !result.Present {
		return nil, nil
	}
	return &datalog.Datom{E: e, A: a, V: typedVector(result.Elements, def.ValueType), Tx: result.MaxElementID}, nil
}

// openCRDTScan opens the CRDT-resolving scan over the run narrowed to the
// current binding: AVET under [A][V] or VAET under [V] for a run narrowed to
// V, and ATEV under [A][T] or TAEV under [T] for one narrowed to T, the
// narrowing value being currentNarrowing. The CRDTResolvingIterator owns the
// raw scan and closes it.
func (it *validatingVBoundIterator) openCRDTScan() (*CRDTResolvingIterator, error) {
	var bound ScanBound

	if it.boundA != nil {
		aKw, ok := it.boundA.(datalog.Keyword)
		if !ok {
			return nil, fmt.Errorf("boundA is not a Keyword")
		}

		bound = ScanBound{
			Index:  it.candidateIndex,
			Prefix: []datalog.Value{aKw, it.currentNarrowing},
		}
	} else {
		bound = ScanBound{
			Index:  it.candidateIndex,
			Prefix: []datalog.Value{it.currentNarrowing},
		}
	}

	// One event per opened scan, reporting the run it addresses. It fires after
	// the bound is built so it can carry it, and on both branches: a branch
	// that reported an encoded key range instead would be naming bytes only
	// this backend's encoder can interpret, which is not what a run is.
	if it.matcher.handler != nil {
		data := map[string]any{
			keyBoundV: it.currentBoundV,
			keyBoundT: it.currentBoundT,
			keyBoundA: it.boundA,
		}
		addBoundFields(data, bound)
		it.matcher.handler(annotations.Event{
			Name:  annotations.VValidationOpenScan,
			Start: time.Now(),
			Data:  data,
		})
	}

	// Raw scan on V-primary index — one candidate scan per binding, peers with
	// no one of them describing the whole, so their count is what stands in a
	// bound's place for this arm.
	rawIter, err := OpenKeyScan(it.matcher.reader, it.report, bound)
	if err != nil {
		return nil, err
	}
	if it.report != nil {
		it.report.peers++
	}

	// Wrap with CRDTResolvingIterator for correct CRDT semantics
	// CRDTResolvingIterator handles:
	// - Group deduplication via contiguous comparison (O(1) space)
	// - Add-wins with same-Tx tiebreaking for CardinalityMany
	// - RGA for CardinalityVector
	// - LWW for CardinalityUnknown, an attribute the schema leaves undefined
	//   (same as CardinalityOne)
	//
	// The matcher carries the view, so candidate resolution reads the
	// snapshot: in as-of mode an entry after the target is skipped, and a
	// value withdrawn or superseded after T does not drop or alter a
	// candidate that existed as of T. Under History this arm is not taken.
	crdtIter := NewCRDTResolvingIterator(rawIter, it.matcher.schema, it.matcher, it.report)

	if it.matcher.handler != nil {
		// The same run the event above announced, restated rather than left to
		// be carried across from it. Two events thirty lines apart are two
		// events to a handler, which sees them interleaved with whatever the
		// other workers emitted in between; an index alone here would name a
		// component order whose components live on a line the reader has to go
		// find.
		opened := map[string]any{"crdt_wrapped": true}
		addBoundFields(opened, bound)
		it.matcher.handler(annotations.Event{
			Name:  annotations.VValidationScanOpened,
			Start: time.Now(),
			Data:  opened,
		})
	}

	return crdtIter, nil
}

func (it *validatingVBoundIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

// RequiresCopy is false: BuildTupleInterned builds a fresh tuple per
// emitted candidate and per unique winner.
func (it *validatingVBoundIterator) RequiresCopy() bool { return false }

func (it *validatingVBoundIterator) Close() error {
	var closeErr error
	if it.crdtIter != nil {
		closeErr = errjoin.Join(closeErr, it.crdtIter.Close())
		it.crdtIter = nil
	}
	if it.vectorAnswers != nil {
		closeErr = errjoin.Join(closeErr, it.vectorAnswers.Close())
		it.vectorAnswers = nil
	}
	it.err = errjoin.Join(it.err, closeErr)

	// Not gated on having opened a candidate scan: the unique short-circuit
	// resolves its claimant through a walk and emits without opening one, so
	// that guard would silence the branch that reads deepest.
	if it.report != nil {
		it.report.close(it.err == nil)
	}
	return closeErr
}

func (it *validatingVBoundIterator) Error() error { return it.err }

// matchFromCache attempts to resolve a pattern using the cache.
// Returns (relation, true, nil) if cache was used, (nil, false, nil) if fallback
// to storage is needed, and a non-nil error if resolution itself failed.
// This provides O(1) access for patterns with E and A bound when querying latest state.
//
// A cardinality-one pair's entry is its resolved datom, the value and the Tx
// of the assertion it was read from, and the pattern admits it when
// matchesDatom holds with the bound V and T
// (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T5). A many or vector
// entry carries the resolution's version for its tuples' Tx and compares no
// bound T.
//
// A resolution failure is distinct from "the cache cannot serve this". Falling
// back to storage on error would re-run the read that just failed and, if it
// failed for a reason the retry also hits, report it from a second place — or,
// worse, succeed and leave the first failure unaccounted. The error is the
// answer.
func (m *PatternMatcher) matchFromCache(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	e datalog.Identity,
	a datalog.Keyword,
	v interface{}, // nil if V is unbound
	tx interface{}, // nil if T is unbound
	def *schema.AttributeDefinition,
	valueType datalog.Keyword,
) (executor.Relation, bool, error) {
	key, ok := m.cacheKey(e, a)
	if !ok {
		return nil, false, nil
	}
	card := resolvedCardinality(def)

	// Get or resolve from cache
	annotating := m.handler != nil
	var opened time.Time
	report := DiscardIntake
	if annotating {
		report = &scanReport{}
		opened = time.Now()
	}
	entry, err := m.cache.GetOrResolve(key, def, m, m.cacheBound(), m.handler, report)
	if err != nil {
		return nil, false, err
	}

	// This arm is the default for every E-and-A-bound pattern, which makes it
	// the most common shape in the engine and the one a trace can least afford
	// to be silent about.
	//
	// It emits no pattern/index-selection because it addresses no run: the
	// cache picks the index by cardinality inside resolution, and a hit reads
	// no index at all. Announcing a bound here would name a run this call did
	// not choose and, on a hit, one that nothing walked.
	//
	// datoms.scanned is what *this call* read — the build cost when this call
	// built the entry, zero when it came from cache. Not entry.scanned, which
	// is the entry's build cost forever: reporting that on a hit would make a
	// trace of a thousand hits claim a thousand index reads that never
	// happened, and the key means this operation's intake everywhere else.
	// Emitted directly rather than through emitScanCompletion, because this arm
	// makes no funnel and that function's whole subject is one. A funnel is
	// three stages of a single scan, each narrowing the last; a hit performs
	// neither of the first two. Passing a scanFunnel here to get the shared
	// envelope would put back the inversion — one value served under zero
	// intake — in exchange for four lines.
	//
	// Intake and matched keep their meanings from the funnel; only the middle
	// term has none here, and values.served is what stands in its place.
	matched := 0
	if annotating {
		defer func() {
			// An absent (E, A) has no entry and therefore serves nothing. The
			// call still happened and still cost its intake, so it reports —
			// the absence of the event would be indistinguishable from a path
			// that never ran.
			served := 0
			if entry != nil {
				served = entry.valueCount()
			}
			m.handler(annotations.Event{
				Name:    annotations.StorageResolveComplete,
				Start:   opened,
				Latency: time.Since(opened),
				Data: map[string]interface{}{
					annotations.KeyPattern:       pattern,
					annotations.KeyCardinality:   card,
					annotations.KeyDatomsScanned: report.scanned,
					annotations.KeyValuesServed:  served,
					annotations.KeyDatomsMatched: matched,
				},
			})
		}()
	}

	if entry == nil {
		// The (E, A) carries no datoms, so the attribute does not exist and the
		// pattern matches nothing. This is the cache answering, not declining:
		// absence is a resolved state, and falling back to storage would re-run
		// the scan that just established it.
		return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
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
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
		}
		// The pair's resolved datom, admitted when the bound V and T hold on it.
		datomBuf.V = val
		datomBuf.Tx = entry.OneTx()
		if !m.matchesDatom(&datomBuf, nil, nil, v, tx, nil) {
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
		}
		tuple := buildTuple(val, entry.OneTx())
		matched = 1
		return executor.NewMaterializedRelationWithOptions(symbols, []executor.Tuple{tuple}, m.options), true, nil

	case schema.CardinalityMany:
		set := entry.ManySet()
		if len(set) == 0 {
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
		}
		// Each member's resolved datom is its newest assertion, whose Tx the
		// entry holds under the member's key; it is admitted when the bound V
		// and T hold on it (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md,
		// T11).
		memberTx := entry.ManyTx()
		if v != nil {
			// V is bound: the set holds each member under its memberKey, so
			// any representation of the bound value finds its member.
			key := memberKey(v)
			if _, exists := set[key]; exists {
				datomBuf.V = v
				datomBuf.Tx = memberTx[key]
				if !m.matchesDatom(&datomBuf, nil, nil, v, tx, nil) {
					return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
				}
				tuple := buildTuple(v, memberTx[key])
				matched = 1
				return executor.NewMaterializedRelationWithOptions(symbols, []executor.Tuple{tuple}, m.options), true, nil
			}
			return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
		}
		// V is unbound: every member the bound T admits
		tuples := make([]executor.Tuple, 0, len(set))
		for key, val := range set {
			datomBuf.V = val
			datomBuf.Tx = memberTx[key]
			if !m.matchesDatom(&datomBuf, nil, nil, v, tx, nil) {
				continue
			}
			tuples = append(tuples, buildTuple(val, memberTx[key]))
		}
		matched = len(tuples)
		return executor.NewMaterializedRelationWithOptions(symbols, tuples, m.options), true, nil

	case schema.CardinalityVector:
		// An entry exists only for an (E, A) that has datoms, so the resolved
		// vector is a value whatever its length — an empty one is a vector that
		// was cleared, and it binds. Absence was handled above, where there is
		// no entry at all.
		list := entry.VectorList()
		resolved := typedVector(list, valueType)
		if v != nil {
			// V is bound - compare resolved vector against bound value
			if !datalog.ValuesEqual(resolved, v) {
				return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
			}
			// Matched — fall through to build tuple
		}
		tuple := buildTuple(resolved, entry.Version())
		matched = 1
		return executor.NewMaterializedRelationWithOptions(symbols, []executor.Tuple{tuple}, m.options), true, nil
	}

	return nil, false, nil // Unknown cardinality, fallback to storage
}

// resolveKeywordFromBindings searches all binding relations for a single-tuple relation
// containing the given variable and returns its value as a Keyword. This covers scalar
// inputs and single-tuple tuple inputs where A has exactly one known value.
func resolveKeywordFromBindings(aVar query.Variable, bindings executor.Relations) (kw datalog.Keyword, found bool, err error) {
	for _, rel := range bindings {
		idx := query.SymbolIndex(rel.Symbols(), aVar.Name)
		if idx < 0 {
			continue
		}
		if rel.Size() != 1 {
			return nil, false, nil // multi-tuple — can't extract single value
		}
		iter := rel.Iterator()
		if iter.Next() {
			kw, found = iter.Tuple()[idx].(datalog.Keyword)
		}
		if scanErr := errjoin.Join(iter.Error(), iter.Close()); scanErr != nil {
			return nil, false, scanErr
		}
		return kw, found, nil
	}
	return nil, false, nil
}

// matchWithBindingsFromCache handles patterns where E comes from bindings and A is known.
// Uses cache for O(1) lookup per bound E value instead of storage scans.
// Returns (relation, true) if cache was used, (nil, false) if fallback to storage is needed.
//
// When aSymIdx >= 0, A is extracted per-tuple from bindingRel[aSymIdx] instead of using
// the fixed `a` parameter. This handles the case where both E and A are symbols in the
// binding relation (e.g., from join results with varying attributes per tuple).
//
// Both call sites reach this from inside a live-cache, non-history branch, so
// the one refusal cacheKey makes is already decided before the loop starts.
// That is what lets the completion event cover every path out of the loop: a
// refusal first noticed on the fourth binding would have to report three
// bindings' intake under a call that, as far as the event could say, read the
// index and served nothing.
func (m *PatternMatcher) matchWithBindingsFromCache(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
	a datalog.Keyword,
	aSymIdx int,
) (bindingMatch executor.Relation, fromCache bool, matchErr error) {
	// Find which symbol in the binding relation has E
	eVar, isVar := pattern.GetE().(query.Variable)
	if !isVar {
		return nil, false, nil // E is not a variable, can't get it from bindings
	}

	eSymIdx := query.SymbolIndex(bindingRel.Symbols(), eVar.Name)
	if eSymIdx < 0 {
		return nil, false, nil // E variable not in bindings
	}

	// A keyword wider than storage holds names no stored attribute: its
	// encoding into the key keeps its first 32 bytes, the key of whatever
	// attribute shares them, so it is never encoded and contributes no tuple.
	// With A fixed for the call the whole match answers nothing.
	if aSymIdx < 0 && datalog.ValidateAttributeWidth(a) != nil {
		return executor.NewMaterializedRelationWithOptions(symbols, nil, m.options), true, nil
	}

	// Pre-compute fixed-A values when A is constant across all tuples
	var fixedCard datalog.Keyword
	var fixedValueType datalog.Keyword
	var fixedDef *schema.AttributeDefinition
	if aSymIdx < 0 {
		fixedCard = schema.CardinalityOne
		if m.schema != nil {
			fixedDef = m.schema.GetAttribute(a)
			if fixedDef != nil {
				fixedCard = fixedDef.Cardinality
				fixedValueType = fixedDef.ValueType
			}
		}
	}

	// Extract V if bound
	var v interface{}
	if elem := pattern.GetV(); elem != nil {
		v = m.extractValue(elem)
	}

	// The bound T: the pattern's constant, or the binding's value at the
	// pattern's T variable when the binding carries it. A cardinality-one
	// entry's datom, and each member of a cardinality-many entry, is admitted
	// under it as under the bound V
	// (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T5, T11).
	var txConst interface{}
	txSymIdx := -1
	if elem := pattern.GetT(); elem != nil {
		txConst = m.extractValue(elem)
		if tVar, ok := elem.(query.Variable); ok {
			txSymIdx = query.SymbolIndex(bindingRel.Symbols(), tVar.Name)
		}
	}

	// Under a constant attribute a binding carrying the pattern's value
	// variable answers the datoms agreeing with it at the value too, as every
	// scan arm answers (docs/proposals/BOUND_VALUE_CACHE_MATCH_PROOF.md, T2).
	// The typed selection admits no vector at that position under a constant
	// attribute, so the value is a key of an entry's set. Under a variable
	// attribute the value is read from no binding, and the vector arm, reached
	// under a variable attribute alone, compares the pattern's constant alone.
	vSymIdx := -1
	if aSymIdx < 0 {
		if vVar, ok := pattern.GetV().(query.Variable); ok {
			vSymIdx = query.SymbolIndex(bindingRel.Symbols(), vVar.Name)
		}
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

	// One call covers a whole binding set, so each total is a sum over the
	// bindings it read and binding.size is what they are summed over.
	//
	// No index is named: the cache picks one per entry by cardinality, and this
	// call resolves many. Cardinality is named only when A is fixed for the
	// call, for the same reason — with A varying per tuple, one value would be
	// whichever tuple came last.
	annotating := m.handler != nil
	var opened time.Time
	report := DiscardIntake
	served, bindingsRead := 0, 0
	// Assigned on the successful return only. Every other way out of the loop is
	// a refusal or a failure that hands the pattern back for a full scan, and
	// those served the caller nothing however much they cost to discover.
	matched := 0
	if annotating {
		report = &scanReport{}
		opened = time.Now()
		defer func() {
			data := map[string]interface{}{
				annotations.KeyPattern:       pattern,
				annotations.KeyBindingSize:   bindingsRead,
				annotations.KeyDatomsScanned: report.scanned,
				annotations.KeyValuesServed:  served,
				annotations.KeyDatomsMatched: matched,
			}
			if aSymIdx < 0 {
				data[annotations.KeyCardinality] = fixedCard
			}
			m.handler(annotations.Event{
				Name:    annotations.StorageResolveComplete,
				Start:   opened,
				Latency: time.Since(opened),
				Data:    data,
			})
		}()
	}

	// Iterate bindings and collect results from cache. The bindings' count is
	// the result's under cardinality one and its floor under cardinality
	// many, so the result is sized to it when the relation reports one.
	var resultTuples []executor.Tuple
	if size := bindingRel.Size(); size > 0 {
		resultTuples = make([]executor.Tuple, 0, size)
	}
	iter := bindingRel.Iterator()
	defer func() {
		if closeErr := iter.Close(); closeErr != nil {
			bindingMatch, fromCache, matchErr = nil, false, errjoin.Join(matchErr, closeErr)
		}
	}()

	for iter.Next() {
		bindingsRead++
		tuple := iter.Tuple()
		if eSymIdx >= len(tuple) {
			continue
		}

		// The bindings hold typed values alone, selected at the match's entry
		// (MatchWithConstraints): an Identity at the entity position and a
		// Keyword at the attribute position.
		eIdent := tuple[eSymIdx].(datalog.Identity)

		// The value the binding bounds this match to: its own, when it
		// carries the pattern's value variable under a constant attribute,
		// else the pattern's constant.
		boundV := v
		if vSymIdx >= 0 {
			boundV = tuple[vSymIdx]
		}

		// Determine A and cardinality for this tuple
		var tupleCard datalog.Keyword
		var tupleValueType datalog.Keyword
		var tupleA datalog.Keyword
		var tupleDef *schema.AttributeDefinition
		if aSymIdx >= 0 {
			// Per-tuple A: extract from binding tuple
			if aSymIdx >= len(tuple) {
				continue
			}
			aKw := tuple[aSymIdx].(datalog.Keyword)
			if datalog.ValidateAttributeWidth(aKw) != nil {
				continue
			}
			tupleCard = schema.CardinalityOne
			if m.schema != nil {
				tupleDef = m.schema.GetAttribute(aKw)
				if tupleDef != nil {
					tupleCard = tupleDef.Cardinality
					tupleValueType = tupleDef.ValueType
				}
			}
			tupleA = aKw
			datomBuf.A = aKw
		} else {
			tupleCard = fixedCard
			tupleValueType = fixedValueType
			tupleDef = fixedDef
			tupleA = a
		}

		// The one refusal cacheKey makes is settled above, so the key here is
		// the pair itself.
		key := CacheKey{E: eIdent, A: tupleA}

		// Get from cache
		entry, err := m.cache.GetOrResolve(key, tupleDef, m, m.cacheBound(), m.handler, report)
		if err != nil {
			return nil, false, err
		}
		if entry == nil {
			// No datoms for this (E, A): the attribute is absent on this
			// entity, which contributes no tuple. It still cost its intake to
			// establish, which is already counted above.
			continue
		}
		if annotating {
			served += entry.valueCount()
		}

		// The transaction the binding bounds this match to: its own, when it
		// carries the pattern's T variable, else the pattern's constant.
		boundT := txConst
		if txSymIdx >= 0 {
			boundT = tuple[txSymIdx]
		}

		// Process based on cardinality
		switch tupleCard {
		case schema.CardinalityOne:
			val := entry.OneValue()
			if val == nil {
				continue // No value for this E
			}
			// The pair's resolved datom, admitted when the bound V and T hold
			// on it.
			datomBuf.E = eIdent
			datomBuf.V = val
			datomBuf.Tx = entry.OneTx()
			if !m.matchesDatom(&datomBuf, nil, nil, boundV, boundT, nil) {
				continue
			}
			resultTuples = append(resultTuples, buildTuple(eIdent, val, entry.OneTx()))

		case schema.CardinalityMany:
			set := entry.ManySet()
			if len(set) == 0 {
				continue
			}
			// Each member's resolved datom is its newest assertion, whose Tx
			// the entry holds under the member's key; it is admitted when the
			// bound V and T hold on it.
			memberTx := entry.ManyTx()
			datomBuf.E = eIdent
			if boundV != nil {
				// The set holds each member under its memberKey, so any
				// representation of the bound value finds its member.
				key := memberKey(boundV)
				if _, exists := set[key]; !exists {
					continue
				}
				datomBuf.V = boundV
				datomBuf.Tx = memberTx[key]
				if !m.matchesDatom(&datomBuf, nil, nil, boundV, boundT, nil) {
					continue
				}
				resultTuples = append(resultTuples, buildTuple(eIdent, boundV, memberTx[key]))
			} else {
				for key, val := range set {
					datomBuf.V = val
					datomBuf.Tx = memberTx[key]
					if !m.matchesDatom(&datomBuf, nil, nil, boundV, boundT, nil) {
						continue
					}
					resultTuples = append(resultTuples, buildTuple(eIdent, val, memberTx[key]))
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
			resultTuples = append(resultTuples, buildTuple(eIdent, typedVector(list, tupleValueType), entry.Version()))
		}
	}
	// A failed bindings scan is not an exhausted one — surface it rather
	// than answering from a truncated binding set.
	if err := iter.Error(); err != nil {
		return nil, false, err
	}

	matched = len(resultTuples)
	// Bindings carrying the positions this match reads alone answer distinct
	// tuples. The read positions are the entity, the attribute under a
	// variable attribute, the value under a constant attribute when the
	// bindings carry it, and the transaction when they carry it. The bindings
	// are a set, each answers distinct tuples carrying its own values at the
	// read positions, and a pattern names each variable at one position
	// (query.UnifyPatterns). Bindings carrying another of the pattern's
	// variables can repeat the read positions and so repeat tuples, which the
	// deduplicating constructor removes
	// (docs/proposals/BINDING_MATCH_PROOF.md, T3, and
	// docs/proposals/BOUND_VALUE_CACHE_MATCH_PROOF.md, T3).
	readPositions := 1
	if aSymIdx >= 0 {
		readPositions = 2
	}
	if vSymIdx >= 0 {
		readPositions++
	}
	if txSymIdx >= 0 {
		readPositions++
	}
	properties := executor.RelationProperties{Identities: executor.AnswerIdentities(pattern)}
	if len(bindingRel.Symbols()) == readPositions {
		return executor.NewMaterializedRelationFromSet(symbols, resultTuples, m.options, properties), true, nil
	}
	return executor.NewMaterializedRelationWithProperties(symbols, resultTuples, m.options, properties), true, nil
}

// matchCardinalityManyAsRelation answers a pattern binding E and A on a
// cardinality-many attribute with V free: each member the add-wins rule keeps
// in the set, as its resolved datom, the member's newest assertion, when the
// bound T, when the pattern binds one, holds on that assertion.
func (m *PatternMatcher) matchCardinalityManyAsRelation(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	e datalog.Identity,
	a datalog.Keyword,
	tx interface{},
) (rel executor.Relation, matchErr error) {
	// The resolver acquires its scan through this report, so the run it walks is
	// recorded here without the arm holding an iterator or writing an emit.
	// Intake is every add and remove ever written for the (E, A); the members
	// are what add-wins left standing.
	var report *scanReport
	if m.handler != nil {
		report = &scanReport{
			handler:  m.handler,
			opened:   time.Now(),
			strategy: annotations.ScanDirect,
			cause:    map[string]interface{}{annotations.KeyPattern: pattern},
		}
		defer func() { report.close(matchErr == nil) }()
	}

	result, err := m.resolveAddWinsSet(e, a, report)
	if err != nil {
		return nil, fmt.Errorf("add-wins resolution failed: %w", err)
	}
	if report != nil {
		report.resolved += len(result.Members)
		m.emitIndexSelection(pattern, result.Bound)
	}

	// Each member's resolved datom is its newest assertion, whose Tx the
	// resolution holds under the member's key; it is admitted when the bound
	// T holds on it (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T11).
	tupleBuilder := m.getTupleBuilder(pattern, symbols)
	datom := datalog.Datom{E: e, A: a}
	tuples := make([]executor.Tuple, 0, len(result.Members))
	for key, member := range result.Members {
		datom.V = member
		datom.Tx = result.MemberTx[key]
		if !m.matchesDatom(&datom, nil, nil, nil, tx, nil) {
			continue
		}
		tuples = append(tuples, tupleBuilder.BuildTupleInterned(&datom))
	}
	if report != nil {
		report.matched += len(tuples)
	}

	return executor.NewMaterializedRelation(symbols, tuples), nil
}

// matchCardinalityVectorAsRelation handles cardinality-vector patterns using RGA resolution.
// Returns the entire reconstructed vector as a single value bound to the V variable.
func (m *PatternMatcher) matchCardinalityVectorAsRelation(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	e datalog.Identity,
	a datalog.Keyword,
	v interface{},
	valueType datalog.Keyword,
) (rel executor.Relation, matchErr error) {
	// Resolve the vector using RGA reconstruction
	var opened time.Time
	report := DiscardIntake
	if m.handler != nil {
		report = &scanReport{}
		opened = time.Now()
	}
	result, err := m.resolveVector(e, a, report)
	if err != nil {
		return nil, fmt.Errorf("vector resolution failed: %w", err)
	}

	// Reported on every exit below, of which there are three. An RGA group's
	// intake is every insert and tombstone ever written for the (E, A), and the
	// exits that produce no tuple — never set, bound V mismatched — are exactly
	// the ones where the gap between what was read and what came out is widest,
	// so those are the paths that most need to say so.
	matched := 0
	if m.handler != nil {
		m.emitIndexSelection(pattern, result.Bound)
		defer func() {
			run := map[string]interface{}{
				annotations.KeyPattern:  pattern,
				annotations.KeyStrategy: annotations.ScanDirect,
			}
			addBoundFields(run, result.Bound)
			emitScanCompletion(m.handler, annotations.StorageScanComplete,
				opened,
				scanFunnel{
					scanned:  report.scanned,
					resolved: len(result.Elements),
					matched:  matched,
				},
				matchErr == nil,
				run)
		}()
	}

	// No datoms at all → attribute was never set
	if !result.Present {
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

	// Past the TotalElements check above, an empty element list means the
	// vector was set and then cleared: datoms exist and RGA reconstruction
	// resolved them to nothing live. That is a value — the empty vector — and
	// it binds. Absence is the case handled above, where the (E, A) carries no
	// datoms at all, and it is the only case that produces no tuple here.

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
		case pattern.GetT().IsVariable() &&
			pattern.GetT().(query.Variable).Name == sym:
			// Use the max ElementID as the "current" transaction
			tuple[i] = result.MaxElementID
		}
	}

	// Return single-tuple relation with the vector
	matched = 1
	return executor.NewMaterializedRelation(symbols, []executor.Tuple{tuple}), nil
}

// matchVectorWithBindings handles vector patterns when E is bound via join bindings.
// For each entity in the bindings, it resolves the vector using RGA reconstruction
// and returns tuples with the reconstructed vector as the V value.
func (m *PatternMatcher) matchVectorWithBindings(
	pattern *query.DataPattern,
	bindingRel executor.Relation,
	symbols []query.Symbol,
	attr datalog.Keyword,
	v interface{},
	valueType datalog.Keyword,
) (rel executor.Relation, matchErr error) {
	// Find which symbol in bindings provides the entity
	var eSymIdx int = -1
	bindingSyms := bindingRel.Symbols()

	// Find the entity variable in the pattern and match it to binding symbols
	if pattern.GetE() != nil && pattern.GetE().IsVariable() {
		eSymIdx = query.SymbolIndex(bindingSyms, pattern.GetE().(query.Variable).Name)
	}

	if eSymIdx == -1 {
		// The binding binds the value and not the entity. A vector's value
		// names the whole collection, so no run is narrowed by it: the
		// pattern's entity, or every entity when it names none, has its
		// vector resolved and the bound values are compared at the collapse.
		return m.matchVectorScanAllEntities(pattern, symbols, attr, v, valueType)
	}

	// One RGA resolution per binding tuple, each walking its own run, so this
	// names no index and reports the count instead. An RGA group is every insert
	// and tombstone ever written for the (E, A), and the exits below that
	// produce no tuple — never set, bound V mismatched — are where the gap
	// between what was read and what came out is widest.
	var opened time.Time
	report := DiscardIntake
	var resolutions, elementsResolved int
	if m.handler != nil {
		report = &scanReport{}
		opened = time.Now()
	}

	// Iterate through bindings and resolve vector for each entity
	var tuples []executor.Tuple
	it := bindingRel.Iterator()
	defer func() {
		if closeErr := it.Close(); closeErr != nil {
			rel, matchErr = nil, errjoin.Join(matchErr, closeErr)
		}
	}()

	if m.handler != nil {
		defer func() {
			emitScanCompletion(m.handler, annotations.StorageScanComplete,
				opened,
				scanFunnel{
					scanned:  report.scanned,
					resolved: elementsResolved,
					matched:  len(tuples),
				},
				matchErr == nil,
				map[string]interface{}{
					annotations.KeyPattern:       pattern,
					annotations.KeyStrategy:      annotations.ScanPerBinding,
					annotations.KeyRunsAddressed: resolutions,
				})
		}()
	}

	for it.Next() {
		bindingTuple := it.Tuple()
		eVal := bindingTuple[eSymIdx]

		ident, ok := eVal.(datalog.Identity)
		if !ok {
			continue // Skip non-identity values
		}

		// Resolve the vector using RGA reconstruction
		resolutions++
		result, err := m.resolveVector(ident, attr, report)
		if err != nil {
			return nil, fmt.Errorf("vector resolution failed for entity: %w", err)
		}
		elementsResolved += len(result.Elements)

		resolved := typedVector(result.Elements, valueType)

		neverSet := !result.Present

		// Entity has no datoms at all for this attribute — skip always
		if neverSet {
			continue
		}

		// If V is bound, compare the resolved vector against bound value
		if v != nil && !datalog.ValuesEqual(resolved, v) {
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
			case pattern.GetT().IsVariable() &&
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

	return executor.NewMaterializedRelationWithProperties(symbols, tuples, executor.ExecutorOptions{},
		executor.RelationProperties{Identities: executor.AnswerIdentities(pattern)}), nil
}

// matchCardinalityManyMembership answers a pattern binding E, A and V on a
// cardinality-many attribute: the member's resolved datom, its newest
// assertion, when the add-wins rule keeps the value in the set and the bound
// T, when the pattern binds one, holds on that assertion.
func (m *PatternMatcher) matchCardinalityManyMembership(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	e datalog.Identity,
	a datalog.Keyword,
	v interface{},
	tx interface{},
) (executor.Relation, error) {
	// Bind [E][A][V] on EAVT, which covers every op recorded for this value.
	// A string or bytes V makes the run inexact, so the store steps over the
	// keys the range over-covers; the statistics below are what make that
	// stepping visible.
	bound := ScanBound{
		Index:  EAVT,
		Prefix: []datalog.Value{e, a, v},
	}
	report := DiscardIntake
	if m.handler != nil {
		m.emitIndexSelection(pattern, bound)
		report = &scanReport{
			handler:  m.handler,
			opened:   time.Now(),
			strategy: annotations.ScanDirect,
			cause:    map[string]interface{}{annotations.KeyPattern: pattern},
			run:      &bound,
		}
	}

	// The member's resolved datom under add-wins, nil when the value is not
	// in the set.
	resolved, err := m.resolvedMemberDatom(bound, report)
	if err != nil {
		return nil, fmt.Errorf("membership check failed: %w", err)
	}
	// The run selected E, A and V, so the datom is admitted when the bound T
	// holds on it.
	matched := resolved != nil && m.matchesDatom(resolved, nil, nil, v, tx, nil)
	if report != nil {
		// A membership check resolves to the one member it asked about or to
		// nothing, and the member matches when the bound T holds on its
		// assertion.
		if resolved != nil {
			report.resolved = 1
		}
		if matched {
			report.matched = 1
		}
		// Closed inline, past the error return above, so the scan's outcome is
		// settled here. The deferred closes elsewhere read a named error return
		// instead, because they run at an exit this one is nowhere near.
		report.close(true)
	}

	if !matched {
		return executor.NewMaterializedRelation(symbols, nil), nil
	}

	tuple := m.getTupleBuilder(pattern, symbols).BuildTupleInterned(resolved)
	return executor.NewMaterializedRelation(symbols, []executor.Tuple{tuple}), nil
}

// cardinalityManyScanAllEntitiesIterator streams results for [?e :attr ?v]
// patterns where E is unbound and cardinality is many. It iterates through
// entities at the entity level: for each entity it resolves the add-wins set
// and yields each member the bound T admits as the member's resolved datom,
// its newest assertion.
type cardinalityManyScanAllEntitiesIterator struct {
	matcher      *PatternMatcher
	pattern      *query.DataPattern
	symbols      []query.Symbol
	a            datalog.Keyword
	def          *schema.AttributeDefinition
	tx           interface{} // the bound T, nil when the pattern binds none
	tupleBuilder *query.InternedTupleBuilder
	storageIter  Iterator
	seenEntities map[datalog.Identity]bool

	// report accounts for this arm. The per-entity add-wins resolutions acquire
	// through it as well: the outer AEVT scan only finds the entities and every
	// set beneath it is a scan of its own, so the outer intake alone would
	// report a fraction of the reads. The resolver accrues into it directly, so
	// a resolution that fails keeps what it read instead of taking the count
	// down with the result it never built.
	report *scanReport

	// Current entity state: each member of the entity's set as its resolved
	// datom, the member's newest assertion
	// (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T11).
	currentMembers   []datalog.Datom
	currentMemberIdx int
	currentTuple     executor.Tuple
	err              error // First error from storage operations
}

func (it *cardinalityManyScanAllEntitiesIterator) Next() bool {
	for {
		// Yield the current entity's next member the bound T admits.
		for it.currentMemberIdx < len(it.currentMembers) {
			member := &it.currentMembers[it.currentMemberIdx]
			it.currentMemberIdx++
			if !it.matcher.matchesDatom(member, nil, nil, nil, it.tx, nil) {
				continue
			}
			it.currentTuple = it.tupleBuilder.BuildTupleInterned(member)
			if it.report != nil {
				it.report.matched++
			}
			return true
		}

		// Need to find next entity with non-empty set
		for it.storageIter.Next() {
			datom, err := it.storageIter.Datom()
			if err != nil {
				it.err = err
				return false
			}

			// Skip if we've already processed this entity
			if it.seenEntities[datom.E] {
				continue
			}
			it.seenEntities[datom.E] = true

			// The entity's current set under add-wins, each member beside its
			// newest assertion's Tx: through the cache when the matcher has one
			// and its view is served by it (cacheKey), as the route for a bound
			// entity reads it, and from storage otherwise. A hit reads no
			// index; a build and a storage resolution accrue their intake into
			// the report at scan time, so it survives the error return below.
			var members map[string]any
			var memberTx map[string]datalog.ElementID
			key, cached := it.matcher.cacheKey(datom.E, it.a)
			if cached && it.matcher.cache != nil {
				entry, err := it.matcher.cache.GetOrResolve(key, it.def, it.matcher, it.matcher.cacheBound(), it.matcher.handler, it.report)
				if err != nil {
					it.err = err
					return false
				}
				if entry != nil {
					members = entry.ManySet()
					memberTx = entry.ManyTx()
				}
			} else {
				result, err := it.matcher.resolveAddWinsSet(datom.E, it.a, it.report)
				if err != nil {
					it.err = err
					return false
				}
				members = result.Members
				memberTx = result.MemberTx
			}
			if it.report != nil {
				it.report.resolved += len(members)
			}

			// If set has members, set up iteration over their resolved datoms
			if len(members) > 0 {
				it.currentMembers = make([]datalog.Datom, 0, len(members))
				for valueKey, member := range members {
					it.currentMembers = append(it.currentMembers, datalog.Datom{E: datom.E, A: it.a, V: member, Tx: memberTx[valueKey]})
				}
				it.currentMemberIdx = 0
				break // Go back to top of outer loop to yield first member
			}
		}

		// If no more entities or we just found one with members
		if it.currentMemberIdx < len(it.currentMembers) {
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

// RequiresCopy is false: Next makes a fresh tuple per set member.
func (it *cardinalityManyScanAllEntitiesIterator) RequiresCopy() bool { return false }

func (it *cardinalityManyScanAllEntitiesIterator) Close() error {
	if it.storageIter == nil {
		return nil
	}
	err := it.storageIter.Close()
	if it.report != nil {
		it.report.close(it.err == nil)
	}
	return err
}

func (it *cardinalityManyScanAllEntitiesIterator) Error() error { return it.err }

// matchVectorScanAllEntities answers a pattern on a declared vector attribute
// by resolving each entity's collection from its element datoms (RGA): the
// entity the pattern names when its entity element is a constant, every
// entity carrying the attribute when it is a variable or a blank. With v
// bound, an entity answers when its resolved collection equals v; with v nil,
// every entity whose vector a write reached answers, an emptied one binding
// the empty vector.
func (m *PatternMatcher) matchVectorScanAllEntities(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	a datalog.Keyword,
	v interface{},
	valueType datalog.Keyword,
) (executor.Relation, error) {
	// AEVT orders the attribute's datoms by entity, so the pattern's entity,
	// when it names one, extends the prefix to that entity's own.
	prefix := []datalog.Value{a}
	if entity, ok := pattern.GetE().(query.Constant); ok {
		prefix = append(prefix, entity.Value)
	}
	bound := ScanBound{Index: AEVT, Prefix: prefix}
	report := DiscardIntake
	if m.handler != nil {
		m.emitIndexSelection(pattern, bound)
		report = &scanReport{
			handler:  m.handler,
			opened:   time.Now(),
			strategy: annotations.ScanDirect,
			cause:    map[string]interface{}{annotations.KeyPattern: pattern},
			run:      &bound,
		}
	}
	storageIter, err := OpenScan(m.reader, report, bound)
	if err != nil {
		return nil, fmt.Errorf("AEVT scan failed: %w", err)
	}

	iter := &vectorScanAllEntitiesIterator{
		matcher:      m,
		pattern:      pattern,
		symbols:      symbols,
		a:            a,
		v:            v,
		valueType:    valueType,
		storageIter:  storageIter,
		seenEntities: make(map[datalog.Identity]bool),
		report:       report,
	}

	return executor.NewStreamingRelationWithProperties(symbols, iter, m.options,
		executor.RelationProperties{Identities: executor.AnswerIdentities(pattern)}), nil
}

// vectorScanAllEntitiesIterator streams results for vector patterns with E unbound.
// For each unique entity, resolves the RGA vector and yields a tuple if it matches.
type vectorScanAllEntitiesIterator struct {
	matcher      *PatternMatcher
	pattern      *query.DataPattern
	symbols      []query.Symbol
	a            datalog.Keyword
	v            interface{}
	valueType    datalog.Keyword
	storageIter  Iterator
	seenEntities map[datalog.Identity]bool

	// report accounts for this arm. The per-entity RGA reconstructions acquire
	// through it as well: an RGA group is every insert and tombstone ever
	// written for its (E, A), so those reads dominate the outer scan that only
	// finds the entities. The resolver accrues into it directly, so a
	// reconstruction that fails keeps what it read instead of taking the count
	// down with the result it never built.
	report *scanReport

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

		if it.seenEntities[datom.E] {
			continue
		}
		it.seenEntities[datom.E] = true

		// Intake accrues into the report at scan time, so it survives the error
		// return below; only what the result carries has to wait for one.
		result, err := it.matcher.resolveVector(datom.E, it.a, it.report)
		if err != nil {
			it.err = err
			return false
		}
		if it.report != nil {
			it.report.resolved += len(result.Elements)
		}

		// Never set — skip
		if !result.Present {
			continue
		}

		resolved := typedVector(result.Elements, it.valueType)

		// V is bound — compare
		if it.v != nil && !datalog.ValuesEqual(resolved, it.v) {
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
			case it.pattern.GetT().IsVariable() &&
				it.pattern.GetT().(query.Variable).Name == sym:
				tuple[i] = result.MaxElementID
			}
		}
		it.currentTuple = tuple
		if it.report != nil {
			it.report.matched++
		}
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

// RequiresCopy is false: Next makes a fresh tuple per matching entity.
func (it *vectorScanAllEntitiesIterator) RequiresCopy() bool { return false }

func (it *vectorScanAllEntitiesIterator) Close() error {
	if it.storageIter == nil {
		return nil
	}
	err := it.storageIter.Close()
	if it.report != nil {
		it.report.close(it.err == nil)
	}
	return err
}

func (it *vectorScanAllEntitiesIterator) Error() error { return it.err }

// matchVectorUnderOpenAttribute answers a pattern whose value position holds
// a vector and whose attribute position is open. A vector names a declared
// vector attribute's whole collection, so the answer is each declared vector
// attribute under which the pattern's entity, or every entity when the
// pattern names none, carries a collection equal to it, each attribute
// resolved and compared as matchVectorScanAllEntities compares it under a
// known one. The schema names the vector attributes, and a matcher with no
// schema declares none and answers nothing. The caller answers History
// itself, where nothing resolves and a vector equals no datom as written.
func (m *PatternMatcher) matchVectorUnderOpenAttribute(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	v interface{},
) (executor.Relation, error) {
	var tuples []executor.Tuple
	if m.schema != nil {
		for _, def := range m.schema.Attributes() {
			if def.Cardinality != schema.CardinalityVector {
				continue
			}
			rel, err := m.matchVectorScanAllEntities(pattern, symbols, def.Ident, v, def.ValueType)
			if err != nil {
				return nil, err
			}
			copies := rel.RequiresCopy()
			if err := executor.ForEach(rel, func(tuple executor.Tuple) error {
				if copies {
					held := make(executor.Tuple, len(tuple))
					copy(held, tuple)
					tuple = held
				}
				tuples = append(tuples, tuple)
				return nil
			}); err != nil {
				return nil, err
			}
		}
	}
	return executor.NewMaterializedRelationWithProperties(symbols, tuples, m.options,
		executor.RelationProperties{Identities: executor.AnswerIdentities(pattern)}), nil
}

// matchCardinalityManyScanAllEntities handles [?e :attr ?v] where E is unbound
// Scans all entities with the attribute and resolves each set using add-wins;
// each member is admitted under the bound T, when the pattern binds one.
func (m *PatternMatcher) matchCardinalityManyScanAllEntities(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	a datalog.Keyword,
	def *schema.AttributeDefinition,
	tx interface{},
) (executor.Relation, error) {
	// Scan AEVT to find all entities with this attribute
	bound := ScanBound{Index: AEVT, Prefix: []datalog.Value{a}}
	report := DiscardIntake
	if m.handler != nil {
		m.emitIndexSelection(pattern, bound)
		report = &scanReport{
			handler:  m.handler,
			opened:   time.Now(),
			strategy: annotations.ScanDirect,
			cause:    map[string]interface{}{annotations.KeyPattern: pattern},
			run:      &bound,
		}
	}
	storageIter, err := OpenScan(m.reader, report, bound)
	if err != nil {
		return nil, fmt.Errorf("AEVT scan failed: %w", err)
	}

	// Create streaming iterator
	iter := &cardinalityManyScanAllEntitiesIterator{
		matcher:      m,
		pattern:      pattern,
		symbols:      symbols,
		a:            a,
		def:          def,
		tx:           tx,
		tupleBuilder: m.getTupleBuilder(pattern, symbols),
		storageIter:  storageIter,
		seenEntities: make(map[datalog.Identity]bool),
		report:       report,
	}

	return executor.NewStreamingRelationWithProperties(symbols, iter, m.options,
		executor.RelationProperties{Identities: executor.AnswerIdentities(pattern)}), nil
}

// cardinalityManyAVETValueIterator streams results for [?e :attr "value"] patterns
// using the AVET index for O(k) lookup. It scans entries with [A][V] prefix and applies
// add-wins resolution per entity to determine if the value is currently in each entity's
// set, and answers each member entity as the member's resolved datom, the value's
// newest assertion, when the bound T holds on it
// (docs/proposals/BOUND_TRANSACTION_POSITION_PROOF.md, T11).
//
// AVET key order: [A][V][E][Op][Tx↓]
// - Same A and V (from prefix)
// - Entities grouped together
// - Within entity: Op=0 (Add) before Op=1 (Remove), then by Tx descending
type cardinalityManyAVETValueIterator struct {
	matcher      *PatternMatcher
	pattern      *query.DataPattern
	symbols      []query.Symbol
	tupleBuilder *query.InternedTupleBuilder
	a            datalog.Keyword
	v            interface{}
	tx           interface{} // the bound T, nil when the pattern binds none
	storageIter  Iterator

	// report accounts for this arm, and carries the run whole rather than as
	// its index alone. Here the difference is the whole reading: this scan
	// binds V, and it is the bound V that makes the run inexact, so an event
	// naming AVET without saying what it bound reports the amplification
	// below without its cause.
	//
	// Add-wins is resolved inline rather than by a nested scan, so intake is
	// the AVET scan's alone — and with a string or bytes V that run is inexact,
	// which is exactly when intake exceeds what the loop sees.
	report *scanReport

	// Current entity being processed
	currentEntity    datalog.Identity
	hasCurrentEntity bool

	// Add-wins state for the current entity: the value's newest assertion and
	// newest withdrawal. Membership compares their Lamports, and the newest
	// assertion is the member's resolved datom.
	highestAddTx    datalog.ElementID
	highestRemoveTx datalog.ElementID
	hasAdd          bool
	hasRemove       bool

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
			// End of scan: the last entity is answered when it is a member the
			// bound T admits. Also surface any deferred error from the inner
			// iterator so Error() reflects deep failures.
			if srcErr := it.storageIter.Error(); srcErr != nil && it.err == nil {
				it.err = srcErr
			}
			it.done = true
			return it.hasCurrentEntity && it.emitCurrentEntity()
		}

		datom, err := it.storageIter.Datom()
		if err != nil {
			it.err = err
			return false
		}

		// A datom after the view's as-of target is not read.
		if it.matcher.shouldFilterTx(datom.Tx) {
			continue
		}

		// Check if this is a new entity
		if !it.hasCurrentEntity {
			// First entity
			it.startNewEntity(datom.E, datom.Op, datom.Tx)
			continue
		}

		if datom.E != it.currentEntity {
			// Moving to a new entity: the previous one is answered when it is
			// a member the bound T admits, and the new one is tracked from
			// this datom.
			emitted := it.emitCurrentEntity()
			it.startNewEntity(datom.E, datom.Op, datom.Tx)
			if emitted {
				return true
			}
			continue
		}

		// Same entity - update add-wins state
		it.updateAddWinsState(datom.Op, datom.Tx)
	}
}

func (it *cardinalityManyAVETValueIterator) startNewEntity(e datalog.Identity, op datalog.CRDTOp, tx datalog.ElementID) {
	it.currentEntity = e
	it.hasCurrentEntity = true
	it.highestAddTx = datalog.ElementID{}
	it.highestRemoveTx = datalog.ElementID{}
	it.hasAdd = false
	it.hasRemove = false
	it.updateAddWinsState(op, tx)
}

func (it *cardinalityManyAVETValueIterator) updateAddWinsState(op datalog.CRDTOp, tx datalog.ElementID) {
	if op == datalog.OpCRDTAdd {
		if !it.hasAdd || tx.Compare(it.highestAddTx) > 0 {
			it.highestAddTx = tx
			it.hasAdd = true
		}
	} else if op == datalog.OpCRDTRemove {
		if !it.hasRemove || tx.Compare(it.highestRemoveTx) > 0 {
			it.highestRemoveTx = tx
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
	return it.highestAddTx.Lamport >= it.highestRemoveTx.Lamport
}

// emitCurrentEntity answers the current entity when the value is in its set
// and the bound T holds on the value's newest assertion, the member's resolved
// datom: the tuple is built from that datom. The bound V is the pattern's, so
// an entity holding the value resolves to a member by construction, and the
// bound T decides the match.
func (it *cardinalityManyAVETValueIterator) emitCurrentEntity() bool {
	if !it.isCurrentEntityMember() {
		return false
	}
	if it.report != nil {
		it.report.resolved++
	}
	member := datalog.Datom{E: it.currentEntity, A: it.a, V: it.v, Tx: it.highestAddTx}
	if !it.matcher.matchesDatom(&member, nil, nil, nil, it.tx, nil) {
		return false
	}
	it.currentTuple = it.tupleBuilder.BuildTupleInterned(&member)
	if it.report != nil {
		it.report.matched++
	}
	return true
}

func (it *cardinalityManyAVETValueIterator) Tuple() executor.Tuple {
	return it.currentTuple
}

// RequiresCopy is false: emitCurrentEntity builds a fresh tuple per member entity.
func (it *cardinalityManyAVETValueIterator) RequiresCopy() bool { return false }

func (it *cardinalityManyAVETValueIterator) Close() error {
	if it.storageIter == nil {
		return nil
	}
	err := it.storageIter.Close()
	if it.report != nil {
		it.report.close(it.err == nil)
	}
	return err
}

func (it *cardinalityManyAVETValueIterator) Error() error { return it.err }

// emitIndexSelection announces the run a pattern's scan is about to walk.
//
// The caller guards on m.handler; that guard belongs to the caller because it
// gates the caller's own argument preparation as well as the map and
// describeRun's two slices in here — and because at the call site it marks the
// block as observability rather than a step in opening the scan.
//
// Every path that opens a scan for a pattern emits this: the general arm, and
// each cardinality arm that returns before reaching it. Deliberately no count —
// a number in prose is a claim about
// the tree that nothing rechecks.
//
// A bound only narrows observably if the scan says what it narrowed to: where
// the run is inexact the store steps over the keys the byte range over-covers,
// and such a key is otherwise indistinguishable in the stream from one that was
// never in range.
//
// The cache arms do not emit it, and deliberately: they address no run. The
// cache chooses an index by cardinality inside resolution, and a hit reads no
// index at all, so a bound announced here would name a run the call did not
// choose and, on a hit, one that nothing walked. They report their cost on
// storage/resolve-complete instead, where a hit's zero intake is a real answer
// rather than a missing one.
//
// Callers pass the same ScanBound they hand the reader, so the announced run
// and the walked run cannot drift.
func (m *PatternMatcher) emitIndexSelection(pattern *query.DataPattern, bound ScanBound) {
	data := map[string]interface{}{annotations.KeyPattern: pattern}
	addBoundFields(data, bound)
	m.handler(annotations.Event{
		Name:  annotations.PatternIndexSelection,
		Start: time.Now(),
		Data:  data,
	})
}

// matchCardinalityManyFindEntitiesWithValue handles [?e :attr "value"] where E
// is unbound and the attribute is cardinality-many: every entity whose set
// holds the value, each as the value's newest assertion, admitted under the
// bound T when the pattern binds one.
func (m *PatternMatcher) matchCardinalityManyFindEntitiesWithValue(
	pattern *query.DataPattern,
	symbols []query.Symbol,
	a datalog.Keyword,
	v interface{},
	tx interface{},
) (executor.Relation, error) {
	// Bind [A][V] on AVET, which seeks straight to entries carrying this
	// value — O(k) in those entries, not O(n) in the attribute's entities.
	// A string or bytes V makes this run inexact, so the store steps over the
	// keys the range over-covers; the event is what makes that stepping visible.
	bound := ScanBound{Index: AVET, Prefix: []datalog.Value{a, v}}
	report := DiscardIntake
	if m.handler != nil {
		m.emitIndexSelection(pattern, bound)
		report = &scanReport{
			handler:  m.handler,
			opened:   time.Now(),
			strategy: annotations.ScanDirect,
			cause:    map[string]interface{}{annotations.KeyPattern: pattern},
			run:      &bound,
		}
	}
	storageIter, err := OpenScan(m.reader, report, bound)
	if err != nil {
		return nil, fmt.Errorf("AVET scan failed: %w", err)
	}

	// Create streaming iterator that applies add-wins resolution per entity
	iter := &cardinalityManyAVETValueIterator{
		matcher:      m,
		pattern:      pattern,
		symbols:      symbols,
		tupleBuilder: m.getTupleBuilder(pattern, symbols),
		a:            a,
		v:            v,
		tx:           tx,
		storageIter:  storageIter,
		report:       report,
	}

	return executor.NewStreamingRelationWithProperties(symbols, iter, m.options,
		executor.RelationProperties{Identities: executor.AnswerIdentities(pattern)}), nil
}
