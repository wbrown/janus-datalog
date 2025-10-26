package storage

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
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
	strategy := analyzeReuseStrategy(pattern, bindingRel)

	// Emit strategy selection event
	if m.handler != nil {
		m.handler(annotations.Event{
			Name:  "storage/reuse-strategy",
			Start: time.Now(),
			Data: map[string]interface{}{
				"pattern":       pattern.String(),
				"strategy_type": strategy.Type.String(),
				"index":         indexName(IndexType(strategy.Index)),
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
					"index":         indexName(IndexType(strategy.Index)),
				},
			})
		}

		switch joinStrategy {
		case HashJoinScan:
			// Use hash join for medium selectivity (1-50%)
			return m.matchWithHashJoin(pattern, bindingRel, columns, strategy.Position, IndexType(strategy.Index), constraints)

		case MergeJoin:
			// Use merge join for high selectivity (>50%) with large binding sets
			return m.matchWithMergeJoin(pattern, bindingRel, columns, strategy.Position, IndexType(strategy.Index), constraints)

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
			matcher:      m,
			index:        index,
			start:        start,
			end:          end,
			pattern:      pattern,
			columns:      columns,
			e:            e,
			a:            a,
			v:            v,
			tx:           tx,
			keyMask:      keyMask,
			constraints:  constraints, // Still need for non-mask constraints
			tupleBuilder: m.getTupleBuilder(pattern, columns),
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
			matcher:      m,
			index:        index,
			start:        start,
			end:          end,
			pattern:      pattern,
			columns:      columns,
			e:            e,
			a:            a,
			v:            v,
			tx:           tx,
			constraints:  constraints,
			tupleBuilder: m.getTupleBuilder(pattern, columns),
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
	var bindingTuples []executor.Tuple
	it := bindingRel.Iterator()
	for it.Next() {
		bindingTuples = append(bindingTuples, it.Tuple())
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
		index:            IndexType(strategy.Index),
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
		IndexType(strategy.Index),
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
