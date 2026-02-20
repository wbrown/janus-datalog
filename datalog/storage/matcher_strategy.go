package storage

import (
	"github.com/wbrown/janus-datalog/datalog/executor"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// ReuseType indicates the type of iterator reuse strategy
type ReuseType int

const (
	NoReuse ReuseType = iota
	SinglePositionReuse
	MultiPositionReuse
)

func (rt ReuseType) String() string {
	switch rt {
	case NoReuse:
		return "no-reuse"
	case SinglePositionReuse:
		return "single-position-reuse"
	case MultiPositionReuse:
		return "multi-position-reuse"
	default:
		return "unknown"
	}
}

// ReuseStrategy describes how iterator reuse can be applied
type ReuseStrategy struct {
	Type            ReuseType
	Position        int       // Which position is changing (0=E, 1=A, 2=V, 3=T)
	Index           IndexType // Which index to use
	NeedsValidation bool      // If true, V-bound query needs CRDT validation
	ValidationIndex IndexType // Index to use for validation (EATV for point lookups)
	BoundA          any       // The bound A value (for validation lookups)
	BoundV          any       // The bound V value (for validation comparison)
}

// analyzeReuseStrategy determines if and how iterator reuse can be applied.
// Returns the strategy and the (possibly materialized) binding relation.
// For multi-position cases, the relation may be materialized to allow cardinality counting.
func analyzeReuseStrategy(pattern *query.DataPattern, bindingRel executor.Relation) (ReuseStrategy, executor.Relation) {
	// TESTING: Re-enable to verify performance with sorted keys
	// Previous benchmarks showed 2x slower, but that may have been due to:
	// 1. Not sorting the binding relation properly
	// 2. Opening too wide an iterator range
	// 3. Complex boundary checking overhead
	// return ReuseStrategy{Type: NoReuse} // ENABLED for testing to force buggy code path

	// Original logic preserved below but bypassed:
	if bindingRel == nil {
		return ReuseStrategy{Type: NoReuse}, bindingRel
	}

	// CRITICAL FIX: Don't call IsEmpty() on StreamingRelations
	// IsEmpty() peeks at the first tuple by calling Next(), which CONSUMES it!
	// When Size() is later called, materialization only captures remaining tuples.
	// Result: Entity joins lose first tuple (returns 4/5 results instead of 5/5)
	// If the relation is empty, subsequent iteration will discover that naturally.
	// Skip the IsEmpty() check - it's a minor optimization that causes data loss.
	// See: docs/bugs/BUG_ENTITY_JOIN_LOSES_FIRST_TUPLE.md

	// Check if empty - but only for MaterializedRelations where it's safe
	if _, isStreaming := bindingRel.(*executor.StreamingRelation); !isStreaming {
		if bindingRel.IsEmpty() {
			return ReuseStrategy{Type: NoReuse}, bindingRel
		}
	}

	// For streaming relations with unknown size, we can't optimize for size==1
	// but that's okay - the overhead of reuse with one binding is negligible

	// Count which positions have variables that are bound
	bindingCols := bindingRel.Columns()
	bindingSet := make(map[query.Symbol]bool)
	for _, col := range bindingCols {
		bindingSet[col] = true
	}

	// Track which positions have bound variables
	var boundPositions []int

	if v, ok := pattern.GetE().(query.Variable); ok && bindingSet[v.Name] {
		boundPositions = append(boundPositions, 0)
	}
	if v, ok := pattern.GetA().(query.Variable); ok && bindingSet[v.Name] {
		boundPositions = append(boundPositions, 1)
	}
	if v, ok := pattern.GetV().(query.Variable); ok && bindingSet[v.Name] {
		boundPositions = append(boundPositions, 2)
	}
	if len(pattern.Elements) > 3 {
		if v, ok := pattern.GetT().(query.Variable); ok && bindingSet[v.Name] {
			boundPositions = append(boundPositions, 3)
		}
	}

	// Single position bound - check if we can use iterator reuse
	if len(boundPositions) == 1 {
		position := boundPositions[0]

		// Determine which index to use and if reuse is beneficial
		var indexType IndexType
		var canReuse bool

		switch position {
		case 0: // E is bound
			// Check if A is also constant (not in binding set)
			if _, isConstant := pattern.GetA().(query.Constant); isConstant {
				// E is bound, A is constant → use AETV for CRDT-aware lookups
				// Pattern like [?e :attr/name ?v] where ?e is bound to many values
				// AETV has Tx descending: first entry for each (A, E) is LWW winner
				// AETV allows: seek to (:attr/name, entity1), seek to (:attr/name, entity2), etc.
				indexType = AETV // direct lookup by (A, E) pair with Tx descending
				canReuse = true  // Try iterator reuse with Seek() between entities
			} else {
				// E is bound, A varies → use EATV (E-primary with Tx descending)
				indexType = EATV // E is primary sort key, Tx descending for CRDT
				canReuse = true
			}

		case 1: // A is bound
			indexType = AETV // A is primary sort key, Tx descending for CRDT
			canReuse = true

		case 2: // V is bound
			// V-bound queries use candidate + validate pattern for cardinality-one.
			// See docs/reference/INDEX_SELECTION_PROOF.md Theorem 4.
			//
			// Strategy: Use V-primary index for candidate discovery, then validate
			// each candidate with EATV point lookup.
			//
			// For cardinality-many, validation is not needed (add-wins semantics).
			// Caller must check cardinality and clear NeedsValidation if appropriate.
			if aConst, ok := pattern.GetA().(query.Constant); ok {
				// A is constant: use AVET for (A, V) prefix scan
				indexType = AVET
				canReuse = true
				// Return strategy with validation setup
				// Caller will check cardinality and may clear NeedsValidation
				return ReuseStrategy{
					Type:            SinglePositionReuse,
					Position:        position,
					Index:           indexType,
					NeedsValidation: true,         // Assume cardinality-one; caller clears for many
					ValidationIndex: EATV,         // Point lookup on (E, A)
					BoundA:          aConst.Value, // For validation lookups
				}, bindingRel
			} else {
				// A is variable: use VAET for V-primary scan
				// Validation will need to determine A from each candidate
				indexType = VAET
				canReuse = true
				return ReuseStrategy{
					Type:            SinglePositionReuse,
					Position:        position,
					Index:           indexType,
					NeedsValidation: true, // Must validate since A varies
					ValidationIndex: EATV,
				}, bindingRel
			}

		case 3: // T is bound
			indexType = TAEV // T is primary sort key ✓
			canReuse = true
		}

		if canReuse {
			return ReuseStrategy{
				Type:     SinglePositionReuse,
				Position: position,
				Index:    indexType,
			}, bindingRel
		}
		// Fall back to NoReuse for patterns where reuse doesn't help
		return ReuseStrategy{Type: NoReuse}, bindingRel
	}

	// Multiple positions bound - choose the most selective position for reuse
	if len(boundPositions) > 1 {
		return chooseBestMultiPositionStrategy(pattern, boundPositions, bindingRel, bindingSet)
	}

	return ReuseStrategy{Type: NoReuse}, bindingRel
}

// chooseBestMultiPositionStrategy handles the case where multiple positions are bound
// from the binding relation. It chooses the most selective position for iterator reuse.
//
// Key insight: When both E and V are bound, choose the position with FEWER distinct
// values as the grouping dimension. The other position will use hash-based filtering.
//
// Example: Pattern [?e :entity/code ?code] with 81 entities and 1 code value:
// - V has 1 distinct value (more selective)
// - Use AVET index with A+V prefix to scan, filter by E hash set
// Returns the strategy and the (possibly materialized) binding relation.
func chooseBestMultiPositionStrategy(
	pattern *query.DataPattern,
	boundPositions []int,
	bindingRel executor.Relation,
	bindingSet map[query.Symbol]bool,
) (ReuseStrategy, executor.Relation) {
	// CRITICAL: Materialize streaming relations before iterating
	// This function iterates the relation to count cardinalities, and later code
	// (matchWithIteratorReuse, matchWithHashJoin) needs to iterate again.
	// StreamingRelation panics if Iterator() is called twice without Materialize().
	// See: TestMultiPositionWithStreamingBinding
	if streamRel, isStreaming := bindingRel.(*executor.StreamingRelation); isStreaming {
		bindingRel = streamRel.Materialize()
	}

	// Track column index and distinct count per bound position.
	// Uses a slice parallel to boundPositions for deterministic iteration
	// (maps cause nondeterministic tiebreaking via random iteration order).
	type posInfo struct {
		pos      int // datom position (0=E, 1=A, 2=V, 3=T)
		colIdx   int // column index in binding relation (-1 if not found)
		distinct int // count of distinct values
	}
	info := make([]posInfo, 0, len(boundPositions))

	// Build position-to-column-index mapping
	for _, pos := range boundPositions {
		var varName query.Symbol
		switch pos {
		case 0: // E
			if v, ok := pattern.GetE().(query.Variable); ok {
				varName = v.Name
			}
		case 1: // A
			if v, ok := pattern.GetA().(query.Variable); ok {
				varName = v.Name
			}
		case 2: // V
			if v, ok := pattern.GetV().(query.Variable); ok {
				varName = v.Name
			}
		case 3: // T
			if len(pattern.Elements) > 3 {
				if v, ok := pattern.GetT().(query.Variable); ok {
					varName = v.Name
				}
			}
		}

		if varName == nil {
			continue
		}

		colIdx := -1
		for ci, col := range bindingRel.Columns() {
			if col == varName {
				colIdx = ci
				break
			}
		}
		info = append(info, posInfo{pos: pos, colIdx: colIdx})
	}

	// Count distinct values for each position
	sets := make([]map[interface{}]bool, len(info))
	for i := range sets {
		sets[i] = make(map[interface{}]bool)
	}

	it := bindingRel.Iterator()
	for it.Next() {
		tuple := it.Tuple()
		for i, pi := range info {
			if pi.colIdx >= 0 && pi.colIdx < len(tuple) {
				sets[i][tuple[pi.colIdx]] = true
			}
		}
	}
	it.Close()

	for i := range info {
		info[i].distinct = len(sets[i])
	}

	// Find the position with the MOST distinct values.
	// On ties, prefer A (position 1) over E (position 0) because A-primary
	// indices (AETV) produce scans with uniform cardinality per scan — each
	// attribute has exactly one CRDT resolution strategy. E-primary indices
	// (EATV) mix cardinalities within a scan (one entity may have One, Many,
	// and Vector attributes), forcing the CRDTResolvingIterator through
	// cross-cardinality group transitions.
	bestIdx := -1
	maxCardinality := 0
	for i, pi := range info {
		if pi.distinct > maxCardinality || (pi.distinct == maxCardinality && bestIdx >= 0 && pi.pos > info[bestIdx].pos) {
			maxCardinality = pi.distinct
			bestIdx = i
		}
	}

	bestPosition := -1
	if bestIdx >= 0 {
		bestPosition = info[bestIdx].pos
	}

	if bestPosition == -1 {
		return ReuseStrategy{Type: NoReuse}, bindingRel
	}

	// Determine index based on best position for reuse
	var indexType IndexType
	switch bestPosition {
	case 0: // E has most distinct values - use AETV or EATV
		// Check if A is constant
		if _, isConstant := pattern.GetA().(query.Constant); isConstant {
			indexType = AETV // A is constant, seek by E (Tx descending for CRDT)
		} else {
			indexType = EATV // seek by E (Tx descending for CRDT)
		}
	case 1: // A has most distinct values - use AETV (Tx descending for CRDT)
		indexType = AETV
	case 2: // V has most distinct values - use AVET or VAET
		if _, isConstant := pattern.GetA().(query.Constant); isConstant {
			indexType = AVET // A is constant, seek by V
		} else {
			indexType = VAET // seek by V
		}
	case 3: // T has most distinct values - use TAEV
		indexType = TAEV
	default:
		return ReuseStrategy{Type: NoReuse}, bindingRel
	}

	return ReuseStrategy{
		Type:     SinglePositionReuse, // Reuse single position reuse logic
		Position: bestPosition,
		Index:    indexType,
	}, bindingRel
}
