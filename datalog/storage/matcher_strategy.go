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
	Type     ReuseType
	Position int // Which position is changing (0=E, 1=A, 2=V, 3=T)
	Index    int // Which index to use (maps to IndexType)
}

// analyzeReuseStrategy determines if and how iterator reuse can be applied
func analyzeReuseStrategy(pattern *query.DataPattern, bindingRel executor.Relation) ReuseStrategy {
	// TESTING: Re-enable to verify performance with sorted keys
	// Previous benchmarks showed 2x slower, but that may have been due to:
	// 1. Not sorting the binding relation properly
	// 2. Opening too wide an iterator range
	// 3. Complex boundary checking overhead
	// return ReuseStrategy{Type: NoReuse} // ENABLED for testing to force buggy code path

	// Original logic preserved below but bypassed:
	if bindingRel == nil {
		return ReuseStrategy{Type: NoReuse}
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
			return ReuseStrategy{Type: NoReuse}
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
		var indexType int
		var canReuse bool

		switch position {
		case 0: // E is bound
			// Check if A is also constant (not in binding set)
			if _, isConstant := pattern.GetA().(query.Constant); isConstant {
				// E is bound, A is constant → use AEVT for direct lookups
				// Pattern like [?e :attr/name ?v] where ?e is bound to many values
				// AEVT allows: seek to (:attr/name, entity1), seek to (:attr/name, entity2), etc.
				indexType = 1   // AEVT - direct lookup by (A, E) pair
				canReuse = true // Try iterator reuse with Seek() between entities
			} else {
				// E is bound, A varies → use EAVT
				indexType = 0 // EAVT - E is primary sort key
				canReuse = true
			}

		case 1: // A is bound
			indexType = 1 // AEVT - A is primary sort key ✓
			canReuse = true

		case 2: // V is bound
			// Check if A is constant OR bound to a single value (not in binding relation)
			aIsFixedValue := false
			if _, ok := pattern.GetA().(query.Constant); ok {
				// A is a constant in the pattern (e.g., :price/symbol)
				aIsFixedValue = true
			} else if v, ok := pattern.GetA().(query.Variable); ok && !bindingSet[v.Name] {
				// A is a variable but NOT in the binding relation,
				// so it must be bound to a scalar/constant elsewhere
				aIsFixedValue = true
			}

			if aIsFixedValue {
				// AVET index: A is primary, V is secondary
				// When A is fixed, all V values for that A are contiguous
				// We CAN efficiently seek between V values within that A range
				indexType = 2   // AVET
				canReuse = true // Re-enabled with fixed "moved past" logic
			} else {
				// A varies with the binding relation
				// Use VAET where V is the primary sort key
				indexType = 3 // VAET
				canReuse = true
			}

		case 3: // T is bound
			indexType = 4 // TAEV - T is primary sort key ✓
			canReuse = true
		}

		if canReuse {
			return ReuseStrategy{
				Type:     SinglePositionReuse,
				Position: position,
				Index:    indexType,
			}
		}
		// Fall back to NoReuse for patterns where reuse doesn't help
		return ReuseStrategy{Type: NoReuse}
	}

	// Multiple positions bound - future optimization
	if len(boundPositions) > 1 {
		// For now, no reuse. Future: implement merge join
		return ReuseStrategy{Type: NoReuse}
	}

	return ReuseStrategy{Type: NoReuse}
}
