package executor

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Relations represents a collection of relations that can be analyzed together
// to make optimal query execution decisions
type Relations []Relation

// Project finds the best relation that contains all requested symbols.
// Returns nil if no relation contains all symbols.
func (rs Relations) Project(symbols ...query.Symbol) Relation {
	if len(rs) == 0 || len(symbols) == 0 {
		return nil
	}

	var bestRel Relation
	minExtraColumns := int(^uint(0) >> 1) // Max int

	for _, rel := range rs {
		if containsAll(rel.Columns(), symbols) {
			extraColumns := len(rel.Columns()) - len(symbols)
			if extraColumns < minExtraColumns {
				minExtraColumns = extraColumns
				bestRel = rel
			}
		}
	}
	return bestRel
}

// FindBestForPattern finds the relation that best constrains the given pattern.
// It prefers relations that bind more selective positions (E > A > V) and
// smaller relations when there's a tie.
func (rs Relations) FindBestForPattern(pattern *query.DataPattern) Relation {
	if len(rs) == 0 {
		return nil
	}

	type candidate struct {
		rel   Relation
		score int
	}

	var bestCandidate *candidate

	for _, rel := range rs {
		score := 0
		cols := rel.Columns()
		colSet := make(map[query.Symbol]bool)
		for _, col := range cols {
			colSet[col] = true
		}

		// Score based on which positions are bound
		// E position is most selective (uses EAVT index efficiently)
		if v, ok := pattern.GetE().(query.Variable); ok && colSet[v.Name] {
			score += 1000
		}
		// A position is moderately selective (uses AEVT index)
		if v, ok := pattern.GetA().(query.Variable); ok && colSet[v.Name] {
			score += 100
		}
		// V position is least selective (uses VAET index)
		if v, ok := pattern.GetV().(query.Variable); ok && colSet[v.Name] {
			score += 10
		}
		// T position is rarely used
		if len(pattern.Elements) > 3 {
			if v, ok := pattern.GetT().(query.Variable); ok && colSet[v.Name] {
				score += 1
			}
		}

		// Only consider relations that bind at least one pattern variable
		if score > 0 {
			// Prefer smaller relations as tie-breaker
			// Subtract size so smaller relations get higher scores
			score = score - rel.Size()

			if bestCandidate == nil || score > bestCandidate.score {
				bestCandidate = &candidate{rel: rel, score: score}
			}
		}
	}

	if bestCandidate != nil {
		return bestCandidate.rel
	}
	return nil
}

// FindRelationsForSymbols returns all relations that contain at least one of the given symbols
func (rs Relations) FindRelationsForSymbols(symbols ...query.Symbol) Relations {
	if len(symbols) == 0 {
		return nil
	}

	symbolSet := make(map[query.Symbol]bool)
	for _, sym := range symbols {
		symbolSet[sym] = true
	}

	var result Relations
	for _, rel := range rs {
		for _, col := range rel.Columns() {
			if symbolSet[col] {
				result = append(result, rel)
				break
			}
		}
	}
	return result
}

// Product returns a streaming Cartesian product of multiple relations.
// This is used for expressions/predicates that reference symbols from disjoint relations.
// Returns a single relation for 0-1 inputs (passthrough), or a ProductRelation for 2+.
func (rs Relations) Product() Relation {
	if len(rs) == 0 {
		return NewMaterializedRelation(nil, nil)
	}
	if len(rs) == 1 {
		return rs[0] // Pass through - no product needed
	}

	// Multiple relations - create streaming Cartesian product
	return NewProductRelation(rs)
}

// Collapse joins relations that share columns and returns all relation groups.
// Relations that can be joined are combined into single relations.
// Relations that share no columns remain separate.
func (rs Relations) Collapse(ctx Context) Relations {
	if len(rs) == 0 {
		return Relations{}
	}

	if len(rs) == 1 {
		// BUGFIX: Return a new slice, not the same slice
		// This prevents iterator reuse issues
		return Relations{rs[0]}
	}

	// Keep track of independent relation groups
	var groups Relations

	// Process relations one by one
	remaining := Relations(rs)

	for len(remaining) > 0 {
		// Start a new group with the first remaining relation
		currentGroup := remaining[0]
		remaining = remaining[1:]

		// Keep joining relations into this group until no more can join
		changed := true
		for changed {
			changed = false

			for i := 0; i < len(remaining); i++ {
				// Check if this relation shares columns with current group
				if hasSharedColumns(currentGroup, remaining[i]) {
					// Join them
					currentGroup = ctx.JoinRelations(currentGroup, remaining[i], func() Relation {
						return currentGroup.Join(remaining[i])
					})

					// Remove from remaining
					remaining = append(remaining[:i], remaining[i+1:]...)
					changed = true

					// CRITICAL: Don't call IsEmpty() - it consumes streaming iterators!
					// Empty detection happens naturally in subsequent operations

					break // Restart the loop
				}
			}
		}

		// Add the completed group
		groups = append(groups, currentGroup)
	}

	return groups
}

// hasSharedColumns checks if two relations share any columns
func hasSharedColumns(r1, r2 Relation) bool {
	cols1 := r1.Columns()
	cols2 := r2.Columns()

	for _, c1 := range cols1 {
		for _, c2 := range cols2 {
			if c1 == c2 {
				return true
			}
		}
	}

	return false
}

// containsAll checks if cols contains all symbols
func containsAll(cols []query.Symbol, symbols []query.Symbol) bool {
	colSet := make(map[query.Symbol]bool)
	for _, col := range cols {
		colSet[col] = true
	}

	for _, sym := range symbols {
		if !colSet[sym] {
			return false
		}
	}
	return true
}
