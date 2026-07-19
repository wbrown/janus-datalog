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
	minExtraSymbols := int(^uint(0) >> 1) // Max int

	for _, rel := range rs {
		if containsAll(rel.Symbols(), symbols) {
			extraSymbols := len(rel.Symbols()) - len(symbols)
			if extraSymbols < minExtraSymbols {
				minExtraSymbols = extraSymbols
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
		syms := rel.Symbols()
		symSet := make(map[query.Symbol]bool)
		for _, sym := range syms {
			symSet[sym] = true
		}

		// Score based on which positions are bound
		// E position is most selective (uses EAVT index efficiently)
		if v, ok := pattern.GetE().(query.Variable); ok && symSet[v.Name] {
			score += 1000
		}
		// A position is moderately selective (uses AEVT index)
		if v, ok := pattern.GetA().(query.Variable); ok && symSet[v.Name] {
			score += 100
		}
		// V position is least selective (uses VAET index)
		if v, ok := pattern.GetV().(query.Variable); ok && symSet[v.Name] {
			score += 10
		}
		// T position is rarely used
		if len(pattern.Elements) > 3 {
			if v, ok := pattern.GetT().(query.Variable); ok && symSet[v.Name] {
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
		for _, sym := range rel.Symbols() {
			if symbolSet[sym] {
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

// Collapse joins relations that share symbols and returns all relation groups.
// Relations that can be joined are combined into single relations.
// Relations that share no symbols remain separate.
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
				// Check if this relation shares symbols with current group
				if hasSharedSymbols(currentGroup, remaining[i]) {
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

// hasSharedSymbols checks if two relations share any symbols
func hasSharedSymbols(r1, r2 Relation) bool {
	syms1 := r1.Symbols()
	syms2 := r2.Symbols()

	for _, c1 := range syms1 {
		if query.ContainsSymbol(syms2, c1) {
			return true
		}
	}

	return false
}

// containsAll checks if syms contains all symbols
func containsAll(syms []query.Symbol, symbols []query.Symbol) bool {
	symSet := make(map[query.Symbol]bool)
	for _, sym := range syms {
		symSet[sym] = true
	}

	for _, sym := range symbols {
		if !symSet[sym] {
			return false
		}
	}
	return true
}

// containsAny checks if syms contains any of the symbols
func containsAny(syms []query.Symbol, symbols []query.Symbol) bool {
	symSet := make(map[query.Symbol]bool)
	for _, sym := range syms {
		symSet[sym] = true
	}

	for _, sym := range symbols {
		if symSet[sym] {
			return true
		}
	}
	return false
}
