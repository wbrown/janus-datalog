package executor

import (
	"time"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// reportJoin times fn and reports the join's sizes and symbols through handler.
// The early return is the whole of the annotations-off path: fn is the work
// either way, and nothing below it is prepared unless someone will read it.
func reportJoin(handler annotations.Handler, left, right Relation, fn func() Relation) Relation {
	if handler == nil {
		return fn()
	}

	start := time.Now()
	leftSize := -1 // Use -1 to indicate unknown size
	rightSize := -1

	// Don't call Size() on StreamingRelations before the join: Size() can
	// trigger materialization which may lose tuples if the iterator was
	// partially consumed. Only ask relations where it is safe.
	if left != nil {
		if _, isStreaming := left.(*StreamingRelation); !isStreaming {
			leftSize = left.Size()
		}
	}
	if right != nil {
		if _, isStreaming := right.(*StreamingRelation); !isStreaming {
			rightSize = right.Size()
		}
	}

	result := fn()

	resultSize := 0
	if result != nil {
		resultSize = result.Size()
	}

	data := map[string]interface{}{
		"left.size":   leftSize,
		"right.size":  rightSize,
		"result.size": resultSize,
	}

	if leftSize+rightSize > 0 {
		data["amplification"] = float64(resultSize) / float64(leftSize+rightSize)
	}

	if left != nil && right != nil {
		data["left.symbols"] = left.Symbols()
		data["right.symbols"] = right.Symbols()
	}

	if left != nil {
		leftAttrs := make([]string, len(left.Symbols()))
		for i, sym := range left.Symbols() {
			leftAttrs[i] = sym.String()
		}
		data["left.attrs"] = leftAttrs
	}
	if right != nil {
		rightAttrs := make([]string, len(right.Symbols()))
		for i, sym := range right.Symbols() {
			rightAttrs[i] = sym.String()
		}
		data["right.attrs"] = rightAttrs
	}
	if result != nil {
		resultAttrs := make([]string, len(result.Symbols()))
		for i, sym := range result.Symbols() {
			resultAttrs[i] = sym.String()
		}
		data["result.attrs"] = resultAttrs
	}

	handler(annotations.TimedEvent(annotations.JoinHash, start, data))
	return result
}

// reportCollapse times fn and reports the reduction, when there was one.
func reportCollapse(handler annotations.Handler, rels []Relation, fn func() []Relation) []Relation {
	if handler == nil {
		return fn()
	}

	start := time.Now()

	inputCount := len(rels)
	inputTuples := 0
	for _, rel := range rels {
		if rel != nil {
			inputTuples += rel.Size()
		}
	}

	result := fn()

	outputCount := len(result)
	outputTuples := 0
	for _, rel := range result {
		if rel != nil {
			outputTuples += rel.Size()
		}
	}

	if outputCount < inputCount || outputTuples < inputTuples {
		handler(annotations.TimedEvent(annotations.CollapseSuccess, start, map[string]interface{}{
			"relations.before": inputCount,
			"relations.after":  outputCount,
			"tuples.before":    inputTuples,
			"tuples.after":     outputTuples,
			"reduction.pct":    (1.0 - float64(outputTuples)/float64(inputTuples)) * 100,
		}))
	}

	return result
}

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
//
// handler receives each join it performs; nil reports nothing.
func (rs Relations) Collapse(handler annotations.Handler) Relations {
	if len(rs) == 0 {
		return Relations{}
	}

	if len(rs) == 1 {
		// Return a new slice, not the same slice
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
					currentGroup = reportJoin(handler, currentGroup, remaining[i], func() Relation {
						return currentGroup.Join(remaining[i])
					})

					// Remove from remaining
					remaining = append(remaining[:i], remaining[i+1:]...)
					changed = true

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
