package planner

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// Helper type for scored patterns
type scoredPattern struct {
	pattern *query.DataPattern
	score   int
	index   int
}

// patternGroup represents a group of patterns that share an entity symbol
type patternGroup struct {
	entitySym query.Symbol
	patterns  []*query.DataPattern
	symbols   map[query.Symbol]bool // All symbols referenced in the group
}

// groupPatternsByEntity groups patterns by their primary entity symbol
func (p *Planner) groupPatternsByEntity(patterns []*query.DataPattern) []patternGroup {
	groups := make(map[query.Symbol]*patternGroup)

	for _, pattern := range patterns {
		// Get the entity element
		elem := pattern.GetE()
		if elem == nil || !elem.IsVariable() {
			// Pattern without variable entity - create a special group
			// using a synthetic symbol based on the pattern
			syntheticSym := query.Symbol(fmt.Sprintf("_pattern_%p", pattern))
			if groups[syntheticSym] == nil {
				groups[syntheticSym] = &patternGroup{
					entitySym: syntheticSym,
					patterns:  []*query.DataPattern{},
					symbols:   make(map[query.Symbol]bool),
				}
			}
			groups[syntheticSym].patterns = append(groups[syntheticSym].patterns, pattern)
			// Add all symbols from the pattern
			for _, e := range pattern.Elements {
				if e != nil && e.IsVariable() {
					if v, ok := e.(query.Variable); ok {
						groups[syntheticSym].symbols[v.Name] = true
					}
				}
			}
			continue
		}

		// Get the entity variable
		v, ok := elem.(query.Variable)
		if !ok {
			continue
		}

		entitySym := v.Name
		if groups[entitySym] == nil {
			groups[entitySym] = &patternGroup{
				entitySym: entitySym,
				patterns:  []*query.DataPattern{},
				symbols:   make(map[query.Symbol]bool),
			}
		}

		groups[entitySym].patterns = append(groups[entitySym].patterns, pattern)

		// Add all symbols from the pattern
		for _, e := range pattern.Elements {
			if e != nil && e.IsVariable() {
				if v, ok := e.(query.Variable); ok {
					groups[entitySym].symbols[v.Name] = true
				}
			}
		}
	}

	// Convert to slice and sort for deterministic ordering
	var result []patternGroup
	for _, group := range groups {
		result = append(result, *group)
	}

	// Sort by entity symbol to ensure deterministic order
	// This prevents non-deterministic test failures caused by Go's randomized map iteration
	sort.Slice(result, func(i, j int) bool {
		return result[i].entitySym < result[j].entitySym
	})

	return result
}

// orderPatternGroups orders pattern groups based on symbol relationships
func (p *Planner) orderPatternGroups(groups []patternGroup, findVars []query.Symbol) []patternGroup {
	if len(groups) == 0 {
		return groups
	}

	var ordered []patternGroup
	remaining := append([]patternGroup{}, groups...)
	resolvedSymbols := make(map[query.Symbol]bool)

	// Find the best starting group (lowest score = most selective)
	var startGroup *patternGroup
	bestScore := 999999
	startIdx := -1

	for i, group := range remaining {
		score := p.scorePatternGroup(group, resolvedSymbols)
		if score < bestScore {
			bestScore = score
			startGroup = &remaining[i]
			startIdx = i
		}
	}

	if startGroup != nil {
		ordered = append(ordered, *startGroup)
		// Add symbols from first group
		for sym := range startGroup.symbols {
			resolvedSymbols[sym] = true
		}
		// Remove from remaining
		remaining = append(remaining[:startIdx], remaining[startIdx+1:]...)
	}

	// Order remaining groups by their relationship to resolved symbols
	for len(remaining) > 0 {
		bestScore := -1
		bestIdx := -1

		for i, group := range remaining {
			score := 0
			// Count intersections with resolved symbols
			for sym := range group.symbols {
				if resolvedSymbols[sym] {
					score += 10
				}
			}
			// Bonus for groups that bind find variables
			for _, findVar := range findVars {
				if group.symbols[findVar] {
					score += 5
				}
			}

			if score > bestScore {
				bestScore = score
				bestIdx = i
			}
		}

		// If no group has connections, just take the first one
		if bestIdx < 0 {
			bestIdx = 0
		}

		// Add the best group
		ordered = append(ordered, remaining[bestIdx])
		for sym := range remaining[bestIdx].symbols {
			resolvedSymbols[sym] = true
		}

		// Remove from remaining
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
	}

	return ordered
}

// scorePatternGroup scores a pattern group for initial selection
func (p *Planner) scorePatternGroup(group patternGroup, resolved map[query.Symbol]bool) int {
	// Find the most selective pattern in the group
	bestSelectivity := 999999
	for _, pattern := range group.patterns {
		selectivity := p.scorePattern(pattern, resolved)
		if selectivity < bestSelectivity {
			bestSelectivity = selectivity
		}
	}

	// Use the best pattern's selectivity as the group score
	// Lower scores are better (more selective)
	return bestSelectivity
}

// selectPatternsForPhase selects patterns that can be executed with current bindings
func (p *Planner) selectPatternsForPhase(patterns []*query.DataPattern, resolved map[query.Symbol]bool) ([]*query.DataPattern, []*query.DataPattern) {

	var scorable []scoredPattern
	var unscorable []*query.DataPattern

	for i, pattern := range patterns {
		score := p.scorePattern(pattern, resolved)
		if score >= 0 {
			scorable = append(scorable, scoredPattern{
				pattern: pattern,
				score:   score,
				index:   i,
			})
		} else {
			unscorable = append(unscorable, pattern)
		}
	}

	// Sort by score (lower = more selective = better)
	sort.Slice(scorable, func(i, j int) bool {
		return scorable[i].score < scorable[j].score
	})

	// Select patterns for this phase
	var selected []*query.DataPattern
	var unselected []*query.DataPattern

	if p.options.EnableDynamicReordering && len(scorable) > 0 {
		// Group patterns that share entities or can be executed together
		selected = p.groupRelatedPatterns(scorable)

		// Remaining patterns go to unselected
		selectedSet := make(map[*query.DataPattern]bool)
		for _, pat := range selected {
			selectedSet[pat] = true
		}
		for _, sp := range scorable {
			if !selectedSet[sp.pattern] {
				unselected = append(unselected, sp.pattern)
			}
		}
	} else {
		// When not using dynamic reordering, be more conservative
		// Only select patterns that:
		// 1. Have at least one bound element, OR
		// 2. Are the first pattern (to bootstrap)
		if len(resolved) == 0 && len(scorable) > 0 {
			// First phase - select the most selective pattern
			selected = append(selected, scorable[0].pattern)
		} else {
			// Subsequent phases - select patterns more intelligently
			// Group patterns by how they connect
			for _, sp := range scorable {
				pattern := sp.pattern

				// Count how many positions have bound variables
				boundCount := 0
				if elem := pattern.GetE(); elem != nil && p.isElementBound(elem, resolved) {
					boundCount++
				}
				if elem := pattern.GetA(); elem != nil && p.isElementBound(elem, resolved) {
					boundCount++
				}
				if elem := pattern.GetV(); elem != nil && p.isElementBound(elem, resolved) {
					boundCount++
				}

				// Select if:
				// 1. Has at least one bound element (can filter)
				// 2. Or is one of the first few patterns and very selective
				if boundCount > 0 || (len(selected) < 3 && sp.score < 100) {
					selected = append(selected, pattern)
				} else {
					unselected = append(unselected, pattern)
				}
			}

			// If nothing selected but we have patterns, force select the best one
			if len(selected) == 0 && len(scorable) > 0 {
				selected = append(selected, scorable[0].pattern)
			}
		}
	}

	// Add unscorable patterns back to unselected
	unselected = append(unselected, unscorable...)

	return selected, unselected
}

// estimatePatternSelectivity estimates the selectivity of a pattern (lower is more selective)
// The resolved parameter should contain all symbols that are currently available (including input parameters)
func (p *Planner) estimatePatternSelectivity(pattern *query.DataPattern, resolved map[query.Symbol]bool) int {
	return p.scorePattern(pattern, resolved)
}

// scorePattern estimates the selectivity of a pattern
func (p *Planner) scorePattern(pattern *query.DataPattern, resolved map[query.Symbol]bool) int {
	score := 0
	boundCount := 0

	// Check entity
	if elem := pattern.GetE(); elem != nil {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok && resolved[v.Name] {
				boundCount++
			} else {
				score += 1000 // Unbound entity is least selective
			}
		} else {
			boundCount++
			// Constant entity is extremely selective
			score -= 800 // Huge bonus for constant entity
		}
	}

	// Check attribute
	if elem := pattern.GetA(); elem != nil {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok && resolved[v.Name] {
				boundCount++
				score += 10
			} else {
				score += 100 // Unbound attribute is moderately unselective
			}
		} else {
			boundCount++
			// Use cardinality statistics if available
			if constant, ok := elem.(query.Constant); ok {
				if attr, ok := constant.Value.(datalog.Keyword); ok {
					if card, exists := p.stats.AttributeCardinality[attr.String()]; exists {
						score += card / 100 // Higher cardinality = less selective
					}
				}
			}
		}
	}

	// Check value
	if elem := pattern.GetV(); elem != nil {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok {
				if resolved[v.Name] {
					// Variable is already bound - can use it to filter
					// Treat bound variables (especially input parameters) as selective as constants
					boundCount++
					score -= 500 // Bound value is as selective as constant
				} else {
					// Variable is unbound - will match many rows
					score += 500 // Very unselective
				}
			} else {
				// Shouldn't happen - variable check failed
				score += 50
			}
		} else {
			boundCount++
			// Constant values are highly selective
			score -= 500 // Big bonus for constant value
		}
	}

	// Patterns with no bound elements can't be executed yet
	if boundCount == 0 && len(resolved) > 0 {
		return -1
	}

	// Bonus for patterns that bind new variables
	newBindings := 0
	for _, elem := range pattern.Elements {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok && !resolved[v.Name] {
				newBindings++
			}
		}
	}
	score -= newBindings * 10 // Patterns that bind more variables are preferred

	return score
}

// planPattern creates a pattern plan with index selection
func (p *Planner) planPattern(pattern *query.DataPattern, resolved map[query.Symbol]bool) PatternPlan {
	plan := PatternPlan{
		Pattern:  pattern,
		Bindings: make(map[query.Symbol]bool),
	}

	// Determine which elements are bound
	if elem := pattern.GetE(); elem != nil {
		plan.BoundMask.E = p.isElementBound(elem, resolved)
	}
	if elem := pattern.GetA(); elem != nil {
		plan.BoundMask.A = p.isElementBound(elem, resolved)
	}
	if elem := pattern.GetV(); elem != nil {
		plan.BoundMask.V = p.isElementBound(elem, resolved)
	}
	if elem := pattern.GetT(); elem != nil {
		plan.BoundMask.T = p.isElementBound(elem, resolved)
	}

	// Select index
	plan.Index = p.selectIndex(plan.BoundMask)

	// Calculate selectivity
	plan.Selectivity = p.scorePattern(pattern, resolved)

	// Extract variables that will be bound
	for _, elem := range pattern.Elements {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok {
				plan.Bindings[v.Name] = true
			}
		}
	}

	return plan
}

// selectIndex chooses the best index based on bound elements
func (p *Planner) selectIndex(mask BoundMask) IndexType {
	switch {
	case mask.E && mask.A && mask.V:
		return EAVT // All bound - most selective
	case mask.E && mask.A:
		return EAVT // Entity + attribute
	case mask.A && mask.V:
		return AVET // Attribute + value (for reverse lookups)
	case mask.A && mask.E:
		return AEVT // Attribute + entity
	case mask.E:
		return EAVT // Entity only
	case mask.A:
		return AEVT // Attribute only
	case mask.V:
		return VAET // Value only (for ref lookups)
	default:
		return EAVT // Full scan - try to avoid
	}
}

// groupRelatedPatterns groups patterns that should be executed together
func (p *Planner) groupRelatedPatterns(scorable []scoredPattern) []*query.DataPattern {
	if len(scorable) == 0 {
		return nil
	}

	// Start with the most selective pattern
	selected := []*query.DataPattern{scorable[0].pattern}
	selectedVars := p.extractPatternVariables(scorable[0].pattern)

	// Check if patterns share the same entity (constant in E position)
	var sharedEntity interface{}
	if elem := scorable[0].pattern.GetE(); elem != nil && !elem.IsVariable() && !elem.IsBlank() {
		if c, ok := elem.(query.Constant); ok {
			sharedEntity = c.Value
		}
	}

	// Add patterns that:
	// 1. Share the same constant entity, OR
	// 2. Share multiple variables with already selected patterns, OR
	// 3. Have complementary variable bindings (one provides what another needs)
	for i := 1; i < len(scorable); i++ {
		pattern := scorable[i].pattern

		// Check for same entity
		if sharedEntity != nil {
			if elem := pattern.GetE(); elem != nil && !elem.IsVariable() && !elem.IsBlank() {
				if c, ok := elem.(query.Constant); ok && c.Value == sharedEntity {
					selected = append(selected, pattern)
					continue
				}
			}
		}

		// Check for shared variables
		patternVars := p.extractPatternVariables(pattern)
		sharedVars := 0
		providesNeeded := false
		needsProvided := false

		for v := range patternVars {
			if selectedVars[v] {
				sharedVars++
			}
		}

		// Check if this pattern provides variables needed by selected patterns
		for v := range selectedVars {
			if patternVars[v] {
				providesNeeded = true
				break
			}
		}

		// Check if selected patterns provide variables needed by this pattern
		for v := range patternVars {
			if selectedVars[v] {
				needsProvided = true
				break
			}
		}

		// Include pattern if it's closely related
		if sharedVars >= 2 || (sharedVars >= 1 && (providesNeeded || needsProvided)) {
			selected = append(selected, pattern)
			// Update selected variables
			for v := range patternVars {
				selectedVars[v] = true
			}
		}
	}

	return selected
}
