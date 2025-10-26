package planner

import (
	"fmt"
	"sort"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// addPatternSymbols adds all symbols from a pattern to the set
func (p *Planner) addPatternSymbols(pattern query.Pattern, symbols map[query.Symbol]bool) {
	if dp, ok := pattern.(*query.DataPattern); ok {
		for _, elem := range dp.Elements {
			if elem != nil && elem.IsVariable() {
				if v, ok := elem.(query.Variable); ok {
					symbols[v.Name] = true
				}
			}
		}
	}
}

// patternUsesSymbol checks if a pattern uses a symbol
func patternUsesSymbol(pattern query.Pattern, sym query.Symbol) bool {
	if dp, ok := pattern.(*query.DataPattern); ok {
		for _, elem := range dp.Elements {
			if elem != nil && elem.IsVariable() {
				if v, ok := elem.(query.Variable); ok && v.Name == sym {
					return true
				}
			}
		}
	}
	return false
}

// isElementBound checks if a pattern element is bound
func (p *Planner) isElementBound(elem query.PatternElement, resolved map[query.Symbol]bool) bool {
	if elem == nil {
		return false
	}
	if elem.IsVariable() {
		if v, ok := elem.(query.Variable); ok {
			return resolved[v.Name]
		}
		return false
	}
	return !elem.IsBlank()
}

// Helper functions

func (p *Planner) getResolvedSymbols(resolved map[query.Symbol]bool) []query.Symbol {
	var symbols []query.Symbol
	for sym := range resolved {
		symbols = append(symbols, sym)
	}
	sort.Slice(symbols, func(i, j int) bool {
		return symbols[i] < symbols[j]
	})
	return symbols
}

func (p *Planner) extractPatternVariables(pattern *query.DataPattern) map[query.Symbol]bool {
	vars := make(map[query.Symbol]bool)
	for _, elem := range pattern.Elements {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok {
				vars[v.Name] = true
			}
		}
	}
	return vars
}

func (p *Planner) sharesVariables(pattern *query.DataPattern, vars map[query.Symbol]bool) bool {
	for _, elem := range pattern.Elements {
		if elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok && vars[v.Name] {
				return true
			}
		}
	}
	return false
}

func (p *Planner) determineKeepSymbols(phase Phase, findVars []query.Symbol, remainingPatterns []*query.DataPattern, remainingPredicates []query.Predicate) []query.Symbol {
	keep := make(map[query.Symbol]bool)

	// Keep all find variables
	for _, sym := range findVars {
		keep[sym] = true
	}

	// Keep variables needed by remaining patterns
	for _, pattern := range remainingPatterns {
		for _, elem := range pattern.Elements {
			if elem.IsVariable() {
				if v, ok := elem.(query.Variable); ok {
					keep[v.Name] = true
				}
			}
		}
	}

	// Keep variables needed by remaining predicates
	for _, pred := range remainingPredicates {
		for _, sym := range pred.RequiredSymbols() {
			keep[sym] = true
		}
	}

	// Convert to slice
	var symbols []query.Symbol
	for sym := range keep {
		symbols = append(symbols, sym)
	}
	sort.Slice(symbols, func(i, j int) bool {
		return symbols[i] < symbols[j]
	})

	return symbols
}

func (p *Planner) optimizePhase(phase *Phase) {
	// Sort patterns by selectivity within the phase
	sort.Slice(phase.Patterns, func(i, j int) bool {
		return phase.Patterns[i].Selectivity < phase.Patterns[j].Selectivity
	})
}

func (p *Planner) validatePlan(phases []Phase, expressions []*query.Expression, subqueries []*query.SubqueryPattern, findVars []query.Symbol, inputSymbols map[query.Symbol]bool) error {
	resolved := make(map[query.Symbol]bool)

	// Input parameters are already resolved
	for sym := range inputSymbols {
		resolved[sym] = true
	}

	// Track what symbols are resolved by the plan
	for _, phase := range phases {
		for _, pattern := range phase.Patterns {
			for sym := range pattern.Bindings {
				resolved[sym] = true
			}
		}

		// Also check for ground predicates that bind variables
		for _, pred := range phase.Predicates {
			if pred.Type == PredicateEquality && pred.Variable != "" && pred.Value != nil {
				// This is a ground predicate that binds a variable to a constant
				resolved[pred.Variable] = true
			}
		}
	}

	// Track what symbols are resolved by expressions
	for _, expr := range expressions {
		resolved[expr.Binding] = true
	}

	// Track what symbols are resolved by subqueries
	for _, subq := range subqueries {
		switch b := subq.Binding.(type) {
		case query.TupleBinding:
			for _, v := range b.Variables {
				resolved[v] = true
			}
		case query.CollectionBinding:
			resolved[b.Variable] = true
		case query.RelationBinding:
			for _, v := range b.Variables {
				resolved[v] = true
			}
		}
	}

	// Check that all find variables will be resolved
	for _, sym := range findVars {
		if !resolved[sym] {
			return fmt.Errorf("find variable %s will not be bound by query", sym)
		}
	}

	// Validate all FunctionPredicates use registered functions
	for _, phase := range phases {
		for _, predPlan := range phase.Predicates {
			if fp, ok := predPlan.Predicate.(*query.FunctionPredicate); ok {
				argCount := len(fp.Args)
				if err := query.DefaultRegistry.Validate(fp.Fn, argCount); err != nil {
					return fmt.Errorf("invalid function predicate: %w", err)
				}
			}
		}
	}

	return nil
}

// CacheStats returns cache statistics if caching is enabled
func (p *Planner) CacheStats() (hits, misses int64, size int, enabled bool) {
	if p.cache == nil {
		return 0, 0, 0, false
	}
	hits, misses, size = p.cache.Stats()
	return hits, misses, size, true
}

// ClearCache clears the query plan cache
func (p *Planner) ClearCache() {
	if p.cache != nil {
		p.cache.Clear()
	}
}

// SetCache sets a custom cache or disables caching (if nil)
func (p *Planner) SetCache(cache *PlanCache) {
	p.cache = cache
}
