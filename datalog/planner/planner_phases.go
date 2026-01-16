package planner

import (
	"sort"
	"strings"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// createPhases groups patterns into execution phases based on dependencies
func (p *Planner) createPhases(dataPatterns []*query.DataPattern, predicates []query.Predicate, expressions []*query.Expression, subqueries []*query.SubqueryPattern, notClauses []*query.NotClause, notJoinClauses []*query.NotJoinClause, orClauses []*query.OrClause, orJoinClauses []*query.OrJoinClause, findElements []query.FindElement, inputSymbols map[query.Symbol]bool) []Phase {
	// Extract symbols from findElements for pattern ordering
	var findVars []query.Symbol
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			findVars = append(findVars, e.Symbol)
		case query.FindAggregate:
			findVars = append(findVars, e.Arg)
		}
	}

	// Check if we should use fine-grained phases
	if p.options.EnableFineGrainedPhases {
		return p.createFineGrainedPhases(dataPatterns, predicates, expressions, subqueries,
			notClauses, notJoinClauses, orClauses, orJoinClauses,
			findElements, inputSymbols)
	}

	// Default: group patterns by their primary entity symbol (like Clojure planner)
	patternGroups := p.groupPatternsByEntity(dataPatterns)

	// Then order the groups based on symbol relationships and selectivity
	orderedGroups := p.orderPatternGroups(patternGroups, findVars)

	// Convert groups to phases with proper symbol tracking
	var phases []Phase
	availableSymbols := make(map[query.Symbol]bool)
	// Start with input symbols as available
	for sym := range inputSymbols {
		availableSymbols[sym] = true
	}
	remainingPredicates := append([]query.Predicate{}, predicates...)

	for _, group := range orderedGroups {
		phase := Phase{
			Available: p.getResolvedSymbols(availableSymbols),
		}

		// Determine what symbols this phase will provide
		provides := make(map[query.Symbol]bool)
		for _, pattern := range group.patterns {
			for _, elem := range pattern.Elements {
				if elem != nil && elem.IsVariable() {
					if v, ok := elem.(query.Variable); ok {
						provides[v.Name] = true
					}
				}
			}
		}

		// Create pattern plans with current available symbols
		for _, pattern := range group.patterns {
			patternPlan := p.planPattern(pattern, availableSymbols)
			phase.Patterns = append(phase.Patterns, patternPlan)
		}

		// Add predicates that can be evaluated with available + provides
		// NOTE: We do NOT include expression outputs here - that happens later
		// in assignExpressionsToPhases() which properly assigns expressions first,
		// then reassigns predicates to handle expression dependencies.
		allSymbols := make(map[query.Symbol]bool)
		for sym := range availableSymbols {
			allSymbols[sym] = true
		}
		for sym := range provides {
			allSymbols[sym] = true
		}

		var stillRemaining []query.Predicate
		for _, pred := range remainingPredicates {
			if p.canEvaluatePredicate(pred, allSymbols) {
				phase.Predicates = append(phase.Predicates, p.createPredicatePlan(pred))
			} else {
				stillRemaining = append(stillRemaining, pred)
			}
		}
		remainingPredicates = stillRemaining

		// Set provides for this phase
		phase.Provides = p.getResolvedSymbols(provides)

		// Update available symbols for next phase
		for sym := range provides {
			availableSymbols[sym] = true
		}

		phases = append(phases, phase)
	}

	// Handle any remaining predicates
	if len(remainingPredicates) > 0 && len(phases) > 0 {
		lastPhase := &phases[len(phases)-1]
		for _, pred := range remainingPredicates {
			if p.canEvaluatePredicate(pred, availableSymbols) {
				lastPhase.Predicates = append(lastPhase.Predicates, p.createPredicatePlan(pred))
			}
		}
	}

	// If we have no phases but have predicates/expressions/subqueries/OR clauses, create at least one phase
	if len(phases) == 0 && (len(predicates) > 0 || len(expressions) > 0 || len(subqueries) > 0 || len(orClauses) > 0 || len(orJoinClauses) > 0) {
		// Create a phase that can execute predicates/expressions/OR clauses with input symbols
		phase := Phase{
			Available: p.getResolvedSymbols(availableSymbols),
			Provides:  []query.Symbol{},
		}
		phases = append(phases, phase)
	}

	// Assign expressions to phases
	p.assignExpressionsToPhases(phases, expressions, predicates)

	// Mark expression bindings as available for validation
	for _, expr := range expressions {
		availableSymbols[expr.Binding] = true
	}

	// Assign subqueries to phases
	p.assignSubqueriesToPhases(phases, subqueries)

	// Mark subquery bindings as available for validation
	for _, subq := range subqueries {
		// Extract variables from binding form
		switch b := subq.Binding.(type) {
		case query.TupleBinding:
			for _, v := range b.Variables {
				availableSymbols[v] = true
			}
		case query.CollectionBinding:
			availableSymbols[b.Variable] = true
		case query.RelationBinding:
			for _, v := range b.Variables {
				availableSymbols[v] = true
			}
		}
	}

	// Assign OR/OR-JOIN clauses to phases (they are data sources that provide symbols)
	p.assignOrClausesToPhases(phases, orClauses, orJoinClauses)

	// Mark OR clause outputs as available
	for _, orClause := range orClauses {
		syms := extractOrClauseSymbols(orClause)
		for _, sym := range syms.Provides {
			availableSymbols[sym] = true
		}
	}
	for _, orJoinClause := range orJoinClauses {
		for _, sym := range orJoinClause.JoinVars {
			availableSymbols[sym] = true
		}
	}

	// Assign NOT/NOT-JOIN clauses to phases (they filter based on required symbols)
	p.assignNotClausesToPhases(phases, notClauses, notJoinClauses)

	// Extract symbols from findElements for Keep calculation
	findSymbols := make([]query.Symbol, 0, len(findElements))
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			findSymbols = append(findSymbols, e.Symbol)
		case query.FindAggregate:
			findSymbols = append(findSymbols, e.Arg)
		}
	}

	// Determine what symbols to keep for each phase
	p.determinePhaseKeepSymbols(phases, findSymbols, expressions, predicates)

	// Set find elements for each phase (preserves aggregates)
	for i := range phases {
		phases[i].Find = findElements
	}

	return phases
}

// createFineGrainedPhases creates smaller, more focused phases to avoid large cross-products
func (p *Planner) createFineGrainedPhases(dataPatterns []*query.DataPattern, predicates []query.Predicate, expressions []*query.Expression, subqueries []*query.SubqueryPattern, notClauses []*query.NotClause, notJoinClauses []*query.NotJoinClause, orClauses []*query.OrClause, orJoinClauses []*query.OrJoinClause, findElements []query.FindElement, inputSymbols map[query.Symbol]bool) []Phase {
	// Extract symbols from findElements for pattern ordering
	var findVars []query.Symbol
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			findVars = append(findVars, e.Symbol)
		case query.FindAggregate:
			findVars = append(findVars, e.Arg)
		}
	}

	var phases []Phase
	availableSymbols := make(map[query.Symbol]bool)
	// Start with input symbols as available
	for sym := range inputSymbols {
		availableSymbols[sym] = true
	}
	remainingPatterns := append([]*query.DataPattern{}, dataPatterns...)
	remainingPredicates := append([]query.Predicate{}, predicates...)

	// Phase 1: Process patterns with constants first (most selective)
	var constantPatterns []*query.DataPattern
	var variablePatterns []*query.DataPattern

	for _, pattern := range remainingPatterns {
		hasConstant := false
		// Check if pattern has any constant values
		for _, elem := range pattern.Elements {
			if elem != nil && !elem.IsVariable() {
				hasConstant = true
				break
			}
		}

		if hasConstant {
			constantPatterns = append(constantPatterns, pattern)
		} else {
			variablePatterns = append(variablePatterns, pattern)
		}
	}

	// Sort constant patterns by estimated selectivity
	// Pass availableSymbols so input parameters are treated as bound
	sort.Slice(constantPatterns, func(i, j int) bool {
		return p.estimatePatternSelectivity(constantPatterns[i], availableSymbols) < p.estimatePatternSelectivity(constantPatterns[j], availableSymbols)
	})

	// Process constant patterns, grouping those with the same entity variable
	processedEntities := make(map[query.Symbol]bool)

	for _, pattern := range constantPatterns {
		// Get entity variable if present
		var entitySym query.Symbol
		if elem := pattern.GetE(); elem != nil && elem.IsVariable() {
			if v, ok := elem.(query.Variable); ok {
				entitySym = v.Name
			}
		}

		// If we haven't processed this entity yet, create a phase for it
		if entitySym != "" && !processedEntities[entitySym] {
			phase := Phase{
				Available: p.getResolvedSymbols(availableSymbols),
			}

			// Add this pattern
			phase.Patterns = append(phase.Patterns, p.planPattern(pattern, availableSymbols))

			// Find other patterns with the same entity that we can include
			for _, other := range constantPatterns {
				if other == pattern {
					continue
				}
				if elem := other.GetE(); elem != nil && elem.IsVariable() {
					if v, ok := elem.(query.Variable); ok && v.Name == entitySym {
						phase.Patterns = append(phase.Patterns, p.planPattern(other, availableSymbols))
					}
				}
			}

			// Also add variable patterns with this entity
			for _, other := range variablePatterns {
				if elem := other.GetE(); elem != nil && elem.IsVariable() {
					if v, ok := elem.(query.Variable); ok && v.Name == entitySym {
						phase.Patterns = append(phase.Patterns, p.planPattern(other, availableSymbols))
					}
				}
			}

			// Update provides and available symbols
			provides := make(map[query.Symbol]bool)
			for _, patPlan := range phase.Patterns {
				pattern := patPlan.Pattern.(*query.DataPattern)
				for _, elem := range pattern.Elements {
					if elem != nil && elem.IsVariable() {
						if v, ok := elem.(query.Variable); ok {
							provides[v.Name] = true
						}
					}
				}
			}
			phase.Provides = p.getResolvedSymbols(provides)

			// Add applicable predicates
			// NOTE: We do NOT include expression outputs here - that happens later
			// in assignExpressionsToPhases() which properly assigns expressions first,
			// then reassigns predicates to handle expression dependencies.
			allSymbols := make(map[query.Symbol]bool)
			for sym := range availableSymbols {
				allSymbols[sym] = true
			}
			for sym := range provides {
				allSymbols[sym] = true
			}

			var stillRemaining []query.Predicate
			for _, pred := range remainingPredicates {
				if p.canEvaluatePredicate(pred, allSymbols) {
					phase.Predicates = append(phase.Predicates, p.createPredicatePlan(pred))
				} else {
					stillRemaining = append(stillRemaining, pred)
				}
			}
			remainingPredicates = stillRemaining

			// Update available symbols
			for sym := range provides {
				availableSymbols[sym] = true
			}

			phases = append(phases, phase)
			processedEntities[entitySym] = true
		}
	}

	// Remove processed patterns
	var stillRemaining []*query.DataPattern
	for _, pattern := range remainingPatterns {
		processed := false
		for entitySym := range processedEntities {
			if elem := pattern.GetE(); elem != nil && elem.IsVariable() {
				if v, ok := elem.(query.Variable); ok && v.Name == entitySym {
					processed = true
					break
				}
			}
		}
		if !processed {
			stillRemaining = append(stillRemaining, pattern)
		}
	}
	remainingPatterns = stillRemaining

	// Process remaining patterns using default grouping
	if len(remainingPatterns) > 0 {
		groups := p.groupPatternsByEntity(remainingPatterns)
		orderedGroups := p.orderPatternGroups(groups, findVars)

		for _, group := range orderedGroups {
			phase := Phase{
				Available: p.getResolvedSymbols(availableSymbols),
			}

			// Create pattern plans
			for _, pattern := range group.patterns {
				phase.Patterns = append(phase.Patterns, p.planPattern(pattern, availableSymbols))
			}

			// Update provides
			provides := make(map[query.Symbol]bool)
			for _, pattern := range group.patterns {
				for _, elem := range pattern.Elements {
					if elem != nil && elem.IsVariable() {
						if v, ok := elem.(query.Variable); ok {
							provides[v.Name] = true
						}
					}
				}
			}
			phase.Provides = p.getResolvedSymbols(provides)

			// Add applicable predicates
			// NOTE: We do NOT include expression outputs here - that happens later
			// in assignExpressionsToPhases() which properly assigns expressions first,
			// then reassigns predicates to handle expression dependencies.
			allSymbols := make(map[query.Symbol]bool)
			for sym := range availableSymbols {
				allSymbols[sym] = true
			}
			for sym := range provides {
				allSymbols[sym] = true
			}

			var stillRemaining []query.Predicate
			for _, pred := range remainingPredicates {
				if p.canEvaluatePredicate(pred, allSymbols) {
					phase.Predicates = append(phase.Predicates, p.createPredicatePlan(pred))
				} else {
					stillRemaining = append(stillRemaining, pred)
				}
			}
			remainingPredicates = stillRemaining

			// Update available symbols
			for sym := range provides {
				availableSymbols[sym] = true
			}

			phases = append(phases, phase)
		}
	}

	// Handle any remaining predicates
	if len(remainingPredicates) > 0 && len(phases) > 0 {
		lastPhase := &phases[len(phases)-1]
		for _, pred := range remainingPredicates {
			if p.canEvaluatePredicate(pred, availableSymbols) {
				lastPhase.Predicates = append(lastPhase.Predicates, p.createPredicatePlan(pred))
			}
		}
	}

	// If we have no phases but have predicates/expressions/subqueries/OR clauses, create at least one phase
	if len(phases) == 0 && (len(predicates) > 0 || len(expressions) > 0 || len(subqueries) > 0 || len(orClauses) > 0 || len(orJoinClauses) > 0) {
		// Create a phase that can execute predicates/expressions/OR clauses with input symbols
		phase := Phase{
			Available: p.getResolvedSymbols(availableSymbols),
			Provides:  []query.Symbol{},
		}
		phases = append(phases, phase)
	}

	// Assign expressions to phases
	p.assignExpressionsToPhases(phases, expressions, predicates)

	// Mark expression bindings as available
	for _, expr := range expressions {
		availableSymbols[expr.Binding] = true
	}

	// Assign subqueries to phases
	p.assignSubqueriesToPhases(phases, subqueries)

	// Mark subquery bindings as available
	for _, subq := range subqueries {
		// Extract variables from binding form
		switch b := subq.Binding.(type) {
		case query.TupleBinding:
			for _, v := range b.Variables {
				availableSymbols[v] = true
			}
		case query.CollectionBinding:
			availableSymbols[b.Variable] = true
		case query.RelationBinding:
			for _, v := range b.Variables {
				availableSymbols[v] = true
			}
		}
	}

	// Assign OR/OR-JOIN clauses to phases (they are data sources that provide symbols)
	p.assignOrClausesToPhases(phases, orClauses, orJoinClauses)

	// Mark OR clause outputs as available
	for _, orClause := range orClauses {
		syms := extractOrClauseSymbols(orClause)
		for _, sym := range syms.Provides {
			availableSymbols[sym] = true
		}
	}
	for _, orJoinClause := range orJoinClauses {
		for _, sym := range orJoinClause.JoinVars {
			availableSymbols[sym] = true
		}
	}

	// Assign NOT/NOT-JOIN clauses to phases (they filter based on required symbols)
	p.assignNotClausesToPhases(phases, notClauses, notJoinClauses)

	// Extract symbols from findElements for Keep calculation
	findSymbols := make([]query.Symbol, 0, len(findElements))
	for _, elem := range findElements {
		switch e := elem.(type) {
		case query.FindVariable:
			findSymbols = append(findSymbols, e.Symbol)
		case query.FindAggregate:
			findSymbols = append(findSymbols, e.Arg)
		}
	}

	// Determine what symbols to keep for each phase
	p.determinePhaseKeepSymbols(phases, findSymbols, expressions, predicates)

	// Set find elements for each phase (preserves aggregates)
	for i := range phases {
		phases[i].Find = findElements
	}

	return phases
}

// determinePhaseKeepSymbols determines what symbols each phase needs to keep
func (p *Planner) determinePhaseKeepSymbols(phases []Phase, findVars []query.Symbol, expressions []*query.Expression, predicates []query.Predicate) {
	// First, determine what symbols are needed for the final result
	finalNeeded := make(map[query.Symbol]bool)
	for _, sym := range findVars {
		finalNeeded[sym] = true
	}

	// NOTE: We do NOT add expression inputs to finalNeeded!
	// Expression inputs are available during phase execution (in Provides)
	// but don't need to be in Keep unless they're:
	//   1. In findVars (already in finalNeeded), OR
	//   2. Needed by future phases (handled in loop below)
	//
	// The old code here tried to trace through expression dependencies,
	// but this was buggy because it treated "needed for computation" the same
	// as "needed in output". For single-phase queries, Keep should only be findVars.
	// For multi-phase queries, the loop below (lines 602-639) already handles
	// keeping symbols that are needed by future phase expressions/predicates.

	// For each phase, determine what symbols it needs to keep
	for i := range phases {
		keep := make(map[query.Symbol]bool)

		// Get all symbols available after this phase
		available := make(map[query.Symbol]bool)
		for _, sym := range phases[i].Available {
			available[sym] = true
		}
		for _, sym := range phases[i].Provides {
			available[sym] = true
		}

		// 1. Keep symbols needed for final result
		for sym := range finalNeeded {
			if available[sym] {
				keep[sym] = true
			}
		}

		// 2. Keep symbols needed for joins with future phases
		// Only keep symbols that are actually used in future phase patterns
		for j := i + 1; j < len(phases); j++ {
			// Check which symbols from this phase are used in future patterns
			for _, pattern := range phases[j].Patterns {
				if dp, ok := pattern.Pattern.(*query.DataPattern); ok {
					for _, elem := range dp.Elements {
						if v, ok := elem.(query.Variable); ok {
							if available[v.Name] {
								keep[v.Name] = true
							}
						}
					}
				}
			}
		}

		// 3. Keep symbols needed for join predicates in future phases
		for j := i + 1; j < len(phases); j++ {
			for _, jp := range phases[j].JoinPredicates {
				// Keep the left symbol if it's available in this phase
				if available[jp.LeftSymbol] {
					keep[jp.LeftSymbol] = true
				}
			}
		}

		// 3.5. Keep symbols needed for subqueries in future phases
		for j := i + 1; j < len(phases); j++ {
			for _, sq := range phases[j].Subqueries {
				// Keep symbols that are inputs to the subquery
				for _, input := range sq.Inputs {
					if available[input] {
						keep[input] = true
					}
				}

				// CRITICAL FIX: Also keep symbols that are OUTPUTS of the subquery
				// if they already exist in the current phase. This enables the JOIN
				// between existing values and subquery results.
				//
				// Example: If phase 1 has pattern [?task :task/status ?s ?maxTx] and
				// phase 2 has subquery [(q [...] ?task) [[?maxTx]]], we need to keep
				// ?maxTx so it can be joined with the subquery's output.
				var bindingSyms []query.Symbol
				switch b := sq.Subquery.Binding.(type) {
				case query.TupleBinding:
					bindingSyms = b.Variables
				case query.RelationBinding:
					bindingSyms = b.Variables
				case query.CollectionBinding:
					bindingSyms = []query.Symbol{b.Variable}
				}
				for _, sym := range bindingSyms {
					if available[sym] {
						keep[sym] = true
					}
				}
			}
		}

		// 3.75. Keep symbols needed for expressions in future phases
		for j := i + 1; j < len(phases); j++ {
			for _, ep := range phases[j].Expressions {
				// Keep symbols that are inputs to the expression
				for _, input := range ep.Inputs {
					if available[input] {
						keep[input] = true
					}
				}
			}
		}

		// 4. For non-first phases, ensure we keep at least one join symbol
		if i > 0 && len(phases[i].Available) > 0 {
			hasJoinSymbol := false
			for _, sym := range phases[i].Available {
				if keep[sym] {
					hasJoinSymbol = true
					break
				}
			}

			// If no join symbol is kept, keep the most selective one
			// Prefer entity variables over others
			if !hasJoinSymbol {
				bestSym := phases[i].Available[0]
				for _, sym := range phases[i].Available {
					// Prefer symbols that appear in more patterns (likely entity vars)
					if strings.HasPrefix(string(sym), "?p") || strings.HasPrefix(string(sym), "?e") {
						bestSym = sym
						break
					}
				}
				keep[bestSym] = true
			}
		}

		// Convert to slice, preserving findVars order
		// For symbols in findVars, use the original order
		// For other symbols (needed by future phases), append them after
		var keepSlice []query.Symbol

		// First, add findVars symbols that are in keep, in their original order
		for _, sym := range findVars {
			if keep[sym] {
				keepSlice = append(keepSlice, sym)
				delete(keep, sym) // Remove so we don't add again
			}
		}

		// Then add any remaining symbols (not in findVars but needed by future phases)
		// Sort these alphabetically for deterministic output
		var remaining []query.Symbol
		for sym := range keep {
			remaining = append(remaining, sym)
		}
		sort.Slice(remaining, func(i, j int) bool {
			return remaining[i] < remaining[j]
		})
		keepSlice = append(keepSlice, remaining...)

		phases[i].Keep = keepSlice

		// Debug: log what we're keeping
		// fmt.Printf("DEBUG Phase %d: Provides=%v, Keep=%v\n", i+1, phases[i].Provides, keepSlice)
	}
}

// assignOrClausesToPhases assigns OR and OR-JOIN clauses to appropriate phases
// OR clauses are data sources that provide symbols (like patterns)
func (p *Planner) assignOrClausesToPhases(phases []Phase, orClauses []*query.OrClause, orJoinClauses []*query.OrJoinClause) {
	if len(phases) == 0 {
		return
	}

	// Track symbols provided by OR clauses
	orProvides := make(map[query.Symbol]bool)

	// OR clauses don't require prior bindings - they are data sources
	// Assign them to the first phase
	for _, orClause := range orClauses {
		phases[0].OrClauses = append(phases[0].OrClauses, orClause)
		// Track what this OR clause provides
		syms := extractOrClauseSymbols(orClause)
		for _, sym := range syms.Provides {
			orProvides[sym] = true
		}
	}

	for _, orJoinClause := range orJoinClauses {
		phases[0].OrJoinClauses = append(phases[0].OrJoinClauses, orJoinClause)
		// OR-JOIN provides exactly its join vars
		for _, sym := range orJoinClause.JoinVars {
			orProvides[sym] = true
		}
	}

	// Add OR clause symbols to the phase's Provides list
	// (they weren't already there because Provides is normally from patterns)
	existingProvides := make(map[query.Symbol]bool)
	for _, sym := range phases[0].Provides {
		existingProvides[sym] = true
	}
	for sym := range orProvides {
		if !existingProvides[sym] {
			phases[0].Provides = append(phases[0].Provides, sym)
		}
	}
}

// assignNotClausesToPhases assigns NOT and NOT-JOIN clauses to appropriate phases
// NOT clauses are filters that require prior bindings
func (p *Planner) assignNotClausesToPhases(phases []Phase, notClauses []*query.NotClause, notJoinClauses []*query.NotJoinClause) {
	if len(phases) == 0 {
		return
	}

	// For each NOT clause, find the earliest phase where all required symbols are available
	for _, notClause := range notClauses {
		syms := extractNotClauseSymbols(notClause)
		assigned := false

		for i := range phases {
			// Collect all symbols available after this phase
			available := make(map[query.Symbol]bool)
			for _, sym := range phases[i].Available {
				available[sym] = true
			}
			for _, sym := range phases[i].Provides {
				available[sym] = true
			}

			// Check if all required symbols are available
			allAvailable := true
			for _, req := range syms.Requires {
				if !available[req] {
					allAvailable = false
					break
				}
			}

			if allAvailable {
				phases[i].NotClauses = append(phases[i].NotClauses, notClause)
				assigned = true
				break
			}
		}

		// If not assigned yet, add to the last phase
		if !assigned && len(phases) > 0 {
			phases[len(phases)-1].NotClauses = append(phases[len(phases)-1].NotClauses, notClause)
		}
	}

	// Same logic for NOT-JOIN clauses
	for _, notJoinClause := range notJoinClauses {
		syms := extractNotJoinClauseSymbols(notJoinClause)
		assigned := false

		for i := range phases {
			available := make(map[query.Symbol]bool)
			for _, sym := range phases[i].Available {
				available[sym] = true
			}
			for _, sym := range phases[i].Provides {
				available[sym] = true
			}

			allAvailable := true
			for _, req := range syms.Requires {
				if !available[req] {
					allAvailable = false
					break
				}
			}

			if allAvailable {
				phases[i].NotJoinClauses = append(phases[i].NotJoinClauses, notJoinClause)
				assigned = true
				break
			}
		}

		if !assigned && len(phases) > 0 {
			phases[len(phases)-1].NotJoinClauses = append(phases[len(phases)-1].NotJoinClauses, notJoinClause)
		}
	}
}
