// Package planner creates optimized query execution plans from parsed queries.
//
// File organization:
//   - planner.go: Core Planner struct and Plan() entry point
//   - planner_phases.go: Phase creation and symbol dependency tracking
//   - planner_patterns.go: Pattern grouping, scoring, and index selection
//   - planner_predicates.go: Predicate classification and planning
//   - planner_expressions.go: Expression assignment to phases
//   - planner_subqueries.go: Subquery planning and decorrelation
//   - planner_utils.go: Symbol extraction and validation utilities
//
// Start with Plan() in planner.go to understand the planning flow.
package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// Planner creates optimized query plans
type Planner struct {
	stats             *Statistics
	options           PlannerOptions
	expressionOutputs map[query.Symbol]bool // Track which variables are provided by expressions
	cache             *PlanCache            // Query plan cache
}

// NewPlanner creates a new query planner
func NewPlanner(stats *Statistics, options PlannerOptions) *Planner {
	if stats == nil {
		stats = &Statistics{
			AttributeCardinality: make(map[string]int),
			EntityCount:          1000000, // Default estimate
		}
	}
	return &Planner{
		stats:   stats,
		options: options,
		cache:   options.Cache, // Use cache from options if provided
	}
}

// Options returns the planner options
func (p *Planner) Options() PlannerOptions {
	return p.options
}

// Plan creates an optimized query plan
func (p *Planner) Plan(q *query.Query) (*QueryPlan, error) {
	// Check cache first (with planner options)
	if p.cache != nil {
		if cached, ok := p.cache.GetWithOptions(q, p.options); ok {
			return cached, nil
		}
	}

	// Plan the query
	plan, err := p.PlanWithBindings(q, nil)
	if err != nil {
		return nil, err
	}

	// Cache the plan (with planner options)
	if p.cache != nil {
		p.cache.SetWithOptions(q, plan, p.options)
	}

	return plan, nil
}

// PlanWithBindings creates an optimized query plan with initial bindings
// This is used for subqueries where input parameters are already bound
func (p *Planner) PlanWithBindings(q *query.Query, initialBindings map[query.Symbol]bool) (*QueryPlan, error) {
	// Separate patterns by type
	dataPatterns, predicates, expressions, subqueries := p.separatePatterns(q.Where)

	// Extract find symbols from FindElements
	var findSymbols []query.Symbol
	findSymbolSet := make(map[query.Symbol]bool)
	for _, elem := range q.Find {
		switch e := elem.(type) {
		case query.FindVariable:
			if !findSymbolSet[e.Symbol] {
				findSymbols = append(findSymbols, e.Symbol)
				findSymbolSet[e.Symbol] = true
			}
		case query.FindAggregate:
			// Aggregates need their argument variable to be available
			if !findSymbolSet[e.Arg] {
				findSymbols = append(findSymbols, e.Arg)
				findSymbolSet[e.Arg] = true
			}
		}
	}

	// Extract input symbols from :in clause (excluding database)
	inputSymbols := make(map[query.Symbol]bool)
	for _, input := range q.In {
		switch inp := input.(type) {
		case query.ScalarInput:
			inputSymbols[inp.Symbol] = true
		case query.CollectionInput:
			inputSymbols[inp.Symbol] = true
		case query.TupleInput:
			for _, sym := range inp.Symbols {
				inputSymbols[sym] = true
			}
		case query.RelationInput:
			for _, sym := range inp.Symbols {
				inputSymbols[sym] = true
			}
		}
	}

	// Merge with initial bindings (for subqueries where input parameters are already bound)
	if initialBindings != nil {
		for sym := range initialBindings {
			inputSymbols[sym] = true
		}
	}

	// Collect expression output variables to avoid treating them as ground predicates
	expressionOutputs := make(map[query.Symbol]bool)
	for _, expr := range expressions {
		if expr.Binding != "" {
			expressionOutputs[expr.Binding] = true
		}
	}

	// Store expression outputs in planner for use by canEvaluatePredicate
	p.expressionOutputs = expressionOutputs

	// Create phases with the new types directly
	// Pass q.Find ([]query.FindElement) to preserve aggregates in Phase.Find
	phases := p.createPhases(dataPatterns, predicates, expressions, subqueries, q.Find, inputSymbols)

	// Reorder phases to maximize symbol connectivity (if enabled)
	if p.options.EnableDynamicReordering {
		phases = p.reorderPhasesByRelations(phases, inputSymbols)

		// After reordering, recalculate Available fields FIRST
		// This ensures expression assignment has correct symbol availability
		phases = updatePhaseSymbols(phases, q.Find, inputSymbols)

		// Re-assign expressions after reordering to ensure they're in phases where inputs are available
		// Phase reordering can move patterns, breaking expression input availability
		// NOTE: This clears existing expression assignments and reassigns from scratch
		p.assignExpressionsToPhases(phases, expressions, predicates)

		// Re-assign subqueries after reordering to ensure their inputs are still available
		// Phase reordering can separate subqueries from the expressions/patterns they depend on
		// NOTE: assignSubqueriesToPhases adds subquery outputs to Provides
		p.assignSubqueriesToPhases(phases, subqueries)

		// After expression and subquery assignment, recalculate symbols again to include their outputs
		phases = updatePhaseSymbols(phases, q.Find, inputSymbols)
	}

	// Optimize each phase
	for i := range phases {
		p.optimizePhase(&phases[i])
	}

	// Apply predicate propagation to push predicates to storage
	if p.options.EnablePredicatePushdown {
		for i := range phases {
			phases[i].PushPredicates()
		}
	}

	// Apply semantic rewriting to transform expensive predicates
	if p.options.EnableSemanticRewriting {
		plan := &QueryPlan{Query: q, Phases: phases}
		rewriteTimePredicates(plan, p.options)
		phases = plan.Phases
	}

	// Apply conditional aggregate rewriting to eliminate correlated subqueries
	if p.options.EnableConditionalAggregateRewriting {
		plan := &QueryPlan{Query: q, Phases: phases}
		if err := rewriteCorrelatedAggregates(plan, p.options); err != nil {
			return nil, fmt.Errorf("conditional aggregate rewriting failed: %w", err)
		}
		phases = plan.Phases

		// After rewriting, we need to recalculate phase symbols because new expressions were added
		// This MUST happen regardless of whether reordering is enabled
		phases = updatePhaseSymbols(phases, q.Find, inputSymbols)
	}

	// Validate that all find variables will be bound
	if err := p.validatePlan(phases, expressions, subqueries, findSymbols, inputSymbols); err != nil {
		return nil, err
	}

	return &QueryPlan{
		Query:  q,
		Phases: phases,
	}, nil
}

// separatePatterns splits patterns into data patterns, predicates, expressions, and subqueries
func (p *Planner) separatePatterns(patterns []query.Clause) ([]*query.DataPattern, []query.Predicate, []*query.Expression, []*query.SubqueryPattern) {
	var dataPatterns []*query.DataPattern
	var predicates []query.Predicate
	var expressions []*query.Expression
	var subqueries []*query.SubqueryPattern

	for _, pattern := range patterns {
		switch pat := pattern.(type) {
		case *query.DataPattern:
			dataPatterns = append(dataPatterns, pat)
		case query.Predicate:
			// New predicate interface types
			predicates = append(predicates, pat)
		case *query.Expression:
			// Use new Expression directly
			expressions = append(expressions, pat)
		case *query.SubqueryPattern:
			subqueries = append(subqueries, pat)
		}
	}

	return dataPatterns, predicates, expressions, subqueries
}
