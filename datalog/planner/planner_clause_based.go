package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/query"
)

// ClauseBasedPlanner is the new planner that operates on clauses directly
// This implements the "optimize-first, phase-once" architecture (Stage C)
type ClauseBasedPlanner struct {
	stats   *Statistics
	options PlannerOptions
	cache   *PlanCache
}

// NewClauseBasedPlanner creates a new clause-based planner
func NewClauseBasedPlanner(stats *Statistics, options PlannerOptions) *ClauseBasedPlanner {
	if stats == nil {
		stats = &Statistics{
			AttributeCardinality: make(map[string]int),
			EntityCount:          1000000,
		}
	}
	return &ClauseBasedPlanner{
		stats:   stats,
		options: options,
		cache:   options.Cache,
	}
}

// Plan creates an optimized query plan using the clause-based approach
func (p *ClauseBasedPlanner) Plan(q *query.Query) (*RealizedPlan, error) {
	// Check cache first
	if p.cache != nil {
		if cached, ok := p.cache.GetWithOptions(q, p.options); ok {
			return cached.Realize(), nil
		}
	}

	// Plan with no initial bindings
	plan, err := p.PlanWithBindings(q, nil)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

// PlanWithBindings creates an optimized query plan with initial bindings
func (p *ClauseBasedPlanner) PlanWithBindings(q *query.Query, initialBindings map[query.Symbol]bool) (*RealizedPlan, error) {
	// Extract input symbols from :in clause
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

	// Merge with initial bindings
	if initialBindings != nil {
		for sym := range initialBindings {
			inputSymbols[sym] = true
		}
	}

	// Extract find symbols
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
			if !findSymbolSet[e.Arg] {
				findSymbols = append(findSymbols, e.Arg)
				findSymbolSet[e.Arg] = true
			}
		}
	}

	// Stage C Architecture: Optimize FIRST, then phase ONCE

	// Step 1: Start with the clause list from the query
	clauses := q.Where

	// Step 2: Apply optimizations as pure clause transformations
	// TODO: Implement semantic rewriting as pure clause transformation
	// TODO: Implement decorrelation as pure clause transformation
	// For now, these complex optimizations are disabled in the clause-based planner
	// The architectural difference (optimize-first, greedy phasing) is the key change

	// Step 3: Phase the optimized clause list ONCE using greedy algorithm
	clausePhases, err := createPhasesGreedy(clauses, findSymbols, inputSymbols)
	if err != nil {
		return nil, fmt.Errorf("phasing failed: %w", err)
	}

	// Step 4: Convert ClausePhases to RealizedPhases
	realizedPhases := make([]RealizedPhase, len(clausePhases))

	for i, cp := range clausePhases {
		isLastPhase := (i == len(clausePhases)-1)

		// Compute remaining clauses after this phase
		var remainingClauses []query.Clause
		if !isLastPhase {
			for j := i + 1; j < len(clausePhases); j++ {
				remainingClauses = append(remainingClauses, clausePhases[j].Clauses...)
			}
		}

		// Compute keep symbols
		keep := computeKeepSymbols(cp, remainingClauses, findSymbols)

		// Build the query fragment for this phase
		phaseQuery := &query.Query{
			Find:  buildFindClause(cp.Provides, q.Find, isLastPhase),
			In:    buildInClause(cp.Available),
			Where: cp.Clauses,
		}

		realizedPhases[i] = RealizedPhase{
			Query:     phaseQuery,
			Available: cp.Available,
			Provides:  cp.Provides,
			Keep:      keep,
			Metadata:  make(map[string]interface{}),
		}
	}

	return &RealizedPlan{
		Query:  q,
		Phases: realizedPhases,
	}, nil
}

// Note: Semantic rewriting and decorrelation are complex optimizations that currently
// operate on Phase structures. Converting them to pure clause transformations is future work.
// The key architectural innovation is the greedy phasing algorithm and optimize-first flow.

// buildFindClause constructs the :find clause for a phase
func buildFindClause(provides []query.Symbol, originalFind []query.FindElement, isLastPhase bool) []query.FindElement {
	if isLastPhase {
		// Last phase uses the original find clause
		return originalFind
	}

	// Intermediate phases find all symbols they provide
	findElems := make([]query.FindElement, len(provides))
	for i, sym := range provides {
		findElems[i] = query.FindVariable{Symbol: sym}
	}
	return findElems
}

// buildInClause constructs the :in clause for a phase
func buildInClause(available []query.Symbol) []query.InputSpec {
	if len(available) == 0 {
		// First phase with no inputs - just database
		return []query.InputSpec{query.DatabaseInput{}}
	}

	// Create relation input with all available symbols
	inClause := []query.InputSpec{
		query.DatabaseInput{},
		query.RelationInput{Symbols: available},
	}
	return inClause
}
