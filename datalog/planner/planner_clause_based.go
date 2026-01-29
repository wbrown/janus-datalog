package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
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
			return cached, nil
		}
	}

	// Plan with no initial bindings
	plan, err := p.PlanWithBindings(q, nil)
	if err != nil {
		return nil, err
	}

	// Store in cache
	if p.cache != nil {
		p.cache.SetWithOptions(q, plan, p.options)
	}

	return plan, nil
}

// PlanWithBindings creates an optimized query plan with initial bindings
func (p *ClauseBasedPlanner) PlanWithBindings(q *query.Query, initialBindings map[query.Symbol]bool) (*RealizedPlan, error) {
	// Extract input symbols from :in clause, tracking scalar inputs separately
	inputSymbols := make(map[query.Symbol]bool)
	var scalarInputs []query.Symbol
	for _, input := range q.In {
		switch inp := input.(type) {
		case query.ScalarInput:
			inputSymbols[inp.Symbol] = true
			scalarInputs = append(scalarInputs, inp.Symbol)
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
	// Semantic rewriting: [(year ?t) ?py] + [(= ?py 2025)] -> [(>= ?t start)] + [(< ?t end)]
	rewriter := NewSemanticRewriter(p.options)
	clauses = rewriter.Rewrite(clauses)

	// TODO: Implement decorrelation as pure clause transformation
	// (Conditional aggregate rewriting is buggy and moved to experimental/)

	// Step 2b: Detect constant-bindable scalar inputs
	// Scalars that only appear in predicates/expressions (not data patterns)
	// can be resolved as constants rather than creating separate relation groups.
	constantBindable := findConstantBindableScalars(scalarInputs, clauses)

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

		// Store constant-bindable scalars for phases that reference them
		if len(constantBindable) > 0 {
			// Check which constant-bindable symbols this phase's clauses use
			phaseSyms := make(map[query.Symbol]bool)
			for _, clause := range cp.Clauses {
				cs := extractClauseSymbols(clause)
				for _, sym := range cs.Requires {
					phaseSyms[sym] = true
				}
			}
			var phaseConstBindable []query.Symbol
			for _, sym := range constantBindable {
				if phaseSyms[sym] {
					phaseConstBindable = append(phaseConstBindable, sym)
				}
			}
			if len(phaseConstBindable) > 0 {
				realizedPhases[i].Metadata["constant_bindable_inputs"] = phaseConstBindable
			}
		}

		// Populate explain fields for detailed plan output
		// Include both phase-available symbols and input symbols
		availableSet := make(map[query.Symbol]bool)
		for sym := range inputSymbols {
			availableSet[sym] = true
		}
		for _, sym := range cp.Available {
			availableSet[sym] = true
		}
		analyzeClausesForExplain(&realizedPhases[i], cp.Clauses, availableSet, p.stats)
	}

	return &RealizedPlan{
		Query:  q,
		Phases: realizedPhases,
	}, nil
}

// Note: Semantic rewriting is now implemented as pure clause transformation.
// Decorrelation (conditional aggregate rewriting) is deferred - the implementation
// was buggy and has been moved to experimental/. The key architectural innovation
// is the greedy phasing algorithm and optimize-first flow.

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
		return []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}}
	}

	// Create relation input with all available symbols
	inClause := []query.InputSpec{
		query.DatabaseInput{Name: datalog.SymDollar},
		query.RelationInput{Symbols: available},
	}
	return inClause
}
