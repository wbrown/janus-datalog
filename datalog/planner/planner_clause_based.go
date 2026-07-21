package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
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

// Plan creates an optimized query plan using the clause-based approach. The
// handler (may be nil) is request-scoped annotation state, threaded through
// planning rather than stored on the planner, so concurrent queries through a
// shared planner do not race on it.
func (p *ClauseBasedPlanner) Plan(q *query.Query, handler annotations.Handler) (*RealizedPlan, error) {
	// Check cache first
	if p.cache != nil {
		if cached, ok := p.cache.GetWithOptions(q, p.options); ok {
			return cached, nil
		}
	}

	// Plan with no initial bindings
	plan, err := p.PlanWithBindings(q, nil, handler)
	if err != nil {
		return nil, err
	}

	// Store in cache
	if p.cache != nil {
		p.cache.SetWithOptions(q, plan, p.options)
	}

	return plan, nil
}

// PlanWithBindings creates an optimized query plan with initial bindings. The
// handler (may be nil) is passed through to the algebra bridge rather than read
// from shared planner state.
func (p *ClauseBasedPlanner) PlanWithBindings(q *query.Query, initialBindings map[query.Symbol]bool, handler annotations.Handler) (*RealizedPlan, error) {
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

	// Collect every symbol required by final projection and finalization.
	findSymbols := terminalSymbols(q)
	findSymbolSet := make(map[query.Symbol]bool, len(findSymbols))
	for _, symbol := range findSymbols {
		findSymbolSet[symbol] = true
	}

	// Retain effective :order-by symbols through phasing so the last phase's
	// relation still carries them: the executor sorts the assembled result,
	// then strips it back to the declared :find shape. Without this, a sort
	// key bound only in :where is projected away at the first Keep boundary
	// and the sort has nothing to resolve against. (Empty for aggregate
	// queries and for constant-input keys — see query.RetainedSortSymbols.)
	retainedSort := query.RetainedSortSymbols(q)
	for _, sym := range retainedSort {
		if !findSymbolSet[sym] {
			findSymbols = append(findSymbols, sym)
			findSymbolSet[sym] = true
		}
	}

	// Stage C Architecture: Optimize FIRST, then phase ONCE

	// Step 1: Start with the clause list from the query
	clauses := q.Where

	// Step 2: Optimize Datalog through the algebra IR, then return to Datalog
	// before physical phase planning.
	if p.options.EnableAlgebraOptimizer {
		options := p.options
		if len(initialBindings) > 0 {
			options.EnableJoinProjectInsertion = false
		}
		optimized, err := optimizeViaAlgebra(q, options, handler)
		if err != nil {
			return nil, fmt.Errorf("algebra optimization failed: %w", err)
		}
		clauses = optimized.Where
	}

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

		// Preserve constant-only scalar bindings as ScalarInput entries in the
		// phase Datalog. Other available symbols flow through RelationInput.
		var phaseConstBindable []query.Symbol
		if len(constantBindable) > 0 {
			phaseSyms := make(map[query.Symbol]bool)
			for _, clause := range cp.Clauses {
				for _, sym := range query.ScopeOf(clause).Correlates {
					phaseSyms[sym] = true
				}
			}
			for _, sym := range constantBindable {
				if phaseSyms[sym] {
					phaseConstBindable = append(phaseConstBindable, sym)
				}
			}
		}

		phaseAvailable := cp.Available
		if i > 0 {
			phaseAvailable = append([]query.Symbol(nil), realizedPhases[i-1].Keep...)
			for _, symbol := range phaseConstBindable {
				if !query.ContainsSymbol(phaseAvailable, symbol) {
					phaseAvailable = append(phaseAvailable, symbol)
				}
			}
		}

		phaseFind := buildFindClause(keep, q.Find, isLastPhase, retainedSort)

		// Build the query fragment for this phase.
		phaseQuery := &query.Query{
			Find:  phaseFind,
			In:    buildInClause(phaseAvailable, phaseConstBindable),
			Where: cp.Clauses,
		}
		phaseAvailable = physicalInputSymbols(phaseQuery.In)
		if len(clausePhases) == 1 &&
			len(cp.Clauses) == 1 &&
			len(phaseAvailable) == 0 &&
			len(q.OrderBy) > 0 &&
			q.Limit != nil {
			_, isPattern := cp.Clauses[0].(*query.DataPattern)
			hasAggregate := false
			for _, element := range q.Find {
				if element.IsAggregate() {
					hasAggregate = true
					break
				}
			}
			if isPattern && !hasAggregate {
				phaseQuery.OrderBy = append([]query.OrderByClause(nil), q.OrderBy...)
				limit := *q.Limit
				phaseQuery.Limit = &limit
			}
		}

		phaseProvides := physicalFindSymbols(phaseFind)
		var phaseKeep []query.Symbol
		if !isLastPhase {
			phaseKeep = append([]query.Symbol(nil), phaseProvides...)
		}

		realizedPhases[i] = RealizedPhase{
			Query:     phaseQuery,
			Available: phaseAvailable,
			Provides:  phaseProvides,
			Keep:      phaseKeep,
		}

		// Populate explain fields for detailed plan output
		// Include both phase-available symbols and input symbols
		availableSet := make(map[query.Symbol]bool)
		for sym := range inputSymbols {
			availableSet[sym] = true
		}
		for _, sym := range phaseAvailable {
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
func buildFindClause(provides []query.Symbol, originalFind []query.FindElement, isLastPhase bool, retainedSort []query.Symbol) []query.FindElement {
	if isLastPhase {
		// Last phase uses the original find clause, plus any retained
		// :order-by symbols so they survive the final projection for the
		// executor's sort; the executor strips them after sorting.
		if len(retainedSort) == 0 {
			return originalFind
		}
		find := make([]query.FindElement, len(originalFind), len(originalFind)+len(retainedSort))
		copy(find, originalFind)
		for _, sym := range retainedSort {
			find = append(find, query.FindVariable{Symbol: sym})
		}
		return find
	}

	// Intermediate phases find all symbols they provide
	findElems := make([]query.FindElement, len(provides))
	for i, sym := range provides {
		findElems[i] = query.FindVariable{Symbol: sym}
	}
	return findElems
}

// buildInClause preserves the binding modes required by a phase. Scalar inputs
// are execution-environment constants; all other available symbols flow through
// the phase relation.
func buildInClause(available, scalarInputs []query.Symbol) []query.InputSpec {
	inClause := []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}}
	scalarSet := make(map[query.Symbol]bool, len(scalarInputs))
	for _, sym := range scalarInputs {
		scalarSet[sym] = true
		inClause = append(inClause, query.ScalarInput{Symbol: sym})
	}

	relationSymbols := make([]query.Symbol, 0, len(available))
	for _, sym := range available {
		if !scalarSet[sym] {
			relationSymbols = append(relationSymbols, sym)
		}
	}
	if len(relationSymbols) > 0 {
		inClause = append(inClause, query.RelationInput{Symbols: relationSymbols})
	}
	return inClause
}
