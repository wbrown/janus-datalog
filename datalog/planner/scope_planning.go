package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A query's forms and the bodies its invocations run are planned at
// PlanQuery (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1): every
// scope of an or-join, an or-default-join, a not-join and a subquery is
// framed as the executor frames it and planned through PlanScope, and every
// invocation's bodies are planned as the executor's evaluation plans them,
// so a scope or a body no order runs is refused here on both planner paths,
// and the executor reads the plans the planner made. A form or an
// invocation inside a branch or a body is planned when that scope is, since
// PlanScope and planBody plan through Plan.

// planFormScopes walks each realized phase's clauses in the planner's order,
// carrying the symbols a relation group binds before each clause, the
// phase's relation inputs and what each clause provides, and the symbols the
// environment alone binds, the phase's scalar inputs, and plans each form's
// frames and each invocation's bodies. The scopes are planned under the rule
// set and the held invocations alone, the bindings the executor plans them
// under, so the cache serves one plan to both.
func (p *ClauseBasedPlanner) planFormScopes(phases []RealizedPhase, bindings query.Bindings, handler annotations.Handler) error {
	scopeBindings := query.Bindings{Rules: bindings.Rules, Held: bindings.Held}
	for _, phase := range phases {
		var carried, environment []query.Symbol
		for _, in := range phase.Query.In {
			switch spec := in.(type) {
			case query.ScalarInput:
				environment = append(environment, spec.Symbol)
			case query.RelationInput:
				carried = append(carried, spec.Symbols...)
			}
		}
		for _, clause := range phase.Query.Where {
			if err := p.planFormScope(clause, carried, environment, scopeBindings, handler); err != nil {
				return err
			}
			for _, sym := range query.ScopeOf(clause, bindings.Rules).Provides {
				if !query.ContainsSymbol(carried, sym) {
					carried = append(carried, sym)
				}
			}
		}
	}
	return nil
}

// planInvocation plans the bodies an invocation runs, as the executor's
// evaluation runs them. An invocation of scope/required reads the scope's
// run and runs no body. Under the rewrite's reading of a scope's order
// (query.HeldEvery) no body is planned, since the rewrite decides each site
// itself. Under a body a fixpoint runs (query.HeldByFixpoint) an invocation
// of a predicate without required positions reads the relation the fixpoint
// holds, and one of a predicate with required positions is derived at its
// clause. Under a query every invocation is derived at its clause. A
// derivation at the clause runs the set the invocation evaluates
// (EvaluatedSet) over the strata the predicate needs (NeededStrata), each
// stratum's bodies planned (PlanStratum), under the positions bound before
// the invocation: the constants it holds and the variables a group or the
// environment binds, as the executor reads them at the clause
// (BoundPositions). A required position none of them binds is the rule
// set's refusal, as it is at the clause.
func (p *ClauseBasedPlanner) planInvocation(inv *query.RuleInvocation, carried, environment []query.Symbol, bindings query.Bindings, handler annotations.Handler) error {
	rs := bindings.Rules
	if inv.Name == query.ScopeRequired || bindings.Held == query.HeldEvery {
		return nil
	}
	if bindings.Held == query.HeldByFixpoint && query.CountBound(rs.Required(inv.Name)) == 0 {
		return nil
	}
	bound := BoundPositions(inv, func(sym query.Symbol) bool {
		return query.ContainsSymbol(environment, sym) || query.ContainsSymbol(carried, sym)
	})
	if err := rs.CheckRequired(inv.Name, bound, nil); err != nil {
		return err
	}
	magic, err := EvaluatedSet(p, rs, inv.Name, bound, handler)
	if err != nil {
		return err
	}
	for _, stratum := range NeededStrata(magic.Rules, magic.Predicate) {
		if _, err := PlanStratum(p, magic.Rules, stratum, handler); err != nil {
			return err
		}
	}
	return nil
}

// planFormScope frames a form's branches as the executor frames them over
// the groups in hand and plans each frame. An or-join no header symbol of
// which a group binds runs each branch once over the environment's tuple,
// so its branches are framed over the required run alone; one some group
// binds is framed over the required run and the carried outputs, the
// outputs a group or the environment binds, and planned first over the
// required run alone when it carries any. An or-default-join and a not-join
// are framed over the required run. A subquery is framed over its inner
// symbols. The given symbols the subject carries are those standing for a
// symbol a group binds. An invocation's bodies are planned as planInvocation
// says. Any other clause plans nothing.
func (p *ClauseBasedPlanner) planFormScope(clause query.Clause, carried, environment []query.Symbol, bindings query.Bindings, handler annotations.Handler) error {
	var f Framing
	var branches [][]query.Clause
	switch c := clause.(type) {
	case *query.RuleInvocation:
		return p.planInvocation(c, carried, environment, bindings, handler)
	case *query.OrJoinClause:
		find := make([]query.FindElement, len(c.OutputVars))
		for i, sym := range c.OutputVars {
			find[i] = query.FindVariable{Symbol: sym}
		}
		f = Framing{Required: c.RequiredVars, Find: find, Given: c.RequiredVars}
		headerBound := false
		for _, sym := range append(append([]query.Symbol(nil), c.RequiredVars...), c.OutputVars...) {
			if query.ContainsSymbol(carried, sym) {
				headerBound = true
				break
			}
		}
		if headerBound {
			var carriedOutputs []query.Symbol
			for _, sym := range c.OutputVars {
				if query.ContainsSymbol(carried, sym) || query.ContainsSymbol(environment, sym) {
					carriedOutputs = append(carriedOutputs, sym)
				}
			}
			f.Given = GivenSymbols(c.RequiredVars, carriedOutputs, false)
			f.PlanOverRequired = len(carriedOutputs) > 0
		}
		f.StandsFor = f.Given
		branches = c.Branches
	case *query.OrDefaultJoinClause:
		find := make([]query.FindElement, len(c.OutputVars))
		for i, sym := range c.OutputVars {
			find[i] = query.FindVariable{Symbol: sym}
		}
		f = Framing{Required: c.RequiredVars, Find: find, Given: c.RequiredVars, StandsFor: c.RequiredVars}
		branches = c.Branches
	case *query.NotJoinClause:
		f = Framing{Required: c.RequiredVars, Given: c.RequiredVars, StandsFor: c.RequiredVars}
		branches = [][]query.Clause{c.Clauses}
	case *query.SubqueryPattern:
		frame, err := ReadSubqueryFrame(c)
		if err != nil {
			return err
		}
		f = Framing{Required: frame.Given, Find: frame.Find, Given: frame.Given, StandsFor: frame.StandsFor}
		branches = [][]query.Clause{frame.Clauses}
	default:
		return nil
	}
	f.Carried = f.CarriedBy(carried)
	for i, frame := range f.Branches(branches, bindings.Rules) {
		if frame.Check != nil {
			if _, err := p.PlanScope(*frame.Check, bindings, handler); err != nil {
				return fmt.Errorf("branch %d: %w", i+1, err)
			}
		}
		if _, err := p.PlanScope(frame.Scope, bindings, handler); err != nil {
			return fmt.Errorf("branch %d: %w", i+1, err)
		}
	}
	return nil
}
