package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A query's forms are planned at PlanQuery
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1): every scope of an
// or-join, an or-default-join, a not-join and a subquery is framed as the
// executor frames it and planned through PlanScope, so a branch no order
// runs is refused here on both planner paths, and the executor's framing
// reads the plans the planner made. A form inside a branch is planned when
// the branch's scope is, since PlanScope plans through Plan.

// planFormScopes walks each realized phase's clauses in the planner's order,
// carrying the symbols a relation group binds before each clause, the
// phase's relation inputs and what each clause provides, and the symbols the
// environment alone binds, the phase's scalar inputs, and plans each form's
// frames. The scopes are planned under the rule set alone, the bindings the
// executor's framing plans them under, so the cache serves one plan to both.
func (p *ClauseBasedPlanner) planFormScopes(phases []RealizedPhase, rules *query.RuleSet, handler annotations.Handler) error {
	scopeBindings := query.Bindings{Rules: rules}
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
			for _, sym := range query.ScopeOf(clause, rules).Provides {
				if !query.ContainsSymbol(carried, sym) {
					carried = append(carried, sym)
				}
			}
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
// symbol a group binds. Any other clause plans nothing.
func (p *ClauseBasedPlanner) planFormScope(clause query.Clause, carried, environment []query.Symbol, bindings query.Bindings, handler annotations.Handler) error {
	var f Framing
	var branches [][]query.Clause
	switch c := clause.(type) {
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
			for _, sym := range f.Given {
				if query.ContainsSymbol(carried, sym) {
					f.Carried = append(f.Carried, sym)
				}
			}
			f.PlanOverRequired = len(carriedOutputs) > 0
		}
		branches = c.Branches
	case *query.OrDefaultJoinClause:
		find := make([]query.FindElement, len(c.OutputVars))
		for i, sym := range c.OutputVars {
			find[i] = query.FindVariable{Symbol: sym}
		}
		f = Framing{Required: c.RequiredVars, Find: find, Given: c.RequiredVars}
		for _, sym := range c.RequiredVars {
			if query.ContainsSymbol(carried, sym) {
				f.Carried = append(f.Carried, sym)
			}
		}
		branches = c.Branches
	case *query.NotJoinClause:
		f = Framing{Required: c.RequiredVars, Given: c.RequiredVars}
		for _, sym := range c.RequiredVars {
			if query.ContainsSymbol(carried, sym) {
				f.Carried = append(f.Carried, sym)
			}
		}
		branches = [][]query.Clause{c.Clauses}
	case *query.SubqueryPattern:
		frame, err := ReadSubqueryFrame(c)
		if err != nil {
			return err
		}
		f = Framing{Required: frame.Given, Find: frame.Find, Given: frame.Given}
		for i, sym := range frame.Given {
			if standsFor := frame.StandsFor[i]; standsFor != nil && query.ContainsSymbol(carried, standsFor) {
				f.Carried = append(f.Carried, sym)
			}
		}
		branches = [][]query.Clause{frame.Clauses}
	default:
		return nil
	}
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
