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
// invocation's evaluation, the set it evaluates and the bodies of each
// stratum it needs, is planned as the executor's evaluation runs it and
// carried by the plan, so a scope or a body no order runs is refused here on
// both planner paths, and the executor reads the evaluations and the plans
// the planner made. A form or an invocation inside a branch or a body is
// planned when that scope is, since PlanScope and planBody plan through Plan.

// planFormScopes walks each realized phase's clauses in the planner's order,
// carrying the symbols a relation group binds before each clause, the
// phase's relation inputs and what each clause provides, and the symbols the
// environment alone binds, the phase's scalar inputs, and plans each form's
// frames, each invocation's evaluation, and the window scope of each
// windowable clause, a data pattern or an invocation derived at its clause
// that stands after a clause of the phase other than scope/required and
// shares a symbol with what is bound before it
// (docs/proposals/EXECUTION_PLANS_NOTHING_PROOF.md §1), returning the
// evaluations by clause for the plan to carry. The scopes are planned under
// the rule set and the held invocations alone, the bindings the executor
// plans them under, so the cache serves one plan to both.
func (p *ClauseBasedPlanner) planFormScopes(phases []RealizedPhase, bindings query.Bindings, handler annotations.Handler) (map[*query.RuleInvocation]*InvocationEvaluation, error) {
	scopeBindings := query.Bindings{Rules: bindings.Rules, Held: bindings.Held}
	evaluations := map[*query.RuleInvocation]*InvocationEvaluation{}
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
		// preceded reports whether a clause of the phase other than
		// scope/required has been walked: a group that reports no size holds
		// such a clause's result, so only a clause after one can window.
		preceded := false
		for _, clause := range phase.Query.Where {
			switch c := clause.(type) {
			case *query.RuleInvocation:
				planned, err := p.planInvocation(c, carried, environment, scopeBindings, handler)
				if err != nil {
					return nil, err
				}
				if planned != nil {
					evaluations[c] = planned
					if preceded {
						bound := append(append([]query.Symbol(nil), carried...), environment...)
						if err := p.planWindowScope(c.Variables(), c, bound, scopeBindings, handler); err != nil {
							return nil, err
						}
					}
				}
			case *query.DataPattern:
				if preceded {
					if err := p.planWindowScope(c.Symbols(), c, carried, scopeBindings, handler); err != nil {
						return nil, err
					}
				}
			default:
				if err := p.planFormScope(clause, carried, environment, scopeBindings, handler); err != nil {
					return nil, err
				}
			}
			if inv, ok := clause.(*query.RuleInvocation); !ok || inv.Name != query.ScopeRequired {
				preceded = true
			}
			for _, sym := range query.ScopeOf(clause, bindings.Rules).Provides {
				if !query.ContainsSymbol(carried, sym) {
					carried = append(carried, sym)
				}
			}
		}
	}
	return evaluations, nil
}

// planWindowScope frames a windowable clause as the executor's windowed
// route frames it, through WindowFraming over the symbols bound before the
// clause, and plans the frame, so a run reads it from the cache
// (docs/proposals/EXECUTION_PLANS_NOTHING_PROOF.md T1). A clause sharing no
// bound symbol frames over no required run and never windows, so it is
// planned nothing.
func (p *ClauseBasedPlanner) planWindowScope(symbols []query.Symbol, clause query.Clause, bound []query.Symbol, bindings query.Bindings, handler annotations.Handler) error {
	f := WindowFraming(symbols, bound)
	if len(f.Required) == 0 {
		return nil
	}
	f.Carried = f.CarriedBy(bound)
	for _, frame := range f.Branches([][]query.Clause{{clause}}, bindings.Rules) {
		if _, err := p.PlanScope(frame.Scope, bindings, handler); err != nil {
			return err
		}
	}
	return nil
}

// planInvocation plans the evaluation an invocation runs, as the executor's
// evaluation runs it, and returns it for the plan to carry; nil when the
// invocation evaluates nothing at its clause. An invocation of
// scope/required reads the scope's run and runs no body. Under the rewrite's
// reading of a scope's order (query.HeldEvery) no body is planned, since the
// rewrite decides each site itself. Under a body a fixpoint runs
// (query.HeldByFixpoint) an invocation of a predicate without required
// positions reads the relation the fixpoint holds, and one of a predicate
// with required positions is derived at its clause. Under a query every
// invocation is derived at its clause. A derivation at the clause is planned
// (PlanEvaluation) under the positions bound before the invocation: the
// constants it holds and the variables a group or the environment binds, as
// the executor reads them at the clause (BoundPositions). A required
// position none of them binds is the rule set's refusal, as it is at the
// clause.
func (p *ClauseBasedPlanner) planInvocation(inv *query.RuleInvocation, carried, environment []query.Symbol, bindings query.Bindings, handler annotations.Handler) (*InvocationEvaluation, error) {
	rs := bindings.Rules
	if inv.Name == query.ScopeRequired || bindings.Held == query.HeldEvery {
		return nil, nil
	}
	if bindings.Held == query.HeldByFixpoint && query.CountBound(rs.Required(inv.Name)) == 0 {
		return nil, nil
	}
	bound := BoundPositions(inv, func(sym query.Symbol) bool {
		return query.ContainsSymbol(environment, sym) || query.ContainsSymbol(carried, sym)
	})
	if err := rs.CheckRequired(inv.Name, bound, nil); err != nil {
		return nil, err
	}
	return PlanEvaluation(p, rs, inv.Name, bound, handler)
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
