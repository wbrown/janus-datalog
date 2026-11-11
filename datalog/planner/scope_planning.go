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

// planClauses walks each realized phase's clauses in the planner's order,
// carrying the symbols a relation group binds before each clause, the
// phase's relation inputs and what each clause provides, and the symbols the
// environment alone binds, the phase's scalar inputs, and plans each form's
// frames, each invocation's evaluation, and the window scope of each clause
// it windows, the frame a run reads from the cache
// (docs/proposals/EXECUTION_PLANS_NOTHING_PROOF.md T1), returning them as the
// clausePlans the plan carries. A data pattern or an invocation evaluated at
// its clause is read in windows when a group sharing one of its variables is
// one the phase produces (phaseGroups). The scopes are planned under the rule
// set and the held invocations alone, the bindings the executor plans them
// under, so the cache serves one plan to both.
func (p *ClauseBasedPlanner) planClauses(phases []RealizedPhase, bindings query.Bindings, handler annotations.Handler) (clausePlans, error) {
	scopeBindings := query.Bindings{Rules: bindings.Rules, Held: bindings.Held}
	plans := clausePlans{
		evaluations: map[*query.RuleInvocation]*InvocationEvaluation{},
		frames:      map[query.Clause][]FramePlan{},
		windows:     map[query.Clause]bool{},
	}
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
		groups := phaseGroups{of: map[query.Symbol]*phaseGroup{}}
		groups.join(query.InputSymbols(query.BindingInputs(phase.Query.In)), true)
		for _, clause := range phase.Query.Where {
			// given reports whether the clause reads a relation the scope is
			// given: an invocation the plan evaluates nothing for reads the
			// relation the scope's bindings hold, the required run or a
			// predicate the fixpoint holds.
			given := false
			switch c := clause.(type) {
			case *query.RuleInvocation:
				planned, err := p.planInvocation(c, carried, environment, scopeBindings, handler)
				if err != nil {
					return clausePlans{}, err
				}
				given = planned == nil
				if planned != nil {
					plans.evaluations[c] = planned
					if groups.produced(c.Variables()) {
						plans.windows[c] = true
						bound := append(append([]query.Symbol(nil), carried...), environment...)
						if err := p.planWindowScope(c.Variables(), c, bound, scopeBindings, handler); err != nil {
							return clausePlans{}, err
						}
					}
				}
			case *query.DataPattern:
				if groups.produced(c.Symbols()) {
					plans.windows[c] = true
					if err := p.planWindowScope(c.Symbols(), c, carried, scopeBindings, handler); err != nil {
						return clausePlans{}, err
					}
				}
			default:
				planned, err := p.planFormScope(clause, carried, environment, scopeBindings, handler)
				if err != nil {
					return clausePlans{}, err
				}
				if len(planned) > 0 {
					plans.frames[clause] = planned
				}
			}
			scope := query.ScopeOf(clause, bindings.Rules)
			groups.join(append(append([]query.Symbol(nil), scope.Correlates...), scope.Provides...), given)
			for _, sym := range scope.Provides {
				if !query.ContainsSymbol(carried, sym) {
					carried = append(carried, sym)
				}
			}
		}
	}
	return plans, nil
}

// clausePlans is what a plan states about its clauses beyond their phases, by
// clause: each invocation's evaluation, each form's frame plans in branch
// order, and each clause the executor reads in windows.
type clausePlans struct {
	evaluations map[*query.RuleInvocation]*InvocationEvaluation
	frames      map[query.Clause][]FramePlan
	windows     map[query.Clause]bool
}

// phaseGroups is the relation groups a phase's clauses build as the executor
// collapses them, by the symbols each holds, and whether the scope is given
// each or produces it. A group the scope is given holds only relations it
// receives: its binding inputs, as which a later phase receives its
// predecessor's groups, or the relation an invocation its bindings hold
// reads. A clause that computes its result, or joins a result with a group,
// makes a group the scope produces.
type phaseGroups struct {
	of map[query.Symbol]*phaseGroup
}

type phaseGroup struct {
	symbols []query.Symbol
	given   bool
}

// join merges the groups holding any of symbols with a relation over them.
// The result is given when given is set and it merges no group.
func (g phaseGroups) join(symbols []query.Symbol, given bool) {
	if len(symbols) == 0 {
		return
	}
	merged := &phaseGroup{given: given}
	seen := map[*phaseGroup]bool{}
	for _, sym := range symbols {
		if group := g.of[sym]; group != nil && !seen[group] {
			seen[group] = true
			merged.given = false
			merged.symbols = append(merged.symbols, group.symbols...)
		}
	}
	for _, sym := range symbols {
		if !query.ContainsSymbol(merged.symbols, sym) {
			merged.symbols = append(merged.symbols, sym)
		}
	}
	for _, sym := range merged.symbols {
		g.of[sym] = merged
	}
}

// produced reports whether a group holding one of symbols is one the scope
// produces: the executor reads a clause sharing a variable with such a group
// in windows of it.
func (g phaseGroups) produced(symbols []query.Symbol) bool {
	for _, sym := range symbols {
		if group := g.of[sym]; group != nil && !group.given {
			return true
		}
	}
	return false
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
// symbol a group binds. Any other clause plans nothing. It returns the
// frames in branch order, each frame's plan beside whether its branch reads
// nothing of what it is given; a check scope, planned only so its refusal
// surfaces, is no frame.
func (p *ClauseBasedPlanner) planFormScope(clause query.Clause, carried, environment []query.Symbol, bindings query.Bindings, handler annotations.Handler) ([]FramePlan, error) {
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
			return nil, err
		}
		f = Framing{Required: frame.Given, Find: frame.Find, Given: frame.Given, StandsFor: frame.StandsFor}
		branches = [][]query.Clause{frame.Clauses}
	default:
		return nil, nil
	}
	f.Carried = f.CarriedBy(carried)
	var planned []FramePlan
	for i, frame := range f.Branches(branches, bindings.Rules) {
		if frame.Check != nil {
			if _, err := p.PlanScope(*frame.Check, bindings, handler); err != nil {
				return nil, fmt.Errorf("branch %d: %w", i+1, err)
			}
		}
		plan, err := p.PlanScope(frame.Scope, bindings, handler)
		if err != nil {
			return nil, fmt.Errorf("branch %d: %w", i+1, err)
		}
		planned = append(planned, FramePlan{Plan: plan, ReadsNothing: frame.ReadsNothing})
	}
	return planned, nil
}
