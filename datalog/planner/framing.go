package planner

import (
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A form's branches are framed by one computation, from the header and the
// symbols bound before the form
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1 and §4.3): the
// planner frames every form of a query at PlanQuery and plans each frame,
// and the executor frames the same form from the groups in hand and reads
// the plans the planner made.

// Framing is how a form frames its branches: the header's required run and
// its find beyond it, the outputs as variables for a branch form and the
// inner :find for a subquery; what a branch is given, the required run, and
// under the union the carried outputs after it; per given symbol, the symbol
// of the enclosing scope it stands for, by which a relation group is read
// for it, a subquery's inner :in symbol standing for the variable its call
// passes at that place, nil for a constant argument, and a branch form's
// symbol standing for itself; the given symbols a relation group binds
// before the form, the rest being the environment's alone; and whether a
// branch holding no invocation is first planned over the required run alone,
// the union's check that the language's scope admits the branch before its
// seek binds the carried outputs into it.
type Framing struct {
	Required         []query.Symbol
	Find             []query.FindElement
	Given            []query.Symbol
	StandsFor        []query.Symbol
	Carried          []query.Symbol
	PlanOverRequired bool
}

// CarriedBy is the given symbols a relation group binds before the form:
// those standing for a symbol among bound, in the given order. A given
// symbol standing for nothing is the environment's, whatever bound holds.
func (f Framing) CarriedBy(bound []query.Symbol) []query.Symbol {
	var carried []query.Symbol
	for i, sym := range f.Given {
		if standsFor := f.StandsFor[i]; standsFor != nil && query.ContainsSymbol(bound, standsFor) {
			carried = append(carried, sym)
		}
	}
	return carried
}

// BranchFrame is one branch's scope under its form's framing: the scope the
// branch runs over; whether the branch reads nothing of what it is given, so
// it runs once over the environment's tuple or the unit relation; and, under
// the union with carried outputs, the scope over the required run alone the
// branch is planned under first, nil otherwise.
type BranchFrame struct {
	Scope        Scope
	ReadsNothing bool
	Check        *Scope
}

// Branches frames each branch as a scope under the framing. A branch holding
// an invocation is framed over the required run, so the invocation is seeded
// by the header; a branch reading nothing of what it is given is framed over
// the required symbols the environment alone binds, so its one run carries
// the input's values; every other branch is framed over the given symbols.
// The scope's find is the given symbols the branch provides beyond its frame,
// as variables, then the framing's find. The symbols stand in the header's
// order, so a frame is a function of the text and of which header symbols
// are bound before the form, and the planner and the executor frame alike.
func (f Framing) Branches(branches [][]query.Clause, rules *query.RuleSet) []BranchFrame {
	var fromEnvironment []query.Symbol
	for _, sym := range f.Required {
		if !query.ContainsSymbol(f.Carried, sym) {
			fromEnvironment = append(fromEnvironment, sym)
		}
	}
	frames := make([]BranchFrame, len(branches))
	for i, branch := range branches {
		var frame BranchFrame
		switch {
		case HoldsInvocation(branch):
			frame.Scope.Required = f.Required
		case len(BranchReads(branch, f.Given, rules)) == 0:
			frame.Scope.Required = fromEnvironment
			frame.ReadsNothing = true
		default:
			if f.PlanOverRequired {
				frame.Check = &Scope{Required: f.Required, Find: f.Find, Clauses: branch}
			}
			frame.Scope.Required = f.Given
		}
		for _, sym := range BranchProvides(branch, rules) {
			if query.ContainsSymbol(f.Given, sym) && !query.ContainsSymbol(frame.Scope.Required, sym) {
				frame.Scope.Find = append(frame.Scope.Find, query.FindVariable{Symbol: sym})
			}
		}
		outputs := physicalFindSymbols(f.Find)
		for position, elem := range f.Find {
			if sym := outputs[position]; !query.ContainsSymbol(frame.Scope.Required, sym) && !query.ContainsSymbol(frame.Scope.Outputs(), sym) {
				frame.Scope.Find = append(frame.Scope.Find, elem)
			}
		}
		frame.Scope.Clauses = branch
		frames[i] = frame
	}
	return frames
}

// SubqueryFrame is a subquery call as its text frames the inner query: the
// inner symbols, one per call position that binds a symbol, a collection
// input's as the holder ScopeInput mints for its position; the clauses, the
// opening identity binding each collection input's members from its holder,
// then the inner clauses under the sources the call passes; the
// substitution of each inner symbol and each inner :find element's symbol
// to the enclosing name standing for it; per inner symbol the enclosing
// variable the call passes at its place, nil for a constant argument; and
// per call position the inner symbol it binds, nil where it binds none.
type SubqueryFrame struct {
	Given     []query.Symbol
	Find      []query.FindElement
	Clauses   []query.Clause
	Subst     query.Substitution
	StandsFor []query.Symbol
	Inner     []query.Symbol
}

// ReadSubqueryFrame reads a subquery call's frame from its text, once, for
// the planner's walk and the executor's framing alike
// (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.4).
func ReadSubqueryFrame(subq *query.SubqueryPattern) (SubqueryFrame, error) {
	positions := query.InputPositions(subq.Query.In)
	frame := SubqueryFrame{Find: subq.Query.Find, Inner: make([]query.Symbol, len(positions))}
	var from, to []query.Symbol
	var opening []query.Clause
	for position, at := range positions {
		sym := at.Symbol
		if sym == nil {
			continue
		}
		if collection, ok := at.Spec.(query.CollectionInput); ok {
			sym = query.ScopeInput(position)
			opening = append(opening, &query.Expression{
				Function: query.IdentityFunction{Arg: query.VariableTerm{Symbol: sym}},
				Binding:  query.CollectionBinding{Variable: collection.Symbol},
			})
		}
		frame.Inner[position] = sym
		frame.Given = append(frame.Given, sym)
		var standsFor query.Symbol
		if arg, ok := subq.Inputs[position].(query.Variable); ok {
			standsFor = arg.Name
			from = append(from, sym)
			to = append(to, arg.Name)
		}
		frame.StandsFor = append(frame.StandsFor, standsFor)
	}
	from = append(from, physicalFindSymbols(subq.Query.Find)...)
	to = append(to, subq.Binding.BoundVariables()...)
	subst, err := query.SubstitutionOf(from, to)
	if err != nil {
		return SubqueryFrame{}, err
	}
	frame.Subst = subst
	clauses := query.RenameSources(subq.Query.Where, query.CallSources(subq))
	frame.Clauses = append(append([]query.Clause(nil), opening...), clauses...)
	return frame, nil
}

// HoldsInvocation reports whether the clauses invoke a predicate at any
// depth, a subquery's inner query included.
func HoldsInvocation(clauses []query.Clause) bool {
	found := false
	query.WalkClauses(clauses, func(clause query.Clause) bool {
		if found {
			return false
		}
		switch typed := clause.(type) {
		case *query.RuleInvocation:
			found = true
			return false
		case *query.SubqueryPattern:
			if typed.Query != nil && HoldsInvocation(typed.Query.Where) {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

// BranchProvides is the symbols a branch's clauses provide, read under the
// rule set, in first-appearance order.
func BranchProvides(clauses []query.Clause, rules *query.RuleSet) []query.Symbol {
	var provided []query.Symbol
	for _, clause := range clauses {
		for _, sym := range query.ScopeOf(clause, rules).Provides {
			if !query.ContainsSymbol(provided, sym) {
				provided = append(provided, sym)
			}
		}
	}
	return provided
}

// GivenSymbols is what a branch is given: the required run under the
// fallback, and the required run with the carried outputs under the union.
func GivenSymbols(required, carried []query.Symbol, shortCircuit bool) []query.Symbol {
	given := append([]query.Symbol(nil), required...)
	if shortCircuit {
		return given
	}
	for _, output := range carried {
		if !query.ContainsSymbol(given, output) {
			given = append(given, output)
		}
	}
	return given
}

// WindowFraming is how a data pattern or an invocation over a streaming
// group frames its one branch, from the clause's symbols and the symbols
// bound before it (docs/proposals/EXECUTION_PLANS_NOTHING_PROOF.md §1): the
// required run is the clause's symbols among the bound, in the clause's
// order, and the find is the rest, each as a variable; the given symbols are
// the required run, each standing for itself. The planner's walk at
// PlanQuery and the executor's two routes frame through it alike, so the
// scope the walk plans is the one a run reads.
func WindowFraming(symbols, bound []query.Symbol) Framing {
	var required []query.Symbol
	var find []query.FindElement
	for _, sym := range symbols {
		if query.ContainsSymbol(bound, sym) {
			required = append(required, sym)
		} else {
			find = append(find, query.FindVariable{Symbol: sym})
		}
	}
	return Framing{Required: required, Find: find, Given: required, StandsFor: required}
}

// BranchReads is the given symbols a branch reads, in the given order: a
// symbol a data pattern mentions, or one any clause consumes, whether or not
// another clause of the branch provides it, since a clause binding a run
// symbol unifies with it. A branch whose clauses each provide the symbol
// another consumes reads both: framed over no run, neither would be bound
// and the planner would refuse the branch. A symbol the branch only provides
// is not read, so a lone ground or an uncorrelated subquery is built once
// over the environment's tuple and joined on what it provides.
func BranchReads(branch []query.Clause, given []query.Symbol, rules *query.RuleSet) []query.Symbol {
	var consumed, mentioned []query.Symbol
	for _, clause := range branch {
		clauseScope := query.ScopeOf(clause, rules)
		consumed = append(consumed, clauseScope.Correlates...)
		if _, isPattern := clause.(*query.DataPattern); isPattern {
			mentioned = append(mentioned, clauseScope.Provides...)
		}
	}
	var reads []query.Symbol
	for _, sym := range given {
		if query.ContainsSymbol(mentioned, sym) || query.ContainsSymbol(consumed, sym) {
			reads = append(reads, sym)
		}
	}
	return reads
}
