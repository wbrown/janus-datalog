package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A Scope is a nested form's body as the planner frames it: a header, the
// required run and the find beyond it, over a clause list. Each branch of an
// or-join or an or-default-join is one scope under the form's header, its
// find the outputs as variables; a not-join's body is one under its
// all-required header; a subquery's inner query is one under its data
// inputs, its find the inner :find as written, aggregates included; and a
// rule body under a bound invocation is one under the head's bound
// positions, its find the free ones. A scope reads the header and nothing
// else of the enclosing scope.
type Scope struct {
	Required []query.Symbol
	Find     []query.FindElement
	Clauses  []query.Clause
}

// Outputs is the symbols the scope's find binds beyond the required run, a
// variable's own and an aggregate's by its display form.
func (s Scope) Outputs() []query.Symbol {
	return physicalFindSymbols(s.Find)
}

// Header is the required run followed by the outputs: the symbols the
// scope's query finds and its result carries.
func (s Scope) Header() []query.Symbol {
	outputs := s.Outputs()
	header := make([]query.Symbol, 0, len(s.Required)+len(outputs))
	header = append(header, s.Required...)
	return append(header, outputs...)
}

// Query frames the scope as the query the planner plans: it finds the
// required run as variables followed by the scope's find, declares $ and %,
// and opens with the invocation of scope/required over the required run,
// followed by the scope's clauses. The planner leads with the invocation,
// since scope/required is a seed-restricted predicate, and the text carries
// the binding, so the plan caches under it.
func (s Scope) Query() *query.Query {
	find := make([]query.FindElement, 0, len(s.Required)+len(s.Find))
	for _, sym := range s.Required {
		find = append(find, query.FindVariable{Symbol: sym})
	}
	find = append(find, s.Find...)
	args := make([]query.PatternElement, len(s.Required))
	for i, sym := range s.Required {
		args[i] = query.Variable{Name: sym}
	}
	where := make([]query.Clause, 0, len(s.Clauses)+1)
	where = append(where, &query.RuleInvocation{Name: query.ScopeRequired, Args: args})
	where = append(where, s.Clauses...)
	return &query.Query{
		Find:  find,
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
		Where: where,
	}
}

// PlanScope plans a scope's query under the caller's bindings. The plan is
// served from the cache under the query's text, so a scope is planned once
// however many runs read it.
func (p *ClauseBasedPlanner) PlanScope(s Scope, bindings query.Bindings, handler annotations.Handler) (*RealizedPlan, error) {
	plan, err := p.Plan(s.Query(), bindings, handler)
	if err != nil {
		return nil, fmt.Errorf("planning the scope: %w", err)
	}
	return plan, nil
}

// plannedClauses is a scope's clauses as a reader that runs nothing, the
// magic rewrite, takes them from the scope's plan: the phases' clauses in
// phase order, less the scope/required invocation that opens them.
func plannedClauses(plan *RealizedPlan) []query.Clause {
	var clauses []query.Clause
	for _, phase := range plan.Phases {
		for _, clause := range phase.Query.Where {
			if inv, ok := clause.(*query.RuleInvocation); ok && inv.Name == query.ScopeRequired {
				continue
			}
			clauses = append(clauses, clause)
		}
	}
	return clauses
}
