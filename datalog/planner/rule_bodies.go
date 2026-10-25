package planner

import (
	"fmt"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/annotations"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The bodies an invocation runs are computed here, beside the rewrite, and
// read by the executor's evaluation and by the planner's walk at PlanQuery
// alike (docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md §4.1): the set an
// invocation evaluates, the strata that set runs for the predicate, and the
// bodies each stratum runs, each planned as a query of its own under the
// plan cache. So every body a query reaches is planned at PlanQuery, and
// Query, Explain and ExplainAlgebra refuse a body no order runs alike.

// BoundPositions marks the positions of an invocation the clause binds: a
// constant, or a variable binds reports bound, a repeated variable at its
// first position only.
func BoundPositions(inv *query.RuleInvocation, binds func(query.Symbol) bool) []bool {
	bound := make([]bool, len(inv.Args))
	seen := map[query.Symbol]bool{}
	for i, arg := range inv.Args {
		switch a := arg.(type) {
		case query.Constant, query.VectorConstant:
			bound[i] = true
		case query.Variable:
			if seen[a.Name] {
				continue
			}
			seen[a.Name] = true
			bound[i] = binds(a.Name)
		}
	}
	return bound
}

// EvaluatedSet is the set an invocation evaluates: the rule set as given,
// deriving the predicate whole, when nothing binds its arguments and no body
// the predicate needs invokes a predicate with required positions, and the
// magic-set rewrite otherwise, seeded by the bound arguments when there are
// any, so the fixpoint derives only the tuples those arguments reach. A free
// invocation reaching a predicate with required positions is rewritten with
// its head all free, so that predicate is adorned from the clauses before
// each of its invocations and derived in the rewritten set, never drawn
// whole into an evaluation without its bindings and never derived at its
// clause on every run of the body invoking it
// (NESTED_SCOPES_AS_QUERIES.md §5.5). The rewrite reads each scope in the
// planner's order, and every rewrite is accepted (Rewrite). A rewritten set
// is a set other than the one given.
func EvaluatedSet(p QueryPlanner, rs *query.RuleSet, name query.Symbol, bound []bool, handler annotations.Handler) (*Magic, error) {
	if query.CountBound(bound) == 0 && !rs.ReachesRequired(name) {
		return &Magic{Rules: rs, Predicate: name}, nil
	}
	return Rewrite(p, rs, name, bound, handler)
}

// NeededStrata is the strata an evaluation of the predicate runs, in the
// set's order: each stratum holding a predicate the invoked one depends on.
func NeededStrata(rs *query.RuleSet, invoked query.Symbol) []query.Stratum {
	needed := neededPredicates(rs, invoked)
	var strata []query.Stratum
	for _, stratum := range rs.Strata() {
		for _, p := range stratum.Predicates {
			if needed[p] {
				strata = append(strata, stratum)
				break
			}
		}
	}
	return strata
}

// Bodies is a stratum's planned bodies, in three kinds. Plain holds each
// body with no same-stratum invocation at its top level, planned once and
// run at the first window's iteration zero over the view; a body whose
// same-stratum occurrences all sit inside or-join branches or subquery
// bodies is among them, since a branch or an inner query invoking no
// same-stratum predicate produces without any delta. Delta holds, per
// same-stratum occurrence of a body, at the top level, in a branch or in a
// subquery body, the copy whose occurrence invokes the predicate's delta
// symbol, run each iteration after the first. Lower holds, per occurrence
// of a seed-restricted predicate of a lower stratum, the copy whose
// occurrence invokes that predicate's delta symbol, run at a later window's
// iteration zero over the tuples the window added to that predicate.
type Bodies struct {
	Plain []PlannedBody
	Delta []PlannedBody
	Lower []PlannedBody
}

// PlannedBody is one body a stratum runs: its rule, its plan, and for a
// lower body the lower-stratum predicate its copy is marked on.
type PlannedBody struct {
	Rule  *query.Rule
	Plan  *RealizedPlan
	Lower query.Symbol
}

// PlanStratum plans every body of the stratum under the rule set, through
// the planner. Each plan is a query's plan under its own text, which carries
// % and the delta symbols, so the planner serves and stores it through the
// plan cache as it does any query's, and the executor's evaluation reads the
// plans the walk at PlanQuery made.
func PlanStratum(p QueryPlanner, rs *query.RuleSet, stratum query.Stratum, handler annotations.Handler) (Bodies, error) {
	inStratum := map[query.Symbol]bool{}
	for _, predicate := range stratum.Predicates {
		inStratum[predicate] = true
	}
	var bodies Bodies
	for _, predicate := range stratum.Predicates {
		for _, rule := range rs.Rules(predicate) {
			occurrences := 0
			var lowerOrder []query.Symbol
			lowerCount := map[query.Symbol]int{}
			for _, inv := range query.Invocations(rule.Body) {
				switch {
				case inStratum[inv.Name]:
					occurrences++
				case query.SeedRestricted(inv.Name):
					if lowerCount[inv.Name] == 0 {
						lowerOrder = append(lowerOrder, inv.Name)
					}
					lowerCount[inv.Name]++
				}
			}
			topLevel := 0
			for _, clause := range rule.Body {
				if inv, ok := clause.(*query.RuleInvocation); ok && inStratum[inv.Name] {
					topLevel++
				}
			}
			if occurrences == 0 || topLevel == 0 {
				plan, err := planBody(p, rs, rule, rule.Body, handler)
				if err != nil {
					return Bodies{}, err
				}
				bodies.Plain = append(bodies.Plain, PlannedBody{Rule: rule, Plan: plan})
			}
			for j := 0; j < occurrences; j++ {
				plan, err := planBody(p, rs, rule, markDelta(rule.Body, inStratum, j), handler)
				if err != nil {
					return Bodies{}, err
				}
				bodies.Delta = append(bodies.Delta, PlannedBody{Rule: rule, Plan: plan})
			}
			for _, name := range lowerOrder {
				marked := map[query.Symbol]bool{name: true}
				for j := 0; j < lowerCount[name]; j++ {
					plan, err := planBody(p, rs, rule, markDelta(rule.Body, marked, j), handler)
					if err != nil {
						return Bodies{}, err
					}
					bodies.Lower = append(bodies.Lower, PlannedBody{Rule: rule, Plan: plan, Lower: name})
				}
			}
		}
	}
	return bodies, nil
}

// planBody plans a rule body as a query finding the rule's head, under the
// rule set the body's invocations resolve against, with the fixpoint's
// predicates held (query.HeldByFixpoint). The planner reads from each
// invoked predicate's name whether a seed restricts its relation: a magic or
// adorned predicate, or the delta of one, leads the body's patterns, and any
// other invocation joins them at collapse.
func planBody(p QueryPlanner, rs *query.RuleSet, rule *query.Rule, body []query.Clause, handler annotations.Handler) (*RealizedPlan, error) {
	find := make([]query.FindElement, len(rule.Head))
	for i, head := range rule.Head {
		find[i] = query.FindVariable{Symbol: head}
	}
	q := &query.Query{
		Find:  find,
		In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}},
		Where: body,
	}
	plan, err := p.PlanQuery(q, query.Bindings{Rules: rs, Held: query.HeldByFixpoint}, handler)
	if err != nil {
		return nil, fmt.Errorf("planning rule %s: %w", rule.Name, err)
	}
	return plan, nil
}

// neededPredicates is the set of predicates the invoked one depends on,
// itself included: its rules' invocations, transitively. A predicate with
// required positions is not among them and is not walked through: it is
// evaluated at its invocation, under the invoking body's bindings, never
// whole.
func neededPredicates(rs *query.RuleSet, invoked query.Symbol) map[query.Symbol]bool {
	needed := map[query.Symbol]bool{}
	pending := []query.Symbol{invoked}
	for len(pending) > 0 {
		p := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if needed[p] || query.CountBound(rs.Required(p)) > 0 {
			continue
		}
		needed[p] = true
		for _, rule := range rs.Rules(p) {
			for _, inv := range query.Invocations(rule.Body) {
				if !needed[inv.Name] {
					pending = append(pending, inv.Name)
				}
			}
		}
	}
	return needed
}

// markDelta copies clauses with the target-th invocation of a predicate in
// marked, in clause order at the top level, inside or-join branches and
// inside subquery bodies at any depth, replaced by a copy invoking the
// predicate's delta symbol. The or-join forms and the subqueries on the path
// are copied, a subquery over its inner query derived over the walked
// clauses, and every other clause is shared: a same-stratum invocation
// stands nowhere else, since the bodies walked are the inferred ones the
// rule set holds, whose every form carries a header.
func markDelta(clauses []query.Clause, marked map[query.Symbol]bool, target int) []query.Clause {
	counter := 0
	return markDeltaWalk(clauses, marked, target, &counter)
}

func markDeltaWalk(clauses []query.Clause, marked map[query.Symbol]bool, target int, counter *int) []query.Clause {
	out := make([]query.Clause, len(clauses))
	for i, clause := range clauses {
		switch c := clause.(type) {
		case *query.RuleInvocation:
			out[i] = c
			if marked[c.Name] {
				if *counter == target {
					out[i] = &query.RuleInvocation{Name: query.DeltaSymbol(c.Name), Args: c.Args}
				}
				*counter++
			}
		case *query.OrJoinClause:
			out[i] = c
			if branches, found := markDeltaBranches(c.Branches, marked, target, counter); found {
				out[i] = &query.OrJoinClause{RequiredVars: c.RequiredVars, OutputVars: c.OutputVars, Branches: branches}
			}
		case *query.SubqueryPattern:
			out[i] = c
			before := *counter
			walked := markDeltaWalk(c.Query.Where, marked, target, counter)
			if before <= target && target < *counter {
				out[i] = &query.SubqueryPattern{Query: c.Query.WithWhere(walked), Inputs: c.Inputs, Binding: c.Binding}
			}
		default:
			out[i] = clause
		}
	}
	return out
}

// markDeltaBranches walks a form's branches and reports whether the target
// occurrence fell inside them, which is when the walked copy replaces the
// form; otherwise the form is shared.
func markDeltaBranches(branches [][]query.Clause, marked map[query.Symbol]bool, target int, counter *int) ([][]query.Clause, bool) {
	before := *counter
	walked := make([][]query.Clause, len(branches))
	for b, branch := range branches {
		walked[b] = markDeltaWalk(branch, marked, target, counter)
	}
	return walked, before <= target && target < *counter
}
