package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A delta invocation is the first clause of the first phase of the plan of
// the scope its occurrence stands in, on both planner paths
// (docs/proposals/DELTA_LEADS_ITS_COPY_PROOF.md, T2): the body for an
// occurrence at the top level, a union's branch for one inside it, planned
// under the form's header as planClauses frames it, and a subquery's
// inner query for one inside it, planned under its bound :in symbols. At the
// body, the form or the call holding the occurrence stands where the rank
// and the score place it.
func TestPlanStratum_EveryCopyOpensWithItsDeltaWhereverTheOccurrenceStands(t *testing.T) {
	delta := query.DeltaSymbol(sym("reach"))
	holdsDelta := func(clauses []query.Clause) bool {
		for _, inv := range query.Invocations(clauses) {
			if inv.Name == delta {
				return true
			}
		}
		return false
	}
	// scopeOf is the scope of the form or the call holding the delta among
	// the copy's clauses, framed as planClauses frames it, and nil when the
	// occurrence stands at the top level.
	scopeOf := func(copy []query.Clause) *Scope {
		for _, clause := range copy {
			switch c := clause.(type) {
			case *query.OrJoinClause:
				for _, branch := range c.Branches {
					if !holdsDelta(branch) {
						continue
					}
					find := make([]query.FindElement, len(c.OutputVars))
					for i, out := range c.OutputVars {
						find[i] = query.FindVariable{Symbol: out}
					}
					return &Scope{Required: c.RequiredVars, Find: find, Clauses: branch}
				}
			case *query.SubqueryPattern:
				if !holdsDelta(c.Query.Where) {
					continue
				}
				var required []query.Symbol
				for _, in := range query.InputSymbols(c.Query.In) {
					if in != nil {
						required = append(required, in)
					}
				}
				return &Scope{Required: required, Find: c.Query.Find, Clauses: c.Query.Where}
			}
		}
		return nil
	}
	cases := []struct{ name, rules string }{
		{"two occurrences at the top level", `[[(reach ?a ?b) [?a :page/next ?b]]
		                                       [(reach ?a ?c) (reach ?a ?b) (reach ?b ?c)]]`},
		{"an occurrence inside a union's branch", `[[(reach ?a ?b) [?a :page/next ?b]]
		                                            [(reach ?a ?c) [?a :page/next ?b] (or-join [?b ?c] (reach ?b ?c) [?b :page/jump ?c])]]`},
		{"an occurrence inside a subquery's inner query", `[[(reach ?x ?y) [?x :page/next ?y]]
		                                                   [(reach ?x ?z) [?x :page/next ?y] [(q [:find ?w :in $ % ?y :where (reach ?y ?w)] $ % ?y) [[?z] ...]]]]`},
	}
	for _, c := range cases {
		for _, algebra := range []bool{true, false} {
			t.Run(c.name, func(t *testing.T) {
				rs, err := parser.ParseRules(c.rules)
				require.NoError(t, err)
				p := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: algebra})
				bodies, err := PlanStratum(p, rs, rs.Strata()[0], nil)
				require.NoError(t, err)
				require.NotEmpty(t, bodies.Delta, "algebra=%v: the recursive rule has a delta copy", algebra)
				for _, body := range bodies.Delta {
					plan := body.Plan
					if scope := scopeOf(plan.Query.Where); scope != nil {
						plan, err = p.PlanScope(*scope, query.Bindings{Rules: rs, Held: query.HeldByFixpoint}, nil)
						require.NoError(t, err)
					}
					first := plan.Phases[0].Query.Where[0]
					inv, ok := first.(*query.RuleInvocation)
					require.True(t, ok && inv.Name == delta, "algebra=%v: the scope of the copy marked on %s opens with %v, want %s:\n%s", algebra, body.Marked, first, delta, plan)
				}
			})
		}
	}
}
