package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A scope's query finds its header, the required run as variables followed
// by the find, declares $ and %, and opens with the invocation of
// scope/required over the required run, followed by the scope's clauses. The
// header is the required run then the outputs, an aggregate's by its display
// form, and the query's text parses back to itself
// (SCOPE_EXECUTION_AND_CONTRACTS.md §4.1).
func TestScopeQueryFramesTheHeaderOverScopeRequired(t *testing.T) {
	a, b := datalog.NewSymbol("?a"), datalog.NewSymbol("?b")
	parsed, err := parser.ParseQuery(`[:find ?b (count ?c) :in $ ?a :where [?a :page/next ?b] [?b :page/n ?c]]`)
	require.NoError(t, err)
	s := Scope{Required: []query.Symbol{a}, Find: parsed.Find, Clauses: parsed.Where}

	outputs := s.Outputs()
	require.Len(t, outputs, 2)
	require.Equal(t, b, outputs[0])
	require.Equal(t, "(count ?c)", outputs[1].String(), "an aggregate's output is its display form")
	require.Equal(t, append([]query.Symbol{a}, outputs...), s.Header())

	q := s.Query()
	require.Len(t, q.Find, 3)
	require.Equal(t, query.FindVariable{Symbol: a}, q.Find[0], "the required run leads the find as variables")
	require.Equal(t, parsed.Find, q.Find[1:], "the scope's find follows")
	require.Equal(t, []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}}, q.In)
	require.Len(t, q.Where, 3)
	opening, ok := q.Where[0].(*query.RuleInvocation)
	require.True(t, ok, "the query opens with an invocation, got %v", q.Where[0])
	require.Equal(t, query.ScopeRequired, opening.Name)
	require.Equal(t, []query.PatternElement{query.Variable{Name: a}}, opening.Args)
	require.Equal(t, parsed.Where, q.Where[1:], "the scope's clauses follow the invocation")

	reparsed, err := parser.ParseQuery(q.String())
	require.NoError(t, err, "the framing is text the parser reads")
	require.Equal(t, q.String(), reparsed.String())
}

// A rule body planned as a scope under its head's bound positions is placed
// by the planner's rank and gate on both planner paths, read as the rewrite
// reads a scope's plan, the phases' clauses less the scope/required
// invocation that opens them: an invocation reading a variable a pattern
// binds waits on that pattern, which seeks on the required run, and an
// invocation whose argument the required run binds precedes a pattern
// reading nothing of it, a scan of a whole extent (REWRITE_ORDER_PROOF.md
// T4).
func TestPlannedClausesPlaceABodysInvocationAgainstItsPatterns(t *testing.T) {
	a, c := datalog.NewSymbol("?a"), datalog.NewSymbol("?c")
	follows := datalog.NewSymbol("follows")
	binderAfter, err := parser.ParseRules(`[[(follows ?a ?b) [?a :page/next ?b]]
	                                        [(follows ?a ?c) (follows ?b ?c) [?a :page/next ?b]]]`)
	require.NoError(t, err)
	recursionFirst, err := parser.ParseRules(`[[(follows ?a ?b) [?a :page/next ?b]]
	                                           [(follows ?a ?c) (follows ?a ?b) [?b :page/next ?c]]]`)
	require.NoError(t, err)
	for _, mode := range []struct {
		name    string
		algebra bool
	}{{"algebra_on", true}, {"algebra_off", false}} {
		t.Run(mode.name, func(t *testing.T) {
			p := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: mode.algebra})
			planned := func(rs *query.RuleSet) []query.Clause {
				body := rs.Rules(follows)[1].Body
				plan, err := p.PlanScope(Scope{Required: []query.Symbol{a}, Find: []query.FindElement{query.FindVariable{Symbol: c}}, Clauses: body}, query.Bindings{Rules: rs}, nil)
				require.NoError(t, err)
				got := plannedClauses(plan)
				require.Len(t, got, 2, "the two clauses of the body")
				return got
			}
			got := planned(binderAfter)
			_, ok := got[0].(*query.DataPattern)
			require.True(t, ok, "the binder-after body is planned %v, want the pattern binding ?b before the invocation reading it", got)
			got = planned(recursionFirst)
			_, ok = got[0].(*query.RuleInvocation)
			require.True(t, ok, "the recursion-first body is planned %v, want the invocation bound at ?a before the pattern reading its ?b", got)
		})
	}
}
