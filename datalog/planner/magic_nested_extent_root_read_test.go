package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A read is correlated on the extent roots a clause it carries mentions, and
// the read of the scope inside is one of the clauses it carries
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md L4): a
// required invocation inside a branch inside a branch, whose required
// argument is a given root an extent clause of the body provides, is read
// by a subquery two deep, and each read's :in carries the root the inner
// read passes beside the seed.
func TestMagic_ANestedReadIsCorrelatedOnTheRootsTheReadItCarriesMentions(t *testing.T) {
	root := &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: sym("?r")}, query.Constant{Value: datalog.NewKeyword(":page/kind")}, query.Constant{Value: "root"}}}
	pairs := defineRule("pairs", []string{"?r", "?m", "?q"}, scopePattern("?r", "page/next", "?q"), scopePattern("?m", "page/next", "?q"))
	pairs.Required = []query.Symbol{sym("?r")}
	header := func() ([]query.Symbol, []query.Symbol) {
		return []query.Symbol{sym("?r"), sym("?x")}, []query.Symbol{sym("?q")}
	}
	required, outputs := header()
	inner := &query.OrJoinClause{RequiredVars: required, OutputVars: outputs, Branches: [][]query.Clause{
		{scopePattern("?x", "page/next", "?m"), scopePattern("?r", "page/link", "?m"), invoke("pairs", "?r", "?m", "?q")},
		{scopePattern("?r", "page/other", "?q")},
	}}
	outer := &query.OrJoinClause{RequiredVars: required, OutputVars: outputs, Branches: [][]query.Clause{
		{inner},
		{scopePattern("?r", "page/other", "?q")},
	}}
	p := defineRule("p", []string{"?x", "?q"}, scopePattern("?x", "page/n", "_"), root, outer)
	magic := mustMagic(t, mustRuleSet(t, []*query.Rule{pairs, p}), "p", true, false)

	rules := magic.Rules.Rules(sym("magic.bbf/pairs"))
	require.Len(t, rules, 1, "one magic rule for pairs bound at the root and the seed-connected position")
	body := rules[0].Body
	require.NotEmpty(t, body)
	outerRead, ok := body[len(body)-1].(*query.SubqueryPattern)
	require.True(t, ok, "the body's last clause is its read of the outer branch, got %T", body[len(body)-1])
	innerRead, ok := outerRead.Query.Where[len(outerRead.Query.Where)-1].(*query.SubqueryPattern)
	require.True(t, ok, "the outer read's last clause is its read of the inner branch, got %T", outerRead.Query.Where[len(outerRead.Query.Where)-1])

	wantIn := []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?r")}, query.ScalarInput{Symbol: sym("?x")}}
	wantInputs := []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Constant{Value: datalog.NewSymbol("%")}, query.Variable{Name: sym("?r")}, query.Variable{Name: sym("?x")}}
	require.Equal(t, wantIn, innerRead.Query.In, "the inner read is correlated on the root its clauses mention and the seed")
	require.Equal(t, wantInputs, innerRead.Inputs)
	require.Equal(t, wantIn, outerRead.Query.In, "the outer read is correlated on the root the inner read mentions and the seed")
	require.Equal(t, wantInputs, outerRead.Inputs)
}
