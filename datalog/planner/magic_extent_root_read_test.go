package planner

import (
	"testing"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A read inside a branch is correlated on an extent root only when a clause
// it carries mentions it, and the enclosing scope reads the root's own
// extent chain (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md
// L4): a required invocation inside a union's branch whose required argument
// is a given symbol an extent clause of the body provides, and whose other
// argument a seed-connected pattern of the branch provides, is adorned at
// both; its magic rule reads the branch through a subquery over the seed
// alone, the given root standing in the head under its own name and bound
// by the body's read of its chain.
func TestMagic_AReadIsCorrelatedOnAnExtentRootOnlyWhereAClauseItCarriesMentionsIt(t *testing.T) {
	root := &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: sym("?r")}, query.Constant{Value: datalog.NewKeyword(":page/kind")}, query.Constant{Value: "root"}}}
	pairs := defineRule("pairs", []string{"?r", "?m", "?q"}, scopePattern("?r", "page/next", "?q"), scopePattern("?m", "page/next", "?q"))
	pairs.Required = []query.Symbol{sym("?r")}
	form := func(invocation string) *query.OrJoinClause {
		return &query.OrJoinClause{RequiredVars: []query.Symbol{sym("?r"), sym("?x")}, OutputVars: []query.Symbol{sym("?q")}, Branches: [][]query.Clause{
			{scopePattern("?x", "page/next", "?m"), invoke(invocation, "?r", "?m", "?q")},
			{scopePattern("?r", "page/other", "?q")},
		}}
	}
	p := defineRule("p", []string{"?x", "?q"}, scopePattern("?x", "page/n", "_"), root, form("pairs"))
	magic := mustMagic(t, mustRuleSet(t, []*query.Rule{pairs, p}), "p", true, false)
	assertRules(t, "adorned.bf/p", magic.Rules.Rules(sym("adorned.bf/p")), []*query.Rule{
		defineRule("adorned.bf/p", []string{"?x", "?q"},
			invoke("magic.bf/p", "?x"),
			root,
			scopePattern("?x", "page/n", "_"),
			form("adorned.bbf/pairs")),
	})
	assertRules(t, "magic.bbf/pairs", magic.Rules.Rules(sym("magic.bbf/pairs")), []*query.Rule{
		defineRule("magic.bbf/pairs", []string{"?r", "?magic.bbf/pairs.1"},
			invoke("magic.bf/p", "?x"),
			root,
			scopePattern("?x", "page/n", "_"),
			branchRead("?x", []query.Clause{scopePattern("?x", "page/next", "?m")}, "?m", "?magic.bbf/pairs.1")),
	})
}

// The same at a subquery's inner query: the call's argument for the required
// position is a variable an extent clause of the body provides, so the read
// of the inner query passes the :in symbol standing for the seed alone, and
// the required position's :in symbol stands in the head under its argument's
// variable.
func TestMagic_ASubqueryReadIsCorrelatedOnAnExtentRootOnlyWhereAClauseItCarriesMentionsIt(t *testing.T) {
	root := &query.DataPattern{Elements: []query.PatternElement{query.Variable{Name: sym("?r")}, query.Constant{Value: datalog.NewKeyword(":page/kind")}, query.Constant{Value: "root"}}}
	pairs := defineRule("pairs", []string{"?r", "?m", "?q"}, scopePattern("?r", "page/next", "?q"), scopePattern("?m", "page/next", "?q"))
	pairs.Required = []query.Symbol{sym("?r")}
	dollar := query.Constant{Value: datalog.SymDollar}
	rules := query.Constant{Value: datalog.NewSymbol("%")}
	call := func(invocation string) *query.SubqueryPattern {
		return &query.SubqueryPattern{
			Query: &query.Query{
				Find:  []query.FindElement{query.FindVariable{Symbol: sym("?w")}},
				In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?s")}, query.ScalarInput{Symbol: sym("?y")}},
				Where: []query.Clause{scopePattern("?y", "page/next", "?m"), invoke(invocation, "?s", "?m", "?w")},
			},
			Inputs:  []query.PatternElement{dollar, rules, query.Variable{Name: sym("?r")}, query.Variable{Name: sym("?x")}},
			Binding: query.RelationBinding{Variables: []query.Symbol{sym("?q")}},
		}
	}
	p := defineRule("p", []string{"?x", "?q"}, scopePattern("?x", "page/n", "_"), root, call("pairs"))
	magic := mustMagic(t, mustRuleSet(t, []*query.Rule{pairs, p}), "p", true, false)
	assertRules(t, "adorned.bf/p", magic.Rules.Rules(sym("adorned.bf/p")), []*query.Rule{
		defineRule("adorned.bf/p", []string{"?x", "?q"},
			invoke("magic.bf/p", "?x"),
			root,
			scopePattern("?x", "page/n", "_"),
			call("adorned.bbf/pairs")),
	})
	assertRules(t, "magic.bbf/pairs", magic.Rules.Rules(sym("magic.bbf/pairs")), []*query.Rule{
		defineRule("magic.bbf/pairs", []string{"?r", "?magic.bbf/pairs.1"},
			invoke("magic.bf/p", "?x"),
			root,
			scopePattern("?x", "page/n", "_"),
			&query.SubqueryPattern{
				Query: &query.Query{
					Find:  []query.FindElement{query.FindVariable{Symbol: sym("?m")}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?y")}},
					Where: []query.Clause{scopePattern("?y", "page/next", "?m")},
				},
				Inputs:  []query.PatternElement{dollar, rules, query.Variable{Name: sym("?x")}},
				Binding: query.RelationBinding{Variables: []query.Symbol{sym("?magic.bbf/pairs.1")}},
			}),
	})
}
