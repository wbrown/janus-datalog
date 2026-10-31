package planner

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// symbolSet is a set over the symbols named.
func symbolSet(names ...string) map[query.Symbol]bool {
	set := make(map[query.Symbol]bool, len(names))
	for _, name := range names {
		set[sym(name)] = true
	}
	return set
}

// An entity constant is a constant at a data pattern's entity position,
// which names one entity; a constant at the attribute or the value
// position, and any other clause, is none.
func TestEntityConstant_IsAConstantAtTheEntityPosition(t *testing.T) {
	hub := &query.DataPattern{Elements: []query.PatternElement{query.Constant{Value: datalog.NewIdentity("hub")}, query.Constant{Value: datalog.NewKeyword(":page/next")}, query.Variable{Name: sym("?h")}}}
	require.True(t, entityConstant(hub), "a constant at the entity position")
	require.False(t, entityConstant(scopePattern("?x", "page/kind", "_")), "a constant at the attribute alone")
	require.False(t, entityConstant(scopePattern("?x", "page/kind", "leaf")), "a constant at the value position")
	require.False(t, entityConstant(invoke("follows", "?a", "?b")), "an invocation")
}

// The seed component is the roots and every clause shared variables connect
// to them or to an entity-constant pattern, whatever order the clauses stand
// in: a value-constant pattern before the clause connecting it is in, an
// entity-constant pattern and the chain hanging off it are in, and a clause
// connected to neither is out
// (A_CLAUSE_OUTSIDE_THE_SEED_COMPONENT_BINDS_NOTHING_PROOF.md §1).
func TestSeedOf_IsTheConnectedComponentOfTheRootsInAnyOrder(t *testing.T) {
	w := &magicRewriter{rs: mustRuleSet(t, []*query.Rule{smallRule()})}
	leaf := scopePattern("?x", "page/kind", "leaf")
	root := scopePattern("?r", "page/kind", "root")
	hub := &query.DataPattern{Elements: []query.PatternElement{query.Constant{Value: datalog.NewIdentity("hub")}, query.Constant{Value: datalog.NewKeyword(":page/next")}, query.Variable{Name: sym("?h")}}}
	onRoot := invoke("small", "?r")
	clauses := []query.Clause{leaf, root, scopePattern("?a", "edge", "?x"), hub, scopePattern("?h", "edge", "?g"), onRoot}
	seed := w.seedOf(symbolSet("?a"), clauses)
	require.Equal(t, symbolSet("?a", "?x", "?h", "?g"), seed, "the root, the leaf pattern the edge connects to it, the hub and its chain; the root pattern and the invocation on its symbol stay out")
	require.True(t, w.seedConnected(leaf, seed), "the leaf pattern shares ?x with the component")
	require.True(t, w.seedConnected(hub, seed), "an entity-constant pattern is in the component")
	require.False(t, w.seedConnected(root, seed), "the root pattern shares nothing with the component")
	require.False(t, w.seedConnected(onRoot, seed), "the invocation correlates on the root pattern's symbol alone")
}

// split is the bound set and the extent set of the passing's innermost
// scope: at the body, the seed component's variables and what the other
// clauses provide; in a level, the level's roots and extent roots split the
// same way, an extent root a clause connects to the seed joining the bound
// set and one nothing connects staying in the extent set with what its
// chain provides.
func TestSplit_PartitionsTheInnermostScopesSymbols(t *testing.T) {
	w := &magicRewriter{rs: mustRuleSet(t, []*query.Rule{smallRule()})}
	body := passing{head: symbolSet("?a"), prefix: []query.Clause{invoke("magic.bf/p", "?a"), scopePattern("?r", "page/kind", "root"), scopePattern("?a", "edge", "?x")}}
	bound, extent := w.split(body)
	require.Equal(t, symbolSet("?a", "?x"), bound, "the head and what the edge connects to it")
	require.Equal(t, symbolSet("?r"), extent, "what the root pattern provides")

	level := scopeLevel{roots: symbolSet("?x"), extentRoots: symbolSet("?k", "?j"), clauses: []query.Clause{scopePattern("?k", "page/next", "?m"), scopePattern("?x", "page/next", "?j")}}
	bound, extent = w.split(passing{head: symbolSet("?a"), levels: []scopeLevel{level}})
	require.Equal(t, symbolSet("?x", "?j"), bound, "the level's root and the extent root the pattern connects to it")
	require.Equal(t, symbolSet("?k", "?m"), extent, "the extent root nothing connects and the symbol its chain provides")
}

// A level's reads are its seed-connected clauses and the extent chains the
// needed symbols touch, in the walk's order, and the read passes the extent
// roots those mention and a needed symbol that is an extent root itself.
func TestLevelReads_CarriesTheSeedComponentAndTheNeededChains(t *testing.T) {
	loose := defineRule("loose", []string{"?n"}, scopePattern("?n", "page/n", "_"))
	w := &magicRewriter{rs: mustRuleSet(t, []*query.Rule{loose})}
	root := scopePattern("?r", "page/kind", "root")
	edge := scopePattern("?a", "edge", "?x")
	next := scopePattern("?k", "page/next", "?m")
	onRoot := invoke("loose", "?r")
	clauses := []query.Clause{root, edge, next, onRoot}
	roots, extentRoots := symbolSet("?a"), symbolSet("?k")

	read, passed := w.levelReads(roots, extentRoots, clauses, nil)
	require.Equal(t, []query.Clause{edge}, read, "the seed component alone when nothing is needed")
	require.Empty(t, passed, "no extent root is mentioned")

	read, passed = w.levelReads(roots, extentRoots, clauses, symbolSet("?m"))
	require.Equal(t, []query.Clause{edge, next}, read, "?m's chain, the pattern reading the extent root")
	require.Equal(t, symbolSet("?k"), passed, "the extent root the chain mentions")

	read, passed = w.levelReads(roots, extentRoots, clauses, symbolSet("?r"))
	require.Equal(t, []query.Clause{root, edge, onRoot}, read, "?r's chain, the pattern and the invocation on it, in the walk's order")
	require.Empty(t, passed, "?r's chain mentions no extent root")

	read, passed = w.levelReads(roots, extentRoots, []query.Clause{edge}, symbolSet("?k"))
	require.Equal(t, []query.Clause{edge}, read, "a needed extent root nothing here provides adds no clause")
	require.Equal(t, symbolSet("?k"), passed, "and is passed for the enclosing scope to read")
}

// A branch's read passes the sources, the rule set and the given symbols
// passed, in the given order, each standing for the enclosing scope's symbol
// of its name; a call's read passes its :in and arguments over the symbols
// passed, each :in symbol standing for its argument's variable.
func TestPasses_IsTheReadsInputsOverTheSymbolsPassed(t *testing.T) {
	dollar := query.Constant{Value: datalog.SymDollar}
	rules := query.Constant{Value: datalog.NewSymbol("%")}
	branch := scopeLevel{given: []query.Symbol{sym("?k"), sym("?m"), sym("?x")}}
	in, inputs := branch.passes(symbolSet("?x", "?k"))
	require.Equal(t, []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?k")}, query.ScalarInput{Symbol: sym("?x")}}, in, "the given symbols passed, in the given order")
	require.Equal(t, []query.PatternElement{dollar, rules, query.Variable{Name: sym("?k")}, query.Variable{Name: sym("?x")}}, inputs, "each under its own name")
	require.Equal(t, sym("?k"), branch.outside(sym("?k")), "a branch's given symbol stands for the enclosing scope's symbol of its name")

	names, err := query.SubstitutionOf([]query.Symbol{sym("?p")}, []query.Symbol{sym("?y")})
	require.NoError(t, err)
	call := scopeLevel{
		call: &query.SubqueryPattern{
			Query:  &query.Query{In: []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?p")}, query.ScalarInput{Symbol: sym("?q")}}},
			Inputs: []query.PatternElement{dollar, rules, query.Variable{Name: sym("?y")}, query.Variable{Name: sym("?z")}},
		},
		names: names,
	}
	in, inputs = call.passes(symbolSet("?p"))
	require.Equal(t, []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?p")}}, in, "the :in symbols passed")
	require.Equal(t, []query.PatternElement{dollar, rules, query.Variable{Name: sym("?y")}}, inputs, "and their arguments")
	require.Equal(t, sym("?y"), call.outside(sym("?p")), "a call's :in symbol stands for its argument's variable")
}
