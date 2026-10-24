package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// A magic rule reads no negation and no aggregating subquery at any depth of
// what it reads, so no magic rule depends non-monotonically on any predicate
// and the rewritten set is accepted
// (docs/proposals/NESTED_SCOPES_AS_QUERIES.md §5.5,
// docs/proposals/NON_MONOTONE_FORMS_PROOF.md §1).

// mustParseRules parses text as a rule set.
func mustParseRules(t *testing.T, text string) *query.RuleSet {
	t.Helper()
	rs, err := parser.ParseRules(text)
	require.NoError(t, err)
	return rs
}

// rewriteOf rewrites the set for an invocation of name under bound, through a
// planner on the clause-based path, returning the refusal.
func rewriteOf(rs *query.RuleSet, name string, bound ...bool) (*Magic, error) {
	return Rewrite(NewClauseBasedPlanner(nil, PlannerOptions{}), rs, sym(name), bound, nil)
}

// An aggregating subquery the planner places before the recursion is not
// read by the recursion's magic rule, so the inner invocation it adorns stays
// below the recursive stratum and the recursion after it, whose argument the
// subquery alone provides, keeps the original predicate.
func TestMagic_AnAggregatingSubqueryBeforeTheRecursionIsNotRead(t *testing.T) {
	rs := mustParseRules(t, `[[(low ?a ?b) [?a :edge ?b]]
	                          [(p ?a ?b) [?a :edge ?b]]
	                          [(p ?a ?c) [?a :edge _] [(q [:find ?w (count ?z) :in $ % ?x :where (low ?x ?w) (low ?w ?z)] $ % ?a) [[?v ?n]]] [(identity ?v) ?m] (p ?m ?c)]]`)
	magic, err := rewriteOf(rs, "p", true, false)
	require.NoError(t, err, "the rewritten set is accepted")
	require.Empty(t, magic.Rules.Rules(sym("magic.bf/p")), "the recursion after the subquery is not adorned, since its argument is provided through a clause no magic rule reads")
	require.NotEmpty(t, magic.Rules.Rules(sym("p")), "the recursion keeps the original predicate")
	require.NotEmpty(t, magic.Rules.Rules(sym("adorned.bf/low")), "the subquery's inner invocation is adorned from the clauses before the form")
}

// A not-join inside an or-join branch the magic rule reads is left out of
// the read, so the magic rule depends on nothing through the negation and
// the set stratifies.
func TestMagic_ANegationInsideAReadBranchIsLeftOutOfTheRead(t *testing.T) {
	rs := mustParseRules(t, `[[(low ?a ?b) [?a :edge ?b]]
	                          [(p ?a ?b) [?a :edge ?b]]
	                          [(p ?a ?c) [?a :edge ?m] (or-join [[?m] ?k] (and (not-join [?m] (low ?m ?z)) [(identity ?m) ?k]) (and [?m :type :node] [(identity ?m) ?k])) (p ?k ?c)]]`)
	magic, err := rewriteOf(rs, "p", true, false)
	require.NoError(t, err, "the rewritten set is accepted")
	rules := magic.Rules.Rules(sym("magic.bf/p"))
	require.Len(t, rules, 1, "the recursion is adorned from the or-join's output")
	for _, clause := range rules[0].Body {
		form, ok := clause.(*query.OrJoinClause)
		if !ok {
			continue
		}
		for _, branch := range form.Branches {
			for _, inner := range branch {
				_, negation := inner.(*query.NotJoinClause)
				require.False(t, negation, "the magic rule's read of the or-join holds no not-join: %s", form)
			}
		}
	}
}

// A read of a subquery scope passes the call's inputs the passing binds and
// no other, so the magic rule it builds is safe when the sealed passing
// leaves an input unbound.
func TestMagic_ACallInputTheSealedPassingLeavesUnboundIsUnbound(t *testing.T) {
	rs := mustParseRules(t, `[[(low ?a ?b) [?a :edge ?b]]
	                          [(p ?a ?b) [?a :edge ?b]]
	                          [(p ?a ?c) (p ?a ?b) [?b :edge ?c] [(q [:find (count ?z) :in $ % ?x ?y :where [?x :edge ?w] (low ?w ?z) [(!= ?z ?y)]] $ % ?a ?c) [[?n]]] [(> ?n 0)]]]`)
	magic, err := rewriteOf(rs, "p", true, false)
	require.NoError(t, err, "the rewritten set is accepted")
	assertRules(t, "magic.bf/low", magic.Rules.Rules(sym("magic.bf/low")), []*query.Rule{
		defineRule("magic.bf/low", []string{"?magic.bf/low.0"},
			invoke("magic.bf/p", "?a"),
			&query.SubqueryPattern{
				Query: &query.Query{
					Find:  []query.FindElement{query.FindVariable{Symbol: sym("?w")}},
					In:    []query.InputSpec{query.DatabaseInput{Name: datalog.SymDollar}, query.RulesInput{}, query.ScalarInput{Symbol: sym("?x")}},
					Where: []query.Clause{scopePattern("?x", "edge", "?w")},
				},
				Inputs:  []query.PatternElement{query.Constant{Value: datalog.SymDollar}, query.Constant{Value: datalog.SymRules}, query.Variable{Name: sym("?a")}},
				Binding: query.RelationBinding{Variables: []query.Symbol{sym("?magic.bf/low.0")}},
			}),
	})
}

// A free invocation of a predicate whose rules reach a required predicate is
// rewritten all free at every level, so the required predicate is adorned
// from the clauses before its invocation and derived in the rewritten set,
// and no original rule of the path joins the set
// (docs/reference/RULES.md, the body invocation nothing binds).
func TestMagic_AFreeInvocationIsRewrittenAtEveryLevel(t *testing.T) {
	rs := mustParseRules(t, `[[(succ [?n] ?m) [(+ ?n 1) ?m]]
	                          [(walk ?a ?b) [?a :edge ?b]]
	                          [(walk ?a ?c) (walk ?a ?b) [?b :rank ?k] (succ ?k ?j) [(< ?j 100)] [?b :edge ?c]]
	                          [(top ?a ?c) (walk ?a ?c)]]`)
	magic, err := rewriteOf(rs, "top", false, false)
	require.NoError(t, err)
	require.Len(t, magic.Rules.Rules(sym("adorned.ff/walk")), 2, "walk is rewritten all free under top")
	require.Len(t, magic.Rules.Rules(sym("adorned.bf/succ")), 1, "succ is adorned from the clauses before its invocation")
	require.Empty(t, magic.Rules.Rules(sym("walk")), "no original walk rule joins the set")
	require.Empty(t, magic.Rules.Rules(sym("succ")), "no original succ rule joins the set")
}

// A fallback form the planner places after the recursion walks under the
// passing sealed at the recursion, so its inner invocation is adorned from
// the clauses before the recursion and its magic rule reads those alone
// (docs/proposals/NON_MONOTONE_FORMS_PROOF.md §7).
func TestMagic_AFallbackAfterTheRecursionIsWalkedUnderTheSealedPassing(t *testing.T) {
	rs := mustParseRules(t, `[[(marked ?a ?x) [?a :page/marked ?x]]
	                          [(walk ?a ?c) [?a :page/next ?c]]
	                          [(walk ?a ?c) [?a :page/next ?b] (walk ?b ?c) (or-default (marked ?a ?x) [?c :page/kind ?x])]]`)
	magic, err := rewriteOf(rs, "walk", true, false)
	require.NoError(t, err)
	assertRules(t, "magic.bf/marked", magic.Rules.Rules(sym("magic.bf/marked")), []*query.Rule{
		defineRule("magic.bf/marked", []string{"?a"},
			invoke("magic.bf/walk", "?a"),
			scopePattern("?a", "page/next", "?b")),
	})
	require.Empty(t, magic.Rules.Rules(sym("marked")), "the fallback's predicate is adorned, not included original")
}

// A free invocation under a non-monotone form, of a predicate whose rules
// reach a required one, keeps its original rules when naming it all free
// would place it in the invoking rule's stratum, so the constructor accepts
// the set (docs/proposals/NON_MONOTONE_FORMS_PROOF.md T6). Here the
// aggregating subquery of p3 reads adorned.bf/p2 before it, which draws
// adorned.bf/p0 into adorned.bf/p2's stratum, and the fallback in p2's
// recursive rule invokes p0 free; p0 reaches the required nb.
func TestMagic_AFreeSiteUnderAFallbackWhoseAllFreeNameWouldNotStratifyKeepsTheOriginalRules(t *testing.T) {
	rs := mustParseRules(t, `[[(nb [?x] ?y) [?x :edge ?y]]
	                          [(p0 ?a ?b) [?a :edge ?b]]
	                          [(p0 ?a ?b) [?a :edge ?m] (p0 ?m ?b)]
	                          [(p0 ?a ?b) [?a :color _] (or-join [[?a] ?b] (nb ?a ?b) [?a :edge ?b])]
	                          [(p2 ?a ?b) [?a :edge ?b]]
	                          [(p2 ?a ?b) [?a :edge ?m] (p0 ?m ?b)]
	                          [(p2 ?a ?b) (p2 ?a ?b) (or-default-join [[?b] ?k] (and (p0 ?b ?z) [(ground 1) ?k]) [(ground 0) ?k]) [(= ?k 1)]]
	                          [(p3 ?a ?b) (p2 ?a ?m) [?m :edge ?b] [(q [:find (count ?z) :in $ % ?x :where [?x :edge ?w] (p0 ?w ?z)] $ % ?a) [[?n]]] [(> ?n 0)]]]`)
	magic, err := rewriteOf(rs, "p3", true, false)
	require.NoError(t, err, "the rewritten set is accepted")
	require.NotEmpty(t, magic.Rules.Rules(sym("p0")), "the fallback's free site keeps p0's original rules")
	require.Empty(t, magic.Rules.Rules(sym("adorned.ff/p0")), "no all-free p0 joins the set")
	require.NotEmpty(t, magic.Rules.Rules(sym("adorned.bf/p0")), "the bound sites of p0 stay adorned")
}

// A read of a subquery scope passes the sources, the rule set and the call
// inputs the passing binds, in call order, and no other: an entry of one
// symbol whole or not at all, a tuple or relation entry over its bound
// symbols, and an omitted :in as the one $ the call passes.
func TestPassedInputs_KeepsTheSourcesTheRuleSetAndTheBoundInputs(t *testing.T) {
	dollar := query.Constant{Value: datalog.SymDollar}
	rules := query.Constant{Value: datalog.NewSymbol("%")}
	arg := func(name string) query.PatternElement { return query.Variable{Name: sym(name)} }
	bound := map[query.Symbol]bool{sym("?x"): true, sym("?k"): true, sym("?r"): true}
	call := &query.SubqueryPattern{
		Query: &query.Query{In: []query.InputSpec{
			query.DatabaseInput{Name: datalog.SymDollar},
			query.RulesInput{},
			query.ScalarInput{Symbol: sym("?x")},
			query.ScalarInput{Symbol: sym("?y")},
			query.CollectionInput{Symbol: sym("?c")},
			query.TupleInput{Symbols: []query.Symbol{sym("?j"), sym("?k")}},
			query.RelationInput{Symbols: []query.Symbol{sym("?r"), sym("?s")}},
		}},
		Inputs: []query.PatternElement{dollar, rules, arg("?a"), arg("?b"), arg("?cs"), arg("?j1"), arg("?k1"), arg("?r1"), arg("?s1")},
	}
	in, inputs := passedInputs(call, bound)
	require.Equal(t, []query.InputSpec{
		query.DatabaseInput{Name: datalog.SymDollar},
		query.RulesInput{},
		query.ScalarInput{Symbol: sym("?x")},
		query.TupleInput{Symbols: []query.Symbol{sym("?k")}},
		query.RelationInput{Symbols: []query.Symbol{sym("?r")}},
	}, in, "the unbound scalar, the unbound collection and the unbound symbols of the tuple and the relation are left out")
	require.Equal(t, []query.PatternElement{dollar, rules, arg("?a"), arg("?k1"), arg("?r1")}, inputs, "the arguments follow the entries kept")

	omitted := &query.SubqueryPattern{Query: &query.Query{}, Inputs: []query.PatternElement{dollar}}
	in, inputs = passedInputs(omitted, bound)
	require.Nil(t, in, "an omitted :in stays omitted")
	require.Equal(t, []query.PatternElement{dollar}, inputs, "the one $ the call passes is kept")
}
