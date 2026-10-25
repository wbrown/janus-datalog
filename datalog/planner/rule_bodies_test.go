package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// The bodies an invocation runs are computed once, here, and read by the
// executor's evaluation and the planner's walk at PlanQuery alike: the
// positions an invocation binds, the set it evaluates, the strata that set
// runs, and each stratum's bodies, planned.

// A position is bound when it holds a constant or a variable the caller
// reports bound, a repeated variable at its first position only; a blank is
// never bound.
func TestBoundPositions_AConstantAndTheFirstOccurrenceOfABoundVariableAreBound(t *testing.T) {
	x := sym("?x")
	bound := BoundPositions(invoke("p", "?x", "c", "?x", "?y", "_"), func(s query.Symbol) bool { return s == x })
	require.Equal(t, []bool{true, true, false, false, false}, bound)
}

// The set an invocation evaluates is the set as given for a free invocation
// of a predicate reaching no required predicate, which the rewrite alone
// would adorn; the rewrite seeded by the bound positions for a bound
// invocation; and the rewrite with the head all free, unseeded, for a free
// invocation of a predicate reaching a required one.
func TestEvaluatedSet_IsTheSetAsGivenForAFreeInvocationReachingNoRequiredPredicate(t *testing.T) {
	p := NewClauseBasedPlanner(nil, PlannerOptions{})
	follows := sym("follows")
	rs, err := query.NewRuleSet(followsRules())
	require.NoError(t, err)

	free, err := EvaluatedSet(p, rs, follows, []bool{false, false}, nil)
	require.NoError(t, err)
	require.Same(t, rs, free.Rules, "the set as given")
	require.Equal(t, follows, free.Predicate)
	require.Nil(t, free.Seed)

	bound, err := EvaluatedSet(p, rs, follows, []bool{true, false}, nil)
	require.NoError(t, err)
	require.NotSame(t, rs, bound.Rules, "a rewritten set")
	require.Equal(t, query.AdornedSymbol(follows, []bool{true, false}), bound.Predicate)
	require.Equal(t, query.MagicSymbol(follows, []bool{true, false}), bound.Seed)

	reaching, err := parser.ParseRules(`[[(near [?p] ?q) [?p :page/next ?q]] [(hop ?a ?c) [?a :page/next ?b] (near ?b ?c)]]`)
	require.NoError(t, err)
	hop := sym("hop")
	allFree, err := EvaluatedSet(p, reaching, hop, []bool{false, false}, nil)
	require.NoError(t, err)
	require.NotSame(t, reaching, allFree.Rules, "a rewritten set, the required predicate adorned from the clauses before its invocation")
	require.Equal(t, query.AdornedSymbol(hop, []bool{false, false}), allFree.Predicate)
	require.Nil(t, allFree.Seed)
}

// The strata an evaluation runs are those holding the invoked predicate and
// the predicates it invokes transitively, in the set's order, with a
// predicate with required positions left out, since it is evaluated at its
// invocation, and a predicate nothing invoked depends on left out.
func TestNeededStrata_IsTheTransitiveDependencySetsStrata(t *testing.T) {
	rs, err := parser.ParseRules(`[[(follows ?a ?b) [?a :page/next ?b]]
	                               [(follows ?a ?c) [?a :page/next ?b] (follows ?b ?c)]
	                               [(near [?p] ?q) [?p :page/next ?q]]
	                               [(first ?p) [?p :page/kind _] (not (follows _ ?p)) (near ?p ?q)]
	                               [(marked ?p) [?p :page/kind "marked"]]]`)
	require.NoError(t, err)
	strata := NeededStrata(rs, sym("first"))
	require.Len(t, strata, 2, "the stratum of follows and the stratum of first")
	require.Equal(t, []query.Symbol{sym("follows")}, strata[0].Predicates, "the stratum first depends on comes first")
	require.Equal(t, []query.Symbol{sym("first")}, strata[1].Predicates)
}

// Each body a stratum runs is planned as the query finding the rule's head:
// the base rule once as a plain body, the recursive rule as a delta copy per
// same-stratum occurrence, and no lower copy where no seed-restricted lower
// predicate is invoked.
func TestPlanStratum_PlansEachBodyAsTheQueryFindingTheRulesHead(t *testing.T) {
	rs, err := query.NewRuleSet(followsRules())
	require.NoError(t, err)
	strata := rs.Strata()
	require.Len(t, strata, 1)
	bodies, err := PlanStratum(NewClauseBasedPlanner(nil, PlannerOptions{}), rs, strata[0], nil)
	require.NoError(t, err)
	require.Len(t, bodies.Plain, 1, "the base rule, with no same-stratum invocation")
	require.Len(t, bodies.Delta, 1, "the recursive rule's one occurrence")
	require.Empty(t, bodies.Lower)
	for _, body := range []PlannedBody{bodies.Plain[0], bodies.Delta[0]} {
		require.Len(t, body.Plan.Query.Find, len(body.Rule.Head))
		for i, head := range body.Rule.Head {
			require.Equal(t, query.FindVariable{Symbol: head}, body.Plan.Query.Find[i], "the plan finds the rule's head")
		}
		require.NotEmpty(t, body.Plan.Phases)
	}
}

// A magic body's invocation reads a relation holding only what the seed
// reaches, so its plan runs it before the patterns, on both planner paths.
func TestPlanStratum_BodyInvocationLeadsItsPatterns(t *testing.T) {
	rs, err := query.NewRuleSet(followsRules())
	require.NoError(t, err)
	follows := sym("follows")
	for _, algebra := range []bool{true, false} {
		p := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: algebra})
		magic, err := Rewrite(p, rs, follows, []bool{true, false}, nil)
		require.NoError(t, err)
		stratum := magic.Rules.Strata()[0]
		require.Equal(t, []query.Symbol{magic.Seed}, stratum.Predicates, "algebra=%v: the first stratum is the magic predicate alone", algebra)
		bodies, err := PlanStratum(p, magic.Rules, stratum, nil)
		require.NoError(t, err)
		require.Empty(t, bodies.Plain, "algebra=%v", algebra)
		require.Len(t, bodies.Delta, 1, "algebra=%v: the magic stratum plans its one recursive rule's delta copy alone", algebra)
		require.Empty(t, bodies.Lower, "algebra=%v", algebra)
		plan := bodies.Delta[0].Plan
		require.NotEmpty(t, plan.Phases, "algebra=%v", algebra)
		require.NotEmpty(t, plan.Phases[0].Query.Where, "algebra=%v", algebra)
		first, ok := plan.Phases[0].Query.Where[0].(*query.RuleInvocation)
		require.True(t, ok && first.Name == query.DeltaSymbol(magic.Seed), "algebra=%v: the body's first clause is the delta occurrence leading the edge pattern:\n%s", algebra, plan)
	}
}

// An invocation of the rule's own stratum inside a subquery body is an
// occurrence of the semi-naive step: the body's delta copy invokes the
// predicate's delta symbol inside the inner query, and the body is planned
// plain as well, for iteration zero, as a body whose occurrences all sit
// inside forms is.
func TestPlanStratum_AnOccurrenceInsideASubqueryBodyIsADeltaCopy(t *testing.T) {
	rs, err := parser.ParseRules(`[[(reach ?x ?y) [?x :edge ?y]]
	                               [(reach ?x ?z) [?x :edge ?y] [(q [:find ?w :in $ % ?y :where (reach ?y ?w)] $ % ?y) [[?z] ...]]]]`)
	require.NoError(t, err)
	reach := sym("reach")
	for _, algebra := range []bool{true, false} {
		bodies, err := PlanStratum(NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: algebra}), rs, rs.Strata()[0], nil)
		require.NoError(t, err)
		require.Len(t, bodies.Plain, 2, "algebra=%v: both rules plain", algebra)
		require.Len(t, bodies.Delta, 1, "algebra=%v: the second rule's one delta copy", algebra)
		var inner []*query.RuleInvocation
		for _, phase := range bodies.Delta[0].Plan.Phases {
			for _, clause := range phase.Query.Where {
				if subq, ok := clause.(*query.SubqueryPattern); ok {
					inner = append(inner, query.Invocations(subq.Query.Where)...)
				}
			}
		}
		require.True(t, len(inner) == 1 && inner[0].Name == query.DeltaSymbol(reach), "algebra=%v: the delta copy's inner query invokes the one delta occurrence %s, got %v:\n%s", algebra, query.DeltaSymbol(reach), inner, bodies.Delta[0].Plan)
		original := query.Invocations(rs.Rules(reach)[1].Body)
		require.True(t, len(original) == 1 && original[0].Name == reach, "algebra=%v: the rule's own body is shared and unmarked, got %v", algebra, original)
	}
}

// A rule no magic predicate guards derives an unrestricted relation, so its
// plan scans the pattern first and joins the delta afterwards, on both
// planner paths.
func TestPlanStratum_UnguardedBodyKeepsThePatternFirst(t *testing.T) {
	rs, err := query.NewRuleSet(followsRules())
	require.NoError(t, err)
	for _, algebra := range []bool{true, false} {
		bodies, err := PlanStratum(NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: algebra}), rs, rs.Strata()[0], nil)
		require.NoError(t, err)
		require.NotEmpty(t, bodies.Delta, "algebra=%v", algebra)
		for _, body := range bodies.Delta {
			first := body.Plan.Phases[0].Query.Where[0]
			_, ok := first.(*query.DataPattern)
			require.True(t, ok, "algebra=%v: the recursive body's first clause is %v, want the edge pattern before the delta occurrence:\n%s", algebra, first, body.Plan)
		}
	}
}

// A predicate with required positions is evaluated at its invocation under
// the invoking body's bindings, never whole, so the walk of an invoked
// predicate's dependencies leaves it out; every other dependency stays in.
func TestNeededPredicates_LeavesOutARequiredPredicate(t *testing.T) {
	rs, err := parser.ParseRules(`[[(succ [?n] ?m) [(+ ?n 1) ?m]]
	                               [(next ?x ?y) [?x :page/rank ?k] (succ ?k ?j) [?y :page/rank ?j]]
	                               [(follows ?x ?y) [?x :page/next ?y]]
	                               [(chain ?x ?z) (follows ?x ?y) (next ?y ?z)]]`)
	require.NoError(t, err)
	needed := neededPredicates(rs, sym("chain"))
	for _, name := range []string{"chain", "follows", "next"} {
		require.True(t, needed[sym(name)], "%s is among the predicates chain needs", name)
	}
	require.False(t, needed[sym("succ")], "succ requires its first position bound and is not among the predicates chain needs")
}

// The target occurrence of a same-stratum predicate becomes a copy invoking
// the predicate's delta symbol, the forms on its path are copied, and
// everything else is shared.
func TestMarkDelta_RenamesOneOccurrenceAndCopiesThePath(t *testing.T) {
	follows := sym("follows")
	delta := query.DeltaSymbol(follows)
	inStratum := map[query.Symbol]bool{follows: true}
	rs, err := parser.ParseRules(`[[(follows ?a ?b) [?a :page/next ?b]]
	                               [(follows ?a ?c) (follows ?a ?b)
	                                                (or-join [?b ?c]
	                                                  (follows ?b ?c)
	                                                  [?b :page/next ?c])]]`)
	require.NoError(t, err)
	body := rs.Rules(follows)[1].Body
	first := markDelta(body, inStratum, 0)
	if inv := first[0].(*query.RuleInvocation); inv.Name != delta || inv == body[0] || body[0].(*query.RuleInvocation).Name != follows {
		t.Errorf("marking occurrence 0 replaces the first invocation by a copy invoking %s, got %s with the body's own %s", delta, inv, body[0])
	}
	if len(first[0].(*query.RuleInvocation).Args) != 2 {
		t.Error("the copy keeps the occurrence's arguments")
	}
	if first[1] != body[1] {
		t.Error("an or-join holding no marked occurrence is shared, not copied")
	}
	second := markDelta(body, inStratum, 1)
	if second[0] != body[0] {
		t.Error("marking occurrence 1 shares the first invocation")
	}
	orJoin, ok := second[1].(*query.OrJoinClause)
	if !ok || orJoin == body[1] {
		t.Fatal("an or-join holding the marked occurrence is copied")
	}
	if inv := orJoin.Branches[0][0].(*query.RuleInvocation); inv.Name != delta {
		t.Errorf("the or-join branch's invocation invokes %s, got %s", delta, inv)
	}
	if orJoin.Branches[1][0] != body[1].(*query.OrJoinClause).Branches[1][0] {
		t.Error("a branch without the marked occurrence shares its clauses")
	}
	if len(markDeltaWalk(body, inStratum, 5, new(int))) != len(body) {
		t.Error("a target past the last occurrence copies the body with nothing marked")
	}
}
