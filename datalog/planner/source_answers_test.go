package planner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/parser"
	"github.com/wbrown/janus-datalog/datalog/query"
)

// answeringTrue and answeringFalse are a source's answer to whether a match
// of an attribute on it answers its lookup's one value per entity, the same
// for every source and attribute.
func answeringTrue(query.Symbol, datalog.Keyword) bool  { return true }
func answeringFalse(query.Symbol, datalog.Keyword) bool { return false }

// getElseFormsIn counts, among the phases' clauses, the get-else expressions
// a declined rewrite leaves as per-tuple lookups and the or-default-joins an
// applied rewrite decompiles to.
func getElseFormsIn(phases []RealizedPhase) (lookups, rewritten int) {
	for _, phase := range phases {
		for _, clause := range phase.Query.Where {
			switch typed := clause.(type) {
			case *query.Expression:
				if _, isGetElse := typed.Function.(*query.GetElseFunction); isGetElse {
					lookups++
				}
			case *query.OrDefaultJoinClause:
				rewritten++
			}
		}
	}
	return lookups, rewritten
}

// A plan made where the sources answer that a match answers the lookup holds
// the get-else rewrite. One plan cache does not serve it to a planner whose
// sources answer otherwise, and the plan that planner makes keeps the
// get-else as a per-tuple lookup.
func TestThePlanCacheServesAPlanOnlyWhereTheSourcesAnswerAsWhenItWasMade(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`)
	require.NoError(t, err)
	cache := NewPlanCache(10, 0)
	answeringYes := NewClauseBasedPlanner(nil, PlannerOptions{Cache: cache, EnableAlgebraOptimizer: true, MatchAnswersLookup: answeringTrue})
	answeringNo := NewClauseBasedPlanner(nil, PlannerOptions{Cache: cache, EnableAlgebraOptimizer: true, MatchAnswersLookup: answeringFalse})

	made, err := answeringYes.Plan(q, query.Bindings{}, nil)
	require.NoError(t, err)
	lookups, rewritten := getElseFormsIn(made.Phases)
	require.Equal(t, [2]int{0, 1}, [2]int{lookups, rewritten}, "the plan made under a true answer holds the rewrite")

	requested, err := answeringNo.Plan(q, query.Bindings{}, nil)
	require.NoError(t, err)
	require.NotSame(t, made, requested, "the cache serves the plan only where the sources answer as when it was made")
	lookups, rewritten = getElseFormsIn(requested.Phases)
	require.Equal(t, [2]int{1, 0}, [2]int{lookups, rewritten}, "the plan made under a false answer keeps the get-else")
}

// A planner given no answer declines every get-else rewrite: its plan keeps
// the get-else as a per-tuple lookup.
func TestAPlannerGivenNoAnswerDeclinesTheGetElseRewrite(t *testing.T) {
	q, err := parser.ParseQuery(`[:find ?e ?v :where [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]`)
	require.NoError(t, err)
	plan, err := NewClauseBasedPlanner(nil, PlannerOptions{EnableAlgebraOptimizer: true}).Plan(q, query.Bindings{}, nil)
	require.NoError(t, err)
	lookups, rewritten := getElseFormsIn(plan.Phases)
	require.Equal(t, [2]int{1, 0}, [2]int{lookups, rewritten})
}

// A plan carries the answers its embedded body plans were made under: one
// plan cache does not serve a plan whose rule body holds the get-else rewrite
// to a planner whose sources answer otherwise, though the query's own clauses
// hold no get-else.
func TestThePlanCacheReadsTheAnswersOfTheBodyPlansAPlanEmbeds(t *testing.T) {
	rules, err := parser.ParseRules(`[[(valued ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`)
	require.NoError(t, err)
	parsed, err := parser.ParseQuery(`[:find ?e ?v :in $ % :where (valued ?e ?v)]`)
	require.NoError(t, err)
	q, err := query.Prepare(parsed, rules)
	require.NoError(t, err)
	bindings := query.Bindings{Rules: rules}
	cache := NewPlanCache(10, 0)
	answeringYes := NewClauseBasedPlanner(nil, PlannerOptions{Cache: cache, EnableAlgebraOptimizer: true, MatchAnswersLookup: answeringTrue})
	answeringNo := NewClauseBasedPlanner(nil, PlannerOptions{Cache: cache, EnableAlgebraOptimizer: true, MatchAnswersLookup: answeringFalse})

	bodyForms := func(plan *RealizedPlan) (lookups, rewritten int) {
		require.NotEmpty(t, plan.Evaluations, "the plan carries the invocation's evaluation")
		for _, evaluation := range plan.Evaluations {
			for _, stratum := range evaluation.Strata {
				for _, body := range stratum.Bodies.Plain {
					l, r := getElseFormsIn(body.Plan.Phases)
					lookups += l
					rewritten += r
				}
			}
		}
		return lookups, rewritten
	}

	made, err := answeringYes.Plan(q, bindings, nil)
	require.NoError(t, err)
	lookups, rewritten := bodyForms(made)
	require.Equal(t, [2]int{0, 1}, [2]int{lookups, rewritten}, "the body planned under a true answer holds the rewrite")

	requested, err := answeringNo.Plan(q, bindings, nil)
	require.NoError(t, err)
	require.NotSame(t, made, requested, "the cache serves the plan only where its bodies' sources answer as when it was made")
	lookups, rewritten = bodyForms(requested)
	require.Equal(t, [2]int{1, 0}, [2]int{lookups, rewritten}, "the body planned under a false answer keeps the get-else")
}

// A plan carries the answers of the scope plans its evaluation's rewrite
// read. The rewrite of an invocation's set takes a rule's body, a form's
// branch beside an adorned invocation and a subquery's inner query beside one
// from scope plans, and a scope whose get-else it turned into a scan asked a
// question. The plan made under a true answer records the question, and one
// plan cache does not serve it to a planner whose sources answer otherwise.
func TestThePlanCacheReadsTheAnswersOfTheScopePlansARewriteReads(t *testing.T) {
	cases := []struct {
		name  string
		rules string
		query string
	}{
		{"a body",
			`[[(ga ?e ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`,
			`[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`},
		{"a branch beside an adorned invocation",
			`[[(ga ?e ?v) [?e :t/name _] (or-join [[?e] ?v] (and (gb ?e) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]))]
			  [(gb ?e) [?e :t/kind _]]]`,
			`[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`},
		{"a subquery beside an adorned invocation",
			`[[(ga ?e ?v) [?e :t/name _] [(q [:find ?v :in $ % ?x :where (gb ?x) [?x :t/name _] [(get-else $ ?x :t/a "none") ?v]] $ % ?e) [[?v]]]]
			  [(gb ?x) [?x :t/kind _]]]`,
			`[:find ?e ?v :in $ % :where [?e :t/name _] (ga ?e ?v)]`},
		{"a free invocation reaching a required predicate",
			`[[(ga ?e ?v) [?e :t/name _] (gr ?e ?v)]
			  [(gr [?e] ?v) [?e :t/name _] [(get-else $ ?e :t/a "none") ?v]]]`,
			`[:find ?e ?v :in $ % :where (ga ?e ?v)]`},
	}
	question := SourceAnswer{Source: datalog.SymDollar, Attr: datalog.NewKeyword(":t/a"), Answer: true}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rules, err := parser.ParseRules(c.rules)
			require.NoError(t, err)
			parsed, err := parser.ParseQuery(c.query)
			require.NoError(t, err)
			q, err := query.Prepare(parsed, rules)
			require.NoError(t, err)
			bindings := query.Bindings{Rules: rules}
			cache := NewPlanCache(10, 0)
			answeringYes := NewClauseBasedPlanner(nil, PlannerOptions{Cache: cache, EnableAlgebraOptimizer: true, MatchAnswersLookup: answeringTrue})
			answeringNo := NewClauseBasedPlanner(nil, PlannerOptions{Cache: cache, EnableAlgebraOptimizer: true, MatchAnswersLookup: answeringFalse})

			made, err := answeringYes.Plan(q, bindings, nil)
			require.NoError(t, err)
			rewritten := false
			for _, evaluation := range made.Evaluations {
				rewritten = rewritten || evaluation.Rewritten
			}
			require.True(t, rewritten, "the invocation evaluates a rewritten set")
			require.Contains(t, made.SourceAnswers, question, "the plan records the question its rewrite's scope plan asked")

			requested, err := answeringNo.Plan(q, bindings, nil)
			require.NoError(t, err)
			require.NotSame(t, made, requested, "the cache serves the plan only where the sources answer the rewrite's scope plans as when it was made")
		})
	}
}

// The options answer through their MatchAnswersLookup, and false for every
// source when they carry none.
func TestPlannerOptionsAnswerThroughTheirMatchAnswersLookup(t *testing.T) {
	source, attr := datalog.SymDollar, datalog.NewKeyword(":t/a")
	require.True(t, PlannerOptions{MatchAnswersLookup: answeringTrue}.matchAnswersLookup(source, attr))
	require.False(t, PlannerOptions{MatchAnswersLookup: answeringFalse}.matchAnswersLookup(source, attr))
	require.False(t, PlannerOptions{}.matchAnswersLookup(source, attr), "options carrying no answer answer false")
}

// Options answer recorded questions alike exactly when they answer each as it
// was recorded.
func TestPlannerOptionsAnswerRecordedQuestionsAlikeWhenEachAgrees(t *testing.T) {
	a := datalog.NewKeyword(":t/a")
	b := datalog.NewKeyword(":t/b")
	opts := PlannerOptions{MatchAnswersLookup: func(_ query.Symbol, attr datalog.Keyword) bool { return attr == a }}
	require.True(t, opts.answersAlike(nil), "no recorded question")
	require.True(t, opts.answersAlike([]SourceAnswer{{datalog.SymDollar, a, true}, {datalog.SymDollar, b, false}}))
	require.False(t, opts.answersAlike([]SourceAnswer{{datalog.SymDollar, a, true}, {datalog.SymDollar, b, true}}),
		"a question the options answer otherwise")
}

// The answers evaluations embed are every recorded question of each
// stratum's plain, delta and lower bodies, and every question of the scope
// plans each evaluation's rewrite read.
func TestEvaluationAnswersCollectEveryBodyPlansQuestions(t *testing.T) {
	question := func(attr string) SourceAnswer {
		return SourceAnswer{Source: datalog.SymDollar, Attr: datalog.NewKeyword(attr), Answer: true}
	}
	body := func(attr string) PlannedBody {
		return PlannedBody{Plan: &RealizedPlan{SourceAnswers: []SourceAnswer{question(attr)}}}
	}
	evaluations := map[*query.RuleInvocation]*InvocationEvaluation{
		{}: {
			Magic: &Magic{SourceAnswers: []SourceAnswer{question(":t/rewrite")}},
			Strata: []PlannedStratum{{Bodies: Bodies{
				Plain: []PlannedBody{body(":t/plain")},
				Delta: []PlannedBody{body(":t/delta")},
				Lower: []PlannedBody{body(":t/lower")},
			}}},
		},
	}
	require.ElementsMatch(t,
		[]SourceAnswer{question(":t/rewrite"), question(":t/plain"), question(":t/delta"), question(":t/lower")},
		evaluationAnswers(evaluations))
}
