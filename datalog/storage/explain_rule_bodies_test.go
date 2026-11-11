package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

func openRuleBodiesDB(t *testing.T) *Database {
	t.Helper()
	s, err := schema.NewBuilder().
		Attribute(":item/kind").Type(schema.TypeKeyword).Add().
		Attribute(":item/next").Type(schema.TypeRef).Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: s})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

// reachRules derives the items an item reaches along :item/next.
const reachRules = `[[(reach ?a ?b) [?a :item/next ?b]]
                     [(reach ?a ?c) [?a :item/next ?b] (reach ?b ?c)]]`

// TestExplainRendersTheBodiesOfTheRulesAQueryInvokes pins that the rendered
// plan of a query invoking a rule carries the plan of the rule's body, its
// patterns with their index and selectivity, as it carries the query's own
// patterns, so the rendering shows what the executor runs for the invocation.
func TestExplainRendersTheBodiesOfTheRulesAQueryInvokes(t *testing.T) {
	db := openRuleBodiesDB(t)

	const rules = `[[(linked-widget ?e) [?e :item/kind :kind/widget] [?e :item/next ?n]]]`
	const q = `[:find ?e :in $ % :where (linked-widget ?e)]`
	plan, err := db.Explain(q, rules)
	require.NoError(t, err)
	require.NotEmpty(t, plan.Evaluations, "the plan holds what the invocation evaluates, the body's plan among it")

	rendered := plan.String()
	t.Logf("rendered plan:\n%s", rendered)
	require.Contains(t, rendered, "(linked-widget ?e)", "the invocation renders in its phase")
	assert.Contains(t, rendered, "[?e :item/kind :kind/widget]", "the body's first pattern renders with its index and selectivity")
	assert.Contains(t, rendered, "[?e :item/next ?n]", "the body's second pattern renders with its index and selectivity")
}

// TestExplainRendersTheDeltaBodiesOfARecursiveRule pins that the rendered
// evaluation of a recursive predicate carries the delta body its fixpoint
// runs, with the predicate the copy is marked on.
func TestExplainRendersTheDeltaBodiesOfARecursiveRule(t *testing.T) {
	db := openRuleBodiesDB(t)

	plan, err := db.Explain(`[:find ?a ?c :in $ % :where (reach ?a ?c)]`, reachRules)
	require.NoError(t, err)
	rendered := plan.String()
	t.Logf("rendered plan:\n%s", rendered)

	var delta int
	for _, evaluation := range plan.Evaluations {
		for _, stratum := range evaluation.Strata {
			delta += len(stratum.Bodies.Delta)
		}
	}
	require.Positive(t, delta, "the recursive predicate's evaluation runs a delta body")
	assert.Contains(t, rendered, "[?a :item/next ?b]", "the bodies' pattern renders")
	assert.Contains(t, rendered, "delta body, marked reach", "the delta body renders with the predicate its copy is marked on")
}

// TestExplainRendersABoundInvocationAsItsMagicSetsRewrite pins that the
// rendered evaluation of an invocation with a bound argument says that it
// evaluates a magic-sets rewrite, names the adorned predicate the rewrite
// derives, and renders the rewritten bodies.
func TestExplainRendersABoundInvocationAsItsMagicSetsRewrite(t *testing.T) {
	db := openRuleBodiesDB(t)

	start := datalog.NewIdentity("rule-bodies:start")
	plan, err := db.Explain(`[:find ?c :in $ % ?a :where (reach ?a ?c)]`, reachRules, start)
	require.NoError(t, err)
	rendered := plan.String()
	t.Logf("rendered plan:\n%s", rendered)

	require.NotEmpty(t, plan.Evaluations)
	for _, evaluation := range plan.Evaluations {
		require.True(t, evaluation.Rewritten, "a bound invocation evaluates a magic-sets rewrite")
		assert.Contains(t, rendered, "magic-sets rewrite deriving "+evaluation.Magic.Predicate.String(),
			"the rendering names the adorned predicate the rewrite derives")
	}
	assert.Contains(t, rendered, ":item/next", "the rewritten bodies render")
}

// TestExplainAlgebraAndAnalyzeRenderTheRuleBodies pins that the algebra
// explanation and the analysis, which render the plan, carry the bodies of
// the rules the query invokes.
func TestExplainAlgebraAndAnalyzeRenderTheRuleBodies(t *testing.T) {
	db := openRuleBodiesDB(t)

	const q = `[:find ?a ?c :in $ % :where (reach ?a ?c)]`
	explanation, err := db.ExplainAlgebra(q, reachRules)
	require.NoError(t, err)
	assert.Contains(t, explanation.String(), "[?a :item/next ?b]", "the algebra explanation renders the rule bodies")

	analysis, err := db.Analyze(q, reachRules)
	require.NoError(t, err)
	assert.Contains(t, analysis.String(), "[?a :item/next ?b]", "the analysis renders the rule bodies")
}
