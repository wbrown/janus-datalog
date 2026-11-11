# The rendered plan omits the bodies of the rules it invokes

**Status**: Resolved. The body states the defect as filed; the Resolution at the end states the fix.

The planner plans every body of every rule a query invokes when it plans the
query, and the plan carries the result: for each invocation, the set it
evaluates, whether that set is a magic-sets rewrite and under which
adornment, the strata the predicate needs, and for each body of each stratum
a realized plan of its own, with the clause order, the index each pattern
scans and its selectivity. The executor runs those body plans. The rendering
of a plan shows none of it: an invocation renders as its clause and the
variables bound before it, and the bodies render nowhere. `Explain`,
`ExplainAlgebra`, `Analyze` and the CLI's `-plan-only` all render through
that method, so a reader of a rule-set query's plan sees the plan of the
query's own clauses and, for the rules, one line each.

## The route

- `PlanEvaluation` (`datalog/planner/rule_bodies.go`) plans what an
  invocation evaluates: `EvaluatedSet` decides the set and whether it is a
  rewrite, and `PlanStratum` plans each needed stratum's bodies. Its doc:
  "The planner's walk plans every invocation's evaluation through it, and
  the plan carries the result." The result is an `InvocationEvaluation`:
  `Bound`, `Magic`, `Rewritten` and `Strata`, each stratum a
  `PlannedStratum` whose `Bodies` hold the plain, delta and lower copies,
  each a `PlannedBody` with its `Rule`, its `Plan`, a `*RealizedPlan`, and
  the predicate its copy is marked on.
- The plan holds them at `RealizedPlan.Evaluations`
  (`datalog/planner/types.go`), "what each invocation the plan's phases
  derive at its clause evaluates, by the clause, planned with the plan; the
  executor's evaluation reads it here." The executor does:
  `stratumEvaluation.run` (`datalog/executor/rules.go`) executes `body.Plan`
  for each body, and `invocationEvaluation` reads `planned.Rewritten` and
  `planned.Bound`.
- The rendering reads none of it. A phase's invocation is an
  `InvocationPlan` (`types.go`), the clause and the bound variables, and
  `RealizedPhase.String` renders those two under `Invocations:`.
  `RealizedPlan.String` renders the query and its phases and never reaches
  `Evaluations`. A body's own `RealizedPlan` has the same `String`, so what
  it would render exists; no caller renders it.
- Every plan reader renders through that method: `Database.Explain`
  (`datalog/storage/database.go`) returns the plan for `plan.String()`;
  `AlgebraExplanation.String` (`datalog/planner/explain_algebra.go`) ends
  with `e.Plan.String()` and renders the algebra and rewrites of the query's
  own clauses; `AnalyzeResult.String` (`database.go`) opens with it; and the
  CLI's `runPlanOnly` (`cmd/datalog/main.go`) prints the algebra
  explanation.

What the execution events report afterwards is part of what the plan
decided: `rules/magic-sets` carries the predicate and adornment of each
rewritten invocation (`magicSetsEvent`, `rules.go`), and the stratum events
carry the predicates, iterations and tuples. The clause order and index
choice inside a body appear in no event.

## What was observed

The reproducer below: a rule of one body with two patterns, invoked by a
query of one clause, planned by `Explain`. The plan's `Evaluations` holds one
entry, and the rendering is:

```
Realized Query Plan:
  Phases: 1

Original Query:
  [:find ?e
   :in $ %
   :where (linked-widget ?e)]

Phase 1:
  Query:
    [:find ?e
     :in $ %
     :where (linked-widget ?e)]
  Provides: [?e]
  Invocations:
    (linked-widget ?e)
```

Neither body pattern, `[?e :item/kind :kind/widget]` or `[?e :item/next ?n]`,
appears, nor the index or selectivity the planner gave either.

## Contracts it contradicts

- `Database.Explain`'s doc (`database.go`): "The plan shows: Index
  selection (EAVT, AEVT, AVET, VAET, TAEV); Selectivity scores for patterns;
  Symbol availability and binding through phases; Predicate and expression
  assignment." For a query that invokes a rule, the patterns the executor
  scans are the bodies', and the plan shows none of their index selection or
  selectivity.
- `ExplainAlgebra`'s doc (`database.go`): "the full record of what planning
  does to a query". Planning a rule-set query plans its bodies, and the
  record stops at the invocation.
- The CLI's `-plan-only` help (`cmd/datalog/main.go`): "print the compiled
  algebra, every rewrite decision, the rewritten query, and the physical
  plan". The magic-sets rewrite is a rewrite decision the plan carries in
  `InvocationEvaluation.Rewritten`, and the physical plan of each body is in
  `PlannedBody.Plan`; neither prints.
- `PlanEvaluation`'s doc: "the plan carries the result". It does; the
  rendering of the plan does not.

## The fix, as filed

Derived from the structures the plan already holds:

- `RealizedPhase.String` renders, under each invocation, the invocation's
  evaluation: its adornment and whether the set is a rewrite, then each
  stratum's predicates and, under them, each body's rule head and its plan,
  rendered by the body plan's own `String`, indented, with the copy's kind,
  plain, delta or lower, and its marked predicate. A body plan that invokes
  a rule renders its invocations the same way, so a nested rule set renders
  to its depth.
- `RealizedPhase` holds no reference to the plan it belongs to, so either
  the phase's `InvocationPlan` gains the `*InvocationEvaluation` the plan
  made for its clause, set where the plan's `Evaluations` is set, or
  `RealizedPlan.String` renders the evaluations after the phases, keyed by
  the invocation clause. The first keeps each invocation's plan beside the
  invocation.

## Reproducer

`datalog/storage/explain_rule_bodies_test.go`. At `79fe33f` it fails at
both pattern assertions, the rendering above holding neither body pattern,
after the evaluations check passes.

```go
package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wbrown/janus-datalog/datalog/schema"
)

// TestExplainRendersTheBodiesOfTheRulesAQueryInvokes pins that the rendered
// plan of a query invoking a rule carries the plan of the rule's body, its
// patterns with their index and selectivity, as it carries the query's own
// patterns, so the rendering shows what the executor runs for the invocation.
func TestExplainRendersTheBodiesOfTheRulesAQueryInvokes(t *testing.T) {
	s, err := schema.NewBuilder().
		Attribute(":item/kind").Type(schema.TypeKeyword).Add().
		Attribute(":item/next").Type(schema.TypeRef).Add().
		Build()
	require.NoError(t, err)
	db, err := NewDatabaseWithOptions(DatabaseOptions{Store: NewBadgerEmulatorStore(nil), Schema: s})
	require.NoError(t, err)
	defer db.Close()

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
```

## Related

- `BUG_AN_AS_OF_HANDLE_VALIDATES_EVERY_CANDIDATE_AGAINST_STORAGE.md`,
  resolved: found on a rule-set query whose plan this rendering could not
  show.
- `BUG_THE_MAGIC_SETS_OUTNUMBER_THE_TUPLES_THEY_SAVE.md`, open: the
  rewrite's adornment per site, which this rendering would show before a
  run, is read there from the derived tuples after one.

## Resolution

- `RealizedPhase.render` (`datalog/planner/types.go`) takes the plan's
  evaluations and renders, under each invocation, the evaluation the plan's
  `Evaluations` holds for its clause. `RealizedPhase.String` renders with
  none, and `RealizedPlan.String` renders with its own.
- `InvocationEvaluation.String` renders the rewrite or the set as given,
  each stratum's predicates, and each body by kind, with its marked
  predicate and its plan. A body plan that invokes a rule renders its own
  evaluations, so a nested rule set renders to its depth.
- The evaluations stay in the one map the plan holds; no second copy goes
  on `InvocationPlan`.
- The bodies render by default, for three reasons:
  - `Explain`'s doc promises index selection and selectivity for the
    patterns.
  - For an invocation, the patterns the executor scans are the bodies'.
  - An option would leave the default rendering contradicting both
    `Explain` and `-plan-only`.
- The tests, in `datalog/storage/explain_rule_bodies_test.go`:
  - `TestExplainRendersTheBodiesOfTheRulesAQueryInvokes`
  - `TestExplainRendersTheDeltaBodiesOfARecursiveRule`
  - `TestExplainRendersABoundInvocationAsItsMagicSetsRewrite`
  - `TestExplainAlgebraAndAnalyzeRenderTheRuleBodies`
