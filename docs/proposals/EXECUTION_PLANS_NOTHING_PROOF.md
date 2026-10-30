# Running a planned query plans nothing, as a proof

## Sources

- The code: `datalog/planner/planner_clause_based.go` (`ClauseBasedPlanner.Plan`, `planWithBindings`), `datalog/planner/interface.go` (`PlanQuery`), `datalog/planner/scope.go` (`Scope.Query`, `PlanScope`), `datalog/planner/scope_planning.go` (`planFormScopes`, `planInvocation`, `planFormScope`), `datalog/planner/framing.go` (`Framing`, `Framing.Branches`, `BranchReads`, `HoldsInvocation`), `datalog/planner/cache.go` (`PlanCache`), `datalog/executor/executor.go` (`Executor.planner`, `ExecuteWithRelations`, `runPhases`, `materializePhaseBoundary`), `datalog/executor/executor_utils.go` (`BindQueryInputs`), `datalog/executor/scope.go` (`planScope`, `runScope`, `requiredRun`, `executeOrJoinClause`), `datalog/executor/windowed_form.go` (`frameBranches`, `windowFrame.windowRun`), `datalog/executor/bound_pattern.go` (`streamsThrough`, `executeBoundPattern`), `datalog/executor/bound_invocation.go` (`executeBoundInvocation`), `datalog/executor/query_executor.go` (`DefaultQueryExecutor.Execute`), `datalog/annotations/types.go`.
- The scope design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md`.
  - Goal 7: cost is measured, never claimed. An option that is off costs nothing.
  - Goal 10: a query is planned once per database per distinct prepared query, rule set and bound symbols.
  - §4.1: every form of the prepared query is framed and planned at `PlanQuery`, and the executor frames the same form from the groups in hand and reads each frame's plan from the cache. A frame's text is a function of the text and of which header symbols are bound before the form, which the planner reads from the phases and the executor from the groups. Two routes plan at the handler, a data pattern and a bound invocation over a streaming group, since their scope exists only when a group reports no size. The alternative, a scope planned for every data pattern and invocation at `PlanQuery`, is excluded there for the cost of a plan per pattern that a subject held in memory never reads.
  - §4.4: the two routes' framing. A group held in memory binds a pattern through the matcher or seeds an invocation through its projection.
  - §4.9: every fact the engine reports is an event named in `datalog/annotations/types.go`. `Explain` and `ExplainAlgebra` plan under no handler and report nothing on any path.
  - §7: executing a query `Explain` planned plans no body, pinned by `TestExplain_ExecutingAnExplainedQueryPlansNoBody`.
- The proofs this one stands on: `docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md` T6, a join result reports no size, and `docs/proposals/INVOCATION_CONTINUATION_PROOF.md` P8, a frame's run derives on the held route.
- The failure: `TestExplain_ExecutingAnExplainedQueryPlansNoBody` (`datalog/storage/explain_plans_rule_bodies_test.go`), its cases "a lower seed-restricted predicate" and "a required predicate derived at its clause inside a body", in both optimizer modes. Each explains a query, records the plan cache's misses and size, runs the query to its end, and finds one plan more than the planner made. Under the pull-based join a pattern after the body's join takes the windowed route, and its frame is planned at the handler.
- The decision: the owner chose both halves, the instrument and the design change, so that `Explain` accounts for every plan a query can run and running a planned query plans nothing.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **plan made** is one call of `planWithBindings` from `Plan`: the cache held no plan under the request's key, and the planner made one and stored it.
- The **walk** is `planFormScopes` over a plan's realized phases. Per phase it carries **B**, the bound set at each clause: the symbols the phase's relation inputs bind and the symbols each earlier clause of the phase provides, and **E**, the phase's scalar inputs.
- A **clause's symbols** are `pattern.Symbols()` for a data pattern and `inv.Variables()` for an invocation.
- The **window framing** of a clause with symbols S over a bound set X is `WindowFraming(S, X)`: `Required` is the members of S in X, in S's order; `Find` is the other members of S, each as a find variable; `Given` and `StandsFor` are `Required`. The planner package holds this one function, and the walk and the executor's two routes call it.
- The **window scope** of a clause over X is the one scope `WindowFraming(S, X).Branches({{clause}})` answers. Its query is `[:find R F :in $ % :where (scope/required R) clause]`, with R the required run and F the find.
- A **windowable clause** of a phase is a data pattern, or an invocation derived at its clause, that stands after a clause of the phase other than `(scope/required …)` and shares a symbol with its bound set: B for a pattern, B with E for an invocation.
- The **event** is `PlannerPlanMade`, named `planner/plan.made` in `datalog/annotations/types.go`. `Plan` emits it once per plan made, under a handler, carrying the query planned under `KeyQuery` and, under `bindings`, the bindings the plan was made under: the symbols bound, the rule set and the held set, typed as the planner's `Bindings`. It carries nothing under the scan family's `bound`, which names a run's bound positions.
- A **run** is one `ExecuteWithRelations` after `PlanQuery` has answered its plan: `executeRealizedPlan` over the plan's phases to the relation's end.

## 2. Preconditions

- **P1 (where a run plans).** `Executor.planner` is read at two production sites: `ExecuteWithRelations`, which calls `PlanQuery` once before the run, and `planScope`. `planScope` is called by `frameBranches` and by `runScope`. `runScope` is called once, by the or-join's unit route in `executeOrJoinClause`. `frameBranches` is called by every windowed form: the or-join, the or-default-join, the not-join, the subquery, `executeBoundPattern` and `executeBoundInvocation`. So every plan a run can ask for is asked through `planScope`.
- **P2 (the cache).** `Plan` answers the cached plan when the cache holds one under the query, the rule set, the options, the bound symbols and the held set, and otherwise makes one and stores it. `PlanQuery` is `Plan`. `PlanScope` frames the scope through `Scope.Query` and calls `Plan`.
- **P3 (what `PlanQuery` plans today).** The walk plans every form's frames through `planFormScope`, which frames the branches as the executor's forms frame them and plans each through `PlanScope`, and every invocation's evaluation through `planInvocation`, whose bodies are planned through `PlanStratum` and `planBody` under `Plan`. Each of those plans is walked in turn, so every form and body a query reaches is planned at `PlanQuery`. The or-join's unit route runs `runScope` over the scope `{Required, Find, Clauses: branch}` of its form, which is the scope `Framing.Branches` answers for that branch when no given symbol is carried, as `planFormScope` frames it when no header symbol is bound before the form. `TestExplain_ExecutingAnExplainedQueryPlansNoBody`'s case "an invocation inside an or-join branch" is green on that route.
- **P4 (the two routes today).** `executeBoundPattern` takes as required the pattern's symbols the subject carries and as outputs the rest. `executeBoundInvocation` takes as required the invocation's variables the subject or the environment binds and as outputs the rest. Each builds `planner.Framing{Required, Find, Given: Required, StandsFor: Required}` through `frameBranches`, frames `{{clause}}` through `Framing.Branches`, and plans the scope through `planScope`. The frame's text is the window scope's. Neither frame is planned at `PlanQuery`, so a run that reaches either route makes a plan on its first reach of that text.
- **P5 (when a clause windows).** `Execute` takes the windowed route for a pattern when `streamsThrough(pattern.Symbols(), groups)` is true, and for an invocation not held when `streamsThrough(inv.Variables(), groups)` is true. `streamsThrough` is true when a group sharing a symbol answers a size below zero. The groups a phase starts with are held: the bound inputs (`BindQueryInputs` answers a materialized relation), a non-final phase's result (`materializePhaseBoundary`), and a scope's run under `scope/required` (`windowFrame.windowRun` and `requiredRun` answer materialized relations). So a group answering no size holds the result of a clause of the same phase other than `scope/required`, and a clause that windows stands after such a clause and shares a symbol with a group, hence with B.
- **P6 (the bound sets agree).** At run time a group binds the symbols its clauses provide, the phase's inputs and the scope's run, as the plan's phases state; this is the premise every form's frame rests on (§4.1, "which the planner reads from the phases and the executor from the groups"). `findOuterRelation` joins every group holding a symbol of the clause, so the subject's symbols hold every symbol of the clause a group binds, and no other symbol of the clause. The environment at run time is the scope's single-valued inputs, the phase's scalar inputs E in the walk. So the members of S the executor's bound set holds are the members of S the walk's bound set holds, for a pattern over B and for an invocation over B with E.
- **P7 (a window scope's own walk).** The window scope's query holds two clauses, `(scope/required R)` and the clause. In its walk the clause stands after no clause other than `scope/required`, so it is not windowable, and the walk plans no window scope inside a window scope.
- **P8 (the event's site).** `Plan` is the one function that makes a plan. Under a handler it emits `PlannerPlanMade` once after `planWithBindings` answers, carrying the query. Under no handler it emits nothing, and the guard is at the call site, so a nil handler costs no argument preparation (goal 7). `Explain` plans under no handler (§4.9).
- **P9 (the executor's path).** `executeBoundPattern` and `executeBoundInvocation` read their frame's plan through `planScope`, whatever made it, and run it as `INVOCATION_CONTINUATION_PROOF.md` P8 states. No other step of the executor reads the plan cache.

## 3. Theorems

**T1 (every plan a run can make is planned at `PlanQuery`).** For every clause that windows at run time, the walk planned its window scope, and the plan `planScope` asks for is served from the cache.

**T2 (a run plans nothing).** A run of a query `PlanQuery` has planned asks `planScope` only for plans the cache holds. So it makes no plan, emits no `PlannerPlanMade`, and leaves the cache's misses and size as `PlanQuery` left them. A second run of the same query does the same.

**T3 (`Explain` accounts for every plan).** After `Explain`, which is `PlanQuery`, the cache holds every plan a run of the query reads.

**T4 (no drift).** The frame the executor asks for and the frame the walk planned are one text under one bindings key, by construction: both are `WindowFraming` over the clause's symbols and a bound set, the bound sets agree on the clause's symbols, and `Framing.Branches` and `Scope.Query` are the one framing and the one rendering.

**T5 (the walk terminates).** Planning the window scopes adds no cycle to the walk: a window scope's own walk plans no window scope.

**T6 (answers unchanged).** Every run answers the tuples it answered before: the executor's path is unchanged, and each frame it reads is equal in text and bindings to the one it made at run time before.

**T7 (the instrument discriminates).** A run that makes any plan, a body's or a frame's, emits `PlannerPlanMade` and moves the cache's miss count. So a test asserting no event and no moved count during a run of an explained query fails on any plan made, and names the plan in the event.

## 4. Lemmas

### L1. A windowing clause is windowable (T1)

By P5 a clause that windows stands after a clause of its phase other than `scope/required` and shares a symbol with a group, so with B. A pattern that windows is a data pattern. An invocation that windows is not held, so it is derived at its clause, and `planInvocation` plans its evaluation at the same position of the walk. So the clause meets the definition of a windowable clause in the walk of the plan the run executes. ∎

### L2. The walk plans the frame the run asks for (T1, T4)

The walk plans, for each windowable clause, the window scope `WindowFraming(S, X).Branches({{clause}})` over X, B for a pattern and B with E for an invocation, under the bindings the plan is walked with: the rule set and the held set. The run frames the same clause through `WindowFraming(S, X')` with X' the subject's symbols, with the environment's for an invocation, and plans under the context's rule set and held set, which are the plan's (`planScope`'s doc). By P6 the members of S in X' are the members of S in X, in S's order, so `Required` and `Find` are equal, and `Framing.Branches` and `Scope.Query` render one text. By P2 the cache key is that text, the rule set, the options, the bound symbols and the held set, all equal. So `Plan` serves the plan the walk stored. ∎

### L3. A run plans nothing (T2, T3)

By P1 a run's plans are asked through `planScope` from the windowed forms and the unit route. The forms' frames and the unit route's scopes are served (P3). The two routes' frames are served (L1, L2). So every `Plan` a run reaches is a hit (P2), no plan is made, and by P8 no event fires and no miss is counted. `PlanQuery` is `Plan` (P2), so what `Explain` plans is what a run of the query reads. The second run reads the same cache. ∎

### L4. The walk terminates (T5)

Before the change the walk terminates: a form's frames and a body's plans are walked in turn, and each is a scope of the prepared query or of the rule set. Each window scope the change adds holds `scope/required` and one clause of a scope already walked, and by P7 its own walk adds no window scope. So the walk adds, per scope it already walks, at most one window scope per windowable clause, each of depth one, and terminates. ∎

### L5. The answers (T6)

By P9 the executor's execution path reads its frame's plan through `planScope` and nothing else of the cache. By L2 the plan served is equal in text and bindings to the plan it made before the change, and a plan is a function of its text and bindings (P2, goal 10). So every window runs the plan it ran before, over the same subject, and answers the same tuples. The forms, the unit route and the held routes are untouched. ∎

### L6. The instrument (T7)

Every plan made passes through `Plan` (P2, P8), where the event fires and the miss is counted. A plan made during a run, whatever its shape, fires the event with its query. A test asserting no event between the run's start and end, and the cache's misses and size unchanged, fails on the first plan made and reads its query from the event. `Explain` emits nothing (P8), so the events a test collects are the run's alone. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestExplain_ExecutingAnExplainedQueryPlansNothing` (storage), the renamed and extended `TestExplain_ExecutingAnExplainedQueryPlansNoBody`: the database runs under a handler collecting `PlannerPlanMade`; after `Explain` the query runs to its end, and the test asserts no event, the cache's misses and size unchanged, and the same again for a second run, in all six cases and both optimizer modes (T2, T3, T7). Its two red cases are the red of record, by the cache count. T7 predicts the event assertion's verdict on those two cases: a plan is made during the run, so the event fires and the assertion fails, until the walk plans the window scopes.
- `TestPlanQuery_PlansTheWindowScopeOfAWindowableClause` (planner, new). Under a handler collecting `PlannerPlanMade`, `PlanQuery` over a query whose second pattern shares a symbol with its first reports the window scope's text among the plans made, and `PlanQuery` over a scope whose one clause stands after `scope/required` alone reports no window scope (T1, T5).
- `TestAWindowedClauseReadsItsFrameFromThePlan` (executor, new). An executor plans a query through its planner under a handler, runs it, and asserts no `PlannerPlanMade` during the run, for a pattern after a join and for an invocation after a join (T2, T4).
- `TestAPatternOverAJoinResultTakesTheWindow` (executor), `TestRules_InvocationUnderAFormEvaluatesOncePerQuery` and `TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes` (storage), run unchanged (T6).
- `TestSubqueryConstantArgument_ExecutingAnExplainedQueryPlansNothing` (storage), run unchanged (T2).
- `TestQueriesAnswerAlikeUnderBothOptimizerModes` and `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` (storage), the differentials, run unchanged (T6).
- `make test`, the gate.

## 6. The change the proof licenses

- `datalog/annotations/types.go`: `PlannerPlanMade`, named `planner/plan.made`, with its doc under the query lifecycle, carrying the query under `KeyQuery` and the bindings the plan was made under, typed, under `bindings`.
- `datalog/planner/planner_clause_based.go`, `Plan`: emits `PlannerPlanMade` under a handler after a plan is made.
- `datalog/planner/framing.go`: `WindowFraming(symbols, bound)`, as §1 defines it.
- `datalog/planner/scope_planning.go`, `planFormScopes`: plans the window scope of every windowable clause, through `PlanScope`, beside the form frames and the evaluations it plans.
- `datalog/executor/bound_pattern.go` and `datalog/executor/bound_invocation.go`: build their framing's required run and find through `planner.WindowFraming` over the subject's symbols, with the environment's for the invocation.
- `datalog/executor/windowed_form.go`, `frameBranches`'s doc: every frame it reads was planned at `PlanQuery`.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1: the two routes' frames are planned at `PlanQuery`.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1: executing a planned query plans nothing.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.1: the former exclusion's cost is restated as the cost accepted.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4: the two routes' bullets state that the frame is read from the plan.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.9: names the event.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §7: states "plans nothing" and names the renamed test.
- The tests of §5.

## 7. What the proof does not cover

- A phase whose inputs are of tuple or collection shape at a clause's bound set. The walk reads a phase's scalar and relation inputs. The tests of §5 carry none at a windowable clause, and a drift there fires the event (T7).
- A pattern the attribute-fetch fusion consumes under `EnableAttributeFetchFusion`: its window scope is planned and read by nothing.
- A windowable clause whose group is held at run time, one after a predicate or an expression: its window scope is planned and read by nothing. That is the cost the former ruling cited, one plan per such clause per database per text, accepted by the decision.
- The planner's cost on a cold cache. `Explain` carries the whole of it, one plan per scope, form, body and window scope of the query. `BenchmarkPlannerWithoutCache` and `BenchmarkComplexQueryCheckpoint` are the instruments, and their figures belong in `PERFORMANCE_STATUS.md`.

## 8. Alternatives excluded

- The two routes left at the handler, with the test narrowed to "plans no body" through the event. `Explain` then accounts for less than a run plans, and a run keeps planning. The decision took both halves.
- The walk simulating the collapse, to plan a window scope only for a clause whose group streams. That is a second computation of which groups form, in the planner, the shape §4.1 excludes for the rewrite's reader.
- A listing of the cache's plans as the instrument. The handler is the one channel every fact the engine reports takes (§4.9), and the event names the plan where it is made.
