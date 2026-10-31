# An invocation continues one evaluation across its routes, as a proof

## Sources

- The code: `datalog/executor/rules.go` (`executeRuleInvocation`, `invocationSource`, `deriveInvocation`, `invocationEvaluation`, `magicSetsEvent`, `seedRelation`, `ruleEvaluation`, `newRuleEvaluation`, `extend`, `runStratum`, `complete`, `ruleContinuations`, `newRuleContinuations`, `standFor`, `clauseOf`), `datalog/executor/query_executor.go` (`DefaultQueryExecutor.Execute`, its `*query.RuleInvocation` case), `datalog/executor/bound_pattern.go` (`streamsThrough`), `datalog/executor/bound_invocation.go` (`executeBoundInvocation`), `datalog/executor/windowed_form.go` (`frameBranches`, `continuationsFor`, `newWindowedIterator`, `windowedIterator.nextWindow`, `windowedIterator.buildBranch`, `windowedIterator.Close`), `datalog/executor/kept_branch.go` (`buildBranchTuples`), `datalog/executor/scope.go` (`planScope`, `runPlan`), `datalog/executor/executor.go` (`runPhases`, `queryExecutor`), `datalog/executor/context.go` (`Context`, `WithRequiredRun`, `WithContinuations`, `Derived`), `datalog/executor/relation.go` (`MaterializedRelation.Rename`), `datalog/executor/join.go` (`HashJoinWithOptions`), `datalog/planner/scope.go` (`Scope.Query`, `PlanScope`), `datalog/planner/planner_clause_based.go` (`ClauseBasedPlanner.Plan`), `datalog/planner/framing.go` (`Framing.Branches`), `datalog/planner/scope_planning.go` (`planFormScopes`, `planInvocation`), `datalog/planner/rule_bodies.go` (`PlanEvaluation`, `InvocationEvaluation`), `datalog/planner/magic.go` (`Rewrite`).
- The scope design: `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md`.
  - Goal 2: a bound invocation costs one rewrite and one evaluation, wherever it stands.
  - Goal 10: a query is planned once per database per distinct prepared query, rule set and bound symbols.
  - §4.1: a scope's plan is cached under its text and made once per distinct scope text. A bound invocation over a streaming group is planned at the handler, since its scope exists only on a run-time fact.
  - §4.2: a branch holding an invocation continues its fixpoint across the windows. Each window's closure from the carried sets is the least fixpoint above them, and by induction over the windows it is the fixpoint over the keys seen so far. The seed decides which keys the adorned predicates carry, never what a key carries.
  - §4.3: the continuations are the evaluations the branches' invocations continue across windows. The outermost windowed form's iterator owns them, and every form inside it shares them.
  - §4.4, "A bound invocation at the top level": the window is for an invocation that derives at its clause over a group that reports no size. A group held in memory seeds the derivation through its projection and joins the result at the collapse. The run a frame holds is such a group, so the invocation a frame runs is seeded through the run and never windowed again.
  - §7: a branch holding an invocation runs one rewrite and one stratum evaluation per form execution, continued across the windows, whatever the subject's size.
- The proof this one stands on: `docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md`. Its T5 states that an empty build answers the empty relation, held, without opening the probe side. Its T6 states that a join result answers no size, so a later invocation sharing its symbols takes the windowed route.
- The failure: `TestRules_InvocationUnderAFormEvaluatesOncePerQuery/subquery_in_a_branch` (`datalog/storage/rules_test.go`) and `TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes` (`datalog/storage/invocation_continuation_test.go`). Each runs a fallback whose branch is a subquery invoking `reach` at both positions, over a chain whose sink is bound. Each reports, against a top-level invocation bound at the same positions, a second rewrite and a second begin per stratum, in both optimizer modes. In the trail the window whose subquery body matches nothing derives with no window event of the invocation's own, and a window whose body matches derives under one.

## 1. Definitions

Every name below is the code's, given a symbol for the proof.

- A **clause** is a `query.Clause` value. Two clauses are **the same object** when they are one pointer, and **equal** when they render to one text.
- A **scope's plan** is the `RealizedPlan` the executor runs for a scope. Its **clauses** are the objects in its phases' `Where` lists. **inv** names an invocation clause of a scope's plan that is derived at its clause: `ctx.Derived(inv.Name)` answers nothing held.
- A **derivation at a clause c** is one call of `deriveInvocation(ctx, c, groups)`.
- An **evaluation** is one `ruleEvaluation`. It is created by `invocationEvaluation`, extended by `extend` with one seed per derivation, and completed by `complete`.
- A **form execution** is one life of a `windowedIterator` whose `ownsContinued` is true, from `newWindowedIterator` to the subject's exhaustion or `Close`. Its **continuations** C are the `ruleContinuations` that `continuationsFor` created for it. Every scope run under that iterator reads C through its context.
- The **held route** for inv is the branch of `Execute`'s invocation case that calls `executeRuleInvocation`. It derives at inv itself, with the groups in hand.
- The **windowed route** for inv is the branch that calls `executeBoundInvocation`. It frames inv and derives at the frame's clause, once per window of the frame's subject.
- The **frame** of inv is the `planner.Scope` that `frameBranches` builds for the branch `{inv}`: its required run R, its find F, and its clauses `{inv}`. Its **query** is `[:find R F :in $ % :where (scope/required R) inv]`. Its **plan** is what `planScope` answers for it. Its **clause**, written **framed**, is the invocation clause of the frame's plan whose name is not `scope/required`.
- The **standing** is the map `standsFor` of C. `standFor(framed, inv)` writes `standsFor[framed] = inv`. `clauseOf(c)` answers `standsFor[c]` when it is set and c otherwise.
- The **key** of a derivation at c under C is `clauseOf(c)`.
- A **route split** is a form execution in which one inv is derived on both routes.

## 2. Preconditions

- **P1 (the route).** `Execute`'s invocation case takes the windowed route when `inv` is not held and `streamsThrough(inv.Variables(), groups)` is true, and the held route otherwise. `streamsThrough` is true when some group sharing one of the variables answers a size below zero.
- **P2 (the held route's derivation).** `executeRuleInvocation` reads its source through `invocationSource`. For a clause not held, `invocationSource` answers `deriveInvocation(ctx, inv, groups)`. So the held route derives at inv under the context in hand.
- **P3 (the derivation, before the change).** `deriveInvocation` reads `planned` from `e.plan.Evaluations[c]`. When `ctx.continued` is set it reads `byClause[c]`, creates the evaluation through `invocationEvaluation` when none is held, and writes it under `byClause[c]`. It builds the seed from the groups through `seedRelation` under `planned.Bound`, extends the evaluation with it, and completes the evaluation at once when `ctx.continued` is nil.
- **P4 (the continuations).** `continuationsFor` answers the context's continuations when it carries any, and otherwise new ones, owned by the iterator that asked. Every derived context carries `continued` on: `forkContext`, `WithEnvironment`, `WithRequiredRun`, `WithRules`, `WithDerived`, `WithHeld`, `WithContinuations`. The owning iterator calls `continued.complete()` at the subject's exhaustion in `nextWindow` and in `Close`. `ruleContinuations.complete` calls `complete` on every evaluation held in `byClause`.
- **P5 (the frame).** `executeBoundInvocation` computes R as the variables of inv the subject or the environment binds, F as the others, and calls `frameBranches` over the branch `{inv}`. `Framing.Branches` frames a branch holding an invocation with `Required = f.Required` and `Clauses = branch`, the clause objects as given. `Scope.Query` lists the required run as find variables, then F, declares `$` and `%`, and opens the where with `(scope/required R)` followed by the scope's clauses. `planScope` calls `PlanScope` under the context's rule set and held set, and `PlanScope` calls `Plan`.
- **P6 (the plan cache).** `Plan` answers the cached plan when the cache holds one under the query, the rule set, the options, the bound symbols and the held set, and otherwise makes a plan and stores it under that key. So every later planning of a frame whose query renders to one text, under one rule set, options, bound set and held set, answers the one plan object stored first, with that object's clauses.
- **P7 (the frame's clauses).** The frame's query holds exactly two clauses in its where: `(scope/required R)` and inv. A plan carries each clause of its query once across its phases, so the frame's plan holds exactly one invocation clause other than `scope/required`. On a cache hit that clause is the stored plan's object. On a miss on the algebra path it is an object `optimizeViaAlgebra` made from the query. On a miss on the other path it is inv itself.
- **P8 (the frame's run).** `newWindowedIterator` stores the context `continuationsFor` answers, carrying C, as `it.ctx`. `buildBranch` builds a branch through `buildBranchTuples`, which calls `runPlan(it.ctx, plan, run)` with the window's run, a `*MaterializedRelation`. `runPlan` calls `runPhases(ctx.WithRequiredRun(run), plan, nil, nil)`, and `runPhases` builds one `DefaultQueryExecutor` for that plan through `queryExecutor` and runs `Execute` over the plan's phases. Inside, `Derived(query.ScopeRequired)` answers the run, so `(scope/required R)` is held and `executeRuleInvocation` answers for it `MaterializedRelation.Rename` of the run, a held relation, since R is distinct variables and the run's context carries no environment. So framed meets a held group, `streamsThrough` is false, and framed is derived on the held route, by the frame plan's executor, under `it.ctx`. The frame's first build happens at the first pull of the windowed result, after `executeBoundInvocation` has returned.
- **P9 (the evaluation).** `newRuleEvaluation` reads the rule set, the predicate and the seed predicate from `planned.Magic`. The first `extend` prepares the strata from `planned.Strata`. Each `extend` absorbs its seed into the seed predicate's set as the window's delta and runs each stratum that changed. `runStratum` fires the begin event once per stratum, at the first run that reaches it, and numbers its iterations from zero on every run. `complete` fires the complete event once per stratum that began. The evaluation's relation after a sequence of seeds is the fixpoint over the keys those seeds carry, by §4.2.
- **P10 (the two plannings agree).** `planFormScopes` plans inv's evaluation under the positions bound before inv, the symbols a group or the environment binds there. `executeBoundInvocation` takes as R the variables a group or the environment binds at inv, and the frame's planning plans framed under the positions `(scope/required R)` binds. So inv's evaluation and framed's evaluation are planned for one predicate under one adornment, under one rule set and one held set. `Rewrite` is a function of the predicate and the adornment (§4.2), and each stratum's bodies are planned under their own texts through the cache, so the two evaluations agree in `Bound`, in `Magic.Predicate`, `Magic.Seed` and the rules of `Magic.Rules`, and in the bodies of each stratum.
- **P11 (the route split exists).** Under the pull-based join a join result answers no size, and a join whose build side is empty answers a held empty relation (`PULL_BASED_HASH_JOIN_PROOF.md` T5, T6). A scope's plan that joins a held run with a pattern's held result and then invokes a predicate over the join therefore takes the windowed route in a window whose pattern matches and the held route in a window whose pattern matches nothing. The failure's trails show both routes in one form execution.
- **P12 (the events).** `invocationEvaluation` fires the magic-sets event once per evaluation it creates, when the set is a rewrite. The begin and complete events fire once per stratum per evaluation (P9).

## 3. Theorems

**T1 (one evaluation).** In a form execution whose continuations are C, for an inv whose frame no other clause of the execution frames, every derivation at inv and every derivation at inv's framed continues one evaluation, created at the first of them.

**T2 (the answers).** The tuples every route answers are unchanged. On either route the answer is the route's groups joined with the evaluation's relation for the predicate on the bound positions, and that join is the same whether the evaluation carries the other route's seeds or not.

**T3 (the events).** For such an inv the form execution reports one magic-sets event when the set is a rewrite, one begin and one complete per needed stratum, and every window's iterations numbered from zero. These are the counts of a top-level invocation bound at the same positions, which goal 2 and §7 require.

**T4 (nothing else changes).** A derivation with no continuations in the context is unchanged. A derivation at a clause nothing stands for continues under itself. A frame whose clause is inv, planned fresh on the clause-keeping path, is unchanged. A frame's clause is never itself framed, so the standing has no chain.

**T5 (the standing is total).** For every bound invocation's frame, framed exists and is unique, so `standFor(framed, inv)` records the frame's one standing.

## 4. Lemmas

### L1. The two keys, before the change (the failure)

By P2 and P3 the held route derives at inv and keys the continuation by inv. By P8 the windowed route derives at framed and keys it by framed. By P6 and P7 framed is the object of the plan the cache serves for the frame's text, which is inv only on a miss on the clause-keeping path. In the failure the baseline query frames the same text first, so the plan the form's windows are served holds the baseline's clause. By P11 one form execution derives inv on both routes. So the held route misses `byClause[framed]` and creates a second evaluation, registered under inv, and the windowed route never reads it. Each evaluation fires its own magic-sets event and its own begin and complete per stratum (P12). That is the failure's trail. ∎

### L2. The standing is written before any derivation at framed (T5, T1)

By P5 and P7 the frame's plan holds exactly one invocation clause other than `scope/required`, so framed is defined and unique, and `standFor(framed, inv)` writes the one standing the frame has. `executeBoundInvocation` writes it after `newWindowedIterator` and before it returns. By P8 the first derivation at framed happens at the first build, after the return. So at every derivation at framed in this execution `standsFor[framed]` is set. Under T1's hypothesis no other clause writes `standsFor[framed]`, so it is inv throughout. ∎

### L3. A frame's clause is never framed (T4)

By P8 a derivation at framed is on the held route inside the frame's run, since the group framed meets is the held rename of the run. So framed never reaches `executeBoundInvocation`, is never the clause of a frame, and is never the first argument of `standFor`. So `clauseOf(framed)` is one read, and `clauseOf(inv)` for an inv that is a scope's clause and not a frame's clause answers inv. ∎

### L4. Both routes key by inv (T1)

A derivation at inv on the held route keys by `clauseOf(inv)`, which is inv by L3. A derivation at framed on the windowed route keys by `clauseOf(framed)`, which is inv by L2. Both read and write `byClause[inv]` of the one C the execution's contexts carry (P4). The first derivation finds nothing under inv and creates the evaluation through `invocationEvaluation`. Every later one, on either route, finds it. So one evaluation serves every derivation of inv in the execution. ∎

### L5. The seeds and the relation (T2)

Each derivation extends the one evaluation with its own seed. On the held route the seed is `seedRelation` over the groups in hand under `planned.Bound`. On the windowed route it is `seedRelation` over the held rename of the window's run under the frame plan's `planned.Bound`, which equals inv's by P10. By P10 the evaluation created from either plan's `planned` evaluates the same set over the same strata, so which route creates it does not matter. By P9 the evaluation's relation after any sequence of seeds is the fixpoint over the keys those seeds carry, and by §4.2 a key's tuples in it do not depend on the other keys seeded. Each route joins its own groups with that relation on the bound positions: the held route at the collapse that follows `executeRuleInvocation`, the windowed route at the frame's own collapse of `(scope/required R)` with framed's result. The join keeps the tuples of the route's keys alone. Before the change each route's evaluation was seeded by that route's keys alone and answered those keys' tuples. So each route answers the same tuples. ∎

### L6. The events (T3)

By L4 one evaluation is created, so `invocationEvaluation` runs once and fires the magic-sets event once when the set is a rewrite (P12). By P9 the begin event fires once per stratum, at the first run that reaches it, whichever route's derivation that is. By P4 the owning iterator completes C once at the subject's exhaustion or at `Close`, and `ruleContinuations.complete` reaches the one evaluation once, firing the complete event once per stratum that began. Every `runStratum` numbers its iterations from zero, so each window's iterations number from zero as before. A top-level invocation bound at the same positions runs one evaluation over its one subject and reports the same events, by goal 2. ∎

### L7. Nothing else changes (T4)

With `ctx.continued` nil `deriveInvocation` reads no key and completes the evaluation at once, as before. A clause nothing stands for keys by itself, as before (L3). When framed is inv, `standFor(inv, inv)` makes `clauseOf(inv)` answer inv, and both routes key by inv, as before. By L3 no standing stands for a clause that itself stands for another. ∎

## 5. Tests

The change carries these tests, each pinning the theorem named.

- `TestRules_InvocationUnderAFormEvaluatesOncePerQuery/subquery_in_a_branch` (storage). The windowed route then the held route: the sink is scanned last. Red before the change with a second rewrite and a second begin per stratum, in both optimizer modes (T1, T3).
- `TestRules_AnInvocationContinuesOneEvaluationWhicheverRouteEachWindowTakes` (storage, new). The held route then the windowed route: the subject is a relation input with the sink first. Red before the change the same way, in both modes. It asserts the answers (T2), the counts against the top-level invocation's (T1, T3) and the per-window numbering (T3).
- `TestAFramesInvocationClauseContinuesUnderTheClauseItStandsFor` (executor, new). A clause nothing stands for continues under itself, and a frame's clause continues under the clause it stands for (L2, L3).
- The other forms of `TestRules_InvocationUnderAFormEvaluatesOncePerQuery`, `TestRules_ABoundInvocationOverARelationInputRunsOnce` and `TestRules_TwoInvocationsUnderAFormKeepTheirOwnSeeds` (storage), run unchanged (T4).
- `TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation` and `TestQueriesAnswerAlikeUnderBothOptimizerModes` (storage), the differentials over rule evaluation and real queries, run unchanged (T2).

## 6. The change the proof licenses

- `rules.go`, `ruleContinuations`: gains `standsFor`, `standFor` and `clauseOf`, as §1 defines them.
- `rules.go`, `deriveInvocation`: reads and writes `byClause` under `clauseOf(c)` in place of c.
- `bound_invocation.go`, `executeBoundInvocation`: after `newWindowedIterator`, records through `standFor` that the frame plan's invocation clause other than `scope/required` stands for inv.
- `docs/proposals/SCOPE_EXECUTION_AND_CONTRACTS.md` §4.4, the bound-invocation bullet: states that the frame's invocation clause stands for the clause the frame was planned for, and that a derivation at either continues the one evaluation.
- The tests of §5.

## 7. What the proof does not cover

- Two invocation clauses of one form execution whose frames render to one text. Each `executeBoundInvocation` call writes the standing for its clause, and a frame's build between another clause's write and this clause's next continues the other clause's evaluation. Each evaluation is still a fixpoint over the keys it was seeded with, so L5's argument holds for the answers. The evaluations such a pair runs are at most two, where before the change they were at most three: one under the frame's clause for every windowed run of either, and one under each clause for its held runs.
- A data pattern's frame, which holds no invocation and creates no evaluation.
- The cost of the standing: one map read per derivation under continuations and one map write per windowed invocation, per run and never per tuple.

## 8. Alternatives excluded

- The evaluation keyed by a value both plans carry. `Rewrite` builds a fresh `*Magic` and a fresh rule set on every call, and `PlanEvaluation` a fresh `*InvocationEvaluation`, so no object is shared. Interning them in the planner is a cache the planner does not have, keyed on the rule set's structure, the predicate, the adornment and the held set, and it changes the plan's contract that it carries each invocation's evaluation by its clause. It is not licensed here.
- A plan per framing, with the frame's clause being inv. Goal 10 excludes it: a scope is planned once per distinct text.
- The held route canonicalized through the cache, by planning inv's frame to read its clause. It renders the frame's text on every held derivation, and it keys an evaluation by a rendering.
- The evaluation created at `executeBoundInvocation` and registered under both clauses. It fires the magic-sets event at the form's construction, before any window runs, and fires it for a subject that yields no window.
