# Outstanding-Decision Ledger

**Convention**: this is the single cross-cutting view of every decision pending the owner and every open fix ruling. Item numbers are stable — resolved items keep their number with a `RESOLVED` status rather than being deleted. Each item's source of truth is its cited document; when an item moves, update both. Grouped by kind.

**Form**: an entry is a status line plus bullets. No paragraph runs longer than a few sentences. Derivations, mechanisms and reproducer lists live in the cited document — this file records what was decided and where to read the rest.

## A. Bug-fix direction rulings

1. **Decorrelation mishandles predicate-only `:in` symbols** — RESOLVED (2026-07-20).

    - Correlation parameters are classified before the rewrite fires: data-bound, equality-bound translation, or decline with `algebra/decorrelate-skip`.
    - The correlation equality becomes the join condition. `LateralJoin` carries the call-site argument list verbatim, so declined subqueries round-trip losslessly.
    - The corrected rewrite exposed two independent engine defects, fixed under their own entries: the planner's readiness-restricted subquery deferral gate, and the executor's expression-binding unification. All seven guards green both modes.
    - `BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS`, `BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS`, `BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE` (all resolved).

2. **Algebra bridge compiles in source order (R4)** — RESOLVED (2026-07-20). Direction ruled: pre-order by `ScopeOf` readiness.

    - Readiness single-homed as `query.ClauseBlockers`/`query.ClauseReady` (plus `query.CountProviders`), consumed by both the planner's `clauseSelectable` and the bridge's `orderClausesForCompile`.
    - `compileClausesFrom` orders every clause list — top level, OR branches, NOT bodies — before folding. The same change fixed `optimizeAlgebra` and the decorrelation inner compiles stripping `:in` from the queries they compile.
    - `TestClauseOrderIndependenceForNot` green all four legs. Two invalid-query rejection tests now reject at the ordering gate, still naming the unbindable symbol.
    - `BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER`.

3. **not-join header validation only on algebra path** — RESOLVED (2026-07-20), closed by item 8. Header completeness validates at the shared boundary via `NotJoinClause.Validate()` under `query.ValidateStaticClauseShapes`. `BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH`.

4. **Bridge rejects single-branch or-join in NOT bodies** — RESOLVED (2026-07-20). Union IR arity relaxed to ≥ 1; clause-level minimums remain language rules at `Validate`. Decompile round-trip preserved. `BUG_ALGEBRA_NOT_REJECTS_SINGLE_BRANCH_ORJOIN`.

5. **Bridge rejects or-default as first clause** — RESOLVED (2026-07-20). The lowering extends to a unit-relation outer group (childless empty `Project` = join identity): one code path, no special case. `BUG_ALGEBRA_ORDEFAULT_FIRST_CLAUSE_REJECTED`.

6. **AsOf/History drop planner options** — RESOLVED (2026-07-20). Temporal handles inherit `plannerOptions` and `parseCache`; the 18-field disposition table is enforced by a reflection classification guard, so a new `Database` field must declare its temporal-handle treatment. `BUG_TEMPORAL_HANDLES_DROP_PLANNER_OPTIONS`, `BUG_TEMPORAL_DATABASE_HANDLES_ARE_SHALLOW`.

7. **Pre-existing, distinct.**

    - `BUG_SUBQUERY_MULTIPLE_INPUTS` — RESOLVED (2026-07-20) as a **misdiagnosis**. Multi-input constant forwarding was never broken; the reproducer's date constant was a bare EDN string matched against `time.Time` datoms, and type-strict matching correctly returns empty. With `#inst` it is green both modes with no engine change. Skip removed; `TestSubqueryWithTwoInputs` pins the forwarding, alongside `MultipleInputsWithDatabase`.
    - `BUG_STREAMING_RELATION_TRANSFORMS_SHARE_RAW_ITERATOR` — RESOLVED (2026-07-20 triage), overtaken by intervening work. `StreamingRelation.Filter` went with the Filter-system deletion; `FilterWithPredicate`/`EvaluateFunction` compose from the guarded `r.Iterator()`; the requested replay test exists verbatim as `TestStreamingFilterUsesRelationIteratorAndBuildsReplayCache`.
    - `BUG_WASM_STORAGE_GC_BAD_POINTER_CRASH` — upstream Go issue. CI rerun policy in place; tracking only unless posture changes.
    - `BUG_MISSING_ON_LOOKUPLESS_MATCHER_SILENTLY_EMPTY` — RESOLVED (2026-07-20). Both defects fixed at their invariants after the owner ruled the most-correct derivation. The committed-red reproducer is green on all four legs. Discovery context preserved in the doc: initially mis-triaged and retracted, restored by owner ruling.
        - **(B) capability lies.** `AnnotatedMatcher.LookupAttribute` and both `SourceRouter.LookupAttribute` arms error on absent capability. "Capability absent" is never a value in the answer domain.
        - **(A) laundering.** The eager producer family returns errors in-band — the deferred convention is exclusively for lazily-discovered streaming errors — and the irreducible errored-empty remainder is probed by the single home `executor.EmptyRelationError` at every emptiness-branching consumer.
    - Stale-triage DONE (2026-07-20), both moved to `resolved/`. `CONDITIONAL_AGGREGATE_STREAMING_DEPENDENCY` is OBSOLETE: its reproducers, cited files, and two of four cited options are gone, and the failing configuration is unrepresentable. `SUBQUERY_POSITIONAL_MAPPING_ISSUE` is RESOLVED: its own long-term proposal is the current implementation in `executor/subquery.go`, with the tests it asked for in tree.

## B. Design ratifications

8. **Boundary-validation principle** — RATIFIED AND LANDED (2026-07-20).

    - Static properties of query text validate once at the shared user boundary — parser, qb, executor entry — pre-planning, so both modes agree. Never first-detected in mode machinery.
    - First applications: subquery binding-arity `Validate()`, not-join header completeness, or-default-join header, all under `query.ValidateStaticClauseShapes`.
    - Closed item 3 and the arity red leg. `BUG_SUBQUERY_BINDING_ARITY_VALIDATED_AT_DIFFERENT_LAYERS`.

9. **CLI optimizer-mode flag** — RESOLVED (2026-07-20). Ruled: add the flag, optimize by default.

    - `cmd/datalog` gained `-optimize` (default true; `-optimize=false` is the baseline planner), carried on the database's planner options via `openDatabaseOrEDN` so `db.Query`, single-query and interactive all honor it.
    - The two CLI executor sites use `db.NewExecutor()`, inheriting options and plan cache.
    - Six subprocess query tests loop the axis via `mode.cliFlag()`; matrix exemption closed in `docs/wip/OPTIMIZER_MODE_MATRIX.md`.

## C. API-surface decisions (July audit — details and Status lines in `docs/reviews/ANTIPATTERN_AUDIT_2026_07.md`)

10. **C1–C4, C7** — RESOLVED (2026-07-20), ruled with role facts after two under-framed first presentations.

    - C1 (`UnionRelation` subsystem), C2 (`PrependedRelation`), C3 (`TransformIterator`/`ConcatIterator`) — deleted.
    - C4 (the six algebra methods σ/⋉/▷/extend/γ on `Relation`) — **kept**. The ruling inverted the finding: the engine's free-standing operators are the deviation. Reunification is item 20.
    - C7 — split. `WrapStreamingAsLazy` and `NewLazySeqRelation` kept by ruling as the streaming→replayable bridge; the `NoDedupe` pair superseded by `NewMaterializedRelationFromSet` (boundary, admission-checked) plus interior `newMaterializedRelationFromSet` (properties-carrying); `Result` alias and `MaterializeResult` deleted.
    - Removed exported API, for the PR's compatibility notes: `UnionRelation`/`NewUnionRelation`/`UnionIterator`, `PrependedRelation`/`NewPrependedRelation`/`PrependedIterator`, `TransformIterator`/`NewTransformIterator`, `ConcatIterator`/`NewConcatIterator`, `NewMaterializedRelationNoDedupe`, `NewMaterializedRelationNoDedupeWithOptions` (renamed `NewMaterializedRelationFromSet`), `MaterializeResult`, `Result`.

11. **C5: the function namespace has one home** — RESOLVED (2026-07-20), executed by ruling.

    - `query.DefaultRegistry` gained implementations (`RegisterImplementation`/`Implementation`, mutex-guarded); `executor.RegisterCustomFunction`/`CallCustomFunction` — the published API — forward to it.
    - Consumption is live in both positions. `FunctionPredicate.Eval` consults the registry: bool results filter, and non-bool, unbound-argument and unregistered names error loudly with the unknown-function message preserved. Expression position parses to `query.CustomFunction` when the name has an implementation, results normalized via `datalog.NormalizeValue`; unknown names keep the parse-time rejection.
    - Pinned by `query/function_registry_test.go` and the both-modes `TestCustomFunctionQueryEndToEnd`.
    - The dead unexported value functions (`addValues` through `extractTimeComponent`) were deleted by owner ruling, zero callers re-verified. `custom_functions.go` now holds only the two registry forwards, closing B2's time-field-switch triplication.

12. **C6: delete the parallel constraint home** — RESOLVED (2026-07-20).

    - `executor/constraints_impl.go` removed. Only tests instantiated it; production pushdown uses `constraints.TimeRangeConstraint` plus the fetch/`ValuesEqual` route.
    - Pins relocated to their semantic homes: canonical comparison asserts directly against `datalog.CompareValues`/`ValuesEqual` in `datalog/compare_canonical_test.go`; the matcher's constraint-filtering test uses a test-local `timeWindowConstraint`.
    - Also removes the third copy of the six-way time-field switch (B2 residual).

13. **C9: keep the file, fix the error swallows** — RESOLVED (2026-07-20). The swallows were already fixed in place by the error-swallow sweep (`c6d11d0`): `CompareRelations`/`CompareRelationsIgnoreSymbolOrder` return `(bool, error)` with scan errors surfaced, `RelationDiff` and `FormatRelationSummary` include scan failures, and both tuple collectors propagate `Error()`/`Close()`. No code change remained; the ruling closed the keep-vs-delete half.

14. **D7 / E1** — D7 RESOLVED (2026-07-20); **E1 OPEN**.

    - D7: the two remaining un-hoisted per-datom position loops in `matcher_relations.go` converted. `validatingVBoundIterator.buildTuple` deleted in favor of its already-carried `InternedTupleBuilder`, unifying the T representation with the cache paths; `cardinalityManyAVETValueIterator.buildTuple` moved onto a construction-time `query.TupleIndexer`.
    - The `evaluateExpressionWithLookup` bullet was closed earlier by `alignBinding`'s precomputed table; the other two were already settled as recorded.
    - **E1 open**: the two `Materialize()` conventions. Closes via item 20's stage 1 when the reunification arc resumes.

## D. Process calls

15. **Push + PR #112 body update** — RESOLVED (2026-07-20). Pushes landed through `d6d4721`. The body was rewritten from scratch by owner ruling: the old one narrated the branch's growth chronologically and ended at Class C. The new one states what the PR is — a correctness campaign around four commitments (one value domain; one definition per concept; loud failure; optimizer transparency enforced by the dual-mode matrix) — derived from a full read of all 27 commit messages, with the compatibility and test sections carried forward.

16. **`explain_test.go` gray area** — RESOLVED (2026-07-20), ruled: matrix axis. `TestExplain`, `TestAnalyze` and `TestAnalyze_ConsumesStreamingResults` run under `optimizerModes` via `createOptimizerModeDB`; all legs green, with index-selection, bound-mask and event assertions holding on both modes.

17. **Benchmarks outside the matrix** — CONFIRMED by owner (2026-07-20): the matrix ruling covers tests; benchmarks stay outside it.

21. **Re-re-review findings (at `d6d4721`)** — both RESOLVED (2026-07-20), red-first.

    - The clause-ordering `:in`-bound-correlate hole: first-pick generator preference. The initial all-generators-first version broke the join-project selective-child lowering and was narrowed. `BUG_ALGEBRA_ORDERING_IN_BOUND_CORRELATE_FOLDS_FIRST`.
    - The predicate-position unknown-function silent empty. `BUG_PREDICATE_POSITION_UNKNOWN_FUNCTION_SILENT_EMPTY`.
    - The three-door extension was ruled and executed the same day: the rule's one home is `FunctionPredicate.Validate()` under `query.ValidateStaticClauseShapes`, so parser, qb `Build` and executor entry all reject with one message. The parser constructs unconditionally and defers to the walk. The boundary contract is "registered by the time the query enters the engine", with the per-tuple eval guard as backstop.
    - Side discovery ledgered under item 7: the lookup-less `missing?` silent empty.

22. **Third re-review finding, and the baseline defect it exposed (at `00fc6e4`)** — both RESOLVED (2026-07-21), red-first, pushed through `dacfdfa`.

    - The finding: consumer-only WHERE (`[:find ?e :in $ ?e :where (not [?e :x/flag true])]`) executed on the baseline but errored in the bridge. Fixed by seeding `compileClausesFrom` with a childless Project of the environment symbols when the first ordered clause provides nothing — the unit-relation concept from item 5. Pinned by `TestConsumerOnlyWhereWithInBoundCorrelates`, not/not-join × both modes.
    - Pinning it exposed a **baseline** defect on both modes: `executePredicate` silently skipped predicates whose required symbols are all constant-resolved scalar `:in` bindings, and nothing rendered constant-bound `:find` symbols into results.
    - Fixed as the completed derivation in `docs/wip/CONSTANT_BOUND_PREDICATE_FIX_DERIVATION.md`, including the two amendments the red baseline forced on it: fail carries as empty groups rather than a zero-symbol empty, and verdict absorption at the find boundary replaced the falsified single-phase containment assumption.
    - The shape: total predicate dispatch (evaluate-once constant arm with entity lookup; loud unprovided-symbols arm, zero hitters in the full suite), zero-symbol unit/empty verdict relations absorbed at the find boundary by the join laws, and `renderConstantFindSymbols` for find variables, pull variables and aggregate arguments.
    - Reproducers: `TestLeadingMissingWithInBoundEntity` (consumer-only and mixed, both polarities), `TestConstantOnlyPredicateFiltersUniformly`, `TestConstantInputRenderedInFind`. Gate green native + wasm; PR body updated.

23. **`Size()` semantics and sizing economics** — RULED AND DOCUMENTED (2026-07-21).

    - CORE_MODEL Pillar 3's "Size() peek-and-consume" claim did not describe the implementations: no `Relation.Size()` consumes the caller's tuples. Each answers free, answers -1, or self-realizes into the pass-through materialized form, spending only an owned upstream source.
    - Economics ruled: stream consumption is the default optimum; realizing for a size pays only when incremental processing is expensive or the realized relation is itself passed through directly afterward; a capacity hint must never be the call that forces work.
    - Call-site behavior left as-was by explicit ruling — the materialized-only pre-allocation guards in `relation_ops.go` stand. Any change is a separate discussion. The peek hazard is re-aimed at uncaptured iteration.
    - **Follow-on ruled and executed the same day: `Relation.IsEmpty()` deleted.** Bool cannot decline the way `Size()`'s -1 does, so streaming implementations either lied (`StreamingRelation` returned hardcoded `false` in the default mode) or consumed (`OrFallbackRelation` stepped an iterator), and the engine carried four "don't call IsEmpty" warnings around it. All seven implementations removed; the three production callers converted — `Size() == 0` at both matcher empty-binding sites, subsuming their `*StreamingRelation` type-guards, and consume-then-decide in the table formatter.
    - **Extended by owner ruling the same day** ("the entire concept is a virus"): `BufferedIterator.IsEmpty()`, `query.Relation.IsEmpty()` and `OrderedSet.IsEmpty()` eradicated, the last stale comments removed, and the live interface listings in ARCHITECTURE.md, RELATIONAL_ALGEBRA_OVERVIEW.md and ARCHITECTURAL_PHILOSOPHY.md corrected — the last of which also carried a false "Size() forces materialization, may lose data" row. Zero `IsEmpty` identifiers remain in Go code; historical narratives left as records.

## Added since the ledger was first drawn

18. **Correctness-measurement hierarchy** — PROPOSED (2026-07-20), **pending ruling**. Oracle tiers by independence; staged S1 clause-order invariance (also the property form of item 2) → reference evaluator plus three-way differential → grammar generator → metamorphic suites. `docs/wip/CORRECTNESS_MEASUREMENT.md`.

19. **Wasm sibling for `default_store_test`** — RESOLVED (2026-07-20). `datalog/storage/default_store_wasm_test.go` / `TestOpenDefaultStoreUsesMemory`, build-tagged `js && wasm`, asserts `openDefaultStore` returns `*MemoryStore`, mirroring the native Badger assertion. Green under js/wasm.

20. **Relation-algebra reunification arc (the C4 ruling's execution)** — DESIGN REWORKED AND PARTIALLY RATIFIED (2026-07-22); implementation queued behind correctness work. `docs/wip/RELATION_ALGEBRA_REUNIFICATION.md`, reworked against the environment-relation and set-semantics-at-birth arcs.

    - **(a) `Replayable()` rejected.** Reuse is a static property of the consuming algorithm — no consumer has ever not known it needs to reuse a relation — and interface declarations carry knowledge from the side that has it to the side that lacks it. `RequiresCopy()` passes that test; a replay predicate flows backwards, which is the deleted-`IsEmpty()` shape. Consumption is requested through `Materialize()`'s sharpened E1 contract (lazy lift, self-when-shareable), pinned behaviorally. Stage 1 closes item 14's E1 with no interface change.
    - **(b) Clauses close over the environment *relation*.** At evaluation sites the environment is consulted, not joined — Available, never Provides — which is lexical scope, and its formalization is closure. `ClosePredicate`/`CloseExpression` live in the executor (the capture is the executor's `Relation` type; closed types implement the `query` interfaces), superseding the draft's `query`-package map capture, the carrier the campaign retired.
    - Closure gives "a clause evaluates under its scope's environment" one enforcement point — the algebra lowering's forgot-to-thread `not`-leg bug becomes unrepresentable — and is the only shape under which the interface methods absorb the database-function family so the free operators can be deleted. Joins remain joins where environment symbols enter schemas: branch correlation, `notBodyBinding`, find rendering.
    - **Open sub-decisions**: `Union` error shape; `EvaluateFunction` removal compatibility bundling. Stage-2 benchmark re-gate required against the environment-arc carrier-swap result.

    Ruled and queued, not pending decisions: the collection-aggregate substrate `(min n ?x)`/`(max n ?x)`/`(distinct ?x)`; result-side collection binding `(q ...) [?coll ...]`, with one embedded semantic sub-choice (empty result drops the outer tuple vs binds `[]`); `count-distinct`; then subquery `:limit`/`:order-by`. See `TODO.md` Medium Term.

24. **The environment is a Relation (or-join `:in`-bound correlates)** — RULED AND EXECUTED (2026-07-22), both stages.

    - `:in` scalar/tuple parameters are the query scope's environment: ambient in every clause scope, never branch locals, never alpha-renamed.
    - The carrier is **one single-tuple Relation** on the executor `Context` (`Environment()`/`WithEnvironment`), bound at every query-scope boundary and reaching consumers **by join** — never as a `map[Symbol]interface{}`, the carrier the 2025-06-26 Bindings→Relations migration removed and `599266f`'s `constantBindings` reintroduced. No scalar shadows: consumers read `Symbols()`/`Get(0)` from the relation. Single-consumer derived state is a closure capture, not struct surface.
    - Or-branch visibility derives from the canon — `query.ScopeOf` Provides ∪ Correlates for explicit-header forms, branch free variables for inference forms. The executor's parallel derivations (`headerSyms`, `collectOrBranchRequiredSymbols`) are deleted.
    - **Stage 1**: the fix, with mode-matrixed reproducers in `orjoin_in_bound_correlate_test.go`, all-env residual-group absorption at the find boundary, and the `clausesNeedCorrelation` NOT-taxonomy fix.
    - **Stage 2**: the `constantBindings` map channel retired. `filterWithPredicateAndLookup`, `evaluateExpressionWithLookup`, `thetaJoinWithPredicate`, `thetaJoinPair` and `crossJoinWithExpression` take the environment Relation and bind its row by reference per evaluation. `evalPredicateOnce` deleted — env-only predicates filter the environment's single tuple, symbol-free predicates the zero-symbol unit. `renderConstantFindSymbols` is `group.Join(env.Project(missing))`. Checkpoint benchstat wall −3.4% (p=0.002), B/op and allocs flat.
    - Follow-up still pending: subquery `inputValues`/`outerValues` maps convert in their own arc.
    - `BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP` got its own arc — RESOLVED (2026-07-22). Pinned by `not_spanning_groups_test.go` and the formerly ruled-red `TestOrJoinBranchNotConsumesWhereBoundExternal`. Fix doc: `BUG_ORJOIN_IN_BOUND_CORRELATE_TREATED_AS_BRANCH_LOCAL`.
        - `executeNotClause`/`executeNotJoinClause` select the clause's subject via `findOuterRelation`: spanning groups join once, since the clause's correlation is the connector — the anti-join analog of the bridging theta-join — and keyless groups pass through. Then anti-join once on the full key set, with `replaceGroups` substituting the filtered join.
        - `notBodyBinding` renders the environment's row into the body binding, so the `not` body scope boundary treats the environment like every other. That also fixes the algebra lowering's narrow-header form.

25. **Correct-by-construction plans, with Datalog as the IR** — RAISED by owner (2026-07-22); DERIVATION DOCUMENTED AND EMPIRICALLY VERIFIED, **pending ratification and scheduling**. `docs/wip/CORRECT_BY_CONSTRUCTION_PLANS.md`.

    - The observation, from the anti-join bridging arc: the executor's after-planning machinery — per-clause subject discovery, runtime bridging, loud unbound checks, find-boundary absorption, the strict phase contracts — exists because RealizedPhases specify symbol flow but not group topology. The clause loop improvises topology at runtime, and this campaign's bugs clustered in exactly that improvised layer.
    - The algebra path constructs the topology explicitly and then discards it at decompile: the sound computation is the one thrown away.
    - **The owner's position that the IR remains expressible as Datalog is verified, not assumed.** The counter-allegation — that the language can't carry group topology — was falsified against the tree-built CLI on both planner modes: shared-symbol multi-input unification executes as the join, disjoint inputs under a spanning predicate as the theta-join, and a spanning `not-join` over two relation inputs as the bridged anti-join.
    - The construction: a plan is a DAG of query fragments whose `:in` lists name the specific prior outputs each fragment consumes. Subject selection, bridging and absorption become declared fragment properties; strict contracts become one plan-construction validation; fragment granularity is the planner's dial — declare topology at compound-clause scope boundaries, keep greedy Collapse fragment-internal between them, deferring the statistics dependency.
    - **Open for ratification**: granularity policy; the dual-mode matrix's role afterward; sequencing behind item 20, since fragments consume the reunified operator surface.
    - Incidental defect found during verification, at the CLI layer: `BUG_CLI_RELATION_INPUT_EDN_REJECTED` (open).

26. **Algebra provenance as values (`ExplainAlgebra`)** — RULED AND EXECUTED (2026-07-22).

    - The gap: downstream applications had no surface for "what is the algebra of this query" or "what transforms happened". The optimized tree died inside the bridge at decompile, `Database.Explain` returned the physical plan only with a nil handler, and transform provenance existed as annotation crumbs from one of two passes — the get-else rewrite fired invisibly.
    - Ruled: provenance returns **as values, never by event scraping**, per the annotations-are-never-a-data-channel rule. Events remain the streaming view derived from the same call sites, and every pass and bridge transform reports uniformly.
    - `algebra.RewriteRecord`/`RewriteSink` are the source of truth; `Collect` gates accumulation so the normal query path pays nothing. Pass constructors take the sink (`DecorrelationPass`, `GetElseScanRewritePass`, `DefaultPasses` — exported signature changes), and `Optimizer.WithHandler` with its by-name pass-rebuild is deleted, closing the hazard of a new pass silently losing its handler.
    - The get-else pass and the bridge's join-project insertion gain full provenance, with new events `algebra/getelse-scan-{apply,skip}` and `algebra/join-project-{apply,skip}`.
    - `planner.AlgebraExplanation` plus `QueryPlanner.ExplainPlan` (cache-bypassing in both directions), and `Database.ExplainAlgebra(queryInput, inputs...)` on the public surface, returning original, compiled tree, rewrite records, optimized tree, rewritten Datalog and physical plan. With the optimizer disabled the compiled algebra still returns — the algebra of a query is a fact about the query — alongside the baseline plan.
    - CLI wiring, owner-directed, same arc: `cmd/datalog -plan-only` plans without executing and prints the explanation, composing with `-optimize` and `-in`; `-plan-only` without `-query` errors.
    - Pinned by `algebra/provenance_test.go`, `storage/explain_algebra_test.go` and `TestCLI_PlanOnly`. Compatibility notes for the PR: the three pass-constructor signatures, `Optimizer.WithHandler` removal, and `QueryPlanner` gaining `ExplainPlan`.

27. **not/not-join binding domains include the environment; freeness is the algebra's environment representation** — RULED AND EXECUTED (2026-07-22).

    - The ruling, ACCEPT canonical on four grounds — Datomic compatibility strongest, then inference-form coherence, or-join symmetry, vacuous-truth declaration: an `:in`-bound symbol is an ordinary bound variable in every not/not-join boundness judgment. Declaring one in a not-join header is valid, and a body consuming one only through a predicate is valid with the header declaring it.
    - Derivation and the empirical six-shape × two-mode matrix at `docs/wip/NOTJOIN_HEADER_ENV_BINDING_DERIVATION.md`. The four-home class fix: `boundDomain` reified once in the algebra compiler (`compileNotJoin`'s three arms, `compileNot`).
    - `analyzeAntiJoin`'s left-child-production errors became free-requirement propagation, uniform with Select/Map/Union — **`Analyze` must not learn about inputs**: compile is value-free because plans cache across input values, so freeness at the root *is* the query's environment demand.
    - `filterWithNotJoinClause` classifies header variables: subject-carried → key; environment-bound → body constraint via `notBodyBinding`, vacuous in the key; neither → loud error, never the silent-existential intersection.
    - Second divergence found and fixed in the same arc: bare `not` with predicate-only env consumption (`compileNot`). Shape 4 — omitted header, predicate-only — stays a parse error by the header-declaration rule, mode-uniform, now pinned for the first time.
    - Pinned by ten mode-matrixed tests in `not_spanning_groups_test.go`, three `analyzeAntiJoin` structural units, and a two-shape `ExplainAlgebra` structure pin.
    - **Amended after the round-4 review**: over-declaration is rejected on both modes. A header symbol the body neither produces nor consumes declares a correlation that does not exist — declared ⇔ produced-or-consumed, with or-join symmetry decisive. Executor-only fix mirroring the compile arm's message, plus two mode-matrixed pins covering the `:in`-bound shape (the review's find) and the WHERE-bound sibling that had diverged since before the campaign.

28. **Expression results resolve at one admission boundary** — RULED AND EXECUTED (2026-07-22).

    - `admitExpressionResult` — already called at every evaluation site — consumes the get-some absence sentinel and validates the resolved value, returning `(value, found, error)`. The signature change makes site-completeness compiler-enforced.
    - It replaced four hand-rolled unwraps and closed two sites that leaked the raw `*query.GetSomeResult` into tuples, panicking at tuple-key hashing: the all-constants environment arm (both `Found` states) and the zero-required-symbols arm (get-some over a tagged-literal entity — a sixth site no report had caught, probe-confirmed).
    - Absence semantics per site: per-tuple operators skip the tuple; no-groups and literal-entity arms return empty; the with-groups arm empties every group schema-preservingly, so the phase contract's Provides survives.
    - The interface-change alternative (Eval returning `(interface{}, bool, error)`) was considered and declined: it rebuilds every Function implementation for one absence-capable function, and the boundary achieves the same single-homed invariant.
    - Fix doc: `BUG_GETSOME_ALL_ATTRS_MISSING_PANICS_TUPLE_KEY_HASH` (resolved).

29. **Typed memory datom indexes** — THREE RULINGS RATIFIED (2026-07-25); PR 0 satisfied, PR A executed, **PR B queued**. `docs/proposals/MEMORY_DATOM_INDEXES.md`.

    - Motivating context: `MemoryStore` is the wasm build's only backend, and a wasm32 linear memory cannot exceed 4 GiB. `sysFreeOS` cannot shrink it, so *peak* rather than retained is the binding measure — and today every assert and scan pays `BinaryKeyEncoder`/`DatomFromKey` in a process with no disk and no Badger.
    - **Ruling 1**: the store-agnostic read seam speaks typed datom bounds, and `Encoder()` leaves it; the Badger adapter projects to bytes at its own boundary.
    - **Ruling 2**: trees are **persistent** — a database version *is* a root pointer, a commit a root swap, a read session a retained root. Forced rather than preferred: the documented `NewReadSession` snapshot contract admits only three implementations under mutable-plus-locks, and all three are unacceptable (a session-lifetime read lock blocking writers for arbitrary query durations, an O(N) copy at every session open, or breaking the guarantee).
    - **Ruling 3**: all eight index orders become trees, with encoder and comparator as derived projections of one declarative component-order specification, and differential ordering tests as the proof.
    - PR A executed on branch `feat/typed-bound-scan-seam`: `ScanBound` — index plus the leading components of its order bound to datalog values — across every `StoreReader` scan and `Iterator.Seek`. `BenchmarkComplexQueryCheckpoint` unchanged: interleaved n=20 per side across fork point, arc-without-conversion and arc-with; wall, bytes and allocations all wash.
    - Three departures from Ruling 1's scope list, recorded in the proposal under *What landed (PR A)*: `Get` removed rather than converted (Tx is what resolution determines, so a reader holding a complete key has nothing left to ask); `maxElementIDForAttributeByScan` deleted with the unwired cache gate above it — an implementation, not the capability, since ATEV's layout still makes the first key under `[A]` the attribute's max-Tx datom; and a bound is not always a pure prefix. Declined and recorded: the set-of-prefixes bound shape.

    **Amended after review (2026-07-26)**, `docs/reviews/PR114_TYPED_SCAN_BOUND_2026_07.md`. Nineteen findings, every one confirmed against this tree by reproducer or reference analysis, with three of the received review's claims corrected in place. A1, A2, A5, C2, C3, D1 and D2 fixed; A3, A4, B1, C1 and D3 resolved by removing their subject. CI green on all three jobs including `race`, which `make test` does not run. The earlier SHA citation in this entry is withdrawn — no commit on the branch carried it, and by construction none written inside a branch can be its delivered head; cite the PR.

    - **A2's resolution corrects this entry's own framing.** The review offered a V-payload delimiter or a documented limit, and both assumed the byte range is the only thing that can be exact. A delimiter was implemented and reverted: the format was never ambiguous, because every component behind V is fixed width and Op announces AfterRef, so `DecodeKey` already recovers V exactly.
    - What was inexact was the *range*. `EncodeScanBound` returns an `EncodedRun` carrying the range plus a membership test on key length, and **the on-disk format is unchanged**. The lesson for PR B: the seam's contract is logical — a scan yields the datoms whose bound components equal the bound's values — and how a backend achieves that is not seam vocabulary.
    - **`Through` is removed**, and with it the iterator-reuse join strategy that was its only live producer. A `ScanBound` now names an equality constraint on the leading components of an index's order and nothing else.
    - That strategy had been default-off since 2025-10-26 on a benchmark that does not survive inspection. Its binding tuples hold `**datalog.identity` in the entity position, outside the closed value domain, so it panics at relation construction under the enforcement added since. Its stated cause, the `Sorted()` call, runs once per `Match` and cannot produce a curve linear in binding count at ~760µs per binding. And it was independently incorrect: its stated precondition of bindings sorted in index order is unmet for the Tx and V positions, where `CompareValues` order deliberately differs from key order.
    - **RESOLVED (2026-07-27), ruled and executed: no ordered-range predicate; `Seek` honours the whole run instead.** A value-ordered range cannot be seam vocabulary, because the backends do not agree on value order: Tx is encoded bitwise-NOT in the Tx↓ indices, so key order there is the reverse of value order and forward elsewhere, and V orders by type tag in the key but by `typeRank` in `CompareValues`. "Between" would mean one thing to a typed memory backend and another to Badger — the same disagreement that made the iterator-reuse strategy's sortedness precondition false.
    - What the removal actually cost was not ranges. `EncodeScanBound` returns `EncodedRun{Start, End, Membership}` and `Seek` took two of the three, discarding `End` under a documented "a seek moves within a scan, it does not open one". A caller seeking a narrower run inside an open scan therefore could not say where it stopped, and `pull_batch.go` re-derived the end as `key[1:21]` against the entity hash — the index prefix byte and the 20-byte E, sliced out of an encoded key above the seam whose whole purpose is that no caller holds a key layout, and correct only while the prefix is one byte and E is 20.
    - `Seek` now adopts all three parts on both backends; `memoryIterator` gained the `end` its `keys`-subset construction never needed, and its `Next` stops there rather than treating past-the-end as a key to step over. `pull_batch.go`'s key arithmetic and its second decoded entity check are deleted. `TestSeekHonoursTheRunItNames` pins the shared-scan shape — seek, exhaust the run, seek again — across both backends. `TestTimeBasedQueries`' Tx range on TAEV, `Through`'s one correct use, is that shape.
    - The one form still unavailable is the open interval, "every Tx from T onward". No caller in the tree wants it — `chooseIndex` reaches ATEV with an exact `[aKw, eid]` prefix, and as-of elsewhere is the per-datom `shouldFilterTx` — and building it against zero callers is how `Through` arrived. The shape that would justify rebuilding index-nested-loop is small *k* against a large attribute, since `matchWithHashJoin` narrows to the bound value only at `keyCount == 1` and so reads the attribute's whole range at two or more bindings. The measurement is hash-join cost as a function of attribute size at small fixed *k*.

30. **Transaction envelopes** — PROPOSED AND REVIEWED AGAINST THE CODEBASE (implementation plan appended 2026-07-18), execution blocked on item 29. `docs/proposals/TRANSACTION_ENVELOPES.md`.

    - The dependency is structural: the proposal stores typed `TransactionRecord` trees, and landing it against today's byte-mirroring `MemoryStore` would build the transaction log twice.
    - **(a) PR 4's mechanism under Ruling 2** — pending. Persistent roots mean a fixed-basis temporal read can be served by *retaining the basis root* rather than evaluating `Visibility.Includes` per datom, which changes PR 4's cost profile and turns basis-not-retained into a reachability fact rather than truncation bookkeeping. The change is not uniform: Badger has no roots, and a frontier basis names a per-replica high-water set that need not correspond to any root this replica held. Open: whether `Visibility` stays the single mechanism with root retention beneath it, or fixed-basis reads select between the two at the seam — and whether either answer moves work out of PR 4.
    - **(b) PR 5's query-shape gap** — pending, open in the proposal since it was written. Between PR 3 and PR 5, `[?tx :db/txInstant ?instant]` returns nothing, because instants are reachable only through the typed Go API. Acceptable, or PR 5 folds into PR 3 and the flag day grows.
    - `BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES` (open, ruled out of scope 2026-07-25) resolves here as a class: the envelope write boundary makes notification-miss cache staleness unrepresentable.

32. **Scan reporting binds to acquisition; Family 2 is closed** — RULED AND EXECUTED (2026-07-29), PR #114. `docs/reviews/PR114_TYPED_SCAN_BOUND_ROUND4_2026_07.md`, Family 2 and the mechanism amendment.

    - **The plumbing half**: one inward `*scanReport` replaces the four outward intake channels — the `Scanned` fields on both resolution results, the `scanned` return on all three `CacheResolver` methods, and the int from `GetOrResolve` — and since then the `scanned`/`scansOpened` returns on `walkUniqueEntityValue`, `resolveMaxOtherTxForValue`, `resolveAVLWW`, `checkSetMembership` and `lookupAllAttributesFallback`. An error path keeps what it read, and no caller adds counts up.
    - **The forgetting half**: part 2 as ruled was **falsified** by deriving it against the six arms that actually forgot. "Emission rides on the iterator" reaches an arm holding one, and exactly one of the six does — having no natural place to hang an emit is *why* they forgot. Amended and ruled: bind the obligation to **acquisition**, which all six share.
    - `openScan`/`openKeyScan` are free functions over `StoreReader`, not matcher methods, because a pull batch, a prefetch and the write path each hold a reader by their own field. Every acquisition in the package goes through them.
    - **The generator is closed by `TestScanAcquisitionGoesThroughAReport`**, which parses the whole module each run, finds every one-argument `Scan`/`ScanKeysOnly`, attributes it to its enclosing function, and reds on anything not exempted — or on a stale exemption. Detection is by call shape rather than receiver name: the first version whitelisted `reader`/`store` and would have missed `db.Store().Scan(bound)`, whose receiver is a call, while arity separates a storage acquisition from `bufio.Scanner.Scan`.
    - `OpenScan`, `OpenKeyScan` and `DiscardIntake` are exported for that reach — ruled 2026-07-29, after the claim that exporting was impossible was withdrawn as a conflation of Go visibility with public API. The report type stays unexported, also ruled, so an outside caller can say "nothing accounts for this" and cannot fabricate an accounting, and the first real out-of-package query path reds the gate rather than that being decided by accident.
    - **The exemption list holds five functions and no read a query causes** — the seam implementing itself (`MemoryStore.MaxElementID`, `MaxTxForEntity`, `DatomsAfter`, and the shared `maxElementIDByScan`/`maxTxForEntityByScan`), which sit below the layer a report lives at. A sixth reds. Widening to the module required moving `moduleRoot`/`resolveTestDataPath` out of the wasm-constrained builder into `testdata_path.go`: they are path arithmetic, the constraint was incidental, and the check runs under both targets.
    - It earned itself three times: the twenty-two hand-enumerated sites were wrong in thirteen ways; the module walk surfaced the five seam-internal reads no receiver-name detector would find; and a wildcard-pull bug had already slipped through, `ResolveAllAttributes` announcing EATV while its history branch walked EAVT.
    - **The check enforces that an event exists, not that its payload is right**, which is how that one survived. Pins cover the dispatch arms; `prefetch`, `pull_batch` and `ResolveAllAttributes` have none.
    - **An arm declares its run rather than the report inferring it**: `run` is set by arms addressing a single bound, `peers` counted by arms without one (per-binding, v-validation, prefetch) and reported in the bound's place. The earlier `opens == 1` inference conflated peer scans with an arm's subordinate reads and dropped the bound exactly where an arm had announced an index it then had to price. Caught by `TestEveryDispatchArmAnnouncesItsRunAndReportsItsFunnel`.
    - **A contract change fell out and is load-bearing**: `CRDTResolvingIterator.Scanned()` no longer folds the unique walk's AVET reads into its total — with the source acquired through a report beneath it, that would lose them — so the walk accrues into the arm's report where it happens.
    - Also ruled here, **D1**: a `*-complete` event fires whether or not the scan finished, and carries `annotations.KeySuccess`, matching the eleven producers in `executor`/`reflect` that already answered it that way. Separately fixed in passing: fifteen funnel counters incremented per tuple outside the handler guard that reads them.

33. **Family 1 is closed; D2 ruled** — RULED AND EXECUTED (2026-07-29), PR #114. `docs/reviews/PR114_TYPED_SCAN_BOUND_ROUND4_2026_07.md`, Family 1 and open decision D2.

    - **1a** was resolved at the admission point (2026-07-27), which **dissolved 1b**: `Add` is the only write into `s.attributes`, so a non-nil `Unique` is necessarily a member and `HasUniqueConstraint`'s presence test is correct rather than paranoid. A membership re-test there would be the caveated-invariant shape.
    - **1c**: `KeyCardinality` is declared to carry a `datalog.Keyword` and four producers write it, but the v-validation arm filed `"one"/"many"/"vector"/"unknown"` from a second lookup that existed only to render one — so under the formatter's `%v` that arm printed `one` where the rest printed `:db.cardinality/one`. The call site now uses `getCardinalityEnum`, and `getCardinality` is deleted, its only caller having been that line.
        - `TestCardinalityAnnotationKeyCarriesOnlyAKeyword` asserts the type on every producer in the trace **and**, as a conjunction, that the v-validation event is among them carrying one. Two independent flags pass with a different event satisfying the type check while the arm that was wrong satisfies only presence.
    - **1e**: `cardinality_inference.go` rendered both keywords to detect the ATEV attribute boundary, under a comment declining to rely on Keyword equality without giving a reason. Keywords are interned, so the boundary is `d.A != curA`; `datom_decoder.go` interns every decoded `A`, and `CRDTResolvingIterator` already detects the same boundary the same way. `==` rather than `Equal`: interning makes pointer equality exact, and `Equal`'s panic branch requires the compared pair to *be* an orphan and its replacement, which two attributes from one scan through one decoder cannot be. Removes a string allocation per datom across a full ATEV scan at open on any schemaless database.
    - **1d**: the three closed sets are keyed by the pointers the `define*` functions captured at init, and `TestClearInternsPreservesWellKnownIdentity` pinned only the `datalog` side. `TestVocabularyMembershipSurvivesClearInterns` closes the consumer half: after a clear, a definition whose vocabulary keywords are interned *fresh from text* — the way a parsed schema's arrive — must still be admitted, and each package variable must still be the pointer a fresh intern returns.
        - `require.Same`, not `require.Equal`, which reflect-compares the pointed-to structs and so passes for two distinct pointers carrying one string — precisely the orphan case.
        - 1d's other reported mutation, `Schema.IsMany`'s `==` swapped for a rendered comparison, returns identical answers for every input. Recorded in the review as **not test-detectable**, rather than left looking like unfinished work.
    - **1f and 5d are struck, and both upgrade guides deleted.** Their subject was `datalog/storage`'s and `datalog/schema`'s cross-package plumbing, which Go's export marker makes reachable and which no consumer imports. Recording an internal signature in a consumer-facing document creates the compatibility obligation rather than reporting it, and binds every later refactor to a migration note.
    - **D2 ruled: pin the set, not the switches.** `TestCardinalitySetIsClosedAtThree` asserts `cardinalities` holds exactly `CardinalityOne`, `CardinalityMany` and `CardinalityVector`, with `CardinalityUnknown` outside it.
        - R1a had already ruled out the runtime alternative: twenty-two unreachable `default` arms, which on the read paths would mean threading an error return through signatures like `valueCount() int` for a value admission makes unreachable. What remained was the exposure R1a does not cover — the set growing while the switches do not.
        - The set rather than the switches, because `defineCardinality` is the single registration point, so the pin reds when the set widens. A test enumerating the twenty-two dispatch sites is a list that rots.
    - With D2 ruled the round-4 review has no decision pending.

31. **The value domain is enforced at all three of its doors** — RULED AND EXECUTED (2026-07-29). Derivation and reproducers in `BUG_VALUE_DOMAIN_UNENFORCED_IN_COMPARISON` and `BUG_VECTOR_VALUES_DEGENERATE_ORDERING`. The class: the domain is *defined* by `hashValue`, `ValuesEqual` and `CompareValues`, and was enforced by one.

    Rulings:

    - `typeRank` enumerates the domain; its default **panics**. The silent catch-all was the defect, not a slot to assign — it is what absorbed vectors into a comparison by rendered form, making ascending `:order-by` and `(min ?v)` over a cardinality-vector attribute return the vector holding 10 before the one holding 2.
    - Vectors rank **last**, after `ElementID`, on the composite-versus-scalar asymmetry: a vector is the domain's only member that contains domain values. Chosen rather than inherited — the prior order also put vectors last, but only because the silent default did.
    - Vectors compare **element-wise** through `CompareValues`, lexicographic, shorter prefix first, so `ValuesEqual(a,b) ⇒ cmp == 0` holds by induction on the scalar case. The converse still fails across the numeric split, deliberately.
    - `ValuesEqual` enforces the **domain** rather than comparability, on **both** operands. Every arm dispatches on the left and only assertion-tests the right, and a failed assertion is indistinguishable from a legitimate type mismatch — so an out-of-domain right operand was reported unequal. Ordering already had the symmetry, because `compareByRank` ranks both sides.
    - **`nil` is not a value anywhere**, `hashValue` included. Hashing had become the *permissive* door once the other two rejected, which is worse than either alone: nil-bearing tuples share bucket 0, and equality is then asked, inside the lookup resolving that collision, whether one absence equals another.
    - `*uint64`, `*Identity`, `*Keyword` and `*Symbol` **leave the domain**. `*uint64` is a Tx representation predating the Lamport `ElementID`; the other three are double pointers under interning, with no constructor. `uint64` stays.

    Consequences and residue:

    - User-visible: element-wise recursion means a heterogeneous vector's position follows its first element's rank, so mixed-element vectors interleave rather than cluster.
    - Two defects surfaced and are fixed — the V-validation arm in `matcher_relations.go` fed a tombstoned `OneValue()` to `ValuesEqual` where five sibling readers guard it, and `deduplicateTuples` hashed the unfilled placeholder slot `perTupleInputBuilder.Session` pre-wires.
    - A `TypeVector` tag and encoder were built on a false premise and reverted in full: a vector literal in a pattern is a `query.VectorConstant`, not a `Constant`, so it never reaches `Type`/`ValueBytes` — whose `default: panic` is what proves a whole vector never reaches storage.
    - Open: `BUG_SCAN_FINGERPRINT_RENDERS_TO_KEY`.

34. **Families 4 and 5: one real defect of four** — EXECUTED (2026-07-29), PR #114. `docs/reviews/PR114_TYPED_SCAN_BOUND_ROUND4_2026_07.md`.

    - **5b, real, fixed.** `memoryIterator.ElementID` tested `closed` and the position bounds but neither `end` nor the membership rule, both of which `Seek` sets — so an exhausted run still yielded an ElementID read from the next entity's first key while `Key()` and `Datom()` correctly reported nothing. It routes through `positioned()` now, the predicate those two already use. `TestSeekHonoursTheRunItNames` gained the assertion on both backends.
    - **4a, already fixed** before the instance was read: `WarmCache` returns nil when `d.cache == nil`.
    - **4b, false, withdrawn.** Its premise — that the rebuilds and `ResolveEntry` return either `(nil, err)` or a non-nil entry — is contradicted by three `return nil, nil` arms and by `GetOrResolve`'s own `(nil, nil)` for an absent (E, A), whose comment states that entry existence is what every reader downstream reads as the attribute's existence. Those readers are the four guards it called dead; deleting them turns a missing attribute into a nil dereference.
    - **5c's race premise, stale.** It rests on the verbose installers closing over a formatter stateful in `lastIndex`/`lastBound`; those fields are gone, and `OutputFormatter`/`RelationRenderer` hold only construction-time values. One clause was live: `executor/context.go` called the shared collector "internally synchronized" while `Collector.Add` calls the handler after unlocking — corrected, since that sentence read as license to keep state in a handler.

    The generator, and the reason this entry exists: the work list was drawn from the instances carrying *Verified* markers without re-deriving them against head. Three of four were stale and one was false — Family 7 acting on the person reading the document to decide what to do. The line citations were each ~10 lines off, which is N12. **Every remaining verify-then-act instance is re-derive-first, not actionable as written.**

35. **The unverified instances, derived** — VERIFICATION PASS (2026-07-29), no code changed. Ten instances of Families 3, 4, 5 and 6 carrying *Unverified* or *Partly verified*. Eight confirmed, one struck, one confirmed with wrong particulars.

    Confirmed as written:

    - **3e** — eleven event names as literals, exactly: seven positional to `RewriteSink.Record`, four assigned to a local. None declared in `annotations/types.go`.
    - **4c** — `Collector.enabled` is always true. All four production construction sites guarantee non-nil, and it is read only by `Add`/`AddTiming`.
    - **4e** — `Record`'s nil-receiver test is unreachable, with the count corrected from eight to **five**: every production site sits behind `observing`, which requires `sink != nil`. Its doc comment claims callers invoke it unconditionally; they compute `observing` first, so as not to prepare payload arguments.
    - **5a** — the `Seek` doc comment states repositioning and the sticky-error rule only. `end` and the membership rule, which `Seek` itself sets, are unstated on the interface.
    - **6a** — `resolved` appears only in the `scanned >= resolved` inequality, which `0` satisfies; `matched` is never value-asserted. Hardcoding either to `0` leaves every arm green.
    - **6b** — the cache path asserts `Positive` cold and `Equal(0)` warm, both satisfied by a constant. The two exact intake assertions are on `ScanDirect`, not `resolve-complete`.
    - **6c** — `require.Less(silent, observing)` is relative, so it holds for any amount of unconditional silent work.
    - **6d** — the arm table sets `DisableCache: true` for every case, so the constant-E arms are unreachable with the cache on.

    Corrected:

    - **4d struck**: `Scanned` no longer appears in `hash_join_matcher.go` at all. Item 32's inward `*scanReport` removed the read-then-nil-check pattern the instance cited.
    - **3d's class holds, its particulars do not.** `describeRun` renders every bound value with `%v` under a comment justifying it as house style, and being the shared seam helper it is inherited by every arm reporting a bound — that is the durable half, and `"bound.values"` beside it is a bare literal key. But the counts are wrong (fifteen flattened fields across five emit sites in the v-validation region, nine in `executor`), and the `cache.go` citation is wrong: `annotateRebuild` already carries the attribute typed, under a comment explaining why.

    The pass produced no code changes. Its output is that eight instances are now actionable and two are not what they said.

36. **A handler is registered at construction, never attached afterwards** — RULED AND EXECUTED (2026-07-29), PR #114. Two instances of Families 4 and 5 named `annotations.Collector`, and the owner-surfaced clock sweep turned on which of two collectors a producer read. The ruling removes the type, and with it the choice.

    - **The handler rides in on the options the caller already builds.** `planner.PlannerOptions.Handler`, copied into `ExecutorOptions` by `ExecutorOptionsFromPlanner`, so it reaches the executor and every matcher and relation constructed under those options. No construction call site changes.
    - **Nil is annotations-off, and is the default.** Absence is a complete state, not a hole awaiting plumbing.
    - **Nothing attaches an observer to a running component.** Deleted: `PatternMatcher.SetHandler`, `SourceRouter.SetHandler`, `WrapMatcher`'s duck-typed push into the matcher it wraps, `HandlerProvider`/`GetHandler`, `CollectorAware`, `Context`'s handler and collector, and `Database.AnnotationHandler` as a field. `DatabaseOptions.AnnotationHandler` remains the documented input, materialized onto the database's planner options at open, which `effectivePlannerOptions` then reads rather than projects.
    - The consequence, and it is a capability removed: **a handler is per-executor, not per-query.** `ExecuteWithContext(NewContext(h), q)` gave one shared executor a different observer per call. A caller wanting its own observer now builds its own executor from its own options — `Database.Analyze` does, registering the closure whose events it returns, accumulating under its own lock because a handler that remembers owns its synchronization. Runtime toggling belongs inside one registered handler reading a flag, not in swapping handlers.
    - **`annotations.Collector` is deleted**, which closes **4c** by removing its subject. It accumulated events nothing read, pooled data maps behind a mutex, and carried the `enabled` flag 4c named. What producers used it for was one clock read and one emit: `annotations.TimedEvent` returns the event, so the guard on whether to emit stays at the call site where the payload is prepared — the call-site guard rule, which a guard inside the emitter cannot keep.
    - **5c's one live clause closes the same way.** The sentence it corrected was in `executor/context.go`, calling a shared collector internally synchronized. `Context` now carries `scanRegistry` and `env` and nothing else.
    - **`ExecutorOptions` can no longer be compared to its zero value**, holding a func. The seven `== (ExecutorOptions{})` sentinels read `populated()`, which `TestExecutorOptionsPopulated` walks reflectively and reds on an unhandled field kind — so a new option cannot be added without a decision about whether it counts as populated.
    - `Query`'s body becomes `queryUnderPlannerOptions`, taking the planner options rather than reading them from the database. It replaced a test-side reimplementation of the query path that skipped the read session, the source router and input conversion. `WithPlannerOptions`, added to the public API for the same purpose, was reverted: the tests vary an internal profile, which is not a consumer capability.
    - Untouched: **3e** and **4e**, whose subject is `RewriteSink` in `algebra`, not the annotations handler.
