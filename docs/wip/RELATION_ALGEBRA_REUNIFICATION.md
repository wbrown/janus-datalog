# Relation Algebra Reunification — the engine consumes the algebra through Relation

**Status**: Reworked 2026-07-22 against the environment-relation and set-semantics-at-birth arcs; two rulings of 2026-07-22 incorporated — consumption is requested, never queried, and clauses close over the environment relation (ratified, superseding this document's original map-closure sketch). Executes the audit C4 ruling (`docs/reviews/ANTIPATTERN_AUDIT_2026_07.md`) and decision-ledger item 20. Companion context: `resolved/BUG_EXPRESSION_BINDING_OVERWRITES_BOUND_VARIABLE.md` (the drift this design makes structurally impossible), audit E1 (the `Materialize()` convention this design closes), and `docs/TALE_AND_LESSONS_OF_RELATIONS_AS_EXCHANGE.md` (why Relations are the only exchange — the narrative behind every carrier decision below).

## The ruling being executed

`Relation` is the engine's fundamental abstraction, and its six ruled-kept methods — `Select` and `FilterWithPredicate` (σ), `SemiJoin` (⋉), `AntiJoin` (▷), `EvaluateFunction` (extend), `Aggregate` (γ) — together with `Join`/`HashJoin` (⋈), `Project` (π), and `Sort` make it algebraically closed. The mistake was that the *engine* performs these operations through free-standing operators beside the interface (`filterWithPredicateAndLookup`, `evaluateExpressionWithLookup`, `ExecuteAggregationsWithContext`, the `not` executors' hand-rolled anti-membership stage — the last verified still present at `filterWithNotClause`'s tail, 2026-07-22), which is how two implementations of one semantic drift apart — the overwrite-vs-unify divergence was exactly this. The correction: the methods are the canonical surface, the engine consumes the algebra through them, and each operator has exactly one generic body.

The 2026-07 arcs already executed this principle in two places and both serve as precedent: `projectionPreservesSet` is one derivation consumed by all four Project homes (`MaterializedRelation`, `StreamingRelation`, `ProductRelation`, `LazySeqRelation`), and `scanProjectionPreservesSet`/`restoreScanSetSemantics` is one derivation applied at both matcher dispatch exits. One operator adds to the surface: **∪** (`Union`).

## Design principle: property-driven generic operators

Each operator gets one generic body that interrogates both operands' declarations and does exactly the work the warranties fail to cover — never more, never less. The declarations:

| Declaration | Meaning | Operator use |
|---|---|---|
| `Properties().Keys` | candidate keys — proven set-ness on a column set | skip redundant dedup (`materializeFilteredLeft`, `projectionPreservesSet` precedents); unique-build-key dispatch (`buildKeysUnique` precedent) |
| `Properties().Ordering` | sortedness | sort elision, order-aware strategies, TopN |
| `RequiresCopy()` | iterator reuses workspace memory | copy at operator boundaries only when declared (`DedupIterator`'s copy mode is the precedent: seen-keys copy exactly when the source declares reuse) |

Concrete types stop being a dispatch mechanism. `MaterializedRelation` vs `StreamingRelation` vs `LazySeqRelation` differ in what they *declare* and how they iterate; the operator bodies are generic over the interface.

### Consumption is requested, never queried (ruled 2026-07-22)

**Reuse is a static property of the consuming algorithm, not a runtime discovery — there has never been a circumstance in which a consumer does not know it needs to reuse a relation.** A nested-loop join re-scans its inner side because that is what a nested-loop join is; the subquery executor replays its groups because the outer query consumes them after combination extraction; a product rewinds operands 1..n by construction; fan-out orchestrators know their consumer count. A capability predicate serves a consumer that doesn't know whether it will need the capability, and that consumer does not exist here.

The general test this yields: **an interface declaration carries knowledge across the boundary from the side that has it to the side that lacks it.** `RequiresCopy()` passes — workspace reuse is producer-side knowledge the consumer cannot see. Pass count is consumer-side knowledge, so it flows the other way, as a directive: `Materialize()`, the consumer telling the producer "I will read this more than once." A capability predicate here would be producer-to-consumer traffic about a consumer-side fact — backwards, and (like the deleted `IsEmpty()`) a describing bool inviting a branch around a request the consumer can make unconditionally. It would also be mutable state posing as a declaration: a stream's answer flips the moment `Materialize()` marks it caching.

No such predicate belongs on the interface or in `RelationProperties`.

What survives is the E1 resolution as `Materialize()`'s sharpened contract: **`Materialize()` returns a shareable Relation, realizing eagerly only when the receiver does not already replay, returning the receiver itself when it does.** The contract is pinned behaviorally — iterate the result twice and assert identical tuples; assert an already-shareable receiver returns itself — never by a self-reported flag. Consumer sites with a statically-known second pass call `Materialize()` once, unconditionally: `thetaJoinPair`'s inner side (replacing its unconditional `BufferedIterator`), the subquery executor's group lift (already written this way; the contract test pins it). Sizing questions stay with the size machinery: `Size()`'s free-or-decline convention and the `materializedSize` guard already answer them, and they were never replay questions.

## Environment closure: clauses close over the environment relation (ratified 2026-07-22)

The engine's σ and extend operators carry two things the algebra signatures must not: an `EntityLookup` (needed only by the database-function clause family — get-else, missing?, get-some) and the query scope's environment. Both are attributes of the *clause*, not of the relational operator: σ_pred(R) takes a black-box predicate of the tuple.

The environment's semantics were ruled during the 2026-07 environment arc: `:in` scalar and tuple parameters are the scope's joint parameter binding — one single-tuple Relation, ambient in every clause scope, Available and never Provides. At evaluation sites the environment is *consulted*, not joined — the strict phase contracts bar its symbols from operator outputs. "Consulted under an ambient scope" is, formally, evaluation under a lexical environment, and the standard semantics of that is closure: the clause captured over its scope.

- `ClosePredicate(pred query.Predicate, lookup query.EntityLookup, env Relation) query.Predicate` and `CloseExpression(expr *query.Expression, lookup, env)` — the executor closes each clause once at clause receipt (per scope entry; per combination in iterated subqueries). **The combinators and the closed types live in the executor** — the capture is the environment *relation* (jointness preserved; the closed Eval binds `env.Get(0)` by reference — never a map, never unpacked slices), and `Relation` is the executor's type; the closed types implement the `query` interfaces, so the dependency direction holds. This supersedes the original draft's `query`-package placement, which captured a `map[Symbol]interface{}` — the carrier the campaign retired.
- Per-row mechanics are unchanged in substance, relocated into one home: the closed `Eval` writes environment pairs if-absent after the operator binds the tuple, so tuple bindings win exactly as they do today.
- Where environment symbols genuinely enter schemas, joins remain joins: or-branch correlation (`envRel` join-in), `not`-body binding (`notBodyBinding`), find-boundary constant rendering (`group.Join(env.Project(missing))`). Closure applies exactly where the arc classified the environment as consulted.

Why this is the correct home, ranked: (1) the invariant "a clause evaluates under its scope's environment" gets one enforcement point instead of one per operator body — the `:in`-bound `not`-leg failure in the algebra lowering was a forgot-to-thread-the-environment bug, a class closure makes unrepresentable; (2) it is the only shape under which the interface methods become the single surface — `FilterWithPredicate(closed)` absorbs the database-function family, so the free operators can actually be deleted, which the `env`-parameter shape forecloses permanently; (3) the per-row binding mechanics stop existing in five copies across `relation_ops.go`/`theta_join.go` — the exact drift surface this document exists to eliminate.

Cost, stated: one wrapper allocation per clause per scope entry (negligible beside per-combination execution); the operator signatures stop self-documenting the environment's existence (mitigated: the closed types are the one named, grep-able home); and the environment arc's −3.4% carrier-swap result must be re-gated after conversion — identical per-row work predicts a wash, and the benchmark rules.

## The operator surface

### σ — `FilterWithPredicate(pred query.Predicate) Relation`, `Select(pred func(Tuple) bool) Relation`

Signatures unchanged; the predicate arrives closed. One generic body (per-tuple bind → closed Eval → keep/drop, first eval error becomes the result's deferred error); `Select` remains the raw-Go-function σ for programmatic consumers and needs no closure. Streaming receivers compose `PredicateFilterIterator`/`selectionIterator`; materialized receivers walk. The engine's `executePredicate` single-relevant-relation path becomes `rel.FilterWithPredicate(closed)` — including the environment-only verdict arm, which is the closed predicate filtering the unit relation (the join identity). `thetaJoinWithPredicate` remains engine-side composition (a multi-relation θ-join is × followed by σ, not a single-relation operator) but its filter stage consumes the same closed predicate. The free `filterWithPredicateAndLookup` is deleted at stage end.

### extend — `EvaluateExpression(expr *query.Expression) Relation` (supersedes `EvaluateFunction`)

The current `EvaluateFunction(fn, outputSymbol)` expresses only the scalar special case; the language's extension clause has scalar, tuple, and multi-row (enumerate) binding forms. The operator takes the closed clause. Per the supersession discipline, `EvaluateFunction` is deleted in the same change (compatibility note; it is exported API).

One generic body on `bindingAlignment` (`expression_binding.go`) — the single home the overwrite-bug fix established: bound binding positions unify (filter, never write), new symbols extend, malformed tuple results error loudly. The ground-constants path folds into the same body: an expression with zero unresolved required symbols evaluates once (against the empty tuple, environment from the capture) and applies the constant values per tuple through the same alignment. The streaming form generalizes `FunctionEvaluatorIterator` to expressions and converts its private unification logic to consume `bindingAlignment.apply`. The free `evaluateExpressionWithLookup` is deleted at stage end.

### ⋉ / ▷ — `SemiJoin` / `AntiJoin(other Relation, joinSyms []query.Symbol) Relation`

Signatures unchanged. The free `SemiJoin`/`AntiJoin` bodies in `join.go` become the single generic implementations (build the key set from `other` in one pass, filter the receiver in one pass — neither side needs replay); the methods already delegate to them. The `not` executors' final stage — the hand-rolled `matchedKeys` anti-membership loop in `filterWithNotClause`/`filterWithNotJoinClause` (verified live, 2026-07-22) — converts to `input.AntiJoin(matchedRel, actualJoinVars)`, with `matchedRel` constructed over the join variables from the matched combinations (distinct by construction — interior FromSet). The correlated inner-clause evaluation that *produces* the matches stays engine orchestration — including the anti-join bridging and environment rendering the `not`-spanning-groups fix added — only the algebra step becomes the method.

### ∪ — `Union(other Relation) (Relation, error)` (new)

Set-native, per the owner's formulation: both operands are already Relations — sets — so the only possible duplicates are cross-duplicates. The receiver streams through untouched while its membership set builds; `other` is anti-membership-filtered. Relations at every point; no bag intermediate exists; no concatenation primitive is needed or reintroduced. N-ary union (the OR executors) is the associative fold sharing one accumulated membership set — each branch filtered against all prior branches.

Precondition note (2026-07-22): the both-operands-are-sets premise is now enforced rather than assumed — the set-at-birth arc (`BUG_SCAN_RELATIONS_NOT_SETS_UNDER_WILDCARD_PROJECTION`) closed the producer class that violated it. Built earlier, this operator would have been a laundering layer.

Schema: operands must carry the same symbol set; a mismatch returns an error, following `Project`'s established `(Relation, error)` precedent (engine call sites cannot mismatch — branch interfaces guarantee agreement — but the method is public surface). Result properties are derived conservatively: no `Ordering`; `Keys` only where provable (initially none). Consumers: the or-union path converts to the fold — re-scope against the direct-probe-emit-era executors (`projectionPlan`, cached-branch cursors) before conversion; the original draft predates them.

### γ — `Aggregate(findElements []query.FindElement) Relation`

Signature unchanged; `ExecuteAggregations` remains the single body (it already is — only the method entry was dead surface). The `WithContext` variant's annotation needs route through the relation's `Options().Collector` — the channel join annotations already use — so no context parameter enters the algebra.

## What does not change

`Join`/`HashJoin`, `Project`, `Sort`, `Materialize` (semantics sharpened per the consumption ruling, signature unchanged), the phase/planner layer, storage, and the clause taxonomy. The environment arc's scope-boundary construction, jointness carrier, and correlation joins all stand — closure relocates only the eval-site consultation. The boundary/interior constructor split (`NewMaterializedRelationFromSet` / `newMaterializedRelationFromSet`) is prior art this design builds on, not a change.

## Staging

Each stage lands red-first-pinned, both matrix modes, full gate green before the next:

1. **`Materialize()` contract** (closes audit E1): the behavioral contract test across all relation types (double-iteration equality; self-return when already shareable) + the consumer conversions (`thetaJoinPair` inner side; the subquery group lift pinned as-is). No interface change.
2. **Clause closure** (`ClosePredicate`/`CloseExpression` in the executor) + the σ and extend generic bodies on `bindingAlignment` + engine call-site conversion (`executePredicate` including the unit-relation verdict arm, `executeExpression` including the ground path folding in) + `FunctionEvaluatorIterator` consumes `bindingAlignment` + `EvaluateFunction` and the free σ/extend operators deleted. Benchmark re-gate against the environment-arc carrier-swap result (wash predicted; the benchmark rules).
3. **Semi/anti single home**: `not`-executor final stages convert to `AntiJoin`; the free bodies become the only implementation.
4. **`Union`** + or-union executor fold conversion (re-scoped first).
5. **γ collector routing** (`Options().Collector`), retiring the context parameter from the aggregation entry the engine uses.

## Sub-decisions

1. **Open**: `Union` error shape — `(Relation, error)` per the `Project` precedent (proposed), vs panic-on-mismatch as an invariant violation.
2. **Resolved 2026-07-22**: the closure combinators and closed types live in the executor, capturing the environment relation; names `ClosePredicate`/`CloseExpression` (they close a clause over its evaluation environment).
3. **Open**: `EvaluateFunction` removal is a compatibility-notes change (exported API), bundled with stage 2 per supersession-requires-deletion (proposed).

## Verification

Per-operator: differential pins (method result ≡ the former free-operator result on the same inputs) during conversion, deleted once the free operators are gone; property-dispatch pins (keys-proven inputs skip dedup — asserted structurally via the iterator-chain assertions the set-semantics arc established, and via existing annotation events where present); the full optimizer mode matrix rides every stage since the engine call sites are exercised by every query test; the behavioral `Materialize()` contract test across all relation types; the stage-2 benchmark gate. The reference-evaluator tier (`docs/wip/CORRECTNESS_MEASUREMENT.md`, pending ruling) would referee the end state independently if ratified.
