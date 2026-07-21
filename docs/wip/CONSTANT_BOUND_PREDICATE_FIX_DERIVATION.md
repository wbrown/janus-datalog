# Derivation: Constant-Bound Predicate Evaluation and Environment→Data Rendering

**Date**: 2026-07-20. **Status**: IMPLEMENTED 2026-07-21, full gate green (native + wasm). Two pieces of this derivation were falsified by the red baseline run and amended during implementation — see "Amendments from red evidence" below before trusting Piece 2's arm table. The rest of the document is the derivation as ratified.

## Amendments from red evidence (2026-07-21)

The red baseline run falsified the containment argument: the generator + constant-predicate pass case returned 0 rows (not the predicted accidentally-correct rows), proving the constant predicate executes *before* the generator — so the zero-symbol verdict relations DO coexist with data groups, within a phase and across phases. Two consequences, both implemented:

1. **Fail-with-no-groups returns `[]Relation{}`** (the established empty-groups early-termination convention — the join zero every annihilating clause already uses), NOT the zero-symbol empty relation this doc specified. The zero-symbol empty would be silently dropped by the find projection's single-group-with-all-symbols path, losing the fail verdict — the same class of wrong answer the unit-row trap was.
2. **The find boundary absorbs verdict relations** (`DefaultQueryExecutor.Execute`, before the aggregate/projection fork): a zero-symbol group with a tuple is the join identity — absorbed once data groups exist (unit × R = R); a zero-symbol group with no tuples is the join zero — the whole result empties (∅ × R = ∅). Without absorption, a lingering unit errors aggregation ("disjoint relations") and pollutes projection.

The environment→data rendering (Piece 3) landed at the same find boundary — `renderConstantFindSymbols`, covering find variables, pull entity variables, and aggregate arguments — since the final projection lives in `Execute`'s find application, not `ExecuteRealized`'s tail (the read-before-build flag in Piece 3 resolved to the former).

## The bug this fixes

`docs/bugs/` reproducer (in tree, RED on both modes, instrumented with event logging): `datalog/storage/database_function_integration_test.go` / `TestLeadingMissingWithInBoundEntity`, the consumer-only portion:

```clojure
[:find ?e :in $ ?e :where [(missing? $ ?e :user/email)]]   ;; bob → expect [bob], get 0 rows, err=nil
```

Fails identically on BOTH optimizer modes on a fully capable matcher (BadgerMatcher). This is a **baseline** silent-wrong-answer, distinct from (and discovered while pinning) the third re-review's bridge-only divergence, which is already FIXED — the environment-placeholder seed in `compileClausesFrom` (`datalog/algebra/compile.go`), green in `TestConsumerOnlyWhereWithInBoundCorrelates` (executor, NOT/not-join legs, both modes).

## Mechanism (established by annotation event stream, not theory)

Event signature: `realized/phase-begin input_groups:0` on both modes; identical correct plans; `query/completed tuples.count:0 success:true`.

The chain:
1. `?e` appears in **no data pattern** (only the predicate consumes it). The planner's `findConstantBindableScalars` (`datalog/planner/clause_utils.go`) classifies scalar `:in` symbols as constant-bindable when they appear in no data pattern and no subquery input — `collectDataPatternSymbols` recurses into NOT bodies, which is why `(not [?e ...])`-only queries work (symbol stays a relation) while predicate-only queries hit this.
2. `ExecuteRealized` (`datalog/executor/executor.go` ~339–403) extracts constant-bindable scalars from the bound input relation into `queryExecutor.constantBindings`, projects them out, and when `len(keepSyms) == 0` sets **`currentGroups = nil`** ("all symbols were constants — no input relation needed").
3. `executePredicate` (`datalog/executor/query_executor.go` ~950–1005): `unresolvedSyms` = requiredSyms minus constantBindings = **empty** → no group is "relevant" → **`len(relevantRels) == 0 → return groups, nil`** — the silent skip. The predicate is never evaluated despite being fully evaluable from constants. (Expressions have constant-handling paths at ~742/770/850; predicates have no constant arm — an asymmetry, not a design.)
4. Even if the predicate were evaluated and passed: `len(groups)==0` → `emptyRelationForQuery` (executor.go:488/501) → the constant-bound find symbol `?e` is never rendered into the result. **Nothing renders environment→data at the result boundary.**

Two distinct defects compose: (D1) the silent predicate skip; (D2) missing environment→data rendering for constant-bound find symbols.

**Blast radius today**: any scalar `:in` symbol consumed ONLY by predicates/expressions AND needed in `:find`. The mixed shape is also broken today (silent empty via a different exit): `[:find ?e ?name :in $ ?e :where [?p :person/name ?name] [(missing? $ ?e :attr)]]` — predicate silently skipped (D1: constants-only requiredSyms means no group is "relevant" even when groups exist), then the find projection hits `projectToSymbols` (`relation_ops.go` ~322) whose missing-symbol arm returns an **empty relation** — predicted mechanism, needs verification by pin (see Tests).

## Rejected alternative (B)

Make `:find` membership exclude constant-bindability at the planner. Rejected: resurrects the disjoint-group hazard the extraction exists to prevent — a 1-tuple `{?e}` group shares no symbols with `{?p ?name}` groups → collapse errors. Converts today's silent-wrong into loud-disjoint-errors for mixed queries instead of into correct results.

## The fix — (A) completed, with the full case analysis

Owner probe history matters here: (A) as first proposed ("evaluate once + render at boundary") contained a NEW wrong answer at the zero-groups arm and left a silent taxonomy arm. The completed derivation below traced every arm. Do not implement the un-completed version.

### Piece 1 — `executePredicate` completes its dispatch taxonomy (NO silent arm)

Current dispatch has three cases; only one is handled:

| Case | Today | Fixed |
|---|---|---|
| Some `unresolvedSyms`, some group carries them | filter (`filterWithPredicateAndLookup` / theta-join) — correct | unchanged |
| `unresolvedSyms` empty (all required symbols in `constantBindings`) | **silent skip** | **evaluate ONCE** against `constantBindings` (with `lookup` for `DatabaseFunctionPredicate` via `EvalWithLookup`; else `Eval`). Error → return error. Pass/fail → see arm table below. |
| `unresolvedSyms` non-empty, NO group carries any | **silent skip** | **loud error** — a planner-contract violation (predicate assignment places predicates where symbols are Available). Caveat flagged to owner: the gate may surface flows that hit this arm today; whatever it surfaces is evidence to examine, not to suppress by keeping the skip. If legitimate hitters exist, report to owner — do not silently re-add the skip. |

Note: `filterWithPredicateAndLookup` already merges `constantBindings` into per-tuple bindings — the evaluate-once arm is the degenerate zero-relation-symbols case of existing machinery, not new machinery.

### Piece 2 — the pass/fail arms and the zero-symbol unit/empty relation

The evaluate-once result must be materialized structurally, because ONLY `executePredicate` knows it, and the result boundary cannot reconstruct it:

| Arm | groups non-empty | groups EMPTY (`len==0`) |
|---|---|---|
| Predicate **passes** | return groups unchanged | return the **zero-symbol UNIT relation** — one empty tuple, zero symbols (`NewMaterializedRelation(nil, []Tuple{{}})` shape — verify constructor admits it; if the boundary constructor rejects an empty-symbol tuple, use the interior constructor with a stated warrant) |
| Predicate **fails** | return each group **schema-preserving emptied** (same symbols, zero tuples — keeps downstream joins/phases coherent; empty joins to empty) | return the **zero-symbol EMPTY relation** — zero tuples |

Why this shape and not flags/markers: the unit relation is the join identity — the exact concept the owner has now ruled TWICE (`c6a5c40`: or-default-first-clause compiles against a childless empty Project; 2026-07-20: `compileClausesFrom`'s environment placeholder). This is its executor-side form.

**Containment argument (why no Collapse/join changes are needed)**: the zero-symbol relation is created ONLY when groups was empty, which happens ONLY in the pure consumer-only/all-constants shape — mixed queries have generators, so extraction leaves a real group or patterns create one, and the pass/fail arms operate on real relations. The zero-symbol relation never coexists with real relations in a group list. (If a future flow violates this, collapse's disjoint detection will error loudly — acceptable.)

**The trap this table exists to prevent**: pass-with-no-groups and fail-with-no-groups both leave `len(groups)==0` if you "return groups unchanged" — the boundary then can't distinguish "row survived" from "row filtered", and rendering constants onto ambiguous emptiness emits the unit row even when the predicate failed (Bob appears in a query that excluded him). This was the wrong answer inside un-completed (A).

**Early-termination interaction**: `executor.go:487` `len(groups)==0 → emptyRelationForQuery` mid-phases. With the arms above, the consumer-only phase returns len==1 (unit or empty zero-symbol relation), so the early return doesn't fire; the fail arm on real groups returns schema-preserving empties (len>0), so it doesn't fire there either. Multi-phase edge (an intermediate phase legitimately zero-group with all-constant keeps) noted as out of scope — single-phase in practice; if encountered, it surfaces as the early return and is a follow-up, not silent.

### Piece 3 — environment→data rendering at the result boundary

Core-model derivation: ":in parameters are environment, not data" — EXCEPT `:find` membership makes one **data**, and "result presentation is rendered at the result boundary in `ExecuteRealized`" (pull output is the precedent; order-by already treats scalar `:in` sort keys as every-row-same-value no-ops — constants-as-virtual-columns has in-tree precedent on the sort side).

At `ExecuteRealized`'s result assembly: for each find symbol present in `constantBindings` (NOTE: constantBindings lives on the `queryExecutor`, constructed per-execution at executor.go:326 — the rendering site has access to `constValues` computed at ~357; thread it to the tail, do NOT re-derive it) and absent from the final relation's symbols, **extend every tuple with the constant value** (zero-symbol unit → one row of pure constants; zero-symbol empty → zero rows; N-row relation → N rows each extended).

**Placement constraint**: rendering must occur BEFORE the final find projection. `projectToSymbols`'s missing-symbol arm returns an empty relation — if projection runs first, the extension never sees the rows. **Read-before-build**: locate the exact final-projection site (candidates: the last phase's Query find handling inside `DefaultQueryExecutor.Execute`, or `ExecuteRealized`'s tail at executor.go ~500-506 before order-by/limit finalization). This was NOT read before context ran out — do not assume; read `ExecuteRealized`'s full tail and the last-phase projection path first. Also check `emptyRelationForQuery` (what symbols does it emit? all find symbols?) — the fail-arm zero rows flowing through it must still produce a relation whose schema includes the constant find symbols, or the tail must handle that.

Also verify: the two OTHER `emptyRelationForQuery` early returns reachable in this flow (executor.go:488, 501) and the relation-input iteration path (executor.go:539 area — already probes `EmptyRelationError` after the laundering fix) don't bypass the rendering for shapes that should carry constants.

### Expressions: check the same holes

`executeExpression` has constant paths (~742/770/850) — believed to handle constants-only inputs, which is WHY the asymmetry framing. During implementation, verify with a pin: `[:find ?y :in $ ?x :where [(+ ?x 1) ?y]]` (expression-only, constant input, ?y in find). If it fails too, the expression path gets the same arm treatment. Also `[:find ?x ?y :in $ ?x :where [(+ ?x 1) ?y]]` — constant in find alongside expression output — exercises Piece 3 independent of predicates.

## Tests (red-first; the first two are already in tree and red)

1. **IN TREE, RED**: storage `TestLeadingMissingWithInBoundEntity` consumer-only portion (both modes, event-logged on failure) — bob → 1 row; alice → 0 rows (the fail arm!). The alice case pins fail-with-no-groups → empty (the trap case).
2. **IN TREE, GREEN** (do not regress): executor `TestConsumerOnlyWhereWithInBoundCorrelates` (NOT/not-join legs — these bypass constant extraction because NOT bodies count as data patterns).
3. NEW: mixed-shape pin — `[:find ?e ?name :in $ ?e :where [?p :person/name ?name] [(missing? $ ?e :attr)]]` both polarities (predicate passes → rows carry constant ?e + pattern ?name; fails → 0 rows), both modes. Predicted broken today via projectToSymbols-missing-symbol — verify the mechanism when it goes red.
4. NEW: constants-only predicate over NON-empty groups where the predicate FAILS → all rows filtered (schema-preserving-empty arm), and where it PASSES → rows unchanged.
5. NEW: expression pins per the section above.
6. The loud unprovided-symbols arm: full gate run reveals hitters; if green, add a direct unit pin constructing the contract violation.

## State at time of writing (what is uncommitted)

- Bridge fix for the reviewer's finding: `compileClausesFrom` environment placeholder + `environmentSymbols` (compile.go, sorted deterministic), `sort` import — DONE, pins green.
- Executor pin `TestConsumerOnlyWhereWithInBoundCorrelates` — DONE, green all four legs.
- Storage consumer-only case in `TestLeadingMissingWithInBoundEntity` + annotations import + `SetAnnotationHandler` instrumentation — DONE, **RED both modes** (this is the committed-red-candidate for THIS bug).
- No bug doc written yet for this baseline defect — write one (mechanism above) when committing, or fix in the same arc.
- Everything above NOT committed. Prior pushed state: `00fc6e4`.
- Also uncommitted by standing rulings: `docs/wip/DECISION_LEDGER.md` (update item 7/21 when this resolves), `RELATION_ALGEBRA_REUNIFICATION.md`.

## Process notes for the implementer

- Owner ruled the completed derivation is the standard: every dispatch arm traced including zero/empty/absent before presenting. See `feedback_most_correct_first` memory (updated 2026-07-20 with the completion-pass discipline) and `feedback_evidence_never_downgraded`.
- The reviewer relationship: this arc is the third re-review finding plus a discovery made while pinning it. The reviewer's finding itself (bridge divergence) is fixed; when reporting, keep the two clearly separated.
- Verification standard: full gate is `make test` (native + wasm). The only tolerated red during the arc is this bug's own reproducer.
