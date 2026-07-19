# Antipattern and Repetition Audit — 2026-07

**Date**: 2026-07-19
**Scope**: `datalog/query`, `datalog/executor`, `datalog/parser`, `datalog/planner`, `datalog/storage`, `datalog/qb`
**Method**: static caller analysis (production vs `_test.go` callers separated for every flagged symbol), switch/default enumeration, delegation-chain termination proofs, and pattern sweeps for the antipattern classes eliminated by the value-semantics consolidation (PR #112).

**Status convention** (same as `docs/bugs/`): every finding below carries a `Status:` line. A finding without `Status: Resolved (date, commit)` is believed open *as of the audit date*. Before treating any entry as live, re-read the cited code; when you fix or refute one, update its `Status:` line in place so the next reader does not re-derive it.

## Context

PR #112 consolidated the engine onto: one total order (`datalog.CompareValues`) and one equality (`datalog.ValuesEqual`); typed pattern positions validated only at user boundaries; NaN excluded at its producers; aggregation and comparison operators dispatched on pre-interned symbols by pointer equality instead of strings; and it deleted two subsystems no production code drove (the executor `Filter` family, the `query.AggregateFunction` family) plus the `EnableIteratorComposition` flag whose false arm recursed unboundedly. This audit looked for remaining instances of those same classes:

- **A** — live defects (swallowed errors, silent defaults over closed taxonomies)
- **B** — string dispatch at evaluation time
- **C** — dead or test-only exported surface (parallel systems)
- **D** — repetition (one operation, many inlined copies)
- **E** — structural landmines (conventions whose violation is an unbounded-recursion bug)

---

## Class A — live defects

### A1. `MaterializedRelation.FilterWithPredicate` swallows predicate evaluation errors

**Status**: Open
**Site**: `datalog/executor/relation.go` (`if passes, err := pred.Eval(bindings); err == nil && passes`)

A predicate `Eval` error silently drops the tuple — a truncated-success bug: wrong answer, no signal. Contrast `PredicateFilterIterator.Next` (defers the error) and `filterWithPredicateAndLookup` in `relation_ops.go` (fails fast). Six relation types delegate their `FilterWithPredicate` to this method (`ProductRelation`, `LimitRelation`, `UnionRelation`, `PrependedRelation`, `OrFallbackRelation`, `StreamingAggregateRelation`), so all inherit the swallow.

Reachability caveat: production predicate filtering flows through `filterWithPredicateAndLookup`/`thetaJoinWithPredicate`, so the swallow is **latent on production paths but live on the public `Relation` API** — the same masked-by-disuse profile as the `EnableIteratorComposition` recursion. A repo-wide sweep confirmed this is the only site of the `err == nil &&` swallow shape.

### A2. `ChainedComparison.Eval` has no default arm for unknown operators

**Status**: Open
**Site**: `datalog/query/predicate.go` (operator switch inside the adjacent-pair loop)

An unknown/unhandled `Op` leaves `ok = false` and returns `(false, nil)` — the predicate silently filters out every tuple with no error. Its sibling `Comparison.Eval` returns `fmt.Errorf("unknown comparison operator: %v", c.Op)` for the same condition. The chained form should fail identically loudly.

### A3. Unguarded deferred-error tails can clobber earlier errors

**Status**: Open
**Sites**: `datalog/executor/or_fallback_relation.go:943, 1034, 1182` (three identical copies of the outer-exhaustion tail, `it.err = it.outerIter.Error()` unconditional); `datalog/executor/relation_ops.go:419, 519`; `datalog/executor/relation.go:324` (`CachingIterator` completion)

The codebase discipline is first-error-wins (`if e := it.Error(); err == nil { err = e }`), applied correctly at many sites (e.g. `relation_ops.go:134, 354`, the storage matcher tails, `union_relation.go:273`). The listed sites assign unconditionally; some are safe today only because no earlier assignment exists on that path, which is exactly the fragile shape. The three `or_fallback` copies can overwrite an already-recorded branch error, possibly with nil.

### A4. `matchesDatom` Tx arm silently returns false where E and A arms panic

**Status**: Open
**Site**: `datalog/storage/matcher.go` (Tx-position `default: return false`, vs the E and A position default arms which panic)

Within the same function, an unexpected binding type in E or A position is a loud invariant panic, but in Tx position it silently drops every datom. The Tx arm should match its siblings.

### A5. `toStorageValue` default coerces unknown types to strings at the write boundary

**Status**: Open
**Site**: `datalog/storage/types.go` (`default: return datalog.String(fmt.Sprintf("%v", v))`)

A value outside the enumerated storable set is silently stringified rather than rejected. This is a value-domain absorber at precisely the boundary where the NaN and position-typing validation was just built; the closed value domain says the default should fail loudly.

### A6. Silent-nil / silent-zero defaults over closed taxonomies

**Status**: Open
**Sites**:
- `datalog/executor/subquery.go:97` — unknown subquery input element appends `nil` as the binding value (wrong results, no error).
- `datalog/executor/query_executor.go` `bindingSymbols` — unknown `BindingForm` returns nil; the binding's variables silently vanish from the schema.
- `datalog/query/function.go` `toNumber`/`toInt64`/`toFloat64` and `datalog/executor/vector_functions.go` `toInt64Coerce` — non-numeric values silently become `0`/`0.0` inside arithmetic.
- `datalog/executor/constraints_impl.go` time-field constraint switch — unknown field silently matches nothing (the query-layer equivalent, `TimeExtractionFunction.Eval`, errors).
- Lower severity, same shape: `pattern_match.go` `matchesElement` default, `indexed_memory_matcher.go:367`, `matcher.go:381`, numeric-position defaults in `constraints_impl.go:82` and `hash_join_matcher.go:506`.

Verified non-hazards (documented, principled): schemaless cardinality-one defaults across cache/pull/database, `cardFromOp`'s explicit non-decisive `false`, display-type and `String()` fallbacks.

---

## Class B — string dispatch at evaluation time

The disease: an operation's identity is a string, and the decision of what to do is made by string switch per tuple/per Eval instead of once at parse. `FindAggregate.Function` and `CompareOp` were cured in PR #112; these remain.

### B1. `query.ArithmeticOp` — CompareOp's twin

**Status**: Open
**Sites**: `datalog/query/function.go` (`type ArithmeticOp string`, constants `OpAdd`/`OpSubtract`/`OpMultiply`/`OpDivide`; `ArithmeticFunction.Eval` switches on `a.Op` twice per call — float path and int path — plus `useFloat := a.Op == OpDivide`)

Per-tuple, production-reachable through the expression evaluation loop. The parser (`parser/function_parser.go`) and qb (`qb/expression.go`) already resolve names once at setup, so symbolization mirrors the CompareOp change exactly: pre-interned `SymAdd`/`SymSub`/`SymMul`/`SymDiv`, pointer switches in Eval. ~3 production files; note `storage/set_entry.go` has an unrelated `OpAdd` namesake (uint8 CRDT op).

### B2. `TimeExtractionFunction.Field` — string kind, switch triplicated

**Status**: Open
**Sites**: `datalog/query/function.go` (`Field string`, per-tuple switch in `Eval`); the same six-case switch duplicated in `datalog/executor/custom_functions.go` `extractTimeComponent` (dead — see C5) and `datalog/executor/constraints_impl.go` `timeExtractionConstraint.Evaluate` (per-datom, but test-only-constructed — see C6)

One interned time-field symbol set would replace all three; two of the three copies are better deleted than symbolized.

### B3. `query.FunctionPredicate.Fn` — per-tuple string switch, mostly unimplemented

**Status**: Open
**Site**: `datalog/query/predicate.go` (`Fn string`; `Eval` switches per tuple; only `"str/starts-with?"` is implemented, everything else errors)

Symbolizing the string is less the point than deciding what this predicate extension mechanism should be (a registry of concrete predicate types, most likely). Constructed only by the parser.

### B4. `query.OrderDirection` — minor

**Status**: Open
**Site**: `datalog/query/types.go` (`type OrderDirection string`; compared per sort-comparison in `executor_utils.go` `compareTuplesByOrder`)

Two-valued flag, typed-constant comparison rather than multi-way dispatch. If touched at all, the fix is a bool/int, not a symbol. Lowest priority in this class.

---

## Class C — no internal consumers: test-only or unwired exported surface

The deleted-`Filter` profile: exported machinery whose only in-repo initiators are tests, while production does the equivalent work elsewhere. Each entry lists what the production path actually is.

**Standard of proof — read before acting on this class.** "No production references" throughout this audit means: *zero callers inside this repository outside `_test.go` files*. That standard proves deadness for **unexported** symbols — nothing outside the repo can reach them. For **exported** symbols of this library it proves only "unused internally": downstream consumers are invisible to in-repo caller analysis, and extension points exist precisely for them. Every exported entry below is therefore a per-symbol **API-surface decision** informed by whether downstream use is plausible — not a mechanical dead-code sweep. (The already-deleted `Filter` system was handled this way: removed as a deliberate API decision, recorded in the PR's compatibility notes.)

### C1. `UnionRelation` — entire subsystem test-only

**Status**: Open
**Site**: `datalog/executor/union_relation.go` (type, `NewUnionRelation`, `UnionIterator`, `relationItem`, `newUnionBuildIterator`)

Every constructor call is in tests. Production OR/union execution goes through `executeOrClauseUnion` and `OrFallbackRelation`.

### C2. `PrependedRelation` — whole file test-only

**Status**: Open
**Site**: `datalog/executor/prepended_relation.go`; `NewPrependedRelation` is called only from `wrapper_relation_copy_test.go`.

### C3. `TransformIterator` and `ConcatIterator` — test-only

**Status**: Open
**Site**: `datalog/executor/iterator_composition.go`; only `iterator_composition_test.go` references them.

### C4. `Relation` interface methods with no production initiator

**Status**: Open
**Sites**: `Select`, `SemiJoin`, `AntiJoin`, `FilterWithPredicate`, `EvaluateFunction`, `Aggregate` on the `Relation` interface (`datalog/executor/relation.go`), plus the free functions `Select`, `SemiJoin`, `AntiJoin`

Every implementation delegates (to a free function or a sibling implementation); only tests initiate the calls. Production equivalents actually used: `filterWithPredicateAndLookup` + `thetaJoinWithPredicate` (filtering), `executeExpression` (function evaluation), `antiJoinOnSymbols` via `executeNotClause` (anti-join), `ExecuteAggregations` (aggregation — its free function is core-live; only the *method* is test-only). This is exactly how the dead `Filter` method hid. These are public API surface, so each removal is an owner decision; A1's swallow lives on one of them.

Production-initiated interface methods (keep): `ProjectFromPattern`, `Join`, `HashJoin`, `Materialize`, `Sort`, `Project`, `Iterator`, `Symbols`, `Options`, and the rest of the core surface.

### C5. `custom_functions.go` — exported extension point with an unwired consumption side

**Status**: Open
**Site**: `datalog/executor/custom_functions.go`

`RegisterCustomFunction` is a deliberate exported extension point (mutex-guarded registry, documented "for use in expressions"). Its consumption side, `CallCustomFunction`, now has zero callers: the only consultation point was `evaluateComparison`, deleted with the `Filter` system — which also severed the one downstream-reachable invocation path (`RegisterCustomFunction` + `rel.Filter(ComparisonFilter{Function: "my-fn"})`, both formerly public). The live query path never consulted the registry: `FunctionPredicate.Eval` errors on unknown names without checking it. As of this branch the registry is **write-only** — a downstream consumer can register functions the engine will never invoke.

This is not dead-code cleanup; it is a product decision about whether user-defined functions are a supported feature:
- **Wire it**: connect the registry to the live evaluation path (`FunctionPredicate` / expression evaluation — the same redesign B3 calls for), making registration functional again and better than the severed `Filter`-based path ever was.
- **Remove it**: deprecate `RegisterCustomFunction`/`CallCustomFunction` deliberately, with a changelog entry, as an API decision.

The unexported value functions in the file (`addValues`, `subtractValues`, `multiplyValues`, `divideValues`, `concatenateStrings`, `extractTimeComponent`) have zero callers and *are* provably dead regardless of the decision above; `extractTimeComponent` is one of the three duplicated time-field switches (B2).

The in-repo registration in `executor_subquery_test.go` registers `same-date?` and then runs a query that never uses it (the test's own comment: "Simplified to test basic functionality first") — the registration is vestigial within its own test, so no current test exercises the registry end to end.

### C6. Test-only constraint implementations

**Status**: Open
**Site**: `datalog/executor/constraints_impl.go` — `timeExtractionConstraint`, `rangeConstraint`, `equalityConstraint` have no production constructors (all construction is in `_test.go`); the production pushdown path uses `constraints.TimeRangeConstraint` and the fetch/`ValuesEqual` route in `query_executor.go`. `rangeConstraint` semantics are pinned by `canonical_comparison_test.go`. Decide: wire them into production pushdown, or delete.

### C7. Small dead exports

**Status**: Open
- `executor.WrapStreamingAsLazy` (`lazy_seq_relation.go`) — zero references anywhere, including tests.
- `executor.Result` alias (`executor_utils.go`) — zero references anywhere; retained only as a compat shim.
- `executor.NewLazySeqRelation` — test-only (the *type* is production-live via struct literals and `scan_sharing_matcher.go`).
- `executor.NewMaterializedRelationNoDedupe` (no-options variant) — test-only; production uses the `WithOptions` variant.
- `executor.MaterializeResult` — test-only.

### C8. `BadgerMatcher.MatchWithHistory` / `MatchAsOf` — superseded temporal API

**Status**: Open
**Site**: `datalog/storage/matcher.go`

Confirmed test-only and *not* the implementation behind time-travel: `Database.AsOf(txID)`/`Database.History()` flow through the normal match path gated by `isHistoryMode()`. These two are an older direct-pattern API that the gated path superseded.

---

## Class D — repetition

### D1. Symbol-position lookup: ~30 inline copies plus 4 duplicate implementations

**Status**: Open

The shape `for i, sym := range symbols { if sym == target { idx = i; break } }` is the most-repeated block in the codebase. Duplicate named implementations of the same operation: `SymbolIndex` free function (`executor/relation.go`), `MaterializedRelation.SymbolIndex`, `query.Relation.SymbolIndex` (`query/types.go`), `storage.findVariableSymbol` (`matcher_relations.go`, args reversed), `reusingIterator.getSymbolIndex` (`matcher_iterator_reusing.go`). Inline copies cluster in `executor/aggregation.go` (8 sites), `executor/query_executor.go` (5, including two near-verbatim `keyIndices` builders), `executor/pull.go` (3), storage matcher files (5), and others.

Two copies run **per tuple** inside hot loops: `aggregation.go` (single-aggregation collection loop re-scans symbols for `agg.Arg` on every tuple — the grouped path already hoists an index table) and `pull.go:259`. These are perf defects as well as copies.

One slice-level `indexOf([]query.Symbol, query.Symbol) int` with the named variants forwarding to it covers everything; an index-table variant (`symbolIndexTable(symbols, targets) []int`) covers the `groupIndices`/`orderBySymbolIndices`/`keyIndices` family.

### D2. `BindingForm` lacks `BoundVariables()` — the 4-case switch is written ~15 times

**Status**: Open

`BindingForm` (`query/types.go`) exposes only `isBindingForm()`/`String()`, so every consumer re-writes the `TupleBinding`/`RelationBinding`/`ScalarBinding`/`CollectionBinding` switch: `planner/clause_utils.go` (3), `parser/parser.go` (2), `executor/query_executor.go` (5), `planner/explain_analysis.go` (3), `executor/or_fallback_relation.go` (3), `executor/subquery.go`, `executor/relation_ops.go`. One additive interface method (`BoundVariables() []Symbol`, four one-line implementations) collapses all of them. A6's `bindingSymbols` silent default disappears with it.

### D3. Per-tuple bindings-map construction: 7 core copies

**Status**: Open

`bindings := make(map[query.Symbol]interface{}); for i, sym := range symbols { bindings[sym] = tuple[i] }` — simple form in `iterator_composition.go` (×2, guarded) and `relation.go` (×2, unguarded); clear-and-repopulate form in `relation_ops.go` (×2) and `theta_join.go`; variants with extra logic in `subquery.go` and `executor.go`. One populate function (clear + constant prelude + guarded fill) covers them. Note this whole shape is also the per-tuple allocation the predicate-compilation architecture question would eliminate; consolidating now still pays either way.

### D4. Eight recursive clause-tree walkers

**Status**: Open

Eight functions re-implement the "range clauses → type-switch → recurse into `NotClause`/`Or*Clause` branches/`SubqueryPattern`" skeleton: `parser.ExtractVariables`, `query.positionSymbolsInClauses`, planner's `extractClauseSymbols` family (+ `collectSubqueryInputSymbols`, `collectDataPatternSymbols`), executor's `collectInnerVars`, `collectOrBranchRequiredSymbols`, `branchesNeedCorrelatedExecution`/`clausesNeedCorrelation`. A single clause-walk framework in `query` (visitor over the traversal skeleton) unifies them; this is the largest structural item and needs its own design. `ExtractVariables`' historically missing clause cases were one of the silent-default detonations — a shared walker makes that class impossible to reintroduce per-walker.

### D5. Parser's admitted copies have a spurious rationale

**Status**: Open
**Sites**: `parser/predicate_parser.go` `validateDatabaseRefPredicate` and `extractKeywordPredicate`, each commented "This is a copy for the predicate parser to avoid circular imports" — but both files are `package parser`. The originals (`validateDatabaseRef`, `extractKeyword` in `function_parser.go`) are directly callable. Delete the copies, repoint the callers.

### D6. Symbol-membership check: 3 named + 3 inline implementations

**Status**: Open

`executor.contains` (`relation.go`), `executor.symbolInSlice` (`relation_properties.go`), `planner.containsSymbol` (`phase_contract.go`), plus inline copies in `query/types.go` (×3, inside the E/A/V/T dedup) and executor join code. One shared function in `query`.

---

## Class E — structural landmine

### E1. Two `Materialize()` conventions coexist with nothing enforcing either

**Status**: Open

`StreamingRelation.Materialize()` and `LazySeqRelation.Materialize()` return the **receiver** (lazy replay caching); `ProductRelation`, `UnionRelation`, `PrependedRelation`, `OrFallbackRelation`, `LimitRelation`, `StreamingAggregateRelation` return a **fresh** `*MaterializedRelation`. Every `r.Materialize().Method()` delegation in the codebase terminates *only because* the receiver-returning types happen never to write that shape (they route through `realizeAll()` or iterate locally). All current chains were proven terminating in this audit — but the `EnableIteratorComposition` recursion was exactly a violation of this unwritten invariant, and nothing prevents the next one. Either document the invariant with guard comments on both receiver-returning `Materialize()` implementations, or unify the convention.

---

## Suggested sequencing

1. **Class A** — the live defects: A1 (surface the Eval error), A2 (default arm), A3 (first-error-wins guards), A4 (Tx panic), A5 (reject at write boundary), A6 (loud defaults). Small, independent, each pinnable with a test.
2. **B1 + B2** — `ArithmeticOp` and time-field symbolization, mirroring the CompareOp commit shape (pre-interned symbols, parse-time resolution, pointer dispatch), deleting the two duplicate time switches with it.
3. **Class C** — per-symbol API decisions under the standard-of-proof note: C1/C2/C3 whole units, C7 small exports, C8 temporal leftovers, the C4 interface-method prune, and the C5 wire-or-remove and C6 decisions. Each removal is an API-surface change for the compatibility notes, not silent cleanup.
4. **Class D cheap wins** — D5 (delete the parser copies), D2 (`BoundVariables()`), D6, D1, D3.
5. **D4** — the clause-walk framework, as its own designed change.
6. **E1** — decide the `Materialize()` convention and enforce or document it.
