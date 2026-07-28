# Antipattern and Repetition Audit — 2026-07

**Date**: 2026-07-19
**Scope**: `datalog/query`, `datalog/executor`, `datalog/parser`, `datalog/planner`, `datalog/storage`, `datalog/qb`
**Method**: static caller analysis (production vs `_test.go` callers separated for every flagged symbol), switch/default enumeration, delegation-chain termination proofs, and pattern sweeps for the antipattern classes eliminated by the value-semantics consolidation (PR #112).

**Evidence-scope caveat (added 2026-07-19; scoped 2026-07-26)**: every "zero callers" / "test-only" claim in this document is an **in-repository** observation, which settles deadness for anything that is not consumer-facing API.

**What is consumer-facing is an editorial fact, not a capitalization.** Go's export marker is how a package's files and tests reach each other and how sibling packages compose; it is not a versioning contract. API is what the module *documents and invites consumers to import* — a doc comment declaring an audience, an entry in the upgrade guide, a documented extension point. That declaration is the counter-evidence a workspace index cannot see, and only then does deletion become a compatibility decision for the owner. Absent one, capitalization means nothing and the reference count settles it.

Two entries below meet that bar and are correctly escalated: C9 (`executor/testing.go`, whose own doc comment reads "Exported for use by external test packages") and C5's `RegisterCustomFunction` (documented extension point). The rest of Class C is engine machinery that happens to be capitalized so sibling packages and tests can reach it.

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

**Status**: Resolved (2026-07-19). The first Eval error stops the loop and surfaces as the result's deferred error; the source relation's own deferred error carries through first. Pinned by `TestMaterializedRelation_FilterWithPredicate_PropagatesEvalError` and `_CarriesSourceError`.
**Site**: `datalog/executor/relation.go` (`if passes, err := pred.Eval(bindings); err == nil && passes`)

A predicate `Eval` error silently drops the tuple — a truncated-success bug: wrong answer, no signal. Contrast `PredicateFilterIterator.Next` (defers the error) and `filterWithPredicateAndLookup` in `relation_ops.go` (fails fast). Six relation types delegate their `FilterWithPredicate` to this method (`ProductRelation`, `LimitRelation`, `UnionRelation`, `PrependedRelation`, `OrFallbackRelation`, `StreamingAggregateRelation`), so all inherit the swallow.

Reachability caveat: production predicate filtering flows through `filterWithPredicateAndLookup`/`thetaJoinWithPredicate`, so the swallow is **latent on production paths but live on the public `Relation` API** — the same masked-by-disuse profile as the `EnableIteratorComposition` recursion. A repo-wide sweep confirmed this is the only site of the `err == nil &&` swallow shape.

### A2. `ChainedComparison.Eval` has no default arm for unknown operators

**Status**: Resolved (2026-07-19). The operator switch errors on unknown symbols, matching `Comparison.Eval`. Pinned by `TestChainedComparisonUnknownOperatorErrors`.
**Site**: `datalog/query/predicate.go` (operator switch inside the adjacent-pair loop)

An unknown/unhandled `Op` leaves `ok = false` and returns `(false, nil)` — the predicate silently filters out every tuple with no error. Its sibling `Comparison.Eval` returns `fmt.Errorf("unknown comparison operator: %v", c.Op)` for the same condition. The chained form should fail identically loudly.

### A3. Unguarded deferred-error tails can clobber earlier errors

**Status**: Resolved (2026-07-19). All six sites use the first-error-wins guard.
**Sites**: `datalog/executor/or_fallback_relation.go:943, 1034, 1182` (three identical copies of the outer-exhaustion tail, `it.err = it.outerIter.Error()` unconditional); `datalog/executor/relation_ops.go:419, 519`; `datalog/executor/relation.go:324` (`CachingIterator` completion)

The codebase discipline is first-error-wins (`if e := it.Error(); err == nil { err = e }`), applied correctly at many sites (e.g. `relation_ops.go:134, 354`, the storage matcher tails, `union_relation.go:273`). The listed sites assign unconditionally; some are safe today only because no earlier assignment exists on that path, which is exactly the fragile shape. The three `or_fallback` copies can overwrite an already-recorded branch error, possibly with nil.

### A4. `matchesDatom` Tx arm silently returns false where E and A arms panic

**Status**: Refuted (2026-07-19). The asymmetry is correct under the boundary model. E and A panic because their bindings are validated at the user boundaries and filtered at seek construction (`filterTypedPositionBindings`), so a mistyped value there is an unreachable invariant violation. The Tx position has **no** boundary validation or construction-time filtering — its default arm is reachable by ordinary interior mixed-type data (a V→Tx join over mixed values), and `return false` is the equality join's typed non-match, exactly like the V position. Making it panic would be a data-reachable panic. Extending boundary typing to the Tx position (ElementID plus integer Lamport sugar) would be a separate design decision, not a defect fix.

### A5. `toStorageValue` default coerces unknown types to strings at the write boundary

**Status**: Resolved (2026-07-19) — by deletion, and the entry's framing was wrong: `toStorageValue` had **zero callers**. It was dead code, not a live write-boundary absorber; the write path never routed through it. Deleted.

### A6. Silent-nil / silent-zero defaults over closed taxonomies

**Status**: Resolved (2026-07-19), except the last bullet, with two ratified behavior changes and one discovered live defect:
- Subquery input assembly (`executor/subquery.go`) errors loudly on unresolved variables and unknown element kinds (was: silent nil binding). Pinned by `subquery_input_validation_test.go`.
- `extractBindingSymbols` covers `ScalarBinding` (previously silently dropped from empty-result schemas) and panics on unknown forms; `BindingForm` verified closed at four implementers.
- **Ratified behavior change**: arithmetic operands must be numbers (int64/float64, Go widths normalized). Non-numeric operands error — and numeric strings are no longer parsed: `(+ "42" 1)` was 43, is now an error. Pinned by `arithmetic_strict_test.go`. `toInt64`/`toFloat64` panic on non-normalized operands (unreachable post-`toNumber`).
- **Ratified behavior change**: vector indices must be integers. `(nth ?vec "1")` returned element 0 (string→0) and `(nth ?vec 1.5)` truncated; both now error (`toVectorIndex`, callers wrap context).
- **Discovered live defect, fixed**: the memory matchers' element handling omitted `VectorConstant` — `matchesElement` returned false (a vector literal matched nothing) and `extractPatternValue` returned nil (treated as unbound). Both handle vectors now; `PatternElement` verified closed at four implementers; defaults panic. Pinned by `pattern_element_taxonomy_test.go`.
- Closed-taxonomy defaults now panic: `extractValue` (storage matcher), `extractProbeKey`, `rangeConstraint` position.
- ~~Still open~~ RESOLVED (2026-07-20): the `timeExtractionConstraint` field switch went with the C6 deletion of `constraints_impl.go`.

Verified non-hazards (documented, principled): schemaless cardinality-one defaults across cache/pull/database, `cardFromOp`'s explicit non-decisive `false`, display-type and `String()` fallbacks.

---

## Class B — string dispatch at evaluation time

The disease: an operation's identity is a string, and the decision of what to do is made by string switch per tuple/per Eval instead of once at parse. `FindAggregate.Function` and `CompareOp` were cured in PR #112; these remain.

### B1. `query.ArithmeticOp` — CompareOp's twin

**Status**: Resolved (2026-07-19). `ArithmeticFunction.Op` is an interned `Symbol`; the parser and qb resolve names once to the pre-interned `SymAdd`/`SymSubtract`/`SymMultiply`/`SymDivide`, and Eval dispatches by pointer equality. The `ArithmeticOp` string type and its constants are deleted. Pinned by `TestArithmeticOperatorsResolveToInternedSymbols`.
**Sites**: `datalog/query/function.go` (`type ArithmeticOp string`, constants `OpAdd`/`OpSubtract`/`OpMultiply`/`OpDivide`; `ArithmeticFunction.Eval` switches on `a.Op` twice per call — float path and int path — plus `useFloat := a.Op == OpDivide`)

Per-tuple, production-reachable through the expression evaluation loop. The parser (`parser/function_parser.go`) and qb (`qb/expression.go`) already resolve names once at setup, so symbolization mirrors the CompareOp change exactly: pre-interned `SymAdd`/`SymSub`/`SymMul`/`SymDiv`, pointer switches in Eval. ~3 production files; note `storage/set_entry.go` has an unrelated `OpAdd` namesake (uint8 CRDT op).

### B2. `TimeExtractionFunction.Field` — string kind, switch triplicated

**Status**: Resolved (2026-07-19) for the live copy, and fully closed (2026-07-20): `Field` is an interned `Symbol` (pre-interned `SymYear`…`SymSecond`), resolved once in `parseTimeExtraction` and in the qb builders, dispatched by pointer equality in Eval. Pinned by `TestTimeExtractionFieldsResolveToInternedSymbols`. Both duplicated dead/test-only copies are gone: `timeExtractionConstraint` with the C6 deletion, `extractTimeComponent` with the C5 dead-value-functions deletion. One copy of the six-way switch remains — the live one.
**Sites**: `datalog/query/function.go` (`Field string`, per-tuple switch in `Eval`); the same six-case switch duplicated in `datalog/executor/custom_functions.go` `extractTimeComponent` (dead — see C5) and `datalog/executor/constraints_impl.go` `timeExtractionConstraint.Evaluate` (per-datom, but test-only-constructed — see C6)

One interned time-field symbol set would replace all three; two of the three copies are better deleted than symbolized.

### B3. `query.FunctionPredicate.Fn` — per-tuple string switch, mostly unimplemented

**Status**: Resolved (2026-07-19), with the extension-mechanism question ruled. `str/starts-with?` is a concrete predicate type (`StrStartsWithPredicate`: unbound term errors, non-string operand is a typed non-match, `strings.HasPrefix` otherwise), constructed by the parser at parse time — no per-tuple string switch. `FunctionPredicate` remains as the placeholder for names the parser doesn't recognize; its `Eval` errors unconditionally. Ruled: user-defined functions ARE a supported feature (`str/starts-with?` was their forcing test), so the placeholder's replacement is the C5 registry wiring, not deletion.
**Site**: `datalog/query/predicate.go` (`Fn string`; `Eval` switches per tuple; only `"str/starts-with?"` is implemented, everything else errors)

Symbolizing the string is less the point than deciding what this predicate extension mechanism should be (a registry of concrete predicate types, most likely). Constructed only by the parser.

### B4. `query.OrderDirection` — minor

**Status**: Resolved (2026-07-19). `OrderByClause` carries `Descending bool` (zero value = ascending); `OrderDirection`/`OrderAsc`/`OrderDesc` are deleted. Removed Go API — changelog-worthy.
**Site**: `datalog/query/types.go` (`type OrderDirection string`; compared per sort-comparison in `executor_utils.go` `compareTuplesByOrder`)

Two-valued flag, typed-constant comparison rather than multi-way dispatch. If touched at all, the fix is a bool/int, not a symbol. Lowest priority in this class.

---

## Class C — no internal consumers: test-only or unwired exported surface

The deleted-`Filter` profile: exported machinery whose only in-repo initiators are tests, while production does the equivalent work elsewhere. Each entry lists what the production path actually is.

**Standard of proof — read before acting on this class (scoped 2026-07-26).** "No production references" throughout this audit means: *zero callers inside this repository outside `_test.go` files*. That standard settles deadness for engine machinery whether or not it is capitalized — capitalization is how sibling packages and tests reach a symbol, not a published contract. It does **not** settle deadness where the code declares an audience the workspace index cannot see: a doc comment naming external consumers, or a documented extension point. In this class that is C9 (`executor/testing.go`) and C5's `RegisterCustomFunction`; for those two, deletion is an owner-level compatibility decision. Everything else here is an ordinary internal call, made on the role facts (what the artifact *is* in the architecture), not on its capital letter.

### C1. `UnionRelation` — entire subsystem test-only

**Status**: Resolved (2026-07-20) — deleted by owner ruling, made with the role facts on the table: this was the concurrent result-merge relation (channel-fed union with streaming dedup and build-once-replay-many caching) whose producer half — the worker pool — was already gone, leaving the orphaned consumer half of parallel subquery execution. Subsystem file and its dedicated tests removed; scaffolding tests converted or pruned. Not the algebra's ∪: a future `Relation.Union` (see C4's ruling) is specified set-natively as pass-through plus anti-membership, so nothing here is a rebuild target.
**Site**: `datalog/executor/union_relation.go` (type, `NewUnionRelation`, `UnionIterator`, `relationItem`, `newUnionBuildIterator`)

Every constructor call is in tests. Production OR/union execution goes through `executeOrClauseUnion` and `OrFallbackRelation`.

### C2. `PrependedRelation` — whole file test-only

**Status**: Resolved (2026-07-20) — deleted by owner ruling with the role stated: peek-without-losing-the-first-tuple was a third mechanism for the single-use-iterator tension that `BufferedIterator` and the LazySeq cells already solve; the owner chose not to keep three.
**Site**: `datalog/executor/prepended_relation.go`; `NewPrependedRelation` is called only from `wrapper_relation_copy_test.go`.

### C3. `TransformIterator` and `ConcatIterator` — test-only

**Status**: Resolved (2026-07-20) — deleted by owner ruling: generic map/concat combinators that purpose-built iterators displaced. No return path exists even for a future `Relation.Union`: union over Relations is specified set-natively (left streamed through, right anti-membership-filtered — Relations at every point, no bag intermediate), so no concatenation primitive is ever needed. The file's other iterators (`ProjectIterator`, `DedupIterator`, `PredicateFilterIterator`, `FunctionEvaluatorIterator`) remain — the latter two are live organs of the streaming algebra-method implementations retained under C4's ruling.
**Site**: `datalog/executor/iterator_composition.go`; only `iterator_composition_test.go` references them.

### C4. `Relation` interface methods with no production initiator

**Status**: Ruled (2026-07-20) — **keep and reunify**; execution pending as a designed arc. The finding's framing was incomplete: these six methods are the relational-algebra operator set — σ (`Select`, `FilterWithPredicate`), ⋉ (`SemiJoin`), ▷ (`AntiJoin`), extend (`EvaluateFunction`), γ (`Aggregate`) — on the engine's fundamental abstraction, which together with `Join`/`HashJoin`/`Project`/`Sort` make `Relation` algebraically closed. The owner ruled the *engine's free-standing operators* are the deviation, not the methods: the engine is to consume the algebra through the interface, each operator getting one generic body dispatching on both operands' declared properties (`Properties().Keys`/`Ordering`, `RequiresCopy()`, plus a new `Replayable()` declaration), with `Union` added as the missing ∪ operator (set-native: pass-through plus anti-membership). Design document for signatures and the conversion inventory is owed for ratification before implementation.
**Sites**: `Select`, `SemiJoin`, `AntiJoin`, `FilterWithPredicate`, `EvaluateFunction`, `Aggregate` on the `Relation` interface (`datalog/executor/relation.go`), plus the free functions `Select`, `SemiJoin`, `AntiJoin`

Every implementation delegates (to a free function or a sibling implementation); only tests initiate the calls. Production equivalents actually used: `filterWithPredicateAndLookup` + `thetaJoinWithPredicate` (filtering), `executeExpression` (function evaluation), `filterWithNotClause`/`filterWithNotJoinClause` via the NOT executors (anti-join; `antiJoinOnSymbols` was deleted 2026-07-19 as a dead pair), `ExecuteAggregations` (aggregation — its free function is core-live; only the *method* is test-only). This is exactly how the dead `Filter` method hid. These are public API surface, so each removal is an owner decision; A1's swallow lives on one of them.

Production-initiated interface methods (keep): `ProjectFromPattern`, `Join`, `HashJoin`, `Materialize`, `Sort`, `Project`, `Iterator`, `Symbols`, `Options`, and the rest of the core surface.

### C5. `custom_functions.go` — exported extension point with an unwired consumption side

**Status**: Resolved (2026-07-20) — owner ruled wire it; executed. The registry moved to the function-namespace home (`query.DefaultRegistry.RegisterImplementation`/`Implementation`, mutex-guarded, metadata + callable in one registration); `executor.RegisterCustomFunction`/`CallCustomFunction` forward to it. `FunctionPredicate.Eval` consults the registry (bool contract enforced loudly; unknown names keep the loud error), and expression position parses to `query.CustomFunction` for registered names (results normalized via `datalog.NormalizeValue`; unknown names still rejected at parse). End-to-end both-modes pin: `executor/custom_functions_test.go`. The dead unexported value functions (`addValues`, `subtractValues`, `multiplyValues`, `divideValues`, `concatenateStrings`, `extractTimeComponent`) were deleted by owner ruling (2026-07-20, zero callers re-verified); `custom_functions.go` now holds only the two registry forwards. The vestigial `same-date?` registration in `executor_subquery_test.go` registers into the live registry but its query still does not use it.
**Site**: `datalog/executor/custom_functions.go`

`RegisterCustomFunction` is a deliberate exported extension point (mutex-guarded registry, documented "for use in expressions"). Its consumption side, `CallCustomFunction`, now has zero callers: the only consultation point was `evaluateComparison`, deleted with the `Filter` system — which also severed the one downstream-reachable invocation path (`RegisterCustomFunction` + `rel.Filter(ComparisonFilter{Function: "my-fn"})`, both formerly public). The live query path never consulted the registry: `FunctionPredicate.Eval` errors on unknown names without checking it. As of this branch the registry is **write-only** — a downstream consumer can register functions the engine will never invoke.

This is not dead-code cleanup; it is a product decision about whether user-defined functions are a supported feature:
- **Wire it**: connect the registry to the live evaluation path (`FunctionPredicate` / expression evaluation — the same redesign B3 calls for), making registration functional again and better than the severed `Filter`-based path ever was.
- **Remove it**: deprecate `RegisterCustomFunction`/`CallCustomFunction` deliberately, with a changelog entry, as an API decision.

The unexported value functions in the file (`addValues`, `subtractValues`, `multiplyValues`, `divideValues`, `concatenateStrings`, `extractTimeComponent`) have zero callers and *are* provably dead regardless of the decision above; `extractTimeComponent` is one of the three duplicated time-field switches (B2).

The in-repo registration in `executor_subquery_test.go` registers `same-date?` and then runs a query that never uses it (the test's own comment: "Simplified to test basic functionality first") — the registration is vestigial within its own test, so no current test exercises the registry end to end.

### C6. Test-only constraint implementations

**Status**: Resolved (2026-07-20) — owner ruled delete; executed. `constraints_impl.go` (`timeExtractionConstraint`, `rangeConstraint`, `equalityConstraint`) is gone: a parallel constraint home only tests kept alive, while the production pushdown uses `constraints.TimeRangeConstraint` and the fetch/`ValuesEqual` route. Its pins moved to the homes of the semantics they pin: the canonical-comparison tests (2^53 int64 exactness, typed range ordering, `ValuesEqual` strictness) now assert directly against `datalog.CompareValues`/`ValuesEqual` in `datalog/compare_canonical_test.go`, and `TestIndexedMatcher_WithConstraints` exercises the matcher's constraint filtering through a test-local `timeWindowConstraint` fixture (the same pattern the storage package's pushdown tests use). This also removed the third copy of the six-way time-field switch (B2 residual).
**Site**: `datalog/executor/constraints_impl.go` (deleted) — the original finding: the three types had no production constructors (all construction was in `_test.go`); the production pushdown path uses `constraints.TimeRangeConstraint` and the fetch/`ValuesEqual` route in `query_executor.go`.

### C7. Small dead exports

**Status**: Resolved (2026-07-20), split by owner ruling on role facts:
- `executor.WrapStreamingAsLazy` — **KEPT** by explicit ruling. Zero references was true and irrelevant: it is the streaming→replayable bridge (lifts a single-use stream into the shared-cell LazySeq algebra without materializing), the lazy implementation the E1 `Materialize()` convention needs on the table.
- `executor.NewLazySeqRelation` — **KEPT** with it (same ruling).
- `executor.NewMaterializedRelationNoDedupe` / `NewMaterializedRelationNoDedupeWithOptions` — **deleted/renamed**: the name advertised an anti-relational capability; the real contract is a set warranty. Superseded by the boundary constructor `NewMaterializedRelationFromSet` (admission-checks raw values entering relational flow) and the interior `newMaterializedRelationFromSet` (operator-constructed, carries derived properties, no re-validation). The two `join.go` sites moved to the interior constructor, eliminating their post-hoc `result.properties = …` pokes; the three empty-relation `hash_join_matcher.go` sites moved to the ordinary constructor; test sites converted per-site, with deliberate-duplicate fixtures (the semi/anti dedup pin, the duplicate-stress benchmark) rebuilt as raw struct literals so no constructor lies about set-ness.
- `executor.Result` alias — deleted (compat shim from the 2025 Result/Relation unification, decayed to zero references).
- `executor.MaterializeResult` — deleted (materialize-with-symbol-relabel utility; the engine renames at binding boundaries, not via relation operators).

### C8. `BadgerMatcher.MatchWithHistory` / `MatchAsOf` — superseded temporal API

**Status**: Resolved (2026-07-19) — deleted by owner ruling. The six test call sites converted to the supported path (`matcher.History()` / `matcher.AsOf(txID)` + `Match`), binding `?tx` where distinct history entries would otherwise deduplicate in the relation, and expressing "as-of before any writes" as a nonzero pre-first-write ElementID (the zero ElementID means history mode in the three-mode convention).
**Site**: `datalog/storage/matcher.go`

Confirmed test-only and *not* the implementation behind time-travel: `Database.AsOf(txID)`/`Database.History()` flow through the normal match path gated by `isHistoryMode()`. These two are an older direct-pattern API that the gated path superseded.

### C9. `executor/testing.go` — exported test utilities with no in-repo references

**Status**: Resolved (2026-07-20) — owner ruled keep + fix the swallows. The three swallows were already fixed in place by the error-swallow sweep (`c6d11d0`): comparison verdicts return `(bool, error)`, the diff/summary formatters include scan failures in their output, and the tuple collectors propagate iterator errors. The exported surface stays published for external test packages. (Found 2026-07-19 during the error-swallow cleanup.) Every exported function in the file (`DualTestExecutorVariants`, `DualTestExecutorVariantsWithBase`, `CompareRelations`, `CompareRelationsIgnoreSymbolOrder`, `RelationDiff`, `FormatRelationSummary`) has **zero references in this repository**, and the unexported layer (`compareSymbols`, `collectSortedTuples`, `collectSortedTuplesReordered`, `compareTuplesOrder`, `compareTuplesEqual`) serves only them. Three of the error-swallow sweep's B-shape sites live in these functions. **In-repo reference counts do not establish deadness here**: these are exported symbols of a published module, and the file's own doc comments declare the audience — "Exported for use by external test packages" — which is exactly what a workspace index cannot see. Whether downstream consumers use this surface is the owner's knowledge. The decision is a public-API-surface one: delete (a compatibility-notes change) or keep, and if kept, fix the three swallows in place.

---

## Class D — repetition

### D1. Symbol-position lookup: ~30 inline copies plus 4 duplicate implementations

**Status**: Resolved (2026-07-19). `query.SymbolIndex`, `query.ContainsSymbol`, and `query.SymbolIndexTable` are the canonical operations (pinned by `query/symbol_index_test.go`); the exported variants (`executor.SymbolIndex`, `MaterializedRelation.SymbolIndex`, `query.Relation.SymbolIndex`) forward to them; the unexported duplicates are deleted and ~40 inline copies converted, each preserving its site's absent-symbol handling. The two per-tuple copies are hoisted (batch single-aggregation argument positions; the symmetric hash join's per-pair join-symbol scan). Benchmarks: allocations identical, timing within noise. Deferred sites are recorded in D7.

The shape `for i, sym := range symbols { if sym == target { idx = i; break } }` is the most-repeated block in the codebase. Duplicate named implementations of the same operation: `SymbolIndex` free function (`executor/relation.go`), `MaterializedRelation.SymbolIndex`, `query.Relation.SymbolIndex` (`query/types.go`), `storage.findVariableSymbol` (`matcher_relations.go`, args reversed), `reusingIterator.getSymbolIndex` (`matcher_iterator_reusing.go`). Inline copies cluster in `executor/aggregation.go` (8 sites), `executor/query_executor.go` (5, including two near-verbatim `keyIndices` builders), `executor/pull.go` (3), storage matcher files (5), and others.

Two copies run **per tuple** inside hot loops: `aggregation.go` (single-aggregation collection loop re-scans symbols for `agg.Arg` on every tuple — the grouped path already hoists an index table) and `pull.go:259`. These are perf defects as well as copies.

One slice-level `indexOf([]query.Symbol, query.Symbol) int` with the named variants forwarding to it covers everything; an index-table variant (`symbolIndexTable(symbols, targets) []int`) covers the `groupIndices`/`orderBySymbolIndices`/`keyIndices` family.

### D2. `BindingForm` lacks `BoundVariables()` — the 4-case switch is written ~15 times

**Status**: Resolved (2026-07-19). `BoundVariables() []Symbol` on the sealed interface (four one-line implementations, pinned by `TestBindingFormBoundVariables`); the symbol-extraction switches in parser, executor, planner, and algebra collapse to direct method calls (no wrapper functions). Switches over `Expression.Binding`/`Subquery.Binding` keep their form — those `interface{}` fields carry a bare `Symbol` case outside the taxonomy — and the structural per-form switches (`applyBindingForm`, or-fallback cardinality checks) rightly remain. The collapse fixed a live gap: `parser.ExtractVariables` was missing `ScalarBinding`, so scalar-bound subquery variables were invisible to variable extraction (pinned by `TestExtractVariablesCoversEveryBindingForm`).

`BindingForm` (`query/types.go`) exposes only `isBindingForm()`/`String()`, so every consumer re-writes the `TupleBinding`/`RelationBinding`/`ScalarBinding`/`CollectionBinding` switch: `planner/clause_utils.go` (3), `parser/parser.go` (2), `executor/query_executor.go` (5), `planner/explain_analysis.go` (3), `executor/or_fallback_relation.go` (3), `executor/subquery.go`, `executor/relation_ops.go`. One additive interface method (`BoundVariables() []Symbol`, four one-line implementations) collapses all of them. A6's `bindingSymbols` silent default disappears with it.

### D3. Per-tuple bindings-map construction: 7 core copies

**Status**: Resolved (2026-07-19). One `bindTuple(dst, symbols, tuple)` in `executor/tuple_bindings.go`; nine sites converted. Owner ruling: unified **loud** — a tuple narrower than its symbols is a broken Relation invariant and panics (the four previously guard-skipping sites lost their silent absorption). Pinned by `tuple_bindings_test.go`. Call sites keep their own clear/constant-prelude steps, which legitimately differ.

`bindings := make(map[query.Symbol]interface{}); for i, sym := range symbols { bindings[sym] = tuple[i] }` — simple form in `iterator_composition.go` (×2, guarded) and `relation.go` (×2, unguarded); clear-and-repopulate form in `relation_ops.go` (×2) and `theta_join.go`; variants with extra logic in `subquery.go` and `executor.go`. One populate function (clear + constant prelude + guarded fill) covers them. Note this whole shape is also the per-tuple allocation the predicate-compilation architecture question would eliminate; consolidating now still pays either way.

### D4. Eight recursive clause-tree walkers

**Status**: Resolved (2026-07-19), with a ratified refinement. `query.WalkClauses` (clause_walk.go) is the single traversal: full 19-type taxonomy enumerated (13 leaves explicit, 6 compounds descend), per-visit descend control, loud default — a new clause type must decide its traversal in exactly one place. Migrated: `positionSymbolsInClauses`, `executor.collectInnerVars`, `executor.clausesNeedCorrelation`. **Refinement (owner-approved)**: walkers whose compound handling is branch-scoped set algebra — `parser.ExtractVariables` (OR-branch intersection), `collectOrBranchRequiredSymbols` (per-branch two-pass), the planner `extractClauseSymbols` family (per-clause Requires/Provides dispatch) — stay bespoke; a flat visitor cannot express their semantics. `ExtractVariables` instead got the loud-enumeration treatment in place (explicit predicate leaves, panic default), which surfaced and fixed its missing `ScalarBinding` case and flushed out the dead `query.Subquery` clause type (deleted, with its six dead consumer arms across planner and executor). Investigating `collectInnerVars`'s explicit-join handling during migration surfaced the open NOT/or-join planning bug — see `docs/bugs/resolved/BUG_NOT_CLAUSE_SCOPED_BODY_SYMBOLS_UNPLANNABLE.md`.

Eight functions re-implement the "range clauses → type-switch → recurse into `NotClause`/`Or*Clause` branches/`SubqueryPattern`" skeleton: `parser.ExtractVariables`, `query.positionSymbolsInClauses`, planner's `extractClauseSymbols` family (+ `collectSubqueryInputSymbols`, `collectDataPatternSymbols`), executor's `collectInnerVars`, `collectOrBranchRequiredSymbols`, `branchesNeedCorrelatedExecution`/`clausesNeedCorrelation`. A single clause-walk framework in `query` (visitor over the traversal skeleton) unifies them; this is the largest structural item and needs its own design. `ExtractVariables`' historically missing clause cases were one of the silent-default detonations — a shared walker makes that class impossible to reintroduce per-walker.

### D5. Parser's admitted copies have a spurious rationale

**Status**: Resolved (2026-07-19). Both copies deleted; `parseMissingAttrPredicate` calls the originals. (Both files are `package parser`; the circular-import rationale was false.)
**Sites**: `parser/predicate_parser.go` `validateDatabaseRefPredicate` and `extractKeywordPredicate`, each commented "This is a copy for the predicate parser to avoid circular imports" — but both files are `package parser`. The originals (`validateDatabaseRef`, `extractKeyword` in `function_parser.go`) are directly callable. Delete the copies, repoint the callers.

### D6. Symbol-membership check: 3 named + 3 inline implementations

**Status**: Resolved (2026-07-19). One `query.ContainsSymbol`. The sweep found more duplicates than the audit's scope: `algebra.containsSymbol` (algebra was outside the audited packages) and `executor.contains`, both deleted alongside the listed ones; `reusingIterator.getSymbolIndex` was already caller-less. All inline membership scans converted, including the E/A/V/T dedup in `DataPattern.Symbols`.

`executor.contains` (`relation.go`), `executor.symbolInSlice` (`relation_properties.go`), `planner.containsSymbol` (`phase_contract.go`), plus inline copies in `query/types.go` (×3, inside the E/A/V/T dedup) and executor join code. One shared function in `query`.

### D7. Deferred conversions from the D1/D3 sweep

**Status**: Resolved (2026-07-20) — owner ruled execute. Disposition per bullet: the matcher's cache-path closures were already on `getTupleBuilder`/`BuildTupleInterned`; the two remaining un-hoisted per-datom loops converted — `validatingVBoundIterator.buildTuple` deleted (the struct already carried the hoisted `InternedTupleBuilder`; both call sites now use it, which also unifies the T-position representation with the cache paths' interned `*ElementID`), and `cardinalityManyAVETValueIterator.buildTuple` fills through a construction-time `query.TupleIndexer` (it emits from add-wins state, not a datom, so the indexer applies directly). The `evaluateExpressionWithLookup` bullet was closed by the expression-binding unification: `alignBinding` precomputes the position table once and `apply` uses it per tuple. Bullets three and four were already settled as recorded below (converted in place; left as is).

Sites the consolidation sweep deliberately did not convert; each needs more than a mechanical repoint:

- **Storage matcher per-datom position-resolve-and-assign loops** (~9 sites in `matcher_relations.go`: `buildTuple` and the match paths around its cache/vector/validation branches). Each compares `pattern.GetX().(Variable).Name == sym` per datom and assigns `tuple[i]` — the classic un-hoisted position lookup. The intended replacement already exists (`query.NewPatternExtractor` / the tuple-indexer machinery, which precompute the position table); converting is an adoption refactor, not a repoint.
- **`evaluateExpressionWithLookup` find-and-assign borderlines** (`executor/relation_ops.go`, the tuple-binding value-placement loops): per-tuple find-index-then-assign over `newSymbols`; hoisting requires restructuring the binding-placement logic around a precomputed table.
- **`filterBranchToOuterTuple` shared-pair discovery** (`executor/or_fallback_relation.go`): runs per outer tuple, but each call re-executes an entire correlated branch, so the symbol scan is noise; hoisting would couple the filter to schema stability across branch re-executions for no measurable gain. Converted to `query.SymbolIndex` in place, not hoisted.
- **`executor.go` constant-extraction variant** (`buildBoundRelation`): a conditional one-shot populate at setup, not the per-tuple Shape-2; left as is.

### D8. Planner `extractClauseSymbols` dispatch: silent default and missing predicate arms

**Status**: Resolved (2026-07-19) — by deletion, via the NOT scoped-body fix (step B). The entire `extractClauseSymbols` family is gone; the planner consumes `query.ScopeOf` (`query/clause_scope.go`), which covers all nine predicate types with one `case Predicate:` arm (`Correlates: RequiredSymbols()`) and panics loudly on unknown clause forms. The four previously uncovered predicate types thereby entered phase-liveness accounting; the scheduling effects rode the step-B red-first tests (`planner/not_scheduling_test.go`, `query/clause_scope_test.go`). See `docs/bugs/resolved/BUG_NOT_CLAUSE_SCOPED_BODY_SYMBOLS_UNPLANNABLE.md` ("Refinements ratified during step B").

The dispatch (`clause_utils.go`) has a silent `default: return ClauseSymbols{}` over the closed clause taxonomy, and its explicit predicate arms cover only five of the nine predicate types — `StrStartsWithPredicate`, `FunctionPredicate`, `DatabaseFunctionPredicate`, and `TxRangePredicate` fall through to "requires and provides nothing." The five covered arms are all `Requires: RequiredSymbols(), Provides: nil`, so one `case query.Predicate:` arm plus a loud default would collapse them and close the gap — but that changes phase-liveness accounting for the four uncovered types, so it needs its own red tests and a look at scheduling effects. Related: the NOT/or-join Requires computation in the same file is the subject of `docs/bugs/resolved/BUG_NOT_CLAUSE_SCOPED_BODY_SYMBOLS_UNPLANNABLE.md`.

---

## Class E — structural landmine

### E1. Two `Materialize()` conventions coexist with nothing enforcing either

**Status**: Open

`StreamingRelation.Materialize()` and `LazySeqRelation.Materialize()` return the **receiver** (lazy replay caching); `ProductRelation`, `UnionRelation`, `PrependedRelation`, `OrFallbackRelation`, `LimitRelation`, `StreamingAggregateRelation` return a **fresh** `*MaterializedRelation`. Every `r.Materialize().Method()` delegation in the codebase terminates *only because* the receiver-returning types happen never to write that shape (they route through `realizeAll()` or iterate locally). All current chains were proven terminating in this audit — but the `EnableIteratorComposition` recursion was exactly a violation of this unwritten invariant, and nothing prevents the next one. Either document the invariant with guard comments on both receiver-returning `Materialize()` implementations, or unify the convention.

---

## Addendum — external review of 2026-07-19 (against `4a43b5d`), audited and confirmed

The review's two live findings are ledgered: `docs/bugs/resolved/BUG_CORRELATED_ORJOIN_GLOBAL_FALLBACK_DROPS_TUPLES.md` — confirmed by reproduction, fixed 2026-07-19 (correlated or/or-join now round-trips through the algebra bridge as itself; the fallback encoding was never a valid lowering for union semantics, a wider defect than the review's correlation-key symptom); `docs/bugs/resolved/BUG_DISJOINT_NOT_FAILS_AT_EXECUTOR.md` — confirmed mechanically, ruled and resolved 2026-07-19: fully-disjoint NOT is rejected at planning, the global-anti-join reading declined, with the aggregate-subquery idiom as the expressible existence gate. Its two cleanups, confirmed:

### R1. `query.unionSymbolSets` is dead

**Status**: Resolved (2026-07-19). Deleted with the or-join fix; `gopls references` re-confirmed zero callers at deletion time. Zero callers after the or-default union→intersection change and the or-default-join declared-outputs change orphaned it (`datalog/query/clause_scope.go`). Unexported, so deadness was provable in-repo.

### R2. `tests/identity_comparison_test.go` narration is inverted

**Status**: Resolved (2026-07-19). Rewritten to the current model, which is interned-pointer, not the `[20]byte` value this entry guessed: `Identity = *identity` and every constructor (including the storage decode path) interns by hash, so `==` is pointer equality that interning makes hash equality. The test now asserts `==`, `.Equal()`, and `Hash()` all positively — the interning invariant is load-bearing and deserves the pin.

Original finding: the comments at lines 72-79 referenced `str`/`l85` struct fields that no longer exist, and the test's narration was backwards — the branch the test called "expected" (struct fields differ) was the impossible one and the "luck" branch the guaranteed one.

## Addendum — external review round 2 (against `a58c0f8`), audited and confirmed

All four findings confirmed against the code:

### R3. `resolveAttributeViaMatcher` CardinalityOne arm swallowed scan errors

**Status**: Resolved (2026-07-19). The error-swallow sweep added the `iter.Error()` guard to the CardinalityMany and CardinalityVector arms but missed the `default:` CardinalityOne arm — the schemaless default on the cache-less/history path. Fixed with the same guard; pinned red-first by `TestResolveEntityAttributesSurfacesScanErrors`. Sweep doc carries the addendum.

### R4. Algebra bridge compiles in source order — NOT before its binder fails only with the optimizer on

**Status**: Open. Ledgered as `docs/bugs/BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md`; fix direction pending an owner ruling (pre-order clauses entering Compile by ScopeOf readiness, or defer unplaceable clauses in compileClausesFrom). This finding also produced the owner ruling that every query test must exercise both optimizer modes — plan of record: `docs/wip/OPTIMIZER_MODE_MATRIX.md`.

### R5. Orphaned `antiJoinOnSymbols` doc comment

**Status**: Resolved (2026-07-19). The deleted function's doc line had survived above `executeOrClauseUnion`; removed.

### R6. `extractEntityIDs` comment described the rejected best-effort contract

**Status**: Resolved (2026-07-19). The comment narrated "prefetch is best-effort and skips" — the design the annotations-are-never-an-error-channel ruling vetoed — while the caller hard-fails the query. Rewritten to state the actual contract.

## Suggested sequencing

1. **Class A** — the live defects: A1 (surface the Eval error), A2 (default arm), A3 (first-error-wins guards), A4 (Tx panic), A5 (reject at write boundary), A6 (loud defaults). Small, independent, each pinnable with a test.
2. **B1 + B2** — `ArithmeticOp` and time-field symbolization, mirroring the CompareOp commit shape (pre-interned symbols, parse-time resolution, pointer dispatch), deleting the two duplicate time switches with it.
3. **Class C** — per-symbol decisions under the standard-of-proof note: C1/C2/C3 whole units, C7 small exports, C8 temporal leftovers, the C4 interface-method prune, and the C5 wire-or-remove and C6 decisions. Each is made on the role facts — what the artifact is in the architecture — not on a reference count. Only C9 and C5's `RegisterCustomFunction` carry a declared external audience and therefore reach the compatibility notes.
4. **Class D cheap wins** — D5 (delete the parser copies), D2 (`BoundVariables()`), D6, D1, D3.
5. **D4** — the clause-walk framework, as its own designed change.
6. **E1** — decide the `Materialize()` convention and enforce or document it.
