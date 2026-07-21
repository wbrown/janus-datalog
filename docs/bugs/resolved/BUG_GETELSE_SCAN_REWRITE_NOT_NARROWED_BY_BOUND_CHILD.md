# BUG: get-else Scan Rewrite Full-Scans the Attribute When the Child Is a Bound Single Entity

**Date**: 2026-06-23 **Severity**: High (performance) — a query that should be a point lookup degrades to a full attribute scan, so its cost scales with the attribute's extent instead of being constant. Correct results, pathological latency on large attributes. **Status**: Resolved (2026-06-23) — Direction 1 implemented; see "Resolution" at the end. **Affected**: `get-else` on an entity that is bound to one (or few) values upstream — i.e. an `:in` input that is also referenced by a pattern. Default planner (`EnableAlgebraOptimizer: true`).

## Summary

`GetElseScanRewritePass` (`datalog/algebra/rewrite_getelse.go`) rewrites `Map(get-else)` into `LeftOuterJoin(child, Scan([?entity :attr ?result]))`. Its premise (stated in the pass comment) is "Replace N per-tuple point lookups with 1 index scan + hash join" — a real win when the child relation has many tuples.

But the produced `Scan` enumerates the **entire `:attr` extent** and is only narrowed afterward by the join on `?entity`. When the child relation is a **single bound entity**, this replaces *one* point lookup with a full scan of every datom for that attribute in the database, then a 1×N hash join — strictly, catastrophically worse. The "N point lookups" the rewrite is amortizing is N=1.

The existing skip-guard (rewrite_getelse.go:67–71) only covers the case where the entity variable is a *pure* `:in` input not referenced by any pattern. The moment the bound entity is also named by a pattern (the common shape — bind one entity, then read one of its optional fields), the guard passes, the rewrite fires, and the scan happens. The guard checks **provenance, not cardinality**.

## Reproducer

Any database with a cardinality-one optional attribute `:repro/note` and a separate referenced attribute `:repro/kind`. Bind one entity (via `:in` or a pattern), require it with one pattern, then read the optional field via `get-else`:

```clojure
;; Triggers the scan: ?e is bound and referenced by [?e :repro/kind _], and the
;; optional field is read via get-else. The rewrite fires and the produced
;; Scan([?e :repro/note ?note]) enumerates the whole :repro/note extent before
;; the join on ?e narrows it.
[:find ?note :in $ ?e :where [?e :repro/kind _] [(get-else $ ?e :repro/note "none") ?note]]
```

Compare against the plain pattern on the same bound `?e`, which is a point lookup:

```clojure
;; Point lookup: ?e is bound by [?e :repro/kind _], so [?e :repro/note ?note] is
;; an EATV lookup (or a fused per-entity LookupAttribute) — no attribute scan.
[:find ?note :in $ ?e :where [?e :repro/kind _] [?e :repro/note ?note]]
```

`?e` is referenced by `[?e :repro/kind _]`, so it is "provided by the child relation" — the skip-guard at rewrite_getelse.go:69 does not fire and the rewrite proceeds. The slowdown scales with the size of the `:repro/note` extent, not with the (singleton) result: with N datoms carrying `:repro/note`, the bound get-else reads all N instead of the one belonging to `?e`.

The in-repo reproduction `TestGetElseBoundEntityScanNotNarrowed` (`datalog/storage/getelse_bound_scan_repro_test.go`) builds a 3000-datom `:repro/note` extent with a single matching entity and asserts the bug via the chosen index and the narrowing annotation (see Verification below).

## Expected Behavior

`get-else` on a bound single entity should be a point lookup — equivalent in cost to the plain `[?e :attr ?v]` pattern, returning the default only when the datom is absent. The annotation cost should be sub-millisecond, dominated by neither `join/hash` nor `collapse`.

## Actual Behavior

The rewrite's `Scan([?e :attr ?result])` is evaluated as a full attribute scan; the `LeftOuterJoin` materializes it (deferred until the join probes the streaming relation), so the entire cost shows up as `join/hash` / `collapse` rather than as a `pattern/storage-scan`. An annotation summary for the `get-else` side, illustrating the deferred-materialization signature:

```
Wall time: 1.093s
  query                   1.09s   12   860.7ms query/completed
  join                  861.0ms   43   860.6ms join/hash
  collapse              860.9ms    5   860.6ms collapse/success
  matches->relations    231.3ms   37
```

The wall time tracks the `:attr` extent: roughly constant in the number of bound entities (one) and linear in the number of datoms carrying `:attr`. (This is the same "the join/hash time is really deferred scan materialization" signature documented in `resolved/slow-single-pattern-input-query.md`.)

## Root Cause Analysis

`getElseScanRewriteTransform` (rewrite_getelse.go:24) builds, for `[(get-else $ ?e :attr default) ?out]`:

```go
scanPattern := &query.DataPattern{Elements: []query.PatternElement{
    query.Variable{Name: entityVar.Symbol},   // ?e
    query.Constant{Value: ge.Attr},            // :attr
    query.Variable{Name: bindingSym},          // ?out
}}
// ... LeftOuterJoin(child, Scan(scanPattern)) on JoinSymbols=[?e]
```

The `Scan` has the entity in E-position as a **variable**, not narrowed to the child's bound value(s). At execution the LeftOuterJoin drives the scan over the full `:attr` extent and filters by `?e` afterward. For a singleton child that is an EAVT point lookup expressed as a whole-attribute scan + 1×N join.

The only gate on the rewrite is provenance, not cardinality:

```go
// rewrite_getelse.go:67-71
// Skip rewrite if entity variable isn't provided by the child relation
// (e.g., it's an input parameter from :in, not a pattern variable).
if !containsSymbol(childNode.Symbols(), entityVar.Symbol) {
    return rebuildWithChildren(node, children)
}
```

When `?e` is *both* an `:in` input and a pattern variable, `childNode.Symbols()` contains `?e`, the guard passes, and the rewrite fires — even though `?e` is bound to a single value, so the child relation is a singleton.

The planner has no cardinality model to fall back on either: `Statistics.AttributeCardinality` (`datalog/planner/types.go`) is allocated empty and never populated. But for this case it isn't needed — the relevant cardinality (the child relation's, here 1) is determined by the bound `:in` input, which is known at plan time.

`GetElseScanRewritePass()` is in `DefaultPasses()` (`datalog/algebra/optimize.go:83`) and `EnableAlgebraOptimizer` defaults to `true`, so the rewrite is on for all default-configured queries.

## Why It Matters

This hits the most ordinary access pattern there is: "I have an entity, give me one of its optional fields, defaulting if absent." `get-else` exists precisely so that absence yields a default instead of dropping the row. Every such call on a bound entity pays the full-attribute-scan cost, and it grows with the database. Callers are pushed to hand-roll the fallback (split into a presence pattern + a separate optional read) to dodge the rewrite — reimplementing `get-else` by hand.

## Proposed Fix

Two directions, not mutually exclusive:

1. **Narrow the produced `Scan` by the child's join-symbol values (preferred).** This is the same fix shape as `resolved/slow-single-pattern-input-query.md`: a scan over an attribute with a bound value in another position should compute a precise index range (here, EAVT by `?e`) instead of scanning the full attribute. If the LeftOuterJoin pushed the child's `?e` values into the scan as a bound-narrowed range (per-entity EAVT lookup), the rewrite would be correct *and* fast for **any** child cardinality — the "1 scan vs N lookups" premise dissolves because the scan is always bounded by the child. This keeps the rewrite universally applicable.

2. **Gate the rewrite on child cardinality.** Skip the scan rewrite (keep per-tuple `get-else`) when the child relation is known-small — in particular when the entity variable traces to a bound `:in` scalar (singleton). This is a narrower, simpler stopgap; it requires the pass (or a later stage) to see the input-binding cardinality, which the current provenance-only guard does not consult.

Direction 1 is the more complete fix; direction 2 protects the singleton case without touching scan narrowing.

## Verification

Run `TestGetElseBoundEntityScanNotNarrowed` (`datalog/storage/getelse_bound_scan_repro_test.go`): the fix is confirmed when the bound get-else stops choosing an attribute-primary index for `:repro/note` (no `pattern/index-selection` for it) and the `or-fallback/branch.narrowed` annotation reports `narrowed=true`. The test is retained as a regression guard against the scan creeping back.

## Related

- `resolved/slow-single-pattern-input-query.md` — same "should be a point lookup, became a full scan; cost shows up as deferred `join/hash` materialization" signature; root cause there was a missing AVET case in `chooseIndexForValues()` (scan not narrowed to bound values). Direction 1 above is the analogue for the get-else scan.
- `resolved/slow-query-multi-position-binding.md` — `NoReuse` strategy not narrowing multi-position bindings.
- `resolved/BUG-ALGEBRA-BRIDGE-GETELSE-CARTESIAN-PRODUCT.md` — adjacent get-else rewrite interaction.
- `resolved/BUG_DECORRELATION_REORDERS_WHERE_CLAUSES.md` — algebra pass producing a worse plan than the input.

## Files Involved

- `datalog/algebra/rewrite_getelse.go` — the rewrite and its provenance-only skip-guard.
- `datalog/algebra/optimize.go` — `DefaultPasses()` includes the rewrite (on by default).
- `datalog/algebra/decompile.go` — `decompileLeftOuterJoin` emits the `OrDefaultJoinClause` the rewrite becomes.
- `datalog/executor/relations.go` / `join.go` — where the LeftOuterJoin materializes the streaming scan (the observed `join/hash` cost).

---

## Verification (2026-06-23)

The bug was confirmed by reading the execution path end-to-end and by an in-repo reproduction. Both are recorded here.

### Execution path trace (confirms the mechanism)

1. **Rewrite** (`rewrite_getelse.go:73-106`): `[(get-else $ ?e :attr d) ?out]` becomes `LeftOuterJoin(child, Scan([?e :attr ?out]))` with `JoinSymbols=[?e]`. The scan has `?e` as a **variable** in E-position. The only skip-guard (`rewrite_getelse.go:69`) checks `containsSymbol(childNode.Symbols(), ?e)` — pure **provenance**. When `?e` is both an `:in` input and a pattern variable, the guard passes and the rewrite fires.
2. **Decompile** (`decompile.go:133-217`): the LeftOuterJoin becomes `OrDefaultJoinClause{JoinVars:[?e], Branches:[ [Scan([?e :attr ?out])], [get-else default] ]}`.
3. **Execution** (`query_executor.go:1462` → `or_fallback_relation.go`): `executeOrDefaultJoinClause` builds an `OrFallbackRelation` with `shortCircuit=true`, `joinSyms=[?e]`, and **`prefetched=false` hardcoded** (`query_executor.go:1471`). In `nextShortCircuit` (`or_fallback_relation.go:760-844`), branch 0 is a single DataPattern, so with `isOrJoin=true`, `isCacheableBranch` returns true → **`execInput = nil`** (line 782). The branch then runs `executeInnerClauses([?e :attr ?out], nil)`.
4. **The unbound collapse** (`matcher_relations.go:45-47`): `Match` with `bindings == nil` calls `matchUnboundAsRelation` → `chooseIndex(e=nil, a=:attr, v=nil)` → for a CardinalityOne attribute this is **AETV**, an A-only prefix range = a scan of the entire attribute extent (`matcher.go:560-577`). The result is materialized once into a hash cache keyed by `?e` and probed for the singleton outer tuple. The EA-cache fast path (`buildBranchFromEACache`, `or_fallback_relation.go:791`) that would avoid this is gated on `it.prefetched`, which is false here; the top-level prefetch (`query_executor.go:105-125`) only fires after the first pattern and only when `len(entities) > 50`, so a singleton never warms it.

The probe is fine; only the **scan** is unbounded.

### Matcher narrowing is already available (this is why Direction 1 works)

The same `BadgerMatcher.Match` path narrows correctly **when bindings are supplied**:

- `bindings == nil` → `matchUnboundAsRelation` → AETV full extent scan (`matcher_relations.go:45-47`). ← the bug.
- `bindings` supplying `?e` → `FindBestForPattern` → `matchWithBindingsFromCache` per-E cache lookup (`matcher_relations.go:107-124`) or `analyzeReuseStrategy` → EATV iterator-reuse / batch-scan (`matcher_relations.go:144-231`). No full scan.

So the bug is literally `execInput = nil` collapsing into the unbound path. The fix is to give the once-evaluated branch the outer relation's join keys as bindings, so it takes the same narrowed path an equivalent bound pattern takes.

### In-repo reproduction

`datalog/storage/getelse_bound_scan_repro_test.go` — `TestGetElseBoundEntityScanNotNarrowed`. Setup: 3000 entities each carrying the optional `:repro/note` field (inflating that attribute's extent); only the target entity also carries the anchor `:repro/kind`, so the child relation `[?e :repro/kind _]` is a singleton. It runs two queries reading the same optional field on the same bound `?e` — a plain pattern vs. `get-else` — and asserts on the **index chosen** for the `:repro/note` access (timing-independent; wall-clock is logged only for the perf framing).

Result (bug reproduced — test fails by design):

```
plain    [?e :repro/note ?note]:        index=        wall=712µs    results=1
get-else (get-else $ ?e :repro/note):   index=AETV    wall=2.02ms   results=1
:repro/note extent=3000 datoms; get-else scan range [ ...A-prefix... !!!, ...A-prefix... $ )
get-else / plain wall-time ratio: 2.8x
BUG REPRODUCED: get-else scanned :repro/note via attribute-primary index AETV —
the full extent of 3000 datoms — for a single bound entity; the equivalent plain
pattern used  (point lookup).
```

Two observations from the output:

- **The plain pattern emits no `:repro/note` scan at all** (empty index). On a bound `?e` it is served by the executor's `tryFuseAttributeFetch` (`query_executor.go:75,419`) — a per-entity, cache-backed `LookupAttribute`, i.e. a pure point lookup. That (or an EATV-narrowed scan) is the cost target.
- **`get-else` picks `AETV`** and the logged scan range confirms a full-extent scan: `start` and `end` share the entire `:repro/note` attribute prefix and differ only in the final terminator byte (`!` → `$`), i.e. an A-only prefix range over the whole attribute, not an E-narrowed range.

The 2.8× wall-time gap is modest here only because everything is in-memory and cached and the extent is just 3000; the **mechanism** (full AETV scan vs. point lookup) is the proof, and it scales with the attribute extent and disk I/O — so on a large, on-disk attribute the same signature reaches thousands-fold. After Direction 1 the `:repro/note` access goes through the bound path (cache lookup → no `pattern/index-selection` event, or EATV) and this test flips green, becoming the regression guard.

---

## Direction 1 — Implementation Plan (proposed, pending approval)

Narrow the produced scan by the child's join-key values at execution time. This keeps the rewrite universally applicable and is correct *and* fast for **any** child cardinality — the "1 scan vs N lookups" premise dissolves because the scan is always bounded by the child.

### The change — single site

`or_fallback_relation.go`, `nextShortCircuit`, the cacheable block. Instead of `execInput = nil`, pass a once-computed, deduplicated projection of the (materialized) outer relation onto the join symbols. The branch still runs **once**, gets cached, and is probed per tuple exactly as today — but the scan is now bounded to the outer entities.

Current:

```go
execInput := inputRel
isOrJoin := len(it.joinSyms) > 0
isCacheable := isCacheableBranch(branch, isOrJoin)
if isCacheable {
    execInput = nil          // ← collapses to the unbound full-scan path
}
...
if branchResult != nil {
    if execInput == nil {    // ← cache-build trigger keyed off the nil proxy
        // buildCachedBranch + probe
    } else {
        // filterBranchToOuterTuple (per-tuple)
    }
}
```

Proposed:

```go
execInput := inputRel
isOrJoin := len(it.joinSyms) > 0
isCacheable := isCacheableBranch(branch, isOrJoin)
if isCacheable {
    execInput = it.outerJoinKeys()   // narrowed bindings; nil on guard-fail → safe fallback
}
...
if branchResult != nil {
    if isCacheable || execInput == nil {   // cache-build: intent, not the nil proxy
        // buildCachedBranch + probe  (UNCHANGED)
    } else {
        // filterBranchToOuterTuple  (UNCHANGED)
    }
}
```

The condition flips from `execInput == nil` to `isCacheable || execInput == nil`. This is required because `execInput` is no longer `nil` in the cacheable case, but it also faithfully **preserves the two existing edge paths**:

- *Unit outer relation* (`len(outerSyms)==0`, so `inputRel==nil`) with a non-cacheable branch → still cache-build → `buildCachedBranch` returns nil (no shared syms) → pass-through. Unchanged.
- *Correlated, non-cacheable branch* (`execInput != nil`) → per-tuple `filterBranchToOuterTuple`. Unchanged.

### New method + iterator fields

```go
// outerJoinKeys returns the outer relation projected onto the or-join's join
// symbols (deduplicated), memoized. These bindings narrow the once-evaluated,
// cached branch's scan to the entities actually present in the outer relation,
// instead of scanning the whole attribute extent. Returns nil when narrowing
// can't be applied safely (no outer relation, no join symbols, or a
// non-re-iterable streaming outer), in which case the caller falls back to the
// existing unbound scan.
func (it *OrFallbackIterator) outerJoinKeys() Relation { ... }
```

Fields on `OrFallbackIterator`: `joinKeyRel Relation`, `joinKeysComputed bool`. Guards inside return `nil` on: `outerRel == nil`, `len(joinSyms) == 0`, `outerRel` is `*StreamingRelation`, or `Project(joinSyms)` errors. `it.outerRel` is already materialized by `findOuterRelationBySymbols`, and `buildBranchFromEACache` already re-iterates it, so the double-iteration (project + outer loop) is an established pattern.

### Why results don't change

The cache built from a narrowed branch contains exactly the `(entity, value)` pairs the probe will ever look up (probes only ever use outer entities' join keys). The full-scan cache merely contains *extra, never-probed* rows. So narrowing removes only dead entries — cache content for any reachable probe is identical, and the default-branch fallback for absent entities is unchanged.

### Scope (what is and isn't touched)

- **Only** `shortCircuit` mode (or-default / or-default-join) with cacheable DataPattern branches in an or-join — i.e. exactly what the get-else rewrite produces. `or-default` without join vars → `isOrJoin=false` → not cacheable → untouched.
- `nextCorrelatedUnion` (regular `or-join`, `shortCircuit=false`) already executes per-outer-tuple narrowed and has no full-scan-cache path — **not modified**.
- The EA-cache fast path (`buildBranchFromEACache`, gated on `it.prefetched`, currently always false for or-default-join) stays dormant — **not modified**. It's a complementary CardinalityOne-specific optimization that can be revisited separately (wiring prefetch / removing the `it.prefetched` gate); scan-narrowing is the general fix that works for any cardinality.
- High-outer-cardinality behavior (the original "1 scan vs N lookups" tradeoff) is **inherited from the matcher's existing strategy selection** — `analyzeReuseStrategy` / batch-scan thresholds already decide hash-join vs. iterator-reuse vs. batch scan for bound patterns. The fix invents no new policy; it routes the branch through the same machinery an ordinary bound pattern uses.

### Test plan

1. **`TestGetElseBoundEntityScanNotNarrowed` flips green.** After the fix the `:repro/note` access goes through the bound path (cache lookup → no `pattern/index-selection`, or EATV) — never AETV. The existing assertion already captures this.
2. **Multi-entity get-else** (new test): bind several entities via a collection/relation input, run get-else, assert (a) correct values including defaults for entities missing the field, and (b) the `:note` access is not attribute-primary. Confirms the fix isn't singleton-only.
3. **Differential / semantic preservation** (new test): run the same get-else query with `EnableAlgebraOptimizer: true` (rewrite + narrowed scan) vs `false` (no rewrite, per-tuple get-else) and assert identical result sets. This is the structural optimization test the project guidance calls for.
4. **Existing get-else / or-default suites** must stay green: `TestGetElseBasic`, `TestGetElseNumericDefault`, `TestGetElseWithConstantEntity`, `TestGetElseWithAnnotationHandler`, `algebra_getelse_product_test.go`, `or_subquery_regression_test.go`, plus vector-default get-else paths.
5. **Full suite**: `GOWORK=off go test -count=1 ./...`. (A parent `go.work` at `~/go/src/github.com/wbrown/go.work` otherwise excludes this module from test runs; `GOWORK=off` scopes the run to this module.)

### Open choices

- **Dedup of join keys**: rely on `Project` to deduplicate; if it doesn't, redundant `?e` values only cause redundant (idempotent) cache lookups. An explicit dedup can be added if we want it tightened.
- **Observability**: optionally emit a small `or-fallback/branch-narrowed` annotation, or just let the existing `or-fallback/cache-build` `branch_size` reflect the narrowed count. Leaning toward not adding a new event unless wanted.

### Architectural note

This edits a shared execution component (`OrFallbackIterator`). The change is surgical and scoped as above. Per the repository's architectural-authority convention, implementation is pending the owner's approval of this shape.

### Comparison to Direction 2 (gate the rewrite on child cardinality)

Direction 2 (skip the rewrite when `?e` traces to a singleton `:in` scalar) is a narrower stopgap: the algebra pass only sees `childNode.Symbols()` (provenance), and `:in`-input cardinality is known at plan time (`PlanQueryWithBindings` receives `initialBindings`) but is not threaded into the algebra optimizer. Direction 2 also protects only the singleton — the "few entities" mid-range still full-scans the attribute. Direction 1 is preferred: one localized execution-layer change, no new cardinality model, correct across the whole cardinality spectrum, and it mirrors the already-resolved `slow-single-pattern-input-query.md` fix.

---

## Resolution (2026-06-23)

Direction 1 implemented in `datalog/executor/or_fallback_relation.go`. All changes are confined to `OrFallbackIterator` (the executor of the `OrDefaultJoinClause` the get-else rewrite decompiles to); the rewrite, decompiler, and planner are unchanged. `go test -count=1 ./...` green.

### What changed

1. **Narrow the cached branch's scan to the outer join keys.** In `nextShortCircuit`, a cacheable DataPattern-only or-join branch (the get-else `Scan([?e :attr ?out])`) was executed with `execInput = nil`, which made `BadgerMatcher.Match` take the unbound path (`matchUnboundAsRelation` → AETV full-extent scan). It is now executed once with `it.outerJoinKeys()` — the outer relation projected onto the join symbols, deduplicated — so the matcher takes its bound path (`matchWithBindingsFromCache` / EATV reuse). The branch is still cached and probed per outer tuple, so the many-outer-tuple case is unchanged; the singleton/few case becomes a point lookup. The cache-build trigger flipped from `execInput == nil` to `isCacheable || execInput == nil` to keep building the cache when `execInput` is the narrowed relation (the unit-relation and correlated paths are preserved).

2. **Materialize a streaming outer up front (the part not in the original plan).** The plan above assumed the outer relation was re-iterable ("`it.outerRel` is already materialized by `findOuterRelationBySymbols`"). It isn't always: when the bound entity comes from a *pattern* (not an `:in` input) the outer is a `*StreamingRelation`, iterable only once. `outerJoinKeys()` must iterate the outer (to extract keys) *and* the per-tuple loop iterates it again, so on a streaming outer `outerJoinKeys()` returned nil and the code **silently fell back to the full scan** — the singleton (`:in`, materialized) narrowed but "few entities from a pattern" did not. Fix: `OrFallbackRelation.Iterator()` now drains a non-materialized outer to a `MaterializedRelation` when a narrowable branch is present. OR-fallback iterates the whole outer per-tuple regardless, so this buffers the (driving) outer relation rather than adding work. A drain error is carried onto the iterator (`err`/`done`), not swallowed.

3. **`or-fallback/branch.narrowed` annotation (added per review).** The narrowing decision is now a first-class event — `{branch, narrowed: bool, join_keys: int, reason: string}` — instead of being inferred from the absence of a `pattern/index-selection` event. This makes a fall-back-to-full-scan *visible* (it would otherwise be silent — exactly how the streaming-outer gap in change 2 hid), gives production observability, and lets the regression tests assert narrowing directly.

4. **Predicate clarity.** Added `isDataPatternOnlyBranch(branch)` and `hasNarrowableBranch(branches)`; the per-branch decision now reads `isOrJoin && isDataPatternOnlyBranch(branch)` instead of the subtle double-call `isCacheable && isOrJoin && !isCacheableBranch(branch, false)`.

### Why results don't change

The narrowed branch cache holds exactly the `(entity, value)` pairs the per-tuple probe can ever look up (probes only use outer entities' join keys); the unbounded scan's cache merely held extra, never-probed rows. Join keys are deduplicated so the matcher can't emit duplicate `(entity, value)` rows for a repeated outer entity. Confirmed by the differential test below.

### Measured

On the `TestGetElseBoundEntityScanNotNarrowed` setup (3000-datom `:repro/note` extent, one bound entity): before, get-else chose AETV and scanned the full extent (~2 ms in-memory); after, get-else issues no attribute scan at all (bound cache path) and is a point lookup (~0.3 ms, on par with / faster than the plain pattern). The gap grows with the attribute extent and on-disk I/O, so the same signature reaches thousands-fold on a large attribute.

### Tests (all green, `datalog/storage/getelse_bound_scan_repro_test.go`)

- `TestGetElseBoundEntityScanNotNarrowed` — the original reproduction; now asserts the `:repro/note` access is not attribute-primary **and** the `or-fallback/branch.narrowed` annotation reports `narrowed=true, join_keys=1`.
- `TestGetElseMultiEntityScanNarrowed` — five pattern-bound entities (some with the field, some defaulting) against a 1500-datom filler extent; asserts correct per-entity values/defaults **and** `narrowed=true, join_keys=5` (the regression guard for the streaming-outer case from change 2).
- `TestGetElseScanNarrowing_SemanticPreservation` — differential: identical `(?e, ?note)` results with the algebra optimizer ON (rewrite + narrowing) and OFF (un-rewritten per-tuple get-else).

### Files changed

- `datalog/executor/or_fallback_relation.go` — `Iterator()` (materialize outer), `nextShortCircuit` (narrowed `execInput`, cache-build condition, annotation), `outerJoinKeys()`, `isDataPatternOnlyBranch()`, `hasNarrowableBranch()`, and the `joinKeyRel`/`joinKeysComputed` iterator fields.
- `datalog/storage/getelse_bound_scan_repro_test.go` — the three tests above.
