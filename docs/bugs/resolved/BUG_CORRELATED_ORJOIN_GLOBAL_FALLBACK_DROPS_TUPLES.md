# BUG: Correlated or-join silently drops rows under the default optimizer

**Status**: Resolved (2026-07-19). Confirmed by reproduction at head `c6d11d0`; reported by external review against `4a43b5d`. Fixed by making correlated or/or-join round-trip through the algebra bridge as itself — see "The fix" below, which supersedes the key-split derivation this doc originally recorded. Pins: `TestCorrelatedOrJoinAllBranchesBindHeaderMatchesUnoptimizedExecution`, `TestCorrelatedOrJoinWithOutputsMatchesUnoptimizedExecution`, `TestCorrelatedOrWithOutputsMatchesUnoptimizedExecution` (`datalog/storage/correlated_not_join_test.go`, differential baseline == optimized), plus structural round-trip pins `TestRoundTrip_CorrelatedOrJoinPreservesClauseType` / `TestRoundTrip_CorrelatedOrPreservesClauseType` (`datalog/algebra/compile_test.go`). All were red before the fix.

## Symptom

A correlated `(or-join [?e] ...)` — branches containing correlated predicates (NOT, missing?), header variable bound by **every** branch — returns under-counted results, no error:

```clojure
[:find ?e
 :where [?e :x/tag _]
        (or-join [?e]
          (and [?e :x/tag _]
               (not [?e :x/flag true]))
          [?e :x/flag true])]
```

With three tagged entities of which one is flagged, the baseline (optimizer off) returns all 3; the optimized path returned 2 — the flagged entity vanished. Silent wrong results on the default path (`EnableAlgebraOptimizer` is true in `DefaultPlannerOptions`).

## Root cause — wider than the reported symptom

The review attributed the drop to `4a43b5d` reusing `ScopeOf`'s scheduling split as execution-correlation keys: `compileOrJoin`'s correlated route derived required/outputs from `query.ScopeOf(oj)`, a header var bound by every branch classified as Provides with empty Correlates, and the emitted `OrDefaultJoinClause{RequiredVars: []}` collapsed the per-tuple exclusive union into a single global fallback. That mechanism is real, and `4a43b5d` did make the filter shape worse — but it is the symptom of a deeper defect that predates it:

**The fallback-exclusive encoding is not a valid lowering for or/or-join at all.** Or-join is union semantics; the fallback machinery (`compileOrFallbackExclusive` → `LateralUnion` → `OrDefaultJoinClause`) is short-circuit semantics — per group, the first non-empty branch wins. The two coincide only in the pure-filter case (every header var outer-bound, no outputs), which is why the filter shape was the case that surfaced. With outputs — a header var the outer does not bind, branches overlapping per group — the exclusive encoding drops every later branch's rows for any group an earlier branch matched. The outputs-shape differential pins reproduce exactly that at head: or-join baseline `(e1 1) (e1 2) (e2 3)` vs. optimized `(e1 1)`; plain-or baseline 3 rows vs. optimized 2 (keeps the entity branch 1 didn't match, drops branch 2's row for the entity it did). The originally derived fix — `RequiredVars = JoinVars ∩ symbolsOf(current)`, outputs the remainder — would have repaired only the filter shape's correlation keys and left the outputs-shape divergence intact.

A secondary defect the derived fix exposed: its emission shape (`RequiredVars` non-empty, `OutputVars` empty) has no legal surface syntax — `Validate` rejects zero-output or-default-join by ratified language rule. An IR clause whose rendering does not parse back violates express-as-Datalog (phases are Datalog query fragments; the IR must not be larger than the language). The inexpressibility was the diagnosis, not an obstacle: the shape it wanted to emit already has a surface form — the or-join itself.

## The fix

Correlated or/or-join has no semantics-preserving rewrite through the fallback machinery, so it round-trips through the algebra bridge as itself. `compileOr`/`compileOrJoin`'s correlated routes call `compileOrUnionCorrelated` (`datalog/algebra/compile.go`): branches compile against the existing schema placeholder (so NOT/missing? see the outer symbols without embedding outer scans, same as the fallback route), and the node is a plain `Union` whose interface comes from the clause's canonical scope — `Output` from `ScopeOf` Provides plus the header, `JoinVars` the declared header verbatim (nil for plain or), `Required` the correlates. Never inferred from the placeholder-inflated children, which would leak outer symbols into the header. `decompileUnion` already emits `OrJoinClause`/`OrClause`, so decompile is identity and execution uses the executor's or/or-join path — the proven-correct baseline path. The decorrelation pass already skips inside Union branches (the 2026-03-21 semantics rule), which is exactly the treatment correlated branches need.

`OrDefaultJoinClause.Validate` is untouched at every level; the fallback machinery remains the lowering for or-default/or-default-join only, where short-circuit is the semantics.

Same conclusion as the Union-branch decorrelation bug: when a rewrite's structural preconditions are not met, the correct transform is no transform.
