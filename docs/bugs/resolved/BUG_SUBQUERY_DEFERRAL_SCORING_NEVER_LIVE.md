# BUG: Subquery deferral scoring has never functioned

**Status**: Resolved (2026-07-19). `scoreClause` defers **correlated** subqueries (any variable among the inputs) at **−1000**; uncorrelated subqueries keep data-source scoring. Pinned by `TestCorrelatedSubqueryDefersBehindSimultaneouslyReadySiblings` and `TestUncorrelatedSubqueryKeepsDataSourceScheduling` (planner) and measured by `BenchmarkSubqueryDeferralScheduling` (executor).

**Magnitude derivation (owner-directed, empirical + arithmetic):**
- The existing benchmark corpus (correlated-subquery pattern, OHLC, complex-query checkpoint, decorrelation) is plan-neutral under every candidate — its subqueries are dependency-ordered, so scoring never decides. Greedy scoring only matters for *simultaneously-ready* tie-breaks.
- The constructed tie-break shape (pattern binds `?e`; a NOT eliminating 90% and a correlated per-`?e` aggregation become ready together): no deferral ~59.0 ms / 107.7 MB / 1,396,774 allocs per op; with deferral ~7.3 ms / 12.2 MB / 163,373 allocs — **8.1× time, 8.5× allocations**.
- **Why −1000 and not the original −50:** the deferral must dominate the provides bonus (+10 per binding variable) at any arity — additive −50 flips back above a NOT clause (+2) at six binding variables. −1000 is the scorer's existing dominance constant (ready predicates score +1000). Flat −50, flat −1000, and correlated-only −1000 are indistinguishable on the benchmark (identical allocation fingerprints); the choice between them is arithmetic and cost-model, not measurement.
- **Why correlated-only:** an uncorrelated subquery executes exactly once wherever it is placed — deferring it cannot reduce its cost and only withholds its bindings from earlier joins.

## Finding

`scoreClause` (planner/clause_utils.go) carried a deferral arm — "Subqueries are expensive - defer if possible", `score -= 50` — that matched only `*query.Subquery`, a clause type with **no producer in the repository's entire public history** (`git log -S '&query.Subquery{'` is empty; the parser has only ever constructed `*query.SubqueryPattern`). The arm was present from the initial public release (bb73fc8) until deleted as dead code with the `Subquery` type (fe7d8be). The live `*SubqueryPattern` type has never had a deferral arm.

## Live consequence

`selectPhaseClauses` (planner/clause_phasing.go) runs every clause, subqueries included, through `scoreClause` in its greedy selection loop — there is no separate subquery scheduling. A `SubqueryPattern` therefore scores `+10 × |binding vars|` (the generic provides bonus): a one-variable subquery ties a one-output expression and schedules **ahead of** NOT clauses (+2) and non-ready clauses (+5), where the stated design intent was strictly last (−50). Correlated subqueries execute once per input combination, so scheduling one before the filters that would narrow its input multiplies whole nested-query executions — the known per-combination cost class (see SUBQUERY_PERFORMANCE_ANALYSIS). Semantics are unaffected; this is scheduling/performance.

## Why "fix" is not "restore"

Because the arm was born dead, every benchmark and every observed plan shape in this codebase's public history was produced **without** subquery deferral. Adding `case *query.SubqueryPattern: score -= 50` implements the stated intent but is a new behavior whose magnitude and interactions were never validated. The fix therefore needs: a structure test pinning "a subquery schedules after simultaneously-executable siblings" (red first), a full-suite run treating any phase-structure test breakage as information, and benchmark comparison on the subquery benchmarks before/after.

## Discovery provenance

Surfaced while deleting the dead `Subquery` type; the deletion changed nothing (the arm never matched a live value) but exposed that the live type lacked the intent.
