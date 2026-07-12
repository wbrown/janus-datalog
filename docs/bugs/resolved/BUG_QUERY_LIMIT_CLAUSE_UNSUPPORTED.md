# BUG: No Top-Level `:limit` Clause — Top-N / Latest Queries Must Over-Fetch

**Date**: 2026-06-29
**Severity**: Low–Medium — feature gap, not wrong results or a crash. Queries that want "the latest row" or "top N" parse-error if they try `:limit`, forcing callers to fetch the entire ordered result set and slice in Go.
**Status**: RESOLVED (2026-06-30) — top-level `:limit` implemented with streaming early-termination per the plan below. `:offset` intentionally not provided. Index-order pushdown remains a documented roadmap item (not yet implemented).
**Affected**: top-level query result limiting. `:order-by` (asc/desc) is supported; capping the number of returned rows is not.

## Summary

A query cannot ask for a bounded number of result rows. There is no `:limit`
clause in the grammar: the `Query` struct has no `Limit` field, and the query
parser rejects `:limit` as an unknown clause. `:order-by` exists and supports
both `:asc` and `:desc`, so the ordering half of "give me the most recent row"
is available — but without a row cap, the only way to get the latest (or top N)
is to run the ordered query, materialize *all* matching rows, and discard all
but the tail in application code.

This is the natural pairing the grammar is missing: `:order-by [[?tx :desc]]`
plus `:limit 1` is the canonical "current value / latest record" query, and
`:order-by [[?score :desc]]` plus `:limit 10` is "top 10". Today neither can be
expressed.

## Reproducer

Any query carrying `:limit` fails at parse time:

```clojure
[:find ?e ?tx
 :where [?e :entity/type :entity.type/telemetry ?tx]
        [?e :event/crawl ?crawl]
 :order-by [[?tx :desc]]
 :limit 1]
```

```
EDN parse error: unknown query clause: :limit
```

(The error comes from `parseQueryVector`'s `default` arm — see below.)

## Expected Behavior

A `:limit N` clause caps the number of result rows to the first `N` after
`:order-by` is applied (and after aggregation, if any). Composed with
`:order-by … :desc`, `:limit 1` returns the single latest row in one round trip,
without transferring or allocating the rest of the result set.

A natural extension (not required for the core feature) is `:offset M` for
pagination, but `:limit` alone covers the latest/top-N cases that motivate this.

## Actual Behavior

`datalog/parser/parser.go::parseQueryVector` (the clause switch at lines 44–127)
handles exactly `:find`, `:in`, `:where`, and `:order-by`. Any other top-level
keyword hits the `default` arm at line 126:

```go
default:
    return nil, fmt.Errorf("unknown query clause: %s", keyword)
```

so `:limit` is a hard parse error. Correspondingly, `query.Query`
(`datalog/query/types.go`) has no `Limit` field — only `Find`, `In`, `Where`,
`OrderBy`, `ScalarReturn`.

## Not To Be Confused With Pull `:limit`

The query package already has `PullLimitExpr` / `ResolvedPullLimitExpr`. That is
a **pull-pattern** limit — it caps how many values of a cardinality-many
attribute a `pull` returns (e.g. `[(:some/attr :limit 5)]`), operating *within*
one entity's attribute. It does **not** limit the number of result *rows* of the
enclosing query, and cannot serve the top-N / latest-row case. This feature is a
distinct, top-level clause.

## Motivating Use Case

Consumers that want "the current/latest record of kind K" have no choice but to
load the full per-key history and take the last element. For example, a
telemetry reader that records one snapshot row per turn and then wants the most
recent one ends up loading every row for the entity/crawl just to return
`rows[len(rows)-1]` — an unbounded read that grows linearly with history length,
on what should be an O(1) point read. The documented workaround is the
`(max ?tx)` subquery idiom (find the max transaction, then match on it), which
works for "latest single" but is more ceremony than `:order-by :desc :limit 1`
and does not generalize to top-N.

## Where To Investigate

1. **Grammar/AST** — add a `Limit *int` (and optionally `Offset *int`) field to
   `query.Query` in `datalog/query/types.go`; reflect it in `Query.String()` so
   it round-trips through the parser.
2. **Parser** — add a `:limit` case to the `parseQueryVector` switch
   (`datalog/parser/parser.go`, alongside `:order-by` at line 104) that reads a
   single non-negative integer node; reject negatives and non-integers loudly.
3. **Executor** — apply the cap after ordering (and after aggregation) in the
   result-assembly path, so it composes with `:order-by`. Truncating the
   ordered relation before materializing the tail is where the actual savings
   come from; a limit applied only after full materialization fixes correctness
   but not the over-fetch cost.
4. **Scalar interaction** — define behavior with `ScalarReturn` (`:find … .`):
   `:limit 1` + scalar should be coherent; `:limit N>1` + scalar should error or
   be disallowed.

---

## Implementation Plan (2026-06-30)

Decisions recorded after a design discussion. Scope: ship a top-level `:limit`
with streaming early-termination; document index-order pushdown as a roadmap
item; **deliberately do not** add `:offset`.

### Decision register

| Decision | Resolution |
|----------|------------|
| `:limit N` | Implement now. Caps result rows **after** `:order-by` and **after** aggregation. |
| Streaming early-termination | When there is **no** `:order-by`, stop pulling from the underlying iterator after N tuples (real scan + transfer savings). When `:order-by` **is** present, the sort already materializes the whole set, so the limit truncates post-sort (savings on transfer/tail only — *not* the scan). Closing that gap for the ordered case is the index-pushdown roadmap item below. |
| `:offset M` | **Deliberately NOT provided.** Rationale below. |
| Scalar `:find … .` | `:limit 0` and `:limit 1` allowed; `:limit N>1` combined with `.` is a **parse error** (it contradicts "return a single value"). |
| `:limit` inside a subquery | **Parse error** (interim). Subqueries execute via `DefaultQueryExecutor.Execute`, which applies neither `:order-by` nor `:limit` (both are top-level finalization steps in `ExecuteRealized`), so a subquery cap would be *silently ignored* — wrong results. Rejecting at parse prevents that. Per-invocation support is a follow-up (see below). |

### Subquery `:limit` — rejected at parse (interim), per-invocation support deferred

Discovered during edge-case testing: a subquery carrying `:limit` had its cap
**silently dropped** (a correlated subquery with `:limit 2` returned all rows,
not 2 per invocation). Root cause: subqueries run their inner query through
`DefaultQueryExecutor.Execute(subq.Query, inputs)` (`executor/query_executor.go`),
which runs clauses + find-projection but performs no finalization. `:order-by`
and `:limit` are applied only at the top-level `ExecuteRealized` boundary, which
subqueries bypass. (The same path means `:order-by` inside a subquery is *also*
silently ignored — pre-existing, untouched by this work.)

To avoid silent wrong results, `:limit` inside a subquery is now a **parse
error**. Proper per-invocation support is deferred because it is an architectural
change with three coupled parts:

1. **Per-invocation finalization** — apply `:order-by` + `:limit` to each
   subquery invocation's result (so the cap is per correlation group, e.g.
   top-1-per-group).
2. **Disable batching when a cap is present** — `canBatchSubquery` runs the
   subquery once with all correlation tuples as a `RelationInput`; a per-invocation
   cap cannot be expressed in that single batched execution.
3. **Guard decorrelation** — with the algebra optimizer on, a correlated subquery
   is rewritten into a join, which drops the cap (the rewrite is unsafe under a
   row limit — the same class as the documented "decorrelation inside Union"
   guard). Skip decorrelation when the inner query has a `:limit`.

The forward-compatible tests (`executor/limit_edge_test.go`) accept the parse
rejection today and assert per-invocation correctness automatically once the
rejection is removed — so implementing the above and deleting the parser check is
all that is needed to land support.

`:order-by` is **optional** with `:limit` — they are independent clauses. `:limit
N` with no `:order-by` is valid and is the streaming-early-termination case (stop
the scan after N); the returned rows are simply the first N the engine produces,
which is *arbitrary but valid* (no ordering guarantee, same as SQL `LIMIT` without
`ORDER BY`). "after `:order-by`" in the table means *operation order when an
`:order-by` is present* (cap applied after the sort), not a requirement that one
be present. Add `:order-by … :desc` only when you need the *latest*/top-N
specifically rather than *any* N.

There is **no Datomic precedent** for any of this: Datomic's datalog has no
top-level `:limit`/`:offset` and no `:order-by` at all — its only `:limit` is the
pull-pattern cap (the "Not To Be Confused With" case above), and result sorting
and pagination are expected to happen in caller code (`take`/`drop`). janus is
already ahead by having `:order-by`; a top-level `:limit` and the scalar rule
above are janus conventions, not inherited ones.
(Sources: [Datomic Query Reference](https://docs.datomic.com/query/query-data-reference.html),
[Datomic pagination thread](https://datomic.narkive.com/ehWHxXQZ/pagination).)

### Why `:offset` is deliberately omitted — pagination is an anti-pattern in a streaming engine

A query result is a streaming `Relation`: the caller drives it through
`rel.Iterator()` / `it.Next()` and pulls exactly as many tuples as it wants.
"The next page" is just **keep pulling from the same iterator**. That iterator
reads from a single consistent storage snapshot for its lifetime, so an
in-process consumer that pauses and resumes gets stable, gap-free, duplicate-free
paging *for free* — no `:offset`, no AsOf pinning, no re-scan.

`:offset M` would be strictly worse on every axis:

- **Cost** — it re-runs the query and *discards* the first M rows every page
  (O(M+N) per page) instead of O(N).
- **Stability** — across a mutating database it is unstable: an assert/retract
  between page fetches shifts the window, causing skipped or duplicated rows.
  Making it stable requires pinning a snapshot (`d.AsOf(tx)`) **and** a total
  order (`:order-by` plus a tie-breaker) — at which point offset is doing nothing
  the snapshot + order don't already give you.

Offset/limit pagination exists in other systems only to serve **stateless,
cross-process** consumers (e.g. an HTTP endpoint) that cannot hold an iterator
open between requests. Even there, the correct serializable encoding of "where I
paused" is **keyset / cursor pagination**, not offset: carry the last-seen sort
key forward as a `:where` bound. This is expressible **today** and is naturally
snapshot-stable under `d.AsOf`:

```clojure
;; page 2+: pin a snapshot via d.AsOf(tx), continue past the last key seen
[:find ?e ?tx
 :in $ ?last-tx
 :where [?e :entity/type :entity.type/telemetry ?tx]
        [(> ?tx ?last-tx)]
 :order-by [[?tx :asc]]
 :limit 100]
```

This is O(N) per page (no discard), uses the index directly, and is exactly the
shape the index-order pushdown roadmap item below turns into a range-scan-then-stop.

**What `:limit` is for (and why it still earns its place):** not pagination, but
*bounding* — "at most N", top-N, latest-1. Crucially, `:limit` is **declarative
and pushdownable**: because the bound lives in the query, the planner/storage can
stop the scan at the source. A consumer pausing its own iterator cannot achieve
that — by the time it pauses, the query has already committed to a plan that may
have over-produced (an unordered scan yields *a* tuple, not the *latest*; only
the engine, knowing `:order-by :desc :limit 1`, can use index order to make it a
point read). `:limit` + pushdown is the streaming-native primitive; `:offset` is
not.

### Mechanical changes (limit: correctness + streaming early-termination)

1. **AST** — add `Limit *int` to `query.Query` (`datalog/query/types.go`). Pointer
   so "unset" is distinguishable from `:limit 0`. Emit `:limit N` in
   `Query.formatWithIndent` so `String()` round-trips through the parser (same
   obligation `:order-by` already has).
2. **Parser** — add a `:limit` case to the `parseQueryVector` switch
   (`datalog/parser/parser.go`, alongside `:order-by`). Read exactly one integer
   node; reject negatives and non-integers loudly. Enforce the scalar rule:
   `ScalarReturn && *Limit > 1` → parse error.
3. **Plan cache** — hash `Limit` into the plan-cache key
   (`datalog/planner/cache.go`, next to the existing `ORDERBY:` hashing).
   **This is a correctness trap, not an optimization:** `RealizedPlan.Query = q`
   (`planner_clause_based.go`), the cache stores that plan, and the executor reads
   the limit from `plan.Query` — the *cached* query. If `Limit` is not in the key,
   two queries differing only by limit collide and the wrong limit is applied.
4. **Executor — unified finalization** (`datalog/executor/executor.go`). Both
   `:order-by` and `:limit` are applied **once**, at the single `ExecuteRealized`
   boundary, on the fully-assembled result: `Sort` then `Limit`.
   `executeRealizedPlan` now returns the raw, un-ordered, un-limited relation; the
   per-path sorts that used to live in the inline path and (per-tuple) in the
   RelationInput-iteration path were removed.
   - **Why unified, not per-path**: a `RelationInput` query's result is the
     *union* of its per-tuple executions. The old code sorted *inside each
     iteration* and concatenated, so `:order-by` over a `RelationInput` query was
     already silently wrong (the per-tuple sort is discarded by the union), and
     `:limit` over it returned an order-dependent slice. Finalizing over the whole
     union makes the inline and RelationInput paths behave identically and yields
     a correct global top-N. This corrected a pre-existing `:order-by` bug as a
     side effect. (Verified contained to top-level queries: the live subquery path
     runs through `DefaultQueryExecutor.Execute`, never `ExecuteRealized`; the
     `subquery.go` `ExecuteRealized` caller is dead — `NestedPlan` is never
     assigned.)
   - **`:limit`** is a small `LimitRelation` wrapper implementing `Relation`
     (delegates `Symbols()`; lazily materializes at most N, pulling the source
     iterator no more than N times; delegates the rest to that bounded result).
     - No `:order-by`: wrapping the (possibly streaming) result stops the
       underlying scan after N — the real saving.
     - With `:order-by`: `Sort` already returned a `MaterializedRelation`, so the
       wrapper just truncates the first N tuples.
5. **qb builder** — add `Limit(n int) *QueryBuilder` (`datalog/qb/builder.go`),
   store it, and wire it into `Build()` alongside `OrderBy`.
6. **Tests** —
   - Parser: `:limit 5` parses; `:limit -1`, `:limit 3.5`, `:limit "x"` error;
     `String()` round-trips a query carrying `:limit`.
   - Scalar: `:find ?x . … :limit 1` ok; `… :limit 2` is a parse error.
   - Executor: limit < / = / > result size; `:limit 0` → empty; limit composes
     with `:order-by` (top-N by sort key); limit **after aggregation** caps the
     number of groups; limit + RelationInput applies globally (not per-tuple).
   - Streaming: with no `:order-by`, assert the underlying scan stops early
     (e.g. via an annotation/scan-count handler) rather than draining the source.
   - Plan cache: two queries identical except for `:limit` do not share a cached
     limit (the L3 trap).

### Index-order pushdown status (implemented 2026-07-11)

The mechanical plan above made `:limit` correct. Bounded Top-N avoids sorting
the complete result, and proven physical ordering now lets a streaming
`LimitRelation` stop storage after N rows.

The implementation deliberately does **not** treat a latest/AsOf
`:order-by [[?tx :desc]] :limit 1` query as "take the first key." EATV/AETV
place descending Tx inside entity/attribute groups; current-state CRDT
resolution must examine the relevant groups, so global Tx-primary early
termination would be unsound.

The sound bounded index paths currently implemented are raw-History shapes:

- ATEV: constant A, `Tx desc` with optional `E asc`.
- TAEV: unfiltered transaction log, `Tx desc` with optional `A asc, E asc`.
- AETV: constant A, `E asc, Tx desc`.
- EATV: constant E, `A asc, Tx desc`.

Each path receives ordering and limit through a one-pattern Datalog query
fragment, validates the complete physical shape in storage, scans exactly N raw
datoms, and has a differential full-sort reference. Latest and AsOf controls
prove that raw-history ordering properties are declined.

Broader pushdown remains subject to structural preconditions:

- **Joins** — the limit may only be pushed onto the *driving* relation, and only
  when the join preserves that relation's order and cannot drop the top-N rows. A
  pushdown that limits a pattern before a join that then filters those rows away
  is wrong. Safe cases (single-pattern queries; the limited/ordered variable comes
  from the relation that survives the join unfiltered) must be detected explicitly.
- **Aggregation** — `:limit` applies *after* aggregation, so it caps groups, not
  scanned rows; it cannot be pushed to the scan when an aggregate sits between.
- **CRDT resolution** — latest/AsOf scans may use the same physical index but
  cannot inherit a raw-history ordering/early-stop proof.
- **Ordering totality** — index order is a total physical order; if the requested
  `:order-by` key has ties, the pushed result must match the materialize-and-sort
  result (or the optimization must be declined).

Future stages may add other CRDT-safe, index-aligned single-pattern shapes, then
consider join-driving-side pushdown with explicit order-preservation proofs.
Every extension requires differential materialize/sort/limit tests at realistic
sizes.
