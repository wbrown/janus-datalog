# BUG: Error-swallow sweep findings, 2026-07 (open work list)

**Status**: Open (2026-07-19). Owner ruling: error swallows get fixed. This document is the enumerated work list from the sweep; entries move to a `Status: Resolved` note (or this doc moves to `resolved/`) as they land. Fixed in the same session, before this sweep: `thetaJoinPair` (eval-error continue + both deferred iterator errors + Close errors), `mergeJoinIterator` (deferred scan error tail).

Three shapes:
- **A** — an eval/decode error inside an iteration loop is treated as skip (`continue`), neither recorded nor returned.
- **B** — a hand-rolled `for iter.Next()` loop never consults `iter.Error()` afterwards: a failed scan is presented as a completed (possibly empty) result.
- **close-only** — `.Error()` is consulted but the `Close()` return is discarded.

## executor

- `query_executor.go:844` — `executeExpression` (get-some/all-constants extend path) — B — drops the group relation's scan error; bare `Close`.
- `query_executor.go:1553`, `:1564` — `antiJoinOnSymbols` — B ×2 — drops right/left scan errors; bare `Close`.
- `query_executor.go:1726` — `extractEntityIDs` — B — drops scan error (prefetch entity-ID collection).
- `query_executor.go:1933` — `filterWithNotClause` final filter loop — B — input materialized first, low severity.
- `query_executor.go:2014` — `filterWithNotJoinClause` final filter loop — B — same; the inner count loop already uses `ForEach`.
- `join.go:757`, `:762` — `crossProduct` — B ×2 — drops left/right scan errors.
- `relation.go:1514` — `Select` — B — drops source scan error.
- `prepended_relation.go:94` — `PrependedRelation.Materialize` — B — drops rest-relation scan error; rest may be storage-backed.
- `pull.go:314` — `getAllAttributesInternal` — B — returns datoms without `.Error()` (wildcard pull).
- `pull.go:635` — `lookupAllValuesInternal` — B — returns values without `.Error()`.
- `lazy_seq_relation.go:219` — `lazySeqIterator.Next` — A — `rest, _ := it.cur.Rest()` discards the lazy-realization error (`First()`'s is captured, `Rest()`'s is not).
- `buffered_iterator.go:158` — `BufferedIterator.Clone` — B (low) — clone's consumers never see the consumed source's deferred error.
- `testing.go:157/:184/:198`, `test_fixtures.go:47` — B — test/debug support, not production-reachable; fix for hygiene or leave, owner's call.

## storage

- `matcher.go:274` — `MatchWithHistory` — B — per-datom errors returned, terminal scan error dropped. (C8 lists this API as superseded/test-only; fix or delete with the C8 decision.)
- `matcher.go:333` — `MatchAsOf` — B — same; same C8 coupling.
- `matcher.go:1084` — `lookupAllAttributesFallback` (LWW re-scan branch) — **A + B** — decode error `continue`d and terminal error dropped; reachable via LookupAllAttributes with cache disabled / wildcard resolution.
- `matcher_relations.go:904` — `validatingVBoundIterator.validateCandidate` — **A + B, high severity** — returns bare `bool`; every storage/decode failure silently collapses to "candidate doesn't match," dropping valid results on the V-bound validation path.
- `matcher_relations.go:1402` — `matchWithBindingsFromCache` — B — materialized bindings, low severity.
- `matcher_relations.go:1672` — `matchVectorWithBindings` — B — binding iterator's error dropped (per-entity resolve errors are returned); low severity.
- `matcher_relations.go:732` — `validatingVBoundIterator.Next` — close-only.
- `matcher_strategy.go:263` — `chooseBestMultiPositionStrategy` — B — feeds a planning heuristic only; low severity.
- `database.go:2789` — `resolveAttributeViaMatcher` (CardinalityMany branch) — B — relation may be lazy; reachable via ResolveEntityAttributes / pull.
- `database.go:333/:336` — `WarmCache` — close-only.
- `simple_batch_scanner.go:361` — `scanAndFilter` — B — terminal scan error dropped; compounding: `Scan()` (:57) returns nil unconditionally at :88 and never returns the captured `s.err`, so even captured errors depend on the consumer calling `scanner.Error()`.
- `simple_batch_scanner.go:98` — `buildBindingSet` — B — materialized bindings, low severity.
- `unique_resolve.go:146` — `resolveMaxOtherTxForValue` — **A + B, high severity** — decode errors `continue`d and terminal error dropped inside unique-attribute supersession checks (CRDT correctness surface).
- `unique_resolve.go:209` — `resolveAVLWW` — **A + B, high severity** — same shape in unique (A,V)-LWW owner resolution. (Sibling `walkUniqueEntityValue` is correct — use it as the template.)
- `prefetch.go:50` — `PrefetchEntities` — A — scan-open failure skips the entity silently; best-effort cache warming, low severity.

## Verified non-hits

`pull_batch.go` resolve functions (second returns are not errors), `resolveWildcardEntity` (caller consults the shared iterator's Error), `cache.go IsAttributeFresh` (error → stale → safe re-resolution), `relation_ops.go` loops (fixed or already correct), `FunctionEvaluatorIterator`/`DedupIterator`/`selectionIterator` (Error() delegates to source).
