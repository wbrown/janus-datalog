# Wildcard Pull Opened One Badger Transaction Per Entity

**Status:** Resolved (2026-07-13)

## Summary

A find expression such as:

```clojure
[:find (pull ?entity [*])
 :where [?entity :entity/type "scenario"]]
```

rendered the pull once per result tuple. Each wildcard called `Database.ResolveAllAttributes`, whose entity scan owned a fresh Badger read transaction and iterator. N matched entities therefore created and discarded N transactions after relational query execution had already completed.

The per-entity loop predates v0.14.0. Moving Pull rendering to the final result boundary in v0.14.0 made its cost fall outside the `query/completed` annotation, which exposed the transaction churn as a large traced-time gap. Downstream issue TTR-381 supplied the decisive scheduler trace:

- 71.07% under `badger.(*Txn).Discard`.
- Approximately 19–21% under iterator seek/block loading.
- 7.73% under `badger.(*DB).NewTransaction`.

## Root Cause

`applyFindPulls` called `PullExecutor.Pull` inside its tuple loop. `PullExecutor.PullMany` also looped over `Pull`. The wildcard resolver interface only accepted one entity:

```go
ResolveAllAttributes(entity datalog.Identity)
```

At the storage boundary, `BadgerStore.Scan` correctly gave each independent scan its own transaction. The defect was issuing independent scans for a result set whose entity IDs were already known together.

There was no existing query-wide Badger transaction to reuse. Adding one would have changed lazy Relation lifetimes, cache/snapshot semantics, subqueries, multi-source execution, and concurrency. The fix is deliberately scoped to wildcard Pull finalization.

## Fix

`BatchEntityResolver` adds ordered multi-entity wildcard resolution. `applyFindPulls` first collects the result tuples, then calls `PullMany` once per pull expression. Exact `[*]` patterns use the batch resolver; explicit, defaulted, limited, and nested patterns retain their existing behavior.

`Database.ResolveAllAttributesMany`:

1. Deduplicates entities and sorts them by identity bytes, matching EATV order.
2. Opens one Badger read transaction and one iterator for the non-unique EATV ranges that dominate wildcard resolution.
3. Seeks each entity's EATV range through that iterator.
4. Groups datoms by attribute and applies the canonical LWW, add-wins, or RGA resolution functions.
5. Populates the EA cache when enabled.
6. Preserves input order in returned results.

Latest and AsOf modes filter transactions exactly as their matcher does. Unique attributes retain walk-based ownership/fallback resolution. History mode retains its prior raw-mode path. Duplicate input entities are scanned once but receive independent result maps.

When a schema is present, the batch path applies the same declared-attribute set as single-entity `ResolveAllAttributes`; stored undeclared attributes remain excluded from wildcard results. Fully tombstoned CardinalityVector attributes are present as typed empty vectors, while never-set vectors remain absent. The cache-enabled and cache-disabled single-entity paths now share that distinction.

Unique-attribute ownership walks and Tier-3 blob dereferences may open additional specialized reads; the single transaction claim applies to the dominant batched EATV traversal, not every possible storage access.

`pull/batch.begin` and `pull/batch.complete` annotations expose entity count, attribute count, latency, mode, and success at the result boundary.

## Verification

Tests cover:

- Query-level `(pull ?entity [*])` over 230 matched entities and exact batch annotation counts.
- Cache-enabled and cache-disabled resolution.
- CardinalityOne tombstones, CardinalityMany sets, CardinalityVector ordering, missing entities/attributes, duplicate entities, and input-order preservation.
- AsOf visibility.
- Unique-attribute ownership fallback.
- Declared-schema filtering of stored undeclared attributes.
- Differential batch-versus-single resolution over randomized schema-backed data.
- Fully tombstoned versus never-set vector semantics in both cache modes.
- History-mode fallback and exact-wildcard gate decline for mixed patterns.
- Multi-valued attributes through the end-to-end rendered Pull boundary.
- Batch resolver and input iterator/close error propagation.

Focused storage benchmark, five attributes per entity with cache disabled:

| Entities | Per-entity | Batch | Time | Memory | Allocations |
|---------:|-----------:|------:|-----:|-------:|------------:|
| 230 | 4.829 ms | 335.4 µs | 14.4× faster | 90.9% less | 89.7% fewer |
| 3,899 | 64.19 ms | 5.917 ms | 10.8× faster | 90.9% less | 89.7% fewer |

The full repository suite passes.

## Architectural Lesson

When a complete binding set is already available, a storage operation should accept that set as one execution unit. Repeating a correctly isolated single-entity API can still create pathological transaction and iterator lifecycle costs. A targeted batch boundary avoids that cost without forcing query-wide snapshot ownership into every lazy Relation.
