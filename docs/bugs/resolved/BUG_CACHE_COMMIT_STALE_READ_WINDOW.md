# BUG: Stale Cached Read Window Between Storage Commit and Cache Update

**Date**: 2026-05-30 **Severity**: Consistency (Low–Moderate; concurrency-only, transient) **Status**: RESOLVED (2026-05-30) — `Commit` marks the touched `(E, A)` keys in-flight in the cache *before* the storage commit (option-2 done with a clobber-proof sentinel); readers resolve in-flight keys straight from storage, so no reader observes the pre-commit value once `stx.Commit()` returns. See [Resolution](#resolution). **Affected**: `datalog/storage/database.go` (`Transaction.Commit`), `datalog/storage/cache.go` (EA cache freshness). Only manifests with concurrent readers on the same `*Database` while a writer commits, and only for `(E, A)` pairs that already have a cached entry. Single-writer / sequential usage is unaffected. History-mode reads (which bypass the cache) are unaffected.

## Summary

`Transaction.Commit` commits the storage transaction first, then updates the EA cache (max-version bookkeeping + entry invalidation) as a separate, non-atomic step. Between those two actions there is a window in which the durable store already reflects the new write but the cache still reports its pre-commit entry as *fresh*. A concurrent reader resolving that `(E, A)` through the cache during the window returns the **old** value for an already-committed transaction.

The window is small (a few `sync.Map` operations) and self-heals once the cache update completes, so it is a transient stale-read, not permanent corruption. But it is a real read-your-writes / linearizability gap for concurrent readers.

## Root Cause

`Commit` orders the two updates as: storage commit, *then* cache update (`datalog/storage/database.go`, abbreviated):

```go
if err := stx.Commit(); err != nil {                 // (A) store now has new datoms
    return datalog.ElementID{}, ...
}

if t.db.cache != nil {
    ...
    for _, d := range t.datoms {
        key := CacheKey{E: ..., A: ...}
        ...
        t.db.cache.UpdateMaxVersion(key, d.Tx)        // (B) bump freshness watermark
    }
    ...
    t.db.cache.Invalidate(touched)                    // (C) drop stale entries
}
```

The cache decides freshness by comparing the stored entry's version against the per-key max version (`cache.go`, `GetOrResolve`):

```go
if val, ok := c.entries.Load(key); ok {
    entry := val.(*CacheEntry)
    if maxVal, ok := c.maxVersions.Load(key); ok {
        if entry.version == currentMax {
            return entry            // "fresh" — returned without consulting storage
        }
    }
}
```

For an `(E, A)` that was already cached at version `V_old` (with `maxVersions[key] == V_old`), the timeline of a commit that writes `V_new` is:

1. **Before (A):** `entries[key] = V_old`, `maxVersions[key] = V_old`.
2. **After (A), before (B):** store has `V_new`; cache still has the `V_old` entry and `maxVersions[key] == V_old`. A concurrent reader sees `entry.version (V_old) == currentMax (V_old)` → **"fresh"** → returns the stale `V_old` value even though `V_new` is committed and durable.
3. **After (B):** `maxVersions[key] = V_new`; now `entry.version != currentMax` → reader falls through and rebuilds from storage → sees `V_new` (correct).
4. **After (C):** the stale entry is deleted outright.

So the stale window is precisely between (A) `stx.Commit()` returning and (B) `UpdateMaxVersion(key, V_new)`. Readers do not hold the committing transaction's `t.mu`, and the cache uses lock-free `sync.Map`s, so concurrent reads proceed freely during the window.

New (never-cached) keys are not affected: a cache miss rebuilds from storage, which already reflects the commit. The bug is specific to *overwriting an existing cached value*.

## Expected Behavior

Once `Commit` returns success, no reader (concurrent or subsequent) should observe the pre-commit value for a committed `(E, A)`. The cache and the store should become visible atomically with respect to readers.

## Actual Behavior

A concurrent reader of an already-cached `(E, A)`, executing in the window between storage commit and the cache's `UpdateMaxVersion`, receives the old value. The anomaly disappears as soon as the cache update completes.

## Why This Is Subtle

- It is concurrency-only and timing-dependent: sequential tests and single-threaded usage never observe it.
- It is transient and self-healing, so even under concurrency it is easy to dismiss as a flaky read rather than a structural ordering issue.
- It affects only keys with a pre-existing cache entry, so a test that writes a fresh key then reads it concurrently won't reproduce it.
- The cache freshness mechanism is otherwise correct (it is designed so false "fresh" never happens *given* `maxVersions` is current) — the gap is purely that `maxVersions` is updated after, not atomically with, the storage commit.

## Reproduction Sketch

Inherently racy; reproduce by widening the window and hammering it:

```go
// Pre-cache (E, :attr) at value v1 by reading it once.
warmup := readAttr(e, attr) // populates cache entry at V_old

var wg sync.WaitGroup
stop := make(chan struct{})

// Readers: continuously resolve (E, :attr) through the cache.
for i := 0; i < 8; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        for {
            select {
            case <-stop:
                return
            default:
                got := readAttr(e, attr)
                // record any read of v1 that happens after the writer's
                // Commit() returned -> stale observation
            }
        }
    }()
}

// Writer: overwrite to v2 and commit.
tx := db.NewTransaction()
tx.Set(e, attr, v2)
txID, _ := tx.Commit() // after this returns, no reader should still see v1
// ... allow readers to run briefly, then close(stop)
```

To make the window deterministic for a regression test, a test-only hook that pauses between `stx.Commit()` and the cache update would let a reader observe the stale entry reliably.

## Fix Direction

Make the store and cache transition atomically *from the reader's perspective*. Options, roughly increasing in invasiveness:

1. **Bump `maxVersions` before, invalidate after — under a per-key guard.** Update `maxVersions[key]` to `V_new` *before* `stx.Commit()` (so any reader that sees the not-yet-committed window treats the entry as stale and rebuilds from storage). Care needed: if `Commit` fails and rolls back, the bumped `maxVersions` must be reverted, and a rebuild during the window must not cache a value derived from the uncommitted store. This is subtle — sequencing alone doesn't fully close it.

2. **Invalidate (delete) cache entries before the storage commit, and bump `maxVersions` after.** A deleted entry forces readers to rebuild from storage; before commit they rebuild `V_old` (correct, pre-commit), after commit `V_new` (correct). The only requirement is that the entry stay absent across the commit so no reader re-caches a stale value mid-flight; a per-key "in-flight" marker or versioned rebuild guard achieves this.

3. **Version-stamped rebuild.** Have `GetOrResolve` capture the store's current max for the key (cheap ATEV/EATV seek or the existing watermark) and only trust a cached entry whose version equals that store-derived max, rather than a separately-maintained `maxVersions` map that can lag the store. This removes the lag by construction at the cost of the freshness check no longer being a pure in-memory `O(1)` map lookup.

The right choice depends on whether the project wants to keep the `O(1)` in-memory freshness check (option 1/2 with careful sequencing + rollback handling) or accept a storage touch on the freshness path (option 3). This is an architectural decision for the owner, not a mechanical fix.

## Verification Plan

- `TestCommit_NoStaleCachedReadAfterCommitReturns` — using a test-only pause hook between storage commit and cache update: warm the cache for `(E, A)`, start a reader, commit a new value, and assert the reader never observes the old value once `Commit` has returned.
- `TestCommit_ConcurrentReadersSeeCommittedValue` — N reader goroutines + a writer overwriting an already-cached key in a loop; assert no reader observes a value older than the most recently returned `Commit`.
- `TestCommit_RollbackDoesNotPoisonCache` — if the chosen fix touches the cache before `stx.Commit()`, verify a failed/rolled-back commit leaves the cache serving the correct pre-commit value (no `V_new` leaks, no stale deletion gap).
- Run the concurrent tests under `go test -race` to catch any new data races introduced by the reordering (note: `-race` is not part of the standard gate; only add it for these targeted tests).

## Resolution

Chosen approach: **option 2 (invalidate before the storage commit), made clobber-proof with an in-flight sentinel** rather than a deletion. Picked over option 1 (pre-bump `maxVersions`) because `UpdateMaxVersion` is monotonic (`cache.go`), so option 1's rollback would have to *lower* `maxVersions` — a non-monotonic, race-prone revert (a too-high `maxVersions` permanently bypasses the cache for that key). Option 3 (storage-derived freshness) was rejected: a per-`(E, A)` freshness seek ≈ the CardinalityOne resolution cost, defeating the cache's purpose.

Mechanism:

- `Transaction.Commit` computes the touched `(E, A)` keys and calls `Cache.BeginInFlight` to store a shared `inFlightEntry` sentinel for each, **before** `stx.Commit()`. The post-commit `Invalidate` (which already deletes the touched entries) clears the sentinels; on commit failure the same `Invalidate` runs, and `maxVersions` was never pre-bumped, so there is nothing to revert.
- `GetOrResolve` returns `rebuild(...)` (resolve from storage, do **not** cache) when it loads a sentinel, so an in-flight key always reflects current storage — `V_old` before the commit is durable, `V_new` after.
- The cache's two write paths (`GetOrResolve`'s rebuild-store and `PopulateFromDatoms`) go through `storeIfNotInFlight`, a compare-and-swap that refuses to overwrite a sentinel. This closes the clobber window where a concurrent rebuild — having resolved the pre-commit value — would otherwise re-cache it over a sentinel set in the meantime.
- `maxVersions` stays monotonic throughout (only ever bumped, after the commit), which is what keeps rollback trivial.

Performance: the read hot path adds a single `entry.inFlight` field read on an already-loaded struct. `BenchmarkGetOrResolve_FreshHit` measured **16.35 → 16.43 ns/op (+0.5%, within noise), 0 allocs** before/after. The cost concentrates on the rare commit path (one sentinel store per touched key) and on reads of the specific keys being committed during the brief commit window (they resolve from storage instead of hitting the cache — the intended correctness trade).

Tests added (`datalog/storage/cache_commit_inflight_test.go`):

- `TestCommit_NoStaleCachedReadAfterCommitReturns` — the regression, via the test-only `Database.onCommitWindow` hook; verified to **fail** when `BeginInFlight` is disabled (it observes the stale `V_old`).
- `TestCommit_ConcurrentReadersSeeCommittedValue` — 8 readers vs a monotonic writer; values never move backward. Race-clean under `-race`.
- `TestCommit_RollbackPathDoesNotPoisonCache` — the failure-path cleanup (`BeginInFlight` then `Invalidate`, no `UpdateMaxVersion`) leaves the cache serving the correct pre-commit value and unpoisoned for the next commit.

Full suite green (`go test -count=1 ./...`); storage package clean under `-race`.
