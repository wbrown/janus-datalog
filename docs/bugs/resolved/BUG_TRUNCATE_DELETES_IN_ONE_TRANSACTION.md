# The rewind deletes the whole tail in one Badger transaction, and Badger caps the transaction

**Status**: ✅ RESOLVED (2026-08-07, lands with this doc move). `BadgerStore.DeleteDatoms` writes its index deletes through a `badger.WriteBatch` instead of one `db.Update` closure, so the batch splits at Badger's own ceiling exactly as `AssertEach` already does on the write side — the arithmetic stays Badger's and cannot drift from it. Pinned by `TestBadgerDeleteDatomsAboveTransactionLimit` (`delete_datoms_batch_size_test.go`), red-first with `delete from EATV index: Txn is too big to fit into one request`. The atomicity the single transaction gave up is documented on `DeleteDatoms` itself and in "What the fix changes" below; the corresponding wording in [BRANCHING_AND_SNAPSHOTS.md](../../proposals/BRANCHING_AND_SNAPSHOTS.md) §12.1 is an owner ruling still outstanding. Native gate green; the wasm leg is red on the unrelated `BUG_WASM_STORAGE_GC_BAD_POINTER_CRASH` signature (occurrence 41).

The body below is the original report, retained as the derivation.

Not a wrong-answer bug. The transaction aborted cleanly, nothing was deleted, and the error reached the caller. What it was: an undeclared size ceiling on the snapshot facility, low enough that the rewinds the facility exists for were the ones that hit it.

## Summary

`BadgerStore.DeleteDatoms` wraps every delete in one `s.db.Update` closure — one Badger transaction. `Database.TruncateTo` hands it the entire post-snapshot tail in a single call, and each datom deletes a key from all eight indices, so the transaction's pending-write set is eight times the tail's datom count. Badger caps a transaction's pending writes at a fraction of the memtable, so the rewind fails outright once the tail exceeds roughly 26,000 datoms.

The failure surfaces as:

```
TruncateTo "<name>": delete: delete from <INDEX> index: Txn is too big to fit into one request
```

The named index is whichever key crossed the threshold — Badger charges per key, not per datom, so it is not the first entry of `Indices` and any of the eight can appear. The reproducer below reports `EATV`.

## Mechanism

`badger.Txn.checkSize` rejects an entry when either axis is reached (`count >= maxBatchCount || size >= maxBatchSize`), and both are fixed at open from `MemTableSize`:

| Quantity | Value | Source |
|---|---|---|
| `MemTableSize` | 128 MiB | `NewBadgerStore` |
| `maxBatchSize` = 15% of `MemTableSize` | 20,132,659 B | badger `Open` |
| `skl.MaxNodeSize` | 96 B | badger `skl` |
| `maxBatchCount` = `maxBatchSize / MaxNodeSize` | 209,715 entries | badger `Open` |
| Index keys written per datom | 8 | `Indices` |
| ⇒ datoms per rewind, count axis | 26,214 | derived |

The size axis lands in the same neighbourhood and moves with the data. `encodeKeyWithParts` builds every index key as prefix(1) + E(20) + A(32) + Tx(16) + encoded V + Op(1), plus AfterRef(16) for an RGA operation — 70 bytes of fixed material and up. Badger charges a delete `len(key) + 2` from `estimateSizeAndSetThreshold` (a delete carries no value) plus 10 for the version, so the charge per key is roughly `len(key) + 12`. At a bare 72-byte key that is ~240,000 keys, above the count axis; once average encoded values exceed about a dozen bytes the size axis binds first and keeps tightening as values widen. Out-of-line values do not rescue it — a tier-3 value puts a hash in the key, which bounds the key but does not shrink it below the fixed 70.

So the practical ceiling is on the order of 25,000 datoms per rewind, falling as values grow. Small tails rewind fine, which is why this did not show earlier.

Blob keys are not part of the arithmetic: the batch charges exactly eight index keys per datom and no blob key, because a content-addressed blob is shared and no delete site can drop one without a reachability answer. Computing that answer is its own defect, recorded in [BUG_BLOBS_ARE_NEVER_RECLAIMED.md](BUG_BLOBS_ARE_NEVER_RECLAIMED.md); its fix runs after this flush, and is what makes an interrupted rewind recoverable rather than leaving blobs no later call could name.

**The failure is clean.** `badger.DB.Update` discards the transaction when `fn` returns an error, so no key is deleted. `TruncateTo` calls `Cache.InvalidateRewind(keys)` on that path to close the in-flight window it opened, and returns before `clock.Restore`, so the clock still sits above `markerMax` and the tail is intact. The database is exactly as it was; the rewind is simply unavailable.

## Reach

`Database.TruncateTo` is the only production caller of `Store.DeleteDatoms` — the other call sites are `TestStoreBackendContract` and the snapshot tests.

Badger only. `MemoryStore.DeleteDatoms` deletes from a map and a key index, and `MemoryTreeStore.DeleteDatoms` removes through a single version builder; neither has a per-transaction cap. A cross-backend test therefore goes red on exactly one case, which is the shape the cache-path tombstone gap already cost once: one semantic operation, several backends, coverage on the ones that work.

## What was verified

By reading, at the identifiers named above:

- `BadgerStore.DeleteDatoms` performs all deletes inside one `s.db.Update`, iterating `Indices` (eight entries) per datom.
- `TruncateTo` collects the tail with `store.DatomsAfter(markerMax)` and passes the whole slice to `DeleteDatoms` in one call, with no chunking anywhere between.
- `badger.Txn.checkSize` returns `ErrTxnTooBig` on `count >= maxBatchCount || size >= maxBatchSize`; `ErrTxnTooBig` is `"Txn is too big to fit into one request"`.
- `badger.DB.Open` computes `maxBatchSize = (15 * MemTableSize) / 100` and `maxBatchCount = maxBatchSize / skl.MaxNodeSize`; `skl.MaxNodeSize` is `unsafe.Sizeof(node{})` — 8 + 4 + 2 + 2 + 20×4 = 96.
- `NewBadgerStore` sets `MemTableSize = 128 << 20`.
- `badger.DB.Update` discards on a non-nil `fn` error, so a failed `DeleteDatoms` leaves storage untouched.
- On reopen, `NewDatabaseWithOptions` restores the clock from `store.MaxElementID()`, so a tail that survives a failed rewind leaves the clock above `markerMax` rather than colliding with it.
- Snapshot markers sit at or below `markerMax` by construction (`TruncateTo` deletes strictly above it), so `lookupSnapshot` and `snapshotMarkerMax` still resolve after a partial delete — a re-run picks up exactly the survivors.

## The correct fix

Write the deletes through a `badger.WriteBatch`, as `BadgerStore.AssertEach` already does for the write side. `WriteBatch.Delete` calls `txn.Delete`, and on `ErrTxnTooBig` commits the current transaction and retries the key in a fresh one — Badger's own commit-and-continue, using Badger's own arithmetic. `AssertEach`'s doc comment already states the reason this is the shape rather than a hand-rolled one:

> The batch splits at that ceiling itself, so the arithmetic stays Badger's and cannot drift from it.

That rules out the two alternatives rather than ranking them:

- **Fixed-size chunks of the datom slice.** The cap is two axes, and the size axis depends on value width, so any constant is wrong for some database — too small everywhere else, still too large for wide values. This is the drift the sentence above names.
- **A hand-rolled commit-and-continue on `ErrTxnTooBig`.** Reimplements `WriteBatch.Delete` exactly, including the "retry once, and if it fails again make the error permanent" rule, with nothing gained.

Raising `MemTableSize` is not a fix either; it moves the ceiling and leaves it undeclared.

## What the fix changes, and what needs a ruling

The single transaction is not an accident to be quietly replaced. [BRANCHING_AND_SNAPSHOTS.md](../../proposals/BRANCHING_AND_SNAPSHOTS.md) §12.1 specifies it — step 4 of the choreography is "physically delete the collected datoms from all eight indices in one `BadgerTx`" — and one of its consequence bullets reads on it:

> Reads never block on a rollback and never see a torn or stale-cached value: they run on MVCC snapshots and bypass the cache for touched keys across the window.

Batching keeps the second clause and weakens the first. Two consequences, both verified above as survivable but neither the owner's to discover after the fact:

1. **A failed or interrupted rewind leaves a torn tail** — some post-snapshot datoms gone, others present — visible through `History()` until the rewind is re-run. It is resumable rather than corrupting: the clock is restored only after every delete succeeds, `InvalidateRewind` still drops cached state for every touched key on the failure path, a crashed process reopens with the clock above `markerMax`, and a re-run's `DatomsAfter(markerMax)` collects exactly the survivors.
2. **A reader's MVCC snapshot can land between chunks** and resolve an (E, A) to an intermediate value — neither the pre-rewind nor the post-rewind state — for the duration of the rewind. The all-or-nothing transaction precluded this; the cache window does not, because it only forces resolution to go to storage, and storage is what is mid-change.

Against that, none of the safety story `TruncateTo`'s own comment tells depends on the delete being atomic: writers are gated and drained before `DeleteDatoms` runs and stay gated until after the clock restore, so no chunk boundary can interleave a writer, and the cache in-flight window (`BeginInFlight` → delete → `InvalidateRewind`) already spans the whole delete either way.

The decision is the owner's, and it is not confined to the code: `BadgerStore.DeleteDatoms`'s doc comment advertises "in a single transaction", `TruncateTo`'s comment and the proposal's consequence bullet both need the reader-visibility clause amended, and the contract for a failed rewind ("re-run to complete; `History()` shows a torn tail until then") has to be written down where a caller will find it.

## Adjacent, found while reading — not the subject

`DeleteDatoms`'s returned count is not a count of keys removed. `badger.Txn.Delete` is a blind write that never returns `ErrKeyNotFound`, so the `err != badger.ErrKeyNotFound` guard is unreachable and `deleted` increments once per input datom regardless of what was there; `MemoryStore.DeleteDatoms` likewise returns `len(datoms)`. `MemoryTreeStore.DeleteDatoms` genuinely counts removals, and its doc comment says so ("reports how many were there"), so the three backends do not agree on what the number means. `TruncateTo` discards it, and `TestStoreBackendContract` asserts it only for a case where present-count and input-count coincide, so nothing currently reads the difference — but a batched rewind defers errors to `Flush`, which forces the question of what the count reports, so it is recorded here for whoever writes that code.

## The pin

`TestBadgerDeleteDatomsAboveTransactionLimit` (`delete_datoms_batch_size_test.go`), red first with `delete from EATV index: Txn is too big to fit into one request`, green since the fix.

It derives the ceiling from `store.db.MaxBatchCount()` rather than restating the constants tabulated above: those are Badger's, they move with its options, and a test that copies them stops testing the real boundary the moment either side changes. It then writes `MaxBatchCount()/len(Indices) + 1024` datoms, asserts they are all in the index, deletes them, and asserts every index is empty.

The asymmetry with the write side is deliberate and is the test's other assertion: `Assert` of the same datoms succeeds, because `AssertEach` writes through a `WriteBatch` that splits at exactly the ceiling this delete runs into.

Gate cost is 0.32s — the write side batches, so the expected expense never materialized. This displaces the alternative of exposing `MemTableSize` through `NewBadgerStore` to shrink the cap: nothing is bought by a surface change.

Not covered: the `TruncateTo` path end to end. The failure is thrown in `DeleteDatoms`, which this addresses directly, and reaching it through `TruncateTo` would mean writing the same tail through the commit path at much higher cost for the same assertion.
