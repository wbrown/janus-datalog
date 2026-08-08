# Physically deleted datoms orphan their tier-3 blobs, and nothing ever reclaims them

**Status**: ✅ RESOLVED (2026-08-07, lands with this doc move). Both physical-delete paths now reference-count blobs after their own deletes have landed, inside the store — nothing above the `Store` seam learned that blobs exist. `DeleteDatoms` gathers candidate hashes from the encode it already performs, flushes its index deletes, then deletes each candidate no surviving datom names; `Transaction.Retract` accumulates candidates on the transaction and counts them in `Commit`, so a value retracted and re-asserted in one transaction keeps its blob. Reference is asked of the hash under every tag in `datalog.HashedValueTypes`, via `BinaryKeyEncoder.VAETHashPrefix`. Pinned by `blob_reclamation_test.go` (four behaviours, both backends) and `blob_reference_probe_test.go` (four unit pins), all red-first or written to fail against a too-eager count. Native gate green; the wasm leg is red on the unrelated `BUG_WASM_STORAGE_GC_BAD_POINTER_CRASH` signature (occurrence 41).

Two things the fix deliberately does not do, both owner rulings: reclamation on the `Transaction.Retract` path is best-effort rather than guaranteed (see "Concurrency" below), and it is scoped to the blobs each delete touched, so blobs orphaned before this change still need export/reimport.

The body below is the original report, retained as the derivation. Its "The fix" section has been rewritten to describe what shipped rather than what was proposed.

A space defect, not a wrong-answer one. The reason it was worth a document is that the obvious fix — delete the blob alongside the datom — *is* a wrong-answer bug, and so is the first correct-looking version of the reference count that replaces it.

## Summary

Tier-3 values live in a content-addressed blob keyspace: `[0xFF][sha1(compressed):20] → compressed bytes`, written by `putBlob`, read by `ReadBlob`/`getBlob`. There is no third operation. Nothing in the tree deletes a blob, and no reachability pass exists.

Two production paths physically remove index keys, and both leave the blob behind:

| Path | Route | Gated against writers |
|---|---|---|
| `Transaction.Retract` (public API) | `Transaction.Commit` → `StoreTx.Retract` → `retractDatom` | No — ordinary commit path |
| `Database.TruncateTo` | `DatomsAfter` → `Store.DeleteDatoms` | Yes — writers drained, new ones dropped |

Only values too large for a key go out of line, so what leaks is exactly the largest payloads, and it leaks permanently: the orphan is a live Badger key, so neither compaction nor value-log GC touches it.

## Why deleting the blob with the datom is wrong

Blobs are deduplicated by content. One `[0xFF][hash]` key serves *every* datom whose value compresses to those bytes — across entities, across attributes, and across history. Deleting it when one referencing datom goes away dangles all the rest, and `ReadBlob` then fails the read with `blob not found for hash %x`.

History makes this ordinary rather than exotic. The store is append-only: setting an attribute to a large value X, then to Y, then back to X leaves two live datoms referencing blob(X) at different Tx. Physically deleting the later one must not drop the blob, because the earlier one is still there and still readable through `History()` and `AsOf`.

So `DeleteDatoms` and `retractDatom` leaving the blob in place is the only safe thing either can do without a reachability answer. The defect is that no one ever supplies the answer.

## What it costs

Unbounded and monotonic. A snapshot → write → `TruncateTo` → rewrite loop returns the logical database to the same state every cycle while the file grows by every out-of-line value the discarded tail wrote. The same holds for any workload that retracts large values.

Today's only reclamation is a dump and reload: `ExportBinary` scans EAVT, so it emits datoms and never sees an orphan, and the reimport writes only blobs its datoms reference.

## The premise this breaks

[BRANCHING_AND_SNAPSHOTS.md](../../proposals/BRANCHING_AND_SNAPSHOTS.md) §6.4 rules reclamation out of scope, on an explicit premise:

> As long as we never prune, all branches, deleted-branch frontiers, and snapshots remain valid forever (append-only). Reclaiming space means dropping datoms unreachable from any live frontier, which must be reachability-aware (git-gc respecting refs + reflog). That is where "no side effects" ends, and it is opt-in, deliberately out of scope here.

Two shipped paths prune. `Transaction.Retract` predates the snapshot work entirely, and `TruncateTo` is Slice B of that same proposal. The premise that licenses deferring GC does not cover either, and §6.4's own answer — reachability-aware — is precisely what the fix below computes, at a far smaller scale than a frontier walk.

## The fix: reference-count by hash, inside the store, after the delete

The reference set of a blob is derivable from the indices; no bookkeeping keyspace and no write-time counter is needed. `VAET` is `[prefix][type][value][A][E][Tx↓][AfterRef?][Op]` — value-leading — and `assertDatom` writes every datom into all eight indices, so VAET is a complete value→datoms index over all of history, not a ref-only index as in Datomic. A tier-3 value's key payload is fixed width: `compressAndRoute` returns `hash[:]` as the encoded value bytes, and `EncodeValueBytes` prepends the one-byte type, giving exactly `[TypeHashedString|TypeHashedBytes][sha1:20]`.

So "how many datoms still refer to this blob" is a prefix seek per tag, and the delete happens at zero. The probe stops at the first surviving reference, which already settles count > 0, rather than enumerating a reference set that may be large.

The question is keyed on the **hash**, not on a value: `BinaryKeyEncoder.VAETHashPrefix(vType, hash)` builds the 22-byte prefix directly. An earlier draft of this section routed the query through the typed seam instead — `ScanBound{Index: VAET, Prefix: []datalog.Value{v}}`, using the value the deleter happens to hold — to avoid naming an encoding artifact above the seam. That was contortion: `ScanBound`'s typed domain governs the query path, not the store's housekeeping over its own keys, and keying on the value rather than the hash is what makes the two-tag trap below look like a special case instead of "there are two prefixes."

The whole operation lives below the `Store` interface. Nothing outside `datalog/storage` references blobs — `datalog.BlobData` is produced by `EncodeValue` and consumed only by the store's own `EncodeValueBytes`/`assertDatom` — so exposing reclamation on `Store`, or having `TruncateTo` invoke it, would have been the first time anything above the seam learned the tier exists. `TruncateTo` is unchanged.

Cost is one or two seeks per distinct blob the delete touched — bounded by the deletion, not by the store.

### The trap: one blob, two type tags

**A reference check that scans the value's own tag alone will delete live blobs.**

`putBlob` keys on `sha1(compressed)` and nothing else — the type tag is not in the blob key. But the *index* key carries it: `compressAndRoute` is called with `(TypeCompressedString, TypeHashedString)` for a `string` and `(TypeCompressedBytes, TypeHashedBytes)` for a `[]byte`. `codec.Compress` is deterministic (a hard requirement of the codec), so the same bytes stored once as a `string` and once as `[]byte` produce identical compressed output, identical SHA1, and therefore **one shared blob under two distinct VAET prefixes**.

A scan on the deleted value's own encoding sees only its own tag, reports zero survivors, and drops a blob the other tag still references — converting a space leak into `blob not found for hash %x` on a legitimate read. The check must union both hashed tags, i.e. be keyed by the hash even though the seam speaks values.

### Ordering: the count is taken after the delete, never derived from before it

The count is a read against a keyspace the same operation is mutating, so when it runs is load-bearing. It runs last, in both paths, and against the settled index:

- `DeleteDatoms` flushes its `WriteBatch` before counting. The batch commits in chunks and offers no read-your-writes, so a probe issued mid-batch would read a mixture; after the flush there is nothing in flight.
- The retract path counts in `Commit`, after every op in the transaction has been applied, using an iterator on that transaction — which merges its own pending writes, so the deletes are already invisible to the probe.

Neither path subtracts its own datoms from a prior count. The same content can appear on several datoms in one call and on others the call never touches, so only the surviving index knows; and counting in `Commit` rather than per-`Retract` is what lets a value retracted and asserted again in one transaction keep its blob.

### Concurrency: the two call sites are not equally safe, by ruling

`TruncateTo` drains in-flight writers and drops new ones for its whole duration, so count-then-delete is final there by construction.

The commit path has no such gate. A concurrent transaction can assert a new reference to the same content between the count and the commit, after which the blob is deleted out from under a datom that references it. `NewBadgerStore` sets `DetectConflicts = false`, so Badger will not abort on the invalidated read set — nothing catches it.

The owner ruling is that the retract path reclaims **best-effort** anyway rather than not at all: physical retraction is a maintenance primitive, not an ordinary write on this engine, and the alternative is a lock over every tier-3 write on the hot commit path. The exposure is stated on `reclaimBlobsInTxn`. In Badger the blob delete joins the caller's transaction, so it is at least atomic with the retract that orphaned it; in memory it goes through `deleteMemoryEntry`, so a failed apply restores the blob along with the datoms.

### What this shape does not cover

Blobs already orphaned by past retracts and rewinds. Reclamation is scoped to the candidates each delete touched — which is what keeps it O(deleted blobs) rather than a scan of the whole blob keyspace — so nothing collects pre-existing garbage. A repair pass would walk the `0xFF` keyspace against the live hash set; export/reimport already performs that, since `ExportBinary` scans EAVT and never sees an orphan.

## What was verified

By reading, at the identifiers named above:

- `blobKeyPrefix` appears in exactly three non-test places: `putBlob` (write), `ReadBlob` (read), and the `memoryReadSession` key filter. No delete path exists anywhere in the tree.
- `Transaction.Retract` appends to `t.retracts`; `Transaction.Commit` passes that buffer to `StoreTx.Retract`, and `retractDatom` deletes the matching stored datom's key from every entry in `Indices` while never touching a blob.
- `TruncateTo` reaches the same outcome through `Store.DeleteDatoms`, which discards `EncodeValueBytes`'s `blobData`.
- `compressAndRoute` returns `hash[:]` with `TypeHashedString` or `TypeHashedBytes` depending only on whether the source was a `string` or a `[]byte`; `BlobData.Hash` is `sha1.Sum(compressed)`, with no type discrimination.
- `EncodeValueBytes` builds `vBytes` as one type byte followed by the encoded data, so a tier-3 payload is 21 bytes wide.
- `encodeKeyWithParts`'s `VAET` arm places `vBytes` immediately after the index prefix, and `assertDatom` writes all eight indices for every datom.
- `ScanBound` carries `Prefix []datalog.Value` and documents that bounds introduce no value kinds beyond the closed domain.
- `NewBadgerStore` sets `DetectConflicts = false`.
- `ExportBinary` scans EAVT, so orphans are not exported.

Backend scope, closed by the reproducer: `MemoryStore` orphans in the same way — it removes only index keys from `s.entries`, where blob entries live under the same prefix. `MemoryTreeStore` cannot orphan because it has no blob tier at all: it holds whole datoms, so a large value has no fixed-width key to escape from. That is what `blobKeys` reports as `hasBlobTier` false, and why `byteKeyBackends` excludes it.

## Adjacent, found while reading — not the subject

`blob_store.go`'s header comment states "The 0xFF prefix separates blobs from all index keys (0x00-0x06)". There are eight indices, `TAEV` is 7, so index keys run 0x00–0x07. The invariant itself holds and is pinned by `blob_store_badgerdb_test.go` asserting `blobKeyPrefix > TAEV`; only the parenthetical is stale.

## The pin

`blob_reclamation_test.go` (four behaviours, across `byteKeyBackends` — Badger and `MemoryStore`) and `blob_reference_probe_test.go` (four unit pins). All green; gate cost under a second.

**`TestBlobReclaimedWhenLastReferenceDeleted`** — the reproducer, red first on both removal paths and both backends. It writes a 200 KB payload that routes to tier 3, removes the sole referencing datom, and asserts both that the datom is gone and that no key remains under the `0xFF` prefix. Before the fix the first assertion passed and the second reported one surviving blob, which is the defect stated exactly: the keys go, the blob stays. Subtests cover `Transaction.Retract` and `Store.DeleteDatoms` separately, because the two paths reach storage differently and are gated differently against concurrent writers.

**`TestBlobReferenceCountedWithinOneDeleteCall`** — three datoms share one blob and two are deleted in a single call, so the count deciding the blob's fate is 1. A count taken before the deletes would read 3 and never reclaim; one derived by subtracting the call's own datoms drifts as soon as the same content appears twice in one call.

**`TestBlobSurvivesWhileAnotherEntityReferencesIt`** and **`TestBlobSharedAcrossStringAndBytesEncodings`** — written as pins on the fix rather than reproducers: they passed trivially against the leaking store, and their job is to fail against a reference count that reclaims too eagerly. Both now genuinely exercise the count, since the retract path they use reclaims. The second asserts the trap directly — `EncodeValue` of the same bytes as a `string` and as a `[]byte` yields `TypeHashedString` and `TypeHashedBytes` with **equal** `BlobData.Hash`, the store holds one blob for the pair, and removing the `[]byte` datom leaves the `string` datom readable. That equality is measured, not reasoned: the two encodings do share one blob, and a count scanning a single tag deletes it.

Neither pin may be relaxed to make a future reclamation change go green — the relaxation is the regression.

The unit pins hold the probe to the key layout it addresses: `TestVAETHashPrefixAddressesTheWrittenKey` (reordering VAET or widening the type tag silently turns every probe into a miss, which reads as "unreferenced"), `TestVAETHashPrefixSeparatesTheHashedTags`, `TestBlobIsReferencedProbesEveryHashedTag` (absence concluded only after every tag is asked), and `TestBlobKeyLayout`.

Not covered: the history case — set a value, change it, set it back, delete the newest datom, and read the oldest through `History()`. Same shared-blob shape as the pins above, reached through time rather than through a second entity.
