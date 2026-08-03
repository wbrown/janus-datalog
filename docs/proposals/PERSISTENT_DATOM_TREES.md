# Persistent datom trees: the durable store as a CoW tree file

## Status

Proposal. The successor project to
[MEMORY_DATOM_INDEXES.md](MEMORY_DATOM_INDEXES.md): that document put typed
datom trees behind the read seam for the in-process store; this one asks
whether the same structure, persisted, replaces Badger as the durable backend.
No decision is recorded here. The measurements that motivate it are in
[`MEMORY_BACKENDS_2026-07-31.md`](../perf/MEMORY_BACKENDS_2026-07-31.md),
[`complex_query_backends_2026-07-31.txt`](../perf/complex_query_backends_2026-07-31.txt),
and `PERFORMANCE_STATUS.md`'s 2026-08 import entries.

## Summary

Badger is an LSM: a structure engineered for overwrite-heavy keyspaces, where
compaction exists to reclaim superseded versions. The janus store is
append-only — datoms are operation records, retraction is an assert, physical
deletion exists only for truncate and rebuild — and CRDT resolution above the
store owns everything Badger's own MVCC provides. The engine pays an overwrite
tax on a workload that never overwrites, eight index keys at a time.

`MemoryTreeStore` is already the memory half of the alternative. Immutable
nodes, structural sharing, one atomic root swap per commit — that is the
copy-on-write B-tree architecture LMDB ships as a durable store. Persist nodes
as pages and commit becomes: write new pages append-only (never over a live
one), fsync, flip one of two meta roots, fsync again. A crash at any point
leaves the previous root fully valid. **Durability without a write-ahead log**,
because nothing is ever modified in place.

## The evidence, and what it licenses

Same engine, same queries, same data; one cold process per backend:

| tier | memory-trees vs badger |
|------|------------------------|
| raw scan (`MEMORY_BACKENDS_2026-07-31`) | 13–19× faster, 3 allocations flat |
| bulk import (62 MB JDZL, 2.7M datoms) | 2.9 s / 1.38 GB / 5.9M allocs vs 43.9 s / 32.2 GB / 338M |
| complex checkpoint | ~17.3 ms vs ~18.5 ms, allocation profiles identical to 0.16% |

The rows license different amounts. The scan and import rows compare a store
with zero durability cost against one paying full durability cost, so they are
ceilings, not predictions. The checkpoint row is the strong one: the EA cache
shields most storage reads on that workload, and the tree store still beats
Badger — meaning the byte-key decode Badger imposes on the residual reads is
measurable even at the cache's back. The uncached paths are where the 13–19×
lives, and a durable tree keeps its hot working set typed (below), so cached
behavior carries over.

What Badger spends that this design does not:

- **Compaction.** LSM levels exist to merge overwritten keys. This keyspace has
  none; every compaction byte is waste heat.
- **A write-ahead log.** CoW plus a two-slot meta root makes torn commits
  structurally impossible rather than repairable.
- **Its own MVCC.** Tx lives in the datom and resolution is the index order;
  Badger's per-key version machinery duplicates a layer the engine owns.
- **Eight full encoded keys per datom** through the whole LSM lifecycle — WAL,
  memtable, every level it passes through.

## The design

### Page = node, encoding at the page boundary

A disk page cannot hold Go pointers, so interned `Keyword`/`Identity` die at
the file boundary. On-disk nodes hold encoded datoms; the typed form lives in a
bounded cache of decoded nodes. This is the parent document's rule — encoding
stays at the boundaries — with the page as a boundary. What changes against
Badger is the amortization: decode happens per node (~256 datoms) on cache
miss, not per key on every read, and the hot working set stays typed with
interned pointers, which is where the checkpoint numbers come from.
Decode-on-fault also concentrates load on the intern tables — every cache
miss re-interns a node's worth of E/A/V — which
[INTERN_SPECIALIZATION.md](INTERN_SPECIALIZATION.md) prices and specializes.

The page encoding is new format surface, so it starts clean:
`BUG_V_PAYLOAD_NOT_PREFIX_FREE` does not get imported into it. An
order-preserving escape of the V payload from birth means the typed comparator
and the page order agree everywhere, and `runMembership` has no counterpart
here at all. The header also records the attribute width the file was written
with (`MaxAttributeBytes` today), so a future width change is a readable
format version rather than a misparse.

### The arena: pages in one linear byte region

Encoded pages need no individual allocations: they live in one linear byte
arena, addressed by offset. Three properties fall out, and each is a reason on
its own.

**The store leaves the garbage collector's world.** An offset arena is `[]byte`
— pointer-free memory Go's collector neither scans nor moves. The datom-slab
work cut the resting store's object count; the arena cuts the bytes the
collector walks to approximately zero for the store proper, leaving only the
bounded decoded-node cache GC-visible. Mark cost becomes O(cache) rather than
O(store), for the store's whole lifetime — the difference that matters most
under wasm's single-threaded collector. A pointer-holding arena was measured
and declined (`PERFORMANCE_STATUS.md`, 2026-08-02): Go slices carved from
shared backing arrays strand live pointers under a cap-walk and buy little
while the datoms stay GC-visible. The hazard and the smallness were both
properties of the half-measure; an offset arena has no pointers to strand.

**The arena is the file.** Pages append at the arena's tail; commit flushes the
tail range and flips the meta slot. Natively that is `pwrite` and fsync of
arena ranges. Under wasm the arena is a contiguous region of linear memory, so
the OPFS adapter degenerates to persisting byte ranges, and handing the store
to a JS host is handing an `ArrayBuffer` view — working form, durable form,
and host-visible form are one representation, with no export step between
them. JDZL remains the compressed portable interchange above it.

**Offsets are 32-bit where it counts.** Any arena wasm32 can address is
reachable by a 4-byte offset. This is the parent document's "32-bit handle
representation" — the one it distinguished from slabs because handles leave
pointer semantics — arriving as the natural consequence of nodes that are
already encoded pages rather than as a separate memory project.

The cost the arena adds beyond the page design's own: offset arithmetic
corrupts silently where a bad pointer faults, which raises the value of the
differential harness another notch, and the arena needs a growth policy —
chunked regions or one reservation, a measurement question.

### Commit is the memory commit plus fsync

The write path is `versionBuilder` as it exists: CoW up the touched paths,
publish by root swap. Persistence adds: serialize the batch's new nodes to
pages appended to the file, fsync, write the new root address into the older of
two meta slots, fsync. LMDB's meta ping-pong, with `versionHolder.publish` as
the in-memory half it already performs. Commit latency's floor becomes fsync
policy — group commit is a policy knob, not a structural change — where today
it is Badger's WAL plus its own sync policy.

### Concurrency transfers unchanged

Single writer, lock-free readers on retained roots — `versionHolder`'s contract
is LMDB's contract. A read session retains a root and therefore the pages that
root reaches; sessions already carry exactly this semantic in memory.

### No freelist: reclaim by rewrite

The file is append-only. Garbage accrues only from superseded branch pages and
abandoned batches — bounded, because the datoms themselves are never
superseded. Instead of a free-page list (LMDB's most intricate component),
reclaim by rewriting: walk EAVT, bulk-build a fresh file through the
order-derivation lattice, swap roots. That subroutine is built and measured —
~3 s for 2.7M datoms — and it is the same philosophy truncate already follows:
rebuild from what remains. Compaction becomes an explicit, occasional,
linear-time operation instead of a continuous background one.

### Lazy subtree loading is the page cache

The parent document's "lazy subtree loading, later" section is this design's
read path: a branch child slot holds an address, a pointer, or both; loads go
through a bounded strong-pointer cache with explicit eviction (not weak
pointers — a Go weak pointer clears at the next GC and converts a cache into a
guaranteed miss). The same mechanism with a different byte sink is the wasm
OPFS/IndexedDB persistence adapter from `TODO.md`: native durability and
browser persistence are one design, which the pure-Go implementation keeps
portable — no mmap dependency, which wasm could not honor anyway.

### What stays

- **The blob tier.** Pages have no 64 KB key ceiling, but large values still do
  not belong inline in searched pages; the content-addressed side store keeps
  its role.
- **JDZL.** Already the backup, migration, and interchange format; a store file
  is a cache of one version's reachable state, and the dump remains the
  portable truth.
- **The `Store` seam.** This lands as a fourth registered backend beside
  Badger, not a cutover. The default moves on measurements or does not move —
  the memory-trees precedent.

## What does not transfer

- **LMDB/Bolt store opaque KV in one tree.** Eight orders sharing one version
  value, a domain-aware comparator, and CRDT resolution as index order have no
  counterpart there; the architecture transfers, the payload semantics do not.
- **mmap-centric reading.** LMDB's read path is the page table. Go's GC and the
  wasm target both argue for explicit reads into the decoded-node cache;
  whether native builds additionally mmap is a measurement question, not a
  design commitment.
- **Badger's hardening.** A decade of production against lying fsyncs, torn
  writes, and filesystem quirks does not transfer. CoW removes the largest
  class structurally — no live page is ever rewritten — but file-format bugs
  eat data, and that risk is the honest price. The mitigation is the position
  this store would be born into: the backend contract suite derives cases from
  `AvailableBackends()`, the optimizer×backend matrix runs every
  query-executing test against every store, and JDZL round-trips pin the
  boundary — three existing implementations acting as oracles for the fourth.

## Open questions

- **Eight trees of full datoms on disk, or a record heap plus reference
  trees.** Badger stores eight full keys today, so eight full trees is parity —
  but the arena tilts this: a heap of encoded datoms plus eight trees of 4-byte
  offsets lands near the typed store's footprint while staying GC-invisible,
  where eight full encoded trees roughly quintuple it. Record-heap-plus-offsets
  is the indicated layout; sizing it is still the first measurement task.
- **Page size versus branching factor** — 256 datoms per node was tuned for
  memory; a page wants alignment with the storage stack.
- **Group-commit policy** and its latency/durability dial.
- **Decoded-node cache sizing and eviction.**
- **Where the format version lives** and whether this rides or follows the
  [TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md) break — the same train
  the V-payload escape is already a candidate for.
- **Whether [PRESORTED_INDEX_SECTIONS.md](PRESORTED_INDEX_SECTIONS.md) is
  subsumed**: a page file in typed order is presorted sections with a branch
  overlay, so hydration from the store file may make wire-format presorting
  moot.

## Relationship to other proposals

[MEMORY_DATOM_INDEXES.md](MEMORY_DATOM_INDEXES.md) is the parent and carries
the representation argument; this document assumes it. Durable roots make
[BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md) durable for free — a
branch is a retained root on disk — and fixed-basis temporal reads become root
retention with a file behind them, which bears on
[TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md) PR 4 the same way the
memory version already does.
