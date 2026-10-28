# Pre-sorted index sections in JDZL

**Scope.** One question: a reader hydrating a store from a dump re-derives all eight index orders by sorting. The writer already had them. Should the dump carry them?

**Not in scope.** Merge and replication semantics. Those are covered, and covered better, by [TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md) §_Distributed contract_, §_Merging independent databases_, §_EDN, JDZL, and replication_, and by [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md) §6–7. See _What this does not decide_ before adding anything about merge here.

**Format authority:** [BINARY_EXPORT.md](../reference/BINARY_EXPORT.md) for JDZL, [EXPORT_IMPORT.md](../reference/EXPORT_IMPORT.md) for EDN. Anything adopted here changes those.

**Builds on:** [MEMORY_DATOM_INDEXES.md](MEMORY_DATOM_INDEXES.md), whose typed trees and order-derivation lattice this accelerates on the bulk-build path.

---

## Motivation: typed trees fix resting size, not hydration

Hydrating the 2.7M-datom production dump into `BadgerEmulatorStore` retains more than the 4 GiB wasm linear-memory ceiling holds, 4,380 MB on the 62 MB import (`PERFORMANCE_STATUS.md`, the cross-backend import table). `MemoryTreeStore` is the js/wasm default, and it retains 608 MB on the same import, 590 MB with datom slabs. The reduction is representational. The dominant term removed is `8 × (20+32+16) = 544` bytes per datom of entity, attribute and transaction re-serialized into eight separate byte keys. Under typed trees the entity and attribute are interned pointers shared across the store, and the transaction lives once in the record. The eight map slots and the parallel B-tree of key strings go with it.

The bar there is a threshold rather than a ratio. `MEMORY_DATOM_INDEXES.md` §_Representation arithmetic_ states it as a permitted datom count. What matters here is that resting size clearing the bar is **necessary and not sufficient**, because hydration has a peak of its own.

The cost of getting there is measured. The 62 MB import takes 8.3 s native and 27.7 s under js/wasm (`PERFORMANCE_STATUS.md`, the bulk-import entry). The order-derivation lattice derives each index's order from one already sorted, and after it sort machinery is about 27% of import CPU, down from about 50% (the lattice entry there). Linear memory never shrinks, so a slow hydrate is also a peak-memory hazard for every transient it allocates along the way. The first-order cost is wall time, and on a page load that is a product problem in its own right.

---

## What the format already gives us

From `BINARY_EXPORT.md`, header and trailer layouts cross-checked against `datalog/storage/export_bin.go`:

- JDZL is **the complete EAVT datom log**, so the base order this proposal builds on is already what the writer produces.
- 32-byte header with magic, format version `1`, **two reserved flag bytes at offset 5**, and a trailer offset. Unknown versions must be rejected.
- Chunks are independently LZJ-compressed and close **at the next entity boundary** past the soft budget.
- The `JDZI` trailer holds `chunk_count`, `max_lamport`, `max_replica`, then 56-byte entries of `{file_offset, cmp_len, unc_len, first_E, last_E}`.

Three of these are affordances and one is a constraint. The base is already sorted. The container already has a versioned trailer-section mechanism and spare header flag bits, so nothing here needs a new format. The constraint is that **chunk order is EAVT order**, see below.

---

## The idea

The writer, on an unconstrained machine, already holds all eight orders in its trees. Rather than shipping only the EAVT stream and making every reader re-derive the other seven, ship the orderings as permutations over the base array. The reader fills an index array from the wire instead of sorting.

### What it buys, and what it does not

It buys CPU on the ordering work that remains after the lattice: for an order the lattice sorts, N × log₂N comparisons become N. The linear term is verification, not a bare copy, see below.

It buys **no peak memory**. Bulk-building a tree in some order requires that order materialized as an index array of N entries, and the array costs the same whether a sort or a read filled it. Building one tree at a time and releasing each array before the next holds one N-sized array live either way.

Worth stating flatly, because the motivating constraint is a memory ceiling: **pre-sorted sections are a hydration-time optimization and do not move the ceiling.**

### Which orders need a stored permutation

The order-derivation lattice in `MEMORY_DATOM_INDEXES.md` derives AEVT and AETV from EAVT and EATV by a stable bucket pass with no datom comparisons, and EATV by a per-`(E, A)` group resort. The rows below agree with it.

With EAVT as the base:

| Order | Derivation from the base | Stored? |
| --- | --- | --- |
| AEVT (A,E,V,T) | Stable counting sort by attribute. Within a bucket the EAVT elements retain E, then V, then T ascending, which is AEVT. O(N) with a counts array sized by distinct attributes, of which there are schema-few. | No |
| EATV (E,A,T↓,V) | Group-local reorder inside each (E,A) run: V,T becomes T↓,V. Groups are the write history of one entity-attribute pair. | No |
| AETV (A,E,T↓,V) | Counting sort by attribute of EATV. | No, given EATV |
| ATEV (A,T↓,E,V) | A real sort within each attribute bucket. Cheaper than global; bad for a skewed attribute. | Probably |
| AVET (A,V,E,T) | A real sort within each attribute bucket. | Probably |
| VAET (V,A,E,T) | A k-way merge of AVET's per-attribute runs, k = distinct attributes. | Yes |
| TAEV (T,A,E,V) | A global sort on the transaction component. The clock is a Lamport counter, and a single-replica append-only dump is plausibly already monotone in it. Worth checking rather than assuming. | Unclear |

So the stored set is plausibly three, not seven. At 4 bytes per index and 2.7M datoms that is roughly **32 MB rather than 76 MB** before compression, and permutations between correlated orders delta-encode into very little, a shape LZJ already handles.

### Verify, don't trust

A stored permutation is **ordering information living in the data**. The encoder and the comparator are two projections of one order that must agree forever (`MEMORY_DATOM_INDEXES.md`, _Comparators compare storage projections_). A stored permutation is a third projection, baked into artifacts that outlive the code.

If the ordering specification ever changes, an old dump builds its trees in the _old_ order, and every seek then returns wrong results. Not a crash: silent wrong answers, which is what that document's comparator section is written to prevent.

The rule: **as the reader walks a stored permutation, check that each consecutive pair is non-decreasing under the current comparator.** N comparisons per order instead of N log N, so roughly 95% of the win survives. A stale permutation becomes a loud error at import rather than a wrong answer at query time. Because the permutation is then a _hint_, falling back to a sort is always available.

**Which comparator, though? There are two.** `BUG_V_PAYLOAD_NOT_PREFIX_FREE`: an encoded value carries no length or terminator, so wherever a component follows V the key's byte order for prefix-related payloads is decided by the following component's first byte, not by the values. Badger's keys and the memory trees therefore disagree on exactly those pairs, and neither order is reachable from the other without re-sorting.

So a permutation is sorted **in the typed comparator's order**, the order the trees it builds are in, and the verification walk uses that same comparator. A permutation derived from Badger's key order would fail that walk on the first prefix-related pair, which is the correct outcome: it is not the order the reader is building.

That makes the sections tree-shaped rather than key-shaped, and it means a writer must produce them from the typed order even when its own storage is Badger. If the bug is fixed at the `TRANSACTION_ENVELOPES.md` break, the two orders converge and the distinction stops mattering. Sections written before that point are in the typed order, and the format version has to be able to say so.

### Chunk boundaries are EAVT-shaped

Chunks close at entity boundaries and the trailer records first and last entity per chunk, so chunk order _is_ EAVT order, and each chunk is independently compressed. For any other order a permutation jumps arbitrarily across chunks, so walking one directly would mean repeatedly decompressing chunks as the walk bounces between them.

This never needs to happen: decompress the EAVT stream once into the resident datom slab, then every permutation fills an index array over datoms already in memory. Peak stays at datoms + one index array + one chunk buffer + the tree under construction.

But the limit is a property that constrains anything built later:

> **Permutations accelerate building trees from a resident datom set. They do not let a non-primary order be streamed out of a compressed file.**

Partial import, attribute-scoped restore, and lazy subtree paging from OPFS all want the latter, and all would need chunks organized by the order in question, a different format decision, not an extension of this one, and one `TRANSACTION_ENVELOPES.md` already cautions against (below).

### Optional sections, not mandatory ones

Today JDZL is a portable datom stream. Per-index permutations move it toward being an image of _this engine's_ index set, and the comparator rule fixes eight orders as of now. If that set ever changes, old dumps carry the wrong number of sections.

This is the `pg_dump`-versus-physical-backup distinction, and the standard resolution is to support both. The existing trailer is the precedent: **an optional section, used when present, valid and version-matched; ignored otherwise**, announced by one of the two reserved header flag bytes so a reader knows without seeking.

Optionality is load-bearing rather than polite. A small incremental dump, the resync case in `TRANSACTION_ENVELOPES.md`, has nothing to gain: at a few hundred datoms, sorting is free while seven sections plus headers is mostly overhead. A mandatory-section design would have to be right for a case that does not want it.

One existing guarantee to preserve: `BINARY_EXPORT.md` promises _deterministic EAVT order for byte-stable re-export when the soft budget and encoder settings match_. Permutation sections must be deterministic under the same conditions, or that guarantee narrows and should be restated.

---

## Interaction with the transaction-envelope work

This is the part that must not be designed in isolation. `TRANSACTION_ENVELOPES.md` §_EDN, JDZL, and replication_ already commits to:

- **JDZL bumps its format version** and **requires a transaction-record section**, while retaining EAVT/entity-aligned datom chunks with _"no datom record or index ordering changes."_
- **Two-phase import into a fresh, exclusively held database**: all datom chunks, then all transaction records, then digest validation, then an import-complete marker. The database refuses queries and writes until the marker is present, and a failed import is discarded, never repaired.
- **Byte-stable export** extended to transaction records.
- A standing instruction: _"Do not reorder JDZL into transaction-first datom chunks until compression, parallel import, random-access, and CRDT-index implications are separately measured and designed."_

Three consequences:

1. **There is one version bump available, not two.** Permutation sections and the transaction-record section should land in the same format version, or the second re-versions a format that just changed. If envelopes go first, this proposal targets _their_ version.
2. **Permutation sections are compatible with "no index ordering changes"**: they record the existing orders rather than altering them. But the standing instruction's posture is the right one: this is a JDZL restructuring and it wants the same measurement bar.
3. **The two-phase import model is where these sections plug in.** Permutations are consumed during the datom-chunk phase, before transaction records and before the digest pass. Nothing about them should reach the marker or the digest.

---

## What this does not decide

Recorded so a future reader does not re-derive it, and does not put it here.

**Merge, replication, and delta sync are settled elsewhere, and are ahead of anything a format discussion produces.** `TRANSACTION_ENVELOPES.md` §_Merging independent databases_ specifies the disjoint-ReplicaID preconditions, including the shared-ID-with-identical-overlap resync case, proven by envelope digest, the `ParentBasis` widening from scalar to frontier that keeps AsOf closed over what it originally saw, `TransactionsAfter` for incremental resync, clock `Receive` on absorption, and the ping-pong argument against automatic merge markers. It also already records that _"between independent clocks, the LWW winner is deterministic but semantically arbitrary — Lamport magnitude reflects write count, not wall-clock recency."_

`BRANCHING_AND_SNAPSHOTS.md` §6 adds the per-cardinality convergence table, semantic clobbering of additive updates, cross-attribute emergent states, and the causal-closure argument: **cherry-picking an RGA insert without its predecessor leaves `AfterRef` dangling**, so any partial extract must be causally closed or restricted. §7 further establishes that uniqueness is a read-time `(A,V)`-LWW resolution rather than a write-time invariant, so a merge resolves it exactly as concurrent writers already do, explicitly _not_ a new merge hazard.

Two traps for anyone extending this document toward merge:

- **Schema is deliberately not carried.** Envelopes assign schema agreement to the caller and call conflicting cardinality or type _semantically undefined_. Proposing to put schema in the dump overturns a stated decision; that argument belongs there, not here.
- **A cutoff-based delta is a cherry-pick** unless causally closed, per the RGA argument above. `DatomsAfter` is a store primitive, not a sanctioned delta boundary; the sanctioned one is `TransactionsAfter` over whole envelopes.

---

## What decides it

- **The win that remains.** Sort machinery is about 27% of import CPU after the lattice (`PERFORMANCE_STATUS.md`, the lattice entry), and three of the seven derived orders cost no comparisons. A stored permutation can remove only the sorts the lattice still runs: the per-attribute resorts of AVET and ATEV, VAET's merge and TAEV's sort. A profile of an import under the lattice, on the orders the table marks stored, decides whether that justifies touching the format at all.
- **The version bump.** Permutation sections ride the envelope revision's bump or take their own. Riding it costs sequencing behind that work. A bump of its own re-versions a format the envelope revision changes again.
- **The byte-stable guarantee.** Permutation sections are deterministic under the conditions `BINARY_EXPORT.md` names, or the guarantee is restated to exclude them.
