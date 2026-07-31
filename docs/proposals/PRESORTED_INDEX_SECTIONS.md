# Pre-sorted index sections in JDZL

**Status:** Proposal. Nothing implemented, nothing measured. Derived in discussion 2026-07-31.

**Scope.** One question: a reader hydrating a store from a dump re-derives all eight index orders by sorting. The writer already had them. Should the dump carry them?

**Not in scope.** Merge and replication semantics. Those are covered, and covered better, by [TRANSACTION_ENVELOPES.md](TRANSACTION_ENVELOPES.md) §*Distributed contract*, §*Merging independent databases*, §*EDN, JDZL, and replication*, and by [BRANCHING_AND_SNAPSHOTS.md](BRANCHING_AND_SNAPSHOTS.md) §6–7. See *What this does not decide* before adding anything about merge here.

**Format authority:** [BINARY_EXPORT.md](../reference/BINARY_EXPORT.md) for JDZL, [EXPORT_IMPORT.md](../reference/EXPORT_IMPORT.md) for EDN. Anything adopted here changes those.

**Depends on:** [MEMORY_DATOM_INDEXES.md](MEMORY_DATOM_INDEXES.md) PR B — this accelerates its bulk-build path and is pointless without it.

---

## Motivation: PR B fixes resting size, not hydration

A wasm deployment reaches the 4 GiB linear-memory ceiling hydrating a production dump of roughly 2.7 million datoms. `MemoryStore` is the wasm build's only backend, so this is the wasm engine's memory profile.

PR B's typed trees address the **resting** side, and address it without arenas. Using `MEMORY_DATOM_INDEXES.md`'s structural model at 2.7M datoms:

| | B/datom | total |
|---|---:|---:|
| Current representation | 1,296–1,424 | 3.5–3.9 GB |
| Typed trees, bulk-built | 145–161 | 390–435 MB |
| Typed trees, incrementally inserted at low branching | 186–218 | 500–590 MB |

The reduction is representational. The dominant term removed is `8 × (20+32+16) = 544` bytes per datom of entity, attribute and transaction *re-serialized into eight separate byte keys*; under typed trees the entity and attribute are interned pointers shared across the store and the transaction lives once in the record. The eight map slots and the parallel B-tree of key strings go with it.

The bar there is a threshold rather than a ratio — the current representation sits at 82–90% of a ceiling that never shrinks, and every projected configuration lands the resting store in the hundreds of megabytes. `MEMORY_DATOM_INDEXES.md` §*Representation arithmetic* states it as a permitted datom count. What matters here is that resting size clearing the bar is **necessary and not sufficient**, because the incident was a hydration peak.

What is **not** addressed is the cost of getting there. Building eight orders from an EAVT stream is roughly **463 million multi-component comparisons** (8 × N × log₂N at N = 2.7M). Linear memory never shrinks, so a slow hydrate is also a peak-memory hazard for every transient it allocates along the way — but the first-order cost is wall time, and on a page load that is a product problem in its own right.

**Nothing above is measured.** `BenchmarkMemoryAssertBulk` exists at `datalog/storage/memory_backend_bench_test.go:266` with Badger mirrors, so the current-side figure is one run from confirmation or falsification. Until then it is a structural model with stated assumptions, and any sizing claim downstream should state its N, its host, and the dump commit.

---

## What the format already gives us

From `BINARY_EXPORT.md`, header and trailer layouts cross-checked against `datalog/storage/export_bin.go`:

- JDZL is **the complete EAVT datom log** — so the base order this proposal builds on is already what the writer produces.
- 32-byte header with magic, format version `1`, **two reserved flag bytes at offset 5**, and a trailer offset. Unknown versions must be rejected.
- Chunks are independently LZJ-compressed and close **at the next entity boundary** past the soft budget.
- The `JDZI` trailer holds `chunk_count`, `max_lamport`, `max_replica`, then 56-byte entries of `{file_offset, cmp_len, unc_len, first_E, last_E}`.

Three of these are affordances and one is a constraint. The base is already sorted; the container already has a versioned trailer-section mechanism and spare header flag bits, so nothing here needs a new format. The constraint is that **chunk order is EAVT order** — see below.

---

## The idea

The writer, on an unconstrained machine, already holds all eight orders in its trees. Rather than shipping only the EAVT stream and making every reader re-derive the other seven, ship the orderings as permutations over the base array. The reader fills an index array from the wire instead of sorting.

### What it buys, and what it does not

It buys CPU at roughly **21×** on the ordering work: 8 × N × log₂N comparisons become 8 × N. The linear term is verification, not a bare copy — see below.

It buys **no peak memory**. Bulk-building a tree in some order requires that order materialized as an index array of N entries, and the array costs the same whether a sort or a read filled it. Building one tree at a time and releasing each array before the next holds one N-sized array live either way.

Worth stating flatly, because the motivating incident was a memory ceiling: **pre-sorted sections are a hydration-time optimization and do not move the ceiling.**

### Which orders actually need a stored permutation

**Derived from the index component orders; not verified against `datalog/storage/key_encoder_binary.go`. Check before relying on it.**

With EAVT as the base:

| Order | Derivation from the base | Stored? |
|---|---|---|
| AEVT (A,E,V,T) | Stable counting sort by attribute. Within a bucket the EAVT elements retain E, then V, then T ascending — exactly AEVT. O(N) with a counts array sized by distinct attributes, of which there are schema-few. | No |
| EATV (E,A,T↓,V) | Group-local reorder inside each (E,A) run: V,T becomes T↓,V. Groups are the write history of one entity-attribute pair. | No |
| AETV (A,E,T↓,V) | Counting sort by attribute of EATV. | No, given EATV |
| ATEV (A,T↓,E,V) | A real sort within each attribute bucket. Cheaper than global; bad for a skewed attribute. | Probably |
| AVET (A,V,E,T) | A real sort within each attribute bucket. | Probably |
| VAET (V,A,E,T) | A global sort on the value component. | Yes |
| TAEV (T,A,E,V) | A global sort on the transaction component — except the clock is a Lamport counter and a single-replica append-only dump is plausibly already monotone in it. Worth checking rather than assuming. | Unclear |

So the stored set is plausibly three, not seven. At 4 bytes per index and 2.7M datoms that is roughly **32 MB rather than 76 MB** before compression, and permutations between correlated orders delta-encode into very little — a shape LZJ already handles.

### Verify, don't trust

A stored permutation is **ordering information living in the data**. Ruling 3 of `MEMORY_DATOM_INDEXES.md` exists precisely because the encoder and the comparator are two projections of one order that must agree forever. A stored permutation is a third projection, baked into artifacts that outlive the code.

If the ordering specification ever changes, an old dump builds its trees in the *old* order, and every seek then returns wrong results. Not a crash — silent wrong answers, which is what that document's design note on comparators is written to prevent.

The rule: **as the reader walks a stored permutation, check that each consecutive pair is non-decreasing under the current comparator.** N comparisons per order instead of N log N, so roughly 95% of the win survives; a stale permutation becomes a loud error at import rather than a wrong answer at query time; and because the permutation is then a *hint*, falling back to a sort is always available.

### Chunk boundaries are EAVT-shaped

Chunks close at entity boundaries and the trailer records first and last entity per chunk, so chunk order *is* EAVT order, and each chunk is independently compressed. For any other order a permutation jumps arbitrarily across chunks, so walking one directly would mean repeatedly decompressing chunks as the walk bounces between them.

This never needs to happen: decompress the EAVT stream once into the resident datom slab, then every permutation fills an index array over datoms already in memory. Peak stays at datoms + one index array + one chunk buffer + the tree under construction.

But the limit is a property that constrains anything built later:

> **Permutations accelerate building trees from a resident datom set. They do not let a non-primary order be streamed out of a compressed file.**

Partial import, attribute-scoped restore, and lazy subtree paging from OPFS all want the latter, and all would need chunks organized by the order in question — a different format decision, not an extension of this one, and one `TRANSACTION_ENVELOPES.md` already cautions against (below).

### Optional sections, not mandatory ones

Today JDZL is a portable datom stream. Per-index permutations move it toward being an image of *this engine's* index set, and Ruling 3 fixes eight orders as of now — if that set ever changes, old dumps carry the wrong number of sections.

This is the `pg_dump`-versus-physical-backup distinction, and the standard resolution is to support both. The existing trailer is the precedent: **an optional section, used when present, valid and version-matched; ignored otherwise**, announced by one of the two reserved header flag bytes so a reader knows without seeking.

Optionality is load-bearing rather than polite. A small incremental dump — the resync case in `TRANSACTION_ENVELOPES.md` — has nothing to gain: at a few hundred datoms, sorting is free while seven sections plus headers is mostly overhead. A mandatory-section design would have to be right for a case that does not want it.

One existing guarantee to preserve: `BINARY_EXPORT.md` promises *deterministic EAVT order for byte-stable re-export when the soft budget and encoder settings match*. Permutation sections must be deterministic under the same conditions, or that guarantee narrows and should be restated.

---

## Interaction with the transaction-envelope work

This is the part that must not be designed in isolation. `TRANSACTION_ENVELOPES.md` §*EDN, JDZL, and replication* already commits to:

- **JDZL bumps its format version** and **requires a transaction-record section**, while retaining EAVT/entity-aligned datom chunks with *"no datom record or index ordering changes."*
- **Two-phase import into a fresh, exclusively held database**: all datom chunks, then all transaction records, then digest validation, then an import-complete marker. The database refuses queries and writes until the marker is present, and a failed import is discarded, never repaired.
- **Byte-stable export** extended to transaction records.
- A standing instruction: *"Do not reorder JDZL into transaction-first datom chunks until compression, parallel import, random-access, and CRDT-index implications are separately measured and designed."*

Three consequences:

1. **There is one version bump available, not two.** Permutation sections and the transaction-record section should land in the same format version, or the second re-versions a format that just changed. If envelopes go first, this proposal targets *their* version.
2. **Permutation sections are compatible with "no index ordering changes"** — they record the existing orders rather than altering them. But the standing instruction's posture is the right one: this is a JDZL restructuring and it wants the same measurement bar.
3. **The two-phase import model is where these sections plug in.** Permutations are consumed during the datom-chunk phase, before transaction records and before the digest pass. Nothing about them should reach the marker or the digest.

---

## What this does not decide

Recorded so a future reader does not re-derive it, and does not put it here.

**Merge, replication, and delta sync are settled elsewhere, and are ahead of anything a format discussion produces.** `TRANSACTION_ENVELOPES.md` §*Merging independent databases* specifies the disjoint-ReplicaID preconditions — including the shared-ID-with-identical-overlap resync case, proven by envelope digest — the `ParentBasis` widening from scalar to frontier that keeps AsOf closed over what it originally saw, `TransactionsAfter` for incremental resync, clock `Receive` on absorption, and the ping-pong argument against automatic merge markers. It also already records that *"between independent clocks, the LWW winner is deterministic but semantically arbitrary — Lamport magnitude reflects write count, not wall-clock recency."*

`BRANCHING_AND_SNAPSHOTS.md` §6 adds the per-cardinality convergence table, semantic clobbering of additive updates, cross-attribute emergent states, and the causal-closure argument: **cherry-picking an RGA insert without its predecessor leaves `AfterRef` dangling**, so any partial extract must be causally closed or restricted. §7 further establishes that uniqueness is a read-time `(A,V)`-LWW resolution rather than a write-time invariant, so a merge resolves it exactly as concurrent writers already do — explicitly *not* a new merge hazard.

Two traps for anyone extending this document toward merge:

- **Schema is deliberately not carried.** Envelopes assign schema agreement to the caller and call conflicting cardinality or type *semantically undefined*. Proposing to put schema in the dump overturns a stated decision; that argument belongs there, not here.
- **A cutoff-based delta is a cherry-pick** unless causally closed, per the RGA argument above. `DatomsAfter` is a store primitive, not a sanctioned delta boundary; the sanctioned one is `TransactionsAfter` over whole envelopes.

---

## Evidence

**Sourced from the reference documents:** the format facts in *What the format already gives us*; the import model and standing instruction in *Interaction with the transaction-envelope work*. From `BINARY_EXPORT.md` and `TRANSACTION_ENVELOPES.md`, with header and trailer layouts cross-checked against `datalog/storage/export_bin.go`.

**Verified against the tree (2026-07-31):** the existence of `BenchmarkMemoryAssertBulk`.

**Derived, not verified:** the per-order derivation table. Check against `datalog/storage/key_encoder_binary.go`.

**Modeled, not measured:** every figure in *Motivation*, taken from `MEMORY_DATOM_INDEXES.md`'s structural model, whose own status says nothing is measured. The comparison counts are arithmetic over a stated N, not timings.

## Open questions

1. **Is the CPU win real?** This rests entirely on the unmeasured claim that ordering dominates hydration. A profile of an import — not a model — decides whether it justifies touching the format at all. Everything else here is contingent on that answer.
2. **Which orders genuinely need stored permutations**, once the derivation table is checked. If the answer is one or two, the proposal shrinks accordingly.
3. **Does this ride the envelope version bump or take its own?** A sequencing question for the owner.
4. **Do permutation sections survive the byte-stable re-export guarantee**, or does that guarantee need restating?
