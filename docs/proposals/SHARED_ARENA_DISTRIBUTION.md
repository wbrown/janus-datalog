# Distributed Execution via Interchangeable `PatternMatcher` Sources over a Shared Arena

**Status:** Proposal
**Author:** wbrown (design), drafted with Claude
**Date:** 2026-06-24
**Relationship:** Orthogonal and complementary to [DISTRIBUTED_JANUS.md](DISTRIBUTED_JANUS.md).
That proposal distributes the **storage/write plane** (content-addressed datom
sharding, Raft replication, scatter-gather). This one distributes the
**read/execution plane**: many processes and hosts run full query executors
locally, reading one owner's state through `PatternMatcher` sources, where the
data crosses the boundary as a shared, versioned, off-heap memory **arena**. The
two axes compose — a shard from the other proposal can expose its state as an
arena under this one.

---

## Abstract

Janus is an embedded library: one process opens the BadgerDB directory (a single
writer lock), and the consistent EA cache that makes queries cheap is an
**in-process** structure. The design philosophy — *lean on Janus rather than hold
application-side state, because Janus is fast and its caches are consistent* —
therefore stops at the process boundary.

This proposal removes that wall while keeping the consistent cache, the query
results, and a uniform interface. Reader processes run their *own* full
parser → planner → executor. The boundary they cross is the `PatternMatcher`
seam: the existing matcher, bound to a memory-mapped/RDMA **arena** backing
instead of a local BadgerDB backing, presents itself in the `SourceRouter` as
**one more source, indistinguishable from any other.** The arena carries the
owner's resolved cache *and* all eight raw index orderings, so the arena-backed
source is a **complete** matcher — every query shape, all eight indexes, no
reduced capability. That completeness is the invariant §3 depends on: because
Datalog sources *compose* at the
`PatternMatcher` level (overlays, sibling sources, cross-host federation), every
source must present *precisely the same interface* or the composition breaks.

We prove the arena-backed source is observationally indistinguishable from a
local Badger source at the matcher interface (Theorem 1 — the formal statement of
"no special snowflake"), that reads are snapshot-consistent (Theorem 3), and that
local overlays give correct read-your-writes and speculative semantics
(Theorem 4). We then examine Janus as a true distributed database: its CAP
posture, the single-writer ↔ multi-writer-CRDT knob, failure modes, and scaling.

---

## 1. Motivation

### 1.1 The wall

- **BadgerDB is single-writer, single-process.** One process holds the directory
  lock. A second process can open read-only, but gets a snapshot frozen at open
  that never sees live writes.
- **The consistent cache is in-process.** The EA `Cache` (`storage/cache.go`) is
  owned by the `Database` and consulted *behind* the matcher
  (`m.cache.GetOrResolve` inside `BadgerMatcher`); the cache-commit atomicity
  protocol (in-flight sentinels, monotonic versions, v0.13.0) lives in
  `Transaction.Commit`. A second process opening read-only gets its own cold,
  snapshot-stale cache — defeating the property ("consistent caches") that makes
  leaning on Janus worthwhile.

"Lean on Janus" holds across goroutines and not across processes. The goal is to make it hold across processes and hosts **with the same
semantics, the same interfaces, and at memory speed.**

### 1.2 The workload that forces the issue

The hard case is not a client that issues a handful of queries per request, with
other work between them to absorb a round trip; an RPC query server would do for
that. The hard case is a **read-saturated client**: a dependency-graph resolver,
a planner, an indexer — code that issues *millions* of small point-lookup /
existence / narrow-scan queries **back-to-back, with nothing between them to
amortize per-query latency**. Such a loop, today, runs in-process at µs-scale
cache hits. Put any per-query round trip in front of it and the round-trip count,
not the work, sets the wall-clock. The only IPC tier that survives is one where a
query is a **memory read**: shared memory locally, RDMA remotely. That is the
design constraint.

---

## 2. What we deliberately do *not* build

- **RPC `Match` as the read path** — a round trip per pattern, called many times
  per query; the read-saturated loop cannot absorb it.
- **An RPC *fallback* for "hard" query shapes** — a *partial* matcher, which §3
  forbids. "All eight indexes" closes the capability gap, so no shape needs
  server-side execution.
- **Read-only Badger multi-open** — cold, snapshot-stale per-process cache; no
  live writes. Defeats the motivating property.
- **Per-query UDS daemon** — fine for query-sparse clients; the round trips
  dominate for query-saturated ones.

The data planes are arena memory reads (§5–6) and the shared-memory submission
ring (§8.2). RPC/UDS survives only as a **control plane** — connection bootstrap,
lease negotiation — never as a read path, a write path, or a capability fallback.

---

## 3. The invariant: substitutability ("no special snowflake")

> **Every source presents *precisely* the same `PatternMatcher` contract, with
> the same capabilities. The arena-backed source is a complete matcher — all
> eight index orderings, the full companion contract, every query shape — and is
> observationally indistinguishable from a local Badger source. Nothing above the
> matcher may branch on the backend.**

This is forced by *composition*.
Datalog sources compose at the `PatternMatcher` level: the `SourceRouter` is
`map[Symbol]PatternMatcher`, and the executor joins across sources, layers
overlays, and federates hosts **by treating each source as an interchangeable
matcher** (§7). A source that is *reduced* (point-lookups only), *special*
(a distinct API), or *partial* (RPC fallback for some shapes) cannot participate
uniformly — the overlay can't layer on it, the join can't span it, the router
can't route to it without a special case. Uniformity at this interface is what makes the system compose.

Two distinct levels follow; keep them separate:

- **Composition seam = `PatternMatcher`.** Where sources plug into the
  `SourceRouter` and compose. Every source — local Badger, remote arena, overlay,
  in-memory, a sibling host's arena — presents this identically.
- **Backing-access interface = *below* the matcher (internal).** How the *single*
  matcher implementation obtains bytes — indexed scans, value fetch, resolved-cache
  access, attribute high-water marks — from either a BadgerDB backing or an arena
  backing. This is an implementation detail that lets one matcher serve both
  backings (no parallel `ArenaMatcher`, per "Stop Creating V2 Versions"). **It is
  not the seam and does not replace `PatternMatcher` as the composition point.**

The matcher's dependency on `*BadgerStore` is narrowed to this backing-access
interface; Badger and the arena both implement it; the one matcher runs over
either. The arena-backed source is then *that same matcher* bound to an arena
backing, presented in the router as a `PatternMatcher` like any other.

---

## 4. Existing assets this builds on

This is mostly a problem of *exposure and layout*; the consistency model already exists:

- **MVCC / temporal core.** Append-only datoms; every write carries
  `ElementID{Lamport uint64, ReplicaID uint64}` (`datalog/element_id.go`) with a
  total order (`ElementID.Less`); `Database.AsOf(txID)` and the three-mode
  `*ElementID` matcher already read *as-of a basis*. **`AsOf` is the multi-reader
  snapshot API**, pointed at an arena instead of Badger.
- **Deterministic CRDT resolution.** LWW / add-wins / RGA are pure functions of
  the datom set under the `ElementID` order (Lemma R1) — locus-independent.
- **Eight indexes, sort-order-preserving L85 keys.** EAVT, EATV, AEVT, AETV,
  ATEV, AVET, VAET, TAEV; the E/A/Tx-keyed orderings use 69-byte fixed-stride keys
  (`E[20]+A[32]+Tx[16]+Op[1]`), lexicographic order = byte order. Sorted keys ⇒
  binary search ⇒ over RDMA, `O(log n)` *bounded* reads with no pointer chasing
  (the value-bearing orderings AVET/VAET embed the variable-length value and are
  binary-searched via an offset index). The storage layer already maintains
  exactly these eight orderings, so "all eight in the arena" reuses the existing
  index set.
- **Hash-addressed identities.** The 20-byte hash *is* the identity; the arena
  keys on hashes, so the key space is pointer-free by construction.
- **The cache + store split carries over directly.** In-process, the matcher
  consults the resolved EA cache (fast path) and falls to indexed store scans +
  CRDT resolution on miss / history / value-scan. The arena mirrors this exactly
  (§6): a resolved cache *and* the eight raw index orderings.
- **The cache-commit atomicity protocol** (v0.13.0). The writer-side correctness
  proof we port to the arena's seqlock.

---

## 5. The seam in practice

The executor calls `PatternMatcher.Match` (`executor/interfaces.go:10`) and its
companions — `PredicateAwareMatcher.MatchWithConstraints`,
`EntityLookupMatcher.LookupAttribute`, `AttributeFetchFusable.CanFuseAttributeFetch`,
`EntityPrefetcher.PrefetchEntities`. **The arena-backed source implements every
one of these, identically to the Badger-backed source, because it is the same
matcher over a different backing.** There is no arena-specific interface and no
shape it cannot serve:

- Current-state point lookups / `LookupAttribute` / get-else / fused fetch → the
  arena's resolved cache (one bounded read; one-sided RDMA remotely).
- History mode, value-position scans (VAET/AVET), as-of an older basis,
  cache-miss resolution → the arena's raw index orderings + the matcher's normal
  CRDT resolution.
- `PrefetchEntities` remains valid (it warms the reader's view); fusion remains
  valid (the matcher still owns schema + temporal mode).

```
            client / remote host                                         owner process
  ┌──────────────────────────────────────┐                       ┌────────────────────────────┐
  │ parser → planner → executor          │                       │ writer (single):           │
  │         │                            │                       │   Transaction.Commit       │
  │         ▼                            │                       │   CRDT resolve             │
  │ SourceRouter (each a PatternMatcher) │ ◀── reads (mmap/RDMA) │   maintain arena (W1)      │
  │   $       → Matcher[arena]           │                       │   publish watermark        │
  │   $local  → OverlayMatcher           │                       │                            │
  │   $peer   → Matcher[arena₂]          │                       │ ARENA (shared, off-heap):  │
  │   $mem    → MemoryPatternMatcher     │   writes (ring) ──▶   │   8 raw index orderings    │
  └──────────────────────────────────────┘                       │   resolved cache (seqlock) │
                                                                 │   value store + blobs      │
                                                                 │   symbol tables            │
                                                                 │   watermark W (atomic)     │
                                                                 └────────────────────────────┘
                                                                   durable: BadgerDB (truth)
```

Every entry in that router is a `PatternMatcher`. The executor cannot tell which
is local, remote, an overlay, or in-memory.

---

## 6. The arena

The arena mirrors the in-process `cache + store` pair, so the one matcher runs
over it unchanged.

### 6.1 Two structures, two concurrency disciplines

- **Eight raw index orderings — append-only immutable segments.** New writes
  flush as immutable, sorted segments (SST-like; fixed-stride for the E/A/Tx keys,
  offset-indexed where the value is in the key); readers
  binary-search within and merge across the segment set whose datoms are `⪯` the
  watermark. Immutable ⇒ **lock-free reads, no torn reads by construction.** These
  serve history, value-scans, as-of-older-basis, and cache-miss resolution.
- **Resolved cache — mutable, seqlock-versioned, owner-authoritative.** `(E,A) →
  (resolved value, lastTx)`, the arena form of `storage/cache.go`, updated by the
  owner on commit; per-entry seqlock (Theorem 2; over RDMA, Theorem 5); serves the
  current-state fast path. **Readers only ever *read* it** (a pure read plus the
  §6.2 basis check) — they never maintain their own resolved cache. This is required for
  correctness: a mutable resolved cache carries
  the cache-commit-atomicity invariants (no stale-read window, tombstone-aware
  LWW, freshness/invalidation, authoritative schema/cardinality), which were hard
  to get right in a *single* place (v0.13.0 stale-read window; the CardinalityOne
  tombstone gap; v0.13.1 schemaless cardinality collapse). Replicating them across
  N readers coordinated with a remote writer is the multiple-resolution-path
  hazard this codebase has repeatedly hit, so **resolution and its
  mutable cache have a single authority: the owner.** (Pure, stateless resolution
  over the immutable segments as-of a pinned basis is still done on the reader —
  for history / value-scans / cache-miss — because that is the same deterministic
  matcher code, Theorem 1, and carries no such invariant.)
- **Value store + blob tier** — out-of-line value bytes, and Tier-3
  content-addressed blobs (`blob_store.go`), that the index keys reference; both
  mapped/RDMA-readable, fetched by offset or hash as a bounded read once the key
  is found.
- **Symbol tables** (keyword/identity dictionaries) so readers decode without a
  shared Go heap; **watermark word** (atomic) — the published basis.

### 6.2 Snapshot reads (basis + freshness mark)

A reader pins the latest published watermark `W` at query start. The append-only
segments expose exactly `D|W` (segments/datoms with `Tx ⪯ W`). For a current-state
lookup on a cardinality one/many/vector attribute, it may use the resolved cache
entry `(v, lastTx)` iff `lastTx ⪯ W` — the value is a function of `(E,A)`'s own
datoms; else it resolves as-of `W` from the segments (Theorem 3). This is the
*same* freshness discipline the in-process matcher already needs for `AsOf`
queries (the latest-reflecting cache must not answer an older-basis read).

For a **unique** attribute the resolved value of `(E,A)` also depends on other
entities' claims to the same value (the `(A,V)` walk), so a competing `(E′,A,V)`
write changes it with no `(E,A)` datom. Its freshness is therefore keyed on the
attribute high-water mark (max `Tx` of any assertion of `A`, in the backing-access
interface, §3), not on `(E,A)`'s `lastTx`: if that high-water mark is `≻ W`, the
reader resolves the entity by walking the segments.

### 6.3 Reclamation

Each reader publishes its pinned basis. A superseded datom (an old version a
segment carries) is reclaimed during compaction only when its supersession
`ElementID` `⪯` the minimum live reader basis (Theorem 6); the resolved cache is
single-version (latest) and is not itself reclaimed. Dead/stalled readers are bounded by
a **lease**: an expired basis leaves the minimum; the reader must re-pin (and may
get "basis too old" → re-query at current).

### 6.4 Local vs remote access

- **Local (`mmap`)**: probe the resolved cache slot or binary-search an index;
  decode + intern into the reader's own heap. Lazy `Relation` streaming is fine.
- **Remote (RDMA)**: one-sided reads. Point lookups are one bounded read; scans
  binary-search then **block-read the matched key range in bounded reads** and
  iterate locally — never stream `Next()` tuple-by-tuple over the fabric. All
  eight indexes are served this way; there is no shape that falls back to
  server-side execution.

---

## 7. Sources and overlay

This is Janus's multi-source design (`SourceRouter`,
`map[Symbol]PatternMatcher`), and §3 is what lets it span processes and hosts.
More matchers in the map: the arena, a local overlay, a peer's arena, an
in-memory source — all `PatternMatcher`s, joined by the executor as today
(`WithSources`, `MemoryPatternMatcher`, `SliceSource`).

Two compositions:

- **Sibling sources** — *across* the router, named (`[?e :a ?v $peer]`). Disjoint
  data, joined. The existing cross-database join, now spanning hosts.
- **Overlay** — *within* one source name. `$` resolves to
  `OverlayMatcher{local, base: arena}`, itself a `PatternMatcher`, merging per
  pattern: LWW resolves the local pending op against the base by `ElementID`;
  add-wins unions the local set onto the base; a unique-attribute write is
  re-resolved by the `(A,V)` walk against the base's competing claims (the base
  exposes them via AVET). (Vector overlays require the base to expose RGA
  element-IDs — Theorem 4.) This gives read-your-writes, speculative `with`-style
  queries, and private ephemeral scratch.

**Write-path unification.** The overlay *is* the client's pending batch. A batched
transaction is an overlay that flushes to the owner (the single writer) via the
submission ring at commit; ephemeral runtime state is an overlay that never
flushes. The batched-transaction pattern and the multi-source pattern are the
same mechanism. After commit, the owner integrates, resolves, advances the
watermark; the client rebases its overlay.

---

## 8. The write plane

Reads fan out to immutable state; writes funnel to an authority. The arena is
fixed as-of a watermark, so any number of readers consume it locally and
`PatternMatcher` composes sources freely. A write mutates, so it passes through
the one authority that owns the Lamport clock, the cache-commit-atomicity
protocol, and watermark publication. The shape is **N readers / no authority** for
reads, **N submitters / one authority** for writes — so the write seam is an
authority funnel, not a composable matcher.

### 8.1 The seam: `Transaction` over a commit-target

The write analog of `PatternMatcher` is the `Transaction` commit path.
`Transaction.Commit()` today assigns ElementIDs, writes the eight indices, updates
the EA cache, and publishes — against the local `BadgerStore`. We abstract *what
Commit does* behind a commit-target: **local** (embedded, or a multi-writer
replica) or **remote** (a single-writer client → the owner). The `Transaction`
API (`Add` / `Set` / `Remove` / `SaveStruct` / `Commit`) is identical across both
— the §3 invariant applied to writes. With the arena read backing, a **client
`Database`** is arena-read + ring-write and presents the identical public API;
application code is unaware of the boundary on either axis.

The overlay (§7) is the staging area: a `Transaction`'s pending datoms *are* the
client's overlay, which gives read-your-writes during the transaction.
`Commit()` flushes that batch.

### 8.2 Submission: a shared-memory ring, the watermark as ack

A client submits a committed batch through a **per-client SPSC shared-memory
ring**, drained by the consumer. Datoms serialize into the ring as POD ops — the
same discipline as the arena, no Go pointers. There is no per-commit RPC on the
submission side; the commit *work* is still serialized at the single consumer.

Completion is observed through the **watermark the client already reads for
queries**: the commit has landed once the published watermark covers the batch's
assigned basis. A per-submission status slot carries that assigned basis (so the
client knows which watermark covers it) and the one possible rejection (§8.4).
Batching is automatic — the whole transaction is a single submission.

### 8.3 Two modes — the CAP knob, on the write side

- **Single-writer (CP).** The client stamps pending ops with **provisional** local
  ElementIDs (Lamport `≻ W`) so the overlay gives read-your-writes pre-commit (§7).
  On drain the owner — the sole ring consumer — reassigns **authoritative**
  ElementIDs in drain order (remapping intra-transaction RGA `AfterRef`s; the
  batch's internal order is preserved, so it resolves among itself as the client
  saw — interaction with concurrently-committed writes still follows Theorem 4).
  That drain order *is* the linearization (Theorem 7, scalar watermark). `Commit()`
  acks after the owner has durably persisted and published (WAL-before-publish,
  §10.4), so read-your-writes holds globally at the returned basis.
- **Multi-writer (AP).** Each replica carries its own `ReplicaID` and Lamport
  clock, assigns ElementIDs locally, commits to its own arena (locally durable at
  once), and ships datoms to peers over replication rings; peers apply and
  converge (Lamport sync on receipt; vector watermark). `Commit()` acks after
  local durable persist; global visibility is eventual.

The `Transaction` API is the same in both; the commit-target and the watermark
type (scalar vs. vector) differ — the same knob that governs reads.

### 8.4 What rejects, and what does not

CRDT resolution means commits are **accept-and-append**: there are no write-write
conflict failures, in either mode.

- **Uniqueness does not reject.** It is resolved at read time — walk-based
  `(A,V)`-LWW (`storage/unique_resolve.go`): the highest-Tx entity owns a value, a
  superseded entity's walk falls back to its first non-superseded assertion, and
  value-view and entity-view agree by construction. The walk is a deterministic
  function of the datom set under `≺` (Lemma R1 applied to the `(A,V)` axis), so
  it converges under multi-writer at no CP cost. The system never enforced
  uniqueness at write time, so there is nothing to reject.
- **The only rejection is value-type validation** — a pure predicate on the
  datom, checkable client-side before submission. No coordination, in either mode.

### 8.5 Boundary: one owner per transaction

A transaction targets a single owner. An atomic write spanning two owners (one
transaction over `$` and `$peer`) is out of scope — that is 2PC, and CRDT gives
cross-owner *convergence*, not *atomicity*. Cross-source remains a read-time join
(§7).

---

## 9. Formal model and proofs

### 9.1 Model

- **Datoms.** `d = (E, A, V, Tx, Op)`, `Tx ∈ ElementID`. `D` is append-only
  (`D₀ ⊆ D₁ ⊆ …`); deletion is a tombstone datom.
- **Order.** `≺` on `ElementID` is lexicographic on `(Lamport, ReplicaID)`
  (`ElementID.Less`). **A1 (uniqueness):** ReplicaIDs unique, per-replica Lamport
  strictly increasing ⇒ every datom has a distinct `Tx`; `≺` is strict total.
- **Basis.** `D|W := { d ∈ D : Tx(d) ⪯ W }`.
- **Resolution `ρ(E,A,S)`** by cardinality: *One (LWW):* the `≺`-max `(E,A)`
  datom's `V`, or `⊥` if its `Op = remove`; *Many (add-wins):*
  `{ v : ∃ add @ t_a ∧ ∄ remove @ t_r ≻ t_a }`; *Vector (RGA):* fold insert/
  tombstone in `≺` order.
- **The one matcher `M`.** A deterministic function of a **backing** `B`
  (providing `B.scan(index, [a,b))` over all eight orderings, value fetch, and a
  resolved-cache accessor `B.resolved(E,A)`) plus schema: `M[B](pattern, bindings)
  → Relation`, performing index selection, scan, CRDT resolution, predicate
  pushdown, lookup, and fusion. **The same `M` is used in-process and over the
  arena; only `B` differs.**
- **Backings.** `B_badger@W` (the in-process backing restricted to `D|W`; `W` is
  the latest basis in normal use); `B_arena(W)` exposing exactly `D|W` across the
  eight orderings and `resolved(E,A)` per §6.2.
- **Arena invariants** (owner obligations): segments contain exactly `D|W`; a
  resolved-cache entry equals `ρ(E,A,D|W)` whenever its freshness mark is `⪯ W`
  (`lastTx` for cardinality one/many/vector; the attribute high-water mark for a
  unique attribute, §6.2).
- **Seqlock / watermark / memory model:** as in the prior revision (A2:
  release/acquire on the watermark; seqlock version fences; over RDMA, Theorem 5).

### 9.2 Lemmas

**Lemma R1 (resolution is deterministic and locus-independent).** By A1, `≺` is a
strict total order with no ties; each resolution rule — the three cardinality
rules and the `(A,V)` uniqueness walk — is a pure function of the datom set under
`≺`. Independent of order, replica, process. ∎

**Lemma S0 (backing substitutability over all eight indexes).** For every index
`i` and range `[a,b)`, `B_arena(W).scan(i,[a,b)) = { d ∈ D|W : key_i(d) ∈ [a,b) }`
in key order `= B_badger.scan(i,[a,b))` restricted to `D|W`. *Proof.* The arena
holds exactly `D|W`'s datoms in the same eight L85 key orderings (immutable sorted
segments merged in key order); the L85 encoding is identical to Badger's, so key
order and range membership coincide. ∎

**Lemma W1 (resolved-cache invariant; owner maintains it).** Induction over
commits. For cardinality one/many/vector, a commit `Δ` updates exactly the `(E,A)`
it touches to `ρ(E,A,D|W_new)` and leaves the rest (whose datom sets are
unchanged) correct; each entry is fresh per its `lastTx`. For a **unique**
attribute, a commit of `(E′,A,V)` also changes the resolved value of every other
entity competing for `V`, so the owner recomputes that competition set, and those
entries are fresh per the attribute high-water mark (§6.2), not their own
`lastTx`. Hence `resolved(E,A) = ρ(E,A,D|W)` whenever the relevant freshness mark
is `⪯ W`. ∎

### 9.3 Theorems

**Theorem 1 (substitutability ⇒ matcher indistinguishability — "no special
snowflake," formally).** For any query `q` at basis `W`:
`M[B_arena(W)](q) = M[B_badger@W](q)`.
*Proof.* `M` is deterministic in (i) the bytes returned by `B.scan`, (ii) the
values returned by `B.resolved`, and (iii) schema (shared). `M` issues identical
scans and lookups in both cases, because index selection and access order are
functions of `q`, bindings, and schema — not of the backing. By Lemma S0 the two
backings return identical scan bytes for `D|W`; by Lemma W1 they return identical
`resolved(E,A)` for the entries `M` is permitted to use at basis `W` (§6.2 gates
the rest to scans, covered by S0). `M` applies CRDT resolution by the same code
over identical inputs (R1). Therefore the output relations are identical. ∎
**Corollary 1.1.** The arena-backed source is observationally indistinguishable
from a Badger source at the `PatternMatcher` interface; no caller above `M` can
branch on the backend, because no observation differs. This proves the §3
invariant.

**Theorem 2 (seqlock read consistency, resolved cache).** Any accepted seqlock
read of a resolved-cache entry observes a single quiescent writer epoch (no torn
read). *Proof.* Monotone versions; writer holds odd during mutation, even when
quiescent; a payload read straddling an epoch is bracketed by differing versions
⇒ retry. (Requires A2.) ∎ *The append-only segments need no seqlock — immutability
gives consistent reads directly.*

**Theorem 3 (snapshot isolation).** A reader pins published `W`. Segments expose
exactly `D|W`. For cardinality one/many/vector a resolved-cache entry is used iff
`lastTx ⪯ W` — then it equals `ρ(E,A,D|W)` by W1, since no `(E,A)` datom lies in
`(lastTx, W]`. For a unique attribute the gate is instead the attribute high-water
mark `⪯ W` (§6.2), because a competing claim with no `(E,A)` datom can change the
value. When the gate fails, `M` resolves as-of `W` from segments. Every `(E,A)`
the query reads equals `ρ(E,A,D|W)`; `W` fixed ⇒ consistent snapshot `D|W`.
Segment-resolution cost is bounded by `(E,A)` modified in `(W, now]` — churn since
the basis, not data size. ∎

**Theorem 4 (overlay correctness).** Let `L` be local pending datoms with
`Tx ≻ W`; `ρ_ov(E,A) := ρ(E,A,(D|W) ∪ L)`, computed by merging `L`'s ops over the
base. **(a)** Read-your-writes: `L` is in the resolved input. **(b)** Speculative
equivalence (no intervening writes): committing `L` as the next writes after `W`
yields `ρ(E,A,D'|W_L) = ρ_ov(E,A)` (`D'=D∪L`, `W_L=max_≺ L`), since
`D'|W_L = (D|W) ∪ L`. **(c)** Convergence under concurrency: with interleaving
writes `X`, the authoritative value is `ρ` over the union (R1) and equals
`ρ_ov` except where `X` conflicts under the cardinality rule (LWW: an `X` datom
`≻` `L`'s max; add-wins: an `X` remove `≻` a local add; RGA: deterministic
re-interleaving) — convergent, not conflict-free-for-the-observer. *Proof.* As in
the prior revision (set identity for (b); CRDT convergence for (c)). Unique
attributes merge by re-running the `(A,V)` walk over `(D|W) ∪ L` against the
base's competing claims, not by `(E,A)`-LWW. **Constraint:** vector overlays
require the base to expose RGA element-IDs, else vector reads under an overlay
fall back to segment resolution. ∎

**Theorem 5 (seqlock over RDMA).** Theorem 2 holds over one-sided RDMA reads of
the resolved cache given (i) version/payload/version reads issue in program order
on one RC queue pair (which preserves read ordering), and (ii) the writer release-
fences before the even-version and watermark stores. *Engineering proof:* rests on
RC-QP ordering + writer fences (A2 extended to the NIC); validate per fabric. The
append-only segments are immutable, so their RDMA reads need no such bracketing. ∎

**Theorem 6 (reclamation safety; liveness caveat).** With readers publishing
pinned bases, reclaiming a superseded datom whose supersession `s ⪯ min_live_basis`
never frees data a live reader reads (a reader at basis `W` needs only datoms
resolving `D|W`; `s ⪯ min_basis ⪯ W` contradicts the `s ≻ W` that would make a
datom needed by `W`). The resolved cache holds only the latest value per entry, so
it is never reclaimed — a reader whose basis predates an entry's freshness mark
resolves from segments instead (Theorem 3). ∎ *Liveness:* bound dead readers with
leases.

**Theorem 7 (single-writer linearizability; multi-writer generalization).**
Single writer ⇒ strictly monotone watermarks, `D|W_i ⊆ D|W_{i+1}`, reads as-of
`W_i` reflect exactly commits `1..i` (linearizable). Replacing scalar `W` with a
per-replica vector `V` (`D|V = {d : d.Tx.Lamport ≤ V[d.Tx.ReplicaID]}`) gives
**convergent** snapshots — all readers at `V` agree — but **not causally closed**:
a per-replica Lamport cut can include an effect without its cause, because
`ElementID` carries a scalar Lamport, not a per-datom vector clock; causal
consistency would need dependency tracking. Convergent, not linearizable and not
causal — the CAP knob (§10). ∎

### 9.4 What the proofs do *not* give

- **Cross-arena global snapshots** without a coordinated vector basis (Theorem 7).
- **Freedom from optimistic divergence** (Theorem 4c — convergent, not
  conflict-free for the speculating reader).
- **Backend-independence beyond the model assumptions** — Theorems 2/5 assume A2 +
  RC-QP ordering; these are implementation obligations.

---

## 10. Implications: Janus as a true distributed database

### 10.1 CAP posture

- **Single-writer mode** (scalar watermark): **CP for writes** (owner is the
  linearization point); **available, bounded-stale reads** (readers serve
  consistent snapshots as-of last-seen `W` even if the writer is unreachable).
  Datomic's posture.
- **Multi-writer CRDT mode** (vector watermark): **AP** — every replica writes and
  converges; no global linear order; heals monotonically (append-only ⇒ no
  rollback).

The same arena + interchangeable-`PatternMatcher` mechanism supports both. The
knob is **scalar vs. vector watermark**, and Janus already carries the
`ElementID(Lamport, ReplicaID)` the vector form needs.

### 10.2 Relationship to Datomic

A Datomic-class system (single writer, peers run queries locally, durable log as
truth) with two divergences:

|                    | Datomic                            | Janus + arena                                |
| ------------------ | ---------------------------------- | -------------------------------------------- |
| Reader             | query engine over storage segments | same matcher, over an `mmap`/RDMA arena      |
| Read transport     | storage round trips + peer cache   | `mmap` / one-sided RDMA                      |
| Resolution         | peer resolves from segments        | same, plus a shared resolved-cache fast path |
| Concurrent writers | forbidden                          | supported (CRDT convergence)                 |
| Snapshot           | value as-of basis-t                | (arena, watermark); any retained basis       |

The peer-runs-the-query-engine shape is identical to Datomic's; the reader runs
the same engine over a different backing (§3).

### 10.3 Scaling profile

- **Reads scale horizontally at ~zero owner cost.** One-sided RDMA reads consume no
  owner CPU; `N` reader hosts add no load to the writer.
- **The owner's resolved cache is shared, so current-state resolution is paid
  once** (owner) and read by all — the in-process "consistent cache" property,
  preserved across the boundary. Cache-miss / history / value-scan resolution is
  paid per reader (memory-speed scans), amortized by the reader's working set.
- **Writes are bounded by one owner** in single-writer mode (commit + eager-resolve
  rate); multi-writer mode lifts this at §10.1's consistency cost.

### 10.4 Failure modes

- **Owner crash** — readers continue at the last watermark (available, stale); no
  writes until recovery replays the durable log and rebuilds the arena. The
  watermark is published only after segments are durable (WAL-before-publish), so
  recovery never resurrects an unpublished basis.
- **Reader crash** — lease expiry drops its basis from the reclamation minimum.
- **Partition** — remote readers stall at the last watermark; the writer continues;
  on heal, monotone catch-up (append-only ⇒ no rollback).

These are single-writer modes. In multi-writer there is no single owner: a replica
crash pauses only its own writes while peers read and write on, and a partition
heals by exchanging missing datoms — convergent and monotone (Theorem 7).

### 10.5 Other consequences

- **Schema + interning are shared state in the arena** — the symbol tables, and
  the schema structure (parsed, or inferred from the store at open). Readers need
  the schema to resolve, so it travels with the arena and is republished when it
  changes.
- **Security/isolation.** A mapped/RDMA arena exposes *all*
  of it to any reader with the mapping — no attribute/row-level access control at
  the memory layer. Untrusted or multi-tenant readers need separate arenas; memory-speed
  sharing is strictly weaker than a query server here.

### 10.6 Composition with the sharding proposal

[DISTRIBUTED_JANUS.md](DISTRIBUTED_JANUS.md) shards datoms across Raft groups and
scatter-gathers. Orthogonal: each shard is a process owning some datoms, and can
expose its state as an arena under *this* proposal — scatter-gather reads become
one-sided RDMA reads against per-shard arena sources, and the query router becomes
a reader joining across shard arenas (sibling sources). Storage/write plane and
read/execution plane of one system.

---

## 11. Risks and limitations

1. **Extracting the backing-access interface + the off-heap arena layout** (eight
   orderings + resolved cache + symbol tables) is a large change. The store
   representation is close to Badger's; the cache and symbol tables are not.
2. **One matcher over two backings** requires the matcher's Badger coupling to be
   genuinely narrowed to the interface; incomplete extraction reintroduces a
   fork (the snowflake). The substitutability differential test
   (Phase 0) is the guard.
3. **RDMA + Go is operationally heavy**: cgo/libibverbs, memory pinned off the GC
   heap, NIC-dependent behavior.
4. **Seqlock-over-RDMA correctness** (resolved cache) rests on RC-QP ordering +
   writer fences (Theorem 5) — validate per fabric.
5. **Reclamation needs leases**; a dead reader grows arena memory until expiry.
6. **Multi-writer mode sacrifices global snapshots/linearizability** (Theorem 7).
7. **Eager-resolve-on-commit raises write latency** — wrong for write-heavy churn.
8. **No in-arena access control** (§10.5).

---

## 12. Phased implementation path

- **Phase 0 — Backing interface + substitutability test.** Narrow the matcher's
  storage dependency to a backing-access interface; build an arena backing
  (eight orderings + resolved cache + symbol tables) the owner maintains alongside
  Badger. **Assert Theorem 1 as a differential test:** `M[arena]` and `M[badger]`
  return identical relations across pattern/scan/lookup/history/value-scan shapes
  and unique-attribute writes with cross-entity competition (the case that breaks
  per-`(E,A)` freshness). This test *is* the no-snowflake guarantee.
- **Phase 1 — Same-process second matcher.** A second in-process `M[arena]` in the
  router; validates the seam and seqlock with no IPC.
- **Phase 2 — Local multi-process.** `mmap` the arena from a second process;
  watermark publication; lease-based reclamation. Multi-process lean-on-Janus,
  locally, at memory speed.
- **Phase 3 — Overlay + client writes.** `OverlayMatcher`; ship-overlay-to-owner
  commit via the submission ring; rebase.
- **Phase 4 — RDMA backing.** Remote `M[arena]` over one-sided reads; bounded
  range materialization; no server-side execution path.
- **Phase 5 — (optional) Multi-writer CRDT federation.** Vector watermarks;
  cross-arena convergent reads.

---

## 13. Alternatives considered and rejected

- **A dedicated `ArenaMatcher` parallel to `BadgerMatcher`** — a V2 fork; the
  snowflake. Rejected for one matcher over a backing-access interface (§3).
- **A reduced arena matcher with RPC fallback for hard shapes** — partial = special
  (§3). "All eight indexes" closes the gap.
- **Demoting the arena to a `Store` below one stock matcher** — collapses the
  composition model: a store is one backing under one matcher, not a source, so
  overlays, sibling joins, and federation (which compose at the `PatternMatcher`
  level) become impossible. The backing-access interface lives below the matcher,
  but the *source* is still a complete `PatternMatcher`.
- **RPC query server / read-only Badger multi-open / per-query daemon** — §2.

---

## 14. Open questions

1. **Watermark granularity** — per-commit vs batched/epoch watermarks under high
   write rates.
2. **Overlay durability** — purely in-memory (lost on client crash) or WAL-backed
   before flush?
3. **Multi-writer basis representation** — vector clock size/compaction as
   ReplicaIDs grow.
4. **RGA positional exposure** — how much RGA metadata the arena carries to keep
   vector overlays merge-correct vs. forcing segment fallback (Theorem 4).
5. **Write backpressure** — when the single-writer consumer falls behind, does a
   full submission ring block the client or spill to a fallback path?
6. **Multi-writer replication topology** — how replicas discover peers and fan
   out their rings (all-to-all, hub, gossip).

---

## References

- [DISTRIBUTED_JANUS.md](DISTRIBUTED_JANUS.md) — storage/write-plane sharding (companion).
- [MULTI_SOURCE_QUERIES.md](MULTI_SOURCE_QUERIES.md) — the `SourceRouter` model this generalizes.
- `datalog/executor/interfaces.go` — `PatternMatcher` and companions (the composition seam).
- `datalog/element_id.go` — `ElementID(Lamport, ReplicaID)` and its total order.
- `datalog/storage/cache.go` — the EA cache (`GetOrResolve`, `CacheResolver`); the resolved cache's in-process ancestor.
- `datalog/storage/database.go` — `AsOf`/`History` (the as-of read API).
- Lamport, *Time, Clocks, and the Ordering of Events* (1978); seqlocks; RCU/epoch reclamation; FaRM (RDMA shared-memory transactions) as prior art.
