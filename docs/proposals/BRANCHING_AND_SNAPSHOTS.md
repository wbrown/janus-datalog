# Branching, Snapshots, and Rollback

**Status:** Proposal — core decisions resolved 2026-06-26 (§9.6 register); build pending
**Author:** wbrown (design), drafted with Claude
**Date:** 2026-06-26
**Builds on:**
- [DISTRIBUTED_JANUS.md](DISTRIBUTED_JANUS.md) — `ReplicaID`/`NodeID`, Lamport-clock conflict-free merging
- [SHARED_ARENA_DISTRIBUTION.md](SHARED_ARENA_DISTRIBUTION.md) — pinned **basis `W`** (snapshot isolation, Thm 3), **overlay** (speculative writes), the vector-basis requirement (Thm 7)
- [../reference/CRDT_UNIQUE_SEMANTICS.md](../reference/CRDT_UNIQUE_SEMANTICS.md) — uniqueness as read-time `(A,V)`-LWW
- [CRDT_COMPOSABLE_TOOLKIT.md](CRDT_COMPOSABLE_TOOLKIT.md) — MV-Register (conflict-surfacing), composable conflict policies

---

## Abstract

This proposal adds git-tree-style branching, named snapshots, and rollback to Janus.
Almost none of it is new machinery: it generalizes one scalar to a vector.

Janus's `AsOf(txID)` already filters the visible datom set by a single comparison,
`txID.Less(datom.Tx)`, against the total order `(Lamport, ReplicaID)`. That is a
single-component version vector. Promote that scalar threshold to a version-vector
frontier (a map `{ReplicaID → maxLamport}`) and the same filter yields isolated,
divergent, mergeable branches. CRDT resolution (LWW / add-wins / RGA / unique
`(A,V)`-LWW) is unchanged; it already operates over whatever survives the filter, not
over a linear history.

A branch is the persistent, named generalization of the overlay that
`SHARED_ARENA_DISTRIBUTION.md` defines for speculative writes; a snapshot (tag) is a
named, immutable instance of the basis `W` that the same document proves gives
snapshot-isolated reads (Theorem 3). Merge is a metadata-only, non-destructive,
deterministic join of two frontiers (component-wise max); `LamportClock.Receive`
already implements the per-component primitive.

The version-vector frontier is the merge-optimal encoding of branch identity, but not
the only one. §9 sets three encodings side by side: version-vector frontier,
content-addressed arena DAG, and a tree-path (CIDR-style) bitmask key prefix, each
optimal for a different operation. The driving use case (a game engine storing all
state in Janus: separate user sessions forked off a shared world, with rollback;
`AsOf()` already in production) is fork-and-diverge plus rollback, with merge-back
deferred, which points at the tree-path prefix (§9.3).

This document records the model, the mechanism, the merge guarantees, and two deep
dives requested during design: uniqueness across merges (§7) and the per-frontier
cache (§8). It then sets out the central encoding choice (§9), the read-path execution
for the recommended tree-path encoding (§10), the implementation sketch (§11), and the
staged plan (§12). The open architectural decisions throughout are wbrown's to make.

---

## 1. Goals and non-goals

### Driving use case

A game engine stores its entire state in Janus. It already runs `AsOf()` in
production ("what was this value at time Y") for a task-tracking subsystem. The new
needs, in priority order:

1. **Separate user sessions** — each session forks off a shared world state and
   diverges independently; a session must not pay for other sessions' writes.
2. **Fork-and-diverge with rollback** — load an earlier state, branch-and-try,
   discard a speculative attempt; non-destructive by default (append-only).
3. **Fork-and-merge-back is a *later* use case**, explicitly not this round.

This priority (diverge and roll back now, merge later) is what makes the tree-path
encoding (§9.3) the front-runner over the merge-optimal frontier.

**Goals**
- **Snapshot point** — name the current state; query it later; immutable (a tag).
- **Branch** — fork the current state, accrue divergent writes, read in isolation.
- **Merge** — fold one branch's writes into another, deterministically. *(Deferred
  per the use case; the design must not preclude it.)*
- **Rollback** — return a branch to an earlier state without destroying data.
- Reuse the existing append-only / CRDT / index machinery. No new storage core.

**Non-goals (this round)**
- Git-style manual 3-way conflict resolution. Merges auto-resolve via the
  per-attribute CRDT policy (decision below). MV-Register can surface conflicts
  later if wanted, but that is opt-in and out of scope here.
- Write-time upsert / entity merging across a merge (Datomic
  `:db.unique/identity` Position 3). This is already deferred project-wide
  (see §7); branching does not change that.
- Destructive history pruning / GC. Append-only forever is the default; reclaiming
  space is a separate, opt-in, reachability-aware operation (§6.4).

### Decisions taken (design conversation, 2026-06-26)

- **Scope: full branching**, encoded as the tree-path prefix (C) branch axis
  composed with `AsOf` for the time axis (§9.4). Frontier (A) is reserved for deferred
  merge-back; content-addressed arenas (B) for the distributed future.
- **Merge semantics: pure CRDT auto-resolve** — highest-Lamport for cardinality-one,
  add-wins for many, RGA for vector, `(A,V)`-LWW for unique; no conflict prompt
  (MV-Register a future opt-in). Merge-back itself is deferred per the use case.
- **The remaining design/encoding decisions are resolved in the §9.6 register** — key
  encoding, label scheme, fork semantics, rollback default, hard-truncate policy,
  `History()`, metadata storage, and clock model (all 2026-06-26).

---

## 2. The core realization

Visibility is enforced in a single predicate, mirrored in two places:

```go
// crdt_resolving_iterator.go (per-datom, during streaming resolution)
if it.txID != (ElementID{}) && it.txID.Less(datom.Tx) { continue }   // skip "future" datoms

// matcher.go: shouldFilterTx — same comparison on the candidate path
return m.txID != nil && *m.txID != (ElementID{}) && m.txID.Less(datomTx)
```

Everything else (the eight indices ordered by total `(Lamport, ReplicaID)` order,
"first entry wins" LWW, add-wins, RGA, unique `(A,V)`-LWW) runs over whatever survives
that predicate. The substrate is already a branching substrate, pinned to one linear
timeline only because the threshold is a scalar.

`SHARED_ARENA_DISTRIBUTION.md` already named both ingredients without calling them
"branches":

- A reader pins a basis `W` and gets snapshot-isolated reads
  (Theorem 3: `reads = ρ(E,A, D|W)`). That is a snapshot/tag.
- An overlay is divergent local writes layered over a base, composed as just
  another `PatternMatcher` source, giving speculative `with`-style semantics.
  That is an uncommitted branch.
- Theorem 7 states multi-writer global snapshots require a coordinated vector
  basis. That vector basis is the frontier.

So this feature promotes the ephemeral overlay to a named, persistent, mergeable
thing, and the scalar basis to a version-vector frontier.

---

## 3. Conceptual model and git mapping

A branch is not a copy of data. All branches write into the same append-only
Badger keyspace and the same eight indices. A branch is a small pair:

```
branch := (frontier, writeIdentity)
  frontier     : map[ReplicaID]uint64   // ReplicaID → max visible Lamport
  writeIdentity: ReplicaID               // new-write stamp (frontier model); under encoding C, identity is the tree-path (§9.1)
```

| Git | Janus |
|---|---|
| Commit | Transaction (an `ElementID` batch) — already append-only, immutable |
| Tag / snapshot | A named, immutable frontier (snapshot-isolated reads: Thm 3) |
| Branch | A named, mutable frontier + its own write-identity |
| HEAD | The frontier the active handle reads/writes against |
| `branch`/fork | Copy parent's current frontier + allocate a fresh write-identity (O(1)) |
| `merge` | Component-wise max of two frontiers (`LamportClock.Receive` is the per-component primitive) |
| `reset` / rollback | Repoint a branch's frontier to an earlier one (non-destructive) |
| `revert` | Append compensating datoms (natural in an append-only store) |
| `gc` / prune | Physically drop datoms unreachable from any frontier (destructive, opt-in, last) |

---

## 4. The mechanism (one extension point)

```go
type Frontier map[uint64]uint64                        // ReplicaID → max visible Lamport

func (f Frontier) Visible(id ElementID) bool {
    return id.Lamport <= f[id.ReplicaID]               // absent replica ⇒ 0 ⇒ invisible
}
```

The single filter becomes `if !frontier.Visible(datom.Tx) { continue }`.

Resolution logic is untouched. Indices scan in descending total order, so the first
visible datom in an `(E,A)` group is still the LWW winner among the visible set;
add-wins / RGA / unique `(A,V)`-LWW just see a filtered stream. `AsOf` is recovered as
the one-component case.

> **Implementation note.** The frontier filter must be threaded through every
> resolution sub-scan, not just the top-level group scan: the EATV first-entry walk
> and the AVET supersession sub-scan used by unique `(A,V)`-LWW (`walkApplyEntry`). A
> resolution path that sees an invisible datom would resolve against state the branch
> can't see, so the predicate must be applied at every scan.

### 4.1 One write-identity per branch

If a single node wrote to two sibling branches under the same `ReplicaID`, a
scalar-per-replica frontier could not separate them: the sibling's higher Lamports
would subsume the other's. Giving each branch its own write-identity makes the
frontier a compact encoding of git ancestry:

- Fork copies the parent frontier; the parent's later writes aren't in the child's
  vector (they use the parent's ReplicaID, which the child froze at fork time).
- Sibling branches use distinct ReplicaIDs, so a node writing to both uses the right
  stamp per branch, with no cross-contamination.
- A branch frontier built by fork-then-write is always causally closed: every datom's
  dependencies are either inherited (in the copied frontier) or are the branch's own
  earlier writes (in its vector). Component-wise max of two closed frontiers stays
  closed, which is what makes merge clean (§6).

The clock stays one shared counter per Database; writes are stamped with the active
branch's ReplicaID. Consequences:

- Single node, many branches ⇒ Lamports are globally unique and monotonic ⇒ no ties,
  and LWW is "last submitted." Ties (and the arbitrary ReplicaID tiebreak) only appear
  across multiple physical clocks.
- A write on branch B advances the shared counter, so branch A's next write gets a
  later Lamport. This is harmless: visibility is keyed on `(ReplicaID, Lamport)`, not
  Lamport magnitude, so B's write stays invisible to A regardless.

### 4.2 Worked example

```
State 0:  order1 :order/status "open"      @ L5@Rmain      main.frontier = {Rmain:5}

Fork exp at L5 → exp.frontier = {Rmain:5}  (copied), write-id = Rexp

exp writes:   :order/status   "shipped"    @ L6@Rexp
              :order/tracking "Z9"          @ L7@Rexp       exp.frontier = {Rmain:5, Rexp:7}
main writes:  :order/status   "cancelled"  @ L8@Rmain      main.frontier = {Rmain:8}
```

The physical store holds every datom, interleaved in one EATV group (descending total
order): `cancelled L8@Rmain`, `shipped L6@Rexp`, `open L5@Rmain`, plus
`tracking Z9 L7@Rexp`. Isolation follows from the one predicate:

| Read | `main` (`{Rmain:8}`) | `exp` (`{Rmain:5, Rexp:7}`) |
|---|---|---|
| `:order/status` | `L8` visible → "cancelled" | `L8`? 8≤5 no → `L6` 6≤7 → "shipped" |
| `:order/tracking` | `L7@Rexp`? no `Rexp` entry → absent | `7≤7` → "Z9" |

Neither branch sees the other's writes, yet nothing is physically partitioned.

---

## 5. Public API sketch (illustrative, not final)

```go
d.Snapshot("before-import")            // name current frontier (immutable tag)
d.AsOfSnapshot("before-import")        // read at that frontier (= existing AsOf, by name)

b := d.Fork("experiment")              // fork from HEAD's frontier; allocate write-id
b.NewTransaction()...Commit()          // writes stamped to this branch
b.Query(...)                           // sees base + its own writes only

d.Merge("experiment", "main")          // main.frontier ← max(main, experiment); clock.Receive
d.Reset("main", "before-import")       // repoint main's frontier (non-destructive)
d.Revert(txID)                         // append compensating datoms
```

`d.Fork(...)` returns a handle that is read/write like a `*Database` but carries its
own frontier and write-identity. This mirrors the existing read-only `AsOf`/`History`
handles (`database.go`), which clone the struct with a different `temporalTxID`; a
branch handle is the read-write generalization. (These names illustrate the frontier
(A) model; the decided C surface — `Fork`, `Snapshot`/`AsOfSnapshot`, `TruncateTo` — is
specified in §11.2 and §12.1.)

---

## 6. Merge: how clean is "no side effects"?

> **Scope.** This section analyzes merge under the version-vector frontier
> (encoding A): the O(1) metadata operation below. Under the recommended tree-path
> encoding (C), merge is deferred (§1) and, when it lands, is materialize-based
> (O(divergence) squash into a fresh subtree, §9.3 / §10.7), because a merge node
> cannot be a single prefix (§9.1). The convergence semantics analyzed here, and §7
> (uniqueness), are encoding-independent; the O(1)-metadata mechanism is A-specific.

Merge `exp → main` is a one-line metadata update:

```
main.frontier ← componentwiseMax({Rmain:8}, {Rmain:5, Rexp:7}) = {Rmain:8, Rexp:7}
main.clock.Receive(L7@Rexp)            // advance logical time past what we absorbed
```

Zero datoms are written, moved, or deleted. The rest of this section states what
that buys, and where it stops.

### 6.1 What merge guarantees

- **Non-destructive.** Merge can't corrupt or lose data because it doesn't write data.
  Every datom that existed still exists and is still queryable.
- **Reversible.** `main`'s pre-merge frontier is still a valid frontier; a bad merge is
  undone by re-pinning it. No reflog needed; nothing was destroyed.
- **Asymmetric / isolated.** `exp` is untouched and reads as before (like `git merge`
  moving only the current branch).
- **Only logical time changes, not data.** `clock.Receive(L7@Rexp)` advances `main`'s
  Lamport counter past everything it just absorbed, so `main`'s future writes causally
  follow the merged-in history. That is a side effect on `main`'s clock, required for
  causality, but it touches no datom and does not affect `exp`, which keeps its own
  clock view. Metadata-only is literal: one frontier value written, one counter
  advanced, no datoms.
- **Deterministic and order-independent**, because the resolution functions are already
  CRDTs, not because of new merge code:

| Cardinality | Resolution | Why merge is order-independent |
|---|---|---|
| One (LWW) | max by total order `(Lamport, ReplicaID)` | `max` is commutative/associative/idempotent; frontier union is too |
| Many (add-wins) | union of visible adds minus higher-Lamport removes | all maxes over a set; set union is order-free |
| Vector (RGA) | DFS over insert/tombstone tree, siblings by ElementID | RGA is designed so concurrent inserts interleave deterministically |
| Unique (`(A,V)`-LWW) | highest-Tx claimant of `(A,V)` is canonical owner | same max over the visible set; see §7 |

Merging three branches in any order or grouping converges to the same resolved state:
merge is the existing resolution applied to a union'd visible set.

### 6.2 Where "no side effects" has edges

The data layer is always safe: nothing is corrupted, nothing is lost, and every datom
that ever existed remains queryable via `History()`. But "no side effects at the data
layer" does not mean "no surprises in the resolved result." The edges below are
consequences of pure auto-resolution, ordered roughly by severity. None is a bug; each
is a property of the chosen semantic, and a couple are decisions worth stating outright.

**1. Semantic clobbering is silent.** LWW resolves an `(E,A)` to a single deterministic
winner; the loser disappears from the resolved view, though it remains in `History()`.
In the §4.2 example, after `merge exp → main` the resolved `:order/status` is
`"cancelled"` (L8 beat L6), and `exp`'s `"shipped"` is gone from the current value. At
the data layer this is a non-event: both datoms are still present and both are returned
by `History()`. At the result layer, the engine will not tell you a write was
overridden unless you ask history; the merge looks clean and the overridden value is
absent. For a status field that is fine. For a counter or a balance, where two branches
each did `balance := balance + 10` and you wanted `+20`, LWW gives you one of the two
`+10` writes. That is intrinsic to last-writer-wins, and it is why choosing pure
auto-resolve (§1) is a real decision: a domain that needs additive merges wants a
counter CRDT (a different per-attribute conflict policy), not LWW.

**2. Cross-attribute emergent states.** Convergence is per-`(E,A)`, not per-entity.
Merge resolves each `(E,A)` group independently, so the merged entity can land in a
combination of attribute values that existed on neither parent. In §4.2: `exp` held
`{status: "shipped", tracking: "Z9"}`; `main` held `{status: "cancelled", no
tracking}`. After `merge exp → main`, `:order/status` resolves by LWW to `"cancelled"`
(L8 > L6) while `:order/tracking` is uncontested and resolves to `"Z9"`. The merged
entity is `{status: "cancelled", tracking: "Z9"}`, a tracking number on a cancelled
order, a state that was never true on either branch. CRDTs guarantee convergence
(everyone who sees the same datoms agrees) and per-attribute sanity, but they do not
preserve cross-attribute invariants. This is not unique to Janus: git's analog is a
semantic merge conflict, where two branches edit different files, the textual merge is
clean, and the build is broken. An application with a cross-attribute invariant
("tracking implies not-cancelled") must enforce it itself, after the merge; the store
will not.

**3. Whole-branch merge is clean; cherry-pick is not.** The reason is causal closure.
§4.1 established that a frontier built by fork-then-write is causally closed: every
datom's dependencies are either inherited at the fork or are the branch's own earlier
writes. Component-wise max of two causally closed frontiers is still causally closed,
so a whole-branch merge can never strand a dependency, which is why merge is safe.
Cherry-pick (grafting some of a branch's writes without their predecessors) breaks that
closure. The clearest case is RGA: a vector insert carries an `AfterRef` pointing at
the element it was inserted after. Cherry-pick the insert without the element it
follows and the `AfterRef` dangles; `resolveRGAGroup` has no anchor to attach it to,
and the vector cannot be reconstructed faithfully. So the safe operation is
whole-frontier merge only; partial / cherry-pick must either be forbidden for vector
(and any causally-dependent) attributes, or first compute and include the transitive
closure of what it grafts. (This is the same closure argument §10.8 relies on to show
RGA resolves correctly across the ancestor chain.)

**4. Lamport ties are deterministic but arbitrary**, and absent on a single node. When
two writes carry the same Lamport (only possible across distinct physical clocks), the
total order breaks the tie by `ReplicaID`. That is stable and deterministic (every
reader agrees) but not semantically meaningful: the winner is "whichever replica id
sorts higher," not "whichever happened later in wall-clock time" (Lamport clocks are
not wall clocks). For the single-node game-engine use case this never arises: one
shared clock (§4.1) hands out globally unique, monotonic Lamports, so there are no ties
and LWW is "last submitted." Ties become relevant only if Janus later runs multiple
physical clocks (the distributed future), at which point "the LWW winner of a true tie
is arbitrary-but-deterministic" is the contract to document.

**5. Uniqueness is not a new hazard.** An earlier framing in the design conversation
called uniqueness "the unsafe case" for merges. That was wrong, and §7 works through
why: uniqueness is already a read-time `(A,V)`-LWW resolution rule, so a merge resolves
it identically to the concurrent-writer case the project already supports. The only
unsafe variant, write-time upsert / entity merging, is one the project has already,
deliberately, deferred for the same reason. See §7.

### 6.3 Provenance (optional)

Visibility needs only the frontier update. For audit ("when did `main` absorb
`exp`?") we may additionally persist a merge record (parent frontiers, time, branch
names). That is metadata about the merge, not a datom that affects resolution.

### 6.4 The only destructive operation: GC

As long as we never prune, all branches, deleted-branch frontiers, and snapshots
remain valid forever (append-only). Reclaiming space means dropping datoms unreachable
from any live frontier, which must be reachability-aware (git-gc respecting refs +
reflog). That is where "no side effects" ends, and it is opt-in, deliberately out of
scope here.

---

## 7. Deep dive: uniqueness across merges (#2)

Branch merge introduces no new uniqueness hazard for the cases Janus supports today.
This corrects an earlier framing in the design conversation that called uniqueness
"the unsafe case." Reading `CRDT_UNIQUE_SEMANTICS.md` shows the substrate already
resolved this.

### 7.1 What Janus already does

`validateUniqueness()` was deleted. There is no write-time uniqueness enforcement.
Instead, uniqueness is a read-time CRDT resolution rule:

> Across all entities currently asserting `(A, V)` (each by `(E,A)`-LWW), the entity
> with the highest `Tx` is the canonical owner. Other entities' claims of `(A,V)`
> are superseded. The entity-view walks back to its most-recent non-superseded
> assertion (or no value).

This is delivered today via `resolveAVLWW` / `walkApplyEntry` (AVET candidate scan
+ EATV-LWW validation) and surfaced through `LookupByUnique`.

### 7.2 Why this makes merges automatically safe

A merge changes only which datoms are visible (the frontier). `(A,V)`-LWW is a pure
function of the visible set, so a merge resolves uniqueness the same way two concurrent
writers do today, a semantic the project already chose deliberately:

```
branch P:  entityX :user/email "a@x"  @ L10@Rp
branch Q:  entityY :user/email "a@x"  @ L20@Rq      (different entity, same value)

merge Q → P:  frontier admits both claims.
              (A,V)-LWW winner = highest Tx = L20@Rq ⇒ entityY is canonical owner.
              entityX's email-view falls back to its prior non-superseded assertion
              (or none). Both entities' datoms remain in History().
```

No "uniqueness violation" exists to raise, because there is no write gate to violate.
This is identical to the documented concurrent-writer takeover case
(`CRDT_UNIQUE_SEMANTICS.md` §"Concurrent takeover"). No new code is needed, provided
the AVET supersession sub-scan applies the frontier filter (§4 implementation note).

### 7.3 The one unsafe case is already deferred

Datomic's `:db.unique/identity` bundles two capabilities that are independent in a
CRDT model:

- Lookup refs (read-time: resolve entity from unique value): supported today
  (Position 2), merge-safe by §7.2.
- Write-time upsert / entity merging (rewrite a tentative entity ID onto an existing
  owner of the value): explicitly not done (Position 3 deferred), because concurrent
  writers who both resolve "no existing owner" both mint entities, producing
  split-entity states with no obvious convergence rule.

A two-branch merge of independent upserts is an instance of that same
concurrent-upserter split-entity case. So branching surfaces the already-known,
already-deferred problem; it does not create a new one. If/when Position 3 is designed
(split-entity convergence: does the loser's data migrate to the winner, by what rule,
preserving what history?), branch-merge upsert is covered by the same design. Until
then, branch merges inherit Position 2: lookup-refs resolve, no entity merging.

### 7.4 Content-addressed entity IDs dissolve most of it anyway

Entity IDs are content-addressed: `NewIdentity(s) = sha1(s)`. If applications name
entities by their natural key (the common pattern, and what `LookupByUnique` upsert
recipes assume), then "the same" entity created independently on two branches mints
the same `E`. The merge is then an ordinary per-`(E,A)` CRDT merge within one entity:
no split-entity, no divergence. Position 3's hazard only arises for random entity IDs
upserted by value. Recommending content-addressed natural keys for would-be-unique
entities is the cheapest mitigation, and aligns with the content-addressed-sharding
theme in `DISTRIBUTED_JANUS.md`.

### 7.5 Net recommendation for §7

- Do nothing special for value-uniqueness / lookup-refs. Read-time `(A,V)`-LWW already
  merges cleanly; thread the frontier filter into the AVET sub-scan.
- Keep write-time upsert out (Position 3 stays deferred). A branch-merge upsert policy
  is part of that future design round, not this one.
- Document that merging branches that independently claimed the same unique value
  yields a single canonical owner by `(A,V)`-LWW (the loser falls back), as with
  concurrent writers; and that natural-key (content-addressed) entity IDs avoid the
  split-entity case entirely.

---

## 8. Deep dive: the per-frontier cache (#5)

> **Scope.** §8 is framed for the frontier encoding (A). Under the recommended
> tree-path encoding (C) the cache is simpler: ancestor views are immutable and keyed
> by `(arena, ceiling)` (see §9.5 and §10.9, which supersede the
> frontier-validity-stamp machinery of §8.4 for the C path). Read §8 as the A-path
> design and the general framing of the problem.

### 8.1 What exists today

The EA `Cache` (`storage/cache.go`) maps `CacheKey{E, A}` → resolved view
(`oneValue` / `manySet` / `vectorList`), with freshness tracked by a scalar
high-water mark:

```go
type CacheEntry struct { version ElementID; ... }      // max ElementID when computed
// fresh iff entry.version == maxVersions[key]          // O(1), no storage seek
```

Snapshot scoping is at the `*Cache`-instance level, not in the key (comment in
`cache.go`): the Database's latest cache and each `AsOf` handle's private cache are
separate `Cache` instances, so `(E,A)` never collides across snapshots and the key
needs no snapshot dimension. History mode doesn't cache. Unique attributes use
conservative attribute-wide invalidation (D3): a write to a unique `(A,·)` invalidates
all cached `(E,A)` for that `A`.

### 8.2 The naive option and why it wastes memory

A branch is a mutable, long-lived `AsOf` handle. The cheapest mapping
onto today's design is one `Cache` instance per branch (the per-`AsOf` pattern).
Correct, but:

- N branches × overlapping `(E,A)` ⇒ N nearly-identical resolved views. Branches
  share the vast majority of base datoms; they differ only where they diverged.
- Every branch switch is a cold cache.

A resolved `(E,A)` differs between two branches only if a datom in the symmetric
difference of their frontiers touches that `(E,A)`. For the large majority of groups
(untouched since the fork), the resolved value is identical across all descendant
branches.

### 8.3 Recommended design: shared base + per-branch overlay (CoW), frontier-validity stamp

Mirror the data model (shared storage + per-branch divergence) in the cache:

```
Cache lookup on branch B for (E,A):
  1. B.overlay[(E,A)]            — entries for groups B has diverged on (its own writes,
                                    or unique-attr invalidations); validity-checked.
  2. else sharedBase[(E,A)]      — the resolution valid at the common-ancestor frontier;
                                    usable iff B's frontier resolves this group identically.
  3. else resolve from storage under B.frontier, then populate the correct layer.
```

This is the SHARED_ARENA overlay pattern applied to the cache, symmetric with the
branch model itself (branch = frontier overlay; branch cache = resolution overlay).
Properties:

- Fork is O(1): an empty overlay over the parent's cache.
- Memory is O(divergence), not O(branches × data); untouched `(E,A)` are served from
  the single shared base for all branches.
- Writes don't stale siblings. A write on B populates B's overlay only; other branches
  never saw it, so their caches stay valid. This is cleaner than the current
  single-clock invalidation, which is global.

### 8.4 Generalizing the freshness stamp from scalar to frontier

Today: fresh iff `entry.version == maxVersions[key]` (scalar equality). Generalize:
an entry computed under frontier `F` is reusable under read-frontier `F'` iff `F`
and `F'` admit the same newest datom(s) for that `(E,A)`. Because an `(E,A)`
group's datoms come from very few ReplicaIDs (usually one or two), the validity
check is tiny:

```
entry.basis : map[ReplicaID]uint64    // per-replica max Lamport AMONG THIS (E,A)'s datoms ≤ F
reusable under F' iff  ∀ r ∈ entry.basis:  F'[r] == entry.basis[r]
                       AND  F' reveals no (E,A) datom from a replica not in entry.basis
```

The "no new replica reveals a datom" half is the version-vector analogue of the
current `maxVersions` high-water check; the existing `attrVersions` (per-attribute
high-water) generalizes the same way for the unique-attribute / A-bound path. The
`ATEV` index already gives O(1) attribute high-water; a per-`(E,A)`-per-replica
high-water is the granular form.

> Open sub-question (§9): the exact validity stamp can be as cheap as "the single
> winning datom's `(ReplicaID, Lamport)`" for cardinality-one (reusable iff that
> datom is still the highest visible one under `F'` and `F'` reveals nothing
> newer for the group). Many/Vector need the per-replica set. Pick the minimal
> stamp per cardinality.

### 8.5 Merge and the cache

Merging `Q → P` changes `P`'s frontier, which can change the resolved value of
`(E,A)` groups where `Q` diverged. Invalidate only `P`'s overlay for those groups
(bounded by `Q`'s divergence set, not a full flush) if we track each branch's
divergence set (the `(E,A)` it has written). For unique attributes, merge invalidation
widens to attribute-wide within `P`'s layer (D3 conservative rule), since `(A,V)`-LWW
can move the canonical owner across entities. The existing D3 rule composes directly
with the overlay; it applies to the merging branch's cache layer instead of the global
cache.

### 8.6 Net recommendation for §8

- Branch cache = a per-branch overlay over a shared base, keyed `(E,A)`, with a
  frontier-validity stamp generalizing the scalar `version`. Fork O(1), memory
  O(divergence), sibling-isolated.
- Reuse the existing in-flight-sentinel / `storeIfNotInFlight` atomicity protocol
  per layer.
- Keep D3 conservative attribute-wide invalidation for unique attrs, scoped to the
  writing/merging branch's layer.

---

## 9. The central choice: three encodings of branch identity

Everything above used a version-vector frontier for branch identity. That is one of
(at least) three ways to answer "which branch does this datom belong to, and what does
a branch see?", and each is optimal for a different core operation. The driving use
case from §1 (fork-and-diverge, rollback, separate sessions; merge deferred) points at
the third.

### 9.1 The three candidates

**A. Version-vector frontier** (the body of this doc). Branch = a vector
`{ReplicaID→Lamport}`; visibility = `Visible(tx)`; the keyspace is shared and reads
filter it.
- ✅ Merge is O(1) (component-wise max) and conflict-free, which is why it leads when
  merge-back matters.
- ✅ Unbounded: a component per write-stream, no ceiling.
- ❌ Read cost ∝ sibling breadth: reading a branch scans the shared keyspace and
  discards other branches' datoms. With many concurrent sessions, every read pays for
  every sibling.
- ❌ Needs a write-identity per branch (overload `ReplicaID`, or widen `ElementID`).

**B. Content-addressed arena DAG** (256-bit = a hash). Branch = a DAG of immutable,
content-hashed datom-layers; reads merge-scan the reachable layers.
- ✅ Verifiable / replicable / dedup-able: a snapshot is a 32-byte root hash; ship a
  branch by Merkle-diff; identical states collapse to one hash; sealed layers cache
  forever.
- ❌ Reads = k-way merge over reachable arenas; needs sealing (mutable head + immutable
  layers) and compaction, a logical LSM atop Badger's LSM.
- Best when Janus goes distributed/multi-tenant; overkill for local branching.

**C. Tree-path bitmask** (256-bit = a CIDR-style path). Branch = a prefix `P/ℓ`;
ancestry = prefix-match (one masked compare); the prefix is the leading key component.
- ✅ Sibling isolation is physical and free: siblings are disjoint key ranges, never
  scanned. Read cost ∝ ancestry depth, not branch breadth (directly fixes A's worst
  cost).
- ✅ A subtree is a contiguous range: ending/dropping a session = one range delete;
  "everything under `session-42/*`" = one range scan.
- ✅ Dissolves the write-identity problem: branch identity is the path. Reuses L85
  sort-preservation (`EncodeFixed32`) and the existing index layout.
- ❌ Merge breaks the tree. A path-prefix encodes fork perfectly (parent ⊑ child) but
  cannot encode merge: a merge node has two parents whose path-labels are not prefixes
  of one another, so no single prefix can mean "visible to both lineages." The
  structure becomes a DAG and the prefix-as-ancestry invariant is lost. So merge under
  C must either materialize (squash the CRDT-resolved union into a fresh subtree, which
  is O(divergence) and keeps the tree pure) or fall back to a frontier edge (axis A) on
  that one node. Both are deferred (§1).
- ❌ 256-bit budget bounds depth × fanout: deep/unbounded forking needs depth reclaim
  (compaction) or relabeling; capacity & relief valves are worked out in §9.7.

*Rejected reading of "bitmask tree."* "A bitmask that represents a tree" also admits a
membership-bitset interpretation: 256 bits = 256 branches, each a bit; a datom's mask
carries the bits of every branch that may see it; fork copies the parent's bits and
sets one new bit. We rejected it: it caps the system at 256 branches, and it encodes
flat set membership, not hierarchy, so there is no ancestor/subtree structure to
exploit for prefix ancestry tests, contiguous-range subtree GC, or the fork-ceiling
staircase (§10.3). The path (CIDR-style) reading was chosen because it makes ancestry a
prefix test and a subtree a key range.

### 9.2 Side by side

| | A. Frontier | B. Arena DAG | C. Tree-path |
|---|---|---|---|
| Optimized for | merge | verify / replicate / dedup | fork / isolation / subtree-ops |
| Branch identity is… | a version vector (separate, not in-key) | a 256-bit content hash | a 256-bit tree path |
| Fork | O(1) copy vector | O(1) new layer | O(1) sub-prefix |
| Merge | O(1) vector max | new 2-parent node + merge-read | breaks tree → materialize |
| Read cost | ∝ sibling breadth | ∝ #reachable arenas | ∝ ancestry depth |
| Sibling isolation | logical (filter) | logical (layer set) | physical (disjoint ranges) |
| Drop a session/subtree | scan-all + test | DAG mark-sweep | one range delete |
| Snapshot identity | a vector (not verifiable) | 32-byte hash (verifiable) | a prefix (positional) |
| Bounded? | unbounded | unbounded | capped by 256 bits |
| Write-identity | needs ReplicaID/ElementID change | in the layer hash | in the path (dissolved) |

### 9.3 Why C fits the game-engine use case

The workload is a game engine storing all state in Janus, with separate user sessions
forked off a shared world, rollback, and `AsOf()` already in production. That maps onto
a two-dimensional coordinate Janus is half-built for:

```
read scope  =  ( WHERE in the branch tree ,  WHEN in that branch's time )
                └─ arena prefix (NEW: axis C)  └─ Lamport-Tx threshold (EXISTING: AsOf)
```

- Sessions are sibling subtrees off the world root. Under C, session A's reads never
  touch session B's datoms (disjoint ranges). Under A (frontier), every session read
  would scan every other session's divergence: the difference between O(depth) and
  O(#sessions) per read.
- Ending or discarding a session (logout, expiry, a thrown-away speculative "what-if"
  lookahead) = one subtree range delete. Spawning a throwaway branch to simulate a move
  and discarding it is O(1) / O(1).
- Rollback composes the two axes, in three flavors. The default is non-destructive
  (storage is append-only); only hard-truncate destroys:
  1. Soft (reuses AsOf): move the session's read-point to `AsOf(T)` within its prefix
     range. Nothing deleted; post-`T` writes still exist, filtered by Tx, the same
     mechanism already shipping for task-tracking, now prefix-scoped.
  2. Branch-on-rollback (the git-tree of saves): fork a new child subtree from
     `(session, T)` and continue; the old timeline remains as a sibling. "Load an
     earlier save and play differently" is a normal fork, C's native operation.
  3. Hard truncate: range-delete the session's datoms with `Tx > T`, bounded to that
     one session's prefix, never touching the shared world or other sessions.
- Merge deferred (§1) means C's one real weakness doesn't bite yet. When merge-back
  arrives, the bridge is merge-by-materialize (squash the CRDT-resolved union into a
  fresh subtree) or a frontier edge on just that merge; A's machinery (§6) is the
  reference for that round.

### 9.4 Recommended shape: C now, layered with A / B later

- Adopt C (tree-path prefix) as the branch axis, composed with the existing Lamport-Tx
  `AsOf` as the time axis. Smallest delta that delivers the whole primary use case;
  reuses sort-preserving L85 + AsOf; dissolves write-identity.
- Keep depth shallow via periodic materialize-to-fresh-root compaction (rewrite a
  session's current state as a shallow node), reclaiming the 256-bit budget
  ("consolidate saves").
- Reserve A (frontier) for the deferred merge-back round (frontier edge or
  merge-by-materialize, §6).
- Reserve B (content-addressed) for the distributed/replication future
  (`DISTRIBUTED_JANUS.md`), where verifiable/Merkle-diffable snapshots earn their cost;
  its immutable-layer caching also slots into §8.
- The three are layerable: C for coarse, long-lived, fork-shaped structure (sessions,
  saves, tenants); A within a node for fine, merge-heavy, ephemeral work; B as the
  durable/portable seal of a published node.

### 9.5 Caching under C (refines §8)

C makes §8 simpler. The ancestor nodes a session forked from are immutable from that
session's view (a session writes only to its own range, never its ancestors). So each
ancestor's per-`(E,A)` resolution is a permanently valid, shared cache entry (no
version-stamp invalidation), and a session needs only a thin cache over its own
divergence plus the cross-range merge result. This is the content-addressed model's
immutable-cache win, available to C without sealing, because ancestry is append-only
and a session never mutates it.

### 9.6 Decision register

Resolved 2026-06-26 unless marked deferred; trade-offs and full reasoning live in the
cited sections.

- **Branch-identity encoding (§9.1–§9.4): tree-path prefix (C)** as the branch axis,
  composed with `AsOf` for the time axis. Frontier (A) reserved for deferred merge-back;
  content-addressed arenas (B) for the distributed future.
- **Key encoding (§11.1, §11.5): flat 32-byte tree-path prefix** on every key (69→101
  bytes). The R5 group-id split (§9.7) is held as the escape hatch only if in-key depth
  budget becomes a problem.
- **Label scheme (§9.7): fixed two-tier** (e.g. 48-bit session + 8-bit rollback levels),
  with ordinal recycling and transient speculation kept in overlays; ORDPATH only if
  trees prove lopsided.
- **Fork semantics (§10.6): snapshot-isolate.** Each session freezes the world at its
  fork point (world = fork-frozen ancestor). Per-attribute live-inherit is deferred
  (additive — a schema flag later); world-as-live-source not adopted.
- **Rollback default (§10.7): soft** (`AsOf` read-point, non-destructive) is the default
  `Reset`; branch-on-rollback and hard-truncate are explicit.
- **Hard-truncate with descendants (§10.7): forbid.** Error if any descendant's
  fork-point exceeds `T`; the caller drops or re-bases those children first. Cascade is
  a possible future opt-in.
- **`History()` across the chain (§10.11): merged lineage, fork-ceilings applied** — the
  raw datoms the session could have seen (own + ancestors up to fork), unresolved.
- **Branch/snapshot metadata storage: reserved Badger `refs/` keyspace** (`name → path,
  parent, forkLamport`) via `GetMetadataUint64`/`SetMetadataUint64`; dogfooding as
  system-attribute datoms deferred.
- **Clock model (§4.1): one shared counter**, stamped per-arena — globally monotonic,
  tie-free on a single node.
- **Deferred (axis A only):** cache validity-stamp granularity (§8.4) — relevant only if
  a frontier edge is later introduced for merge-back.
- **Open (build-time, §11.5):** key-suffix vs. decoded-tuple merge comparison — an
  implementation detail, not an architectural fork.
- **Sequencing — minimum linear slice first (§12.1):** ship snapshots (read-only `AsOf`
  views) + destructive `TruncateTo` with zero storage-core change, before any
  key-format work. **Verb reservation:** `TruncateTo` is the destructive op; `RollbackTo`
  and `Fork` are reserved for the future non-destructive branch-on-rollback.

### 9.7 Label scheme & capacity (C)

The budget is path entropy, not a count. The 256-bit prefix encodes a node's position;
the number of distinct labels (`2^256 ≈ 10^77`) is never the constraint. What you
spend, along a single root→leaf lineage, is:

```
Σ ceil(log2(fanout_i))  over levels i   ≤   256 bits
```

- Breadth at the root is nearly free: every session-path pays the level-1 cost once;
  it does not compound downward. ~10^6 live sessions ≈ 20 bits at level 1, leaving ~236
  for everything below.
- The only way to exhaust the budget is one lineage that is both deep and wide at many
  levels: the programmatic search-tree shape, not human rollback.

**Capacity — uniform width per level:**

| bits/level | max depth (256/w) | fanout/level |
|---|---|---|
| 1 (binary) | 256 | 2 |
| 4 | 64 | 16 |
| 8 | 32 | 256 |
| 16 | 16 | 65,536 |

Uniform is wrong here (sessions want fanout, rollbacks want depth). A two-tier
level-schema matches the real shape:

| Session field | sessions | Rollback levels (rest/w) | nesting depth | forks/level |
|---|---|---|---|---|
| 48 bits | 2.8×10^14 | 208/8 | 26 | 256 |
| 40 bits | 1.1×10^12 | 216/6 | 36 | 64 |
| 32 bits | 4.3×10^9 | 224/4 | 56 | 16 |

⇒ effectively unlimited sessions and ~25–55 levels of nested branch/rollback. Humans
never approach this; only runaway programmatic forking does (handled below).

**Encoding styles.**
- **Fixed level-schema** (recommended start): pre-assigned widths per level. Ancestry
  = mask-and-compare on fixed boundaries; ranges trivial; dead simple. Rigid, and
  wastes bits on unused fanout.
- **ORDPATH-style variable-length** (upgrade if trees turn out lopsided):
  self-delimiting prefix-free per-level codes; small ordinals → short codes, so
  narrow-and-deep lineages stay cheap. Depth then depends on ordinal magnitude.

Two branch-specific simplifications (vs. the XML / `hierarchyid` setting ORDPATH was
built for):
- **No gap-insertion trick needed.** ORDPATH reserves odd ordinals to insert between
  siblings without relabeling, for document order. Branch siblings have no required
  order; just hand out the next free ordinal per parent.
- **Clean pre-order ranges:** require each child code to lead with a `1` bit (so a
  parent's own zero-padded datoms sort first within its subtree, ahead of any child)
  and carry an 8-bit significant-length. ~1 bit/level; removes padding ambiguity;
  makes "subtree = contiguous range" exact.

**The churn trap.** If ordinals only ever increment, per-level cost is set by total
children ever created, not concurrent. A node that forks-and-discards 10^6 speculative
children pushes its ordinal to ~20 bits at that level though ≤1 is ever alive. Fixes
(both wanted anyway):
1. Recycle dead siblings' ordinals (free-list per parent) → per-level cost ∝ max
   concurrent fanout.
2. Keep ultra-transient speculation in overlays (axis A), not labels. Sub-frame
   "what-if" lookahead lives in an in-memory overlay (zero label cost) and materializes
   into a tree node only when kept. C is for branches you intend to keep; A is for
   ephemeral simulation (§9.4).

**Running out of bits — recovery, by frequency of use:**
- **R2 — Materialize-to-fresh-root** (steady-state relief valve; wanted regardless).
  Write a deep session's net divergence from the world as a new shallow node `S'`
  directly under the world root. `S'` stays a child of root, so it inherits the shared
  base by prefix (no copying the base); only the session's own changes are rewritten
  (O(session divergence)). Drop the deep lineage; depth N→1; the session also stops
  paying ancestor-chain read cost. This is "squash / consolidate saves" and doubles as
  read-perf compaction. Cost: intermediate rollback points collapse, so archive the old
  subtree cold if needed, or rely on the AsOf time-axis going forward.
- **R1 — Subtree relabel.** Renumber a subtree with shorter ordinals. General escape,
  always available, but rewrites every key in the subtree (transactional, O(subtree)).
  Rare.
- **R5 — Group-id + intra-group-path hybrid** (graceful overflow; blends toward B).
  Split the field into `(group-id : 64) | (path : 192)`. Intra-group ancestry =
  prefix-match; cross-group ancestry = one parent pointer per group (a tiny group-DAG).
  When a group's path budget runs low, open a new group whose parent is the current
  node; reads walk the shallow group-DAG, then prefix-match within each. Unbounded
  logical depth, bounded in-key depth: C with a content-addressed (B) escape hatch on
  the high bits.
- **R3 — Widen the field (256 → 512).** Doubles the budget; a key-format change that
  postpones rather than cures. Last resort; R2 should mean you never reach it.

**Recommendation.** Start fixed two-tier (48-bit session + 8-bit rollback levels →
~10^14 sessions, 26 nesting levels, 256 forks/level), with ordinal recycling and
transient sims in overlays; R2 compaction is the relief valve for any long-lived deep
session (run for read-perf anyway). Move to ORDPATH only if real trees prove lopsided
enough to waste too many fixed-width bits.

### 9.8 Write-identity for the frontier encoding (A)

Encoding C dissolves the write-identity question (branch identity is the path, §9.1),
so this matters only for the deferred merge-back path, where a frontier edge (axis A)
reappears (§9.3, §10.7). It is recorded here because it is the one place the design
must reconcile with `DISTRIBUTED_JANUS.md`'s `NodeID`, and because the
frontier-as-ancestry encoding has a precondition: one write-identity per branch (§4.1:
two sibling branches written under the same `ReplicaID` cannot be separated by a
scalar-per-replica frontier, because the sibling's higher Lamports subsume the
other's). Three ways to supply that identity were considered:

- **W1 — redefine `ReplicaID` as a "logical write-stream identity" (recommended for
  A).** A branch allocates a fresh `ReplicaID` (`rand.Uint64()`, exactly as `Database`
  already mints replica ids) for its writes; the frontier is then a version vector over
  write-streams. A physical node × branch is one write-stream; a branch written by two
  physical nodes contributes two frontier components. No key-format change: it
  reuses the 16-byte `ElementID`, the total order, all eight indices, and L85 encoding
  unchanged. It subsumes branches and physical replicas under one mechanism, and it is
  the coordinated vector basis that `SHARED_ARENA_DISTRIBUTION.md` Theorem 7 already
  calls for. The only cost is conceptual: `ReplicaID` stops meaning
  "physical node" (its `DISTRIBUTED_JANUS` sense) and starts meaning "write-stream," of
  which a node may own many.

- **W2 — widen `ElementID` to `(Lamport, NodeID, BranchID)` (rejected unless W1 proves
  insufficient).** This cleanly separates physical node from branch, but it grows the
  ID from 16 to 24 bytes, which ripples through the fixed 69-byte key layout, the L85
  encoding, and every one of the eight indices — a storage-core change. It is
  unnecessary if W1 holds, because W1 already gets the separation by treating
  `(node, branch)` as a single derived write-stream id without touching the key format.

- **W3 — keep `BranchID` outside the `ElementID`, in frontier bookkeeping only
  (rejected).** This reintroduces the sibling-collision problem (§4.1): two siblings
  written by the same physical `ReplicaID` are again indistinguishable in the datom's
  stored coordinate, so a version vector cannot separate them. Not viable on its own;
  combining it with W1 is W1.

This whole question is moot under the recommended encoding C and returns only if a
frontier edge is introduced for O(1) merge-back; **W1 is the answer to reach for
then.**

### 9.9 What the content-addressed arena (B) buys, and what it costs

§9.1 summarized B in two bullets; the full case is recorded here because B is the
encoding the distributed/replication future (`DISTRIBUTED_JANUS.md`) will want, and its
advantages are non-obvious.

**Why 256 bits.** A content address must be collision-resistant: a 64-bit id has a
birthday bound near 2³² (collisions become likely at a few billion arenas, feasible at
scale), 128 bits is marginal, and 256 bits (SHA-256) is the first size that is
cryptographically safe (~2¹²⁸ birthday bound). So a 256-bit prefix is itself the signal
that the intended reading is a content hash, not a random or positional id, which is
what distinguishes B from a random arena scheme.

Five things B uniquely buys (none available to A or C):
1. **Verifiable, O(1) snapshot identity.** A snapshot is a 32-byte root hash; two
   snapshots are equal iff their hashes match, and integrity is checkable by rehash. A
   version vector (A) names a frontier but cannot attest to contents; a tree-path (C)
   is positional, not content-derived.
2. **Merkle-diff replication + structural dedup.** Ship a branch by sending only the
   layer hashes a peer lacks (`git fetch` / OCI layer pull); identical datom-sets on two
   branches collapse to a single hash automatically. Anti-entropy becomes a Merkle-DAG
   walk.
3. **DAG-reachability GC.** Dropping a branch is ref-counting / mark-sweep over 32-byte
   hashes (git gc). Contrast A, whose GC must scan every datom and test it against every
   live frontier; B's GC never touches datom payloads at all.
4. **Immutability makes caching simpler, not harder.** A sealed (frozen,
   content-addressed) layer's per-`(E,A)` resolution is valid forever; it can never be
   invalidated, because the layer can never change. The cache splits into immutable
   per-layer caches (shared, permanent) plus a thin per-branch merge cache. (This is the
   same property C earns from append-only ancestry without sealing, §9.5/§10.9.)
5. **Storage-tier and distribution unification.** A layer can live in memory, on disk,
   in a blob store, or on a peer, addressed uniformly by hash. `SHARED_ARENA`'s memory
   arena and this storage arena become the same concept.

**What it costs:**
- **32 bytes × 8 indices × every datom.** The fixed 69-byte key grows to ~101 bytes
  (+46%), in every one of the eight indices. As a sorted leading prefix, a run of one
  arena's keys delta-compresses to near-zero on disk (Badger/LSM prefix compression),
  so the cost is in iteration/memory, not bytes-at-rest, but it is real and must
  be measured, not assumed.
- **Reads become k-way merges** over the reachable layer set, deduplicating shared
  ancestors (diamonds): LSM-style read amplification, bounded only by compaction
  (periodically sealing a branch's layer stack into one arena, git repack).
- **CRDT resolution moves above the merge:** a k-way merge-by-Tx first restores global
  order, then the resolver runs over the merged stream.
- **Content-addressing forces sealing.** A hash needs frozen contents, so an active
  branch needs a mutable head layer that is periodically sealed into immutable
  content-addressed layers (the memtable→SSTable split, or git's index→commit).
- **Diamonds need dedup:** a node reachable by two paths must not be counted or resolved
  twice.
- **Net:** B is the content-addressed-immutable-layer pattern (git's object model,
  OCI image layers, Nix store paths, Datomic's segments) applied to datoms, plus a
  second, logical LSM layered on top of Badger's own LSM. That is a large mechanism. It
  earns its place when Janus needs verifiable, replicable, dedup-able snapshots (the
  distributed future) and is overkill for local branching, where C already
  delivers isolation and cheap fork/teardown.

---

## 10. Read path: ancestor-chain merge (axis C)

This section is the execution counterpart to §9's choice of the tree-path encoding
(C). It is written out in full because the read path is where the encoding meets the
existing `CRDTResolvingIterator`, and the details (group splitting, the byte-suffix
merge, the fork-ceiling staircase, and how `AsOf(T)` composes) each carry a correctness
argument.

### 10.1 Why a single range scan is not enough

With axis C the arena (tree-path) prefix is the leading component of every key, so
each of the eight indices becomes (the current 69-byte key layout, with the prefix
prepended):

```
EATV:  [path:32B][E:20B][A:32B][Tx↓:16B][V…][Op:1B]
AVET:  [path:32B][A:32B][V…][E:20B][Tx↓:16B][Op:1B]
…and so on for all eight indices
```

Within a single arena the path bytes are constant, so the rest of the key sorts
exactly as it does today. But across arenas the leading prefix differs, so all of
arena X's keys sort before all of arena Y's keys whenever `path(X) < path(Y)`: each
arena occupies a disjoint, contiguous key range.

The consequence for resolution: a single `(E,A)` group is physically split across the
ancestor arenas. Suppose the world root set `e :hp 100`, parent `P` overwrote it to
`90`, and session `Q` overwrote it to `80`. Those three datoms live in three different
path ranges:

```
[path=root][e][:hp][Tx↓…]  → 100 @ L5
[path=P   ][e][:hp][Tx↓…]  →  90 @ L40
[path=Q   ][e][:hp][Tx↓…]  →  80 @ L70
```

A naive "scan `Q`'s range" sees only `80@L70` and misses the inherited base. A naive
"scan the whole keyspace" sees every session's `:hp`, not just `Q`'s lineage. Neither
is correct. We must scan exactly `Q`'s ancestor chain and re-collapse those split
groups so the resolver sees one contiguous `(E,A)` group in Tx-descending order; that
re-collapse is a merge.

**Why leading and not trailing.** Putting the arena id at the front of the key is what
buys physical contiguity: an arena (and a whole subtree) is one key range, so sibling
sessions never interleave, and ending a session is one range delete (§9.1). The
alternative, a trailing arena suffix, would preserve the global index order (no merge,
no per-arena ceilings to stitch together) but would scatter each arena's keys across
the whole keyspace, at which point reading a branch is again a per-datom filter over a
shared keyspace, i.e. the frontier encoding (A) with a fatter 32-byte tag. So the
leading-prefix choice is what makes C a different architecture from A rather than a
heavier spelling of it; the merge in §10.2 is the price of that choice.

### 10.2 The merge: k-way, lexicographic, on key-suffixes

`Q`'s ancestor chain is the sequence of path prefixes from the world root down to `Q`,
and it is read directly off `Q`'s own path — the path encodes ancestry (each
node-boundary prefix is an ancestor, §9.7), so no separate parent-pointer walk is
needed to enumerate the chain. For a chain `[A₀=root, A₁, …, A_k=Q]` (k = depth), the
matcher opens k+1 sub-iterators, one per arena, each a standard index scan over
`[path_i][pattern-key…]`. Because the path prefix is constant within a sub-iterator,
each sub-iterator yields datoms in the existing index order; for EATV that is
`[E][A][Tx↓]`.

The trick that keeps this cheap: strip the index byte and 32-byte path prefix and the
per-arena streams are byte-comparable in the order the resolver wants. Two facts make
it work:

- The eight indices already encode `Tx` with a bitwise NOT so that a higher `Tx` sorts
  first, descending (`storage/element_id.go`, key encoder). So within an `(E,A)` group
  the existing byte order is Tx-descending, which is what "first entry wins" LWW already
  relies on.
- All arenas share the identical key layout after the path prefix. So comparing two
  datoms from different arenas by their key suffix (everything after the index byte and
  32-byte path) yields exactly `(E asc, A asc, Tx desc)`, the resolver's required order.

The merge is then a standard k-way lexicographic merge on key-suffixes: at each step,
advance whichever sub-iterator has the smallest suffix. No custom comparator, no value
decoding for ordering (decode only to hand a datom to the resolver). The output is a
single stream in which each `(E,A)` group is contiguous (gathered across all arenas)
and Tx-descending within, bit-for-bit the shape a single-arena scan produces today. So
the `CRDTResolvingIterator` consumes it unchanged:

```go
// matcher: read pattern in session Q, query-time AsOf = T (T = ∞ for "latest")
chain := Q.ancestorChain()                 // [root, …, Q], from Q's path prefixes (§9.7)
subs  := make([]Iterator, len(chain))
for i, arena := range chain {
    subs[i] = store.ScanIndex(idx, arena.pathPrefix, patternKey).
                    WithTxCeiling(ceiling(arena, chain))   // structural fork ceiling; ∞ for the leaf
}
merged := NewAncestorMergeIterator(subs)   // k-way merge on key-SUFFIX (bytes after the 32-byte path)
return NewCRDTResolvingIterator(merged, schema, T, uniqueMatcher)   // UNCHANGED; T is the "when"
```

`AncestorMergeIterator` implements the `Iterator` interface (`Next` / `Datom` / `Seek`
/ `Close` / `Error`), so the resolver wraps it transparently. `Seek(key)` fans out to
seek every sub-iterator (each prepends its own path prefix to `key`); `Error()` returns
the first sub-iterator error; `Close()` closes all subs. This is one focused new type;
the resolver and the index-selection logic are untouched. We extend the source, not the
resolver.

### 10.3 The isolation bug, and the fork-ceiling staircase

A naive "merge all ancestor arenas with no time bound" is wrong, and the bug is subtle.
Suppose `Q` forks from parent `P` at Lamport 100. After the fork, `P` keeps writing
(another session's activity, or world-head updates), producing datoms in `P`'s arena
with `Tx > 100`. Those are `P`'s future, on a timeline `Q` never witnessed. But they
live in `P`'s arena, so the naive merge pulls them into `Q`'s read. `Q` would see `P`'s
post-fork divergence. Isolation broken.

The fix: each ancestor is read with a Tx ceiling equal to the fork-Lamport of its child
in the chain. Forking `Q` from `P` at Lamport 100 means "`Q` inherits `P`'s state as of
Lamport 100." So when reading through `P` on `Q`'s behalf, apply ceiling `F = 100`: `P`
contributes only datoms with `Tx ≤ 100`. The ancestor chain becomes a staircase of
nested AsOf snapshots:

```
root :  Tx ≤ F₁     (F₁ = Lamport when the session line forked off root)
  P  :  Tx ≤ F₂     (F₂ = Lamport when Q forked off P)
  Q  :  Tx ≤ ∞      (the leaf — capped only by the query, §10.4)
```

Each ancestor is frozen at the instant its child diverged; the leaf is open-ended. This
is the semantics we want: a session sees the world as it was at the instant it forked,
plus its own subsequent changes.

The metadata cost is one number per arena: `forkLamport(arena)` = the Lamport at which
that arena was created (forked from its parent). The parent itself is implicit in the
path (strip one level), so no parent pointers are stored; only the fork-Lamport is
extra state. Reading `Q`: walk the prefixes of `Q`'s path, look up each one's
`forkLamport`, and the ceiling applied to ancestor `A_i` is `forkLamport(A_{i+1})`, the
fork-point of the next-deeper arena in the chain.

In other words, fork is AsOf inheritance: the whole ancestor chain is a sequence of
nested AsOf views, which is why it reuses the AsOf machinery (§10.4) rather than
introducing a parallel notion of "visible."

### 10.4 How `AsOf(T)` composes (two filters meeting at `min`)

There are now two independent time bounds on a read, and they compose without any
new logic:

- **Structural fork-ceilings `F_i`**: applied by each ancestor sub-iterator (the leaf
  has no structural ceiling, ∞). These encode the tree's shape, where a session
  diverged from each ancestor. They are fixed by the fork events and never change.
- **The query AsOf `T`**: applied by the `CRDTResolvingIterator`'s existing, unchanged
  per-datom filter (`if it.txID.Less(datom.Tx) { continue }`, line 160). This is the
  when of the read: "show me this session as of time T" (T = ∞, encoded as
  `ElementID{}`, for "latest").

A datom survives iff it passes both the upstream per-arena `F_i` and the downstream
global `T`. Two `≤` bounds AND-ed together is a `min`, so the effective visibility of a
datom is:

```
ancestor A_i :  visible(d)  ⟺  d.Tx ≤ min( F_i , T )
leaf Q       :  visible(d)  ⟺  d.Tx ≤ T                 (F = ∞)
```

Nothing had to be added to make them compose: `F_i` lives in the sub-iterator, `T`
lives in the resolver, and the conjunction is automatic. The branch axis ("where", via
arena selection + fork ceilings) and the time axis ("when", via the existing AsOf
filter) are orthogonal filters stacked on one stream. The three cases:

- **Latest read of `Q`** (`T = ∞`): the resolver's filter is a no-op
  (`txID == ElementID{}`); each ancestor is bounded by its structural `F_i`; the leaf
  is unbounded. `Q` sees the newest state of its own lineage.
- **Soft rollback / `AsOf(T)`**: set the query AsOf to `T`. Ancestors become
  `min(F_i, T)`, the leaf becomes `T`. If `T` is after every fork point, the ancestors
  are unaffected (their `F_i ≤ T`) and only the leaf is trimmed: you are rewinding
  within the session's own writes. If `T` is before a fork point, that ancestor is
  further trimmed to `T` and the session's own (later) writes vanish entirely. Both
  cases are correct and need no special-casing; `min` handles them.
- **Rollback to a `T` before `Q` even existed**: the leaf has no datoms `≤ T`, so `Q`
  contributes nothing; ancestors are capped at `min(F_i, T) = T`; the read shows the
  lineage's state at `T`, which is entirely ancestor state. Correct.

Because rollback only sets `T`, it is non-destructive and reuses the AsOf mechanism
already in production for task-tracking, now scoped under `Q`'s prefix instead of
globally. (The destructive variants are §10.7.)

### 10.5 Worked trace

```
Arena layout (one EATV-ordered keyspace; ‖ marks a path-prefix boundary):

  ‖ root ‖ e :hp 100 @ L5          (root is the world)
  ‖ root ‖ e :hp  90 @ L15         ← world-head update AFTER the session forked
  ‖ sess ‖ e :hp  80 @ L70         forkLamport(sess) = 10   (sess forked off root at L10)
  ‖ sess ‖ e :mp  50 @ L71
```

Chain for `sess` = `[root, sess]`. Ceilings: root → `forkLamport(sess) = 10`; the leaf
`sess` → ∞ (then capped by the query `T`).

**Read `sess`, latest (`T = ∞`):**
- root sub-iterator, ceiling 10: emits `:hp 100@L5` (5 ≤ 10 ✓); **skips** `:hp 90@L15`
  (15 ≤ 10 ✗ — the post-fork world update, correctly invisible to this session).
- sess sub-iterator, ceiling ∞: emits `:hp 80@L70`, `:mp 50@L71`.
- merge on suffix → group `(e,:hp)` = `[80@L70, 100@L5]` (Tx desc); group `(e,:mp)` =
  `[50@L71]`.
- resolver: `(e,:hp)` CardinalityOne → first entry `80` wins; `(e,:mp)` → `50`.
- **Result: `:hp = 80, :mp = 50`** — the session's own value over a fork-frozen base,
  ignoring the world's post-fork change.

**Read `sess` AsOf `T = 20` (soft rollback to before the session's own writes):**
- root ceiling `min(10, 20) = 10`: emits `:hp 100@L5`.
- sess ceiling `min(∞, 20) = 20`: `:hp 80@L70` (70 ≤ 20 ✗) and `:mp 50@L71` (✗) both
  **skipped**.
- merge → `(e,:hp)` = `[100@L5]`; no `:mp`.
- **Result: `:hp = 100, :mp` absent.** The pre-damage state, reached by lowering `T`,
  no deletes.

### 10.6 A mutable shared world needs no special handling

A natural worry: if many sessions fork off a shared world, and the world keeps being
updated, must the world be frozen? No. The fork-ceiling makes each ancestor logically
immutable from a given session's view even while it is physically still growing.
World-head can append `:hp 90@L15`, `:hp 70@L200`, … to the root arena forever; session
`sess` (forked at L10) never sees anything with `Tx > 10` there. A new session
forked at L250 sees the world up to L250. The ceiling is per-(session, ancestor), so
each session is frozen at its own fork point against a single, ever-growing world
arena. There is no copy-on-fork and no per-session world snapshot: the ceiling is the
snapshot. (This also sharpens §9.5's "immutable ancestor" caching claim: it is not that
the ancestor arena stops growing, but that the prefix of it visible to a given session,
`Tx ≤ F`, is immutable. See §10.9.)

**The alternative: live-inherit, and a topology choice.** Snapshot-isolation is a
default, not a law. Reading an ancestor with ceiling = ∞ instead of its fork-point
("live-inherit") lets the world's post-fork writes flow into the session. The hazard: a
world write at `Tx > F` then beats the session's own earlier edit by LWW (the living
world silently clobbers a session's local change), which is why snapshot-isolation is
the right default. But live-inherit is legitimately wanted for some state (a global
clock, weather, server-wide events that should reach every live session), so the real
design space is per-attribute: most attributes snapshot at fork; a declared few
live-inherit. Underneath sits a topology decision: is the shared world an ancestor
sessions snapshot at fork, or a live source they join? The ceiling model assumes the
former (world-head is a mutable branch; sessions freeze at their fork point, §9.3).
**Decided (2026-06-26): snapshot-isolate is the default** — the world is a fork-frozen
ancestor (the ceiling model). Per-attribute live-inherit is deferred (additive; a
schema flag added later); world-as-live-source was not adopted. See the §9.6 register.

### 10.7 Rollback variants, precisely

The three flavors differ in what (if anything) they destroy:

- **Soft (default):** read `AsOf(T)` within the session prefix (§10.4).
  Non-destructive; the session's post-`T` writes remain and become visible again if you
  raise `T`. This is "scrubbing the timeline," and it is the default `Reset`.
- **Branch-on-rollback:** `Fork(sess, T)` — create a child arena whose
  `forkLamport = T` and continue writing there. The original `sess` lineage is
  untouched and remains a sibling ("the save you came back from"). This is the git-tree
  of saves, and mechanically it is an ordinary fork whose ceiling is in the past.
- **Hard truncate:** range-delete the session's own datoms with `Tx > T`, bounded to
  the key range `[sess-prefix + after-T]`. Destructive, but confined to that one
  session's arena: it cannot touch the shared world (different prefix) or sibling
  sessions. Use it when you want to reclaim the post-`T` writes, not merely hide them.

> **Correctness rule: hard-truncate vs descendants.** Hard-truncate is safe only if the
> session has no descendants forked after `T`. A child forked at `T' > T` inherits its
> base through a ceiling `forkLamport = T'` that points into the truncated range;
> deleting those datoms would orphan the child's inherited state. (This was not caught
> when the rollback variants were first sketched, a gap in the reasoning, not just the
> writeup.) **Decided (2026-06-26): forbid.** Hard-truncate errors if any descendant's
> fork-point exceeds `T`; the caller must drop or re-base those children first. This is
> cheap to enforce: the `refs/` keyspace records each branch's parent and `forkLamport`
> (§11.2), so descendants past `T` are a direct lookup. Cascade (auto-invalidating
> descendants) is a possible future opt-in, not the default. Soft rollback and
> branch-on-rollback carry no such hazard: they delete nothing.

### 10.8 Cost, and why a tree beats a DAG on reads

- **Point read** (CardinalityOne, E and A bound — e.g. "this session's `:hp`"): each
  sub-iterator does a direct seek to `[path_i][E][A]` (a key prefix), so it is k+1
  seeks, not k+1 scans. The merge surfaces the highest-Tx visible datom first; the
  resolver emits it and skips the rest of the group. A point read is therefore
  O(depth), short-circuited; depth is held shallow by R2 compaction (§9.7).
- **Range scan** (e.g. "all of entity X's attributes," or an AVET value scan): each
  sub-iterator scans its slice; cost is depth × per-arena slice, merged. Again
  depth-bounded.
- **No dedup needed.** Because C is a tree, every datom has exactly one home arena and
  the ancestor chain is a simple path: there are no diamonds, so the merge never sees
  the same datom twice and needs no dedup pass. (Contrast model B, the content-addressed
  DAG, where a node reachable by two paths must be de-duplicated; and contrast what
  happens once merge-back lands and turns C's tree into a DAG, which is where dedup, and
  the hard part of merge, enter. One more reason merge-back is the deferred case.)
- **Tombstones across arenas** need no special handling. A session locally "deletes" an
  inherited attribute by writing a `Remove` tombstone in its own arena at high `Tx`. The
  merge places it first in the group; the CardinalityOne resolver sees `Op = Remove`
  first and reports the attribute absent, for that session only. Other sessions (whose
  ceilings exclude the tombstone, or who never scan this session's arena) still see the
  inherited value. CardinalityMany add-wins composes identically: a session's
  `Remove@high-Tx` for a set member beats the inherited `Add@low-Tx` in `processAddWins`,
  removing it from that session's view alone.
- **RGA (vectors) across arenas** is correct by causal closure. A session inserting into
  a vector writes RGA elements whose `AfterRef` points at the element they follow. That
  anchor was inserted earlier (at `Tx ≤ fork-point` if inherited, or in the session's
  own arena). Either way it is visible under the session's ceilings (an inherited anchor
  has `Tx ≤ F`, which the ancestor ceiling admits). So `resolveRGAGroup` always finds
  every `AfterRef` target inside the merged group; no dangling references. This is the
  same causal-closure argument that makes whole-branch merge safe (§6.2, edge 3), and
  the same reason cherry-pick, which can strand an `AfterRef`, is unsafe.

### 10.9 Caching keyed by `(arena, ceiling)` — refining §9.5

A cache entry's validity scope is exactly an `(arena, ceiling)` pair — which is
the shape of the existing per-`AsOf`-handle cache (`storage/cache.go`: a separate
`Cache` instance per snapshot). An ancestor view `(A_i, F_i)` is the resolution of
`A_i`'s `(E,A)` groups restricted to `Tx ≤ F_i` (a frozen prefix of an append-only
arena), so it is immutable and shared by every session that forked from `A_i` at the
same point `F_i`. The hot shared-world base is therefore resolved and cached once and
reused by all sessions forked at that point; each session keeps only a thin cache over
its own arena's divergence, plus the merged result. Two sessions that forked from the
world at different times get different `(root, F)` views, correctly distinct, each
itself immutable and cacheable (the AsOf cache machinery already in place, now keyed by
fork point). The freshness machinery (`maxVersions`/`attrVersions`) need only track the
leaf arena, the one the session is actively writing; ancestor views never invalidate.

### 10.10 Arena-aware unique resolution

Everything above is the E-primary group-resolve path (EATV/EAVT), which feeds the
`CRDTResolvingIterator` a merged stream. The value-lookup / unique path is structurally
different and needs the same fan-out applied in its own shape. Today,
resolving "who owns `(A, V)`" (and the unique entity-view fallback) is
candidate-and-validate: an AVET scan produces candidate entities, each validated by an
EATV-LWW point lookup (`matchWithVValidation` / `walkApplyEntry`, per
`CRDT_UNIQUE_SEMANTICS.md`). Under axis C both halves fan across the chain:

- the AVET candidate scan becomes an ancestor-chain merge over the AVET ranges (so
  candidates from the inherited world and the session's own writes are seen), with the
  same per-arena ceilings; and
- each per-candidate EATV validation becomes an ancestor-chain point-merge (the
  §10.2 point read).

This is the concrete form of the §4 / §7.5 note "thread the filter into the AVET
sub-scan": the sub-scan is not merely filtered, it is fanned across the ancestor chain
and ceiling-bounded like every other read. With that, unique `(A,V)`-LWW resolves over
exactly the session's visible set, and §7's "merges are uniqueness-safe" guarantee
carries over to session reads unchanged. This, plus `AncestorMergeIterator`, is the
entire new read-path surface; the resolver, the cardinality logic, and index selection
are all reused as-is.

### 10.11 `History()` across the chain

`d.History()` returns raw datoms with no CRDT resolution (`isHistoryMode`, §8.1).
Across a session's ancestor chain this raised a question: should history show only the
session's own arena (its local edit log) or the merged lineage (the session plus every
ancestor it inherited)? And if merged, do the fork-ceilings apply: "the raw history
this session could ever have seen" (ceilings on) versus "every raw datom physically in
the lineage's arenas" (ceilings off, which would expose ancestors' post-fork
divergence)? The merge machinery (§10.2) composes with history mode (feed it the
merged stream and skip the resolver), so the choice is a semantic one.
**Decided (2026-06-26): the merged lineage with fork-ceilings applied** — the raw
datoms the session could ever have seen (its own writes plus ancestors up to their fork
points), unresolved. Own-arena-only was rejected (it loses inherited history);
ceilings-off was rejected (it exposes timelines the session never saw). The world's
full raw history remains available by calling `History()` on the world-head handle.

---

## 11. Implementation sketch (axis C)

Concrete shapes for the recommended encoding, grounded in the existing types: the
`Iterator` interface (`Next`/`Datom`/`Close`/`Seek`/`ElementID`/`Error`,
`storage/store.go`), `BadgerStore.Scan(index, start, end)`, the `BinaryKeyEncoder`
(1-byte index discriminator + parts; Op always last), and `Database.AsOf`'s
struct-clone pattern. These are sketches for review, not final APIs; the architectural
commitments they embed are listed in §11.4 and the points still worth scrutiny in
§11.5.

### 11.1 Key layout — the 32-byte arena prefix

Today every key is `[1-byte index][components…][Op]`: `EncodeKey` does
`concatBytes(prefix, …)` with `prefix = []byte{byte(index)}`, and `DecodeKey` does
`key = key[1:]` before parsing. Axis C injects the arena path as the leading data
component, immediately after the index byte:

```
EATV today:   [idx:1][E:20][A:32][Tx↓:16][V…][Op:1]
EATV axis C:  [idx:1][arena:32][E:20][A:32][Tx↓:16][V…][Op:1]
```

Three mechanical touch-points in `BinaryKeyEncoder`, identical injection for all eight
indices:

```go
const arenaPathLen = 32
type ArenaPath = [32]byte
var rootArena ArenaPath // all-zero = the world root

// EncodeKey gains the arena; it becomes the first concat part after the index byte.
//   key := concatBytes(prefix, arena[:], sd.E[:], sd.A[:], txDesc[:], vBytes)   // EATV
// EncodePrefix / EncodePrefixRange: arena is just the first `part`, so callers pass it:
//   enc.EncodePrefixRange(EATV, arena[:], eBytes[:], aBytes[:])
// DecodeKey: after `key = key[1:]`, also `arena := key[:32]; key = key[32:]`, then unchanged.
```

**Cost & migration (architectural — your call).** Every key grows 69→101 bytes (+46%)
across all eight indices. As a sorted leading prefix the arena bytes delta-compress to
near-zero on disk (Badger/LSM prefix compression), so the cost is in
iteration/memory, not bytes-at-rest (§9.9), but it is real and must be measured. The
world root is the all-zero path, so existing data conceptually lives at root; a format
migration either prepends the zero prefix to existing keys or bumps a storage version
and treats an absent prefix as root. This is the only storage-core commitment in the
proposal.

### 11.2 Session handle + `Fork`

A branch handle is the read-write generalization of `AsOf` (which clones the
`Database` struct sharing `store`/`clock`, sets `temporalTxID`, and panics on write).
The session handle instead carries an arena and its ancestor chain, and does permit
writes:

```go
// arenaRef is one rung of the fork-ceiling staircase (§10.3): an arena and the Tx
// ceiling at which its child diverged. The leaf carries maxElementID (unbounded; the
// query-time AsOf T is applied separately by CRDTResolvingIterator, §10.4).
type arenaRef struct {
    path    ArenaPath
    ceiling datalog.ElementID
}

// added to Database:
//   arena ArenaPath    // this handle's own write target / leaf of the chain
//   chain []arenaRef   // root..self, each ancestor frozen at its child's fork point

func (d *Database) Fork(name string) (*Database, error) {
    child := d.allocateChildPath(d.arena)   // next free ordinal under d.arena (label scheme §9.7)
    forkAt, _ := d.store.MaxElementID()     // committed high-water (not Peek; §10.3): ceiling for d.arena

    if err := d.store.putBranchRef(name, child, d.arena, forkAt); err != nil {
        return nil, err                     // refs keyspace (§9.6): name → (path, parent, forkLamport)
    }

    // The child's chain = d's chain, but d.arena flips from leaf (unbounded) to a
    // ceiling-bounded ancestor, and the new child becomes the unbounded leaf.
    chain := append([]arenaRef(nil), d.chain...)
    chain[len(chain)-1].ceiling = forkAt
    chain = append(chain, arenaRef{path: child, ceiling: maxElementID})

    return &Database{
        store: d.store, schema: d.schema, clock: d.clock, replicaID: d.replicaID,
        cache: NewCache(),          // own (E,A) cache layer; ancestors served from shared (§10.9)
        arena: child,
        chain: chain,
    }, nil
}
```

Writes from `child.NewTransaction()` stamp keys with `child.arena` (the write target);
Lamports still come from the shared `d.clock`, so they stay globally monotonic and
tie-free (§4.1). `Reset` / branch-on-rollback (§10.7) is just `Fork` with `forkAt` set
to a past `T`.

### 11.3 Read path: `ceilingIterator` + `ancestorMergeIterator`

The matcher fans the chosen index across the chain, ceiling-bounds each sub-scan,
merges on key-suffix, and hands the merged stream to the unchanged
`CRDTResolvingIterator`:

```go
// chainScan fans one index across the ancestor chain and merges back to a single
// (E,A,Tx↓)-ordered stream. The caller wraps the result in NewCRDTResolvingIterator.
func (m *BadgerMatcher) chainScan(index IndexType, parts ...[]byte) (Iterator, error) {
    subs := make([]keyedIterator, 0, len(m.chain))
    for _, ar := range m.chain {
        allParts := append([][]byte{ar.path[:]}, parts...)        // arena = leading part
        start, end := m.store.encoder.EncodePrefixRange(index, allParts...)
        raw, err := m.store.Scan(index, start, end)               // existing factory
        if err != nil { closeAll(subs); return nil, err }
        subs = append(subs, &ceilingIterator{src: raw, ceiling: ar.ceiling})
    }
    return newAncestorMergeIterator(subs), nil
}
```

```go
// ceilingIterator yields only datoms with Tx ≤ ceiling — the fork-frozen view of one
// ancestor arena (§10.3). ceiling == maxElementID (the leaf) skips nothing. Uses the
// cheap ElementID() accessor, not Datom(), for the filter. The query-time AsOf T is
// applied downstream by CRDTResolvingIterator, so visibility = Tx ≤ min(ceiling, T) (§10.4).
type ceilingIterator struct {
    src     keyedIterator
    ceiling datalog.ElementID
}
func (it *ceilingIterator) Next() bool {
    for it.src.Next() {
        if !it.ceiling.Less(it.src.ElementID()) { // ElementID() ≤ ceiling → keep
            return true
        }                                          // else: above fork point, invisible — skip
    }
    return false
}
func (it *ceilingIterator) Key() []byte                    { return it.src.Key() }
func (it *ceilingIterator) Datom() (*datalog.Datom, error) { return it.src.Datom() }
func (it *ceilingIterator) ElementID() datalog.ElementID   { return it.src.ElementID() }
func (it *ceilingIterator) Seek(k []byte)                  { it.src.Seek(k) }
func (it *ceilingIterator) Close() error                   { return it.src.Close() }
func (it *ceilingIterator) Error() error                   { return it.src.Error() }
```

```go
// keyedIterator is Iterator plus raw-key access, needed for the suffix merge. Adding
// Key() to BadgerIterator is trivial (it already holds the key); this is the one
// Iterator-interface addition the read path requires.
type keyedIterator interface {
    Iterator
    Key() []byte
}

// ancestorMergeIterator k-way-merges the chain's sub-iterators into one stream ordered
// by key-suffix == (E asc, A asc, Tx desc) — bit-identical to a single-arena scan, so
// CRDTResolvingIterator consumes it unchanged (§10.2). k = ancestry depth (small), so a
// linear min-scan beats a heap. No dedup: in a tree each datom has exactly one home
// arena, so no (E,A,Tx) appears twice across subs (§10.8).
type ancestorMergeIterator struct {
    subs    []keyedIterator
    heads   []*datalog.Datom   // current datom per sub; nil = exhausted
    keys    [][]byte           // current raw key per sub (for ordering)
    primed  bool
    cur     int                // sub last emitted from; -1 before first / when done
    current *datalog.Datom
    err     error
}

func newAncestorMergeIterator(subs []keyedIterator) *ancestorMergeIterator {
    return &ancestorMergeIterator{subs: subs, cur: -1}
}

func (it *ancestorMergeIterator) advance(i int) {
    if it.subs[i].Next() {
        d, err := it.subs[i].Datom()
        if err != nil {
            if it.err == nil { it.err = err }
            it.heads[i], it.keys[i] = nil, nil
            return
        }
        it.heads[i], it.keys[i] = d, it.subs[i].Key()
    } else {
        if e := it.subs[i].Error(); e != nil && it.err == nil { it.err = e }
        it.heads[i], it.keys[i] = nil, nil
    }
}

func (it *ancestorMergeIterator) Next() bool {
    if !it.primed {
        it.heads = make([]*datalog.Datom, len(it.subs))
        it.keys = make([][]byte, len(it.subs))
        for i := range it.subs { it.advance(i) }
        it.primed = true
    } else if it.cur >= 0 {
        it.advance(it.cur)            // refill only the sub we just emitted from
    }
    it.cur = it.selectMin()
    if it.cur < 0 { it.current = nil; return false }
    it.current = it.heads[it.cur]
    return true
}

// selectMin: smallest head by key-suffix (bytes after [idx:1][arena:32]). That suffix
// is [E][A][Tx↓]… so byte order == the resolver's required (E,A,Tx↓) order, by construction.
func (it *ancestorMergeIterator) selectMin() int {
    best := -1
    for i := range it.subs {
        if it.heads[i] == nil { continue }
        if best == -1 || bytes.Compare(it.keys[i][1+arenaPathLen:], it.keys[best][1+arenaPathLen:]) < 0 {
            best = i
        }
    }
    return best
}

func (it *ancestorMergeIterator) Datom() (*datalog.Datom, error) { return it.current, nil }
func (it *ancestorMergeIterator) ElementID() datalog.ElementID {
    if it.current != nil { return it.current.Tx }
    return datalog.ElementID{}
}
func (it *ancestorMergeIterator) Seek(k []byte) {
    for _, s := range it.subs { s.Seek(k) } // each re-seeks within its own arena range
    it.primed, it.cur = false, -1
}
func (it *ancestorMergeIterator) Close() error {
    var first error
    for _, s := range it.subs {
        if e := s.Close(); e != nil && first == nil { first = e }
    }
    return first
}
func (it *ancestorMergeIterator) Error() error { return it.err }
```

Integration is then one substitution in the matcher: where it does
`store.Scan(...)` → `NewCRDTResolvingIterator(...)` today, it does
`chainScan(...)` → `NewCRDTResolvingIterator(...)`. Index selection, the resolver,
cardinality logic, and the unique candidate-and-validate path (which calls `chainScan`
for both its AVET candidate scan and its per-entity EATV validation, §10.10) are
otherwise untouched.

### 11.4 Architectural commitments embedded here (confirm before building)

1. **Key format +32 bytes** in all eight indices, plus the root-arena migration
   (§11.1) — the largest commitment. Confirmed 2026-06-26 (flat prefix chosen over the
   R5 split; §11.5).
2. **One `Iterator`-interface addition:** `Key() []byte` — the merge needs raw-key
   ordering; trivial on `BadgerIterator`, which already holds the key.
3. **Branch metadata in a `refs/` keyspace** (`name → path, parent, forkLamport`), per
   §9.6.
4. **Session handle is a writable `Database` clone** — unlike `AsOf`/`History`, which
   panic on write.

### 11.5 Scrutiny points

- **Key encoding — decided (2026-06-26): flat 32-byte prefix.** The arena id is a flat
  leading 32-byte tree-path on every key (§11.1); the R5 group-id split
  (`(group-id : 64) | (path : 192)`, §9.7) is held as the escape hatch only if the
  in-key depth budget becomes a real problem. Build against §11.1.
- **Still open (build-time, not architectural) — key-suffix vs. decoded-tuple merge
  comparison.** The sketch orders the merge by the raw key suffix
  (`key[1+arenaPathLen:]`), which is `[E][A][Tx↓]…` and therefore byte-identical to the
  resolver's required `(E,A,Tx↓)` order by construction (no decode needed for
  ordering), but it requires the `Key()` accessor (§11.4 item 2). The alternative
  compares the decoded `(E.Hash(), A, Tx-descending)` from each sub's `Datom()`: no
  interface change, but it decodes the value eagerly and risks a comparator that subtly
  disagrees with storage byte order. The key-suffix form is preferred because it cannot
  disagree with the index; settle it when coding the merge.

---

## 12. Staged implementation path

Each stage is independently shippable and reuses prior machinery. This path assumes
the §9.4 recommendation: tree-path axis C now, frontier A / arena B deferred.

### 12.1 Minimum linear slice (A + B) — ships first

The first shippable increment is linear snapshots and rollback with zero storage-core
change: no arena prefix, no `AncestorMergeIterator`, no ceilings, no key-format
migration. In staged-arc terms (§12.2) it is Stage 0 plus the destructive-truncate
sliver of Stage 3, decoupled from the encoding work so it delivers value before any of
that lands. Its public API is designed (see *Forward-compatibility* below) so the
tree-path round is purely additive: never a signature or semantic change.

**The semantic boundary it respects.** Non-destructive rewind-and-continue is
impossible on a single linear timeline. Post-rollback writes either take Lamports
`> old-max` (so any read-ceiling that shows them also shows the rolled-back span,
hiding nothing) or reuse `T+1…` and collide with the existing `T+1…old-max` (same
Lamport + ReplicaID, corrupt). Keeping the old data and diverging requires a distinct
coordinate for the new writes, i.e. the branch axis, which is what the tree-path design
adds and why branch-on-rollback is deferred. So linear rollback is one of two things
only: a read-only view (Slice A) or a destructive rewind (Slice B).

**Slice A — snapshots + read-only views.** Almost pure metadata over existing
machinery; reuses `store.MaxElementID()` (capture) and `d.AsOf(eid)` (read), so there is
no new read code.

```go
// Snapshot names the current state of THIS handle. Internally records the handle's
// high-water point (ElementID today via store.MaxElementID(); (arena, frontier) under
// tree-path) — the signature does not change. Errors if name exists.
func (d *Database) Snapshot(name string) (SnapshotInfo, error)

// AsOfSnapshot returns a READ-ONLY handle viewing the DB as of the named snapshot.
// Internally d.AsOf(ref); AsOf handles already reject writes (NewTransaction panics).
func (d *Database) AsOfSnapshot(name string) (*Database, error)

func (d *Database) Snapshots() ([]SnapshotInfo, error)
func (d *Database) DeleteSnapshot(name string) error

// SnapshotInfo is opaque beyond Name. Its internal ref widens (ElementID → arena+frontier)
// without changing this type's contract; metadata fields can be added additively.
type SnapshotInfo struct {
    Name string
    // unexported: ref, created — internal, free to grow
}
```

`AsOfSnapshot` returns a read-only handle: an `AsOf`/`History` handle rejects writes
(`NewTransaction` panics on a temporal handle). So Slice A is "name a checkpoint;
query/operate as of it"; it does not change the live, writable DB.

**Slice B — destructive rollback (`TruncateTo`).** The only piece with new machinery.

```go
// TruncateTo destructively rewinds THIS handle's writable timeline to the named
// snapshot: data written after it is PHYSICALLY removed (not tombstoned), and the
// handle continues from that point (clock.Restore). Under tree-path this truncates
// within the current branch and additively returns an error if a descendant forked
// past the point (§10.7); linearly that error never fires.
func (d *Database) TruncateTo(name string) error
```

- **Mechanics:** scan TAEV (the Tx-leading index) for `Tx > eid` — a bounded range
  from the index start to `encode(eid)`; for each affected datom, physically delete its
  key from all eight indices in one `BadgerTx`; then `clock.Restore(eid)` so the next
  write resumes at `eid.Lamport+1`; invalidate the cache.
- **The new primitive — physical delete-by-Tx.** Today the store has only `Retract`,
  which appends a tombstone at a higher Tx (forward, the opposite of a rewind), so true
  truncation is new code: scan + 8 key deletes per affected datom.
- **Destructive, by design:** the rolled-back datoms are gone; `History()` will not show
  them either. Safe in the linear case because there are no branches, hence no
  descendants to orphan; the §10.7 guard is vacuous now and becomes active only under
  tree-path.

**Forward-compatibility.** The public surface expresses only what generalizes;
everything that changes stays behind it. Five rules, each tied to the breakage it
prevents:

| Design rule | Breakage it prevents |
|---|---|
| **Name-based API** — snapshots/rollback by `string`; the ref never appears in a signature | Snapshot identity widening `ElementID → (arena, frontier)` would change a return/param type |
| **Methods on `*Database`, returning `*Database`** | A session handle (tree-path) would otherwise need a new type; instead the same methods work on root and session handles |
| **All mutators return `error`** | The future descendant-guard (§10.7) is a new error case, not a new signature (vacuous linearly) |
| **Opaque, version-tagged stored ref** | The record grows `[v1][ElementID:16] → [v2][arena:32][frontier…]` with no migration of old snapshots |
| **`RollbackTo`/`Fork` reserved; destructive op named `TruncateTo`** | No verb ever flips meaning from destructive → non-destructive when branching lands |

**What generalizes underneath the stable surface** (none of it visible in the API):
- `Snapshot` capture: `store.MaxElementID()` → the handle's `(arena, frontier)`.
- Stored ref: version-tagged — `v1 = [ElementID:16]` → `v2 = [arena:32][frontier…]`. Old
  `v1` snapshots stay valid: a linear snapshot is `(rootArena, eid)`, which the §10
  ancestor-chain read path already handles (root is the base of every chain).
- `AsOfSnapshot` read path: `d.AsOf(eid)` → the §10 ancestor-chain merge; same handle
  type and signature.
- `TruncateTo` scope: the whole linear timeline → the current branch's subtree, plus the
  descendant-guard `error` (vacuous linearly).

**Verb reservation (decided 2026-06-26).** The destructive linear op is `TruncateTo`.
`RollbackTo` and `Fork` are reserved for the future non-destructive branch-on-rollback,
so the friendly "rollback" name never has to flip from destructive to non-destructive.

**Tests (the slice is not done until these pass).**
- Snapshot capture; `AsOfSnapshot` returns the pre-snapshot state; post-snapshot writes
  invisible through the handle but visible through the live DB; multiple snapshots; list;
  delete; error on duplicate name.
- `TruncateTo`: post-`T` datoms physically absent (verified via `History()`, not just
  resolved reads); live reads equal the snapshot exactly; the clock resumes at `T` so the
  next write gets `T+1` with no collision; truncate → write → truncate is stable.
- Edges: snapshot of an empty DB; truncate to the latest snapshot (no-op); snapshot the
  rewound timeline and read it back.

### 12.2 Full staged arc

- **Stage 0 — Snapshots (tags) over AsOf.** Persist `name → Tx`; `AsOfSnapshot(name)`
  wraps existing `AsOf`. Pure metadata, no resolution change. Immediately useful and
  independent of the encoding choice.
- **Stage 1 — Arena-prefix as the branch axis.** Add the leading tree-path prefix to
  keys (L85 `EncodeFixed32`, sort-preserving); scope reads to an ancestor-chain range
  set; reuse Lamport-Tx `AsOf` as the time axis. The world root is the empty prefix —
  with one root and no forks, behavior is identical to today (regression-test that
  equivalence).
- **Stage 2 — Session/branch handles.** `Fork` allocates a child sub-prefix (O(1));
  reads merge the ancestor-chain ranges by Tx-descending and run the existing CRDT
  resolution over the merged stream (full mechanism in §10); writes go to the handle's
  own range.
  Immutable-ancestor caching (§9.5): shared permanent cache for ancestors + thin
  per-session cache for divergence.
- **Stage 3 — Rollback + session lifecycle.** Soft rollback = `AsOf(T)` read-point
  within the session prefix (default, non-destructive); branch-on-rollback = fork a
  child from `(session, T)`; hard-truncate = range-delete `Tx > T` within the prefix.
  End/discard a session = one subtree range delete. Depth-reclaim compaction
  (materialize-to-fresh-root) keeps the 256-bit budget healthy.
- **Stage 4 (later) — Merge-back + distribution.** Merge via
  materialize-into-fresh-subtree, or a frontier edge (axis A, §6 machinery) for O(1)
  merges. Content-addressed sealing (axis B) for verifiable / Merkle-diffable
  snapshots and replication (`DISTRIBUTED_JANUS.md`). Uniqueness rides read-time
  `(A,V)`-LWW unchanged (§7) throughout.

---

## 13. Future direction: an incremental fixpoint / materialized-view engine (speculative)

*Out of scope for this proposal; recorded because it is a significant capability the
temporal/branch substrate enables, and a primary motivation for the branching work.*

Janus today evaluates non-recursive, Datomic-style queries. A natural and large future
direction is to make it a real fixpoint engine: recursive rules evaluated to a least
fixpoint (semi-naïve), with incremental view maintenance (IVM) so derived relations
update on base-fact change rather than recomputing from scratch. It belongs in this
document because such an engine composes with the temporal/branch substrate in ways
worth recording:

- **`AsOf(T)` of a derived relation** = the fixpoint over base facts as-of `T`, so
  time-travel extends to conclusions, not just stored facts.
- **A branch's derived relation** = the fixpoint over that branch's visible base
  (§10). So speculative recomputation comes from branching directly: fork a session
  (§11.2), change a base fact, and the engine re-derives only the delta within that
  branch, no orchestration. This is a major payoff of the branch axis for any consumer
  that maintains derived state.

**Why it belongs next to the branch design.** A class of consumer sits on Janus
maintaining incrementally-recomputed derived state: dependency graphs,
staleness/invalidation, closed-world completion conditions over growing sets. That is
itself incremental Datalog, hand-rolled at the application layer. A real fixpoint engine
lets such a consumer express those dependencies as recursive rules instead of
reimplementing the evaluator, and the hardest, most concurrency-sensitive parts
(incremental invalidation; stratified-negation completion conditions) move into one
principled, testable place rather than being rebuilt per consumer.

**The clean boundary.** The fixpoint stays pure: the engine concludes what is stale,
what must be (re)derived, and when a closed-world set is complete. Effectful producers
(anything that calls out, costs, or is non-deterministic) stay in the consumer, fired
by those conclusions and writing their results back as base facts, which trigger the
next delta. Janus never learns the consumer's domain; the consumer never learns about
arenas. The engine schedules and tracks; producers do the effectful work. (This is the
same layer-vocabulary discipline that keeps §6–§11 consistent.)

**The hard part.** Recursion itself is textbook: a semi-naïve loop layered over the
existing matcher. Incremental maintenance is the research-grade core (DRed
(delete/re-derive), counting/provenance, or the DBSP / differential-dataflow algebraic
route), and it is most of the cost and risk. Stratified negation/aggregation (needed for
closed-world completion conditions) constrains which rules are legal.

**The fork it would face**, tying back to this document: materialized vs. virtual
derived facts. Materialized (derived facts written as datoms) makes `AsOf`/branching
apply to them uniformly, but costs storage and demands maintenance; virtual
(recomputed/cached on read) is cheaper to store but makes "branchable derived views"
lean entirely on the per-frontier cache story (§8, §9.5, §10.9). That single choice
shapes the engine.

**Positioning.** Incremental recursive Datalog over a temporal, branchable, CRDT fact
store is a distinctive combination: differential-dataflow / DBSP have the incremental
recursion but no temporal/branch/CRDT substrate; Datomic has recursive rules but
recomputes (no IVM); demand-driven incremental frameworks (Salsa, Adapton, Incremental)
are not Datalog and not a store. This is a separate, large design round, flagged here,
not specified.

---

## 14. Summary

Branching, snapshots, and rollback are a generalization of `AsOf` along a second axis.
Today `AsOf` selects when (a Lamport-Tx threshold); branching adds where (a branch
identity). Reads become scoped by `(where, when)`, and the resolution functions are
already CRDTs that run unchanged over whatever that scope admits. Uniqueness already
merges cleanly because it is already read-time `(A,V)`-LWW (§7), and the one unsafe
case (write-time upsert) is the one the project already deferred.

The open question is how to encode the where, and §9 lays three candidates side by
side: the version-vector frontier (merge-optimal; the body of this doc), the
content-addressed arena DAG (verify / replicate-optimal), and the tree-path bitmask
prefix (fork / isolation-optimal). The driving game-engine use case (separate sessions,
rollback, merge deferred) points at the tree-path prefix (§9.3–9.4): sessions become
physically isolated subtrees, ending a session is one range delete, rollback composes
with the `AsOf` already in production, and the only weakness (merge) is the deferred
case. The frontier and content-addressed encodings layer in later for merge-back and
distribution respectively. The same substrate is, further out, the natural foundation
for an incremental fixpoint / materialized-view engine (§13), where time-travel and
branching extend to derived conclusions, not just stored facts.
