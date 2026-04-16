# Unique Attributes as CRDT Read-Time Resolution

**Status**: Proposal — captures design discussion, not yet implemented
**Date**: 2026-04-16
**Affects**: schema (`Unique` field semantics), storage (`validateUniqueness`), matcher (V-bound query path), EA cache (invalidation), Pull API (read-time resolution)

## Summary

The current uniqueness implementation enforces uniqueness at write time: `Transaction.Commit()` calls `validateUniqueness()` which queries committed state for existing owners and rejects the commit if any other entity already owns the value. This is a Datomic-style strict semantic.

This proposal redesigns uniqueness as a **read-time CRDT resolution rule**, fitting the codebase's append-only, LWW-by-Tx, conflict-free architecture. All writes succeed; the canonical owner of a unique `(A, V)` is determined by `(A, V)`-LWW at read time. The entity view falls back to a non-superseded assertion to preserve symmetry between value-lookup and entity-lookup.

This document captures the discussion that led to this redesign so future readers understand both the *what* and the *why*.

---

## Background: Why the current design doesn't fit

### The codebase architecture

janus-datalog is built around an append-only, CRDT-oriented storage model:

- Storage is append-only — every `Set()` writes a new datom with a new ElementID; old datoms are not deleted
- Cardinality-one uses LWW: highest Tx wins for `(E, A)`, encoded via Tx-descending key order in EATV
- Cardinality-many uses add-wins; vector uses RGA — both conflict-free merge functions
- BadgerDB is opened with `DetectConflicts = false` — concurrent writes don't conflict at the storage layer because the model is designed to never have meaningful collisions
- The matcher uses a candidate-and-validate pattern (`matchWithVValidation` in `matcher_relations.go`): AVET produces candidates, EATV-LWW validates each, only currently-owning entities are emitted

The system reads consistently across views because the matcher applies LWW at read time. "Who currently owns email=x" filters AVET candidates against EATV-LWW. "What's alice's email" picks alice's highest-Tx assertion. These are normally consistent because each entity's view of its own attributes is independent.

### The legacy uniqueness implementation

`validateUniqueness()` in `database.go` runs at the start of `Transaction.Commit()`:

1. Query committed state via the matcher for each unique `(A, V)` being asserted
2. If any entity other than the asserter currently owns `(A, V)`, reject the commit with "uniqueness violation"

This semantic comes from Datomic, where a single transactor serializes all writes and uniqueness can be strictly enforced at commit time. It doesn't fit a system designed for concurrent writes from multiple in-process writers (or, in a future distributed setting, multiple replicas).

### Why every fix attempt at this layer is wrong

We considered several "fix" approaches during the discussion:

1. **Database-wide commit lock** — kills concurrent writes, defeats the whole point of the architecture
2. **Per-attribute lock** — granular but still in-process serialization; doesn't compose with future distribution
3. **Per-(A,V) lock** — most granular in-process variant; same composition problem
4. **Storage-level anchor key + Badger SSI** — would require enabling `DetectConflicts`, slowing all writes (including normal CRDT writes that should never conflict) just to serialize uniqueness, and still wouldn't survive into a distributed setting

Every one of these tries to solve "two concurrent writers should not both succeed" — which is exactly the problem CRDTs are designed to make irrelevant. The fact that we keep needing to add coordination is the signal that the problem statement is wrong.

---

## What `Unique` is actually for

The use cases for unique attributes all reduce to one thing: **lookup by value should resolve to a single canonical entity**.

Concrete examples:

| Use case | What it needs |
|---|---|
| External identifier mapping (email, username, SSN) | Given V, find THE entity |
| Reference-by-value (`[:user/email "x@y"]` as an entity ref) | V identifies one entity unambiguously |
| Upsert / idempotency (re-importing the same record) | Same V → same entity, no duplicates |
| Application invariants ("the user with email x") | One result per V |

None of these intrinsically require write-time enforcement. They require **read-time disambiguation**: when you look up by V, you get one canonical answer.

Datomic delivers this by enforcing at write time, because Datomic's transactor architecture makes that the simplest implementation. In a CRDT system, it's the wrong implementation — and the read-side guarantee can be delivered directly via merge rules.

---

## The proposed model

### (A, V)-LWW resolution

A unique attribute has an additional CRDT rule applied at read time:

> Across all entities currently asserting `(A, V)` (per `(E, A)`-LWW), the entity with the highest `Tx` for that `(A, V)` is the canonical owner. Other entities' claims of `(A, V)` are superseded.

### Symmetric application to both views

For uniqueness to be coherent, both views must agree:

- **Value view** ("who owns x?"): returns the (A, V)-LWW winner, exactly one entity
- **Entity view** ("what's alice's email?"): returns alice's most-recent assertion that is *not* superseded by a higher-Tx (A, V) claim from another entity

If alice's latest assertion of `(email, x)` is superseded by bob's later `(email, x)`, alice's email view falls back to her **next-most-recent assertion that survives the (A, V)-LWW check**. Walk back through alice's `(E, A)` history until either:

- An assertion's V is the (A, V)-LWW winner for its V → that's alice's current value
- Run out of assertions → alice has no current value for this attribute

### Why fallback (not "no value")

The CRDT principle is consistent rule application: every assertion is a fact, the merge function decides what's current. For non-unique cardinality-one, supersession is value-specific — when alice asserts `(email, y)` after `(email, x)`, the older `(email, x)` is superseded by the newer `(email, y)`. The older assertion isn't deleted; it's just not current.

For unique cardinality-one, supersession should remain value-specific. When bob asserts `(email, x, L_B)` and supersedes alice's `(email, x, L_A)`, the conflict is at exactly that V. Alice's other (different-V) assertions are unaffected. So alice's "current" email is her most-recent non-superseded assertion.

The alternative ("alice has no email after bob takes over") would conflate "you lost this value" with "you lost the attribute," throwing away information the storage clearly retains.

---

## Implementation implications

### Write path

**Removed.** No `validateUniqueness` at commit time. All writes succeed. Schema's `Unique` field becomes a read-resolution rule, not a write gate.

### Read path

**V-bound query for unique attribute** (existing `matchWithVValidation` extended):

1. AVET scan → candidate entities
2. For each candidate E: EATV point-lookup → LWW winner for (E, A)
3. If `winner.V == bound V`: this E currently asserts V (passes existing check)
4. **NEW**: among all such Es, return only the one with the highest `Tx` for that (A, V)

This guarantees the value view returns exactly one entity per unique value.

**Entity-bound query for unique attribute** (existing EATV-first-entry path extended):

1. Scan EATV for (E, A) in Tx-descending order
2. For each candidate assertion of `(E, A, V_i, T_i)`:
   - Check whether `T_i` is the (A, V_i)-LWW winner across all entities
   - If yes → emit V_i as current
   - If no → skip, advance to next-older assertion
3. Run out → entity has no current value for this attribute

Common case: most recent assertion is not superseded → O(1). Worst case: walk through history until a non-superseded assertion is found → O(n).

### Cache

The current EA cache is keyed by `(E, A)` and invalidated when that (E, A) is written. With unique attributes, a write to `(bob, email, x)` must also invalidate `(alice, email)` if alice's cached value is x.

Two implementation strategies:

- **(A, V) → list-of-(E, A) reverse index**: maintain a map from each in-use unique (A, V) to the entities currently associated. Invalidation lookups become O(1) per (A, V).
- **Conservative invalidation**: on any write to a unique attribute, invalidate all cached entries for that attribute. Simple but coarse.

### Pull / PullInto

Pull operations resolve attributes via `EntityResolver`. For unique attributes, the resolver must apply the entity-view fallback. This may surface "current value differs from latest write" to applications, which is a behavior change — applications that assumed Datomic-strict semantics will need to handle this.

### Schema

`schema.Unique` field stays. Its meaning shifts from "write-time enforcement constraint" to "read-time resolution rule." The schema builder API is unchanged.

---

## What changes for users

### Behavioral changes

- **Sequential commits** that previously raised `uniqueness violation` will now succeed. The "loser" entity's previous assertion (or absence of one) becomes its current value.
- **Concurrent commits** that previously could race-and-both-succeed (TOCTOU bug) will both succeed *correctly*: read-time resolution picks one canonical owner.
- **Pull/Query** results may differ for unique attributes when an entity's claim has been superseded by another entity.

### Test updates required

- `TestSchemaUniquenessValue`, `TestSchemaUniquenessWithinTransaction` and similar tests expect Datomic-strict failure on duplicates. They need to be rewritten to assert the new contract: both writes succeed, read-time resolution picks the (A, V)-LWW winner.
- New tests are needed for:
  - Sequential takeover: alice claims V, then bob claims V → bob is canonical
  - Concurrent takeover: two writers claim V → highest-Tx wins
  - Entity fallback: alice claims V1 then V2; bob claims V2; alice's email reverts to V1
  - History preservation: superseded assertions remain queryable via `d.History()`

### Application-level migration

Applications that relied on `Commit()` returning a uniqueness-violation error to detect duplicates must instead query for the canonical owner before assuming their write "wins." Patterns:

- "Did my write win?" → after commit, query by V and check if the returned entity is mine
- "Is this value taken?" → query by V before writing, with the understanding that a concurrent writer may take it after the check

---

## Discussion history

This proposal was reached through a back-and-forth that's worth preserving because every dead-end represented a real engineering temptation worth recognizing.

### The original framing (wrong)

The user reported a bug (`docs/bugs/BUG_UNIQUENESS_VALIDATION_TOCTOU.md`) describing two failures of the existing implementation:

- Concurrent commits can both pass validation and both write the same unique value
- A transaction that retracts and re-assigns a unique value within itself is incorrectly rejected

The bug report framed both as bugs in `validateUniqueness` to be fixed. Initial fix attempts followed that framing.

### Dead end 1: Atomicity-only fix preserves the race

First attempt: wrap the whole commit in one Badger transaction (the atomicity fix). This is necessary for the *split-commit* bug regardless and is preserved in this commit. But it doesn't fix the uniqueness race because Badger's iterators don't add scanned keys to the conflict set — and even if they did, two writers asserting different `(E, A)` keys for the same V don't collide.

### Dead end 2: Anchor key + SSI

Second attempt: write a sentinel "anchor" key per `(A, V)` so concurrent writers collide on a shared key. Required enabling `DetectConflicts` in Badger. Code was written.

User pushback (decisive): `DetectConflicts = false` is intentional and architectural. The whole storage layer is designed around the principle that concurrent writes don't conflict. Turning conflict detection on globally to serialize uniqueness imposes per-write overhead on the normal CRDT operations that should never conflict.

The anchor approach was at the wrong layer. Uniqueness is a schema-level constraint sitting on top of a CRDT model; it can't borrow primitives from the storage layer that the storage layer was designed not to need.

### Dead end 3: Per-(A,V) in-process lock

Third attempt: per-(A,V) `sync.Mutex` keyed on a deterministic encoding. More granular than the anchor approach, doesn't disturb storage settings.

User pushback: same theme. "We will have concurrent writes" and serialization at any granularity violates the architectural intent. Even a fine-grained lock is still a process-level coordination mechanism that doesn't compose with the rest of the design.

### Dead end 4: "Why is enforcement necessary at all?"

The user prompted: in this CRDT model with LWW, why is sequential commit failure necessary? Tx 2's Tx is strictly greater than Tx 1's; under LWW semantics, Tx 2 simply wins. There's no asymmetry between sequential and concurrent — both should resolve via the same merge rule.

This was the actual breakthrough. The realization: the entire write-time enforcement is Datomic legacy code that doesn't belong here.

### Use-case analysis

Once the framing changed from "how do we enforce uniqueness at write time" to "what is uniqueness *for*," the answer became clear: lookup. All concrete use cases reduce to "given V, find the canonical entity." That's a read-side guarantee, expressible via (A, V)-LWW resolution.

### Symmetric resolution

One last subtlety: if "who owns V" returns one entity (the (A, V)-LWW winner), the entity-view must agree. Otherwise alice's email could read as `x` while "who owns x" returns bob. The fallback rule (walk back through alice's (E, A) history to find a non-superseded assertion) preserves symmetry while staying within the CRDT principle of consistent merge-rule application across all assertions.

---

## What this commit contains (atomicity only)

The atomicity fix from `BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX.md` is implemented in this commit:

- `Transaction.Commit()` opens one `BadgerTx` and routes retracts, asserts, and metadata through it
- `:db/txInstant` metadata is no longer best-effort — failure rolls back the entire commit
- Test: `TestCommitWritesTxInstantOnSuccess` locks in the metadata-on-success contract

The uniqueness redesign described in this document is **not** implemented in this commit. `validateUniqueness()` still runs Datomic-style at the start of `Commit()` against committed state. The TOCTOU race and the retract-and-reassign rejection from `BUG_UNIQUENESS_VALIDATION_TOCTOU.md` remain. Those bugs should be re-evaluated in light of the redesign rather than fixed at the existing layer.

---

## Open questions

1. **Identity vs Value (Datomic distinction)**: Datomic's `:db.unique/identity` does upsert (asserting a unique value on a "new" entity with the same value MERGES the entities). Should janus support identity merging, or only the value-uniqueness semantic described here?

2. **Pull semantics for superseded entities**: when alice's email has been superseded, should `pull(alice, [:email])` return `nil`, the fallback value, or omit the key entirely? Likely the fallback value (matches the entity-view rule) but should be confirmed by use cases.

3. **Cache invalidation strategy**: conservative attribute-wide vs (A, V) reverse index. Conservative is simpler but may invalidate too much; reverse index is more work but minimal cache churn. Decision depends on observed write patterns for unique attributes.

4. **History queries**: `d.History()` should continue to return all raw assertions including the superseded ones. The CRDT resolution is current-state only; history shows the full append-only record. This needs explicit testing.

5. **Migration path for existing applications**: how to communicate the behavior change to users who depend on Datomic-strict semantics? Likely needs a release note and possibly a deprecated `StrictUniqueness` mode for transition.

---

## Related documents

- `docs/bugs/BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX.md` — atomicity bug, fixed in this commit
- `docs/bugs/BUG_UNIQUENESS_VALIDATION_TOCTOU.md` — uniqueness bug, framing now superseded by this proposal
- `docs/reference/CRDT.md` — the CRDT storage architecture this proposal extends
- `docs/reference/SCHEMA.md` — schema definition; `Unique` field semantics will need updating once the redesign is implemented
- `docs/reference/INDEX_SELECTION_PROOF.md` — the candidate-and-validate pattern in `matchWithVValidation` that the new V-bound resolution extends
