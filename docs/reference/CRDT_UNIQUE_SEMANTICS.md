# Unique Attributes as CRDT Read-Time Resolution

**Status**: Implemented — merged on branch `feature/crdt-unique-resolution`, 5 commits (see git log for 87579c1, a3e5c06, 00678f0, 460bb2e, and the commit promoting this document to `docs/reference/`)
**Date**: 2026-04-16 (proposal) / 2026-04-17 (finalized and implemented)
**Affects**: schema (`Unique` field semantics), storage (`validateUniqueness` removed), matcher (V-bound query path), CRDTResolvingIterator (entity-view fallback), EA cache (invalidation), Pull API (read-time resolution), new public API (`LookupByUnique`)

> **Reader note**: this document was originally written as a proposal
> during the atomicity commit (2026-04-16) and extended with finalized
> design decisions on 2026-04-17. Its structure preserves the narrative
> — discussion history showing the dead-ends, then the decisions that
> resolved them, then the implementation. Sections appear in the order
> they were written rather than being reorganized after implementation.
> For a quick overview of the semantics, see the
> [CRDT.md "Unique Attributes" section](./CRDT.md#unique-attributes-av-lww-with-walk-fallback)
> and [SCHEMA.md "Uniqueness Semantics" section](./SCHEMA.md#uniqueness-semantics).

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

- `TestSchemaUniquenessValue`, `TestSchemaUniquenessWithinTransaction`, `TestSchemaUniquenessIdempotent` encode Datomic-strict failure on duplicates. They are **deleted** in Commit 1 (they express the old contract, not the new one). Retaining them as "translated" tests would create the impression that the same feature is being tested with different assertions — but the feature itself has changed. Cleaner to delete and rebuild.
- New tests in subsequent commits cover:
  - Sequential takeover: alice claims V, then bob claims V → bob is canonical owner (V-view); alice's entity-view falls back to her prior assertion (or nothing)
  - Concurrent takeover: two writers claim V → both commits succeed; highest-Tx wins under read-time resolution
  - Retract-and-reassign in one transaction: previously rejected, now succeeds cleanly
  - Multi-layer fallback: alice asserts V1 then V2; bob takes V2; alice's entity-view returns V1. Bob then takes V1 too; alice's entity-view returns nothing.
  - History preservation: all superseded assertions remain queryable via `d.History()`
  - AsOf snapshot correctness: `d.AsOf(tx)` applies `(A, V)`-LWW restricted to claims with Tx ≤ target
  - Cache invalidation: write to `(bob, email, x)` invalidates alice's cached `(email)` value
  - `LookupByUnique` API: returns canonical owner, nil when no claimant, errors on non-unique attribute
  - Pull for superseded entities: returns the fallback value, not the latest write

## Implementation discipline: test-driven development

The entire implementation cycle follows a strict test-first workflow. This is not a suggestion; it is the workflow for every commit after Commit 1.

**For each commit introducing new behavior (Commits 2–5)**:

1. **Write the tests first.** The new tests express the contract: what the system must do after this commit lands. They are written against the not-yet-implemented API or semantic.
2. **Run the tests. Verify they fail** — with the expected failure mode (undefined symbol, wrong value, panic, whatever). A failing test that fails for the wrong reason is not a valid red baseline.
3. **Implement.** The minimum change that makes the failing tests pass. No speculative functionality, no "while we're here" refactoring that isn't demanded by the tests.
4. **Run the tests. Verify they pass.**
5. **Run the full suite** (`go test -count=1 ./...`). Verify no regressions.
6. **Only then: commit.** The commit contains both the tests and the implementation, landing together.

**Why this matters specifically for this work**:

The uniqueness redesign touches multiple subsystems with subtle interactions (resolution, caching, pulls, history). Writing tests first forces an explicit statement of what correctness means at each layer before any implementation code is written. It prevents the pattern where implementation shapes the test (rather than the test shaping the implementation), which in past work on this codebase has produced tests that exercise the implemented path while missing the actual defect — the tuple-key collision bug and the CardinalityOne tombstone gap are both documented examples.

**For Commit 1 (pure deletion)**, this workflow is inverted: there is no new behavior to test-drive. The commit removes `validateUniqueness`, its call site, and the three Datomic-strict tests that assert its contract. Any test that happens to rely on `validateUniqueness` to reject concurrent-or-duplicate assertions will surface as a failure when the full suite is run after the deletion; those failures are reviewed and either updated or removed depending on whether the underlying assertion remains valid under the new semantics. This is the one commit where the full suite guides the work rather than test-first.

**What "test-first" does not mean**:

- It does not mean one test per commit. A commit introduces a coherent unit of behavior; its tests cover the full contract for that unit.
- It does not mean tests cannot be refined during implementation. If implementing reveals an under-specified corner, the test is extended — but the new assertion is added to the red test before the implementation code that satisfies it.
- It does not forbid baseline tests that already pass. Existing tests that continue to pass as regression guards are fine; the discipline is about *new* behavior being test-driven.

---

## Application-level migration

Applications that relied on `Commit()` returning a uniqueness-violation error to detect duplicates must instead query for the canonical owner before assuming their write "wins." Patterns:

- **"Did my write win?"** → after commit, call `d.LookupByUnique(attr, v)` and check whether the returned Identity equals yours. If it doesn't, a concurrent writer with a higher Tx has taken the value.
- **"Is this value taken?"** → call `d.LookupByUnique(attr, v)` before writing. Keep in mind this is a snapshot: a concurrent writer may claim the value between your check and your commit. Under the CRDT model, whichever claim has the higher Tx wins at read time, regardless of commit order.
- **Upsert by natural key**: as shown in the `LookupByUnique` API section above — look up first, fall through to creating a fresh Identity when no existing entity is returned.

The absence of a write-time error means applications can no longer distinguish "nobody owned this value when I wrote" from "somebody else has concurrently claimed it." Both read as successful commits. If the distinction matters, query after commit.

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

### The `UniqueIdentity` question (added 2026-04-17)

After the atomicity fix landed and implementation of the redesign was about to begin, the open question "what about `UniqueIdentity`?" came back for a decision.

The initial framing — "is `UniqueIdentity` just `UniqueValue` with a different label?" — missed a structural distinction. Datomic's `:db.unique/identity` bundles two capabilities that are independent in a CRDT model: **lookup refs** (pure read-time: resolve entity from unique value) and **upsert / entity merging** (pure write-time: rewrite a transaction's tentative entity ID to an existing entity with the shared unique value). Datomic conflates them because in a single-transactor architecture the write-time operation is "just" a read-then-rewrite with no concurrency consequences.

In our model the two capabilities have radically different properties. Lookup refs are a one-line extension of `resolveAVLWW`: no new concurrency story, no new storage structure, composable with everything we're already building. Upsert is a different beast — two concurrent writers both resolving "no existing owner of V" and both creating new entities produces split-entity states with no obvious convergence rule. The "loser" entity has partial data and a superseded unique claim; the winner has the canonical unique claim but missing the loser's other attributes.

Three positions became visible once the capabilities were separated:

1. **UniqueValue only** — forfeit the natural-key lookup use case entirely. Users hash their natural keys themselves.
2. **Lookup-ref only** — add lookup refs as a read-time feature. No write-time upsert. The natural-key lookup case works cleanly; applications wanting upsert build it themselves on top (`if e := d.LookupByUnique(...); e == nil { e = NewIdentity(...) }`). Concurrent "upserters" produce split-entity states, but those states are visible to the application that created them — the app decides how to handle the collision, because the app is the only layer with enough context to make the decision sensibly.
3. **Full Datomic semantic** — add both lookup refs and write-time upsert. Opaque to the application; the database silently picks a canonical winner and leaves losers dangling. Requires a design round on split-entity convergence that is genuinely its own feature.

Chose Position 2. The split between read-time and write-time is the structural insight that unlocks this choice: it lets us ship the practical use case (lookup-by-unique-value) immediately without committing to a half-designed concurrency semantic for the harder case. Position 3 stays on the table as a future design round; Position 2 is forward-compatible with it (adding upsert later doesn't remove the lookup-ref capability).

This is the same pattern as the original write-time-vs-read-time insight for value uniqueness: "is this feature really a write-time coordination problem, or is it actually a read-time resolution problem?" For `UniqueValue`, it's pure read-time. For `UniqueIdentity`'s lookup refs, also pure read-time. For `UniqueIdentity`'s upsert, genuinely write-time — and that's the part we're not doing this round, specifically because we don't yet have a satisfactory answer for the split-entity convergence question that write-time operations on a CRDT inherently create.

---

## Historical note: atomicity commit

This section records the state of the codebase when the original proposal was written (2026-04-16, before implementation of the redesign began).

The atomicity fix from `BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX.md` shipped in commit `497ea81`:

- `Transaction.Commit()` opens one `BadgerTx` and routes retracts, asserts, and metadata through it
- `:db/txInstant` metadata is no longer best-effort — failure rolls back the entire commit
- Test: `TestCommitWritesTxInstantOnSuccess` locks in the metadata-on-success contract

At that point the uniqueness redesign described in this document was **not** yet implemented. `validateUniqueness()` still ran Datomic-style at the start of `Commit()` against committed state. The TOCTOU race and the retract-and-reassign rejection from `BUG_UNIQUENESS_VALIDATION_TOCTOU.md` remained open.

Implementation of the redesign began on branch `feature/crdt-unique-resolution` (2026-04-17) following the design decisions in the section below.

---

## Design decisions

The open questions from the original proposal are resolved as follows. Each decision records the rationale, the alternatives considered, and why those alternatives were rejected — so future readers understand not just *what* was chosen but *why*.

### D1. Identity vs Value: Position 2 — lookup-ref only, no write-time upsert

**Decision**: `UniqueIdentity` is supported as a **read-time lookup primitive** (resolve an entity from its unique value) but does not perform write-time entity merging. `UniqueValue` and `UniqueIdentity` differ only in whether lookup-by-unique-value is permitted as an entity reference; neither performs upsert at commit time.

**Datomic's full semantic** (for reference):
- `:db.unique/value` — the value must be globally unique. Duplicate assertion fails.
- `:db.unique/identity` — value uniqueness **plus** two additional behaviors:
  1. **Lookup refs**: the tuple `[:user/email "x@y"]` is a valid entity reference anywhere an entity ID is expected (query patterns, transaction data).
  2. **Upsert**: transacting `[:db/add temp-id :user/email "x@y"]` where some existing entity `E` owns that email causes `temp-id` to be merged onto `E` — the rest of the transaction's assertions on `temp-id` are rewritten to `E`.

**Three positions considered**:

| Position | Lookup refs | Write-time upsert | Trade-off |
|---|---|---|---|
| 1. UniqueValue only | No | No | Simplest. Forfeits natural-key lookup. |
| 2. Lookup-ref only (chosen) | Yes | No | Captures the practical 80% with no concurrency hazard. |
| 3. Full Datomic semantic | Yes | Yes | Maximum compat. Inherits a split-entity hazard specific to our CRDT model. |

**Why Position 2 over Position 1**:

The lookup-ref use case is what applications actually reach for: "give me the user with this email." It appears in authentication flows, API integrations, and import/reconciliation logic in nearly every application that uses unique attributes. Position 1 forces users to manage their own value-to-identity mapping (hash the email as the entity ID, carry lookup tables in application memory, etc.) for a feature the database can provide natively with minimal infrastructure.

The implementation cost is genuinely small: lookup refs are exactly one call to the `resolveAVLWW(a, v)` primitive that `UniqueValue` already requires. No new indices, no new locks, no new concurrency semantics. The extension is pure read-time — if `resolveAVLWW` is correct for `UniqueValue`, it's correct for `UniqueIdentity`'s lookup-refs.

**Why Position 2 over Position 3**:

Write-time upsert — resolving a transaction's tentative entity ID to an existing one based on a shared unique value — works cleanly in Datomic's single-transactor architecture because all writes serialize through one writer. It does not work cleanly in a CRDT architecture with concurrent writers.

Consider two concurrent upserters:

```
Writer A, tx1:
  temp-alice-id :user/email "x@y"
  temp-alice-id :user/name  "Alice"

Writer B, tx2 (concurrent with tx1):
  temp-bob-id :user/email "x@y"
  temp-bob-id :user/age   30
```

Under Position 3, each writer queries the database for "who owns email=x@y?" — both see no existing owner — both create new entities. After both commits, storage contains two distinct entities, both claiming `email=x@y`. Under `(A, V)`-LWW resolution, one is the canonical owner; the other is a "loser" entity carrying `:user/name "Alice"` or `:user/age 30` with a superseded email.

This failure mode does not exist in Datomic (the single transactor serializes the check-and-write) and it does not exist in our `UniqueValue` design (no entity reference rewriting happens, so no entity identity is lost). It is specific to Position 3 in our model.

Resolving the failure mode requires additional design: do the "loser" entity's other attributes migrate to the winner? If so, how — by what merge rule, at what time, preserving what history? If not, does the loser stay as a partial record referencing nothing? Neither answer is obviously correct, and the choice has downstream implications for history queries, audit semantics, and application data models.

That design round is worth doing well, not hurrying through as a subsection of this change. **We defer Position 3 as a future design round. `UniqueIdentity` declared today will be forward-compatible with either outcome** — if Position 3 is later added, it becomes an additional behavior on top of the existing lookup-ref, not a replacement for it.

**Graduation path from Position 2 to Position 3 (future, not in scope)**:

If the project later decides to add write-time upsert, the natural extension is:
1. Add a new transaction-time phase after assertion generation and before `BeginTx`: scan the transaction's asserts for `UniqueIdentity` values, resolve each to an existing entity via `resolveAVLWW`, rewrite tentative entity IDs that collide to the existing Identity.
2. Define the split-entity semantics for concurrent upserters (likely: keep both entities; the "loser" stays as-is; applications that care must query for the canonical owner explicitly).
3. Add a `StrictUniqueIdentity` option for applications that prefer the loser's transaction to fail rather than produce a split-entity state.

None of this requires Position 2's semantics to change. The primitive (`resolveAVLWW`), the public API (`LookupByUnique`), and the read-path resolution all compose cleanly with future upsert logic.

**Current implementation status of `UniqueIdentity`**:

Before this work, `UniqueIdentity` was declared in `schema/types.go` but never branched on — it behaved identically to `UniqueValue` (both fell through `validateUniqueness`'s single check). After this work:

- Both produce the same read-time resolution: `(A, V)`-LWW winner selection for V-view, entity-view fallback.
- `UniqueIdentity` additionally permits lookup-by-unique-value as an entity reference (via `LookupByUnique`, and eventually query-language lookup refs).
- Neither performs write-time entity merging.
- Schema API unchanged; applications using `UniqueIdentity` today get improved semantics with no code changes.

### D2. Pull semantics for superseded entities: fallback value

**Decision**: When an entity's most recent assertion of a unique attribute has been superseded by another entity's claim on the same value, `pull(entity, [:attr])` returns the entity's most recent *non-superseded* assertion (the entity-view fallback rule). If no such assertion exists, the key is omitted from the returned map.

**Alternatives rejected**:
- `nil` or empty string — conflates "you lost this specific value" with "you have no value," throwing away storage information about older valid assertions.
- Latest assertion with no resolution — produces inconsistency between Pull and Query for the same attribute on the same entity, which is a trap.

**Consequence for applications**: the value returned by Pull can differ from what the application most recently wrote with `tx.Set`. This is a behavior change documented in the release note and the updated `SCHEMA.md`.

### D3. Cache invalidation: conservative attribute-wide

**Decision**: When a transaction commits assertions on a unique attribute, invalidate *all* cached `(E, A)` entries for that attribute, not just the writer's own `(E, A)`.

**Alternatives rejected**:
- Reverse index `(A, V) → [E]` — precise invalidation but adds persistent state to the cache, complicates cache construction/teardown, and provides diminishing returns if unique-attribute writes are rare (the typical case: set email once on signup).
- Per-`(E, A)` only (current behavior) — incorrect: a write to `(bob, email, x)` can silently stale `(alice, email)`'s cached value if alice had been the prior canonical owner.

The conservative strategy is a single line of code in `Transaction.Commit`: if `def.Unique != ""`, iterate the cache's existing entries for that `A` and invalidate each. It produces correct results and imposes no memory overhead.

If profiling later shows that unique writes churn the cache meaningfully, the upgrade to a reverse index is self-contained — both strategies satisfy the same correctness contract.

### D4. History queries: `d.History()` returns all raw assertions

**Decision**: History mode bypasses `(A, V)`-LWW resolution entirely. `d.History()` returns every asserted datom, including ones that are currently superseded by other entities' claims. No filtering, no fallback, no resolution.

This is consistent with how history mode already treats `(E, A)`-LWW (it doesn't apply it). The implementation check in `CRDTResolvingIterator.isHistoryMode()` already exists; the new `(A, V)`-LWW logic added in Commit 3 must respect it.

Testing requirements: a dedicated test in Commit 3 verifies that after a takeover, all claimants' assertions remain queryable via `d.History()` — including the superseded ones.

### D5. Migration: hard break, no strict mode

**Decision**: `validateUniqueness` is deleted outright. No `StrictUniqueness` database option, no deprecated transition mode, no behavior-preservation shim.

**Rationale**:
- The old behavior was buggy (TOCTOU racing with concurrent commits; false rejection of valid retract-reassign patterns within a single transaction). Preserving it is preserving bugs.
- The codebase is pre-1.0; breaking changes are explicitly permitted.
- Maintaining two uniqueness models permanently doubles the surface area for every future change touching unique attributes (cache, query planner, Pull, history mode, export/import).
- Applications that need "write rejection on duplicate" can achieve it at the application layer: `if owner := d.LookupByUnique(attr, v); owner != nil && !owner.Equal(self) { return ErrDuplicate }`.

The release note documents the behavioral change. Applications checking for `uniqueness violation` error strings from `Commit()` need to migrate to the application-layer pattern described above.

---

## New public API: `LookupByUnique`

A new method on `Database` (Position 2's main new surface):

```go
// LookupByUnique returns the entity currently owning (attr, value) under
// (A, V)-LWW resolution. Returns nil Identity if no entity currently
// claims that value. Returns an error if attr is not a unique attribute
// in the schema, or if the underlying lookup fails.
//
// Available for both UniqueValue and UniqueIdentity attributes. Concurrent
// writers may cause the returned Identity to change over time; callers
// should treat the result as a snapshot.
func (d *Database) LookupByUnique(attr datalog.Keyword, value interface{}) (datalog.Identity, error)
```

Implementation delegates directly to `resolveAVLWW`. Cost: one AVET prefix scan + one EATV point lookup per candidate (typically 1 of each in the common case).

**Use patterns**:

```go
// Upsert-by-natural-key at the application layer
e, _ := d.LookupByUnique(datalog.NewKeyword(":user/email"), "alice@example.com")
if e == nil {
    e = datalog.NewIdentity("user:" + uuid.New())
}
tx := d.NewTransaction()
tx.Set(e, datalog.NewKeyword(":user/email"), "alice@example.com")
tx.Set(e, datalog.NewKeyword(":user/name"),  "Alice")
tx.Commit()

// "Is this value taken?" check before attempting to claim
if owner, _ := d.LookupByUnique(emailAttr, "x@y"); owner != nil {
    return fmt.Errorf("email already in use by %s", owner.String())
}
```

**Future: query-language lookup refs**

A follow-up change (not in this design round) will add lookup-ref syntax so `UniqueIdentity` can be used as an entity reference directly in queries:

```clojure
[:find ?name :where [[:user/email "x@y"] :user/name ?name]]
```

The parser will recognize the tuple form, and the planner will resolve it via `LookupByUnique` at query time before binding the entity variable. This is a parser/planner extension with no change to the underlying resolution semantics — it simply exposes the capability through the query language.

---

## Open items (for implementation, not design)

These are execution-time questions the implementation will answer; they are not design choices.

1. **AVET Tx encoding** (Commit 2): verify whether AVET stores Tx in ascending or descending order. If descending, `resolveAVLWW` is O(1) per candidate (first entry wins). If ascending, O(k) per value with k = number of historical claimants. The key encoder source will answer this immediately; the algorithm is straightforward either way.

2. **AsOf interaction with `(A, V)`-LWW** (Commit 3): `d.AsOf(tx)` should restrict `(A, V)`-LWW resolution to claims with `Tx ≤ tx` — the AsOf filter already in `CRDTResolvingIterator.txID` should chain through naturally, but a dedicated test should verify it produces the snapshot-correct canonical owner at each point in time.

3. **Benchmark regression** (all commits): entity-view reads for unique attributes move from O(1) (first EATV entry) to O(history depth until non-superseded assertion). For attributes with no takeovers, still O(1). For contested attributes, worst case is the length of the entity's history. Common case: negligible. Benchmark the takeover path and add a regression guardrail if it becomes load-bearing.

4. **`Compare`-wise equality on V** (Commit 2): AVET prefix scans compare V by byte-equal storage encoding. Ensure `ValuesEqual` and the encoding agree for edge cases (`[]byte`, `float64` NaN, interned keyword values). This is a verification step, not a new design.

---

## Related documents

- `docs/bugs/BUG_TRANSACTION_COMMIT_SPLIT_BADGER_TX.md` — atomicity bug, fixed in this commit
- `docs/bugs/BUG_UNIQUENESS_VALIDATION_TOCTOU.md` — uniqueness bug, framing now superseded by this proposal
- `docs/reference/CRDT.md` — the CRDT storage architecture this proposal extends
- `docs/reference/SCHEMA.md` — schema definition; `Unique` field semantics will need updating once the redesign is implemented
- `docs/reference/INDEX_SELECTION_PROOF.md` — the candidate-and-validate pattern in `matchWithVValidation` that the new V-bound resolution extends
