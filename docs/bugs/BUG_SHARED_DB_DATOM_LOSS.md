# BUG: Silent Datom Loss on Decode (AfterRef Heuristic)

**Date**: 2026-02-07
**Severity**: High — reference-valued datoms silently missing after successful Commit()
**Status**: FIXED (2026-02-07)

## Summary

`BinaryKeyEncoder.DecodeKey` silently drops ~0.78% of reference-valued datoms
due to a faulty AfterRef detection heuristic. The data is written correctly to
BadgerDB but cannot be read back — the decoder misreads a byte from inside the
SHA1 hash as the Op field, enters the wrong decode path, and returns an error
that is silently swallowed by the iterator and cache layers.

The bug affects any entity with `TypeReference` attributes (Identity values).
Non-reference types (string, keyword, int64, etc.) are unaffected.

The bug reproduces consistently at ~2-3% per 5-datom transaction containing
reference-valued attributes. Earlier testing also observed failures on fresh
per-iteration databases at lower rates (~0.5-3%), though a later independent
run was unable to reproduce the fresh-DB failures.

## Discovery

Found when `PullInto` returned structs with nil Identity reference fields on
entities that had multiple `TypeRef` attributes. Initially suspected to be a
PullInto read-path or cache issue.

Nil guards added to entity load functions caught the symptom: previously these
nil Identity values propagated silently until they caused panics deep in
unrelated code paths (see BUG_PULLINTO_NIL_ENTITY_PANIC.md).

## Reproduction

### Test Matrix

The investigation used multiple test configurations to isolate the cause.
Each test writes entities with a mix of reference and keyword attributes,
then reads them back via both direct Datalog query and `PullInto`.

**Initial results** (Windows):

| Test | What it does | Failure Rate |
|------|-------------|-------------|
| SharedDB | `tx.Set()` per field, one DB for all 200 iterations | **~2%** |
| SaveStruct | `SaveStruct` + `Commit`, fresh DB per iteration (100x) | **~3%** |
| ExplicitSet | `tx.Set()` per field, fresh DB per iteration (100x) | **~1-3%** |
| SeparateTx | Separate `tx.Set()` + `Commit` per field, fresh DB (100x) | **~3%** |
| SingleField | One `tx.Set()` + `Commit`, one datom, fresh DB (200x) | **~0.5%** |

If the per-datom loss rate is ~0.5% (from the single-datom test), then a
5-datom transaction has roughly `1 - (1 - 0.005)^5 ~ 2.5%` chance of losing
at least one datom. This is consistent with the observed multi-datom rates.

**Independent reproduction** (macOS, Apple M4):

| Test | What it does | Failure Rate |
|------|-------------|-------------|
| SharedDB | Same as above | **~2.5% (5/200)** |
| SaveStruct | Same as above | 0/100 |
| ExplicitSet | Same as above | 0/100 |
| SeparateTx | Same as above | 0/100 |
| SingleField | Same as above | 0/200 |

Same janus-datalog version, different OS and hardware. The macOS run
reproduced SharedDB failures but not the fresh-DB failures. The fresh-DB
failures may be platform-dependent (filesystem behavior, memory model, I/O
scheduling) or simply harder to trigger on macOS/ARM. The tests are
non-deterministic (random UUIDs each run).

The SharedDB variant was the most reliable reproducer across both platforms.

### Minimal Failing Pattern

```go
// Single database, reused across iterations
db, _ := storage.NewDatabase(tmpDir)
db.SetSchema(s) // schema with TypeRef attributes

for i := 0; i < 200; i++ {
    entityID := datalog.NewIdentity(generateUUID())
    refA := datalog.NewIdentity(generateUUID())
    refB := datalog.NewIdentity(generateUUID())
    refC := datalog.NewIdentity(generateUUID())

    tx := db.NewTransaction()
    tx.Set(entityID, kwAttr, datalog.NewKeyword(":entity.type/test"))
    tx.Set(entityID, refAttrA, refA)
    tx.Set(entityID, refAttrB, refB)
    tx.Set(entityID, refAttrC, refC)
    tx.Set(entityID, statusAttr, datalog.NewKeyword(":status/active"))
    tx.Commit()  // returns nil error

    // ~2.5% of the time, one reference field is missing:
    var loaded MyEntity
    db.Store().PullInto(entityID, &loaded)
    // loaded.RefB == nil OR loaded.RefC == nil
}
```

### Fresh DB Variant (fails intermittently)

```go
// Fresh database PER ITERATION — usually passes, but initial testing
// observed ~1-3% failure rate. A later run saw 0 failures.
for i := 0; i < 100; i++ {
    tmpDir, _ := os.MkdirTemp("", "test_*")
    db, _ := storage.NewDatabase(tmpDir)
    db.SetSchema(s)

    // Exact same Set+Commit logic as above
    // ...
    db.Close()
    os.RemoveAll(tmpDir)
}
```

### Single Datom Variant (minimal reproducer)

```go
// One entity, one attribute, one value. Fresh DB.
// Initial testing: ~0.5% failure rate. Later run: 0/200.
entityID := datalog.NewIdentity(generateUUID())
refID := datalog.NewIdentity(generateUUID())

tx := db.NewTransaction()
tx.Set(entityID, refAttr, refID)  // returns nil error
_, err = tx.Commit()               // returns nil error

// When it fails: direct query also returns not-found
var result datalog.Identity
found, _ := store.QueryOneInto(&result,
    `[:find ?v :in $ ?e :where [?e :test/ref ?v]]`, entityID)
// found == false — the datom was never written
```

## Failure Pattern

Every observed failure shows:

1. **The entity exists** — keyword attributes are always found
2. **Exactly one field is missing** from both direct query and PullInto
3. **Which field varies** — any reference-typed attribute can be affected
4. **Other fields are intact** — the transaction partially committed
5. **The loss is persistent** — re-querying returns the same result

Example output:
```
FIELD LOSS:
  entity/type: found=true val=:entity.type/test
  query refA=true refB=true refC=false
  pull  refA=EXsO[NkO81Q-a`zM8Ie6 refB=h[3=pyv_ClE!K37A,eSX refC=<nil>
```

The direct Datalog query also returns not-found, confirming the data is absent
from readable storage — not a PullInto read-path issue.

## What This Rules Out

### Not a PullInto / read-path bug

Both direct Datalog query and PullInto miss the same datom. If the data were
in BadgerDB but PullInto couldn't read it, the direct query would still find
it. Both fail.

### Not a SaveStruct / updateField optimization bug

`SaveStruct` has an optimization in `updateField` (writer.go) that skips
writes when the existing value matches the new value. But explicit `tx.Set()`
calls — which bypass `SaveStruct` entirely — show the same failure in the
shared-DB test.

### Not specific to transaction size or shape

The shared-DB test uses the same `tx.Set()` + `Commit()` pattern as the
fresh-DB tests. Same number of datoms, same attributes, same value types.

### Not clearly limited to accumulated state

The shared-DB variant was the most reliable reproducer, and the second test
run only saw failures there. However, the initial testing observed failures
on fresh per-iteration databases as well (including single-datom
transactions). This suggests the underlying cause may not be limited to
accumulated state — it may simply be easier to trigger with a populated
database.

## Possible Causes (pre-root-cause)

The following observations constrained the root cause:

- **Shared-DB reproduces consistently** (~2-3% per run)
- **Fresh-DB reproduces intermittently** (observed in initial testing, not in
  a later run)
- **Single-datom fresh-DB reproduces rarely** (~0.5% initially, 0% later)
- **Both query and PullInto miss the data** — it is genuinely absent from
  readable storage
- **No errors from any write API** — Commit() returns nil

Possible causes considered in the storage layer:

- **BadgerDB compaction** — internal GC/compaction goroutines may race with
  reads. A populated database has more compaction pressure, explaining why
  shared-DB is more reliable to reproduce.
- **Cache coherency** — the `BadgerMatcher` cache (`m.cache != nil` path in
  `LookupAttribute`) accumulates entries across the DB lifetime. A stale
  negative cache entry could mask a recently-written datom.
- **Index iteration boundaries** — a populated database may hit LSM tree
  level boundaries during iteration that a nearly-empty fresh DB never reaches.
- **Write-path data loss** — if the initial fresh-DB results are accurate,
  the bug may be in the write pipeline itself (`Assert` → `db.Update` →
  `txn.Set`), with accumulated state merely increasing the probability.

### Initial recommendations (before root cause found)

These suggestions were proposed during initial investigation but turned out
to be unrelated to the actual cause. Preserved here for completeness:

- Investigate whether `DetectConflicts = false` on BadgerDB opts interacts
  with internal compaction goroutines
- Check whether `txn.Set(key, nil)` (nil value, keys-only indices) triggers
  edge cases in BadgerDB's memtable
- Consider post-commit verification in `Assert()`: scan back to confirm all
  datoms are present
- Evaluate `WriteBatch` API vs `db.Update()` for different atomicity
  guarantees

## Investigation Notes (2026-02-07)

### Write path traced

1. `tx.Set()` for CardinalityOne: appends `Datom` to `t.datoms` with unique
   `elemID` from `t.db.clock.Next()`. No read-before-write for CardinalityOne.
2. `Commit()` calls `t.db.store.Assert(t.datoms)` — single `db.Update()`
   BadgerDB transaction. All datoms written atomically.
3. Inside `Assert`: each datom written to all 7 indices via `txn.Set(key, nil)`.
   If any index write fails, entire BadgerDB transaction rolls back.
4. After `Assert` returns, `Commit` writes tx metadata in a **separate**
   `Assert` call (second `db.Update` transaction).
5. Cache invalidation: `UpdateMaxVersion` then `Invalidate` for all
   touched (E, A) pairs.

### Cache invalidation order in Commit()

```go
// 1. UpdateMaxVersion for each datom (bumps version)
// 2. Invalidate(touched) — deletes cached entries
```

Between steps 1 and 2, `maxVersions` is updated but old cache entry still
exists. A concurrent `GetOrResolve` at this moment would see version mismatch
and fall through to rebuild — so this is **not** a race for stale hits.

However, the reproducer tests are **single-threaded** — no goroutines. So
concurrency races in the cache should not apply.

### BadgerDB configuration

```go
opts.DetectConflicts = false    // Disabled for performance
opts.NumCompactors = 4          // Parallel compaction
opts.ValueThreshold = 1 << 10   // 1KB
opts.MemTableSize = 128 << 20   // 128MB
```

`DetectConflicts = false` was initially considered suspicious. All values are
`nil` (keys-only storage — datom info encoded in key). This means BadgerDB's
value log is not involved; all data lives in the LSM tree.

### Key observation: two separate Assert calls per Commit

`Commit()` calls `Assert` twice:
1. `t.db.store.Assert(t.datoms)` — the actual datoms
2. `t.db.store.Assert(txMetadata)` — the `:db/txInstant` metadata

These are two separate `db.Update()` transactions. The datoms are atomic
with respect to each other (all 5 datoms in one BadgerDB txn), but the
metadata is a separate transaction. This shouldn't cause datom loss since
the metadata is written after the datoms, and the query reads don't depend
on metadata.

### What still needed investigation (pre-root-cause)

1. **BadgerDB `db.Update` semantics with `DetectConflicts = false`**: Does
   disabling conflict detection affect write visibility guarantees? The
   BadgerDB docs say this only affects read-write transaction conflicts, not
   write durability. But worth verifying.

2. **`txn.Set(key, nil)` edge cases**: All datom writes use nil values
   (keys-only). BadgerDB treats nil values as "key exists with empty value."
   Are there edge cases in BadgerDB's memtable or LSM compaction with nil
   values and 4 parallel compactors?

3. **Read-after-write visibility**: After `db.Update()` returns, are all
   keys immediately visible to a new `db.View()` or `db.Update()`? BadgerDB
   guarantees this for MVCC reads, but the guarantee depends on the read
   transaction starting after the write transaction commits.

4. **Cache rebuild path**: When `GetOrResolve` falls through to `rebuild`,
   it opens a new BadgerDB read transaction. Is there any scenario where
   `rebuild` could see a partial view of the write?

5. **`Indices` variable**: `assertDatom` iterates `Indices` to write all 7
   index keys. If `Indices` is somehow short or corrupted, a datom could be
   written to some indices but not all. Need to verify `Indices` is constant.

6. **Key encoding determinism**: If `EncodeKey` produces different keys for
   the same datom on write vs. read (e.g., due to keyword interning or hash
   collision), the datom would be written but not found by queries.

## Root Cause Found (2026-02-07)

### AfterRef detection heuristic in `BinaryKeyEncoder.DecodeKey`

**File**: `datalog/storage/key_encoder_binary.go`, `DecodeKey` method

The bug is in the AfterRef detection heuristic used by all 7 index decoders.
The decoder reads a byte from the WRONG position (inside the value data) and
misinterprets it as the Op byte, causing key decode failure and silent datom
loss.

#### The key format (before fix)

EATV keys were laid out as:

```
[prefix 1][E 20][A 32][Tx↓ 16][ValueType 1][ValueData N][Op 1][AfterRef? 16]
```

AfterRef is present only when Op ∈ {OpRGAInsert(3), OpRGATombstone(4)}.
For normal writes (Op = 0, 1, or 2), AfterRef is absent.

#### The heuristic

The decoder attempts to detect AfterRef by:

1. Checking if the key is long enough to contain AfterRef
2. Reading a byte from `key[len-17]` (where AfterRef Op would be)
3. If that byte is 3 or 4, assuming AfterRef is present

```go
// key_encoder_binary.go, DecodeKey, EATV case
hasAfterRef := len(key) >= minSize+afterRefSize  // minSize=69, afterRefSize=16
if hasAfterRef {
    op = key[len(key)-afterRefSize-opSize]        // key[len-17]
    if datalog.CRDTOp(op).HasAfterRef() {         // true if byte == 3 or 4
        // ... treats key as having AfterRef — WRONG
    } else {
        hasAfterRef = false
    }
}
```

#### Why it fails

For a CardinalityOne reference-valued datom (e.g., a `TypeRef` attribute →
Identity), the key after the prefix byte is:

```
[E 20][A 32][Tx↓ 16][TypeRef 1][SHA1Hash 20][Op=0 1] = 90 bytes
```

The heuristic:
- `minSize + afterRefSize = 69 + 16 = 85`
- `90 >= 85` → **true** (key is "long enough")
- Reads `key[90-16-1] = key[73]` — this is the **5th byte of the SHA1 hash**,
  not the Op byte (which is at `key[89]`)
- If `hash[4] == 3` or `hash[4] == 4`, the decoder enters the AfterRef path

When the AfterRef path triggers incorrectly:
- Value is truncated to 5 bytes: `key[68:73]` instead of `key[68:89]`
- `DatomFromKey` tries to decode a Reference from 4 data bytes (needs 20)
- Decode returns an error → the datom is silently treated as "not found"
- Both the query path (`KeyOnlyIterator.Next()` returns false on decode error)
  and the cache path (`ResolveLWW` returns error → `rebuildOne` returns nil)
  fail identically

#### Probability analysis

SHA1 hash bytes are uniformly distributed.
**P(hash[4] ∈ {3, 4}) = 2/256 ≈ 0.78% per reference-valued datom.**

Keyword-valued attributes (e.g., `:entity/type`) have ASCII bytes
(always > 0x20) at the critical position, so they never trigger the bug.

Expected vs observed failure rates:

| Test                 | Ref datoms | Expected              | Observed       |
|----------------------|------------|-----------------------|----------------|
| SingleField (1 ref)  | 1          | 0.78%                 | 0.3% (3/1000)  |
| SharedDB (3 refs)    | 3          | 1-(1-0.0078)^3 ≈ 2.3% | 2.8% (28/1000) |
| ExplicitSet (3 refs) | 3          | 2.3%                  | 2.6% (13/500)  |
| SaveStruct (3 refs)  | 3          | 2.3%                  | 2.0% (10/500)  |
| SeparateTx (3 refs)  | 3          | 2.3%                  | 2.2% (11/500)  |

All observed rates fall within the statistical confidence intervals of the
expected rates. The SingleField confidence interval for 0.78% with 1000
trials is approximately [0.16%, 2.27%] (Wilson), which includes the observed
0.3%.

#### Why SharedDB appeared more reliable

Earlier testing on macOS showed failures only for SharedDB and not for
fresh-DB variants. This is statistical noise — the per-datom rate is constant
regardless of database state. With 500 iterations and 2.3% expected failure
rate, seeing 0 failures has probability (1-0.023)^500 ≈ 0.009%. But the
earlier run used 100-200 iterations, where P(0 failures) = (1-0.023)^100 ≈
10% — plausible. The current run with 500-1000 iterations reproduces
failures across all variants.

#### Scope

The identical heuristic existed in **all 7 index decode paths** in
`DecodeKey`. Every `case` block (EAVT, EATV, AEVT, AETV, AVET, VAET, TAEV)
used the same pattern:

```go
hasAfterRef := len(key) >= minSize+afterRefSize
if hasAfterRef {
    op = key[len(key)-afterRefSize-opSize]
    if datalog.CRDTOp(op).HasAfterRef() { ... }
}
```

Any index scan that decodes a key with a sufficiently long value containing
byte 3 or 4 at the critical position will misinterpret the key.

For 3 indices (EATV, AETV, TAEV), `key[73]` lands in value data (hash[4]).
For the other 4 (EAVT, AEVT, AVET, VAET), `key[73]` lands on `Tx↓[0]`,
which can also trigger the bug when `Tx↓[0] ∈ {3, 4}` (i.e., when
`Lamport` has high byte `0xFC` or `0xFB`).

#### Investigation items resolved

| Item                                | Status                                         |
|-------------------------------------|------------------------------------------------|
| 1. BadgerDB `DetectConflicts=false` | Not the cause                                  |
| 2. `txn.Set(key, nil)` edge cases   | Not the cause                                  |
| 3. Read-after-write visibility      | Not the cause                                  |
| 4. Cache rebuild path               | Not the cause (but propagates the error)       |
| 5. `Indices` variable               | Confirmed constant, 7 entries                  |
| 6. Key encoding determinism         | Encoding is deterministic; **decoding is not** |

## Fix

**Approach**: Move Op to always be the last byte of every key. Eliminate the
heuristic entirely.

The key format is inherently ambiguous for AfterRef detection when the value
data is long enough. Three approaches were considered:

**Approach A — Read last byte first (minimal change):**
Read Op from `key[last]`. Op 0/1/2 never have AfterRef, so if the last byte
is 0/1/2, we're done. Op 3/4 always have AfterRef, so if the last byte is
3/4, it can't be the Op (it's part of AfterRef) — re-read from `key[last-16]`.

This fixes 100% of non-RGA keys (the current bug) but introduces a ~1.2%
failure rate for RGA keys where AfterRef's last byte happens to be 0, 1, or 2.

**Approach B — Use value type for disambiguation (more robust):**
After reading the value type byte (at a known fixed position), use the
expected value size for fixed-size types (Reference=21, Int64=9, Bool=2, etc.)
to compute the exact Op position. For variable-size types, fall back to
approach A. This eliminates ambiguity for all fixed-size types including
Reference.

**Approach C — Key format change (complete fix) [CHOSEN]:**
Move Op after AfterRef so Op is always the last byte. Eliminates all
ambiguity for all key types, all value sizes, all Op values.

### New key format (all 7 indices)

```
[...components...][AfterRef?][Op]
```

Op is always `key[len(key)-1]`. If `Op.HasAfterRef()`, AfterRef is the
16 bytes before Op. No heuristic. No ambiguity.

See `docs/reference/OP_POSITION_PROOF.md` for the formal proof that:
1. Op-before-Tx breaks first-entry-wins (Theorem 1)
2. Tx-before-Op enables O(1) CRDT resolution for all types (Theorem 2)
3. Op does not determine sort order between distinct datoms (Theorem 3)
4. Moving Op after AfterRef does not change scan results (Theorem 4)

### Decoder change

Before:
```go
hasAfterRef := len(key) >= minSize+afterRefSize
if hasAfterRef {
    op = key[len(key)-afterRefSize-opSize]  // WRONG: reads inside value data
    if datalog.CRDTOp(op).HasAfterRef() { ... }
}
```

After:
```go
op = key[len(key)-1]  // Always correct
tailSize := opSize
if datalog.CRDTOp(op).HasAfterRef() {
    tailSize = afterRefSize + opSize
    afterRef = txFromDescending(key[len(key)-opSize-afterRefSize : len(key)-opSize])
}
```

### Binary compatibility

For non-AfterRef datoms (the vast majority), the physical key layout is
**identical** — Op was already the last byte. The change only affects keys
with AfterRef (RGA ops), where Op and AfterRef swap positions.

### Files changed

- `datalog/storage/key_encoder_binary.go` — EncodeKey + DecodeKey (production)
- `datalog/storage/key_encoder_l85.go` — EncodeKey + DecodeKey (consistency)
- `datalog/storage/afterref_heuristic_bug_test.go` — Reproducer test suite

## Reproducer Tests

Reproducer tests are in `datalog/storage/afterref_heuristic_bug_test.go`:

```bash
go test -v -run TestAfterRefHeuristicBug ./datalog/storage/
```

| Test | What it verifies |
|------|------------------|
| `_Unit` | All 7 indices decode correctly with crafted trigger inputs |
| `_KeyLayout` | Diagnostic: prints exact byte positions for EATV ref key |
| `_Integration` | Full BadgerDB write+read path preserves trigger ref |
| `_Statistical` | 10,000 random refs: 0% decode failure (was 0.78%) |
| `_NonRefValues` | Non-reference types unaffected (keyword, string, int, etc.) |
| `_FullDBRoundTrip` | 500 entities × 3 ref fields: 0 losses (was ~2.3%) |

The `_Unit` test forces `key[73] == 3` on every index:
- EATV/AETV/TAEV: crafted SHA1 hash with `hash[4] == 3`
- EAVT/AEVT/AVET/VAET: crafted Tx with `Lamport=0xFC00000000000000`
  so `Tx↓[0] == ^0xFC == 0x03`

## Files

- `datalog/storage/key_encoder_binary.go` — Root cause and fix
- `datalog/storage/key_encoder_l85.go` — Consistency fix
- `datalog/storage/afterref_heuristic_bug_test.go` — Reproducer tests
- `docs/reference/OP_POSITION_PROOF.md` — Formal proof of Op-at-end correctness
