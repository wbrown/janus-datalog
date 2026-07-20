# BUG: Attribute Cache Freshness Check Is Not O(1)

**Date**: 2026-05-25 **Severity**: Performance / Documentation Drift (Medium) **Status**: Resolved (2026-05-25) — see Resolution at the end **Affected**: A-bound cache freshness checks, `Cache.IsAttributeFresh`, `BadgerStore.MaxElementIDForAttribute`

## Summary

The cache documentation says attribute freshness checks are O(1), but `MaxElementIDForAttribute` scans all keys for the attribute in the AEVT index to compute the maximum ElementID.

This is not a correctness bug, but it can make warm-cache A-bound queries scale with attribute cardinality despite comments describing constant-time behavior.

## Code Evidence

`Cache.IsAttributeFresh` calls into storage on every whole-attribute freshness check:

```go
func (c *Cache) IsAttributeFresh(a Attribute, store Store) bool {
    val, ok := c.attrVersions.Load(a)
    if !ok {
        return false
    }
    cachedMax := val.(datalog.ElementID)
    storeMax, err := store.MaxElementIDForAttribute(a[:])
    if err != nil {
        return false
    }
    return cachedMax == storeMax
}
```

The implementation of `MaxElementIDForAttribute` scans the AEVT range for the attribute:

```go
for it.Valid() {
    key := it.Item().Key()
    if len(key) < 33 || key[0] != byte(AEVT) {
        break
    }
    if !bytesEqual(key[1:33], aevtPrefix[1:33]) {
        break
    }

    _, _, _, tx, _, _, err := s.encoder.DecodeKey(AEVT, key)
    if err != nil {
        it.Next()
        continue
    }
    elemID := Tx(tx).ToElementID()
    if elemID.Compare(maxID) > 0 {
        maxID = elemID
    }

    it.Next()
}
```

The comments around the cache and store interface describe this as O(1), but the actual cost is O(number of datoms for the attribute).

## Root Cause

AEVT is ordered by:

```text
A -> E -> V -> Tx
```

Within a single `(A, E, V)` group, the first entry has the highest Tx. But the highest Tx for the whole attribute may be in any entity/value group. A forward seek to the attribute prefix cannot reveal the global maximum Tx without either:

- scanning the attribute range, or
- maintaining a separate index/metadata value where Tx sorts before E/V.

## Expected Behavior

Either:

1. The comments and docs should state the true complexity, or
2. The storage/cache design should provide a real O(1) or O(log n) attribute high-water mark.

## Actual Behavior

The implementation performs a range scan over every key for the attribute while claiming O(1) freshness checks.

## Impact

For high-cardinality attributes, this can make "warm" A-bound queries pay a large freshness-check cost before deciding whether cached attribute results are usable. The bug is most visible for attributes shared by many entities, such as:

- `:entity/type`
- `:person/name`
- time-series attributes
- unique or lookup-heavy attributes

## Fix Direction

Options:

1. Documentation-only fix: update comments in `Store`, `Cache`, and `BadgerStore` to describe the scan honestly.
2. Metadata fix: maintain an attribute max-ElementID value during commit and read it directly during freshness checks.
3. Index fix: add an A-Tx-primary index such as `[A][Tx↓][E][V]` if the query planner can also benefit from it.

The metadata approach is probably the smallest targeted fix if the only need is cache freshness.

## Verification Plan

Add a benchmark or regression guard that creates many entities with the same attribute and measures `IsAttributeFresh` / A-bound cache checks. The benchmark should make the current linear behavior visible and prevent future comments from claiming constant-time cost unless the implementation changes.

---

## Resolution (2026-05-25)

**Resolved by adding a new ATEV index** (option 3 from Fix Direction, chosen for consistency with the existing 7-index design and to unlock AsOf-by-attribute access patterns beyond the freshness check).

### Correcting one assumption in the original report

The report (and my first reading of it) treated reverse-Tx ordering as if it might already give O(1) somewhere. It does not. Tx **is** bitwise-NOT encoded (descending) in AEVT, AETV, AVET, VAET, and TAEV — but in each of those it sits *after* at least one other sort component, so the descending-Tx ordering only applies *within* fixed (A,E,V) or (A,E) groups, not across the whole attribute. No reverse-seek on any of the existing seven indices yields the global max-Tx for an attribute in one operation; the `cache.go:201` "O(1) reverse seek" comment was wrong on physics, not just doc drift. The implementation in `badger_store.go:263` was honest about this in its own inline comments ("for a true O(1) solution, we'd need an index like EATV where Tx comes earlier").

### What changed

- **New index**: **ATEV** = `[prefix][A][Tx↓][E][type][V][AfterRef?][Op]`. Positioned so the first entry under prefix `[A]` is the global max-Tx datom for the attribute. `MaxElementIDForAttribute` is now a single forward seek plus a Tx decode — genuinely O(1). The previously misleading comments in `cache.go` and `store.go` are now accurate.
- **Matcher integration**: `chooseIndex` routes A-bound + Tx-bound + V-unbound patterns to ATEV. `chooseIndexForValues` (hash-join path) and `simple_batch_scanner.buildKey` learned the ATEV layout so joined/batched scans produce a tight `[A][Tx↓][E]` prefix instead of degrading.
- **Cache integration**: `Cache.IsAttributeFresh` is unchanged in shape — it calls `MaxElementIDForAttribute` and compares — but the call it makes is now constant-time.

### Defensive-code cleanup that came with the work

Two latent silent-error patterns surfaced and were fixed in the same PR:

1. `extractElementIDFromKey` used to `return datalog.ElementID{}` for unknown indexes (conflating "iterator not positioned" with "programmer added an index and forgot to update this switch"). It now panics on unknown index, matching the encoder switches in `key_encoder_{binary,l85}.go`. This is how a missing ATEV case got caught by a test that asserted the *value* of the returned Tx — without that, `"HEAD"` would have looked like a normal empty result.
2. Five `case` blocks that branched on `tx.(uint64)` for a legacy Lamport-only Tx form (`chooseIndex` ATEV+TAEV; `chooseIndexForValues` ATEV+TAEV; `simple_batch_scanner.buildKey` TAEV; plus the `constT` extraction I had freshly added). `Tx` is always an `ElementID` — the `uint64` branches were dead code that silently routed contract violations into wrong-shape keys. `NewTxFromUint` and `toStorageTx` were deleted with no remaining callers.

### Tests added (`datalog/storage/atev_index_test.go`)

- `TestATEVEncoderRoundTrip` — binary + L85 encode/decode
- `TestATEVDescendingTxOrder` — first entry under `[A]` is the max-Tx datom
- `TestATEVIsPopulatedOnCommit` — write path populates ATEV
- `TestMaxElementIDForAttributeUsesATEV` — multi-entity max via the new path
- `TestMaxElementIDForAttribute_AfterRetraction` — documents that retracts *delete* (no tombstone), so the high-water mark can drop after a retract; cache freshness still works because the comparison is inequality, not direction
- `TestMaxElementIDForAttribute_MultipleWritesToSameEA` — LWW overwrite case
- `TestCache_IsAttributeFresh_Integration` — full chain on a real BadgerStore
- `TestChooseIndex_ABoundPlusTxBound_PicksATEV` and `TestChooseIndex_ABoundPlusTxBoundPlusVBound_DoesNotPickATEV` — routing
- `TestChooseIndexForValues_ATEV` — hash-join prefix builder (A+Tx, A+Tx+E)
- `TestSimpleBatchScanner_BuildKey_ATEV_VVaries` — batch scanner V-varies case
- `TestEndToEndABoundTxBoundQuery` — full pipeline against a real BadgerStore
- `TestChooseIndex_TxOnly_TAEV_WithElementID` — pin TAEV routing after the uint64-branch removal (covers `ElementID` and `*ElementID`)

### Measurements (`datalog/storage/atev_index_bench_test.go`, Apple M5)

`BenchmarkMaxElementIDForAttribute_ATEVSeek_vs_AEVTScan`:

| N (datoms-for-A) | ATEV seek | AEVT scan | Speedup |
|------------------|-----------|-----------|---------|
| 10               | 876 ns    | 1,943 ns  | 2.2×    |
| 100              | 995 ns    | 8,505 ns  | 8.5×    |
| 1,000            | 1,045 ns  | 70 µs     | 67×     |
| 10,000           | 1,121 ns  | 622 µs    | 555×    |

ATEV is flat across four orders of magnitude (true O(1)); the AEVT scan it replaces grows linearly (10× per decade) — exactly as the asymptotic prediction required.

`BenchmarkAssert_WriteCost`: ~6.5 µs/datom across the 8-index write path. ATEV is one of those 8, so the marginal cost it adds is ≈ 0.8 µs/datom (~14%). This is estimated from the index ratio, not directly measured 7-vs-8; the per-index key construction and Badger `Set` are the variable cost (value encoding is amortized via `EncodeKeyWithValueBytes`).

### Files

- `datalog/storage/store.go` — `ATEV` added to `IndexType` and `Indices`; doc
- `datalog/storage/key_encoder_binary.go`, `key_encoder_l85.go` — ATEV encode/decode
- `datalog/storage/badger_store.go` — `MaxElementIDForAttribute` is the single-seek path; `extractElementIDFromKey` panics on unknown index
- `datalog/storage/matcher.go` — `chooseIndex` ATEV case; `indexName`; TAEV cleanup
- `datalog/storage/hash_join_matcher.go` — `chooseIndexForValues` ATEV case; TAEV cleanup
- `datalog/storage/simple_batch_scanner.go` — `constT` extraction; `buildKey` ATEV case; TAEV cleanup
- `datalog/storage/cache.go` — comments now accurate
- `datalog/storage/types.go` — `NewTxFromUint` and `toStorageTx` removed
- `datalog/storage/atev_index_test.go` (new) — 13 tests
- `datalog/storage/atev_index_bench_test.go` (new) — read + write benchmarks
- `ARCHITECTURE.md`, `CLAUDE.md`, `PERFORMANCE_STATUS.md` — updated

### Verification

`go build ./...`, `go vet ./...`, and `go test -count=1 ./...` all green.
