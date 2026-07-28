# A V-bound prefix scan matches a superseded cardinality-one value

**Status**: **Fixed** (2026-07-28, PR #114). `chooseIndex`'s E+A+V arm is cardinality-aware and admits a bound V to the prefix only for a cardinality-many member, where that member's adds and removes all carry its own value and the resolution decision is therefore contained in the V-filtered run. Both reproducers — `TestConstantPatternCacheParity/one/superseded, V bound to the superseded value` and the schemaless sibling in `TestConstantPatternCacheParitySchemaless` — are green, and `make test` is green, native and wasm.

## Summary

A pattern that binds E, A and V as constants returns the **superseded** datom when the EA cache is off, and nothing when it is on.

```
:person/name  Set "Alice"  then  Set "Alicia"
[:find ?tx :where [#id "…" :person/name "Alice" ?tx]]

cache off → one tuple      (wrong: "Alice" lost LWW)
cache on  → no tuples      (right)
```

Not schemaless-specific. The declared cardinality-one attribute above and a schemaless `:loose/value` set to 7 then 9 diverge identically.

## Mechanism

With E, A and V all constant, `chooseIndex` builds an **AEVT** bound whose prefix is `[A, E, V]`. That prefix is the defect: it narrows the scan to the datoms carrying **the queried value**, which for a superseded value is exactly the losing datom and nothing else.

`CRDTResolvingIterator` then resolves what the scan handed it. Its cardinality-one arm emits the first entry of each (E, A) group, which is correct only when the group it sees *is* the group — EATV/EAVT order Tx descending, so the first entry is the winner. Here the group has been pre-filtered to one datom before resolution ever runs, so the loser is the first entry the iterator sees, and it is emitted as the winner. The iterator cannot detect this: the higher-Tx datom for the same (E, A) sits outside the scan range and it has no way to know the range was narrowed on V.

`matchFromCache` gets the same query right because it resolves the whole (E, A) entry first and only then compares against the bound V. That is why the divergence is visible as a cache-parity failure rather than as a plain wrong answer: the cache path happens to hold the correct algorithm.

**A tombstone does not trigger it.** `one/tombstoned, V bound to the removed value` passes, because the highest-Tx entry in that group *is* inside the V-bound range and `Op` interpretation reads it as absence. Supersession is the gap: the loser is still an `OpCRDTAdd`, indistinguishable from a winner once its group has been reduced to it alone.

## Why the existing defence does not cover this

The candidate-plus-validate design exists for exactly this hazard — find candidates by V on a V-primary index, then validate each against the EATV winner before emitting (`validatingVBoundIterator`, `docs/reference/INDEX_SELECTION_PROOF.md` Theorem 4). It is reached from `analyzeReuseStrategy`, the binding-driven path, and is gated on `strategy.NeedsValidation`.

A fully-ground pattern has no bindings. It goes through `matchUnboundScan`, which never consults `analyzeReuseStrategy`, so no validation is constructed. The protection and the hole are on two different paths.

Two further notes on the gate. The first stands; the second has since been checked and did not hold.

- `NeedsValidation` is set only for an **explicitly declared** `CardinalityOne` attribute. A schemaless attribute has no schema definition, so even on the binding-driven path it would not be marked — and schemaless resolves as LWW everywhere else, so the omission looks unintended rather than deliberate.
- ~~Whether any binding-driven shape reaches a V-bound scan without validation is not established here.~~ **Checked, and the adjacent shape is defended.** `chooseIndex` answers `AVET` with prefix `[A, V]` for an A-and-V-bound pattern with E unbound, which is the same "filter by V, then resolve the filtered group" shape one arm over. `TestVBoundScanWithUnboundEntityRejectsSupersededValue` — a renamed entity queried by its former name, both cache modes — passes without any change. That arm was never broken, and an early claim in this session that it was is withdrawn.

## Reachability

Requires the EA cache to be unavailable for the pattern. Concretely: `DisableCache: true`, or any (E, A) the cache declines. History mode also bypasses the cache, but history is meant to expose superseded datoms, so that is not a wrong answer there.

With the cache on — the default — `matchFromCache` intercepts every constant-E/constant-A pattern and answers correctly, which is why this has not surfaced in ordinary use.

## Relationship to BUG_CACHE_EMPTY_VECTOR_NEVER_SET

Possibly the same defect stated twice, and that is an owner call. Both are instances of *a V-bound prefix scan resolving a group it has already filtered*:

- there, the filtered group is empty and the arms disagree about whether an absent vector matches `[]`;
- here, the filtered group holds exactly the losing datom and the arms disagree about whether a superseded value matches.

If they are one, the fix is one: resolution must be handed the whole (E, A) group and the V comparison applied after, on every path — which is what the cache arm already does and what candidate-plus-validate does on the binding-driven path. If they are two, they are two because the empty-vector case additionally needs `CacheEntry` to carry the never-set/cleared distinction, which is a storage-representation change and not about scan bounds at all.

### Resolved: they were two

The shared framing above — *a V-bound prefix scan resolving a group it has already filtered* — is what made them look like one, and it described the symptom rather than either cause.

- **This one is a scan-bound defect.** The group really was filtered before resolution, and the fix is that V does not enter the prefix where the resolution decision is not contained in the V-filtered run.
- **The empty-vector one was never about scan bounds.** Its cause was that attribute presence had never been modelled anywhere — four correctness sites read it off `RGAStats.TotalElements`, a debugging statistic, and the cache path had none to read. Its fix was to resolve presence and stop caching an absent (E, A), touching no scan bound.

The second horn above guessed closer than the first: it did need a representation change. It named `CacheEntry` as the place, and the representation was missing a layer further up, in resolution itself.

Worth keeping as the general shape: two defects sharing a symptom, where one is about the range being scanned and the other about a fact nobody had represented. Treating the second as a variant of the first would have produced a scan-bound change for a value-domain problem.

## Severity

Wrong answer, silent, on a query shape that reads naturally — "does this entity still have this exact value?" — and returns *yes* for a value the entity had and lost. No error, no annotation anomaly; the funnel reports one datom scanned, one resolved, one matched, all internally consistent.
