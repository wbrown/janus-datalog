# PR #114 review — typed scan bounds — 2026-07

**Date**: 2026-07-26
**Subject**: PR #114, branch `feat/typed-bound-scan-seam`, head `566e806` over merge-base `88dadaa`
**Scope**: `datalog/storage`, `datalog/annotations`, and the documentation the branch wrote or invalidated
**Method**: a review was received against `566e806`; every claim in it was then independently verified in this tree — by executable reproducer where behaviour was alleged, by `gopls references` where deadness was alleged, and by reading where a document or a constant was alleged wrong. Nine reproducers were written; all are red at `566e806` and compile on native and js/wasm.

**Evidence-scope caveat**: every "zero callers" claim below is an **in-repository** observation from `gopls references`, which settles deadness for anything that is not consumer-facing API. Only for consumer-facing API does an in-repo count fall short, because a downstream caller no workspace index can see may exist.

**What is consumer-facing is an editorial fact, not a capitalization** (corrected by owner ruling, 2026-07-26). An earlier draft of this caveat said that removing *exported* surface is a compatibility decision rather than a reference count, stated as a general rule. That is wrong, and it produced four escalations in this review that should never have been raised — `Store.Get`, `MaxElementIDForAttribute`, `Cache.IsAttributeFresh`/`UpdateAttributeVersion`, and `EncodePrefixRange`. Go's export marker is how a package's own files and tests reach each other and how sibling packages in a module compose; it is not a versioning contract. What this module invites consumers to import is what it documents. In `datalog/storage` that is the injectable backend contract — `Store`, `StoreTx`, `Iterator` — which `docs/BREAKING_RELEASE_UPGRADE_v0.15.0.md` documents under *Custom backends* for exactly that purpose. `BinaryKeyEncoder` and its methods are machinery those implementations happen to share, and are **not** API; their surface carries no compatibility question and needs no ruling. The one place the two touch is `Store.Encoder() *BinaryKeyEncoder`, which sits on the consumer-facing interface and which a typed memory backend under PR B has nothing to return — a PR B question, not a surface question.

**Status convention** (as `docs/bugs/`): each finding carries a `Status:` line. Anything without `Status: Resolved (date, commit)` is open as of this document's date. Re-read the cited code before treating an entry as live, and update the `Status:` line in place when you fix or refute one.

**On age.** An earlier draft of this document carried a second column sorting each finding as *introduced* by the branch or *inherited* from before it, and asserted that only the former could block the merge. That column has been removed and the assertion withdrawn. `CLAUDE.md`'s *The Baseline Is Green* forbids attributing a defect to pre-existing conditions in reasoning or reporting; renaming "pre-existing" to "inherited" and promoting it to a taxonomy column is the same blame-deflection with better manners, and it was used here to argue for narrowing scope. Every finding below is live in code this branch touches or under a contract this branch wrote, and every one is to be fixed. Where the branch's own history is the story — a statement it authored and then falsified — that is recorded because it explains how the defect escaped, not to sort defects into owned and disowned.

---

## Summary

| # | Finding | Verdict | Evidence |
|---|---------|---------|----------|
| A1 | `incrementLastByte` over-covers a sibling subtree | confirmed | reproducer |
| A2 | V component has no length delimiter | confirmed | reproducer |
| A3 | Reuse span endpoints chosen in the wrong order | confirmed | reproducer |
| A4 | `seekBound` refuses to bind A from the binding tuple | confirmed | reproducer |
| A5 | Enum-literal drift in the reuse iterator | confirmed, **consequence corrected** | reading |
| B1 | Batch scanner has no caller | confirmed | `gopls` |
| B2 | Four orphaned symbols | confirmed, **corollary refuted** | `gopls` |
| C1 | `Through` never reaches the annotation stream | confirmed | reproducer |
| C2 | `bound` key carries two payload types | confirmed | reproducer |
| C3 | Scan-bound errors render `IndexType` with `%v` | confirmed | reproducer |
| D1 | The transcription pin shares the defect's oracle | confirmed | reading |
| D2 | `iterator_statistics_test.go` is tagged native-only | confirmed | reading |
| D3 | The VAET batch-scanner case passes for the wrong reason | confirmed | reading |
| E1 | ARCHITECTURE.md declares the removed API | confirmed | reading |
| E2 | Upgrade note says `Store.Get` remains | confirmed | reading |
| E3 | Freshness path present-tense in nine places | confirmed | reading |
| E4 | `BadgerMatcher` survives the rename | confirmed | reading |
| E5 | Ledger item 29 cites a SHA not on the branch | confirmed | construction |
| E6 | `BUG_IMPORT` citations drifted; a call is missing | confirmed | `gopls` |
| E7 | Proposal status line and sizing figure | confirmed | reading |

Three claims in the review are corrected below rather than accepted; they are marked in place.

---

## Class A — correctness

### A1. `incrementLastByte` leaves trailing `0xFF` in place, so a prefix bound over-covers

**Status**: Resolved (2026-07-26). `incrementLastByte` carries, and panics on the
all-`0xFF` prefix, which no bound can reach: every key opens with its index byte.
**Site**: `datalog/storage/key_encoder_base.go:5-19`; reached from `EncodeScanBound` (`key_encoder_binary.go:348`) and `EncodePrefixRange` (`:330`)

`prefixEnd`, deleted by this branch, carried on overflow: for a prefix `Q||0xFF` it produced `Q'||0x00`, the least key strictly greater than everything under the prefix. `incrementLastByte`, which now serves those eight converted sites, increments the first byte below `0xFF` walking backwards and **leaves the skipped bytes in place**, producing `Q'||0xFF`. That sorts above the true successor, so the range `[start, end)` covers the whole of the next sibling subtree below that byte.

The eight sites moved from the correct arithmetic to the incorrect one when they were converted; `chooseIndex` already used `EncodePrefixRange` and so already had the defect on its own paths.

This is reachable on the default path with no options set. `orderedInt64` encodes `-1` as `0x7FFFFFFFFFFFFFFF`, so **every** bound naming a negative long ends `0xFF`; one 20-byte entity hash in 256 does too, which `maxTxForEntityByScan` (`read_session.go:57`) exercises by binding EAVT on E alone.

No AVET consumer re-checks V: `matchCardinalityManyFindEntitiesWithValue` binds `[A][V]`, `cardinalityManyAVETValueIterator.Next` groups only on entity change, and `buildTuple` writes the requested `it.v` into the V position. So an over-wide range is emitted verbatim.

Observed, with `:person/count` declared long and cardinality-many, `e3 = -1` and `e4 = 7`:

```
end       = …0180ffffffffffffff     0x80 incremented, trailing FFs retained
neighbour = …0180000000000007…      key for value 7, sorts below end
```

`[:find ?e :where [?e :person/count -1]]` returns both entities.

The repair belongs in `incrementLastByte`, not at its call sites: `maxTxForEntityByScan` has no call site to correct, and every caller wants the exclusive successor. Note the all-`0xFF` case, where `prefixEnd` returned `nil` ("scan to end") and `incrementLastByte` appends `0x00`; the index byte leads every real bound and is never `0xFF`, so no live bound reaches it, but the function is general.

### A2. The V component carries no length delimiter, so a V-terminated bound is a byte prefix

**Status**: Resolved (2026-07-26), not as first proposed. A delimiter was added to the
V payload and then reverted: the format is not ambiguous — every component behind V is
fixed width and Op announces AfterRef, so DecodeKey already recovers V exactly. The
defect was an unfiltered range, not an encoding. `EncodedRun` now narrows a V-bound
prefix range to the keys whose length matches the bound, so the on-disk format is
unchanged. Pinned backend-matrixed by `TestStoreBackendVBoundRunExcludesValueExtensions`.
**Site**: `encodeBoundComponent` → `encodeValueForSearch` (`key_encoder_binary.go:420-421`, `matcher.go:42-51`)

`encodeValueForSearch` renders `[type]` followed by raw bytes, with no length and no terminator, and AVET and EAVT both place V inline ahead of E. No end key separates `"abc"` from `"abcd"`. The same two consumers never compare `datom.V`, and `checkSetMembership` (`set_resolution.go:248`) tallies every Add and Remove the range yields.

Observed, `:person/tag` string and cardinality-many, `e1 = "abc"`, `e2 = "abcd"`: `[:find ?e :where [?e :person/tag "abc"]]` returns both.

What this branch introduced is not the defect but a **false contract**: `scan_bound.go:65-68` states that a bound names a contiguous run of exactly the bound values. For a variable-length V the run is neither exact nor contiguous. Either the encoding gains a delimiter or the doc comment stops claiming what it cannot deliver.

### A3. The reuse scan picks span endpoints by typed order and closes at one binding's successor

**Status**: Resolved by removal (2026-07-26). The reuse strategy is deleted.
**Site**: `matcher_iterator_reusing.go:56-68`; endpoints from `bindingRel.Sorted()` (`matcher_relations.go:599`)

`Sorted()` orders lexicographically over the relation's symbol order using `CompareValues`, and `datalog/compare.go:17-18` states that rank order is deliberately **not** the on-disk tag order. TAEV and ATEV encode Tx with bitwise NOT, so ascending typed order is descending byte order there. The run is then closed at `incrementLastByte(through)` — the end of *one* binding's subtree, which does not dominate the others once V is variable-length (A2).

The endpoint selection predates this branch: the previous code took `calculateSeekKey(tuples[0])`'s start and `calculateSeekKey(tuples[len-1])`'s end. What is new is `Through` and its emptiness guard, which converts some previously-silent wrong ranges into a hard error.

Observed against the default path (`IndexNestedLoopThreshold` 0 vs 999999), same pattern and bindings:

| bindings | default | reuse |
|---|---|---|
| `(:a/x,"a") (:a/x,"z") (:a/y,"m")` on `[?e ?a ?v]` | 3 tuples | **2** — drops `(e2,:a/x,"z")` |
| `int64(5)`, `"abc"` on `[?e :a/mixed ?v]` | 2 tuples | **abort**: `scan bound on 5: Through ends at or below where prefix begins, naming an empty range` |

`simple_batch_scanner.calculateScanBound` solves the same problem correctly — it encodes each candidate and takes the byte-wise minimum and maximum — but that code is dead (B1).

### A4. `seekBound` refuses to bind A from the binding tuple

**Status**: Resolved by removal (2026-07-26). The reuse strategy is deleted.
**Site**: `matcher_iterator_reusing.go:299-304`

A is read only from a pattern `Constant`. For `[?e ?a ?v]` with bindings on `?a`, `analyzeReuseStrategy` returns position 1 (`matcher_strategy.go:117-119`), but `seekBound` calls `chooseIndex(nil, nil, nil, nil)`, which falls through to `ScanBound{Index: EATV}` — whole index, empty Prefix, empty Through (`matcher.go:474`). Every binding's `Seek` re-encodes the bare index byte and rewinds to the head of EATV, and the position-1 `movedPast` arm breaks on the first differing attribute.

Observed, one entity carrying `:a/one` and `:a/two`, bindings `{:a/one, :a/two}`: default path 2 tuples, reuse path **0**. (The received review predicted 1; the starvation is worse than reported.)

### A5. Enum-literal drift in the reuse iterator's `movedPast` test

**Status**: Resolved (2026-07-26). The literals became named constants; the arms went
with the reuse iterator.
**Site**: `matcher_iterator_reusing.go:125`, `:168`

Both literals name different indices than their comments: `it.index == 1` is commented "AEVT" but 1 is `EATV`; `it.index == 2` is commented "AVET" but 2 is `AEVT` (`store.go:15-24`). This is the same drift `simple_batch_scanner.go:236-243` documents as having caused silent under-counting.

The review states the consequence is that "an EATV run takes the arm that omits the intra-entity attribute-order exit and reads out the rest of each entity's datoms." That is not what the reachable combinations produce:

- **`== 1` is behaviourally inert.** EATV is selected at position 0 only when A is *non-constant* (`matcher_strategy.go:112-114`), and the else-arm's extra exit is guarded by `it.pattern.GetA().(query.Constant)`. With A non-constant that guard fails, so both arms check only entity change.
- **`== 2` is unreachable, and that is the live consequence.** Position 2 selects AVET (5) or VAET (6), never AEVT (2), so the AVET attribute-boundary exit never fires for an AVET scan. The scan over-runs its attribute rather than exiting to the next binding.

The literals should be named constants regardless; the reachability analysis is what tells you which arm to prioritise.

---

## Class B — dead code

### B1. The batch scanner has no caller, and a defect this PR reported as found lives inside it

**Status**: Resolved by removal (2026-07-26). `simple_batch_scanner.go` and its two
unreachable entry points are deleted.
**Site**: `simple_batch_scanner.go`; entry points `matcher_relations.go:1166`, `:1199`

`gopls references` returns nothing for `matchWithSimpleBatchScanning` or `matchWithBatchScanning`, in production or test. The dispatch never names them. So `calculateScanBound`'s correct byte-wise endpoint selection, the VAET type-tag correction, and `scanAndFilter` all execute zero times.

Two consequences for this PR's record. Its commit message and PR body report the VAET type-tag defect as something "the conversion turned up" without stating that the arm is unreachable. And `ScanBound.Through`'s doc comment (`scan_bound.go:76-84`) attributes the span to two scans, one of which cannot run — leaving the reuse iterator as the sole live producer, with the endpoint defects of A3.

### B2. Four orphaned symbols

**Status**: Resolved (2026-07-26). Re-verified after the iterator-reuse removal, which changed the orphan set, and all four deleted. None is consumer-facing API, so the in-repo count settles it. **Corollary refuted** (below).

**A fifth was reported here and withdrawn.** `BinaryKeyEncoder.EncodePrefixRange` has no production caller — its only use is as the oracle in `TestScanBoundEncodesAsPrefixRange`, which D1 already records as sharing the arithmetic it is meant to check. This entry previously raised its removal as a compatibility decision because the method is exported. Per the owner ruling above, that is not a question: the key encoder is not API, so its surface carries no compatibility weight, and nothing here needs deciding. Nothing outside `datalog/storage` calls **any** exported method of `BinaryKeyEncoder`, or `Store.Encoder()`. What actually keeps `EncodePrefixRange` alive is a test, and whether that test still earns its oracle is an ordinary internal question for whoever next touches it.
**Sites**: `read_session_badger.go:41`, `read_session_memory.go:36`, `matcher_relations.go:1143`, `badger_store.go:307`

All four report zero references:

- `badgerReadSession.Encoder()` and `memoryReadSession.Encoder()` survive after `StoreReader` stopped declaring the method, under a doc comment stating that no binary encoder is exposed. Both receivers are unexported and escape only as `ReadSession`.
- `validatingVBoundIterator.encodeValue` lost its only caller when the `v-validation/scan-range` event was folded into `open-scan` and `value_bytes` was dropped.
- `bytesEqual` lost its only caller with `MaxElementIDForAttribute` (`3684e04`).

**Correction.** The review states that `encodeValue` "is the only production read of `PatternMatcher.encoder`", and that removing it therefore makes the matcher's two `store.Encoder()` calls removable, leaving `database.go:147` as the last holder of `Encoder()` on `Store`. This is false: `m.encoder` is read live by `set_resolution.go:71` and `:79` (`resolveAddWinsSet` → `scanAddWinsMemory`/`scanAddWinsBadger`) and by `unique_resolve.go:56` (`walkApplyEntry`). The orphan is real; the chain it is claimed to unblock breaks at its first link, and `Encoder()` stays on `Store` regardless.

---

## Class C — observability

### C1. `addBoundFields`' `Through` branch cannot execute, and the one scan that builds a `Through` emits no bound annotation

**Status**: Resolved by removal (2026-07-26). `Through` is deleted; no bound spans two
prefixes, so there is no span to report.
**Site**: `scan_bound.go:109-113`; callers `matcher_relations.go:490`, `:1099`; producer `matcher_iterator_reusing.go:68`

`bound.through` is written only for a non-empty `Through`, and both callers pass Prefix-only bounds. The sole live `Through` producer is the reuse iterator, whose only event is `emitIteratorStatistics`, carrying pattern, index and datom counts. A span is therefore indistinguishable from a single-prefix scan in the annotation stream — which is why A3 and A4 carry no structural test.

### C2. The annotation key `bound` carries two payload types inside one event family

**Status**: Resolved (2026-07-26). The two v-validation leaf events write `bound.v`;
`bound` carries the run's bound positions across the whole family.
**Site**: `scan_bound.go:107` versus `matcher_relations.go:744`, `:1013`

`addBoundFields` sets `data["bound"]` to the run's bound positions (`[]string`) on `v-validation/open-scan`. The siblings `v-validation/candidate` and `v-validation/no-winner` set the same key to `fmt.Sprintf("%v", it.currentBoundV)` (a `string`). A handler filtering `v-validation/*` must type-switch to learn which it received. Both types are observed in the event stream of a single V-bound query.

### C3. The new scan-bound errors render `IndexType` with `%v`

**Status**: Resolved (2026-07-26). `IndexType.String()`.
**Site**: `key_encoder_binary.go:361`, `:379`, `:387`; `scan_bound.go:61`

`IndexType` has no `String()` method, so the loud failure this branch added reaches the operator as `scan bound on 5: prefix at position 0: A must be bound to a non-nil Keyword, got string`. `indexName` (`matcher.go:626`) already renders it as `AVET`.

---

## Class D — test integrity

### D1. The transcription pin computes its expected end the same wrong way

**Status**: Resolved (2026-07-26). `TestScanBoundContainsItsDatomKey` gained an
exclusion assertion against real keys and a carry fixture; both are oracle-free.
**Site**: `scan_bound_test.go:33-76`

`TestScanBoundEncodesAsPrefixRange` asserts `EncodeScanBound` equals `EncodePrefixRange`, and both derive their end from the same `incrementLastByte` call. The oracle shares the defect of A1, so the pin is self-consistent over exactly the carry that is broken. Its fixture value `int64(0x5150)` ends `0x50`, so no case in it produces a `0xFF`-terminated prefix. `TestScanBoundContainsItsDatomKey` asserts only that a datom's own key sorts *below* the end, never that a neighbouring value's key sorts at or above it.

The test's own comment claims that a deliberately independent component-order table guards against self-agreement. It does — for the component order. The end arithmetic was shared and unguarded, and that is where the defect was.

### D2. `iterator_statistics_test.go` is tagged native-only without cause

**Status**: Resolved (2026-07-26). Tag removed; the file compiles and runs on both legs.
**Site**: `iterator_statistics_test.go:1`

The file carries `//go:build !(js && wasm)` while importing only `testing`, `testify`, and three in-repo packages, and driving the public database API. The Makefile reserves that tag for "files bound to native-only surface (direct Badger imports, BadgerStore APIs)". The untagged `scan_bound_test.go` drives the same shape. The tag excludes the only pin on `PatternStorageScan`'s latency from the wasm leg; `matcher_iterator_reusing.go:241`'s emitter is unpinned on both legs.

### D3. The VAET batch-scanner case passes for the wrong reason

**Status**: Resolved by removal (2026-07-26). The batch scanner is deleted.
**Site**: `correctness_bugs_test.go` — `{"VAET_Vbound", VAET, 0, "some-value", nil, nil}`

Position 0 is E (`simple_batch_scanner.go:18`). The case names itself V-bound and passes the E position; it passes only because the VAET arm is the one arm that never reads `s.position`.

---

## Class E — documentation the branch wrote or invalidated

### E1. ARCHITECTURE.md declares the API this branch removed

**Status**: Resolved (2026-07-26). The Store block now matches the interface
method for method, with a paragraph explaining what a `ScanBound` is — the
listing alone left an implementer to guess. Swept for other stale API; none.
**Site**: `ARCHITECTURE.md:425-427`, `:435`

The Store block declares byte-range `Scan` and `ScanKeysOnly`, `Get`, and `MaxElementIDForAttribute`. A backend written from it cannot satisfy `storage.Store` (`store.go:49-51`).

Commit `50c119b` — the **first commit on this branch** — expanded that listing from five methods to sixteen and wrote those four declarations in. Commits `d22fc7d` and `3684e04` then removed them from the interface, and `3684e04` edited ARCHITECTURE.md four lines below (the ATEV rationale row) without correcting the listing above it. The branch authored the statement and falsified it in its own history.

### E2. The upgrade note says `Store.Get` remains on the interface

**Status**: Resolved (2026-07-26). The note no longer claims `Get` remains, and
gained a migration section for the typed bound plus the other removals a caller
meets. Two of those are genuinely the backend contract — `Store.Get` and
`Store.MaxElementIDForAttribute` are methods a custom backend no longer has to
provide — and `IndexNestedLoopThreshold` is a planner option a caller may have
set. `StoreReader.Encoder()` is documented as a fact rather than an obligation:
it changes nothing a backend must implement, since `Encoder()` remains on
`Store`.
**Site**: `docs/BREAKING_RELEASE_UPGRADE_v0.15.0.md:19`; cited by `store.go:47-48`

`Store.Get(index, key)` was removed by `d22fc7d`. The note still describes it as present, and `store.go` points readers at that note.

### E3. The deleted per-attribute freshness path is present tense in nine places

**Status**: Resolved (2026-07-26). No Go file mentions the path. Two stale
comments in `cache.go`, three test comments justifying live tests by a dead
consumer, and the reference doc's per-attribute diagram, code block and rationale
are corrected; `PERFORMANCE_STATUS.md` §16 and the `TODO.md` entry are amended in
place, keeping the May 2026 measurements as record. **Framing corrected by owner
ruling (2026-07-26)**: what was removed is the unwired cache gate, not the
capability. ATEV's layout still makes the first key under `[A]` the attribute's
max-Tx datom; the mark may get a working implementation later, and the docs now
say so rather than describing the capability as retired.
**Sites**: `PERFORMANCE_STATUS.md:712-734` (which also cites the deleted `BenchmarkMaxElementIDForAttribute_ATEVSeek_vs_AEVTScan`), `TODO.md:82`, `docs/reference/KEY_ENCODING_AND_CRDT.md:565`, `:596-597`, `:608-620`, `:652-674`, `docs/dev-notes/READ_PATH.md:141`, `datalog/storage/cache.go:288-295`, `atev_index_test.go:62`, `:94`, `unique_benchmark_test.go:67`

**Correction.** The review cites `READ_PATH.md` under `docs/reference/`; the file is at `docs/dev-notes/READ_PATH.md`. Its `:141` is the freshness path (`attrVersions`); its `:106` is stale for a different reason — the removed byte-range `ScanKeysOnly(index, start, end)` signature.

### E4. `BadgerMatcher` survives the rename in live source and in the status document

**Status**: Resolved (2026-07-26). Twenty-one references across fifteen live Go
files, including two failure-message strings that printed the old name. The
status document's `chooseIndexForValues`/`buildKey` mention went with the ATEV
section rewrite.
**Sites**: `matcher_relations.go:23`, `:26`, `convenience.go:83`, `database.go:624`, `cache_resolver.go:8`; failure text in `cache_integration_test.go:49` and `database_function_integration_test.go:1123`; `PERFORMANCE_STATUS.md:729` also names `chooseIndexForValues` and `buildKey`, both renamed by `566e806`

A further fourteen test comments carry the old name; the review's list is a subset.

### E5. Ledger item 29 attributes PR A to a SHA not on the branch

**Status**: Resolved (2026-07-26). The entry cites PR #114 rather than a SHA, per
this finding's own advice: no SHA written inside a branch can be its delivered
head.
**Site**: `docs/wip/DECISION_LEDGER.md:68`

The entry reads "PR A executed (`53caf46`, branch `feat/typed-bound-scan-seam`)". `53caf46` was amended into `566e806` when the docs were folded in; no commit on the branch carries that SHA. The entry ships **inside** the branch it records, so no SHA written there can be the delivered head. Name the PR, or a merge commit, or nothing.

### E6. `BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES` citations drifted and its notification list is incomplete

**Status**: Resolved (2026-07-26). Cited by enclosing function rather than line —
this finding's own lesson. The notification surface is four methods, not three:
the missing one was `UpdateMaxVersion`, the only call that advances
`slot.version` and therefore the only one whose absence produces the bug.
**Site**: `docs/bugs/BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES.md:23-24`, `:55-56`

Cited `database.go:2287`, `:2351`, `:2357`; actual `:2265`, `:2329`, `:2335` — off by 22 on all three. The document's claim that "the complete notification surface is three calls" omits `Cache.UpdateMaxVersion` (`database.go:2305`, `:2320`), which the review identifies as the only call that advances `slot.version`.

### E7. Proposal status line and sizing figure

**Status**: Partly resolved (2026-07-26). The status line now reflects PR 0
satisfied, PR A landed, PR B queued. **The sizing disagreement stands and is
recorded in place**: 67 and 73 are not reconcilable from the stated inputs — eight
slots of one `*Datom` is 64 bytes of content, so 67 implies ~95.5% node fill and
73 implies ~87.7%, while the paragraph's own "under 1% overhead" gives ~65. The
fill fraction has to be stated; inventing a reconciliation would defeat the
section's purpose. The ~9× headline holds either way.
**Site**: `docs/proposals/MEMORY_DATOM_INDEXES.md:7`, `:303` vs `:313-316`

`:7` still reads "Implementation not started" with PR 0 satisfied and PR A executed. Separately, `:303` states the eight tree slots cost **67 B/datom** bulk-built and that the table below uses that figure; `:313` reads **73**, and `:315-316` are computed from 73 (64+73+8 = 145). With 67 the projected per-datom figure is 139. The section exists so the case need not be re-derived (`:296`), which makes the internal disagreement the whole cost.

---

## Reproducers

All red at `566e806`; all compile under native and `GOOS=js GOARCH=wasm`.

| File | Test | Finding |
|---|---|---|
| `scan_bound_end_test.go` | `TestScanBoundEndIsTheExclusiveSuccessor` | A1, unit |
| | `TestScanBoundOnNegativeLongDoesNotMatchItsNeighbour` | A1, end-to-end |
| | `TestScanBoundOnStringDoesNotMatchItsExtension` | A2 |
| `iterator_reuse_equivalence_test.go` | `TestIteratorReuseMatchesTheDefaultPath/A_bound_from_bindings` | A4 |
| | `…/A_and_V_bound_from_bindings` | A3 |
| | `…/V_bound_from_bindings,_mixed_value_types` | A3 abort, C3 |
| `scan_bound_reporting_test.go` | `TestScanBoundErrorNamesItsIndex` | C3 |
| | `TestBoundAnnotationKeyHasOneType` | C2 |
| | `TestSpanningScanReportsItsThrough` | C1 |

B1, B2, D1–D3 and all of class E are established by reference analysis or reading; a test is not the instrument for "this symbol has no caller" or "this sentence is false".

**Disposition of the reproducers (2026-07-26).** The A1 and A2 pins are in the tree and green; A2's is joined by `TestStoreBackendVBoundRunExcludesValueExtensions`, which asserts the same property against both backends. `iterator_reuse_equivalence_test.go` and `TestSpanningScanReportsItsThrough` were removed with their subject when the reuse strategy and `Through` were deleted — the behaviour they pinned ceased to exist rather than becoming unreachable.

---

## Disposition

Ruled 2026-07-26: **every finding is fixed here.** An earlier draft proposed deferring A2, A3, A4, A5, B1 and D3 on the grounds that they predate the branch. That reasoning is withdrawn — see *On age* above.

Two constraints on the repairs, both of which the findings themselves establish:

**A2 is fixed by making the claim true, not by making it smaller.** The contract in `scan_bound.go` says a bound names a contiguous run of exactly the bound values. Narrowing that sentence to accommodate a V encoding that cannot deliver it would write the defect into the specification and close the finding without explaining it. Either the encoding gains what the contract requires, or the contract's failure is documented as a known limit with its own bug record — not silently softened.

**Correction (2026-07-26).** Those are not the only two outcomes, and the pair is the same error the rest of this document is about: both assume the *byte range* is the only thing that can be exact, which is the contract stated physically. A delimiter was added to the V payload on that reasoning and then reverted. The format was never ambiguous — every component behind V is fixed width and Op announces AfterRef, so `DecodeKey` already recovers V exactly. What was inexact was the range, and the fix is a membership test on key length inside the binary store. The on-disk format is unchanged. The general lesson: the seam's contract is logical — a scan yields the datoms whose bound components equal the bound's values — and how a backend achieves that is not seam vocabulary.

**A5's repair is named constants, but its priority comes from reachability.** The mislabelled `== 1` arm is behaviourally inert for every combination that reaches it; the mislabelled `== 2` arm is unreachable, which is what silently disables the AVET attribute-boundary exit. Replacing integer literals with `IndexType` constants makes both statements checkable by the compiler instead of by this document.

Sequencing is the owner's; the ordering used was A1 first as the live regression, then A3/A4/A5 together in the reuse iterator, then B1 with D3 falling out of it, then A2 last as the largest surface.
