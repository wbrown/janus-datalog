# PR #114 re-review — round 2 — 2026-07

**Date**: 2026-07-26
**Head reviewed**: `6c41f83` (`feat/typed-bound-scan-seam`)
**Scope**: the seventeen round-1 findings' resolution status, plus what the round-1 remediation itself introduced or left behind
**Method**: a round-2 review was received against `6c41f83`; every claim in it was then independently verified against this tree by reading the cited code, never accepted from the report. Where the report's line citation was off, the claim was re-checked at the symbol rather than the line and the discrepancy is recorded. Two of the report's own claims are corrected below.

**Remediation status (2026-07-26)**: every finding is closed, including the metric decision that was briefly left open under N2. **N13** is a coverage observation rather than an action. Two further defects were found during remediation and are recorded as **N15** and **N16**. `make test` green on both legs.

**Verification scope**: every finding in this document was checked directly. Nothing here is carried on the reporter's citation alone. Line numbers are as of `6c41f83` and have since drifted; cite by symbol when acting.

**Status convention** (as `docs/bugs/`): each finding carries a `Status:` line. `Open` means confirmed present and unfixed. `Resolved` requires a date and the change that closed it.

---

## Part 1 — the seventeen round-1 findings

Thirteen were already resolved; R13, R14 and R16 were partial and are now closed; R17 remains open.

### Resolved, verified

| # | Finding | Evidence |
|---|---|---|
| R1 | `incrementLastByte` carry | `key_encoder_base.go` zeroes each carried byte and panics on all-0xFF. Pinned by `TestScanBoundEndIsTheExclusiveSuccessor` and the carry subtests of `TestScanBoundContainsItsDatomKey`. |
| R2 | V-terminated bound is a byte prefix | `EncodeScanBound` sets `Membership.size`. Pinned by `TestStoreBackendVBoundRunExcludesValueExtensions` on both backends. |
| R3 | Reuse span endpoints | Resolved by removal; `matcher_iterator_reusing.go` absent. |
| R4 | `seekBound` would not bind A | `matcher_strategy.go`'s `case 1: // A is bound` → `AETV` routes to `scanBoundForValues`, whose `AEVT, AETV` arm binds the Keyword then E. |
| R5 | Batch scanner uncalled | Resolved by removal. The surviving `VAET` arm passes typed `Identity`/`Keyword`, so the tag comes from the encode inside `encodeBoundEndpoint`. |
| R6 | Four orphans | `Encoder()` correctly stays on `Store` — `set_resolution.go` and `unique_resolve.go` read `m.encoder`. |
| R7 | Index literals with wrong comments | Resolved by removal. |
| R8 | Self-consistent oracle | `scan_bound_test.go` states it directly: "Equality is all it proves," and names the two tests that compare against keys the encoder laid down. |
| R9 | `addBoundFields` `Through` arm | Three unconditional writes. |
| R10 | `bound` key with two payload types | Closed; see D1 — the fix itself introduced a naming defect, now also closed. |
| R11 | `%v` on `IndexType` | `IndexType.String()` with a non-panicking `IndexType(%d)` default. See N5 — the mapping is now pinned too. |
| R12 | Native-only tag | `iterator_statistics_test.go` is bare `package storage`; `GOOS=js GOARCH=wasm go vet ./datalog/storage` clean. |
| R15 | Ledger item 29's SHA | The ledger is correct under its amend convention; the defect was in the status line reporting it. See S2. |

### R13. Documentation presenting removed API as live

**Status**: Resolved (2026-07-26). Eleven of thirteen locations were corrected in round 1; the remaining two are now fixed. `database.go`'s `WarmCache` doc no longer lists "Attribute-level version tracking for freshness checks" among what it populates, and `READ_PATH.md` shows the typed `ScanKeysOnly(ScanBound{…})` rather than the byte-range form the same commit had corrected elsewhere.

### R14. Rename residue

**Status**: Resolved (2026-07-26), deliberately partial in scope. The three comments in `choose_index_values_test.go` naming `chooseIndexForValues` now name `scanBoundForValues`, the live function.

The six `TestChooseIndexForValues*` function names and the filename are **kept**. `CRDT_VECTOR_STORAGE_IMPLEMENTATION_PLAN.md` and `BUG_CACHE_ATTRIBUTE_FRESHNESS_NOT_O1.md` cite four of those names as completed-verification records; renaming breaks those citations to gain name-agreement with an unexported function. Same reasoning the owner applied to `PERFORMANCE_STATUS.md`: a record of what was verified is not a live claim to be rewritten.

### R16. `BUG_IMPORT_LEAVES_STALE_CACHE_ENTRIES` citations

**Status**: Resolved (2026-07-26). The two remaining line citations are re-anchored to the enclosing function and its two `GetOrResolve` arms. The notification surface is corrected from four methods to **five**: `Cache.InvalidateRewind`, called from `Database.TruncateTo`, is not on the commit path, which is why the "every call inside `Transaction.Commit`" framing hid it — the same open-world hazard the bug itself is an instance of.

### R17. `MEMORY_DATOM_INDEXES.md` sizing

**Status**: Resolved (2026-07-26). The prose said 67 B/datom while the table's tree-slot row said 73 and its totals were computed from 73. The prose now states the 67–114 range and says the table uses 73, and the paragraph flagging the disagreement is gone.

Nothing was re-derived and no fill fraction was pinned: this is an order-of-magnitude projection whose own ~9× headline holds across the whole range, so the precision the earlier note demanded was cognition the estimate does not repay (owner ruling, 2026-07-26).

---

## Part 2 — new findings

### N1. PR B's design of record specified the removed shape

**Status**: Resolved (2026-07-26). **This was the most consequential item in the review** — the only one that propagates a defect forward into code not yet written.

`MEMORY_DATOM_INDEXES.md` had specified `ScanBound`'s optional `Through`, its subtree-end semantics, the inverted-`Through` rejection, and both deleted files as what PR A landed, with no mention of `EncodedRun` or membership. It now records what actually landed: `Through` is gone with both files that motivated it, and a `ScanBound` is an equality constraint on leading components and nothing else.

It also now states the part an implementer will not infer from the type: **the seam's contract is logical, and narrowing is the backend's obligation.** A scan yields exactly the datoms whose bound components equal the bound's values; for a byte-key backend that is not free, and `EncodedRun`/`runMembership` is how the in-tree stores discharge it. A tree-backed backend comparing typed components has no such gap and needs no membership test — which is exactly why the obligation has to be written as a contract rather than left implicit in the byte encoder.

`BREAKING_RELEASE_UPGRADE_v0.15.0.md`'s *Custom backends* section, which had listed four obligations and no membership requirement, now carries the same statement.

### N2. The one non-exact run reachable from query text emitted no annotation

**Status**: Resolved (2026-07-26).

`matchCardinalityManyFindEntitiesWithValue` opens `AVET[a, v]` with a string V — a non-exact `Membership`, so the store steps over keys the range over-covers — and returned before the `pattern/index-selection` emit. A key dropped by `holds()` was indistinguishable in the stream from a key never in range and from a scan that never narrowed, on exactly the query class the round-2 fix was built for.

`emitIndexSelection` now builds and emits, and **every call site carries its own visible `if m.handler != nil`**. The guard is the caller's: it gates the caller's own argument preparation as well as the map, the `pattern.String()` and `describeRun`'s slices, and at the call site it marks the block as observability rather than a step in opening the scan. Four scan-opening arms — the general one and the three cardinality-many/-vector arms that returned before it — announce their bound, each passing the same `ScanBound` value it hands the reader so the announced run cannot drift from the walked one.

**Corrected in round 3**: "all four" was wrong, and `emitIndexSelection`'s own doc comment repeated it as a completeness claim. Three further dispatch arms open scans and announce nothing — `matchCardinalityVectorAsRelation`, `matchCardinalityManyAsRelation`, `matchCardinalityManyMembership` — as does `matchFromCache`, the default for every E-and-A-bound pattern and so the most common shape in the engine. See round 3's T1 and T2.

One downstream consequence is fixed: `pattern/hash-join-complete` carries `Start` and `Latency`, so `Database.Analyze` no longer reports 0 ms for every hash-join scan.

**The other went further than the review's framing, and is now fixed.** The review said `datoms.scanned` had come to count keys yielded rather than spanned. Investigating it showed the name had been wrong since well before this branch: all three counters increment on what their *inner* iterator produced, and on the normal read path that inner iterator is a `CRDTResolvingIterator`. The count therefore sat below resolution, not merely below membership — off by the history depth, not by the handful of keys a bound excludes.

Resolved in two moves, on owner ruling (2026-07-26):

- The existing counter is renamed **`datoms.resolved`**, which is what it measures: resolution's output. Seven readers followed, five of them comma-ok reads that would otherwise have silently reported zero, and one an unchecked `.(int)` in the formatter that would have panicked at render.
- **`datoms.scanned` is restored to its original meaning** — intake, datoms read from the index — and now exists. `Iterator.Scanned() int` is **required** on the backend contract, not an optional capability: an obligation that cannot be audited is not one, and an absent count is indistinguishable from a scan that read nothing and from instrumentation never wired. `BadgerIterator` and `memoryIterator` count keys taken from the range *before* the membership test, so what a bound rejected is included; `CRDTResolvingIterator` delegates. `nonReusingIterator` accumulates each per-binding scan's intake before discarding it, since by Close all but the last are gone.

The pair is the funnel: `scanned/resolved` is what the index charged, `resolved/matched` is what the pattern rejected. `-verbose` renders intake on the scan line only when it exceeds output, so a tight scan stays one number and an amplifying one says so. Pinned by `TestScanReportsIntakeAndResolution` (three writes, 3 read against 1 resolved, on both optimizer modes) and `TestScanLineShowsIntakeWhenItExceedsOutput`; the first also runs its captured events through the formatter, because a producer pin and a formatter pin that never meet is exactly what let the scan line render fields no emitter produced.

**Per-binding scans report a count, not an event each** (owner ruling, 2026-07-26): the stream must let a reader trace a query, not bury it. `nonReusingIterator` already tracked `totalScanned`/`totalMatched` and never reported them; it now emits one `pattern/per-binding-scan-complete` on `Close` carrying `scans.opened`, `binding.size`, `datoms.scanned`, `matches.found`, `Start` and `Latency`. Pinned by `TestPerBindingScanReportsOneCountedEvent`, which asserts *exactly one* event and so reds if the path ever emits per binding.

`checkSetMembership` was named in the review as the same shape. It is not: its only per-entity caller was inside dead code (see N15). Its surviving caller does a single scan with E bound.

### N3. One of four membership assignment sites had no native-leg pin

**Status**: Resolved (2026-07-26). `TestReadSessionVBoundRunExcludesValueExtensions` drives an inexact V-bound run through a read session on both backends and both legs. The store-path test reaches `MemoryStore.scan` and Badger's iterator constructor, never the sessions, so deleting `read_session_memory.go`'s membership assignment left the native gate green. It now reds: `runMembership`'s zero value is `{exact: false, size: 0}`, so `holds` computes `len(key) == keyTailSize(key)` — false for every real key. The absence fails closed.

### N4. `PayloadIsFixedWidth` coverage

**Status**: Resolved (2026-07-26). All six variable-width types are covered.

`TypeKeyword` and `TypeSymbol` joined `TestStoreBackendVBoundRunExcludesValueExtensions` — `:status/act` against `:status/active` is the Keyword form of `"abc"` against `"abcd"`. `TypeCompressedString` and `TypeCompressedBytes` needed their own case, `TestStoreBackendCompressedVBoundRun`, because they exist only when the encoder carries a compression threshold and the parity test opens with the zero-threshold encoder. It asserts the fixture actually lands in Tier 2 rather than skipping if it does not, so a fixture that drifts out of the tier says so instead of passing empty.

The eight fixed-width entries remain unpinnable through `EncodeScanBound`, and provably so: answering a fixed-width tag "variable" is a behavioural no-op, because `size + tail` then equals exactly the length the exact arm admits.

### N5. `IndexType.String()`'s name mapping was pinned by nothing

**Status**: Resolved (2026-07-26). `TestScanBoundErrorNamesItsIndex` compares `String()` against itself, so it reds on deletion and never on a wrong name. `TestIndexTypeNamesEveryConstant` pins all eight literal names, the loud `IndexType(N)` default, and `len(named) == len(Indices)` so the table cannot fall behind the enum. Swapping two arms now reds.

### N6. The removal sweep tracked the round-1 finding list, not the class the deletions created

**Status**: Resolved (2026-07-26), handled in three kinds rather than as one sweep — the distinction is the point.

**Live status claims, corrected.** README's four freshness sites and `docs/ideas/README.md`. The README figure turned out sound: 2.2×–555× measures the *high-water-mark seek*, which ATEV still provides; only the cache gate built on it was removed. The figure was reattached to what it measured rather than deleted.

One live status claim was missed and is corrected in round 3: `PERFORMANCE_STATUS.md`'s *What's Actually Working* list carried "Batch Scanning with Iterator Reuse (ACTIVE)" with a 100-binding threshold and a line citation that now points at the cache-check block. Both mechanisms it named were removed in v0.15.0. The entry now describes `chooseJoinStrategy`, which is what occupies that decision point.

**Dated changelog entries, annotated in place.** `CLAUDE.md`'s August 2025 and June 2026 blocks and `PERFORMANCE_STATUS.md`'s rejected-consolidation record. These are history; rewriting them erases what was believed at the time. The `PERFORMANCE_STATUS.md` note also records its own internal inconsistency (it says four and lists three).

**Standing instructions, marked discharged or moot.** `ITERATOR_LIFECYCLE_MANAGEMENT.md`'s "Immediate (do now)", the `BUG_ELEMENTID_NOT_FIRST_CLASS` and `BUG_ELEMENTID_ASOF_THROUGHOUT` work tables, `KEYWORD_POINTER_OPTIMIZATION.md`'s location list. These are not records — they direct future work at files that no longer exist.

**Live source and tests**, all corrected: `matcher_relations.go`'s package file map, `matcher_strategy.go`, `iterator_validation.go`, `executor/executor.go`, `key_encoder_binary.go`'s two-endpoint Tx instruction, and the test comments in `hash_join_symbol_index_test.go`, `hash_join_scan_range_test.go`, `merge_join_test.go` and `predicate_pushdown_trace_test.go` (which also rendered an `IndexType` with `%d`, bypassing `String()`). **Three sites the received review missed** are included: `matcher_multi_position_test.go`, and `executor/relation_input_parallel_correctness_test.go` twice.

### N7. `TestBatchScanPerformance` and `BenchmarkBatchScanning` ran one path twice

**Status**: Resolved (2026-07-26), with a correction to the finding.

`batch_scan_performance_test.go` is deleted. `BenchmarkBatchScanning`'s two fictitious arms — `RegularIteratorReuse` and `BatchScanning`, separated only by `oldThreshold := 10000; _ = oldThreshold` — are gone.

**The finding lumped in a third arm that was not fictitious.** `NoConstraints` runs one path, makes no strategy claim, and is cited by full name at `PERFORMANCE_STATUS.md:59` as `BatchScanning/NoConstraints −19.3%` for the interned-identity work. It is kept, under its enclosing benchmark's original name, so that figure stays re-measurable. `PERFORMANCE_STATUS.md` is untouched — it is a record of what was measured, not a live claim.

### N8. `aevt_matcher_bug_test.go` branched on deleted producers

**Status**: Resolved (2026-07-26). The three-name branch is now the one event that has a producer; neither `pattern/iterator-reuse-complete` nor `pattern/multi-match` appears in non-test source. The comment presenting `IndexNestedLoop 2298µs vs HashJoinScan 203µs` as measured fact — from benchmarks that no longer exist — is replaced by a statement of what the scan count means on this path.

### N9. Two symbols with no reader

**Status**: Resolved (2026-07-26). `entityBytesFor` deleted; `ReuseStrategy.BoundV` deleted. Its siblings were checked first and kept: `BoundA`, `NeedsValidation` and `ValidationIndex` all have live readers.

### N10. The seam's published contract described what round 1 said it could not deliver

**Status**: Resolved (2026-07-26). One finding, one fix — see the correction below.

`ScanBound`'s doc comment no longer calls a bound "a contiguous run". It names the datoms whose leading components equal the bound's values, says this is a logical set rather than a byte range, and states narrowing as the backend's obligation. The two comments pointing at "ScanBound's run predicate" now point at `EncodedRun`'s membership rule, where it lives.

The finding also named `store.go`'s `Scan`/`ScanKeysOnly` as silent on the obligation, and the first remediation pass missed it — the statement went into `ScanBound`'s comment and the upgrade note but not onto the interface an implementer actually implements. Both methods now carry it.

### N11. `JoinStrategy` renumbering unrecorded

**Status**: Resolved (2026-07-26). The upgrade note records that removing the first constant moved `HashJoinScan` 1→0 and `MergeJoin` 2→1, that nothing persists a `JoinStrategy`, and that callers should compare against the constants or `String()`.

### N12. `BUG_VALUE_DOMAIN_UNENFORCED_IN_COMPARISON` cited moved lines

**Status**: Resolved (2026-07-26); the line citations drifted again within the same commit, and are corrected in round 3. The `CompareValues` sites are cited by enclosing function, which holds. Their line numbers do not: this round's own edits to `hash_join_matcher.go` — the `Start`/`Latency` block and the intake key on `Close` — moved all three down by eight, so `BUG_VALUE_DOMAIN_UNENFORCED_IN_COMPARISON.md` said `:732`, `:766`, `:774` against an actual `:740`, `:774`, `:782`. A finding about drifted citations, re-drifted by the commit that fixed it.

### N13. Coverage lost with the deleted benchmarks

**Status**: Recorded, no action. Both deleted files set no threshold and ran on `matchWithHashJoin`, carrying result-count assertions that went with them. Read at `88dadaa` there are **four**, not the three the review states: `count != 500` and `len(allResults) != 500` in the profile file, and `count != bindingSize` twice in the threshold benchmark. The discrepancy does not change the substance; the surviving `BenchmarkBatchScanning/NoConstraints` asserts its own count.

### N14. Convention items

**Status**: Resolved (2026-07-26).

Ten test functions across those files loop the optimizer-mode axis: six in `vbound_bytes_validation_test.go`, two in `scan_bound_end_test.go`, one in `scan_bound_test.go`, one in `backend_contract_test.go`. One deliberate exception, stated in the code: `TestVBoundCardinalityOneBytes_DirectMatcher` drives the matcher rather than the executor, so the algebra optimizer is not on its path; it pins one mode explicitly, which is what the convention prescribes instead of looping an axis that means nothing there.

*(This entry said "all five files are on the optimizer-mode axis — fifteen test functions" until round 3. The count was wrong, and so was the framing: `matcher_planner_options_test.go` gained a numeric axis, not this one, and most of `backend_contract_test.go`'s test functions execute no query, so the axis does not apply to them at all. Round 3 also found two tests still off the axis; see T17.)*

`matcher_planner_options_test.go` regains a numeric axis via `MaxSubqueryWorkers`, the remaining numeric `PlannerOptions` field with an `ExecutorOptions` counterpart. Non-vacuous by construction: default 0, custom 7, so a dropped field reds rather than coincidentally matching.

`compressed_key_test.go`'s `d.A.String() == decoded.A.String()` on an interned Keyword is left alone — a tree-wide class at roughly a hundred sites, not a branch defect.

### N15. A dead iterator, found during remediation

**Status**: Resolved (2026-07-26), by owner ruling — dead functions are not kept.

`cardinalityManyFindEntitiesWithValueIterator` — struct plus `Next`, `Tuple`, `Close`, `Error` — had **no constructor anywhere**. It was marked DEPRECATED in favour of `cardinalityManyAVETValueIterator`, which is what the live path builds. Deleted.

This is what corrected N2's scope: the dead iterator held the only per-entity `checkSetMembership` call, so that function was never on a per-binding path. `nonReusingIterator` was the sole genuine per-binding scan site, and is the only one instrumented.

### N16. Seven dead formatter arms, and a guard inside an annotation emitter

**Status**: Resolved (2026-07-26), found while renaming the metric.

`OutputFormatter.Format` carried seven `case` arms for event names with no producer anywhere — `pattern/multi-match`, `pattern/multi-match-filter`, `pattern/match`, `pattern/match-with-bindings`, `badger/match-with-bindings`, `expression/evaluate`, `filter/predicate`. Checked as string literals and as constants; nothing emits any of them. Two of the seven read the key being renamed, which is how they surfaced. All deleted; `colorizeCount` and `RenderRelationWithAttrs` were checked and both keep live callers.

`aevt_bug_test.go` branched on four event names of which three had no producer, and read a datom count from `pattern/index-selection`, which carries none — so it accumulated zero, logged it, and asserted nothing on it. Reduced to the one live name.

Separately, `emitIteratorStatistics` held its own `if handler == nil` guard. **Owner ruling: the existence guard never lives inside the annotation function.** The guard is the caller's because it gates the caller's own argument preparation as well as the work inside, because it is a call plus a branch per scan otherwise, and because at the call site it marks the block as observability rather than part of the operation. Moved to `unboundIterator.Close`, its one caller; a nil handler now panics rather than silently doing nothing.

This is the same ruling that reshaped N2's `emitIndexSelection`, which had been written with the guard inside — copied from this very function. The local convention was the defect.

---

## Part 3 — defects introduced by the round-1 remediation

### D1. The C2 fix introduced a third name for a datum that had one

**Status**: Resolved (2026-07-26). `bound_v` already existed at three sites for the same `it.currentBoundV`; the C2 fix added `bound.v` at two more. Both now write `bound_v`, so the whole v-validation family agrees.

### D2. Dead code added

**Status**: Resolved (2026-07-26). `entityBytesFor` deleted — see N9.

### D3. Stale narration in the two new test files

**Status**: Resolved (2026-07-26).

`scan_bound_reporting_test.go` no longer says `IndexType` has no `String` method or names `indexName`, and its `bound`-key comment describes the current key rather than the pre-fix one.

`scan_bound_end_test.go` needed care, and **the received review stated it imprecisely**. It read as a stale comment; in fact its complaint — that `ScanBound`'s doc comment was wrong for a variable-length V — was still live, because that comment still said "contiguous run". This and N10 are one finding with one fix, not two.

---

## Part 4 — overclaimed statuses in the round-1 document

All three are corrected in `PR114_TYPED_SCAN_BOUND_2026_07.md`, rewritten to say they overclaimed and when they actually closed rather than quietly becoming true.

### S1. E3

**Status**: Resolved (2026-07-26). Claimed no Go file mentioned the freshness path while `database.go`'s `WarmCache` doc did. Both that and `READ_PATH.md` are fixed; the status now records that the claim became true at round-2 remediation, not when written.

### S2. E5

**Status**: Resolved (2026-07-26). Claimed the ledger cites PR #114 rather than a SHA; `DECISION_LEDGER.md` still carries the SHA, correctly, because the ledger's convention is amend rather than rewrite and the withdrawal in the amendment *is* the correction. The finding was addressed; the status describing how was the inaccurate part, and now says so.

### S3. E6

**Status**: Resolved (2026-07-26). Overclaimed twice: two citations were left as drifting line numbers, and the surface is five methods rather than four. A finding whose own lesson is "cite by enclosing function" had shipped a status that cited lines and then drifted — the lesson failing on itself.

---

## Disposition

Closed: R13, R14, R16, R17, N1–N12, N14, N15, N16, D1–D3, S1–S3. Nothing is open.

Deliberately not acted on:

- **`compressed_key_test.go`'s render-to-compare** on an interned Keyword — a tree-wide class at roughly a hundred sites, per the review's own scoping. Fixing one site on this branch would be arbitrary.
- **N13** — a recorded observation. The surviving `BenchmarkBatchScanning/NoConstraints` asserts its own count; the lost assertions measured paths that no longer exist.

Three corrections to the received round-2 review, all recorded above: its stale-comment finding merged into N10/D3 because the complaint was still live rather than stale; its three-versus-four count in N13; and its removal sweep under-counted by three sites, added to N6.

One finding of the review was wrong in a way that mattered and was caught only during remediation: N7's description of `BenchmarkBatchScanning` as two arms running one path. It had three, and the third was a real measurement carrying a live citation. Deleting the whole benchmark on that description would have made a cited figure unreproducible.
