# PR #114 re-review — round 3 — 2026-07

**Date**: 2026-07-26
**Head reviewed**: `400f0e1` (`feat/typed-bound-scan-seam`)
**Scope**: the round-2 findings' resolution status, plus what the round-2 remediation itself introduced or left behind
**Method**: a round-3 review was received against `400f0e1`; every claim in it was then independently verified against this tree by reading the cited code, never accepted from the report. Two of the report's own claims are corrected below.

**Remediation status (2026-07-27)**: of the eighteen received findings, seventeen are closed and T18 alone is partly closed, its five documents held under ruling 4; T19, found in this pass, is closed. Ruling 7 is extended to the payload keys and to the values under them — see "The payload vocabulary" in the disposition. Two further defects were found in this pass rather than reported: an index-key collision that made `Database.Analyze` render a subquery's ordinal as a physical index, and `Seek` discarding the end of the run it was handed.

A third was found the same way and is fixed here: `pull/attr.lookup` reported `found: false` on every event, in every trace, since the context was written.

**Verification scope**: every finding in this document was checked directly against `400f0e1`. Nothing here is carried on the reporter's citation alone. Line numbers are as of `400f0e1`; cite by symbol when acting.

**Status convention** (as `docs/bugs/`): each finding carries a `Status:` line. `Open` means confirmed present and unfixed. `Resolved` requires a date and the change that closed it.

---

## Owner rulings (2026-07-26)

**1. Complete the coverage** (T1, T2, T15, T16, T19). Every path that opens a scan for a pattern announces its bound and reports its funnel on completion; merge join gains a completion event; phase execution gains its `phase/complete` producer, which makes `Database.Analyze`'s phase line and `CLAUDE.md`'s event list true rather than aspirational; the completion events gain formatter arms.

Nested resolution reads — the per-entity and per-entry scans inside set, vector, unique and cache resolution — **contribute their intake to the enclosing pattern's count rather than emitting events of their own**. That is not a narrowing of this ruling; it is ruling 3 applied to ruling 3's siblings, and it is the shape the owner already set for per-binding scans in round 2: the count is the datum, and enumerating one event per nested read would bury the query the stream exists to describe.

**2. `datoms.matched` everywhere** (T14). One funnel, one prefix: `datoms.scanned` → `datoms.resolved` → `datoms.matched`. `matches.found` retires.

**3. Count the unique-mode supersession scans** (T4). `resolveMaxOtherTxForValue`'s AVET reads are real index reads on the resolution path, so they belong in what `CRDTResolvingIterator.Scanned()` reports. The reported intake becomes the read the query actually paid for, which is the counter's stated purpose.

**5. Change `CacheResolver`; `CacheEntry` carries its build cost** (T1, T2). The three resolve methods report the intake they spent, the cache stores it on the entry, and `matchFromCache` reads it back — so the entry knows what it cost to build and a second reader of the same entry can learn it too. A cache hit reads no index and reports zero, which is the honest answer rather than an absent one.

**6. Delete the dead exported event-name constants** (T10). Ten constants name events with no producer. Nothing in-tree references them; an external handler switching on one receives nothing today either, so the arm is already dead on their side.

**7. Delete the other ten too, and stop emitting events as literals** (T10). The ruling extends to the full dead set including the `Error*` taxonomy, and to its mirror image: a producer never writes an event name inline. One vocabulary, declared once, referenced by every emitter and every consumer — which is the only arrangement in which "does anything emit this?" has an answer a search can find.

**8. `rebuild*` must not swallow an error** (found while answering "can a cache entry ever be nil?"). It could, and nil meant exactly one thing: resolution failed. `Cache.rebuild`, `rebuildOne/Many/Vector`, `ResolveEntry` and `GetOrResolve` now return errors, and every caller propagates. Two callers had no error to return and gained one: `Transaction.vectorContainsValue`, where answering `false` on a failed read inserts a duplicate into a set the schema declares unique, and `matchFromCache`, where falling back to storage would re-run the read that just failed.

**9. The annotation handler is a public field** (`Database.AnnotationHandler`). No getter, no setter, no mutex. The getter/setter pair existed because the handler was *copied* into `Cache` and every `Matcher`, and a plain assignment cannot fan out to duplicates — the copies were the cause and the setter was downstream of them. `Cache.handler` and `Cache.SetHandler` are deleted; the handler is a per-call parameter to `GetOrResolve` alongside `bound`, so nothing is retained and nothing can go stale. `PatternMatcher.SetHandler` **stays**: it is the write half of a duck-typed decorator protocol (`executor/annotated_matcher.go`, `source_router.go`) invoked at wrap time, not a field accessor.

`annotations.Synchronized` is no longer applied on the way in. Wrapping at one assignment path and not another would give one field two concurrency contracts, and it put a process-wide lock on every event on the hot path. Handlers that are not concurrency-safe are wrapped by whoever installs them. **This is a change to a documented promise** and belongs in the upgrade guide.

**4. Leave the five held documents alone** (T18). The `CLAUDE.md` convention already tells readers not to infer current state from a dated document and to re-read the cited code before acting. The four documents edited last round under the rejected rationale stay as they are; they are not re-edited here in either direction.

**10. `datoms.scanned` is what *this call* read** (T1, refining ruling 5). Ruling 5 put the build cost on the entry and had `matchFromCache` read it back, which makes a trace of a thousand cache hits claim a thousand index reads that never happened. The two numbers are different and both exist: `CacheEntry.scanned` is what building the entry cost and stays with it for its life, while `GetOrResolve` returns the intake of the call — the build cost when that call built it, zero when it came from cache. Every other producer of the key already means this-operation's intake, and one key means one thing.

The value is returned, not read back through a method. A function whose body is a field access, or a field access behind a nil check, is the expression at the call site written twice; `Cache.GetOrResolve` is measured at 6.1 ns on a hit, which is not a budget for one.

---

## Part 1 — the review's four corrections to round 2

All four correct the *round-2 review's* description of the tree, and all four agree with what the round-2 remediation actually did. No action.

| # | Correction | Disposition |
|---|---|---|
| 1 | `BenchmarkBatchScanning` had three arms, not two. `NoConstraints` is a distinct body making no strategy claim, and is what `PERFORMANCE_STATUS.md:59` cites by full name. | Matches what shipped: the benchmark was kept carrying that arm alone. Recorded in round 2's Disposition as the review's one consequential error. |
| 2 | Four result-count assertions went out with the deleted files, not three — `iterator_reuse_profile_test.go:82`, `:152`; `join_strategy_threshold_bench_test.go:108`, `:143`. | Matches round 2's N13 as amended. |
| 3 | The surviving dead references were under-counted by three: `matcher_multi_position_test.go:594-595` and `relation_input_parallel_correctness_test.go:439-440`, `:501`. | Matches round 2's N6 as amended. |
| 4 | `scan_bound_end_test.go:92-95` was not stale narration — at `head2` it described the tree accurately, because `scan_bound.go:65` still said "contiguous run". One finding with the contract wording, not two. | Matches round 2's D3. |

---

## Part 2 — round-2 findings, as reported and verified

| # | Round-2 finding | Round-3 status |
|---|---|---|
| N1 | PR B's design of record specified the removed shape | Resolved |
| N2 | The one non-exact run reachable from query text emitted no annotation | **Partly resolved.** The intake metric exists and is named correctly; its coverage is not what the remediation claimed. See T1, T2, T3, T4. |
| N3 | `read_session_memory.go` membership had no native pin | Resolved, and verified by mutation — deleting the assignment reds nine native tests |
| N4 | `PayloadIsFixedWidth` coverage | **Partly resolved.** `TypeKeyword` and `TypeSymbol` are real pins. The compressed pair is not. See T12. |
| N5 | `IndexType.String()` unpinned | Resolved |
| N6 | Removal sweep tracked the finding list, not the class | **Partly resolved.** Corrected across roughly twenty sites; survivors remain. See T9, T18. |
| N7 | Batch-scan test and benchmark arms | Resolved |
| N8 | `bound.v` versus `bound_v` | Resolved — no `bound.v` remains |
| N9 | `entityBytesFor`, `ReuseStrategy.BoundV` | Resolved — both deleted |
| N10 | `ScanBound` contract wording | Resolved |
| N11 | `JoinStrategy` renumbering | Resolved — recorded at `BREAKING_RELEASE_UPGRADE_v0.15.0.md:117-121` |
| N12 | `BUG_VALUE_DOMAIN_UNENFORCED_IN_COMPARISON` citations | **Regressed by the same commit.** See T9. |
| N13 | Lost benchmark result-count assertions | Recorded observation; not acted on |
| N14 | Convention items | **Partly resolved.** Five files joined the axis; two tests remain off it and the recorded count is wrong. See T9, T17. |
| N15 | Dead iterator | Resolved — deleted |
| N16 | Seven dead formatter arms, guard inside emitter | **Partly resolved.** Seven string-literal arms deleted, nine constant-named arms remain; the guard ruling landed in one package. See T10, T11. |
| D1 | Third name for one datum | Resolved |
| D2 | Dead code added | Resolved |
| D3 | Stale narration in two new test files | Resolved |
| S1–S3 | Overclaimed round-1 statuses | Resolved for E3, E5, E6 — and two new false statuses took their place. See T9. |

---

## Part 3 — new findings

### T1. The intake counter is emitted by three events, none of them on a path whose run can exclude a key

**Status**: Resolved (2026-07-27). Every arm that opens a scan for a pattern now reports the funnel on a completion event, and `TestEveryDispatchArmAnnouncesItsRunAndReportsItsFunnel` gives each arm a row that reds alone if its emit is deleted. `matchFromCache` reports the call's intake per ruling 10 — the build cost when it built the entry, zero on a hit — on `pattern/cache-resolve-complete`.

Every resolver now returns the intake it spends — `resolveAddWinsSet` and `resolveVector` on their result structs, `checkSetMembership`, `loadRGAElements`, `resolveMaxOtherTxForValue`, `resolveAVLWW` and `walkUniqueEntityValue` as a return — and `CacheResolver` carries it to `CacheEntry`. `matchCardinalityManyMembership` is converted end to end and is the worked example: it builds the `ScanBound`, hands the same value to `emitIndexSelection` and to `checkSetMembership`, and reports the funnel on `pattern/storage-scan`. `LookupByUnique` gained `unique/lookup-complete` for the same reason. What remains is the other arms.

`datoms.scanned` is written at exactly three sites: `iterator_validation.go:101`, `hash_join_matcher.go:689`, `matcher_iterator_nonreusing.go:144`.

Five paths open a bound whose V is an arbitrary domain value, so `encodeBoundEndpoint` marks the run variable-width and `holds()` can reject keys inside the byte range: `checkSetMembership` (`set_resolution.go:257`), `resolveMaxOtherTxForValue` (`unique_resolve.go:144`), `resolveAVLWW` (`:212`), `matchCardinalityManyFindEntitiesWithValue` (`matcher_relations.go:2207`), and `openCRDTScan` (`:1062`). None reports intake. The last announces its bound on `v-validation/open-scan` and `v-validation/scan-opened`, but neither carries a count and the path has no completion event.

The three arms that gained `emitIndexSelection` this round announce their bound and then close silently: `cardinalityManyScanAllEntitiesIterator.Close` (`matcher_relations.go:1818`), `vectorScanAllEntitiesIterator.Close` (`:1953`) and `cardinalityManyAVETValueIterator.Close` (`:2148`) emit nothing.

So on every path the round-2 finding named, a key `holds()` dropped remains indistinguishable from one never in range. The counter's own contract at `store.go:162-167` states that this is the failure it exists to prevent.

### T2. Three sibling dispatch arms open a scan and announce nothing, and `emitIndexSelection`'s doc comment says they do

**Status**: Resolved (2026-07-27). All three announce the bound their scan walked, carried back on the resolution result rather than rebuilt, so the announced run cannot drift from the walked one. `matchFromCache` is the deliberate exception and the doc comment now states it: the cache picks an index by cardinality inside resolution and a hit reads no index, so announcing a bound there would name a run the call did not choose. `TestCacheResolvedPatternReportsItsCostAndAnnouncesNoRun` pins both halves — the absent selection and the present funnel.

`matcher_relations.go:2170` reads "Every path that opens a scan for a pattern emits this, including the arms that return before the general one." The arms at `:439`, `:444` and `:449` return before the general emit at `:486` and reach scans at `vector_resolution.go:67`, `set_resolution.go:48` and `set_resolution.go:257`. The third is the inexact one, reachable from query text at `:386` when E, A and V are all bound on a cardinality-many attribute.

Separately `matchFromCache` (`:358`), the default arm for every E-and-A-bound pattern, resolves through `cache_resolver.go:72` and `set_resolution.go:48` and emits nothing — so the most common pattern shape in the engine produces no storage annotation at all.

A live comment asserts the opposite of the new one: `getelse_bound_scan_repro_test.go:348-351` states that `pattern/index-selection` is emitted only by `matchUnboundAsRelation`, and the negative assertion at `:369-374` rests on that premise. Whichever way this is resolved, the two statements cannot both stand.

### T3. `hashJoinIterator.Close` suppresses the whole event when resolution produced nothing

**Status**: Resolved (2026-07-26). The `datomsResolved > 0` guard is gone; only the handler check remains. The comment that stood above it — "ONLY emit if we actually scanned datoms" — described neither the guard nor the field it named, and the case it suppressed is the one the intake counter exists for.

`hash_join_matcher.go:678` guards the emit on `it.datomsResolved > 0` — the count of what CRDT resolution emitted, not what the index was read for. A hash join on the V position with a single string binding key leaves the run variable-width, so every key the range over-covers is counted into `BadgerIterator.scanned` (`badger_store.go:451`) and never reaches the wrapper; `datomsResolved` stays 0 and the intake number vanishes. A fully tombstoned cardinality-many group reaches it the same way. The comment above the guard still says "ONLY emit if we actually scanned datoms," which now describes neither the guard nor the field it names.

`unboundIterator.Close` has no such guard. `nonReusingIterator.Close` guards on `scansOpened > 0`, which is intake-side and does not have the defect.

### T4. `CRDTResolvingIterator.Scanned()` omits a full index scan per entry, and its doc comment asserts it cannot

**Status**: Resolved (2026-07-27). `resolveMaxOtherTxForValue`, `walkApplyEntry`, `walkUniqueEntityValue` and `resolveAVLWW` all return the intake they spend; `CRDTResolvingIterator` accumulates the supersession scans in `uniqueScanned` and adds them to the source's. The doc comment now states the exception rather than denying it.

`crdt_resolving_iterator.go:495-498` says "Resolution reads no index of its own — it consumes what the source produced." In unique mode, set at `:267` for a cardinality-one attribute carrying a unique constraint, `Next` routes each entry through `processUniqueEntry` (`:204`) into `walkApplyEntry`, which calls `resolveMaxOtherTxForValue` (`unique_resolve.go:72`). That opens a fresh `AVET[a,v]` scan at `:144-147`, drains it, and discards the iterator at `:151` without reading `Scanned()`.

Consequence: for a query over a unique cardinality-one attribute, reported intake omits one AVET scan per historical Set entry in each group — the largest hidden read on that path.

### T5. `Iterator.Scanned() int` is a new required method on an exported interface, and the upgrade guide does not record it

**Status**: Resolved (2026-07-26). The *Custom backends* list gains the method, and a paragraph beside the membership obligation states what it must count — intake, **before** narrowing, so a key the membership rule rejected is included — and why it is required rather than optional.

`store.go:170`. The guide's *Custom backends* section (`docs/BREAKING_RELEASE_UPGRADE_v0.15.0.md:88-107`) is the document that tells an external implementer what `storage.Iterator` obliges, and this round extended it with the membership obligation without adding the method. An external implementation gets a compile failure that the one document enumerating v0.15.0's breaks does not explain.

### T6. Every scan-volume assertion in the tree was repointed to the resolution counter at the moment the intake counter was created

**Status**: Resolved (2026-07-26). All four readers now read `datoms.scanned`, each carrying a line on why intake is the question its assertion asks. All four pass at their existing expected values, which is the review's own finding restated: the fixtures write once per entity, so the two counts coincide and the wrong key was invisible.

Four readers changed key this round and all four now read `datoms.resolved`: `aevt_matcher_bug_test.go:120`, `index_order_limit_test.go:28`, `history_index_order_limit_test.go:37`, `index_order_limit_benchmark_test.go:40`. Each drives an assertion about how much the scan *read*:

- `aevt_matcher_bug_test.go:111` names its local `datomsScanned`, `:147-150` errors "EXCESSIVE SCANNING: Scanned %d datoms" against an expected 10 in a 50-datom database, and `:141-143` explains the expectation as "we scan all `:person/age` datoms (10) and probe the hash set."
- `index_order_limit_test.go:75` and `history_index_order_limit_test.go:226`, `:287` assert `scanned <= limit` for "satisfied index order must stop after the requested rows."

Those are intake questions, and the events now carry intake in the adjacent key. They pass only because their fixtures write once per entity; under any fixture with more than one write per entity — exactly what `TestScanReportsIntakeAndResolution` constructs to make the two numbers 3 and 1 — a scan that reads the whole index and resolves to `limit` rows satisfies `scanned <= limit`.

The review verified the correction is safe: switching `aevt_matcher_bug_test.go:120` to `datoms.scanned` leaves it passing at the same value.

### T7. Three intake counters, one assertion between them, and it is on the wrong backend

**Status**: Resolved (2026-07-27). Two tests, on the two axes the finding names.

`TestMemoryBackendReportsIntakeNatively` injects `NewMemoryStore(nil)` through `DatabaseOptions.Store` instead of opening a temp directory, so the memory store's counter is exercised on the native gate rather than only under `GOOS=js`. It writes three times and asserts intake 3 against resolution 1, so a counter returning a constant zero reds natively.

`TestBindingDrivenStrategiesReportTheirFunnel` covers the other two counters and the one `emitIteratorStatistics` owns, driving `matchWithHashJoin`, `matchWithMergeJoin` and `matchWithoutIteratorReuse` directly — which strategy `chooseJoinStrategy` picks is its own business, and reaching them through a query would pin whichever it picks today and silently stop covering the rest. The shared fixture writes each of three entities three times under a cardinality-one attribute, so the funnel is 9 / 3 / 3 and the three terms are distinguishable: a counter wired to resolution's output, or to nothing, reds.

That fixture depth is the finding's real lesson, and it is T6's as well. `TestPerBindingScanReportsOneCountedEvent` now shares it and keeps the claim only it makes — one event for the whole run, never one per binding.

No test calls `Scanned()` to assert its value. The sole assertion is the annotation payload in `TestScanReportsIntakeAndResolution` (`iterator_statistics_test.go:68-73`), which opens with `Path: t.TempDir()` — Badger natively, `NewMemoryStore` under wasm. So `memory_store.go:538` can return a constant 0 and `go test ./datalog/storage` stays green; only `GOOS=js GOARCH=wasm` reds. On wasm the memory store is the only backend, so its intake is what every `-verbose` scan line and `Database.Analyze` reading depends on there. No case in `storeContractCases()` asserts `Scanned()`.

The other two counters have no assertion at all: replacing `matcher_iterator_nonreusing.go:78`'s accumulate-before-discard with a discard, or hardcoding `hash_join_matcher.go:689` to 0, leaves the suite green. `TestPerBindingScanReportsOneCountedEvent` reads `scans.opened`, `binding.size`, `matches.found` and `Latency`, and neither datom count.

This is round 2's N3 with the backends reversed.

### T8. The three `emitIndexSelection` call sites this round added are pinned by nothing, including the one the round-2 finding was about

**Status**: Resolved (2026-07-27). `TestEveryDispatchArmAnnouncesItsRunAndReportsItsFunnel` gives each arm its own row, naming the index that arm addresses, so deleting one emit reds exactly one row. The cardinality-many and cardinality-vector arms the suite never reached with an appropriate attribute are reached now: `funnelSchema` declares one attribute per cardinality and each row picks the one that routes to its arm.

`Latency` on the three binding-driven completion events is asserted by `TestBindingDrivenStrategiesReportTheirFunnel`, which is what keeps them out of `Database.Analyze`'s 0 ms column.

Deleting all three (`matcher_relations.go:1845`, `:1978`, `:2205`) leaves `go test ./datalog/storage` green. The suite reaches all three, so this is absence of assertion rather than absence of execution: `TestIndexSelectionEventReportsBound` exercises the general arm and the v-validation path only, and no subtest uses a cardinality-many or cardinality-vector attribute.

`pattern/hash-join-complete`'s new `Start` and `Latency` are the same: `aevt_matcher_bug_test.go:119` and `count_repro_test.go:56` read that event's `Data` and never its `Latency`.

### T9. Six status claims in the two review documents are contradicted by the tree, two of them created by this round's own remediation

**Status**: Resolved (2026-07-26). All six rewritten to say what is true and to record that they overclaimed, rather than being quietly amended — the convention round 2's Part 4 established for the same failure. N6's missed live-status claim is corrected at its source as well: `PERFORMANCE_STATUS.md`'s *What's Actually Working* entry 2 now describes `chooseJoinStrategy` instead of the removed batch scanner.

| Claim | Site | Contradiction |
|---|---|---|
| C2 | `PR114_TYPED_SCAN_BOUND_2026_07.md:175` | Says the two v-validation leaf events write `bound.v`; D1 changed both to `bound_v` (`matcher_relations.go:698`, `:967`) and the status was not followed. |
| E7 | `PR114_TYPED_SCAN_BOUND_2026_07.md:309-315` | Says the sizing disagreement stands and the fill fraction has to be stated; R17 withdrew exactly that, and `MEMORY_DATOM_INDEXES.md:309` now says the fraction is deliberately not pinned. |
| N2 | `…ROUND2_2026_07.md:78` | "All four scan-opening arms … announce their bound" — contradicted by T1 and T2. |
| N12 | `…ROUND2_2026_07.md:153` | "cited at their current lines" is off by eight: the same round's edits to `hash_join_matcher.go` moved the three `CompareValues` calls to `:740`, `:774`, `:782`, while `BUG_VALUE_DOMAIN_UNENFORCED_IN_COMPARISON.md:115-116` says `:732`, `:766`, `:774`. |
| N14 | `…ROUND2_2026_07.md:163` | "fifteen test functions" — see the correction in Part 4; the count is ten, and the framing does not hold for `backend_contract_test.go` at all. |
| N6 | `…ROUND2_2026_07.md:115` | The live-status category missed `PERFORMANCE_STATUS.md:118-122`, still "Batch Scanning with Iterator Reuse (ACTIVE)" with a threshold and a line reference to `matcher_relations.go:122-128`, which is now the cache-check block. |

### T10. The formatter sweep removed the seven dead arms written as string literals and left nine written as constants

**Status**: Resolved (2026-07-26; label corrected 2026-07-27 — it read "partly" with no remainder named, and there is none). The nine arms are deleted. The tenth, `PhaseComplete`, was resolved the other way: it and `PhaseBegin` now have producers (T19), so both arms are live and `PhaseBegin`'s is restored — written against the data the producer actually sends, not the `pattern.count` the old arm expected from a producer that never existed. `case JoinHash, JoinNested, JoinMerge:` narrowed to `case JoinHash:`.

Under ruling 6, and ruling 7 extending it, **twenty** dead exported constants are deleted: `RelationIndexing`, `RelationIndexed`, `CombineRelsBegin`, `CombineRelsCollapsed`, `PatternsToRelationsBegin`, `PatternsToRelationsRealized`, `PatternFiltering`, `PatternToRelation`, `JoinNested`, `JoinMerge`, `QueryTuplesTransmitted`, `PhaseScore`, `OrClauseBranchBegin`, `OrClauseFallback`, `OrSubqueryInput`, `OrSubqueryResult`, `ErrorQueryParsing`, `ErrorQueryBinding`, `ErrorQueryInternal`, `ErrorBackend`.

**The inverse half is closed with it, and it is the half that explains the whole finding.** Thirty-six event names were emitted as string literals — `pattern/index-selection` had a constant nobody used, `pattern/storage-scan`'s three sibling completion events had no constant at all, and the v-validation, storage-strategy, or-fallback, subquery, algebra-bridge, cache, prefetch, sort and collapse families were literals end to end. Each now has a constant and every producer emits through it, verified in both directions: no `Name:`, `AddTiming(`, `emit(` or `.Emit(` in the module passes a literal, and every constant in `types.go` has a producer.

That symmetry is why the class survived two sweeps. A search keyed on event-name strings could not see `case PatternFiltering:`; a search keyed on constants could not see `Name: "v-validation/entry"`. The two halves hid each other, and the invariant is now stated at the top of the const block so the next sweep does not have to rediscover it.

`CLAUDE.md`'s event-type list is rewritten: it now names `types.go` as the authority, states that a producer never writes a literal, and describes the funnel and the per-strategy completion events instead of the three families that had no producer.

`PhaseBegin` (`annotations/output.go:88`), `CombineRelsBegin` (`:109`), `CombineRelsCollapsed` (`:117`), `RelationIndexing` (`:162`), `RelationIndexed` (`:168`), `PatternsToRelationsBegin` (`:173`), `PatternsToRelationsRealized` (`:178`), `PatternFiltering` (`:287`) and `PatternToRelation` (`:291`) have no producer anywhere in the module — checked as constants and as their string values.

Every deleted arm was `case "pattern/multi-match":`; every survivor is `case PhaseBegin:` and contains no event-name string, so a search keyed on names could not see it. The sweep was keyed on the wrong token.

`CLAUDE.md:643-647` is the documentation face of the same gap, still listing "filtering, and relation conversion" and "Expression Evaluation: Input/output sizes and computation time" as emitted event types after this round deleted the `expression/evaluate` arm.

### T11. The guard-inside-emitter ruling landed inside `datalog/storage` and nowhere else, and at one site it costs real work with annotations off

**Status**: Resolved (2026-07-27). `optimizeAlgebra`'s guard moved to its four call sites on 2026-07-26, so `compiled.String()` and `optimized.String()` are no longer evaluated on every plan with a nil handler.

**The 2026-07-26 status held the three sibling sites on the claim that "all three take arguments that are free to evaluate". Measured, that is false.**

The `RewriteSink` call sites were the largest instance of the defect in the tree. `rewrite_decorrelate.go` rendered `lj.InnerQuery.String()` — the whole inner query — once per LateralJoin visited, `rewrite_getelse.go` rendered its expression per get-else, `algebra_bridge.go` called `terminalSymbols(q)` three times and `Sprintf`'d each, and every payload map built its values with `%v`. All of it unconditional, on the normal query path where nothing consumes any of it.

The renderings are removed rather than guarded: `RewriteRecord.Subject` is `any` and carries the node, payloads carry `CorrelationVars`, `innerParams`, `optimizedWhere`, `scanPattern` and `ge.Attr`, and `ExplainAlgebra` renders. The call-site guards then cover only the payload map itself, inlined at each site — a nil-safe predicate method would be the same defect one layer up — and the nil test appears only where a nil sink is reachable (`DecorrelationPass(nil)`, `rewrite_decorrelate.go:204`), not at `algebra_bridge.go:102` where the sink is a composite literal fourteen lines above.

Measured over the decorrelation pass with `AllocsPerRun`, pinned by `TestSilentSinkBuildsNoProvenance`:

| | silent sink | collecting sink |
|---|---|---|
| before | 252 | 252 |
| after | 215 | 222 |

The stringification was 30 of the 37 and it helps the observing path too; the guard was 7.

The two `executor/join.go` emitters were argument-free, verified by reading rather than asserted — but the rule is that the guard lives at the call site, and reading its rationale as licence to disapply it is not a reading. Both moved. `emitProbeAnnotation`'s third clause was dead: `metrics` is allocated only when a collector exists, at the single construction site, so `it.options.Collector == nil` could not be true when `it.metrics != nil` was. Its once-only latch stays where the state it latches lives.

`planner/algebra_bridge.go:23-27` holds the guard inside the emitter, and its callers at `:41-43` and `:54-56` pass `compiled.String()` and `optimized.String()`, which Go evaluates before the call — so `algebra.formatNode` walks the whole tree whether or not a handler exists. `planner_clause_based.go:128-133` runs this on every plan, and `PlanWithBindings` is not plan-cached, so it is per subquery plan.

`CLAUDE.md:640` states "Zero overhead when disabled: Pass `nil` handler for production deployments."

The same convention sits at `algebra/provenance.go:69-72` and `executor/join.go:156-159`, `:172-181` without the argument cost.

### T12. `TestStoreBackendCompressedVBoundRun` does not pin the classification it was added for

**Status**: Resolved (2026-07-26), and more broadly than the review asked. `TestScanBoundMembershipFollowsThePayloadClassification` covers **all fourteen** value tags, not the six variable-width ones: it enumerates the tag block, fails when it grows, asserts each fixture still lands on the tag it names, and compares `run.Membership.exact` against the classification.

The first draft was a self-consistent oracle and was caught before it landed: comparing against `datalog.PayloadIsFixedWidth(tag)` reads the same function `encodeBoundEndpoint` uses to set exactness, so moving a tag between arms would have moved both sides of the comparison together and passed. The table now states the classification as literal data, a deliberate second copy in the same spirit as `scanBoundComponentOrder`.

This also retires N4's claim that the eight fixed-width entries are unpinnable. That was true of *behaviour* — answering "variable" for a fixed tag computes `size + tail` to exactly the length the exact arm admits — but the membership field itself distinguishes them, and it is now asserted.

`backend_contract_test.go:567-642` covers `TypeCompressedString` and `TypeCompressedBytes`, but its fixtures compress to payloads sharing no byte prefix, so the exact arm's range selects the same single key the length test selects and both arms return the same result. Moving both types to the fixed-width arm of `PayloadIsFixedWidth` leaves the whole tree green.

Reclassifying either would be a silent behaviour change, since the exact arm admits any key whose compressed payload merely *starts with* the bound's. `EncodeScanBound` returns the `EncodedRun`, so `require.False(run.Membership.exact)` on each of the six variable-width tags pins the classification directly, without needing a prefix-related fixture.

### T13. Two doc comments this commit added now head the wrong function

**Status**: Resolved (2026-07-26). Placement only — each comment now heads the function it describes. Their *content* is T2's and T4's business: `emitIndexSelection`'s still claims a completeness the tree does not have, and `Scanned`'s still says resolution reads no index of its own.

`crdt_resolving_iterator.go:494` is `ElementID`'s comment, immediately followed by the new `Scanned` comment and `Scanned` itself at `:499`, leaving `ElementID` at `:501` undocumented.

`matcher_relations.go:2157-2161` is `matchCardinalityManyFindEntitiesWithValue`'s comment, including the AVET `[A][V]` O(k) rationale, now heading `emitIndexSelection` at `:2178`, with the function it describes starting undocumented at `:2188`.

`go doc` renders both wrongly.

### T14. The funnel's third term has two spellings across the three completion events

**Status**: Resolved (2026-07-26). `datoms.matched` at all sites, per ruling 2. `matches.found` is retired.

`datoms.matched` at `iterator_validation.go:103` against `matches.found` at `hash_join_matcher.go:691` and `matcher_iterator_nonreusing.go:146`. All three increment after the same pattern check plus `validateDatomWithConstraints`, so no single key reads the whole funnel across the three events. This is the shape D1 closed for `bound.v` against `bound_v`, reintroduced in the same commit.

### T15. Two completion events name no index and no bound, and no formatter arm renders either

**Status**: Resolved (2026-07-27), the index question by argument rather than by adding the field.

Four completion events gained formatter arms: `pattern/hash-join-complete` and `pattern/merge-join-complete` share one (they differ only in the strategy name), `pattern/per-binding-scan-complete` and `pattern/cache-resolve-complete` have their own. All four render the funnel through `renderScanFunnel`, which prints all three counts unconditionally — on these paths the gap between what was read and what came out is the reason the event exists, so it is not the conditional suffix the unbound scan line carries.

`pattern/per-binding-scan-complete` still names no index, and that is the answer rather than the omission. `chooseIndex` runs once per binding and can pick a different index each time, so a single `index` field would name whichever binding happened to be last. What that path owes its reader instead is `scans.opened`, which it carries. Same reasoning for `pattern/cache-resolve-complete`, whose arm says so: the cache picks an index by cardinality inside resolution, and a hit reads none.

The adjacent unchecked `.(int)` is fixed. `pattern/storage-scan` is the one event with seven producers, and the formatter now reads both `pattern` and `datoms.resolved` comma-ok, on the same grounds `renderScanFunnel` already stated: a producer that omits a key should cost the reader one wrong line, not panic the formatter mid-trace.

`matcher_iterator_nonreusing.go:140-147` writes `pattern`, `binding.size`, `scans.opened`, `datoms.scanned`, `datoms.resolved`, `matches.found`. The bound is recomputed per binding by `chooseIndex` at `:99` and can be `AEVT[a,e,v]`, `AVET[a,v]` or `VAET[v]` with V of any type, so the run is often inexact and the index varies per binding. Every other scan event in the package carries `index`.

`OutputFormatter.Format` has no case for `pattern/per-binding-scan-complete`, and none for `pattern/hash-join-complete` either — see the correction in Part 4. Both render as raw map dumps through `output.go:295-297`.

Adjacent, in the arm that does exist: `output.go:238` reads `datoms.resolved` with an unchecked `.(int)`, so any future emitter of `pattern/storage-scan` without that key panics at render rather than rendering wrong.

### T16. `matchWithMergeJoin` emits no completion event

**Status**: Resolved (2026-07-26). `mergeJoinIterator` carries `datomsResolved`, `datomsMatched` and `scanStart`, and emits `pattern/merge-join-complete` on `Close` with the full funnel, its index, its binding size and its duration — the same payload as its hash-join sibling, so the two strategies are comparable.

`hash_join_matcher.go:554` opens the bound and `mergeJoinIterator.Close` at `:811-816` closes and returns, while its `hashJoinIterator` sibling emits at `:675`. Merge join is selected at `:78-80` for binding sets above 1000, so the strategy chosen for the largest scans is the one reporting nothing.

### T17. Two tests are off the package's optimizer-mode axis

**Status**: Resolved (2026-07-26). `TestQuerySnapshotConsistency` loops the axis and passes on both legs. `TestPerBindingScanReportsOneCountedEvent` pins `algebra_off` explicitly, with the reason in its doc comment, which is what the convention prescribes for a test that drives the matcher below the planner.

`TestQuerySnapshotConsistency` (`read_session_test.go:226`) calls `d.Query` at `:267` against `NewDatabase(t.TempDir())` — default planner options, one mode, no loop and no explicit `EnableAlgebraOptimizer`. `optimizer_modes_test.go:13-22` requires one or the other.

`TestPerBindingScanReportsOneCountedEvent` (`scan_bound_reporting_test.go:26`) is a weaker instance — see the correction in Part 4.

### T18. Sweep survivors

**Status**: Partly resolved (2026-07-26). The live source and tests are corrected, and so is `PERFORMANCE_STATUS.md`'s live-status entry (recorded under T9). The five documents below are **held**: four of them are the dated bug reports and plans edited last round under a rationale the owner rejected, and re-editing them is the same decision, not a sweep item.

One is reclassified rather than held. `PERFORMANCE_STATUS.md:2286`'s `reusingIterator` bullet sits inside the *Storage Iterator Consolidation (REJECTED)* record, whose opening line was already annotated on 2026-07-26: "Superseded 2026-07-26 — `matcher_iterator_reusing.go` was deleted in v0.15.0, so the consolidation question is moot for the two that remain." The bullet is part of the analysis that annotation governs. Editing its body is what the convention forbids.

Live documents:

- `ITERATOR_LIFECYCLE_MANAGEMENT.md:429-431` still tables `storage/simple_batch_scanner.go:94` and two `executor/helpers.go` lines in a file that does not exist. `:415` gained a "re-derive against the current tree" caveat pointing at those rows; the sibling documents had their equivalent rows struck.
- `BUG_ELEMENTID_NOT_FIRST_CLASS.md:133-175` had its summary rows struck but not the two full instruction sections they point into, and `:22`, `:101` still direct work at `chooseIndexForValues`.
- `CRDT_E_BOUND_A_UNBOUND_INVESTIGATION.md:96` lists `matchWithIteratorReuse` as a live destination and `:118-125` quotes the deleted file's body.
- `TRIAGE_CARDINALITY_VECTOR_COVERAGE.md:46` names `NewBadgerMatcher` under a "Test needed" instruction.

Live source and tests — **all corrected**:

- `batch_scan_trace_test.go` no longer narrates a 100-binding batch-scanning threshold. What the test exercises is constraint pushdown into a binding-driven scan, which at 200 bindings is `HashJoinScan`; the comments now say that.
- `batch_scan_benchmark_test.go` describes what the benchmark measures — binding-driven scan cost across a size series that spans `chooseJoinStrategy`'s 1000-binding boundary — and records that its name predates v0.15.0 and is kept because archived benchmark records cite it.
- `iterator_validation.go:17` now says the consolidation covered the three types that existed then and names the deletion that left two, rather than silently disagreeing with `:25`.

Note on scope: many further hits for these symbols are in `docs/bugs/resolved/` and `docs/archive/`, which the convention annotates rather than rewrites — they are period records, not standing instructions, and are excluded here.

### T19. `Database.Analyze` reported three event names nothing emitted, and the phase lifecycle spoke a second vocabulary

**Status**: Resolved (2026-07-26). Found while checking T10's nine against their producers, not in the received review.

**My first statement of this finding was imprecise and is corrected here.** I wrote that phase execution had no producer. `phase/complete` had none — but the phase loop was instrumented, under `realized/phase-begin`, `realized/phase-output` and `realized/phase-materialized`, three events marked DEBUG that no test read and that the formatter and `Database.Analyze` did not know. The defect was two vocabularies for one lifecycle, which is the shape D1 closed for `bound.v` and T14 closed for `matches.found` — not an absence of instrumentation.

Unified onto the vocabulary the consumers already spoke. `annotations.PhaseBegin` carries the phase number, input group count, `Keep` and the query fragment. `annotations.PhaseComplete` replaces `realized/phase-output` and absorbs `realized/phase-materialized` by moving to **after** boundary materialization, so its duration covers the phase's whole work and its size is free to ask for on every phase that materialized. `Database.Analyze`'s phase line now renders; `PhaseBegin`'s formatter arm is restored, written against the data the producer actually sends rather than the `pattern.count` the deleted arm expected from a producer that never existed.

`tuple.count` is `declaredTupleCount`, which sums `Size()` and returns **-1 if any group declines**. Reporting a phase's size must not be the call that spends the phase's tuples, so a streaming final phase declines rather than being consumed for the number, and the formatter renders that as "an unsized stream" instead of printing minus one.

`JoinNested` and `JoinMerge` were not missing producers — the executor performs hash joins only, so they named operations it does not have. `Analyze`'s `Joins: hash=%d, nested=%d, merge=%d` line always read zero for two of three; it now reports hash joins alone, and gains a binding-driven scan section counting the three storage completion events by strategy. Merge join's absence from that report was the absence of the largest scans in the engine.

---

## Part 4 — corrections to the received round-3 review

### `TestPerBindingScanReportsOneCountedEvent` is not off the axis the way the other one is

The review pairs it with `TestQuerySnapshotConsistency` as "query-executing tests off the axis." It never calls `Query`: it drives `matchWithoutIteratorReuse` directly and says so in its own comment at `scan_bound_reporting_test.go:23-25`, so the planner — and therefore the algebra optimizer — is not on its path. That is the identical shape as `TestVBoundCardinalityOneBytes_DirectMatcher`, the deliberate exception N14 already names. The convention would have it pin a mode explicitly rather than loop the axis, and it pins none. Real, and much smaller. Recorded under T17.

### The formatter finding undercounts, and the count in N14 is wrong in a different way than reported

`pattern/hash-join-complete` has no formatter arm either, and it is the primary binding-driven scan event — both it and `pattern/per-binding-scan-complete` fall to the raw map dump. Recorded under T15.

On N14's count: the review makes it twelve by `grep -c "range optimizerModes"`. Counting the five files the round-2 remediation touched, there are **ten** loops of the axis — `vbound_bytes_validation_test.go` 6, `scan_bound_end_test.go` 2, `scan_bound_test.go` 1, `backend_contract_test.go` 1 — plus the one explicit single-mode pin. `matcher_planner_options_test.go` contributes zero to that axis; it regained a *numeric* axis, which is a different thing. And most of `backend_contract_test.go`'s eight test functions execute no query at all, so "all five files are on the optimizer-mode axis" does not describe that file whatever the count. Recorded under T9.

---

## Disposition

**Closed**: T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T19.

**Partly closed**: T18 (live source, tests and the live status claim corrected; five documents held per ruling 4).

**Open**: none.

The five that were open at the last update — T1, T2, T7, T8, T15 — were one arc: every dispatch arm announces the run it walks and reports the funnel that run cost, each arm's pair of emits is pinned by a row that reds alone, and every completion event has a formatter arm. Ruling 10 settled the one semantic question the arc raised, on what `datoms.scanned` means when the read came from cache.

### The payload vocabulary (ruling 7's other half)

Ruling 7 declared one vocabulary for event *names*. The keys inside the payload were left as literals, and the four hand-built event maps were the visible consequence: `hashJoinIterator.Close`, `mergeJoinIterator.Close`, `nonReusingIterator.Close` and `matchFromCache` each spelled the funnel keys themselves rather than going through the emitter the seven `pattern/storage-scan` producers use.

They did not go through it because it required an index, and two of the four address no single run — cache resolution picks one by cardinality inside resolution and reads none at all on a hit, and the per-binding path runs `chooseIndex` once per binding tuple. **The index is not a property of the event family**, so it is not a parameter: `emitScanCompletion` owns the pattern, the envelope and the funnel, and a producer that walked one run names it in the extras under `annotations.KeyIndex`. All eleven now emit through it.

The funnel travels as `scanFunnel{scanned, resolved, matched}` rather than three positional ints — three ints in a fixed order is the shape where a transposed pair compiles and then reports a scan that returned more than it read.

The keys shared across producers are declared in `annotations/types.go` beside the event names, for the reason stated there: eleven producers and one consumer cannot spell a key at the producer and have the formatter find it. Keys with a single producer stay literals, and the const block says so.

Values travel typed. `IndexType` and `*query.DataPattern` reached the formatter as `.String()` renderings from thirteen producers; both now go in as values and the formatter renders, since it is the renderer and a producer that flattens spends an allocation per emit to hand its consumer something to parse. Neither type is nameable in `annotations` — both live in packages that import it — so the formatter reaches them through `fmt`, which reports a panicking `String` method inline rather than taking the trace down. The three tests that read the index through `.(string)` with comma-ok were converted: each would have gone silently false and asserted nothing.

**A collision found by the sweep, and fixed**: `subquery/input-relation` wrote its relation's ordinal under `"index"`, the key the scan events use for the physical ordering they walked. `Database.Analyze` prints `Data[KeyIndex]` for every event it traces, so a subquery rendered `(index=0)` as though it named a run. It is `"relation.position"` now — in this engine an index is one of the eight orderings — and `TestIndexAnnotationKeyCarriesOnlyAnIndexType` pins that nothing else carries a second meaning there, requiring both event families in the trace so it cannot pass by exercising neither.

### `unique/lookup-complete` was outside the family it belongs to

It reported two of the three funnel terms and built its map by hand. It has a pattern despite the Go signature not spelling one: `LookupByUnique` resolves `[?e attr value]`, the A-bound, V-bound, E-unbound shape, and `tryEmitUniqueWinner` — the other caller of the same `resolveAVLWW` — arrives there from that pattern written out, folding its intake into the enclosing event per ruling 1.

It emits through `emitScanCompletion` now, with that pattern, and `resolveAVLWW` returns a `scanFunnel` rather than a bare count. The third term is load-bearing rather than symmetry: `resolved=1, matched=0` is an AVET entry whose claimant has since replaced the value — the store is append-only, so a superseded value keeps its index entry forever, and rejecting it costs an AVET scan plus a claimant walk. Under the two-term form that read exactly like a value nobody ever wrote, which costs nothing. `TestUniqueLookupReportsItsFunnel` pins all three rows.

Moving it into the family left it the only member without a formatter arm, rendering as a raw payload dump — T15's defect on a sixth event. It has one now, and the two halves meet: the storage test runs the formatter over the events it captured, because separate pins can agree on a payload shape neither side produces.

### `Seek` took two thirds of the run it was given

Found while answering the ledger's open question on whether an ordered-range predicate belongs alongside the equality bound. It does not — the two backends do not agree on value order, so "between" would name different sets (see item 29 for the derivation). But the question turned up a live defect in the seam it was asked about.

`EncodeScanBound` returns `EncodedRun{Start, End, Membership}`. `Seek` took `Start` and `Membership` and discarded `End`, under a comment stating the choice: "the range end stays the scan's — a seek moves within a scan, it does not open one." A caller that seeks a narrower run inside an open scan therefore had no way to say where its run stopped.

`pull_batch.go` is that caller, and it did what the shape forces: `key[1:21]` against the entity hash — the index prefix byte and the 20-byte E, sliced out of an encoded key, above the seam whose whole purpose is that no caller holds a key layout. It is correct only while the index prefix is one byte and E is 20 bytes, and neither fact is stated anywhere near it.

A run is its start, its end and its membership rule together; adopting a subset yields a run nobody asked for. `Seek` now adopts all three on both backends. `memoryIterator` gains the `end` its construction never needed — it filters `keys` to the scan's range instead — and its `Next` stops there rather than treating past-the-end as a key to step over, which would have walked the remainder of the scan looking for a member that cannot be there and counted every key of it as intake.

`pull_batch.go`'s key arithmetic is deleted, along with the second entity check it made after decoding.

`TestSeekHonoursTheRunItNames` pins the shape both the caller and the shared-scan optimisation depend on, across both backends: open wide, seek, get that run and nothing else, seek again. The second seek is not decoration — an iterator that stopped by exhausting itself would pass the first assertion and fail every caller that reuses it.

### `pull/attr.lookup` reported `found: false` on every event

`PullContext.AttributeLookup` took `found bool` as a parameter and the closure that performs the lookup as another. Go evaluates arguments before the call, so the parameter carried the zero value of a variable the closure had not yet assigned — `pull.go:213` and `:223` both declared `var found bool` and passed it straight in. `AnnotatedPullContext.AttributeLookup` then recorded its own parameter. Every attribute lookup in every pull trace reported the attribute as missing, including the ones that returned a value.

Nothing caught it because nothing read the field: the pull annotation test asserted `spec_count` and `success` and never opened an attribute-lookup event.

The outcome now returns through the closure — `fn func() bool`, the shape `AllAttributes(entity, fn func() int) int` three lines above it in the same interface already used — which also deletes the `var found bool` the two call sites were shadowing. `TestPullAttributeLookupReportsWhetherItFound` pins both rows: an attribute that exists and one that does not, because a field hard-wired to either constant passes a one-row test.

The rest of `pull_context.go` carried the same defect the payload vocabulary work removed elsewhere — thirteen `entity.String()` / `attr.String()` renderings at the producer. They are values now, and the test keys its results by the interned `Keyword` rather than a rendering of it, so a producer that goes back to flattening fails rather than passing on a string that reads the same.

### Work in this pass that the review did not raise

Two findings came out of the owner's questions rather than the report, and both were larger than anything in it.

**`rebuild*` swallowed the resolver's error** (ruling 8). Asked whether a `CacheEntry` can be nil, the answer was yes — and nil meant exactly one thing, that resolution had failed. Fixed through `rebuild`, `rebuildOne/Many/Vector`, `ResolveEntry` and `GetOrResolve`, with every caller propagating. Two callers had no error to return and gained one: `Transaction.vectorContainsValue`, where answering `false` on a failed read inserts a duplicate into a set the schema declares unique, and `matchFromCache`, where the old fallback re-ran the read that had just failed.

The verification pass then found one more, in production: `Database.WarmCache` called `GetOrResolve` as a **bare statement** binding nothing, so widening the signature with an error left the discard invisible — no compile error, no vet warning, and no `_` for a search to find. **The dangerous discard is the call with no assignment, not `_`.** Every function widened with an error in this pass had its call sites listed and read; `WarmCache` was the only one.

**The annotation handler was a getter/setter pair guarding a mutex** (ruling 9). Both halves were test-only surface on a public type, the mutex was skipped by a production reader anyway (`convenience.go`), and the whole structure existed because the handler was copied into `Cache` and every `Matcher` — an assignment cannot fan out to duplicates. `Database.AnnotationHandler` is now a field; `Cache` receives the handler per call and holds none; `Synchronized` is the caller's to apply. Recorded in the upgrade guide as two API removals and a changed promise.

Verified by reading `400f0e1` rather than accepted from the report: all eighteen received findings. Two of the report's claims are corrected in Part 4; both narrow a finding rather than retract it.
