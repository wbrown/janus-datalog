# PERFORMANCE_STATUS.md

**Last Updated**: 2026-07-21 (string-identity hot paths, add-wins set resolution, single-allocation key encoding, direct point-lookup resolution, single-scan wildcard resolution, database-shared tuple-builder cache, specialized EA-cache trie, positional tuple keys with split-overflow key map, span-grouped or-fallback branch cache, or-fallback direct probe emit, grouped hash-join build rows, per-query read sessions, session-bounded EA-cache reads)
**Version**: Clause-based planner, QueryExecutor, ready-predicate scheduling, streaming architecture, Pull API, schema support, key encoder optimization, conditional aggregate rewriting (folded into algebra optimizer), CRDT storage, allocation regression fixes, value elimination, LZ77+FSE compression codec with Tier-3 blob store, ATEV index, iterator-error contract, relation-input parallel iteration refactor (worker pool + workspace reuse), hash-join hot-path inner-loop optimizations including unique-key build specialization, compiled storage hash matching, one-pass same-entity attribute bundles, typed aggregation keys, single-lookup dedup insertion, bounded Top-N finalization, typed Relation property propagation including keyed join dedup elision, natural/semi/anti joins, and OR/fallback/union derivation, existing-order scan termination, ATEV/TAEV/AETV/EATV order-aware history matching, branch-wide randomized correctness hardening, the value-semantics correctness campaign (one canonical order and equality, interned-symbol operation dispatch, readiness-ordered algebra compile, dual-mode optimizer test matrix), and string-identity hot paths (structural tuple-builder cache keys, pointer-identity comparisons, span-based add-wins set resolution, single-allocation key encoding).

## Executive Summary

The Janus Datalog engine delivers production-ready performance through architectural improvements and targeted optimizations. All performance claims in this document are verified by actual benchmarks (most recent entry: 2026-07-13, same-entity constant constraint fusion).

### Verified Performance Improvements
- ✅ **New architecture** (clause-based planner + QueryExecutor): **2× faster** on complex OHLC queries (verified)
- ✅ **Pull API**: **9× faster than equivalent queries**, linear scaling (verified 2025-12-17)
- ✅ **Schema validation**: **<1% overhead** for type checking, **~6% overhead** for uniqueness (verified 2025-12-17)
- ✅ **Iterator composition**: **4.06× speedup** (1,259μs → 310μs, 89% memory reduction) (verified 2025-10-25)
- ✅ **Streaming execution**: **2.22× faster** with low-selectivity filters (1,720ms → 774ms), 52% memory reduction (verified 2025-10-25)
- ✅ **Parallel subquery execution**: **2.06× speedup** with 8 workers on M3 Max (730ms → 355ms) (verified 2025-10-25)
- ✅ **Predicate pushdown**: **1.58-2.78× faster** depending on dataset size, up to 91.5% memory reduction (verified 2025-10-25)
- ✅ **Intern cache optimization**: 6.26× speedup on BadgerDB queries (verified)
- ✅ **Time range optimization**: 4× speedup on large datasets (verified - 1.5× on small, 4× on 260-hour dataset)
- ✅ **Hash join pre-sizing**: 24-32% faster with 24-30% less memory (verified)
- ✅ **Identity/Keyword interning**: **10-20% faster** on joins and subqueries, **25-44% memory reduction**, pointer equality for all comparisons (verified 2025-12-24)
- ✅ **Conditional aggregate rewriting**: **7.7× faster**, **5.2× less memory**, **8.1× fewer allocations** for correlated aggregate subqueries (verified 2026-01-16)
- ✅ **CRDT storage**: **~25-35µs writes** across all cardinalities, **O(1) LWW resolution** (965ns for 1000 versions), linear vector scaling (verified 2026-01-31)
- ✅ **CRDT allocation optimization**: **90% faster** (1.9×), **2.2× less memory** than pre-CRDT main branch while adding full CRDT semantics (verified 2026-02-02)
- ✅ **AETV index & value elimination**: **5% faster**, **19% less memory**, **17% fewer allocations** (geomean); complex queries see **35% memory reduction** (verified 2026-02-06)
- ✅ **LZ77+FSE compression codec**: **2.1-2.4 GB/s decompression** (7 allocs), **3.6x on prose**, **10-13x on structured/repetitive** data (verified 2026-03-28)
- ✅ **ATEV index & attribute high-water mark**: `MaxElementIDForAttribute` and every `Cache.IsAttributeFresh` call become **O(1) (~1 µs)** instead of O(datoms-for-A); **2.2× → 555× faster** at 10–10,000 datoms-per-attribute (verified 2026-05-25). Costs ~14% more write work per commit (1 of 8 indices).
- ✅ **Relation-input parallel iteration**: worker pool + workspace reuse for `:in $ [[?x ?y] ...]`-shape queries. **10–25% wall-time improvement** uniformly across worker counts that fit in P-cores; **1.4% fewer allocations** per query. Eliminates per-tuple goroutine spawn (`len(tuples)` goroutines → `numWorkers`), per-call `QueryExecutor`/`modifiedQuery` rebuild, and per-call `BindQueryInputs` machinery. Fixes an iterator-workspace-reuse race on streaming inputs (verified 2026-05-26).
- ✅ **Hash-join hot-path optimizations**: **~25% faster Identity-keyed joins** (entity references — the dominant real-world shape), **~14% faster int64-keyed**, **~4.4% fewer allocations** (n=10 geomean) from six targeted inner-loop findings. Biggest wins: pointer-hashing interned Identity/Keyword instead of their SHA1 content (−12.7% Identity) and hoisting the `combineTuples` projection plan out of the inner loop (−8.8%) (verified 2026-05-29, M3 Ultra).
- ✅ **Same-entity attribute-fetch fusion**: a `[?e :const-attr ?fresh]` fetch on an already-bound `?e` executes as a per-tuple `LookupAttribute` binding attach instead of a separate match + hash join. **1.40–1.94× faster** (scaling with attributes-per-entity), **~2.6–3× fewer allocations**; reaches and at K≤3 beats the no-join Pull floor (flat tuples vs Pull's nested maps). Both paths use the EA cache for the per-`(E,A)` lookup — fusion removes the join around it. CardinalityOne latest-state queries only; history, as-of, and CardinalityMany stay on the ordinary path. On by default (verified 2026-05-29, M5).
- ✅ **Same-entity constant constraint fusion**: `[?e :const-attr literal]` on an already-bound entity now uses `LookupAttribute` plus typed equality instead of storage match + hash join. At 1K/10K entities it is **21.9–23.2% faster**, uses **35.3–38.8% less memory**, and performs **42.3–43.4% fewer allocations**. The production-shaped complex checkpoint improves **11.1% time, 21.8% memory, and 23.2% allocations** (n=10; verified 2026-07-13, darwin/arm64).
- ✅ **Correlated OR outer replacement**: OR/fallback relations already contain their selected outer tuples, so QueryExecutor replaces consumed relation groups instead of appending the result and joining it back to the same outer data. Removing five redundant joins improves the complex checkpoint **11.3% time, 8.3% memory, and 10.6% allocations**. Outer, branch-cache, and close errors now propagate rather than falling through to defaults (n=10; verified 2026-07-13, darwin/arm64).
- ✅ **Correlated-subquery product streaming**: replayable source relations feed their single-use product directly into typed input-combination deduplication instead of materializing and deduplicating the complete product first. Valid 10K/100K set products improve **39.6–44.2% time**, **37.0–38.7% memory**, and **14.3% allocations**. The complex checkpoint does not exercise this multi-group shape and remains unchanged (n=10; verified 2026-07-13, darwin/arm64).
- ✅ **Relation set-invariant construction**: operators that already prove duplicate-free output now construct Relations without repeating typed deduplication. Grouped aggregation publishes its group key; union/fallback realization, join-key extraction, selection, deterministic extension, sorting, limits, phase realization, and lazy replay preserve the set proof. The complex checkpoint improves **5.1% time, 15.6% memory, and 8.6% allocations** (n=10; verified 2026-07-13, darwin/arm64).
- ✅ **Batch wildcard Pull resolution**: `(pull ?e [*])` finalization collects matched entities and resolves their non-unique EATV ranges through one Badger read transaction and iterator instead of opening/discarding one transaction per entity. Focused 230/3,899-entity runs are **14.4×/10.8× faster**, use **90.9% less memory**, and perform **89.7% fewer allocations**. Latest and AsOf preserve CRDT semantics; unique-ownership walks and Tier-3 blob dereferences may open additional reads, and History retains its raw-mode path (10 iterations; verified 2026-07-13, darwin/arm64).
- ✅ **Iterator contract hardening**: early-close materialization reports an incomplete-cache error, predicate evaluation failures propagate, streaming transforms use relation-owned iterators, hash joins retain build/probe close errors, unknown-size inputs bind correctly, and `StreamingRelation.Get` performs real random-access realization. Correctness-only; no performance claim.
- ✅ **Typed aggregation keys**: batch and streaming grouped aggregation now key groups with `TupleKeyMap` instead of delimiter-joined formatted strings. This fixes silent collisions between distinct values and makes grouped aggregation **47.5% faster**, with **25.8% less memory** and **71.3% fewer allocations** (n=10 geomean; verified 2026-07-11, darwin/arm64).
- ✅ **Single-lookup dedup insertion**: eight set-insertion paths now use `TupleKeyMap.PutIfAbsent` instead of `Exists` followed by `Put`. Materialized and streaming deduplication improve **5.4–9.0%** (**7.3% geomean**) with unchanged memory and allocations (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **Bounded Top-N finalization**: ordered limits without non-projected sort keys use an O(N)-memory heap instead of materializing and sorting every row. Across 10K/100K rows and N=1/10/100: **97.1% faster**, **99.96% less memory**, **99.86% fewer allocations** (n=10 geomean; verified 2026-07-11, darwin/arm64). The source is still fully scanned; index-order pushdown remains separate.
- ✅ **One-pass same-entity attribute bundles**: contiguous cardinality-one fetches sharing a bound entity now attach all output bindings in one traversal and one materialization. **13.5% faster, 32.9% less memory, 19.5% fewer allocations** geomean; K=6 at 1,000 entities is **24.8% faster, 56.5% less memory, 36.5% fewer allocations** (n=10; verified 2026-07-11, darwin/arm64).
- ⚠️ **Typed Relation properties (foundation)**: `Relation.Properties()` carries ordering and candidate-key guarantees using Datalog symbols. Initial conservative propagation plus key-aware streaming projection reduces the complex-query checkpoint by **5.45% memory** and **2.71% allocations** with statistically unchanged time. Join/OR derivations and broader storage coverage remain (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **Existing-order scan termination (6a)**: when a relation already satisfies Datalog `:order-by`, ordered limits stream directly through `LimitRelation`. On 10K AETV-ordered entities, N=1/10/100 scans exactly N rows and is **98.1–99.7% faster**, with **97.0–99.4% less memory** and **98.0–99.6% fewer allocations**. Order-aware index selection (6b) remains separate (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **History ATEV order-aware matching (6b first shape)**: `PatternMatcher.Match` now receives a one-pattern Datalog query fragment. Safe history queries with constant A and `Tx desc, E asc` select ATEV and scan exactly N raw datoms: **99.15% faster, 99.07% less memory, 98.89% fewer allocations** geomean. Latest/as-of explicitly decline Tx-primary ATEV (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **History TAEV order-aware matching (6b second shape)**: unfiltered history transaction-log queries ordered by `Tx desc, A asc, E asc` select TAEV and scan exactly N raw datoms. Across N=1/10/100: **99.16% faster, 99.05% less memory, 98.86% fewer allocations** geomean; scans fall from 10,100 to exactly N. Latest/as-of and filtered patterns explicitly decline (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **History AETV order-aware matching (6b third shape)**: constant-attribute raw history ordered by `E asc, Tx desc` consumes AETV directly and scans exactly N datoms. Across N=1/10/100: **99.15% faster, 99.07% less memory, 98.89% fewer allocations** geomean; scans fall from 10,000 to exactly N. Latest/as-of keep their existing CRDT-resolved path without the raw-history property (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **History EATV order-aware matching (6b fourth shape)**: constant-entity raw history ordered by `A asc, Tx desc` consumes EATV directly and scans exactly N datoms. Across N=1/10/100: **99.13% faster, 99.00% less memory, 98.86% fewer allocations** geomean; scans fall from 10,000 to exactly N. Latest/as-of keep their existing CRDT-resolved path without the raw-history property (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **Key-preserving join properties**: natural joins preserve one side's candidate keys when the opposite join symbols contain a candidate key, allowing retained-key streaming projections to omit redundant deduplication. The focused 10K/100K join-projection path is **24.42% faster, uses 25.15% less memory, and performs 9.13% fewer allocations** geomean. The default complex-query checkpoint is statistically unchanged (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **Semi/anti join properties**: semi-joins and anti-joins preserve all left properties and skip redundant result deduplication when a left candidate key proves uniqueness. Focused 10K/100K filters are **27.54% faster, use 32.80% less memory, and perform 20.02% fewer allocations** geomean. The default complex-query checkpoint remains statistically unchanged (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **Keyed hash-join dedup elision**: when a derived result candidate key proves uniqueness, streaming, materialized, and symmetric hash joins omit their internal full-tuple `seen` table. Against the already key-propagating focused baseline this is **32.77% faster, uses 32.77% less memory, and performs 10.05% fewer allocations** geomean. The default complex-query checkpoint remains statistically unchanged because its dominant OR/fallback-fed joins do not carry candidate keys (n=10; verified 2026-07-11, darwin/arm64).
- ✅ **Compiled storage hash matching**: cache-disabled hash-join scans now use typed `TupleKeyMap` probes and precompiled pattern/binding slots instead of per-probe strings and per-candidate symbol maps. The 10K-datom focused path is **7.16% faster, uses 15.65% less memory, and performs 32.83% fewer allocations** (`n=10`; verified 2026-07-11, darwin/arm64).
- ✅ **Ready-predicate scheduling**: once all required symbols are available, the planner places a predicate before unrelated remaining scans in the same emitted `RealizedPlan`. A 10K-entity selective filter before payload retrieval is **62.85% faster, uses 87.12% less memory, and performs 78.88% fewer allocations**. The complex checkpoint is statistically unchanged (`n=10`; verified 2026-07-12, darwin/arm64).
- ✅ **Unique-key hash-build specialization**: when build-side join symbols contain a candidate key, hash joins store one tuple directly instead of allocating a one-element fanout slice. Against the already keyed and dedup-eliding focused baseline this is **5.51% faster, uses 7.25% less memory, and performs 11.11% fewer allocations** geomean. The complex checkpoint remains unchanged because its dominant fallback/subquery build sides lack candidate-key proofs (`n=10`; verified 2026-07-12, darwin/arm64).
- ✅ **OR/fallback/union properties**: singleton `or-default` branches preserve unaffected outer keys, multi-row fallback and correlated union derive conservative composite keys under typed set deduplication, and ordinary unions expose only their full output key absent a branch-disjointness proof. The focused 10K/100K fallback-projection path is **15.09% faster, uses 8.62% less memory, and performs 3.59% fewer allocations** geomean (median of `n=10`; verified 2026-07-12, darwin/arm64). A 500-case randomized differential matrix validates results, candidate keys, and ordering across singleton, pattern, and expanding-expression branches. The complex checkpoint drops to **1.043M allocations/op**; separate runs do not establish a wall-time gain.
- ✅ **Optimization correctness hardening**: independent randomized/property gates now cover value equality/hash consistency (16,000 cases), natural joins and projections against an O(n²) oracle (3,200), OR/union against a direct branch interpreter (4,000), Top-N against a native typed comparator (4,000), all 720 predicate clause permutations, and all four history ordered-limit shapes against full-sort references. Package-wide and focused race gates, repeated shuffled-order execution, typed scan fingerprints, and LazySeq error/close replay are also pinned. The pass fixed signed-zero hash mismatches, expanding-expression key claims, workspace aliasing in semi/anti joins, dropped iterator errors, unsafe AsOf fusion, swallowed lookup errors, scan-sharing physical-fragment collisions, and malformed compiled bindings. These are correctness gates; no new performance claim is attached.
- ✅ **WASM memory backend / injectable Store**: `Database` holds `storage.Store`; native Badger and pure-Go `MemoryStore` share workspace-decoded `Scan`/`ScanKeysOnly`, sticky iterator errors, and Tier-3 blob reads on the active scan session. Public entry points: `db.OpenMemory`, `db.WithStore`. Browser durability is host-managed Export/Import. Hard-gate checkpoint (Apple M5, `n=5`, 2026-07-15): `BenchmarkStorageHashJoinCompiledMatching` ≈ **1.95 ms / 342 KB / 10,293 allocs**; batch wildcard Pull retains its prior gain (~**8–9×** vs per-entity at 230/3,899 entities). Artifacts under `docs/perf/wasm_memory_backend_2026-07-15/`.
- ✅ **Correctness campaign A/B (PR #112)**: the 27-commit value-semantics/optimizer-transparency campaign measured against its own merge-base (`9bd60a4`), same-session back-to-back so both sides carry the May–June perf findings identically and thermal artifacts cancel. Executor hash-join suites (int64 + Identity, 39 sub-benchmarks, n=10): **sec/op geomean −3.56% with B/op and allocs/op identical** — every significant delta an improvement, largest on mat×mat (−5.4 to −8.5%), Identity high-fanout (−5.8 to −7.4%), and 50K-row results (−4.6 to −6.2%). Storage suites (OHLC, time-range, key scan/decode, resolve, complex checkpoint, n=5): **sec/op geomean −2.57%**, OHLC family −4.3 to −8.6%, time-range scan −6.4%. Costs, stated: `ResolveAllAttributesMany` per-entity arms **+1.63% allocs / +3.6–4.1% B/op** (time not significant), `entities=3899/batch` **+5.21% sec/op** at byte-identical allocations, complex checkpoint **+2.42% allocs** at wash time — the resolve/pull paths that gained scan-error propagation (a failed scan previously read as a clean empty; part of the prior number was the speed of a wrong answer on failure paths). The profile diff landed 2026-07-21: the per-entity delta was the tuple-builder cache rendering hash-only `Identity.String()` into its key, and the checkpoint delta was the or/or-default union-correctness fix doing branch-input work short-circuiting previously skipped — see the string-identity entries below. Artifacts: `docs/perf/*_correctness_campaign_*_2026-07-20.txt` (verified 2026-07-20, Apple M5).

- ✅ **Structural tuple-builder cache keys**: `getTupleBuilder` keyed its cache by `pattern.String()` plus rendered symbols — presentation as identity. Patterns embedding an entity constant re-rendered its L85 per Match and grew one cache entry per distinct entity (unbounded on a long-lived matcher). The key is now the builder's structural identity — four position indices plus at most four output symbols, a stack-built comparable struct over a typed map (`sync.Map` would box the key per lookup). Warm lookups allocate **zero** (was 9 allocs/call), patterns differing only in constants share one builder, and per-entity resolve improves **−28.3% allocs, −22.7% B/op, −22% sec/op** (`entities=230/per-entity` vs the campaign-recorded numbers; pinned by structural-sharing and `AllocsPerRun` tests; verified 2026-07-21, Apple M5).
- ✅ **Interned-identity hot paths**: per-datom `String()` renders replaced by interned-pointer identity — the `InternKeyword(datom.A.String())`/`InternIdentity(datom.E)` re-intern round-trips in tuple building (provably returning their own arguments; decode constructs through the intern tables), rendered-string equality comparisons in the reusing match iterator, batch scanner, and pending-transaction scan, the `IndexedMemoryMatcher` attribute index (`map[string]` → `map[Keyword]`), and `entityAttrKey` (`{[20]byte, string}` → `{Identity, Keyword}` pointers). Allocations byte-identical (the round-trips were CPU, not allocs); the pure per-datom paths show the win directly: `DatomFromKeyToTuple` **−13.7%**, `IteratorTupleAllocation` **−12.5/−14.6%**, `BatchScanning/NoConstraints` **−19.3%**, executor `AttributeScan` **−7.4 to −11.1%**, `BuildIndices_100000` **−25.3%**. Apparent regressions on Badger-heavy arms re-ran flat or better at identical allocation profiles. Artifacts: `docs/perf/string_identity_*_2026-07-21.txt` (n=5; verified 2026-07-21, Apple M5).
- ✅ **Add-wins set resolution from key spans**: `resolveAddWinsSet` fully decoded every historical datom — `ValueFromBytes` plus keyword/identity interning per scanned entry, then re-encoded the value it just decoded to key its state — 78.8% of the cardinality-many `Set()` path's allocations (measured via the new `BenchmarkCRDTWrite/CardinalityMany/SetMembership`, the first benchmark to exercise this path). Resolution now scans keys only (`ScanKeysOnly`), reads the raw value span/Tx/Op via `encoder.DecodeKey` through concrete iterator types (direct, inlinable per-entry calls; badger arm build-tagged with a wasm stub; external `Store` iterators keep the full-decode path with identical semantics), keys state by raw span with zero-alloc lookups, and decodes values **only for members that survive resolution** (`valueFromKeySpan`, extracted as the single home the datom decoder also consumes). `InternKeywordFromBytes` also gained a byte-keyed typed-map fast path — zero-alloc hits engine-wide (was a 32-byte string conversion per call on every keys-only decode). Replace-membership at 32 members: **52.8ms → 8.0ms (−85%), 46,494 → 8,645 allocs (−81%)**; at 4 members **−75% / −79%**; allocs/op stays flat as (E,A) history accumulates (per-scanned-datom allocation is structurally gone; scan time remains linear in history — inherent to append-only resolution). Post-fix profile: resolution's own footprint is 1.94%; the remainder is the write itself (Badger machinery + 8-index key encoding) (identical-terms A/B, 200x; verified 2026-07-21, Apple M5).

- ✅ **Single-allocation key encoding**: `encodeKeyWithParts` sized its buffer for the key's parts but not for `Op`/`AfterRef`, so the closing append was guaranteed past capacity — every index key in the store was allocated twice and fully copied twice. Each index arm now only declares its component order and a shared assembly sizes the key once (parts + AfterRef + Op): **1 allocation per key** (was 2), pinned by `TestKeyEncodingAllocations` alongside the value span (3 → **2**) and the scan range (4 → **2**, the start/end ownership floor). Write and delete loops in both stores hoist `ToStorageDatom` and value encoding to once per datom (was once per index — 8×). Measured: `BadgerAssertBulk` **−7.3% allocs / −16% B/op** (the predicted 9 allocs/datom exactly), `CRDTWrite` arms **−6% allocs / −13% B/op**, `SetMembership/32` a further **−6.8% allocs / −15% B/op** on top of the resolution rewrite, and the read side via `EncodePrefixRange` at ~1,150 scans/op: per-entity resolve **−12.1% allocs**, batch resolve **−21.8% allocs**. Artifacts: `docs/perf/key_encoder_after_2026-07-21.txt` (n=5; verified 2026-07-21, Apple M5).
- ✅ **Direct point-lookup resolution**: cache-less per-attribute resolution (`resolveAttributeViaMatcher`) built a `DataPattern` and ran the full relational `Match` machinery — pattern construction, streaming relation, CRDT/counting iterator stack, tuple builders — per (E, A) point lookup, ~70% of the per-entity resolve path's repo-owned allocations in the Badger-excluded profile. Non-history lookups now route through `LookupAttribute`'s direct index scans (cardinality one and many join vector, which already routed); history mode keeps `Match` for raw datom reads. `LookupAttribute`'s cardinality-many fallback was itself a second, stale add-wins implementation — full datom decode per entry, values boxed as `map[interface{}]` keys, and a live panic on `[]byte` members (reachable from `get-else`/`missing?`/pull/fusion; the `[]byte` fix `resolveAddWinsSet` received never propagated to this copy) — it now delegates to `resolveAddWinsSet`, the single home, with the red-first reproducer `TestLookupAttributeManyBytesMembers` pinning `[]byte` membership. Per-entity resolve at 230 entities: **−49.7% sec/op (3.24ms → 1.63ms), −38.8% B/op, −28.9% allocs (45,329 → 32,218)**; the batch path is untouched and byte-identical. Artifacts: `docs/perf/lookup_attribute_routing_2026-07-21.txt` (n=5; verified 2026-07-21, Apple M5).
- ✅ **Single-scan wildcard resolution**: schemaless `ResolveAllAttributes` opened six Badger read transactions per entity — one attribute-discovery scan plus one `LookupAttribute` per attribute. Latest and as-of reads now resolve through the batch wildcard walk (`resolveWildcardEntity`) applied to one entity: a single bounded EATV keys-only iterator discovers and resolves every attribute in the same pass, populating the EA cache write-through exactly as the batch path does. History mode keeps discovery-then-raw-resolve, with discovery reading only the attribute span of each key — values stay undecoded and Tier-3 blobs are never dereferenced during discovery. Per-entity resolve at 230 entities: **−67.7% sec/op (1.63ms → 527µs), −74.2% B/op, −70.7% allocs (32,218 → 9,448)** against the direct point-lookup baseline; per-entity now sits within **1.5×** of the batch path (was 14.4× when batching landed) and holds 41 vs 25 allocs/entity at 3,899 entities. Stated trade: the walk decodes every historical entry of a cardinality-one attribute where `LookupAttribute` stopped at the first — the same trade the batch path ships. Artifacts: `docs/perf/single_scan_resolve_2026-07-21.txt` (n=5; verified 2026-07-21, Apple M5).
- ✅ **Database-shared tuple-builder cache**: every matcher a `Database` minted — per `Matcher()` call, per `Database.Match` pattern, per executor — was born with its own empty tuple-builder cache, so the structurally-keyed builder population never survived the matcher that warmed it. The `Database` now owns one `tupleBuilderCache`, injects it into every matcher it constructs, and temporal handles inherit it (classified `fieldInherited` in the temporal field contract); matcher constructors initialize the cache lazily, so matchers that never build tuples pay nothing. Builders now persist for the database's lifetime instead of one matcher's — pinned structurally by `TestDatabaseMatchersShareTupleBuilders`. Measured on per-entity resolve at 230 entities: **9,448 → 8,988 allocs/op (−4.9%), −18.4 KB/op**, exactly the two dead-weight cache objects per entity the profile predicted; the cross-query effect is structural rather than a benchmark claim. Artifacts: `docs/perf/shared_builder_cache_2026-07-21.txt` (n=5; verified 2026-07-21, Apple M5).
- ✅ **Specialized EA-cache trie**: the EA cache was four `sync.Map`s, and every `GetOrResolve` boxed its 52-byte `CacheKey` into `interface{}` twice — once against `entries`, once against `maxVersions` — hashing through `nilinterhash` and walking two tries per ask; measured at ~15% of complex-checkpoint CPU, with `LookupAttribute` (attribute-fetch fusion, per tuple) driving two-thirds of the asks. The cache now keys one port of Go's `internal/sync.HashTrieMap` (`datalog/storage/hashtriemap.go`, concurrency logic verbatim, GO-LICENSE alongside), fully specialized by owner ruling: `CacheKey` keys, a combined `cacheSlot{entry, version}` value so the resolved view and its freshness high-water mark come back in **one lock-free walk**, and unseeded routing from the key's own bits (`E₀^A₀^A₁^A₂^A₃` — E is a SHA1 content address; engineered routing collisions only lengthen full-key-compared overflow chains, accepted per the embedded threat model). The in-flight/invalidate protocol became single-slot CAS transitions: invalidation clears the entry and keeps the version in one swap, store folds the version advance into the same swap. Measured vs the sync.Map baseline (n=10): cache hit **16.7ns → 6.1ns (−63.7%)**, complex checkpoint **33.5ms → 24.2ms (−27.9%, p=0.000, ±1%)**, allocations byte-identical; `GetOrResolve` lookup machinery 1.14s → 0.31s (−73%) on profiled 200x runs. An RWMutex typed-map experiment measured the bound first (−11.6% checkpoint but +15% on uncontended hits); the lock-free specialized trie strictly dominated it. Trie covered by crafted-collision tests (routing is public, so collision chains are constructed with real keys) and the full cache-protocol suite. Artifacts: `docs/perf/cache_trie_2026-07-21.txt` (verified 2026-07-21, Apple M5).
- ✅ **Positional tuple keys and split-overflow key map**: the tuple-key machinery every join build, dedup set, and group table rides on was ~40% of the complex checkpoint's allocations — one `values []interface{}` slice per `NewTupleKey` (probe or store alike, ~98,600/op) and one bucket backing array per key stored in `TupleKeyMap`'s `map[uint64][]mapEntry` (~120,000/op across `Put`/`PutIfAbsent`). Probes no longer materialize keys: `GetPositions`/`PutIfAbsentPositions` hash and compare directly against tuple positions (hashing mirrors `NewTupleKey` exactly, including the single-position bare-value case, pinned by a cross-representation consistency test), converting the hash-join probe side, `cachedBranch` probes, or-fallback outer-key dedup, and both aggregation group lookups; owned values slices now materialize only on genuine insertion. The map holds each hash's first entry inline in a 40-byte slot with a separate overflow map that stays nil until a real collision — an inline-bucket intermediate hit −27.5% allocs but +12.7% B/op from 64-byte slots; the split-overflow form (owner's call) kept the count win and inverted the bytes. Complex checkpoint: **−27.5% allocs/op (550.4k → 399.1k), −7.64% sec/op (23.99ms → 22.15ms), −3.01% B/op** (n=10, p=0.000). Zero-allocation probe hits pinned by `AllocsPerRun`. Artifacts: `docs/perf/tuplekey_positional_2026-07-21.txt` (verified 2026-07-21, Apple M5).
- ✅ **Allocation-round aggregate (complex checkpoint)**: the 2026-07-21 round measured end-to-end against its fork point (`502086c`, post-PR-#112 main) on `BenchmarkComplexQueryCheckpoint`: **−13.11% allocs/op (633.5k → 550.4k) and −5.36% B/op (47.15 → 44.63 MiB), both ±0% at p=0.000**, with wall time statistically unchanged (p=0.796) and total CPU flat over identical 200-iteration profiled runs (7,430ms → 7,500ms samples, within noise). The checkpoint averages ~1 core and its ~78-byte-average allocations make allocator/GC a minor CPU share, so the round's allocation reductions land as footprint, not checkpoint speed; the round's wall-time wins live on its targeted paths (per-entity resolve 6.2×, `Set()` membership −85%, bulk assert −16% B/op). Observed lead for a future pass: intern-table `HashTrieMap.Load` is ~15% of checkpoint CPU. Artifacts: `docs/perf/round_checkpoint_{before,after,benchstat}_2026-07-21.txt` (n=10; verified 2026-07-21, Apple M5).
- ✅ **Span-grouped or-fallback branch cache**: `cachedBranch` stored one `TupleKey` and one `[]Tuple` per distinct key, boxing each rows slice into the key map's `interface{}` value; an interim pointer-valued variant measured only −3.76% allocs because the escaped slice header washed one-for-one with the boxing it removed. Branch rows now collect once (exact-sized backing when `Size() >= 0`, copy gated on `RequiresCopy()`) and group by key hash into contiguous spans of one shared `[]Tuple` backing via counting-sort placement, indexed by a typed `map[uint64]rowSpan`. A probe hashes the outer tuple's key positions, verifies the key against the span's first row, and returns `rows[start:end]` — zero allocations on hit, miss, and collision paths (pinned by `AllocsPerRun`); distinct keys sharing a hash divert that span to per-key row groups, so correctness never rests on hash uniqueness. Both builders — branch-result indexing and the fused EA-cache lookup arm — share the grouping. Complex checkpoint: **−15.21% allocs/op (399.1k → 338.4k), −6.16% B/op (43.27 → 40.61 MiB), −4.38% sec/op (22.67ms → 21.68ms)** (n=10, p≤0.004). Artifacts: `docs/perf/branchcache_spans_2026-07-21.txt` (verified 2026-07-21, Apple M5).
- ✅ **Or-fallback direct probe emit**: the post-spans reprofile put ~29% of checkpoint allocations (~107k/op, every site 100%-attributed) in `nextShortCircuit`'s per-outer-tuple scaffolding — `branchInput` built a visible-tuple copy plus a single-tuple relation that cached iterations never consumed; each probe's `[]Tuple` was wrapped in `NewMaterializedRelation`, whose constructor **re-deduplicated rows that are a contiguous span of an already-deduplicated branch relation** (a set-invariant violation) and allocated an iterator per probe; `projectTupleWithFallback` built two symbol maps per emitted tuple from per-branch constants; and `outerJoinKeys` materialized a key tuple per outer row including duplicates. Cached branches now emit directly from the probe slice (cursor on the iterator — no wrapper, no re-dedup, no per-probe iterator); a per-branch memoized `projectionPlan` (output position ← branch position, else outer fallback) replaces the per-tuple maps across the short-circuit, correlated-union, and `projectedIterator` paths; `branchInput` computes lazily only when a non-cached branch consumes it; `outerJoinKeys` deduplicates positionally and materializes keys on first sight; and the fused EA-cache builder deduplicates outer entities at collection (one lookup, one row per entity — duplicates were previously masked by the wrapper's re-dedup). Complex checkpoint: **−35.99% allocs/op (338.4k → 216.6k), −22.03% B/op (40.61 → 31.66 MiB), −17.79% sec/op (21.62ms → 17.78ms)** (n=10, p=0.000). Artifacts: `docs/perf/orfallback_direct_emit_2026-07-21.txt` (verified 2026-07-21, Apple M5).
- ✅ **Grouped hash-join build rows**: line-level profiling put ~40.7% of checkpoint objects in the hash-join family — a `TupleKey` values slice per build row duplicating values already in the stored tuple, a one-element `[]Tuple` per distinct key, append-grow plus Get/Put double map access per fanout row, and a key materialized per probe row in the materialized loop (the default path, missed when the positional-probe round converted the streaming iterator). The or-fallback span structure generalizes into `groupedRowIndex` (`datalog/executor/grouped_row_index.go`): build rows collect once and group by key hash into contiguous spans; probes verify against the rows in place, so no key materializes on either side. `cachedBranch` becomes a thin wrapper adding branch symbols; the `buildKeysUnique` candidate-key proof is checked against the grouped data (`keysUnique`); semi/anti-join left probes convert to positional membership checks. Semi/anti right-key stores and aggregation group keys keep `TupleKeyMap` — there the stored key values are the product. Complex checkpoint: **−34.90% allocs/op (216.6k → 141.0k), −5.90% B/op (31.66 → 29.79 MiB), wall time statistically unchanged (p=0.529), CPU time flat (4.46s → 4.47s samples over identical 200-iteration profiled runs)** — these were tiny objects on the malloc fast path in already-tight loops, and GC scan cost tracks bytes more than object count, so the reduction lands as footprint (n=10; verified 2026-07-21, Apple M5). Artifacts: `docs/perf/joinbuild_grouped_2026-07-21.txt`.
- ✅ **Per-query read sessions**: every `BadgerStore.Scan`/`ScanKeysOnly` opened its own read transaction, so a query's successive scans could observe different database states — a write landing between two scans of one query produced rows pairing values from states that never coexisted (pinned red-first by `TestQuerySnapshotConsistency`). All storage reads of one query now flow through one `ReadSession` (`Store.NewReadSession()`): one Badger read-only transaction — handle creation mutex-serialized per Badger's documented contract, iteration concurrent — or a copy-on-write B-tree clone on MemoryStore. The matcher threads a `StoreReader` (store = per-call transactions outside queries; session = one snapshot within one), and the session closes when the query result is exhausted or closed, whichever comes first, with a finalizer backstop. Known residual, ruled and documented: the shared EA cache can serve an entry newer than the query's snapshot under concurrent in-process writes; the query's own rebuilds are snapshot-consistent; session-version-bounded cache reads are deferred. **Wall time and B/op wash, +0.00% allocs (the per-query session object) — and the scheduler wake churn collapses: `pthread_cond_signal` 1,060ms → 300ms (−72%) over identical 200-iteration profiled runs, the wake/spin cluster from ~31% of checkpoint CPU to ~10% (~4.8ms/op of side-CPU eliminated), total CPU −3%.** The wake band was dominated by the two WaterMark channel sends of each per-scan transaction; wall is unchanged because that CPU burned beside the critical path, never on it (n=10; verified 2026-07-21, Apple M5). Artifacts: `docs/perf/read_sessions_2026-07-21.txt`.
- ✅ **Session-bounded EA-cache reads (phase 2)**: review of the read sessions reproduced the remaining tear — cache hits weren't session reads, so a concurrent commit plus a second latest reader could hand a sessioned query content newer than its snapshot mid-execution. `GetOrResolve` now takes a bound (nil = latest, the matcher's `*ElementID` mode convention): a fresh entry serves a sessioned query only when the slot version lies within the snapshot — provably identical to the session's own resolution — and anything newer resolves through the session. `storeIfNotInFlight` refuses to replace an entry with a strictly older-versioned one, ending the born-stale clobber churn. The bound is the session's max ElementID, computed lazily once per matcher (O(1) TAEV seek). Pinned by `TestSessionBoundedCacheRead` (the review's two-reader repro) and `TestStoreGateKeepsFresherEntry`. **Cost: +0.6–0.7ns on the fresh-hit microbench (6.7 → 7.4ns, zero allocs), CPU flat by 200-iteration profile diff, checkpoint wall/B/op wash against a contemporaneous baseline (+0.01% allocs)** — a high-variance measurement window made the first benchstat pair read +15%; re-running the baseline in the same window resolved it to p=0.796 (verified 2026-07-21, Apple M5). Also fixes `ClearInterns` missing the byte-keyed keyword cache (review bug 2): a post-clear `InternKeywordFromBytes` served a stale pointer that panics interned comparison; pinned by `TestClearInternsResetsKeywordByteCache`. Artifacts: `docs/perf/session_bounded_cache_2026-07-21.txt`.

### Claims Requiring Qualification
- ⚠️ **Plan quality**: "13% better plans" not supported by current benchmarks (planners perform identically)
- ⚠️ **In-memory indexing**: "49-4802×" not reproducible (optimizations became pervasive, both paths now fast)
- ⚠️ **Streaming joins**: On the production-shaped complex checkpoint,
  `EnableStreamingJoins=true` is **8.04% slower**, uses **3.78% more memory**,
  and performs **8.34% more allocations** than the materialized default
  (`p=0.000`, `n=10`). Keep streaming joins opt-in.

---

## What's Actually Working ✅

### 0. Clause-Based Planner + QueryExecutor (ACTIVE - ARCHITECTURAL WIN)
**Status**: ✅ Production-ready and default architecture
**Performance**: **2× faster on complex queries** (verified Oct 2025)
**Location**: `datalog/planner/planner_clause_based.go`, `datalog/executor/query_executor.go`

**Measured Results**:
- Full architecture comparison (OHLC queries): ~4-8s (old) → ~2-4s (new) = **2× faster** ✅
- Plan quality isolated (same executor): Both planners perform identically within measurement noise
- Planning overhead: 3-12µs (old) → 1-7µs (new) = 37-88% faster (but negligible impact)

**Key Difference**: The 2× speedup comes from QueryExecutor's clause-by-clause streaming execution, not from better plan quality. Both planners produce equivalent-quality plans.

**Configuration**: Enabled by default. The `db.Open` API uses these defaults automatically:
```go
// Public API — uses default planner/executor configuration
d, _ := db.Open("path/to/db")
d.Query(`[:find ?e ?v :where [?e :price/close ?v]]`)

// Advanced: direct executor construction for non-default options
opts := storage.DefaultPlannerOptions()
exec := executor.NewExecutorWithOptions(matcher, opts)
```

**Details**: See `docs/archive/completed/PLANNER_COMPARISON.md` (archived: one planner now)

### 1. Query Plan Caching (ACTIVE)
**Status**: ✅ Implemented and enabled by default
**Location**: `datalog/planner/cache.go`, `datalog/storage/database.go:34`
**Performance**: ~3× speedup for repeated queries (measured)

### 2. Batch Scanning with Iterator Reuse (ACTIVE)
**Status**: ✅ Implemented, used for large binding sets
**Location**: `datalog/storage/matcher_relations.go:122-128`
**Threshold**: Activated when `bindingRel.Size() > 100`
**Result**: Code clarity improvement, minimal performance impact

### 3. Predicate Classification (ACTIVE)
**Status**: ✅ Infrastructure in place, used by executor
**Location**: `datalog/executor/predicate_classifier.go`
**What it does**: Classifies predicates as pushable vs. non-pushable

### 4. Join Condition Detection (ACTIVE)
**Status**: ✅ Implemented and used
**Location**: `datalog/executor/join_conditions.go`
**What it does**: Detects equality predicates that can be pushed into joins

### 5. Progressive Join Execution (CRITICAL & ACTIVE)
**Status**: ✅ Core safeguard preventing memory explosion
**Location**: `datalog/executor/relation.go`
**Why it matters**: Greedy join ordering + early termination prevents catastrophic intermediate result sizes
**Scale achieved**: Production-ready for datasets from 100K to 10M+ datoms, tested up to 500M+

---

## Recent Optimizations (October 2025) ✅

### 0. Single-Use Iterator Semantics & Streaming (COMPLETE - CORRECTNESS FIX)
**Status**: ✅ Proper iterator lifecycle management with single-use semantics
**Performance**: **4.06× speedup for iterator composition**, **2.22× for streaming** (verified 2025-10-25)
**Commits**: 626e409 (latest), 4a394cb, 4f3b742, 15d196d, 78c930a

**What We Fixed** (2025-10-25):
- ✅ **Single-use iterator semantics** - StreamingRelation enforces one-time iteration
- ✅ **BufferedIterator** - Safe re-iteration support with automatic caching
- ✅ **Iterator lifecycle** - Clear separation between first use (streaming) and re-use (cached)
- ✅ **Correctness** - All tests pass with proper semantics enforcement

**What We Built Earlier** (Oct 2025):
- ✅ **Iterator composition** - Filter/Project/Transform operations stay lazy
- ✅ **Options propagation** - ExecutorOptions flow through entire pipeline
- ✅ **BadgerMatcher streaming** - Returns StreamingRelation instead of materializing
- ✅ **Symmetric hash join** - Streaming-to-streaming joins without materialization

**Current Performance Results** (verified 2025-10-25):

**Iterator Composition Benchmark**:
- Materialized: 1,259 μs, 3.27 MB, 25K allocs
- Composed: 310 μs, 360 KB, 15K allocs
- **Result: 4.06× faster, 89% memory reduction** ✅

**Streaming Scenarios** (10K tuples):
- Large_HighSelectivity (1% pass): 1.07× faster (675μs → 630μs), 2% memory reduction
- Large_MediumSelectivity (10% pass): 1.44× faster (975μs → 676μs), 19% memory reduction
- Large_LowSelectivity (50% pass): **2.22× faster** (1,720μs → 774μs), **52% memory reduction**
- **Key Finding**: Benefits scale with filter selectivity (1.07× to 2.22× depending on selectivity)

**Configuration**: Enabled by default:
```go
EnableIteratorComposition: true  // Lazy evaluation (default)
EnableTrueStreaming: true        // No auto-materialization (default)
```

### 1. In-Memory Indexing (COMPLETE - PERVASIVE OPTIMIZATION)
**Status**: ✅ IndexedMemoryMatcher with hash indices
**Performance**: Hash indices now used throughout (test suite 7s, down from timeouts)
**Commit**: Latest

**What Works**:
- ✅ Hash indices for E/A/V lookups (entityIndex, attributeIndex, valueIndex, eavIndex)
- ✅ Thread-safe lazy initialization with sync.Once
- ✅ Smart index selection (EA > E > A > V > linear)
- ✅ Two-phase value lookup (hash → exact match) for interface{} types

**Historical Note**:
During development, benchmarks showed dramatic speedups (49-4802×) comparing linear scan vs indexed lookups. However, subsequent refactoring made hash indices the default path for both IndexedMemoryMatcher and MemoryPatternMatcher. Current benchmarks show identical performance because both implementations now use the optimized path.

**Impact**: Entity lookups are O(1) instead of O(N). Test suite execution time dramatically reduced. This is a **success story** - the optimization became so pervasive that there's no longer a "slow path" to compare against.

**Details**: See `IN_MEMORY_INDEX_RESULTS.md` (historical benchmarks)

### 2. Time Range Optimization (COMPLETE)
**Status**: ✅ Fully implemented with storage integration
**Performance**: **4× speedup on large datasets** (41s → 10.2s on 260-hour dataset, commit dc2ad4e)
**Note**: Small datasets show minimal benefit (3.5% on 10-day dataset)
**Commits**: Latest in dc2ad4e

**What Works**:
- ✅ Time range extraction from correlation keys (10µs for 260 ranges)
- ✅ Multi-range AVET scanning in BadgerDB (260 time ranges)
- ✅ Metadata propagation (negligible 20ns overhead)
- ✅ Size check optimization (<50 tuples skip extraction)

**Benchmark Results** (BenchmarkOHLCQuery, verified 2025-10-24):
- Small dataset (10 days, 390 bars/day):
  - WithoutPushdown: 48.8ms
  - WithPushdown: 33.3ms
  - WithTimeRangeOpt: 32.1ms (**3.5% improvement**, 1.52× vs no optimization)
- Large dataset (260 hours): **4× speedup** (41s → 10.2s, measured during development)

**Key Insight**: Time-range optimization benefit scales with dataset size and time selectivity. Most valuable for large historical datasets with selective time filters.

**Details**: See `TIME_RANGE_OPTIMIZATION_STATUS.md`

### 3. Hash Join Pre-Sizing (COMPLETE)
**Status**: ✅ Implemented across all join operations
**Performance**: 24-32% faster, 24-30% less memory for hash operations
**Commit**: dc2ad4e

**What Was Done**:
- ✅ Added `NewTupleKeyMapWithCapacity()` for pre-sizing
- ✅ Updated 7 call sites (HashJoin, SemiJoin, AntiJoin, deduplication)
- ✅ Pre-size based on relation sizes to avoid map growth

**Impact**:
- Micro-level: 24-32% faster TupleKeyMap operations
- Macro-level: Minimal impact on OHLC (pattern matching dominates)
- High impact for large joins (>1,000 tuples)

**Details**: See `HASH_JOIN_PRESIZING_SUMMARY.md`

### 4. Semantic Rewriting / `EnableSemanticRewriting` (REMOVED)
**Status**: ❌ Removed in 2026-05. Rewrote `[(year ?t) ?y] [(= ?y N)]` patterns
into range predicates. Two reasons for removal:

1. **Silently wrong for modular time constraints**: only correct when the
   bound time components formed a contiguous suffix from `year` downward.
   `day(?t) = 5` alone became `?t >= 1970-01-05 AND ?t < 1970-01-06`,
   matching nothing on real datasets. Discovered when the OHLC benchmark's
   `WithTimeRangeOpt` sub-benchmark started returning zero rows.
2. **Redundant in the default configuration**: with `EnableAlgebraOptimizer`
   on (default-active), decorrelation handles the same bottleneck. The
   2.6-5.8× standalone speedups collapsed to 1.00× whenever decorrelation
   was also enabled (~97% of time-extraction evaluations were eliminated by
   decorrelation alone, leaving nothing for the rewriter to optimize).

The framework was 285 LOC + ~810 LOC of tests for a default-off
optimization that the active optimizer already covered. Net: ~1,250 LOC
removed including the never-called `TxRangeRewriter` collateral.

If you need this manually: write the range predicate directly:
`[(>= ?t #inst "2025-01-01")] [(< ?t #inst "2026-01-01")]`.

**Historical benchmarks** (kept for the record, pre-decorrelation, in
isolation): year filter 2.6×, day filter 4.1×, hour filter 5.8×.

### 5. Common Subexpression Elimination - CSE (REMOVED)
**Status**: ❌ Removed in v0.10.2. The Selinger-style implementation operated on
filter groups from the old decorrelation path, which is superseded by the algebra
bridge. The option, tests, and implementation were dead code.

**Future**: CSE at the algebra IR level (identifying shared subtrees across
decorrelated subqueries) would require extending the IR from tree to DAG.
See `docs/archive/2025-10/CSE_FINDINGS.md` for historical analysis.

### 6. Parallel Subquery Execution (COMPLETE)
**Status**: ✅ Implemented and enabled by default
**Performance**: **2.06× speedup** with 8 workers (verified 2025-10-25)
**Commits**: 626e409 (with single-use semantics), d645cfd, ec45d77, 2439e0a

**What Works**:
- ✅ Worker pool with bounded parallelism (uses runtime.NumCPU())
- ✅ Query plan reuse across iterations
- ✅ Thread-safe result aggregation
- ✅ Proper iterator lifecycle management

**Benchmark Results** (BenchmarkRelationInputParallel, 2400 input tuples):
- Sequential: 730 ms, 1,101 MB/op
- Parallel-2Workers: 738 ms (1.01× slower, overhead for small dataset)
- Parallel-4Workers: 534 ms (1.37× faster)
- **Parallel-8Workers: 355 ms (2.06× faster)** ✅
- Parallel-16Workers: 436 ms (1.67× faster, diminishing returns)
- Parallel-32Workers: 418 ms (1.75× faster, overhead exceeds benefit)

**Key Finding**: Optimal worker count is 8 on M3 Max. Memory usage remains constant across all configurations (~1.1 GB), demonstrating proper lifecycle management and no memory leaks.

### 7. Intern Cache Optimization (COMPLETE)
**Status**: ✅ Lock-free sync.Map replacing sync.RWMutex
**Performance**: 6.26× speedup for BadgerDB parallel queries
**Commit**: e3c956b

**Impact**:
- Before: 35% CPU time on mutex contention
- After: Near-zero lock contention with atomic operations
- Micro-benchmarks: 13-80× faster intern operations

### 8. Pull API Performance (MEASURED 2025-12-17)
**Status**: ✅ Production-ready with comprehensive benchmarks
**Performance**: **9× faster than equivalent queries**, linear scaling
**Location**: `datalog/executor/pull.go`, `datalog/storage/database.go`

**Why Pull is Fast**:
- Direct index seeks via `EntityLookupMatcher` interface
- No query parsing or planning overhead
- Single AEVT index lookup per attribute
- Wildcard uses single EAVT prefix scan

**In-Memory Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| Single Attribute | 554ns | 19 | Base case |
| 5 Attributes | 2.3µs | 87 | ~470ns/attr |
| Wildcard (5 attrs) | 1.7µs | 36 | **Faster than explicit** |
| Nested (2 levels) | 2.1µs | 72 | +reference follow |
| Deep (3 levels) | 2.6µs | 91 | Linear with depth |
| PullMany (100 entities) | 102µs | 3601 | ~1µs/entity |

**BadgerDB Storage Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| Single Attribute | 1.1µs | 31 | 2× in-memory |
| 5 Attributes | 6.2µs | 147 | ~1.2µs/attr |
| Wildcard (5 attrs) | 2.8µs | 69 | **2.2× faster than explicit** |
| Nested (2 levels) | 5.0µs | 120 | |
| Standalone API | 4.3µs | 116 | Includes parse |
| Cached Pattern | 3.5µs | 89 | Pre-parsed |
| PullMany (100 entities) | 225µs | 6001 | ~2.2µs/entity |

**Pull vs Query Comparison** (3 attributes, BadgerDB):
| Method | Time | Speedup |
|--------|------|---------|
| Pull | 3.5µs | **9.2×** |
| Query | 32.7µs | baseline |

**Wildcard find-pull batching** (5 attributes/entity, cache disabled):

| Entities | Per-entity transactions | Batched EATV scan | Time | Memory | Allocations |
|---------:|------------------------:|----------------------:|-----:|-------:|------------:|
| 230 | 4.829 ms | 335.4 µs | **14.4× faster** | **90.9% less** | **89.7% fewer** |
| 3,899 | 64.19 ms | 5.917 ms | **10.8× faster** | **90.9% less** | **89.7% fewer** |

`applyFindPulls` now collects the result entities and calls `PullMany` once per
pull expression. Exact wildcard patterns use `BatchEntityResolver`;
`Database.ResolveAllAttributesMany` deduplicates and sorts entities by EATV
order, then seeks every non-unique entity range through one Badger transaction
and iterator. Unique-attribute ownership walks and Tier-3 blob dereferences retain
their specialized reads. `pull/batch.begin` and `pull/batch.complete` annotations
make the result-boundary work visible. Explicit-attribute and nested pull patterns
retain their existing per-entity execution.

**Scaling Characteristics**:
- **Per-attribute cost**: ~1.2µs (BadgerDB), ~470ns (in-memory)
- **Per-entity cost**: ~2.5µs (BadgerDB), ~1µs (in-memory)
- **Linear scaling**: Both attributes and entities scale linearly
- **Pattern caching**: 20% speedup by pre-parsing patterns

**Key Insight**: Wildcard `[*]` is 2× faster than explicit attribute lists because it performs one EAVT scan instead of N AEVT lookups.

**Recommended Usage**:
```go
// For hot paths, cache the parsed pattern
pattern, _ := parser.ParsePullPattern(`[:user/name :user/age]`)
puller := executor.NewPullExecutor(d.Unwrap().Matcher())

// Reuse pattern across calls
for _, entity := range entities {
    result, _ := puller.Pull(entity, pattern)
}
```

### 9. Schema Validation Performance (MEASURED 2025-12-17)
**Status**: ✅ Negligible overhead for type validation, minimal overhead for uniqueness
**Performance**: Type validation **<1% write overhead**, uniqueness checking **~6% write overhead**
**Location**: `datalog/schema/`, `datalog/storage/database.go`

**Note**: All schema overhead is on the **write path only**. Reads are completely unaffected.

**Type Validation Overhead** (Apple M4 Max, write path):
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Add without schema | 25,771 | 223 | baseline |
| Add with schema | 25,768 | 225 | **<0.2%** |

**Uniqueness Checking Overhead** (write path):
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Add without schema | 25,771 | 223 | baseline |
| Add with uniqueness | 27,236 | 292 | **~5.8%** |

**Bulk Operations (100 items/transaction)**:
| Benchmark | ns/op | allocs | Overhead |
|-----------|-------|--------|----------|
| Bulk without schema | 1,388,000 | 17,115 | baseline |
| Bulk with schema | 1,408,000 | 17,117 | **~1.4%** |

**Schema Resolution (one-time cost per Pull pattern)**:
| Operation | ns/op | allocs |
|-----------|-------|--------|
| Resolve 5-attr pattern with nested refs | 225 | 10 |

**Pull with Cardinality-Many** (10 values):
| Cardinality | ns/op | allocs |
|-------------|-------|--------|
| Cardinality-one (1 value) | 1,117 | 31 |
| Cardinality-many (10 values) | 3,679 | 96 |

**Key Findings**:
- **Type validation is essentially free** - just a map lookup and type switch (write path)
- **Uniqueness checking adds ~6% to writes** - requires database query to check existing values
- **Reads are unaffected** - schema validation only impacts the write path
- **Schema resolution is negligible** - 225ns one-time cost per Pull pattern
- **Cardinality-many scales linearly** - ~370ns per additional value in Pull results

**Recommendation**: Enable schema validation freely for type safety. Uniqueness constraints are worth the 6% write overhead for data integrity.

### 10. CRDT Storage Performance (MEASURED 2026-01-31)
**Status**: ✅ Production-ready with comprehensive benchmarks
**Performance**: Write operations ~25-35µs, reads scale linearly with cardinality
**Location**: `datalog/storage/database.go`, `datalog/storage/matcher.go`

**Why CRDT Storage**:
- **LWW (Last-Writer-Wins)** for cardinality-one: Highest ElementID wins
- **Add-wins** for cardinality-many: Concurrent add + remove at same Lamport → add wins
- **RGA** for cardinality-vector: Replicated Growable Array for ordered collections
- All writes preserved with ElementIDs for time-travel queries

**Write Operation Benchmarks** (Apple M4 Max):
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| CardinalityOne (LWW) | 24,906 ns | 266 | Single value write |
| CardinalityMany/Add | 26,132 ns | 263 | Add to set |
| CardinalityMany/AddRemove | 33,725 ns | 373 | Add + Remove pair |
| CardinalityVector/Append | 27,126 ns | 270 | RGA append |

**Read Operation Benchmarks**:
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| CardinalityOne | ~1.1µs | 29 | Direct index lookup |
| CardinalityMany/10 members | 3,259 ns | 74 | Set resolution |
| CardinalityMany/100 members | 26,522 ns | 443 | Full set resolution |
| CardinalityVector/10 elements | 21,239 ns | 420 | RGA reconstruction |
| CardinalityVector/100 elements | 204,660 ns | 3,687 | RGA reconstruction |

**Comparison with Documented Pull API Performance**:
| Operation | Documented (Pull) | CRDT Benchmark | Status |
|-----------|-------------------|----------------|--------|
| Cardinality-One | 1,117 ns, 31 allocs | 1,100 ns, 29 allocs | ✅ Match |
| Cardinality-Many (10 values) | 3,679 ns, 96 allocs | 3,259 ns, 74 allocs | ✅ Faster |

**CRDT Resolution Benchmarks**:
| Operation | Time | Allocs | Notes |
|-----------|------|--------|-------|
| AddWins/NoConflict (50 members) | 13,809 ns | 240 | No tombstones |
| AddWins/WithTombstones (100 adds, 50 removes) | 37,911 ns | 649 | With tombstone filtering |
| LWW/ManyVersions (1000 versions) | 965 ns | 31 | First entry = current |

**Scaling Characteristics**:
- **Per-element cost (vectors)**: ~2µs per element for reconstruction
- **Set resolution**: O(n) where n = total operations (adds + removes)
- **LWW resolution**: O(1) - first entry in descending Tx scan is current

**Key Findings**:
- **Write performance is consistent** across cardinalities (~25-35µs, storage I/O dominates)
- **LWW is extremely fast** - 965ns to resolve current value from 1000 versions (EATV: first entry = current)
- **Vector reconstruction is expensive** - RGA reconstruction (21µs for 10 elements) is **~6× slower** than set resolution (3.3µs for 10 members) due to graph traversal
- **Add-wins resolution** scales with complexity - 50 clean members: 14µs; with 50 tombstones: 38µs (~2.7× more work)

**Comparison with Non-CRDT Writes**:
| Benchmark | CRDT (ns/op) | Schema Validation (ns/op) | Notes |
|-----------|--------------|---------------------------|-------|
| Single Add | 24,906 | 25,768 | CRDT ~3% faster |
| With Uniqueness | 27,236 | 27,236 | Same (uniqueness dominates) |

CRDT semantics add negligible overhead to writes while providing:
- Conflict-free replication capability
- Time-travel queries via `d.History()` and `d.AsOf(elementID)`
- Multi-replica merge support

### 11. CRDT Allocation Optimization (COMPLETE - February 2026)
**Status**: ✅ Production-ready with comprehensive benchmarks
**Performance**: **90% faster** (1.9×), **2.2× less memory** than pre-CRDT main branch (verified 2026-02-02)
**Location**: `datalog/storage/`, `datalog/executor/`

**The Story**: CRDT storage added powerful new capabilities (LWW, add-wins sets, RGA vectors, time-travel queries). The initial implementation had allocation overhead. Rather than accept a performance regression, we optimized the entire storage and executor pipeline. The result: **CRDT storage that's faster than the original non-CRDT implementation**.

**What We Optimized** (6 phases targeting hot paths):

**Phase 1: txToDescending optimization**
- Changed return type from `[]byte` to `[16]byte` (stack allocation)
- Result: 16 B/op → 0 B/op, ~29% faster encoding

**Phase 2: Iterator workspace reuse**
- Added `workspace` field to all matcher iterators
- Use `BuildTupleInternedInto()` instead of `BuildTupleInterned()`
- Result: 7-15% memory reduction, 13-29% fewer allocations

**Phase 3: Cache path Datom reuse**
- Pre-allocate `datomBuf` in cache path closures
- Reuse across iterations instead of allocating per tuple
- Result: 5% time improvement, 9% fewer allocations

**Phase 4: DatomFromKey by value**
- Changed return type from `*datalog.Datom` to `datalog.Datom`
- Added `currentDatom` field to iterator structs
- Result: 80 B/op → 0 B/op per datom decode, 25% faster

**Phase 5: RequiresCopy() method**
- Added `RequiresCopy()` to Relation interface
- MaterializedRelation: false (stable slice)
- StreamingRelation: true (iterator reuses workspace)
- Hash join build phase copies only when RequiresCopy()=true

**Phase 6: Conditional copyTuple()**
- Wrapper relations (Union, OrFallback, Prepended) copy once at boundary
- All other copyTuple() calls conditional on RequiresCopy()
- `collectTuplesInto()` helper for consistent conditional copying

**Benchmark Results** (Apple M4 Max, BenchmarkOHLCQuery):

| Metric | Main (pre-CRDT) | With CRDT + Optimizations | Improvement |
|--------|-----------------|---------------------------|-------------|
| Time | 57ms | 30ms | **1.9× faster** |
| Memory | 66MB | 30MB | **2.2× less** |
| Allocations | 897K | 405K | **2.2× fewer** |

This means we added full CRDT semantics (LWW, add-wins sets, RGA vectors, time-travel queries) **and** made the engine nearly 2× faster than before.

**Additional Benchmarks**:

| Benchmark | Improvement | Notes |
|-----------|-------------|-------|
| VectorQuery | 11% faster | Exercises wrapper relation paths |
| CRDT resolution | O(1) LWW | First entry in descending scan |

**Key Insight**: The biggest wins came from eliminating heap allocations in hot paths:
- `DatomFromKey()` called millions of times during scans
- `txToDescending()` called for every key encoding
- Iterator workspace reuse amortizes allocation across all tuples

**Files Changed**:
- `datalog/storage/key_encoder_binary.go` - txToDescending return type
- `datalog/storage/datom_decoder.go` - DatomFromKey by value
- `datalog/storage/matcher_iterator_*.go` - Workspace reuse
- `datalog/storage/matcher_relations.go` - Cache path optimization
- `datalog/executor/relation.go` - RequiresCopy() interface
- `datalog/executor/join.go` - Conditional copy in build phase
- `datalog/executor/*.go` - Conditional copyTuple() throughout

### 12. OHLC Query Performance (MEASURED 2025-10-25)
**Benchmark**: OHLC queries with subqueries and predicate pushdown

**Subquery Performance** (BenchmarkOHLCSubqueries):
- Single aggregation: 17.3 ms/op, 66.7 MB/op, 934K allocs
- Three aggregations: 51.3 ms/op, 199.6 MB/op, 2.8M allocs
- **Result**: Linear scaling (3× subqueries = 2.96× time, 3× memory), proper semantics ✅

**Predicate Pushdown - Small Dataset** (BenchmarkOHLCQuery, 10 days × 3 symbols × 390 bars):
- Without pushdown: 33.6 ms/op, 39.4 MB/op, 534K allocs
- With pushdown: 21.3 ms/op, 20.0 MB/op, 330K allocs
- With time-range opt: 21.5 ms/op, 20.0 MB/op, 330K allocs
- **Result: 1.58× faster, 49% memory reduction, 38% fewer allocations** ✅

**Predicate Pushdown - Large Dataset** (BenchmarkOHLCQueryLargeDataset, 90 days × 50 symbols sparse):
- Without pushdown: 1,043 ms/op, 3,484 MB/op, 15.4M allocs
- With pushdown: 375 ms/op, 296 MB/op, 5.2M allocs
- **Result: 2.78× faster, 91.5% memory reduction, 66.2% fewer allocations** ✅
- **Key insight**: Predicate pushdown scales better with larger datasets (1.58× → 2.78×)

**Key Findings**:
- Iterator semantics correctly enforced (no re-iteration bugs)
- Memory scales linearly with query complexity (predictable)
- Predicate pushdown benefits increase with dataset size
- Large dataset queries complete in <400ms even with 90 days of data

### 13. Identity & Keyword Interning (COMPLETE - December 2025)
**Status**: ✅ Full pointer interning for Identity and Keyword types
**Performance**: **10-20% faster** on join-heavy workloads, **25% memory reduction** (verified 2025-12-24)
**Commits**: a504729

**What We Built**:
- Unexported structs with pointer type aliases: `type Identity = *identity`, `type Keyword = *keyword`
- Storage-aligned cache keys: `[20]byte` for Identity (SHA1), `[32]byte` for Keyword (attribute storage format)
- Zero-allocation lookup from storage via `InternKeywordFromBytes()` and `InternIdentityFromHash()`
- Runtime invariants detect interning failures (same value, different pointer → panic)

**Measured Results** (high-cardinality benchmark, 224 keywords, ~35K datoms):

| Benchmark | Time | Memory | Allocations |
|-----------|------|--------|-------------|
| JoinQuery_CrossNamespace | **-7.6%** | **-44%** | **-38%** |
| WildcardPull_ManyAttributes | **-9.7%** | **-28%** | -1.3% |
| Aggregation_ManyAttributes | -2.3% | -15% | -11% |
| geomean | **-3.8%** | **-25%** | **-14%** |

**Full benchmark suite** (executor package):

| Benchmark | Improvement |
|-----------|-------------|
| SubqueryExecution/Legacy | **-11.8%** |
| SubqueryExecution/Componentized | **-12.1%** |
| SubqueryExecutionLarge | **-10%** |
| SequentialVsParallel | **-19% to -20%** |
| TimeToFirstResult (geomean) | **-17.5%** |
| HashJoin (data_2500+) | **-7% to -11%** |

**Scaling Behavior**:
- Small datasets (< 500 tuples): 3-6% regression (intern lookup cost dominates)
- Large datasets (1000+ tuples): 7-20% improvement (pointer comparison wins amortize)
- Crossover point: ~500-1000 tuples

**Known Regression**: `TimeAggregation` +13% — intern lookup not amortized in this specific pattern.

**Why It Works**: Entities are join keys. Every hash join probe, every equality check in result deduplication, every tuple comparison is now a pointer comparison instead of 20-byte array (Identity) or string (Keyword) comparison. Memory wins come from reusing interned pointers instead of allocating fresh structs per result tuple.

**Key Insight**: The existing `datom_decoder.go` already had `[20]byte` → Identity caching and `[32]byte` → string caching. This optimization completed the picture by making all downstream comparisons pointer-based.

### 14. Conditional Aggregate Rewriting (COMPLETE - January 2026)
**Status**: ✅ Both legacy executor and QueryExecutor now support conditional aggregate rewriting
**Performance**: **7.7× faster**, **5.2× less memory**, **8.1× fewer allocations** (verified 2026-01-16)
**Commits**: Latest

**What It Does**:
Transforms correlated subqueries with aggregates into single-pass conditional aggregation:
```clojure
;; Before: N separate subquery executions
[(q [:find (max ?v) :in $ ?person ?day :where ...] $ ?p ?day) [[?max-value]]]

;; After: Single pass with conditional aggregate
(max ?v :when ?__cond_?pd)  ;; Condition filters which tuples contribute
```

**Benchmark Results** (Apple M4 Max, 3 people × 10 days × 20 events = 600 events):

| Metric | Without Rewriting | With Rewriting | Improvement |
|--------|-------------------|----------------|-------------|
| Time | 16.2 ms/op | 2.1 ms/op | **7.7× faster** |
| Memory | 15.2 MB/op | 2.9 MB/op | **5.2× less** |
| Allocations | 275,359/op | 33,899/op | **8.1× fewer** |

**Executor Comparison** (both with rewriting enabled):

| Executor | Time | Memory | Allocations | vs Legacy |
|----------|------|--------|-------------|-----------|
| Legacy | 1.95 ms | 2.87 MB | 33.4K | baseline |
| QueryExecutor | 1.98 ms | 2.95 MB | 33.9K | **+1.5%** (parity) |

**Scale Test** (with rewriting enabled):

| Scale | Legacy | QueryExecutor | Difference |
|-------|--------|---------------|------------|
| Small (600 events, 30 groups) | 1.96 ms | 1.99 ms | +1.5% |
| Medium (3000 events, 100 groups) | 11.4 ms | 12.1 ms | +6% |

**Why It's Fast**:
- **Without rewriting**: Executes N separate subqueries (one per outer tuple), each scanning and filtering data
- **With rewriting**: Single pass over data with grouped conditional aggregation
- Eliminates repeated index scans, reduces from O(N × M) to O(M)

**Implementation Note** (January 2026 fix):
The planner now emits **two representations** of conditional aggregates:
1. **Metadata**: `phase.Metadata["conditional_aggregates"]` - used by legacy executor
2. **Find clause**: Modified `FindAggregate` with `Predicate` field - used by QueryExecutor

This dual approach maintains backward compatibility while following "Datalog is the IR" principle.

**When It Applies**:
- Correlated subqueries with aggregates (max, min, sum, count, avg)
- Pattern: "For each X, find aggregate of Y where Y relates to X"
- Examples: max value per user per day, latest price per ticker, totals per category

**Configuration**: No separate flag. The rewrite is performed by the default-active
algebra optimizer (`EnableAlgebraOptimizer: true`); there is no
`EnableConditionalAggregateRewriting` option (removed 2026-05).

### 15. Interned-Pointer Index Keys & L85 Cache Removal (COMPLETE - May 2026)
**Status**: ✅ In-memory indices and entity-dedup sets key on interned pointers; the `Identity.l85` cache was removed

**What changed**:
- The in-memory matcher's entity/EA indices and the BadgerMatcher entity-dedup set
  keyed on `Identity.L85()` strings (`E.L85()`, `E.L85()+"|"+A.String()`). Since
  identities and keywords are interned (pointer equality ⟺ value equality), these
  now key on the interned pointers directly — `map[datalog.Identity][]int` and
  `map[eaIndexKey][]int` (an `{Identity, Keyword}` pair). No Base85 encode, no
  per-key string concatenation.
- With no hot `L85()` callers left (only `String()`'s fallback and export), the
  lazily-cached `l85` field on `identity` was removed; `Identity.L85()` now computes
  on demand. This also fixed a data race — the lazy cache write mutated
  globally-interned, shared identities without synchronization
  (`BUG_IDENTITY_L85_LAZY_RACE`).

**Benchmark** (`datalog/executor/index_key_bench_test.go`; (E,A) index build+lookup,
1000 entities × 8 attrs; the string variant uses precomputed L85 to model the prior
cache fairly):

| key | ns/op | B/op | allocs/op |
|-----|-------|------|-----------|
| string (old) | 644,085 | 1,619,084 | 24,033 |
| interned pointer (new) | 240,159 | 851,076 | 8,033 |

~2.7× faster, ~47% less memory, and 1/3 the allocations on the index path — before
counting the removed race and the per-identity L85 string.

**Details**: See `docs/bugs/resolved/BUG_IDENTITY_L85_LAZY_RACE.md`

### 16. ATEV Index — O(1) Attribute High-Water Mark (COMPLETE - May 2026)
**Status**: ✅ New `[A][Tx↓][E][V]` index; `MaxElementIDForAttribute` and every
`Cache.IsAttributeFresh` call are now a single forward seek

**What changed**:
- Added an 8th index, **ATEV** (`[prefix][A][Tx↓][E][type][V][AfterRef?][Op]`),
  positioned so that under a `[A]` prefix the entries sort by `Tx` descending
  across all entities for that attribute. The first entry is therefore the global
  max-Tx datom for the attribute.
- `BadgerStore.MaxElementIDForAttribute` is now a single `Seek([ATEV][A])` plus
  a Tx decode — O(1), where previously it forward-scanned every datom for the
  attribute on AEVT (O(datoms-for-A)) while documentation claimed "O(1) reverse seek."
- **Cache freshness inherits this directly.** `Cache.IsAttributeFresh` always calls
  `MaxElementIDForAttribute` to compute the current store max for comparison
  against `attrVersions`, so every A-bound cached query path (the gate in front
  of attribute-resolved CRDT lookups) is now constant-time, not linear in the
  attribute's datom count.
- `BadgerMatcher.chooseIndex` routes A-bound + Tx-bound + V-unbound patterns to
  ATEV. `hash_join_matcher.chooseIndexForValues` and `simple_batch_scanner.buildKey`
  both learned the ATEV layout so joined/batched scans through ATEV produce a
  tight `[A][Tx↓][E]` prefix instead of degrading to a full-attribute scan.

**Read-side measurement** (`atev_index_bench_test.go`,
`BenchmarkMaxElementIDForAttribute_ATEVSeek_vs_AEVTScan`, Apple M5, Badger v4):

| N (datoms-for-A) | ATEV seek | AEVT scan | Speedup |
|------------------|-----------|-----------|---------|
| 10               | 876 ns    | 1,943 ns  | 2.2×    |
| 100              | 995 ns    | 8,505 ns  | 8.5×    |
| 1,000            | 1,045 ns  | 70 µs     | 67×     |
| 10,000           | 1,121 ns  | 622 µs    | **555×**|

ATEV is flat across four orders of magnitude (876 ns → 1,121 ns — true O(1)); the
AEVT scan it replaces grows linearly (10× per decade). At narrative-generators-scale
(`:task/status` across thousands of tasks), every freshness gate that fronted an
A-bound cache lookup was previously paying tens to hundreds of microseconds and
now pays ~1 µs.

**Write-side measurement** (`BenchmarkAssert_WriteCost`, same hardware):

| Batch size | ns/op  | Per-datom |
|------------|--------|-----------|
| 100        | 627 µs | ~6.3 µs   |
| 1,000      | 6.7 ms | ~6.7 µs   |

ATEV adds 1 of 8 indices, so its share of the per-datom write cost is roughly
~0.8 µs (~14% more than a 7-index baseline) — an estimate from the index ratio,
not a direct 7-vs-8 measurement. The marginal cost is dominated by Badger's
`Set` and the per-index key construction, not value encoding (which is amortized
once per datom across all indices via `EncodeKeyWithValueBytes`).

**Crossover** (writes to amortize one saved freshness check at N=10K): saved
~621 µs ÷ added ~0.8 µs/datom ≈ 775 datoms. Any commit smaller than that into a
10K-cardinality attribute is "paid for" by a single subsequent freshness check
that would have scanned. Read-mostly workloads (the narrative-generators shape)
win convincingly; bulk-import-then-never-query workloads pay the ~14% tax with
no read recovery.

**Migration**: None required. ATEV is populated by every commit; the freshness
seek finds nothing (zero ElementID, treated as "no data") on attributes that have
no writes since the index was added.

**Defensive-code cleanup that came with it**: `extractElementIDFromKey` now
panics on an unknown index type (it previously returned `ElementID{}` silently,
which is how a missing ATEV case slipped past the first test run). Five
historical `case` blocks that branched on `tx.(uint64)` for legacy Lamport-only
Tx values were removed along with `NewTxFromUint`/`toStorageTx` — Tx is always
an `ElementID` now, and the previously dead branch is gone.

### 17. Relation-Input Parallel Iteration — Worker Pool + Workspace Reuse (COMPLETE - May 2026)
**Status**: ✅ `executeRealizedWithRelationInputIterationParallel` reshaped from
per-tuple-goroutine + semaphore to fixed worker pool, with per-query state
hoisted out of the per-tuple inner loop

**Why**: profiling `:in $ [[?x ?y ?z] ...]`-shape queries against persistent
storage with hundreds of input tuples showed `runtime.usleep`, `runtime.lock2`,
`pthread_cond_wait`/`signal`, and `runtime.newstack`/`morestack` dominating
CPU (~70% in scheduler + GC, ~25–30% in application code). Same pattern as
the historical `PrefetchValues=true` thrash noted in CLAUDE.md: a goroutine
spawned per small unit of work, with a semaphore funnelling them through
a worker-count gate.

**What changed** (layered, see PR for individual commits):

1. **Per-tuple goroutine spawn → fixed worker pool.** `numWorkers` long-lived
   goroutines consume from a buffered `jobs` channel. Producer streams from
   `iterationRelation.Iterator()` directly into `jobs` (no upfront
   materialization). Workers write to per-worker slots in `workerResults[wIdx]`
   — no inter-worker synchronization on output. Goroutine count per call:
   `len(tuples)` → `numWorkers`.

2. **Iterator-workspace-reuse race fixed.** When
   `iterationRelation.RequiresCopy()` is true (the default for
   `StreamingRelation`, used by storage-backed sources that reuse tuple
   workspace), the producer copies before sending. Without this, workers
   raced producer workspace overwrites; `go test -race` confirms the race
   directly on the unfixed code. The bug was invisible to tests that only
   use `MaterializedRelation` inputs (`RequiresCopy() == false`).

3. **`forkContext` hoisted from per-tuple to per-worker.** Context state
   safety is a parallel-workers concern, not sequential-within-one-worker;
   `BindQueryInputs` eagerly consumes inputs, production code never sets
   `metadata`, and `ScanRegistry` is safe to share within one worker.

4. **C1: hoist `QueryExecutor` and `modifiedQuery`.** Both are deterministic
   from the plan and never vary across per-tuple calls. Computed once per
   worker.

5. **C2: pre-built bound relation with workspace-reusable tuple slots.**
   Replace per-call `BindQueryInputs` (N `ScalarInput` `MaterializedRelation`
   allocations + N-1 join intermediates) with a single pre-built bound
   relation whose `boundTuple` slots are mutated per call. Direct struct
   construction bypasses `NewMaterializedRelation`'s `deduplicateTuples`
   allocations on a known-singleton tuple. Mirrors the
   `BuildTupleInternedInto` / `it.workspace` pattern used by
   `matcher_iterator_*.go` and `hash_join_matcher.go`. Falls back to per-call
   `BindQueryInputs` for shapes the fast path can't handle (`CollectionInput`,
   etc.).

6. **C3: API consolidation.** Caller code reduces from a four-step
   orchestration (builder + session + prepared + Update + Run) to:

   ```go
   prepared := e.prepareIteration(plan, relationInput, inputRelations, iterationIndex)
   for tuple := range tuples {
       result, err := prepared.Run(ctx, tuple)
   }
   ```

   `preparedIteration` owns the fallback session as a private field,
   allocated only when the fast path can't activate.

**Measurement** (`BenchmarkRelationInputParallel`, `datalog/executor/ohlc_subquery_performance_test.go`,
`n=10`, Apple M5 MacBook Air with 4 P-cores + 6 E-cores):

| Workers     | Baseline | After  | Delta  |
|-------------|---------:|-------:|-------:|
| Sequential  | 522.8m   | 391.3m | -25.2% |
| Parallel-2  | 327.2m   | 270.9m | -17.2% |
| Parallel-4  | 271.3m   | 228.0m | -16.0% |
| Parallel-8  | 258.7m   | 231.3m | -10.6% |

**Hardware interpretation**: above 4 workers on a 4P+6E core machine the
measurement is dominated by P-core over-subscription. The 8-worker case
runs 4 workers on slower E-cores; the 16/32-worker cases are pure
scheduler-thrash measurements. The 10-25% win range above represents the
configurations that fit on the actual performance cores.

**Allocations**: 11.27M → 11.12M (-1.4%). **Memory**: 791.6Mi → 785.3Mi
(-0.86%). The wall-time win is much larger than the allocation delta
because the bulk of the savings is in `QueryExecutor` and `modifiedQuery`
reuse — pointer + struct-copy operations whose CPU cost is dominated by
GC scan time, not just allocator throughput.

**Tests added** (`datalog/executor/relation_input_parallel_correctness_test.go`,
7 tests). All pin invariants the refactor must preserve:
- Sorted-list multiset equality with sequential
- Matcher-error propagation (first error AND after partial success)
- Deferred iterator-error propagation through the parallel path
- No goroutine leaks across many invocations
- Concurrent invocation correctness (16 outer goroutines × 20 iters each,
  multiset-checked per goroutine)
- Workspace-reuse iterator (`StreamingRelation` source): reproduces the
  race; passes after the producer-copy fix; `-race` detector confirms the
  race directly on the unfixed code

All seven pass against the final state on this branch. `go test -count=1
-race ./datalog/executor/` clean. Full `go test -count=1 ./...` green.

**Adjacent tooling**: `.claude/hooks/validate-bash.sh` gains a rule
blocking `rm` / `rmdir` / `unlink` in Bash commands (word-boundary
match; `git rm` passes).

### 18. Typed Aggregation Keys via `TupleKeyMap` (COMPLETE - July 2026)
**Status**: ✅ Batch and streaming grouped aggregation use typed tuple keys

**Problem**: Both aggregation paths formed group identity by formatting each
value and joining the strings with `|`. The representation was expensive and
not injective:

- `(int64(5), "x")` and `("5", "x")` produced the same key.
- `("a|b", "c")` and `("a", "b|c")` produced the same key.
- Distinct groups silently merged and their aggregate values combined.

**Change**:

- Replaced both string-key maps with the existing `TupleKeyMap`, reusing the
  same typed hashing and `ValuesEqual` semantics as joins and deduplication.
- The map value is the aggregation state for that typed key:
  `batchAggregateGroup` for batch mode and `streamingAggregateGroup` for
  streaming mode.
- A parallel first-seen-order slice provides result traversal because
  `TupleKeyMap` is optimized for lookup rather than enumeration.
- Added red/green tests covering delimiter shifts and int, bool, and float
  values paired with their string renderings in both aggregation modes.

**Measurement** (`BenchmarkGroupedAggregationKeying`, 10,000 rows, 100
groups keyed by two symbols, `benchtime=500ms`, `count=10`, darwin/arm64):

| Mode | Time before | Time after | Delta | Bytes delta | Allocs delta |
|------|------------:|-----------:|------:|------------:|-------------:|
| Batch | 1,369.6 µs | 681.4 µs | **−50.25%** | −14.43% | −70.02% |
| Streaming | 926.0 µs | 512.5 µs | **−44.66%** | −35.60% | −72.48% |
| **Geomean** | 1,126 µs | 590.9 µs | **−47.53%** | **−25.77%** | **−71.28%** |

Every timing comparison is significant at `p=0.000`, `n=10`. The full
`go test -count=1 ./...` suite passes.

**Files**:

- `datalog/executor/aggregation.go`
- `datalog/executor/aggregation_key_test.go`
- `datalog/executor/aggregation_test.go`

### 19. Single-Lookup Dedup Insertion (COMPLETE - July 2026)
**Status**: ✅ All add-if-absent dedup paths use `TupleKeyMap.PutIfAbsent`

**Problem**: Eight production paths implemented set insertion as
`if !seen.Exists(key) { seen.Put(key, value) }`. For each new key this repeats
the map lookup and collision-bucket comparison even though `TupleKeyMap`
already provides a single-walk `PutIfAbsent` operation.

**Change**:

- Replaced the eight add-if-absent sequences in materialized and streaming
  deduplication, unions, relation operations, subquery input collection, and
  symmetric hash joins.
- Kept membership-only `Exists` calls in semi/anti joins and NOT filtering.
- Added a direct contract test proving first-insert/repeated-insert reporting
  and preservation of the original map value.
- Set semantics are unchanged; this does not skip deduplication or introduce
  relational property propagation.

**Measurement** (`BenchmarkDedupInsertionPaths`, 10,000 two-position
Identity/string tuples, `benchtime=500ms`, `count=10`, darwin/arm64):

| Workload | Mode | Time before | Time after | Delta |
|----------|------|------------:|-----------:|------:|
| Unique-heavy | Materialized | 504.3 µs | 460.2 µs | **−8.74%** |
| Unique-heavy | Streaming | 492.1 µs | 447.9 µs | **−8.99%** |
| Duplicate-heavy | Materialized | 260.6 µs | 246.6 µs | **−5.36%** |
| Duplicate-heavy | Streaming | 270.3 µs | 253.5 µs | **−6.23%** |
| **Geomean** | | 363.6 µs | 336.9 µs | **−7.34%** |

Memory and allocation counts are unchanged, as expected for removal of
redundant lookup work without a storage-layout change. Every timing comparison
is significant at `p≤0.019`, `n=10`. The full `go test -count=1 ./...` suite
passes.

**Files**:

- `datalog/executor/dedup_put_if_absent_test.go`
- `datalog/executor/iterator_composition.go`
- `datalog/executor/relation.go`
- `datalog/executor/relation_ops.go`
- `datalog/executor/subquery.go`
- `datalog/executor/union_relation.go`
- `datalog/executor/symmetric_hash_join.go`

### 20. Bounded Top-N Finalization (COMPLETE - July 2026)
**Status**: ✅ Ordered limits use a bounded heap for structurally safe shapes

**Problem**: `ORDER BY ... :limit N` materialized every result, sorted all M
rows, then retained N. The cost remained O(M log M) time and O(M) memory even
for latest-1 queries.

**Change**:

- Added `TopNRelation`, a worst-first heap retaining at most N tuples while it
  drains the source, followed by a final sort of those N tuples.
- Shared one tuple comparator between full sort and Top-N, preserving
  ascending/descending and multi-key semantics.
- Preserved workspace-copy requirements and deferred iterator errors.
- Applied Top-N at the existing global finalization boundary, after aggregation
  and RelationInput union and before pull rendering.
- Kept full sort for non-projected sort keys. Their required
  sort→deduplicating-projection→limit sequence is not equivalent to limiting
  before projection.
- This operator still scans every source row. Index-order pushdown is a
  separate post-property-propagation optimization.

**Measurement** (`BenchmarkOrderedLimit`, 10K/100K rows, N=1/10/100,
materialized/streaming, `benchtime=300ms`, `count=10`, darwin/arm64):

| Rows | N | Mode | Time before | Time after | Delta |
|-----:|--:|------|------------:|-----------:|------:|
| 10,000 | 1 | Materialized | 2.233 ms | 62.91 µs | **−97.18%** |
| 10,000 | 1 | Streaming | 2.947 ms | 75.06 µs | **−97.45%** |
| 100,000 | 1 | Materialized | 26.10 ms | 631.6 µs | **−97.58%** |
| 100,000 | 1 | Streaming | 34.16 ms | 776.6 µs | **−97.73%** |
| **Geomean (12 cases)** | | | **9.023 ms** | **259.8 µs** | **−97.12%** |

Geomean memory falls from 10.79 MiB to 3.985 KiB (**−99.96%**) and
allocations from 55.06K to 77.84 (**−99.86%**). Every comparison is
significant at `p=0.000`, `n=10`. The full `go test -count=1 ./...` suite
passes.

**Files**:

- `datalog/executor/top_n.go`
- `datalog/executor/top_n_test.go`
- `datalog/executor/top_n_benchmark_test.go`
- `datalog/executor/executor.go`
- `datalog/executor/executor_utils.go`

### 21. One-Pass Same-Entity Attribute Bundles (COMPLETE - July 2026)
**Status**: ✅ Contiguous fusable attributes attach in one relation traversal

**Problem**: Existing attribute-fetch fusion handled one
`[?e :constant ?fresh]` pattern at a time. K attributes repeated relation
iteration, progressive tuple widening, materialization, and deduplication K
times.

**Profile evidence before the change** (1,000 entities, K=6):

- `tryFuseAttributeFetch`: 16.5% cumulative CPU.
- 82.6% cumulative and 30.9% flat allocated space.
- Per-attribute tuple creation and result growth: 6.29 GiB over the profile.
- Repeated materialization/deduplication: 7.34 GiB cumulative.

**Change**:

- Recognize only contiguous patterns that already satisfy every existing
  fusion gate: default source, no transaction binding, same entity symbol,
  fresh outputs, CardinalityOne, and latest-state mode.
- Traverse the input relation once, perform the same K cache-backed
  `LookupAttribute` calls, and allocate one final-width tuple.
- Preserve per-attribute `pattern/fused-fetch` events and their sequential
  input/output counts.
- Stop bundling at predicates, expressions, cardinality-many attributes, named
  sources, or any other non-fusable clause. Nothing is reordered.
- Added no matcher API, configuration flag, adaptive threshold, or alternate
  storage scan.

**Measurement** (`BenchmarkAttrFetch`, N=100/1000, K=1/3/6,
`benchtime=500ms`, `count=10`, darwin/arm64):

| Entities | K | Time before | Time after | Time delta | Memory delta | Allocs delta |
|---------:|--:|------------:|-----------:|-----------:|-------------:|-------------:|
| 100 | 1 | 67.53 µs | 65.61 µs | −2.85% | ~ | +0.07% |
| 100 | 3 | 101.0 µs | 86.50 µs | **−14.33%** | −32.99% | −19.37% |
| 100 | 6 | 152.4 µs | 117.2 µs | **−23.09%** | −49.99% | −32.03% |
| 1,000 | 1 | 517.3 µs | 528.8 µs | ~ | +0.02% | +0.01% |
| 1,000 | 3 | 859.4 µs | 729.9 µs | **−15.06%** | −37.28% | −21.91% |
| 1,000 | 6 | 1.352 ms | 1.016 ms | **−24.83%** | −56.50% | −36.46% |
| **Geomean** | | **292.4 µs** | **252.8 µs** | **−13.54%** | **−32.88%** | **−19.50%** |

K=1 remains effectively unchanged, demonstrating that the win comes from
removing repeated passes rather than changing lookup semantics. The full
`go test -count=1 ./...` suite passes.

**Files**:

- `datalog/executor/query_executor.go`
- `datalog/executor/attribute_fetch_bundle_test.go`
- `datalog/storage/same_entity_fusion_test.go`
- `datalog/storage/attr_fetch_bench_test.go`

### 21a. Same-Entity Constant Constraint Fusion (COMPLETE - July 2026)
**Status**: ✅ CardinalityOne literal constraints filter by cache-backed lookup

**Profile evidence** (`BenchmarkComplexQueryCheckpoint`, corrected logical
optimizer baseline):

- `executeSubquery`: 53.55% cumulative CPU.
- `HashJoinWithOptions`: 88.62% cumulative allocated space and 92.02% cumulative
  allocation count.
- The query executes four subqueries once, builds five fallback caches once, and
  contains five same-entity constant constraints.

**Change**:

- Extend the existing contiguous same-entity fusion pass from fresh variable
  bindings to literal value constraints.
- Resolve each proven CardinalityOne attribute through `LookupAttribute`, compare
  with `datalog.ValuesEqual`, and discard non-matching tuples in the same
  traversal.
- Preserve existing latest-mode/cardinality/source/transaction gates. History,
  as-of, schema-unknown, CardinalityMany, and CardinalityVector patterns retain
  normal match semantics.
- Emit `pattern/fused-constraint` independently from `pattern/fused-fetch`.

**Focused measurement** (`BenchmarkConstantConstraintFusion`,
`benchtime=500ms`, `count=10`, darwin/arm64):

| Entities | Time before | Time after | Time delta | Memory delta | Allocation delta |
|---------:|------------:|-----------:|-----------:|-------------:|-----------------:|
| 1,000 | 458.4 µs | 358.1 µs | **−21.9%** | **−35.3%** | **−42.3%** |
| 10,000 | 4.991 ms | 3.831 ms | **−23.2%** | **−38.8%** | **−43.4%** |

**Complex-query checkpoint** (`count=10`, darwin/arm64):

| Metric | Before | After | Delta |
|--------|-------:|------:|------:|
| Median time | 46.89 ms | 41.69 ms | **−11.1%** |
| Memory | 84.04 MiB | 65.72 MiB | **−21.8%** |
| Allocations | 1.043M | 800.7K | **−23.2%** |

**Files**:

- `datalog/executor/query_executor.go`
- `datalog/storage/same_entity_fusion_test.go`
- `datalog/storage/constant_constraint_fusion_benchmark_test.go`
- `datalog/storage/optimization_matrix_test.go`

### 21b. Correlated OR Outer Replacement (COMPLETE - July 2026)
**Status**: ✅ OR/fallback results replace the outer groups they already contain

**Profile evidence after constant-constraint fusion**:

- `HashJoinWithOptions`: 62.28% cumulative CPU, 88.21% cumulative allocated
  space, and 92.25% cumulative allocation count.
- The stack was rooted under `OrFallbackIterator`: the fallback relation emitted
  outer symbols plus branch outputs, then QueryExecutor appended it beside the
  original outer relation and collapsed them through another natural join.
- The complex query performed this redundant join five times.

**Change**:

- Record exactly which input relation groups are incorporated into each
  correlated OR/fallback outer relation.
- Replace those groups with the OR/fallback result before collapsing unrelated
  groups. Uncorrelated union retains append-and-join behavior.
- Emit `or/outer-replaced` with consumed and remaining group counts.
- Propagate outer iterator, branch iterator, cache-build, and close failures;
  a failed preferred branch cannot be interpreted as no match and fall through
  to a default.

**Complex-query checkpoint** (`count=10`, darwin/arm64):

| Metric | Before | After | Delta |
|--------|-------:|------:|------:|
| Median time | 42.81 ms | 37.96 ms | **−11.3%** |
| Memory | 64.23 MiB | 58.89 MiB | **−8.3%** |
| Allocations | 793.1K | 709.4K | **−10.6%** |

Combined with constant-constraint fusion, the corrected logical-optimizer
baseline improves from 46.89 ms to 37.96 ms (**−19.0%**), 84.04 MiB to
58.89 MiB (**−29.9%**), and 1.043M to 709.4K allocations (**−32.0%**).

**Lazy outer-selection follow-up**: selecting a single outer relation no longer
requests eager materialization. Four of five fallback chains remain streaming;
one still materializes for join-key narrowing. Full-result measurement is
performance-neutral: 37.96 → 37.78 ms median, 58.89 → 58.86 MiB, and 709.4K →
709.2K allocations. This is retained as streaming groundwork, not a performance
claim.

**Files**:

- `datalog/executor/query_executor.go`
- `datalog/executor/or_fallback_relation.go`
- `datalog/executor/or_outer_replacement_test.go`
- `datalog/executor/or_outer_selection_test.go`
- `datalog/executor/or_fallback_cache_test.go`
- `datalog/storage/optimization_matrix_test.go`
- `datalog/storage/algebra_getelse_product_test.go`

### 21c. Correlated-Subquery Product Streaming (COMPLETE - July 2026)
**Status**: ✅ Single-use products stream into typed combination extraction

**Audit finding**: correlated subqueries marked source groups cacheable for later
outer-query reuse, formed a product, materialized/deduplicated that complete
product, then immediately traversed and deduplicated it again by the requested
input symbols.

**Change**:

- Keep source relations lazy and replayable through their existing caches.
- Stream the combined product exactly once into `getUniqueInputCombinations`.
- Retain typed projected-key deduplication, iterator/close error propagation, and
  outer relation replay.
- Emit `subquery/input-combinations` with group count, product shape,
  realization decision, and distinct combination count.

**Focused measurement** (`BenchmarkSubqueryInputCombinationExtraction`,
set-valid unique tuples with repeated projected input values,
`benchtime=500ms`, `count=10`, darwin/arm64):

| Product tuples | Time before | Time after | Time delta | Memory delta | Allocation delta |
|---------------:|------------:|-----------:|-----------:|-------------:|-----------------:|
| 10,000 | 2.529 ms | 1.412 ms | **−44.2%** | **−37.0%** | **−14.3%** |
| 100,000 | 24.65 ms | 14.89 ms | **−39.6%** | **−38.7%** | **−14.3%** |

The production-shaped complex checkpoint has no multi-group correlated input
product and remains unchanged: 37.78 → 37.98 ms median with stable memory and
allocations.

**Correctness fixes completed during the materialization audit**:

- Incomplete early-close caches fail loudly and replay that failure.
- Predicate evaluation and hash-join close errors propagate.
- Streaming transforms consume relation-owned iterators, preserving cache and
  single-use contracts.
- Unknown-size scalar, tuple, relation, and collection inputs bind correctly.
- `StreamingRelation.Get` performs actual realization for random access.

**Files**:

- `datalog/executor/query_executor.go`
- `datalog/executor/subquery_input_product_test.go`
- `datalog/executor/subquery_input_product_benchmark_test.go`
- `datalog/executor/relation.go`
- `datalog/executor/iterator_composition.go`
- `datalog/executor/join.go`
- `datalog/executor/executor_utils.go`
- `datalog/executor/iterator_contract_hardening_test.go`
- `datalog/executor/bind_query_inputs_streaming_test.go`

### 21d. Relation Set-Invariant Construction (COMPLETE - July 2026)
**Status**: ✅ Set-preserving operators do not repeat deduplication

**Relational contract**: every `Relation` is a set. Physical iterators may need
to establish set semantics before returning a Relation, but a set-preserving
operator does not need to establish them again.

**Change**:

- Add an internal proven-set materialized constructor that wraps already-valid,
  duplicate-free tuples without traversing them.
- Keep value-domain validation at raw/public tuple-entry boundaries; derived
  operators trust their input Relation.
- Remove exact second dedup passes from join-key extraction, uncorrelated union,
  union/fallback realization, and aggregation output.
- Publish grouped-aggregation keys.
- Use proven-set construction for selection, deterministic extension, keyed
  projection, sorting, Top-N, limits, phase realization, lazy replay, and
  same-entity fusion.
- Define `Materialize` as ensuring replayability rather than mandatory eager
  conversion. `LazySeqRelation` is already replayable and returns itself;
  relational transforms remain lazy over its shared sequence.

**Constructor measurement** (`BenchmarkProvenSetConstruction`,
`benchtime=300ms`, darwin/arm64):

| Tuples | Deduplicating constructor | Proven-set constructor |
|-------:|--------------------------:|-----------------------:|
| 10,000 | 462.2 µs, 1.38 MiB, 10.0K allocs | 56.7 ns, 208 B, 3 allocs |
| 100,000 | 4.525 ms, 12.45 MiB, 100.3K allocs | 56.6 ns, 208 B, 3 allocs |

The benchmark stores constructed Relations in a package-level sink so escape
analysis cannot remove the immutable property clone. Proven-set construction
remains O(1); the fixed allocations copy relation and candidate-key property
slices.

**Complex-query checkpoint** (`count=10`, darwin/arm64):

| Metric | Before | After | Delta |
|--------|-------:|------:|------:|
| Median time | 37.98 ms | 36.04 ms | **−5.1%** |
| Memory | 58.85 MiB | 49.65 MiB | **−15.6%** |
| Allocations | 709.2K | 648.5K | **−8.6%** |

**Files**:

- `datalog/executor/relation.go`
- `datalog/executor/relation_properties.go`
- `datalog/executor/aggregation.go`
- `datalog/executor/or_fallback_relation.go`
- `datalog/executor/relation_ops.go`
- `datalog/executor/union_relation.go`
- `datalog/executor/lazy_seq_relation.go`
- `datalog/executor/relation_set_construction_benchmark_test.go`
- set/property/aggregation/LazySeq regression tests

### 22. Typed Relation Property Propagation (FOUNDATION - July 2026)
**Status**: ⚠️ Core contract active; derivation and consumption coverage incomplete

**Architecture**:

- Added `Relation.Properties()` as a required interface method.
- Properties use Datalog vocabulary: `[]query.OrderByClause` for guaranteed
  physical ordering and `[][]query.Symbol` for candidate keys.
- The planner and algebra optimizer remain Datalog-in/Datalog-out; no property
  metadata map or synthetic query clause was added.
- Constructors copy external properties. Interface callers treat returned
  values as immutable, matching the Relation contract and avoiding defensive
  copies in the hot path.

**Propagation**:

- Proven CardinalityOne AETV and validated AVET scans establish E ordering and
  entity uniqueness.
- Filters, limits, materialization, lazy wrapping, and same-entity attachment
  preserve properties.
- Projection retains valid ordering prefixes and fully retained keys.
- Sort establishes ordering; grouped aggregation establishes its group key.
- Relation bindings apply positional ρ-renaming to ordering and candidate-key
  symbols instead of discarding those proofs.
- Fresh expression outputs preserve guarantees.
- Joins, unions, products, and fallback relations conservatively clear them.
- Streaming projection skips deduplication when a candidate key survives.

**Complex-query checkpoint** (`count=10`, darwin/arm64):

| Metric | Before | After | Delta |
|--------|-------:|------:|------:|
| Time | 47.88 ms | 48.48 ms | statistically unchanged (`p=0.247`) |
| Memory | 87.57 MiB | 82.80 MiB | **−5.45%** |
| Allocations | 1.118M | 1.088M | **−2.71%** |

An initial implementation defensively copied properties at every interface
read and regressed time by about 2%. Profiling found no application hotspot but
the regression reproduced. Moving immutability into the explicit interface
contract removed that cost while preserving the memory/allocation win.

Ordering is now consumed when the final relation already satisfies the Datalog
requirement. Order-aware index selection for additional shapes remains separate.

**Post-fusion relation-binding checkpoint** (`count=10`, darwin/arm64):

| Metric | Before | After | Delta |
|--------|-------:|------:|------:|
| Median time | 41.69 ms | 42.81 ms | no supported latency claim |
| Memory | 65.72 MiB | 64.23 MiB | **−2.3%** |
| Allocations | 800.7K | 793.1K | **−0.9%** |

At this intermediate checkpoint the complex query selected two unique hash
builds after grouped-aggregate keys survived `RelationBinding`. The later
correlated-OR replacement removes those joins entirely; ρ-renaming remains
available to other downstream consumers. This is proof propagation with a small
resource reduction, not a wall-time optimization.

**Remaining work**:

- Prove property preservation for specific join and OR/fallback shapes.
- Establish properties for additional storage index and binding strategies.
- Add order-aware index selection only for CRDT-safe proven shapes.
- Measure every added derivation against the complex-query checkpoint.

**Files**:

- `datalog/executor/relation_properties.go`
- `datalog/executor/relation_properties_test.go`
- `datalog/executor/subquery.go`
- `datalog/executor/subquery_binding_properties_test.go`
- `datalog/storage/relation_properties.go`
- `datalog/storage/relation_properties_test.go`

### 23. Existing-Order Scan Termination (6a COMPLETE - July 2026)
**Status**: ✅ Ordered limits consume proven relation ordering

When `Relation.Properties().Ordering` satisfies the effective Datalog
`:order-by`, finalization now skips full sort and bounded Top-N. It leaves the
relation streaming and applies `LimitRelation` after any required
projection/deduplication, so closing after N results closes the storage iterator.

This slice changes no planner behavior and no index selection. Unsatisfied
ordering retains the existing bounded Top-N path.

**Measurement** (`BenchmarkIndexOrderedLimit`, 10,000 AETV-ordered
CardinalityOne entities, `benchtime=500ms`, `count=10`, darwin/arm64):

| Order | N | Time before | Time after | Delta | Datoms before | Datoms after |
|-------|--:|------------:|-----------:|------:|--------------:|-------------:|
| E asc | 1 | 2.057 ms | 7.295 µs | **−99.65%** | 10,000 | 1 |
| E asc | 10 | 2.065 ms | 10.95 µs | **−99.47%** | 10,000 | 10 |
| E asc | 100 | 2.077 ms | 38.93 µs | **−98.13%** | 10,000 | 100 |
| E desc | 1/10/100 | full-scan control | full-scan control | — | 10,000 | 10,000 |

For satisfied ascending order, memory falls 97.0–99.4% and allocations
98.0–99.6%. Descending allocation counts are identical before/after; timing
samples are noisy despite the unchanged execution path.

**Files**:

- `datalog/executor/executor.go`
- `datalog/storage/index_order_limit_test.go`
- `datalog/storage/index_order_limit_benchmark_test.go`

### 24. History ATEV Order-Aware Matching (6b FIRST SHAPE - July 2026)
**Status**: ✅ Datalog matcher fragments select ATEV for a proven history shape

`PatternMatcher.Match` now accepts a Datalog `query.Query` fragment containing
exactly one `DataPattern`. The planner forwards `OrderBy` and `Limit` only for a
single terminal pattern without aggregation or relation inputs. Wrappers and
custom sources forward or ignore those requirements without side metadata.

Storage's first order-aware choice is deliberately narrow:

- History mode only
- Constant A
- Tx variable ordered descending
- Optional E variable ordered ascending as the second key
- Non-nil limit

This maps exactly to ATEV `[A constant][Tx↓][E][V]`. Latest/as-of modes decline
because Tx-primary ATEV cannot perform E/A-grouped current-state CRDT resolution.

**Measurement** (`BenchmarkHistoryIndexOrderedLimit`, 10,000 raw datoms,
`benchtime=500ms`, `count=10`, darwin/arm64):

| N | Time before | Time after | Delta | Memory delta | Allocs delta | Scans before | Scans after |
|--:|------------:|-----------:|------:|-------------:|-------------:|-------------:|------------:|
| 1 | 2.752 ms | 13.48 µs | **−99.51%** | −99.44% | −99.31% | 10,000 | 1 |
| 10 | 2.842 ms | 17.91 µs | **−99.37%** | −99.30% | −99.14% | 10,000 | 10 |
| 100 | 2.808 ms | 55.28 µs | **−98.03%** | −97.89% | −97.71% | 10,000 | 100 |
| **Geomean** | **2.800 ms** | **23.72 µs** | **−99.15%** | **−99.07%** | **−98.89%** | | |

**Files**:

- `datalog/query/types.go`
- `datalog/executor/interfaces.go`
- `datalog/planner/planner_clause_based.go`
- `datalog/storage/matcher_relations.go`
- `datalog/storage/history_index_order_limit_test.go`
- `datalog/storage/history_index_order_limit_benchmark_test.go`

---

## Complex Query Checkpoint (July 2026)

`BenchmarkComplexQueryCheckpoint` promotes the existing production-shaped
optimization-matrix query into a repeatable steady-state benchmark. Its fixture
contains 75 scenarios × 100 tasks with a CardinalityOne schema, and the query
exercises:

- Phase planning and multi-relation joins
- Same-entity attribute bundles
- Correlated and nested subqueries
- Conditional aggregation and typed group keys
- `get-else`, `or-default`, and expressions
- Multi-key ordering and bounded `:limit 25`
- Public query, parse-cache, plan-cache, and result-collection boundaries

**Checkpoint** (`benchtime=1s`, `count=10`, darwin/arm64):

| Metric | Result |
|--------|-------:|
| Time | **47.88 ms/op ±2%** |
| Memory | **87.57 MiB/op** |
| Allocations | **1.118M/op** |

**CPU profile**: scheduler signaling is the largest flat runtime cost (15.4%);
the intern `HashTrieMap.Load` path is 13.2% cumulative. No single application
function dominates CPU.

**Allocation profile**: `HashJoinWithOptions` and
`OrFallbackIterator.nextShortCircuit` each cover about 94% cumulatively on
overlapping stacks. Direct allocation leaders are:

- `TupleKeyMap.PutIfAbsent`: 16.5%
- `NewTupleKeyMapWithCapacity`: 16.5%
- `TupleKeyMap.Put`: 11.3%
- `NewTupleKey`: 3.7%

This is the checkpoint for the next architecture step: statically provable
relation properties. Any claimed sort, deduplication, or join elimination must
improve this benchmark without changing its result.

### Nested Datalog lowering checkpoint (July 13, 2026)

After restoring the logical `Query → Query` optimizer contract and lowering
algebra into nested Datalog before physical planning, the same benchmark produced
(`benchtime=1s`, `count=10`, darwin/arm64):

| Metric | Result |
|--------|-------:|
| Time | **46.89 ms/op median** (44.26–52.90 ms) |
| Memory | **84.04 MiB/op** |
| Allocations | **1.043M/op** |

Against the last documented property checkpoint (48.90 ms, 82.80 MiB, 1.088M
allocations), this does not establish a statistically controlled latency change;
memory is approximately 1.5% higher while allocations are approximately 4.2%
lower. The architectural result is restored logical/physical separation with no
evidence of a general-query regression.

### Materialized join-project experiment (July 12, 2026)

Backward liveness can insert `Project` above narrowed inner-join children and
lower it as a relation-binding subquery, but `EnableJoinProjectInsertion`
remains default-off. On a focused 2,000-entity scan → selective predicate →
project → join query, with algebra optimization enabled in both modes
(`benchtime=500ms`, `count=10`, darwin/arm64):

| Metric | Flat | Materialized project | Delta |
|--------|-----:|---------------------:|------:|
| Median time | 0.913 ms/op | 1.464 ms/op | **+60.4%** |
| Memory | 1.627 MiB/op | 2.614 MiB/op | **+60.7%** |
| Allocations | 20.38K/op | 36.64K/op | **+79.8%** |

The narrower tuple does not repay nested relation-subquery execution. The
transform and benchmark remain as a correctness/proof checkpoint, but this
rewrite must stay inactive until the subquery path can consume the logical
projection without adding this physical cost.

**Files**: `datalog/storage/optimization_matrix_test.go`,
`datalog/storage/algebra_project_benchmark_test.go`

---

## Profiling Results (October 2025)

### In-Memory Execution Path
**Profiled**: MemoryPatternMatcher with OHLC queries

**CPU Bottlenecks** (measured via pprof):
- Pattern matching: 58% (matchesDatomWithPattern, matchesConstant, matchesElement)
- Memory copying: 18% (runtime.duffcopy)
- Hash operations: Various small percentages

**Memory Allocations** (measured via pprof):
- TupleKeyMap operations: 35% (now optimized with pre-sizing)
- Expression evaluation: 9-19% (time extractions)
- Pattern matching: 14%

**Key Finding**: Pattern matching dominates CPU time in-memory queries. Optimizations targeting pattern matching (in-memory indexing) made hash indices the default path throughout the codebase.

### Storage-Backed Execution Path
**Profiled**: BadgerMatcher with OHLC queries
**Query Time**: 56ms for 260 hours (measured)

**Key Finding**: Storage-backed queries already fast enough for production use. Focus has been on correctness and architectural improvements rather than micro-optimizations.

**Details**: See `PROFILING_SUMMARY.md` and `EXECUTION_CHAIN_PROFILING_ANALYSIS.md`

---

## Performance Test Results (Actual Benchmarks)

### Planner/Executor Architecture (2025-10-22)
**Benchmark Suite**: `BenchmarkPlannerOnly`, `BenchmarkPlanQuality`, `BenchmarkFullQueryOldVsNewPlanner`

**Planning Overhead Only** (BenchmarkPlannerOnly):
| Query Type | Old Planner | New Planner | Speedup |
|-----------|-------------|-------------|---------|
| Simple pattern | 3,940 ns | 2,048 ns | 52% faster |
| Single subquery | 4,382 ns | 758 ns | 83% faster |
| OHLC query | 11,546 ns | 1,673 ns | 86% faster |

**Key Finding**: Planning is fast (1-15 microseconds) regardless of planner. Planning speed has negligible impact on total query time.

**Plan Quality** (BenchmarkPlanQuality - both using QueryExecutor, verified 2025-10-24):
| Query Type | Old Planner | New Planner | Difference |
|-----------|-------------|-------------|------------|
| simple_join | 1.609 ms | 1.612 ms | 0.2% slower (within noise) |
| aggregation | 2.003 ms | 2.014 ms | 0.6% slower (within noise) |
| multi_join | 2.882 ms | 2.886 ms | 0.1% slower (within noise) |

**Key Finding**: When isolated (same executor), both planners produce equivalent-quality plans. Performance differences are within measurement noise.

**Full Architecture** (BenchmarkFullQueryOldVsNewPlanner - old planner+executor vs new planner+executor):
| Query Type | Old Architecture | New Architecture | Improvement |
|-----------|------------------|------------------|-------------|
| OHLC queries | ~4-8 seconds | ~2-4 seconds | **~2× faster** |

**Key Finding**: The 2× improvement comes from QueryExecutor's clause-by-clause streaming execution model, not from planner differences. Both planners produce equivalent-quality plans when using the same executor.

**Details**: See `docs/archive/completed/PLANNER_COMPARISON.md` (archived: one planner now)

### Time Range Optimization (2025-10-08)
**Hourly OHLC (260 hours)**:
- Before: 41s
- After: 10.2s
- **Speedup: 4.0×** ✅

**Daily OHLC (22 days)**:
- Before: 217ms
- After: 217ms
- **Speedup: 1.0×** (no regression) ✅

### Hash Join Pre-Sizing (2025-10-08)
| Size | Speed | Memory | Speedup |
|------|-------|--------|---------|
| 100 | 6.9µs → 5.2µs | 17KB → 13KB | 25% faster |
| 1,000 | 105µs → 71µs | 259KB → 181KB | 32% faster |
| 10,000 | 1.34ms → 1.24ms | 2.3MB → 1.7MB | 7% faster |

### Semantic Rewriting (2025-10-07)
| Filter Type | Selectivity | Speedup |
|-------------|-------------|---------|
| Year only | 33% | 2.6× |
| Year+Month+Day | 12.5% | 4.1× |
| Year+Month+Day+Hour | 1.4% | 5.8× |

### Plan Cache (Empirical)
- First query: ~1-5ms planning time
- Cached query: ~0.3ms planning time
- **~3× speedup for repeated queries**

---

## Optimization Priorities

### High Impact, Already Done ✅
All items below are **measured** and **active** in production code:

1. ✅ **New architecture** (clause-based planner + QueryExecutor) - **2× faster on complex queries** (verified 2025-10-24)
2. ✅ **Pull API** - **9× faster than equivalent queries**, linear scaling (verified 2025-12-17)
3. ✅ **Schema validation** - **<1% overhead** for type checking, **~6%** for uniqueness (verified 2025-12-17)
4. ✅ **Iterator composition** - **4.06× faster, 89% memory reduction** (verified 2025-10-25)
5. ✅ **Parallel subquery execution** - **2.06× speedup with 8 workers** (verified 2025-10-25)
6. ✅ **Intern cache optimization** - **6.26× speedup with BadgerDB**
7. ✅ **Query plan caching** - **3× speedup for repeated queries**
8. ✅ **Time range optimization** - **4× speedup on hourly OHLC**
9. ✅ **Semantic rewriting** - **2-6× on time-filtered queries**
10. ✅ **Predicate pushdown** - **1.58-2.78× faster** (scales with dataset size), **up to 91.5% memory reduction** (verified 2025-10-25)
11. ✅ **Streaming execution** - **2.22× on low-selectivity filters, 52% memory reduction** (verified 2025-10-25)
12. ✅ **Hash join pre-sizing** - **24-32% faster, 24-30% less memory**
13. ✅ **In-memory indexing** - Hash indices now default path throughout codebase
14. ✅ **Relation collapsing algorithm** - **Prevents catastrophic Cartesian products**
15. ✅ **Conditional aggregate rewriting** - **7.7× faster, 5.2× less memory** for correlated aggregate subqueries (verified 2026-01-16)
16. ✅ **CRDT storage** - **~25-35µs writes**, **O(1) LWW resolution**, conflict-free replication (verified 2026-01-31)
17. ✅ **CRDT allocation optimization** - **90% faster** (1.9×), **2.2× less memory** than pre-CRDT main while adding full CRDT semantics (verified 2026-02-02)
18. ✅ **Typed aggregation keys** - **47.5% faster, 25.8% less memory, 71.3% fewer allocations** while fixing cross-type and delimiter key collisions (verified 2026-07-11)
19. ✅ **Single-lookup dedup insertion** - **5.4–9.0% faster, 7.3% geomean** across materialized and streaming deduplication with unchanged memory and allocations (verified 2026-07-11)
20. ✅ **Bounded Top-N finalization** - **97.1% faster, 99.96% less memory, 99.86% fewer allocations** for ordered limits where no post-sort deduplicating projection is required (verified 2026-07-11)
21. ✅ **One-pass same-entity attribute bundles** - **13.5% faster, 32.9% less memory, 19.5% fewer allocations** geomean; K=6 improves up to 24.8% time and 56.5% memory (verified 2026-07-11)
22. ⚠️ **Typed Relation properties (foundation)** - **5.45% less memory, 2.71% fewer allocations** on the complex checkpoint with statistically unchanged time; join/OR derivations and broader storage coverage remain (verified 2026-07-11)
23. ✅ **Existing-order scan termination** - proven E-ascending limits scan exactly N rows and improve **98.1–99.7% time**, **97.0–99.4% memory**, and **98.0–99.6% allocations** on 10K entities (verified 2026-07-11)
24. ✅ **History ATEV order-aware matching** - safe history queries scan exactly N raw datoms and improve **99.15% time, 99.07% memory, 98.89% allocations** geomean; latest/as-of explicitly decline (verified 2026-07-11)
25. ✅ **History TAEV order-aware matching** - unfiltered transaction-log limits scan exactly N raw datoms and improve **99.16% time, 99.05% memory, 98.86% allocations** geomean; latest/as-of and filtered shapes decline (verified 2026-07-11)
26. ✅ **History AETV order-aware matching** - constant-attribute entity-first history limits scan exactly N raw datoms and improve **99.15% time, 99.07% memory, 98.89% allocations** geomean (verified 2026-07-11)
27. ✅ **History EATV order-aware matching** - constant-entity attribute-first history limits scan exactly N raw datoms and improve **99.13% time, 99.00% memory, 98.86% allocations** geomean (verified 2026-07-11)
28. ✅ **Key-preserving join properties** - retained-key streaming join projections improve **24.42% time, 25.15% memory, 9.13% allocations** geomean; complex default checkpoint remains statistically unchanged (verified 2026-07-11)
29. ✅ **Semi/anti join properties** - keyed filtering improves **27.54% time, 32.80% memory, 20.02% allocations** geomean; unkeyed controls retain deduplication and the complex checkpoint remains unchanged (verified 2026-07-11)
30. ✅ **Keyed hash-join dedup elision** - proven keyed joins omit the internal result `seen` table for **32.77% faster time, 32.77% less memory, 10.05% fewer allocations** geomean; complex checkpoint remains unchanged (verified 2026-07-11)
31. ✅ **Compiled storage hash matching** - typed probes and precomputed binding slots improve cache-disabled hash scans by **7.16% time, 15.65% memory, 32.83% allocations** (verified 2026-07-11)
32. ✅ **Ready-predicate scheduling** - selective filters run before unrelated scans for **62.85% faster time, 87.12% less memory, 78.88% fewer allocations** on the focused 10K-entity path; complex checkpoint remains unchanged (verified 2026-07-12)
33. ✅ **Unique-key hash-build specialization** - direct tuple storage improves keyed builds by **5.51% time, 7.25% memory, 11.11% allocations** geomean; non-unique fanout and complex checkpoint behavior remain unchanged (verified 2026-07-12)

### Potential Future Work 🎯
These are **ideas**, not commitments. Would require benchmarking before implementation:

1. BadgerDB time range integration - Push time constraints to storage layer
2. Composite index support - For multi-attribute filters

### Known Performance Regressions (Correctness Fixes)

**Set Semantics Fix for StreamingRelation.Project() (2025-12-24)**

`StreamingRelation.Project()` was not deduplicating results, violating set semantics. The fix wraps `ProjectIterator` with `DedupIterator`.

| Metric | Before | After | Change |
|--------|--------|-------|--------|
| Time | 35,326 ns/op | 44,331 ns/op | +25% |
| Memory | 2,520 B/op | 16,744 B/op | +6.6× |
| Allocs | 111 allocs/op | 224 allocs/op | +2× |

**Why this overhead is acceptable**: Without deduplication, projected relations can contain duplicates, violating Datalog semantics and causing incorrect query results. Correctness trumps performance.

**Future optimization**: See `docs/proposals/TUPLEKEYMAP_OPTIMIZATION.md` for proposal to optimize `TupleKeyMap` for set membership (currently stores values we don't need for dedup).

### Rejected After Benchmarking ❌
These were **tried and measured** - data showed they're not worth the complexity:

1. ~~Key mask iterator for int64~~ - Benchmarked slower than simple approach
2. ~~Complex iterator reuse~~ - Simpler code is faster
3. ~~Aggressive CSE~~ - 1-3% sequential, -1% parallel (disabled by default)
4. ~~Interface-based key encoder consolidation~~ - 10% slower, 60% more allocations (Dec 2025)
5. ~~Generic key encoder with type parameters~~ - Go generics don't inline, same overhead as interfaces

---

## Recommended Configuration (October 2025)

**Production Configuration** (all settings are measured and proven):

```go
// Public API — uses all production defaults automatically
d, _ := db.Open("path/to/db")
d.Query(`[:find ?e ?v :where [?e :price/close ?v]]`)

// With schema:
d, _ := db.Open("path/to/db", db.WithSchema(s))

// For advanced planner tuning, use the internal packages directly:
// Advanced tuning starts from the defaults — these ARE the db.Open defaults:
opts := storage.DefaultPlannerOptions()
// opts.EnableAlgebraOptimizer     == true   // decorrelation + predicate pushdown
// opts.EnableIteratorComposition  == true   // lazy evaluation
// opts.EnableTrueStreaming        == true   // streaming, no auto-materialization
// opts.EnableParallelSubqueries   == true   // parallel subquery execution
// opts.EnableStreamingAggregation == true   // streaming aggregation

exec := executor.NewExecutorWithOptions(matcher, opts)
```

**Key Changes from Previous Versions**:
- New clause-based planner is now default (was experimental)
- QueryExecutor is now default (was opt-in)
- Streaming execution always enabled (was toggle)
- All settings backed by measured benchmarks

---

## Performance Philosophy

### What We Got Right
1. **Correctness first** - Semantics before speed
2. **Measure everything** - Benchmarks revealed truth
3. **Simple code wins** - Iterator reuse complexity didn't pay off
4. **Smart algorithms** - Relation collapsing is the real hero
5. **Targeted optimization** - Profile, optimize bottlenecks, verify

### What We Learned
1. **Micro-optimizations fail** - Opens are 3µs, not worth complexity
2. **Architecture matters** - Can't push predicates without storage support
3. **Premature optimization is real** - Key mask, iterator reuse both slower
4. **Document reality** - Aspirational docs cause confusion
5. **Redundant optimizations exist** - Semantic rewriting + decorrelation target same bottleneck
6. **Go interfaces have real cost** - 10% slower, 60% more allocations from interface dispatch in hot paths
7. **Go generics don't inline** - Unlike C++/Rust, Go generics provide type safety but NOT zero-cost abstraction
8. **DRY vs performance trade-off** - In hot paths, duplication IS the optimization; share only cold paths

### What's Next
1. Keep **simple, correct code** (complexity doesn't pay)
2. Let **algorithms win** (relation collapsing, not tricks)
3. Build **only what benchmarks prove** (no more speculation)
4. Reuse established typed-key primitives instead of inventing string encodings

---

## Success Metrics

### Current State (2025-10-25)
All metrics below are **measured** from actual benchmarks, not estimates.

**Verified Performance Improvements** (latest benchmarks):
- New architecture: **2× faster** on complex OHLC queries (old: ~4-8s, new: ~2-4s) ✅
- Iterator composition: **4.06× faster** (1,259μs → 310μs), **89% memory reduction** (3.27 MB → 360 KB) ✅
- Streaming execution: **2.22× faster** on low-selectivity filters (1,720μs → 774μs), **52% memory reduction** ✅
- Parallel subquery execution: **2.06× speedup** with 8 workers (730ms → 355ms) ✅
- Predicate pushdown (small): **1.58× faster** (33.6ms → 21.3ms), **49% memory reduction** ✅
- Predicate pushdown (large): **2.78× faster** (1,043ms → 375ms), **91.5% memory reduction** ✅
- Hourly OHLC (large dataset): **10.2s** (4× speedup from time ranges) ✅
- Parallel BadgerDB: **6.26× speedup** (intern cache optimization) ✅
- Hash join pre-sizing: **24-32% faster, 24-30% less memory** ✅

**Pervasive Optimizations** (now default throughout codebase):
- In-memory indexing: Hash indices are now the default path (previously showed 49-4802× vs linear scan)
- Plan quality: Both planners produce equivalent-quality plans (within measurement noise)
- Single-use iterator semantics: Proper lifecycle management prevents bugs, enables streaming

---

## Documentation Organization

### Active Documentation (Root)
- `PERFORMANCE_STATUS.md` - **This file** (consolidated performance overview)
- `docs/reference/PLANNER_OPTIONS.md` - Planner/executor options reference (defaults + opt-in flags)

### Supporting Documentation
- `TIME_RANGE_OPTIMIZATION_STATUS.md` - Time range extraction and optimization
- `HASH_JOIN_PRESIZING_SUMMARY.md` - Hash join pre-sizing optimization
- `PROFILING_SUMMARY.md` - Complete profiling findings

### Archived Documentation (docs/archive/2025-10/)
- Detailed analyses (EXECUTION_CHAIN_PROFILING_ANALYSIS.md, etc.)
- Implementation docs (SUBQUERY_DECORRELATION_*.md, SEMANTIC_REWRITING_FINDINGS.md, CSE_FINDINGS.md)
- Streaming architecture history (STREAMING_ARCHITECTURE_COMPLETE.md)
- Bug documentation (PARALLEL_DECORRELATION_TUPLE_ORDER_BUG.md)
- Session summaries (SESSION_SUMMARY_*.md)
- Completed work (CSE_FINDINGS.md, SEMANTIC_REWRITING_FINDINGS.md)

---

## The Bottom Line

**What Actually Matters**:
1. ✅ Relation collapsing prevents memory explosion (CRITICAL)
2. ✅ Query plan caching speeds up repeated queries (3× improvement)
3. ✅ Parallel execution eliminates bottlenecks (6.26× speedup)
4. ✅ Time range optimization targets specific queries (4× speedup)
5. ✅ Semantic rewriting optimizes time queries (2-6× speedup)
6. ✅ Code correctness and simplicity beat micro-optimizations

**What Doesn't Matter**:
1. ❌ Iterator open/close overhead (3µs, negligible)
2. ❌ Key mask filtering for simple types (overhead > benefit)
3. ❌ Complex reuse strategies (simpler code performs better)
4. ❌ CSE with parallel execution (removes parallelism opportunity)

The engine is **production-ready for datasets up to 10M+ datoms**. All major optimizations are complete and working well. Performance is excellent for typical workloads (100K-1M datoms), with targeted optimizations for specific patterns (time queries, large joins). Large config testing demonstrates scalability to 500M+ datoms.

---

## Session History

### 2026-04-17: CRDT Unique Resolution Redesign

**Branch**: `feature/crdt-unique-resolution`
**Status**: ✅ Complete — read-time (A, V)-LWW replaces write-time enforcement

**The Story**:
Write-time `validateUniqueness` was incompatible with this codebase's
CRDT-oriented architecture (concurrent writers, `DetectConflicts=false`,
append-only storage with LWW resolution). The bug report framed it as a
TOCTOU race; the real framing was that the whole write-time gate was
the wrong model. Replaced with walk-based `(A, V)`-LWW resolution at
read time: V-view and entity-view share a single rule, all writes
succeed, reads compute the canonical owner via walk.

See `docs/reference/CRDT_UNIQUE_SEMANTICS.md` for the complete design
discussion and decisions (D1–D5).

**What Was Done**:

1. **Walk-based resolution primitive** (`unique_resolve.go`):
   `walkUniqueEntityValue(E, A)` walks an entity's EATV history in
   descending Tx order, handling retractions and supersession by other
   entities. Returns the first non-superseded Set or nothing.

2. **V-view via walk** (`resolveAVLWW`, `LookupByUnique`):
   Finds max-Tx entry for V across all entities, verifies that
   entity's walk emits V. V-view and entity-view are symmetric by
   construction.

3. **Streaming integration** (`CRDTResolvingIterator`):
   CardinalityOne + Unique groups use the walk inline; other paths
   unchanged. Shared `walkApplyEntry` primitive avoids duplicated
   rule-logic between batch and streaming paths.

4. **Cache invalidation** (`Cache.InvalidateAttribute`):
   Conservative: writes to a unique attribute invalidate all cached
   `(E, A)` entries for that attribute, since any write can silently
   stale other entities' walk results.

5. **History-mode bypass**: `ResolveLWW` skips the walk in history
   mode, returning raw first-entry semantics. Fixes a latent issue
   where `d.History().Pull()` via wildcard would return walk-resolved
   fallback values instead of raw assertions.

6. **Error propagation**: `Iterator` interface extended with
   `Error() error`. All storage iterators implement it; wrapping
   iterators propagate deferred errors from inner iterators. Closes
   multiple pre-existing silent-swallow sites (e.g., CRDTResolvingIterator
   `source.Datom() err → continue`).

**Benchmark Results** (Apple M5, 2s duration):

| Benchmark | ns/op | B/op | allocs/op | vs baseline |
|---|---:|---:|---:|---|
| NonUniqueRead_Baseline (cached) | 387 | 801 | 6 | — |
| UniqueRead_Uncontested (cached) | 377 | 802 | 6 | **~0% (noise)** |
| UniqueRead_ContestedLinear (empty result) | 305 | 593 | 5 | -21% (less data) |
| UniqueRead_DeepFallback (5 layers, empty) | 283 | 593 | 5 | -27% (less data) |
| LookupByUnique_Uncontested (V-view warm) | 7319 | 5793 | 93 | n/a |
| LookupByUnique_ColdCache (V-view cold) | 7605 | 5796 | 93 | n/a |

**Key Findings**:

- **Hot path (cached entity-view reads)**: the walk adds effectively
  **zero overhead** vs non-unique CardinalityOne. Both paths land
  around 380 ns/op. The cache stores walk-resolved values, so
  subsequent reads don't repeat the walk.

- **Contested scenarios are faster, not slower**: when the walk finds
  no fallback value (all entries superseded), the result map is empty
  and `ResolveEntityAttributes` has less work. Deep-fallback (5
  superseded entries) comes out at 283 ns/op — faster than both
  uncontested and non-unique — because the empty-result path is
  genuinely cheaper than the populate-map path.

- **LookupByUnique (V-view) is ~7μs per call**. Not cheap, but
  acceptable for realistic use (authentication flows, reconciliation
  lookups). Cold vs warm cache: similar, because V-view doesn't
  benefit from per-(E, A) cache entries. Improving this is a
  follow-up opportunity if profiling warrants it.

- **Allocation profile unchanged**: cached entity-view reads allocate
  the same 6 times as non-unique. No hidden allocations from the
  walk infrastructure.

**The redesign imposes no measurable cost on the read hot path.**

**Files Changed**:

- `datalog/storage/unique_resolve.go` — walk primitive, shared rule
- `datalog/storage/crdt_resolving_iterator.go` — streaming integration
- `datalog/storage/cache_resolver.go` — `ResolveLWW` routes unique
  through walk; history-mode bypass
- `datalog/storage/cache.go` — `InvalidateAttribute`
- `datalog/storage/database.go` — `LookupByUnique` API, removed
  `validateUniqueness`, unique-attr invalidation in `Transaction.Commit`
- `datalog/storage/matcher_relations.go` — V-view single-winner
  selection in `validatingVBoundIterator`
- `datalog/storage/store.go` — `Iterator.Error()` interface extension
- All storage iterator implementations — `Error()` method
- `datalog/storage/unique_benchmark_test.go` — 6 benchmarks
- Multiple test files — 40+ new tests covering V-view, entity-view
  symmetry, retract semantics, history/AsOf, cache invalidation,
  value-encoding edge cases, error propagation
- `docs/reference/CRDT_UNIQUE_SEMANTICS.md` — design doc (promoted
  from `docs/proposals/`)
- `docs/reference/SCHEMA.md`, `docs/reference/CRDT.md` — updated with
  new semantics

---

### 2026-02-06: AETV Index & Value Elimination - Streaming CRDT Fixes

**Branch**: `main`
**Status**: ✅ Complete - CRDT resolution now correct for all query patterns

**The Story**:
The `CRDTResolvingIterator` relies on Tx-descending index order (first entry = LWW winner). When E was bound via input with A constant, the matcher selected AEVT (Tx ascending) instead of a CRDT-aware index, returning stale values. We added AETV as a 7th index and eliminated redundant value storage.

**What Was Done**:

1. **AETV Index** (A → E → Tx↓ → V):
   - New A-primary CRDT index complementing EATV (E-primary CRDT)
   - Index selection updated: A-constant + E-from-input now uses AETV
   - Fixes `CRDTResolvingIterator` returning wrong values for batch entity lookups

2. **Value Elimination**:
   - `assertDatom()` now writes nil values (all datom data is encoded in keys)
   - `BadgerIterator.Datom()` decodes from key via `DatomFromKey()`
   - ~50% storage reduction (no redundant value bytes)

**Benchmark Results** (Apple M4 Max, benchstat n=100):

| Benchmark | Time | Memory | Allocations |
|-----------|------|--------|-------------|
| SimpleQuery | +2.59% | +0.13% | -0.12% |
| JoinQuery | +0.94% | -0.85% | -0.84% |
| CardinalityMany | **-5.16%** | **-32.59%** | **-28.49%** |
| VectorQuery | **-18.60%** | **-35.11%** | **-32.89%** |
| **geomean** | **-5.44%** | **-18.82%** | **-16.97%** |

**Key Findings**:
- Simple queries: minimal change (less datom decoding)
- Complex queries: significant wins from eliminating value reads
- Storage reduced by ~50% (values were 100% redundant with keys)
- All 17 CRDT resolution tests now pass with `DisableCache: true`

**SimpleQuery Regression Explained** (+2.59%):
The regression comes from `KeyCopy(nil)` in `BadgerIterator.Datom()`. Previously, values were read from BadgerDB's value storage. Now we decode from keys, which requires copying the key bytes because BadgerDB reuses its internal buffer.

We investigated key buffer reuse to eliminate this allocation, but it's not possible: `DecodeKey` returns the value component as a slice *into* the key bytes (e.g., `value = key[entitySize+attrSize:vEnd]`). Reusing the key buffer would cause values to be corrupted on subsequent iterations.

This is an acceptable trade-off:
- SimpleQuery: +2.59% time (~1µs absolute on a 44µs query)
- VectorQuery: **-18.60%** time (complex queries dominate real workloads)
- geomean: **-5.44%** time (net positive across all query types)

**Files Changed**:
- `datalog/storage/key_encoder_*.go` - AETV encoding/decoding
- `datalog/storage/matcher_strategy.go` - Index selection for AETV
- `datalog/storage/badger_store.go` - Value elimination in `assertDatom()`
- `datalog/storage/badger_iterator.go` - Decode from key in `Datom()`
- `docs/wip/AETV_INDEX_AND_VALUE_ELIMINATION.md` - Design doc

---

### 2026-02-02: CRDT Allocation Optimization - Faster Than Pre-CRDT

**Branch**: `feature/allocation-regression-fixes`
**Status**: ✅ Complete - CRDT storage now faster than original non-CRDT implementation

**The Story**:
CRDT storage added powerful capabilities (LWW, add-wins sets, RGA vectors, time-travel). Rather than accept performance overhead, we optimized the entire pipeline. Result: **CRDT + 90% faster (1.9×) than pre-CRDT main**.

**What Was Done**:

1. **Storage Layer** (Phases 1-4):
   - `txToDescending()`: `[16]byte` return eliminates heap escape (16 B/op → 0)
   - `DatomFromKey()`: Return by value eliminates pointer allocation (80 B/op → 0)
   - Iterator workspace reuse via `BuildTupleInternedInto()`
   - Cache path `datomBuf` reuse across iterations

2. **Executor Layer** (Phases 5-6):
   - `RequiresCopy()` method on Relation interface
   - Wrapper relations copy once at boundary, return `RequiresCopy()=false`
   - All `copyTuple()` calls conditional on source's `RequiresCopy()`

**Benchmark Results** (Apple M4 Max, BenchmarkOHLCQuery):

| Metric | Main (pre-CRDT) | CRDT + Optimized | Improvement |
|--------|-----------------|------------------|-------------|
| Time | 57ms | 30ms | **1.9× faster** |
| Memory | 66MB | 30MB | **2.2× less** |
| Allocations | 897K | 405K | **2.2× fewer** |

**Key Insight**: Hot path allocations dominated. Eliminating heap escapes in `DatomFromKey()` (called millions of times) and `txToDescending()` (every key encode) yielded massive gains.

**Files Changed**: 40 files, +3293/-284 lines across storage and executor layers

---

### 2026-01-31: CRDT Storage Benchmarks & LookupAttribute Fix

**Branch**: `main`
**Status**: ✅ Complete with comprehensive CRDT benchmarks

**What Was Done**:

1. **Created CRDT benchmark suite** (`datalog/storage/crdt_benchmark_test.go`):
   - Write benchmarks for all three cardinalities (One, Many, Vector)
   - Read benchmarks for set resolution and vector reconstruction
   - CRDT resolution benchmarks (LWW, add-wins with/without tombstones)

2. **Fixed LookupAttribute semantic issue** (`datalog/storage/matcher.go`):
   - **Problem**: `LookupAttribute` for cardinality-many returned single value instead of `[]interface{}`
   - **Impact**: Pull API without schema (unresolved patterns) only got first set member
   - **Fix**: Updated cache path and storage fallback path to return all set members with add-wins resolution

**Benchmark Results** (Apple M4 Max):

| Category | Operation | Time | Notes |
|----------|-----------|------|-------|
| **Writes** | CardinalityOne | 24.9µs | LWW semantics |
| | CardinalityMany/Add | 26.1µs | Add to set |
| | CardinalityVector/Append | 27.1µs | RGA append |
| **Reads** | CardinalityMany/100 members | 26.5µs | Full resolution |
| | CardinalityVector/10 elements | 21.2µs | RGA reconstruction |
| | CardinalityVector/100 elements | 204.7µs | Linear scaling |
| **Resolution** | LWW/1000 versions | 0.97µs | O(1) lookup |
| | AddWins/50 members | 13.8µs | No tombstones |
| | AddWins/150 ops (100 add, 50 remove) | 37.9µs | With tombstones |

**Key Findings**:
- CRDT writes have consistent ~25-35µs performance across cardinalities
- LWW resolution is O(1) - first entry in descending Tx scan is current value
- Vector reconstruction scales linearly at ~2µs per element
- Add-wins tombstone handling adds ~2.7× overhead vs clean sets

**Files Changed**:
- `datalog/storage/crdt_benchmark_test.go` - New comprehensive benchmark suite
- `datalog/storage/matcher.go` - Fixed LookupAttribute to return `[]interface{}` for cardinality-many
- `datalog/storage/crdt_vector_test.go` - Updated test assertions for new semantics

---

### 2026-01-16: Conditional Aggregate Rewriting - QueryExecutor Parity Fix

**Branch**: `main`
**Status**: ✅ Complete with executor comparison benchmarks

**Problem**:
QueryExecutor couldn't handle conditional aggregate rewriting. Tests used `UseLegacyExecutor: true` as workaround. The rewriter stored aggregates in `phase.Metadata` but QueryExecutor didn't know how to interpret this metadata.

**Root Cause**:
1. `rewriteCorrelatedAggregates` stored conditional aggregates in `phase.Metadata["conditional_aggregates"]`
2. Legacy executor had special code to read metadata and apply aggregation
3. QueryExecutor treated metadata as inert data - didn't create the aggregate output symbol
4. Find clause injection was attempted but `updatePhaseSymbols` was overwriting it

**Solution**:
Moved Find clause injection to AFTER `updatePhaseSymbols` in `planner.go`. The planner now emits two representations:
1. **Metadata** (for legacy executor backward compatibility)
2. **Modified Find clause** with `FindAggregate` containing `Predicate` field (for QueryExecutor)

**Key Changes**:
- `datalog/planner/planner.go`: Added conditional aggregate injection after `updatePhaseSymbols`
- `datalog/planner/subquery_rewriter.go`: Added `collectConditionalAggregates()` and `injectConditionalAggregatesIntoFind()` helper functions
- `datalog/executor/executor.go`: Updated comments documenting dual representation

**Benchmark Results** (Apple M4 Max):

| Configuration | Time | Memory | Allocations |
|---------------|------|--------|-------------|
| Without rewriting | 16.2 ms | 15.2 MB | 275K |
| With rewriting | 2.1 ms | 2.9 MB | 33.9K |
| **Improvement** | **7.7×** | **5.2×** | **8.1×** |

**Executor Comparison** (with rewriting):

| Executor | Time | Difference |
|----------|------|------------|
| Legacy | 1.95 ms | baseline |
| QueryExecutor | 1.98 ms | +1.5% (parity achieved) |

**New Benchmarks Added**:
- `BenchmarkConditionalAggregateExecutorComparison`: Compares legacy vs QueryExecutor with/without rewriting
- `BenchmarkConditionalAggregateScale`: Tests scaling behavior at different data sizes

**Files Changed**:
- `datalog/planner/planner.go` - Find clause injection
- `datalog/planner/subquery_rewriter.go` - Helper functions
- `tests/conditional_aggregate_rewriting_benchmark_test.go` - New executor comparison benchmarks

---

### 2025-12-24: Identity/Keyword Pointer Type Alias Optimization

**Branch**: `main` (merged from `feature/intern-all-keywords`)
**Status**: ✅ Complete with comprehensive reflection audit

**What Was Done**:

Changed `Identity` and `Keyword` from value types to pointer type aliases with mandatory interning:

```go
// Before (value types)
type Identity struct { value [20]byte; l85 string; ... }
type Keyword struct { value string }

// After (pointer type aliases)
type identity struct { value [20]byte; l85 string; ... }  // unexported
type Identity = *identity                                  // exported alias

type keyword struct { value string }                       // unexported
type Keyword = *keyword                                    // exported alias
```

**Key Changes**:
1. **Mandatory interning** - All constructors (`NewIdentity`, `NewKeyword`) automatically intern
2. **Pointer equality** - `kw1 == kw2` is O(1) pointer comparison, not O(n) string comparison
3. **Storage-aligned cache keys** - Keyword intern uses `[32]byte` key (matches storage format), Identity uses `[20]byte`
4. **Direct storage reads** - `InternKeywordFromBytes([32]byte)` and `InternIdentityFromHash([20]byte)` avoid string conversion on cache hit
5. **DecodeKey returns arrays** - Changed `DecodeKey` interface to return fixed-size arrays `([20]byte, [32]byte, ...)` instead of slices, avoiding heap escape

**Benchmark Results** (Apple M3 Ultra, n=100):

| Benchmark | Time | Memory | Allocs |
|-----------|------|--------|--------|
| JoinQuery_CrossNamespace | **-7.6%** | **-44.2%** | **-38.5%** |
| WildcardPull_ManyAttributes | **-9.7%** | **-28.3%** | -1.3% |
| Aggregation_ManyAttributes | -2.3% | -14.9% | -10.5% |
| SimpleQuery_HighKeywordVariety | +4.8% | -5.6% | ~ |
| **Geomean** | **-3.8%** | **-24.7%** | **-14.2%** |

**Why JoinQuery Benefits Most** (-44% memory, -38% allocs):
- Hash joins build `TupleKeyMap` using keyword/identity as keys
- Before: Each key comparison called `.String()`, allocating
- After: Pointer comparison is O(1), no allocation
- Join build phase: Fewer allocations for hash table keys
- Join probe phase: Faster lookups with pointer keys

**SimpleQuery Regression** (+4.8% time):
- Small queries have overhead from pointer indirection
- Still uses less memory (-5.6%), acceptable tradeoff
- Query is already fast (47µs), regression is ~2µs absolute

**Implementation Notes**:
- Downstream consumers unaffected - still use `datalog.Identity` and `datalog.Keyword`
- Comparison semantics unchanged - equal values compare equal
- Thread-safe via `sync.Map` for concurrent interning
- Invariant check: `Compare()` panics if two different pointers have same hash (indicates interning bug)

**Files Changed**: 46 files, +923/-686 lines
- `datalog/types.go` - Pointer type aliases, new methods
- `datalog/intern.go` - Storage-aligned cache keys, `InternKeywordFromBytes`
- `datalog/storage/key_encoder_*.go` - `DecodeKey` returns arrays
- `datalog/storage/datom_decoder.go` - Direct byte interning

**Reflection Audit** (completed same day):
- Fixed 4 functions in `datalog/reflect/types.go` that incorrectly dereferenced Identity/Keyword pointer types
- Pattern: Check `t == identityType || t == keywordType` BEFORE `t.Kind() == reflect.Ptr` → `t.Elem()`
- Functions fixed: `InferCardinality`, `IsRefType` (inner loop), `ElementType`, `IsSliceType`
- Added nil-safety to all Identity/Keyword methods (Hash, L85, String, Equal, Compare, etc.)
- Added comprehensive test coverage: `TestNilIdentityHandling`, `TestNilKeywordHandling`

---

### 2025-12-24: Key Encoder Consolidation - A DRY Refactoring Case Study

**Goal**: Reduce code duplication between `L85KeyEncoder` and `BinaryKeyEncoder` (~95% identical logic).

**Attempt 1: Interface-Based Consolidation**
- Created `ComponentEncoder` interface abstracting encode/decode operations
- Created `baseKeyEncoder` struct with shared index ordering logic
- L85/Binary encoders delegated to base via embedded struct + interface field

**Result**:
| Benchmark | Main | Refactored | Regression |
|-----------|------|------------|------------|
| AVETReuse | 29ms, 16MB, 300K allocs | 32ms, 20.8MB, 476K allocs | **+10% time, +30% mem, +60% allocs** |
| BatchScanScaling/1000 | 1.1ms, 1.15MB, 21K | 1.25ms, 1.41MB, 31K | **+14% time, +23% mem, +47% allocs** |

**Root Cause Analysis**:
1. **Interface dispatch overhead** - Every `e.comp.EncodeEntity()` call goes through vtable
2. **Lazy initialization checks** - `ensureInitialized()` branch on every method
3. **Slice escape to heap** - Passing `[20]byte` by value to interface method, returning `[]byte` causes allocation

**Attempt 2: Go Generics**
- Changed `baseKeyEncoder` to `baseKeyEncoder[T ComponentEncoder]`
- Used concrete type parameter to enable compiler inlining

**Result**: No improvement. Go generics don't specialize/inline like C++ templates or Rust generics. The method calls still happen at runtime.

**Attempt 3: Hybrid Approach (SUCCESS)**
- Restored full inline implementations in each encoder (hot paths)
- Extracted only truly shared utilities to `key_encoder_base.go`:
  - `historyIndexToBase()` - Maps history indices to base indices
  - `incrementLastByte()` - Creates end key for prefix scans
- Removed duplicate `l85HistoryIndexToBase` function

**Result**:
| Benchmark | Main | Hybrid | Change |
|-----------|------|--------|--------|
| AVETReuse | 29ms, 16MB, 300K | 28ms, 16MB, 300K | ✅ **3% faster** |
| BatchScanScaling/1000 | 1.1ms, 1.15MB, 21K | 1.0ms, 1.15MB, 21K | ✅ **9% faster** |
| BatchScanScaling/5000 | 6.0ms, 4.6MB, 86K | 5.3ms, 4.6MB, 86K | ✅ **12% faster** |

**Why Hybrid is Faster Than Main**:
Main branch had duplicate functions: `l85HistoryIndexToBase` (in L85 encoder) and `historyIndexToBase` (in Binary encoder) - identical 15-line switch statements. The hybrid approach consolidates these into a single shared `historyIndexToBase()` in `key_encoder_base.go`.

Benefits of consolidation:
1. **Reduced binary size** - One function instead of two identical copies
2. **Better instruction cache** - Single hot function stays in cache vs two copies competing
3. **Compiler optimization** - Single definition allows better inlining/branch prediction

This demonstrates that **strategic code sharing CAN improve performance** when sharing cold/utility paths that don't benefit from inlining, while keeping hot encode/decode logic duplicated.

**Code Impact**:
| Version | Lines |
|---------|-------|
| Main (original) | 662 lines |
| Hybrid (final) | 550 lines |
| **Reduction** | **112 lines (17%)** |

**Key Takeaways**:

1. **Go interfaces have real cost** - Interface dispatch, escape analysis, and allocation overhead are measurable in hot paths. The ~10% regression and 60% more allocations came purely from abstraction.

2. **Go generics ≠ C++ templates** - Go generics provide type safety but NOT specialization. Method calls through generic type parameters still have runtime overhead. Don't expect zero-cost abstractions.

3. **DRY has limits in performance-critical code** - Sometimes duplication IS the optimization. The compiler can't inline what you've abstracted away.

4. **Hybrid approach works** - Share truly common utilities (helper functions, constants) while keeping hot paths inline. This preserves both readability and performance.

5. **Benchmark before and after** - The interface refactor looked cleaner but was 10% slower. Only benchmarks revealed the truth.

6. **Allocation count is a leading indicator** - The 60% allocation increase (300K → 476K) signaled trouble before timing showed it. Extra allocations = GC pressure = slower execution.

**Files Changed**:
- `datalog/storage/key_encoder_base.go` - Shared utilities only (37 lines)
- `datalog/storage/key_encoder_binary.go` - Full inline implementation (172 lines)
- `datalog/storage/key_encoder_l85.go` - Full inline implementation (288 lines)
- `datalog/storage/key_encoder_interface.go` - Factory unchanged (53 lines)

**Recommendation**: For performance-critical code paths called millions of times:
- Prefer inline code over interface abstraction
- Share constants, helper functions, and cold paths only
- Keep hot encode/decode/match logic duplicated but identical
- Use benchmarks to validate any consolidation attempt

### 2025-12-24: DRY Refactoring Analysis - What NOT to Consolidate

**Context: Interface Overhead Constrains Our Options**

The key encoder experiment (above) proved that Go interfaces have real cost in hot paths: 10% slower, 60% more allocations. This fundamentally constrains consolidation options:

- **With interfaces**: Could share logic via polymorphism, but pays performance penalty
- **Without interfaces**: Can only share via extracted helper functions (like `historyIndexToBase()`)

This means any consolidation must either:
1. Accept the interface overhead (unacceptable for hot paths), OR
2. Find substantive duplicated logic that can be extracted to shared functions

With this constraint, we evaluated the remaining consolidation candidates:

**Relation Interface Consolidation (REJECTED)**

Problem statement: `relation.go` has 5-6 types (MaterializedRelation, StreamingRelation, EmptyRelation, etc.) each implementing ~26 methods. Initial estimate: 300-500 lines savings.

Analysis revealed this is **NOT real duplication**:
- `Symbols()` → one-liner returning a field
- `Symbols()` → same as `Symbols()`
- `Options()` → one-liner returning a field
- `Join()` → one-liner calling `HashJoin()`

Each relation type has fundamentally different storage and iteration logic. The "26 methods" are mostly trivial accessors that are **clearer explicit than abstracted**. Consolidating would add interface overhead (as we learned from key encoders) for no readability benefit.

**Storage Iterator Consolidation (REJECTED)**

Problem statement: Four iterator implementations (`matcher_iterator_reusing.go`, `matcher_iterator_nonreusing.go`, `matcher_iterator_unbound.go`) with ~573 lines total. Initial estimate: 200-300 lines savings.

Analysis revealed these are **different iteration strategies**, not duplicated code:
- `unboundIterator`: Simple scan, no bindings
- `nonReusingIterator`: New scan per binding tuple
- `reusingIterator`: Complex range calculation, single scan across all bindings

Shared elements are minimal:
- Field declarations (`tupleBuilder`, `constraints`, `symbols`)
- One-liner validation calls
- Statistics tracking

The core `Next()` logic is completely different for each strategy. Unifying them would create a confusing abstraction with conditionals, not cleaner code.

**Key Insight: Real Duplication vs Structural Similarity**

| Pattern | Example | Should Consolidate? |
|---------|---------|---------------------|
| **Real duplication** | Two identical 15-line switch statements | ✅ YES |
| **Structural similarity** | Multiple types with same field names | ❌ NO |
| **Trivial accessors** | One-liner `Symbols()` methods | ❌ NO |
| **Different algorithms** | Multiple iterator strategies | ❌ NO |

**Decision Criteria for Future Consolidation**:
1. Is the duplicated code **substantive** (>10 lines of real logic)?
2. Would changes to one copy **always** require identical changes to others?
3. Does consolidation **improve readability** or just reduce line count?
4. Is the code on a **hot path** where interface overhead matters?

The key encoder consolidation succeeded because it met criteria 1-3: identical 15-line switch statements that would always change together, and consolidation improved readability. The hybrid approach addressed criterion 4 by keeping hot paths inline.

The relation and iterator consolidations failed criteria 1-3: the "duplication" was structural similarity (same field names, same interface) not identical logic.

**Why Interface-Based Consolidation Was Not An Option**

In languages like Java or C#, the typical solution would be a base class or interface with default implementations. In Go, this would mean:

```go
// Hypothetical base relation (NOT IMPLEMENTED)
type baseRelation struct {
    symbols []query.Symbol
    options ExecutorOptions
}
func (b *baseRelation) Symbols() []query.Symbol { return b.symbols }
// ... 20+ more delegating methods
```

But the key encoder experiment proved this approach costs 10% performance and 60% more allocations due to:
1. Interface dispatch (vtable lookup on every call)
2. Escape analysis failures (values escape to heap through interfaces)
3. Inlining prevention (compiler can't inline through interface calls)

For relations and iterators—called millions of times during query execution—this overhead is unacceptable.

**The Realistic Choices Were:**
1. ❌ Interface-based consolidation → Rejected due to proven performance cost
2. ❌ Extract shared logic to helper functions → Nothing substantive to extract (only trivial accessors)
3. ✅ Keep explicit implementations → Maintains performance, code is already clear

**Conclusion**: The interface overhead lesson from key encoders eliminated our primary consolidation tool. What remained wasn't real duplication—just structural similarity that's clearer left explicit. Line count reduction is not a goal; code clarity and performance are.

---

### 2025-12-17: Schema Support Implementation & Benchmarking
- Implemented Datomic-compatible schema support with type validation, cardinality, and uniqueness
- Schema definable via EDN file or Go builder API (same internal representation)
- Added plan-time resolution for Pull patterns (schema lookup once per pattern, not per entity)
- Created comprehensive benchmark suite for schema performance
- **Measured Results**:
  - Type validation overhead: **<0.2%** (essentially free)
  - Uniqueness checking overhead: **~5.8%** (requires database query)
  - Bulk operations with schema: **~1.4%** overhead
  - Schema resolution: **225ns** one-time cost per pattern
  - Cardinality-many: ~370ns per additional value (linear scaling)
- **Key Finding**: Schema validation is essentially free; use freely for type safety
- **Files created**: `datalog/schema/` package, `docs/reference/SCHEMA.md`, `examples/schema.go`

### 2025-12-17: Pull API Implementation & Benchmarking
- Implemented Datomic-style Pull API with nested references and cycle detection
- Created comprehensive benchmark suite for Pull performance
- **Measured Results**:
  - Pull vs Query: **9.2× faster** (3.5µs vs 32.7µs for 3 attributes)
  - Wildcard: **2.2× faster** than explicit attributes (single scan vs N lookups)
  - Per-attribute cost: ~1.2µs (BadgerDB), ~470ns (in-memory)
  - Per-entity cost: ~2.5µs (BadgerDB), ~1µs (in-memory)
  - Linear scaling with both attributes and entities
- **Key Finding**: Pull avoids query parsing/planning overhead entirely
- **Recommendation**: Cache parsed patterns for hot paths (20% speedup)

### 2025-10-25: Single-Use Iterator Semantics & Performance Verification (Sessions 1-2)
**Session 1**: Initial benchmarking after single-use iterator semantics implementation
- Implemented proper single-use iterator semantics for StreamingRelation
- Added BufferedIterator for safe re-iteration support
- Ran comprehensive performance benchmarks to verify all claims
- **Updated all performance claims to reflect reality**:
  - Iterator composition: 4.2× (was claiming 17.5×)
  - Streaming execution: 1.9× (was claiming 28×)
  - Parallel execution: 2.2× with 8 workers (new measurement)
  - Predicate pushdown: 1.77× (new measurement)

**Session 2**: Debug output cleanup and precise verification
- Discovered debug prints polluting benchmark output (79 fmt.Printf statements)
- Fixed 3 unguarded debug prints in join.go
- Re-ran all benchmarks with clean output
- **Updated to precise measurements**:
  - Iterator composition: **4.06×** (1,259μs → 310μs), 89% memory reduction
  - Streaming execution: **2.22×** (1,720μs → 774μs), 52% memory reduction
  - Parallel execution: **2.06×** (730ms → 355ms) with 8 workers
  - Predicate pushdown (small dataset): **1.58×** (33.6ms → 21.3ms), 49% memory reduction
  - Predicate pushdown (large dataset): **2.78×** (1,043ms → 375ms), 91.5% memory reduction
- Fixed BenchmarkOHLCQueryLargeDataset transaction size bug (commit per-symbol-per-day)
- Key lesson: **Clean benchmarks reveal precise truth, and predicate pushdown scales better with larger datasets**

### 2025-10-08: Profiling, Hash Join & In-Memory Indexing
- Profiled entire OHLC execution chain (in-memory + storage)
- Identified hash join as 35% of allocations
- Implemented map pre-sizing: 24-32% faster, 24-30% less memory
- Confirmed time range optimization too fast to profile (10µs)
- Identified pattern matching as 58% CPU in-memory → implemented hash indices
- **Massive win**: Entity lookups 49-4802× faster, test suite now 7s (down from timeouts)

### 2025-10-07: Time Range Optimization
- Implemented semi-join pushdown via time range constraints
- Achieved 4× speedup on hourly OHLC (41s → 10.2s)
- Fixed daily OHLC regression with size check
- Optimized extractTimeRanges: 4.7× faster, 108× fewer allocations

### 2026-03-28: LZ77+FSE Compression Codec (Phase 1-3)

Custom-owned LZ77+FSE compression codec for transparent value compression in storage keys.
Implementation: `datalog/codec/compress.go`, `fse.go`, `lz77.go`, `sequences.go`.
Design: `docs/proposals/COMPRESSED_STRING_VALUES.md`, `docs/proposals/COMPRESSION_RESEARCH.md`.

**Compression Ratios (LZ77 + FSE, verified):**

| Data Type | Ratio |
|-----------|-------|
| English prose 1KB | 3.6× |
| EDN structured data | 12.9× |
| Source code | 10.6× |
| Repetitive binary | 12.7× |
| All-same bytes | 13.3× |
| Repeated text 50KB | 110× |

**Full Pipeline Throughput (Apple M5 Max, verified):**

| Operation | 256B | 1KB | 4KB | 16KB |
|-----------|------|-----|-----|------|
| Compress | 14 MB/s | 43 MB/s | 112 MB/s | 284 MB/s |
| Decompress | 1.7 GB/s | 2.1 GB/s | 2.3 GB/s | 2.4 GB/s |

**Per-Value Latency:**

| Operation | 256B | 1KB | 4KB |
|-----------|------|-----|-----|
| Compress | 18μs | 24μs | 37μs |
| Decompress | 153ns | 477ns | 1.8μs |

**Allocations:**

| Operation | 256B | 1KB | 4KB | 16KB |
|-----------|------|-----|-----|------|
| Compress | 38 | 56 | 74 | 103 |
| Decompress | 6 | 7 | 7 | 9 |

**Key optimizations:**
- FSE decode table cached via `sync.Map` — eliminates table reconstruction on repeated decompressions
- Encode work buffers pooled via `sync.Pool` — reduces GC pressure on write path
- LZ77 reconstruct: 2.6 GB/s, 1 alloc (just output buffer)
- Determinism verified: 1000× repeated compression + concurrent goroutine compression produce identical output

**Test coverage:** 185 tests including round-trip fuzz (500 random + 200 text + 100 structured), determinism stress, golden byte-level output assertions, safety net validation, and compression ratio assertions.

### 2025-10-04: Parallel Execution & Intern Optimization
- Identified intern cache as 35% CPU bottleneck
- Replaced sync.RWMutex with sync.Map → 6.26× BadgerDB speedup
- Fixed index selection to use AEVT when E+A both bound
- Performance gains: In-memory 6.9×, BadgerDB 1.63× → 6.26×

### 2026-05-29: Hash-Join Hot-Path Optimizations

Six targeted optimizations to the hash-join inner loops
(`datalog/executor/join.go`, `tuple_key.go`, `compare.go`), each measured in
isolation on an Apple M3 Ultra (go1.25.0). Branch: `perf/hash-join-hot-path`.
Full per-finding analysis: `docs/ideas/HASH_JOIN_HOT_PATH_OPTIMIZATIONS.md`; raw
benchmark artifacts under `docs/perf/` (`*_m3ultra_*.txt`).

**Cumulative (branch start → all six findings), n=10 geomean:**

| Metric | int64 keys | Identity keys (entity refs — real joins) |
|--------|-----------:|-----------------------------------------:|
| sec/op | −14.1% | −24.9% |
| B/op | −3.9% | −3.8% |
| allocs/op | −4.6% | −4.4% |

**Per-finding contribution (geomean, Identity / int64):**

| # | Finding | Identity | int64 |
|---|---------|---------:|------:|
| 1 | Hoist `combineTuples` projection plan out of the inner loop | −8.8% | −9.1% |
| 2 | Short-circuit `ValuesEqual` before reflection | −1.0% | wash |
| 5 | Collapse `seen` dedup double-lookup into `PutIfAbsent` | −1.9% | −1.8% |
| 3 | Hash interned Identity/Keyword by pointer, not content | −12.7% | wash |
| 4 | Drop redundant joined copy; gate probe copy on `RequiresCopy` | −3.8% | −3.7% |
| 6 | Gate debug counters / inline `maybeCopy` closure (cleanup) | — | — |

**Highlights:**
- **#3 (pointer-hash) is the single biggest win.** `Identity`/`Keyword` are
  interned pointer types whose `Equal` is pointer comparison (and panics on
  same-content/different-pointer), so the pointer address is already a unique,
  stable key — hashing the 20-byte SHA1 content was redundant. One load replaces
  a 20-byte array copy plus an FNV loop on every key build, on every
  probe/build/dedup tuple position.
- **#1** removed a per-result-row `map[query.Symbol]bool` allocation plus a double
  symbol scan by precomputing the right-side projection plan once per join.
- **#4** is the only finding that reduced allocations: dropping a doubly-redundant
  per-result-row tuple copy (`combineTuplesIndexed` already returns a fresh slice;
  downstream copies at the `StreamingRelation` boundary) cut **−8 to −9% allocs**
  on streaming-iterator joins, reducing the GC pressure the baseline profiles
  showed dominating. The probe-copy is now gated on `RequiresCopy()` (materialized
  probes skip it).
- **#6** was cleanup below benchmark resolution (one closure heap alloc per join
  call + gated per-row counter writes); not benchmarked.

**Method note:** cross-run benchmark comparisons on this machine are thermally
unreliable at the ~2% noise band — a first uncontrolled run produced spurious
+10–14% "regressions" on the longest-running benchmarks. Every per-finding delta
above was a controlled alternating-order A/B (each binary run in both a cool and a
hot slot, pooled to n=10). The cumulative was confirmed by direct `benchstat` of
the saved start/now files, matching the composed product of per-step geomeans to
within tenths of a percent.
