# Janus Datalog Optimization Opportunities

**Reviewed:** 2026-07-11  
**Status:** Items 1–4 and 7 complete; item 5 includes keyed joins,
semi/anti joins, and OR/fallback/union propagation; item 6 has 6a and four
proven 6b history shapes complete; item 8 ready-predicate scheduling complete

Janus Datalog has already harvested most obvious iterator, CRDT, index, and
hash-join gains. The next meaningful improvements are concentrated in typed
grouping, explicit physical properties, and proving when set-semantics work can
be omitted.

## Summary

| Rank | Opportunity | Layer | Evidence | Expected payoff | Effort |
|---:|---|---|---|---|---|
| 1 | Replace string aggregation keys with `TupleKeyMap` | Low-level | Implemented and benchmarked | 47.5% faster geomean | Complete |
| 2 | Use `PutIfAbsent` across deduplication paths | Low-level | Implemented and benchmarked | 7.3% faster geomean | Complete |
| 3 | Introduce a real Top-N physical operator | Relational algebra | Implemented and benchmarked | 97.1% faster geomean | Complete |
| 4 | Fuse whole same-entity attribute bundles | Relational algebra | Implemented and benchmarked | 13.5% faster geomean | Complete |
| 5 | Propagate statically provable relational properties | Relational algebra | Keys consumed inside joins, projections, fallback, and union | Up to 32.8% faster focused paths | In progress |
| 6 | Push Top-N into proven index order | Relational algebra | 6a + four 6b history shapes implemented | Up to 99.7% faster | In progress |
| 7 | Compile storage-bound hash matching once | Low-level | Implemented and benchmarked | 32.8% fewer allocations | Complete |
| 8 | Turn the algebra optimizer into a compositional optimizer | Relational algebra | Ready-predicate scheduling implemented | 62.9% faster focused path | In progress |

## 1. Replace String Aggregation Keys with `TupleKeyMap`

**Status:** Complete in the current working tree.

Both grouped aggregation paths previously serialized group values into
delimiter-joined strings. Besides allocating on every row, that representation
merged distinct typed groups such as string `"1"` and `int64(1)`.

Both paths now key their groups directly with `TupleKeyMap`, reusing the same
typed hashing and `ValuesEqual` semantics as joins and deduplication. Batch
aggregation stores each `batchAggregateGroup` as the map value; streaming
aggregation stores each `streamingAggregateGroup`. A parallel group-order slice
provides deterministic result traversal because `TupleKeyMap` is lookup-only.

Relevant code:

- `datalog/executor/aggregation.go:277-418`
- `datalog/executor/aggregation.go:697-853`
- `datalog/executor/aggregation_key_test.go`

### Measured evidence

`BenchmarkGroupedAggregationKeying`, 10,000 rows and 100 two-column groups,
`benchtime=500ms`, `count=10`:

| Mode | Time before | Time after | Delta | Bytes delta | Allocations delta |
|---|---:|---:|---:|---:|---:|
| Batch | 1,369.6 µs | 681.4 µs | **-50.25%** | -14.43% | -70.02% |
| Streaming | 926.0 µs | 512.5 µs | **-44.66%** | -35.60% | -72.48% |
| Geomean | 1,126 µs | 590.9 µs | **-47.53%** | **-25.77%** | **-71.28%** |

All differences are significant at `p=0.000`, `n=10`. Adversarial correctness
tests cover delimiter shifts and int, bool, and float values paired with their
string renderings in both batch and streaming modes. The full
`go test -count=1 ./...` suite passes.

Source: Go benchmark plus `benchstat`, darwin/arm64, 2026-07-11.

## 2. Use `PutIfAbsent` Across Deduplication Paths

**Status:** Complete in the current working tree.

Eight production paths implemented set insertion as `Exists` followed by `Put`,
even though `TupleKeyMap.PutIfAbsent` already performs the operation in one
bucket walk. They now call `PutIfAbsent` directly. This remains a local
implementation optimization: it does not skip deduplication, change set
semantics, or require relational property propagation.

The changed paths cover materialized and streaming deduplication, union,
subquery input combinations, relation operations, and symmetric hash joins.
A direct contract test pins first-insert/repeated-insert behavior and verifies
that a repeated call does not replace the original map value.

### Measured evidence

`BenchmarkDedupInsertionPaths`, 10,000 two-column Identity/string tuples,
`benchtime=500ms`, `count=10`:

| Workload | Mode | Time before | Time after | Delta |
|---|---|---:|---:|---:|
| Unique-heavy | Materialized | 504.3 µs | 460.2 µs | **-8.74%** |
| Unique-heavy | Streaming | 492.1 µs | 447.9 µs | **-8.99%** |
| Duplicate-heavy | Materialized | 260.6 µs | 246.6 µs | **-5.36%** |
| Duplicate-heavy | Streaming | 270.3 µs | 253.5 µs | **-6.23%** |
| **Geomean** | | 363.6 µs | 336.9 µs | **-7.34%** |

Memory and allocation counts are unchanged, as expected: this step removes
redundant lookup work without changing `TupleKeyMap` storage. Every timing
comparison is statistically significant (`p≤0.019`, `n=10`). The full
`go test -count=1 ./...` suite passes.

A specialized `TupleSet` is not part of this step. Consider one only if a later
memory profile shows that `mapEntry.value` materially contributes to the
remaining cost.

Relevant code and design:

- `datalog/executor/iterator_composition.go:400-425`
- `datalog/executor/relation.go:420-438`
- `datalog/executor/tuple_key.go:309-327`
- `datalog/executor/dedup_put_if_absent_test.go`
- `docs/proposals/TUPLEKEYMAP_OPTIMIZATION.md`

## 3. Introduce a Real Top-N Physical Operator

**Status:** Complete in the current working tree.

`ORDER BY` currently collects every tuple and fully sorts it before `LIMIT`
truncates the result. `TopNRelation` now replaces that path with a bounded
worst-first heap when no non-projected sort symbols require a later
deduplicating projection.

The implementation:

1. Drain the source into a bounded worst-first heap containing the best N rows.
2. Sort only those N rows before returning them.
3. Preserve `RequiresCopy`, deferred iterator errors, multi-key ordering, global
   `RelationInput` finalization, aggregation, and pull rendering.

This changes CPU and memory complexity from O(M log M) time and O(M) memory to
O(M log N) time and O(N) memory. It still scans all M rows; storage scan
reduction remains the separate index-order optimization below.

Queries with non-projected sort symbols retain the full-sort path. Their current
sequence is sort, deduplicating projection, then limit; taking N before a
projection that collapses rows can return fewer than N results and miss
lower-ranked distinct rows.

Test-first proof:

- Differential tests against `SortRelation` followed by `LimitRelation`.
- Ascending, descending, multi-key, ties, and N values of 0, 1, below/equal to/
  above the input size.
- Materialized and workspace-reusing streaming inputs.
- Aggregated and global `RelationInput` results.
- Deferred iterator errors after a clean prefix.
- Benchmarks at 10K and 100K rows with N = 1, 10, and 100.

### Measured evidence

`BenchmarkOrderedLimit`, 10K/100K rows, N = 1/10/100, materialized and
streaming,
`benchtime=300ms`, `count=10`, darwin/arm64:

| Rows | Limit | Mode | Time before | Time after | Delta |
|---:|---:|---|---:|---:|---:|
| 10,000 | 1 | Materialized | 2.233 ms | 62.91 µs | **-97.18%** |
| 10,000 | 1 | Streaming | 2.947 ms | 75.06 µs | **-97.45%** |
| 100,000 | 1 | Materialized | 26.10 ms | 631.6 µs | **-97.58%** |
| 100,000 | 1 | Streaming | 34.16 ms | 776.6 µs | **-97.73%** |
| **12-case geomean** | N = 1/10/100 | Both | **9.023 ms** | **259.8 µs** | **-97.12%** |

Geomean memory falls from 10.79 MiB to 3.985 KiB (**-99.96%**), and
allocations from 55.06K to 77.84 (**-99.86%**). Every comparison is significant
at `p=0.000`, `n=10`. The full `go test -count=1 ./...` suite passes.

Relevant code and design:

- `datalog/executor/executor_utils.go:125-178`
- `datalog/executor/executor.go:220-262`
- `datalog/executor/limit_relation.go:34-61`
- `datalog/executor/top_n.go`
- `datalog/executor/top_n_test.go`
- `datalog/executor/top_n_benchmark_test.go`

## 4. Fuse Whole Same-Entity Attribute Bundles

**Status:** Complete in the current working tree.

The existing cardinality-one fetch fusion is a strong specialization, but it
handles one pattern at a time and materializes a fresh relation after every
attached attribute. K attributes still require K passes and K intermediate
tuple sets.

The executor now recognizes only a contiguous run of already-fusable
`[?e :constant ?fresh]` patterns. It walks the outer tuples once, performs the
same K `LookupAttribute` calls, and allocates one final-width tuple. Existing
cardinality/temporal gates and per-attribute `pattern/fused-fetch` events are
preserved. No matcher interface, adaptive threshold, or entity-wide scan was
added.

### Measured evidence

`BenchmarkAttrFetch`,
`benchtime=500ms`, `count=10`, darwin/arm64:

| Entities | Attributes | Time before | Time after | Time delta | Memory delta | Alloc delta |
|---:|---:|---:|---:|---:|---:|---:|
| 100 | 1 | 67.53 µs | 65.61 µs | -2.85% | ~ | +0.07% |
| 100 | 3 | 101.0 µs | 86.50 µs | **-14.33%** | -32.99% | -19.37% |
| 100 | 6 | 152.4 µs | 117.2 µs | **-23.09%** | -49.99% | -32.03% |
| 1,000 | 1 | 517.3 µs | 528.8 µs | ~ | +0.02% | +0.01% |
| 1,000 | 3 | 859.4 µs | 729.9 µs | **-15.06%** | -37.28% | -21.91% |
| 1,000 | 6 | 1.352 ms | 1.016 ms | **-24.83%** | -56.50% | -36.46% |
| **Geomean** | | **292.4 µs** | **252.8 µs** | **-13.54%** | **-32.88%** | **-19.50%** |

K=1 remains effectively unchanged, while gains scale with bundle width as
intended. The full `go test -count=1 ./...` suite passes.

### Profile evidence

For 1,000 entities and six attributes before bundling:

- `tryFuseAttributeFetch` accounts for **16.5% cumulative CPU**.
- It accounts for **82.6% cumulative allocated space** and **30.9% flat**.
- Per-attribute tuple creation and result-slice growth allocate 6.29 GiB over
  the profile run.
- Repeated `NewMaterializedRelationWithOptions` calls contribute 7.34 GiB
  cumulatively through deduplication.

The profile directly confirms that repeated traversal, tuple widening, and
materialization are the remaining costs. CPU is dominated by runtime scheduler
signaling, consistent with the allocation pressure.

After bundling, `tryFuseAttributeFetchBundle` drops to 63.2% cumulative and
15.0% flat allocated space while total per-operation memory falls from
2.365 MiB to 1.029 MiB.

Relevant code and benchmarks:

- `datalog/executor/query_executor.go:389-512`
- `datalog/executor/attribute_fetch_bundle_test.go`
- `datalog/storage/attr_fetch_bench_test.go`
- `datalog/storage/same_entity_fusion_test.go`
- `PERFORMANCE_STATUS.md:30`

## 5. Propagate Statically Provable Relational Properties

**Status:** Core interface contract, first storage guarantees, key-preserving
natural joins, semi/anti filtering, and OR/fallback/union rules complete;
propagation coverage remains in progress.

The query syntax, selected index, schema, and `Relation` type already establish
facts such as candidate keys, uniqueness, ordering, and rewindability, but those
facts are not carried explicitly. Preserving them can enable safe deduplication
and sort elimination without collected statistics or cost-based planning.

`Relation.Properties()` now carries ordering and candidate keys using Datalog
symbols and `OrderByClause` values. Constructors copy external property values;
callers treat returned properties as immutable under the existing Relation
contract.

Implemented propagation:

- Storage establishes E ordering and entity uniqueness for proven
  CardinalityOne AETV and validated AVET scans.
- Filters, limits, materialization, lazy wrapping, and same-entity bundle
  attachment preserve properties.
- Projection retains only the valid ordering prefix and keys whose symbols
  survive.
- Sort establishes its requested ordering.
- Fresh expression outputs preserve properties; replacing a property-bearing
  symbol clears affected guarantees.
- Grouped aggregation establishes its group-by symbols as a candidate key.
- Natural joins preserve one side's candidate keys when the opposite side's
  join symbols contain a candidate key. Join ordering remains unclaimed.
- Hash joins omit their internal full-tuple `seen` table when the derived result
  key already proves every output tuple unique.
- Hash joins store a single tuple directly when the build join symbols contain
  a candidate key; non-unique build keys retain fanout slices.
- Semi-joins and anti-joins preserve all left ordering and candidate keys
  because they only filter left rows.
- Short-circuit fallback preserves unaffected outer keys when each branch is
  statically at-most-one row per outer tuple. Multi-row fallback and correlated
  union derive composite outer-plus-output keys after typed deduplication.
- Eager and channel-backed set unions establish their full output schema as a
  candidate key; smaller branch keys are not preserved without a cross-branch
  disjointness proof.
- Products still clear properties until a derivation rule proves otherwise.
- Streaming projection skips deduplication when a candidate key survives.

No persistent cardinality statistics, cost model, heuristic override, or
planner metadata was added. The syntax-visible greedy planner remains the
planning model.

Relevant code:

- `datalog/planner/clause_utils.go:602-686`
- `datalog/algebra/types.go:53-84`
- `datalog/executor/relation.go`
- `datalog/executor/relation_properties.go`
- `datalog/storage/relation_properties.go`

### Complex-query checkpoint

The existing production-shaped optimization-matrix query is now a reusable
schema-backed benchmark with `:limit 25`. It exercises phase planning, joins,
same-entity bundles, correlated and nested subqueries, conditional aggregation,
`get-else`, `or-default`, expressions, ordering, and bounded Top-N over 75
scenarios × 100 tasks.

`BenchmarkComplexQueryCheckpoint`, steady-state public query API,
`benchtime=1s`, `count=10`, darwin/arm64:

- **47.88 ms/op ±2%**
- **87.57 MiB/op**
- **1.118M allocations/op**

The allocation profile is dominated cumulatively by `HashJoinWithOptions` and
`OrFallbackIterator.nextShortCircuit` (overlapping call stacks). Direct
allocation leaders are `TupleKeyMap.PutIfAbsent` (16.5%),
`NewTupleKeyMapWithCapacity` (16.5%), and `TupleKeyMap.Put` (11.3%). This is the
checkpoint against which property-driven work is measured.

After property propagation and key-aware streaming projection:

- Time: **48.48 ms/op**, statistically unchanged (`p=0.247`)
- Memory: **82.80 MiB/op**, **-5.45%**
- Allocations: **1.088M/op**, **-2.71%**

### Key-preserving join checkpoint

When the right join symbols contain a candidate key, each left row can match at
most one right row, so left candidate keys remain valid after the join. The
mirror rule preserves right keys when the left join symbols contain a candidate
key. Streaming, materialized, and symmetric hash joins apply the same rule;
ordering remains empty because hash-join output order is not a contract.

`BenchmarkKeyPreservingJoinProjection`, streaming one-to-one hash join followed
by projection retaining the left key, `benchtime=300ms`, `count=10`,
darwin/arm64:

| Rows | Time before | Time after | Time delta | Memory delta | Alloc delta |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 3.337 ms | 2.564 ms | **-23.19%** | -26.01% | -9.14% |
| 100,000 | 38.48 ms | 28.62 ms | **-25.63%** | -24.29% | -9.13% |
| **Geomean** | **11.33 ms** | **8.565 ms** | **-24.42%** | **-25.15%** | **-9.13%** |

The production-shaped `BenchmarkComplexQueryCheckpoint` is statistically
unchanged: 47.54 → 48.90 ms/op (`p=0.075`), 82.80 MiB/op (`p=0.739`), and
1.088M allocations/op (`p=0.648`). The focused win is real, but the default
checkpoint does not currently contain a streaming key-preserving
join-then-projection bottleneck.

The second join-key step consumes the same proof inside the hash join itself.
When a result candidate key exists, streaming, materialized, and symmetric hash
joins omit the full-result `seen` table and per-row `NewTupleKeyFull` call.
Against the already key-propagating focused baseline:

| Rows | Time before | Time after | Time delta | Memory delta | Alloc delta |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 2.564 ms | 1.782 ms | **-30.48%** | -33.61% | -10.05% |
| 100,000 | 28.62 ms | 18.60 ms | **-35.00%** | -31.92% | -10.04% |
| **Geomean** | **8.565 ms** | **5.758 ms** | **-32.77%** | **-32.77%** | **-10.05%** |

The complex checkpoint remains statistically unchanged: 49.17 → 50.07 ms/op
(`p=0.579`), 82.80 → 82.82 MiB/op (`p=0.247`), and 1.088M allocations/op
(`p=0.268`). Its dominant joins do not currently carry candidate keys through
the OR/fallback and subquery shapes that feed them.

The third join-key step specializes the build hash table itself. When the build
side's join symbols contain a candidate key, each hash entry stores one `Tuple`
directly instead of allocating a one-element `[]Tuple`; non-unique builds retain
the fanout slice and duplicate-key control path. Against the keyed,
dedup-eliding focused baseline:

| Rows | Time before | Time after | Time delta | Memory delta | Alloc delta |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 1.782 ms | 1.680 ms | **-5.72%** | -7.10% | -11.10% |
| 100,000 | 18.60 ms | 17.62 ms | **-5.30%** | -7.40% | -11.11% |
| **Geomean** | **5.758 ms** | **5.441 ms** | **-5.51%** | **-7.25%** | **-11.11%** |

The complex checkpoint remains statistically unchanged: 47.94 → 48.66 ms/op
(`p=0.218`), 82.80 → 82.81 MiB/op (`p=0.912`), and 1.088M allocations/op
(`p=0.535`). This confirms that the dominant fallback/subquery joins still lack
candidate-key proofs at their build boundaries.

### Semi/anti join checkpoint

Semi-joins and anti-joins iterate the left relation in order and only choose
whether to emit each row. They therefore preserve every left property. When a
left candidate key exists, the result materialization also skips redundant
deduplication; unkeyed internal relations retain the existing deduplicating
control path.

`BenchmarkSemiAntiJoinPropertyPropagation`, 50%-selective keyed left input,
`benchtime=300ms`, `count=10`, darwin/arm64:

| Operation | Rows | Time before | Time after | Time delta | Memory delta | Alloc delta |
|---|---:|---:|---:|---:|---:|---:|
| Semi | 10,000 | 786.4 µs | 550.0 µs | **-30.07%** | -34.38% | -20.01% |
| Anti | 10,000 | 782.2 µs | 550.4 µs | **-29.63%** | -34.38% | -20.01% |
| Semi | 100,000 | 8.376 ms | 6.087 ms | **-27.33%** | -31.19% | -20.03% |
| Anti | 100,000 | 8.162 ms | 6.292 ms | **-22.91%** | -31.19% | -20.03% |
| **Geomean** | | **2.547 ms** | **1.845 ms** | **-27.54%** | **-32.80%** | **-20.02%** |

The complex checkpoint is again statistically unchanged: 48.22 → 49.17 ms/op
(`p=0.089`), with 82.80 MiB/op (`p=0.853`) and 1.088M allocations/op
(`p=0.590`) unchanged.

### OR/fallback and union checkpoint

`or-default` now distinguishes singleton branches from multi-row branches.
Scalar/tuple subquery bindings, non-expanding expressions, and decorrelated
aggregate relation bindings whose group keys are outer-bound emit at most one
row per outer tuple; they preserve unaffected outer keys. Multi-row fallback
and correlated union perform typed full-tuple deduplication and derive the
conservative key `outer key + branch output symbols`. Ordinary eager and
channel-backed unions advertise only the full output key because keys that are
valid within each branch can still collide across branches.

The production query demonstrates both categories: its first two aggregate
fallbacks preserve `?scenario`; the argmax fallback is relation-valued after
decorrelation and therefore carries
`[?scenario ?lastKey ?lastUpdatedAt]`, not the unsound singleton key
`[?scenario]`.

A deterministic 500-case randomized differential test mixes singleton ground
expressions, multi-row data patterns, expanding `enumerate` expressions,
short-circuit fallback, and correlated union over randomly shaped outer
relations. It compares the optimized iterator against independent per-outer
branch evaluation, then validates every advertised candidate key and ordering
against the produced tuples. The first run exposed a vector-value panic in
fallback correlation filtering; that path now delegates slice equality to the
canonical typed `datalog.ValuesEqual` implementation and has a focused
regression test.

`BenchmarkOrFallbackPropertyPropagation`, keyed vs unproven outer input,
`benchtime=300ms`, `count=10`, darwin/arm64:

| Rows | Time before | Time after | Time delta | Memory delta | Alloc delta |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 8.692 ms | 7.509 ms | **-13.61%** | -9.24% | -3.59% |
| 100,000 | 84.52 ms | 70.54 ms | **-16.54%** | -8.00% | -3.59% |
| **Geomean** | **27.10 ms** | **23.01 ms** | **-15.09%** | **-8.62%** | **-3.59%** |

The current complex checkpoint has a 51.98 ms median, 84.05 MiB/op, and 1.043M
allocations/op (`n=10`). Compared with the preceding documented checkpoint,
allocations are about 4.2% lower, while wall time and memory do not show a gain
across separate runs. The post-change allocation profile still attributes
48.4% cumulative space to overlapping fallback/hash-join stacks; direct leaders
remain hash-table construction and insertion.

### Join materialization check

`BenchmarkComplexQueryJoinMaterialization` runs the same production-shaped
query with only `EnableStreamingJoins` changed:

| Mode | Time | Memory | Allocations |
|---|---:|---:|---:|
| Materialized (default) | 48.57 ms | 82.78 MiB | 1.088M |
| Streaming | 52.48 ms | 85.91 MiB | 1.179M |
| Delta | **+8.04%** | **+3.78%** | **+8.34%** |

Every difference is significant (`p=0.000`, `n=10`). Materialization is not an
accidental regression for this workload: streaming adds iterator composition
and caching costs without avoiding the join's own result deduplication. The
streaming CPU profile attributes 12.15% cumulative time to
`hashJoinIterator.Next`, 8.68% to `TupleKeyMap.PutIfAbsent`, 7.99% to
`ProjectIterator.Next`, and 7.64% to `deduplicateTuples`.

Keep materialized joins as the default. The narrower internal-`seen` elision is
now implemented for proven keyed results; its focused gain does not change the
default materialization conclusion.

Ordering is carried but not yet consumed for storage early termination; that
remains the separate index-order Top-N step.

Remaining property work:

- Derive smaller ordinary-union keys only where branch domains are provably
  disjoint.
- Propagate join ordering only if an execution strategy establishes it.
- Establish guarantees for additional storage index and binding strategies.
- Consume ordering in index-order Top-N.
- Re-run the complex checkpoint after each new derivation rule.

Relevant benchmark:

- `datalog/storage/optimization_matrix_test.go`

## 6. Push Top-N into Proven Index Order

**Status:** 6a complete; history/ATEV, history/TAEV, history/AETV, and
history/EATV 6b shapes complete; broader order-aware index selection pending.

Bounded Top-N still consumes every source row. After property propagation can
prove that a scan's physical order satisfies `ORDER BY`, the storage iterator
can instead stop after N rows. This is the optimization that turns latest-1 and
keyset-page queries into bounded range reads.

### 6a. Consume existing ordering guarantees

The first slice requires no planner or index-selection change. When the final
relation already satisfies the Datalog `:order-by`, finalization now applies
`LimitRelation` directly instead of bounded Top-N. The streaming limit closes
the storage iterator after N rows. Non-projected sort keys still project and
deduplicate before limiting.

**Measurement** (`BenchmarkIndexOrderedLimit`, 10,000 CardinalityOne entities,
`benchtime=500ms`, `count=10`, darwin/arm64):

| Direction | N | Time before | Time after | Time delta | Scans before | Scans after |
|---|---:|---:|---:|---:|---:|---:|
| E ascending | 1 | 2.057 ms | 7.295 µs | **-99.65%** | 10,000 | 1 |
| E ascending | 10 | 2.065 ms | 10.95 µs | **-99.47%** | 10,000 | 10 |
| E ascending | 100 | 2.077 ms | 38.93 µs | **-98.13%** | 10,000 | 100 |
| E descending | 1 | 2.235 ms | full-scan control | — | 10,000 | 10,000 |
| E descending | 10 | 2.914 ms | full-scan control | — | 10,000 | 10,000 |
| E descending | 100 | 3.377 ms | full-scan control | — | 10,000 | 10,000 |

For satisfied ascending order, memory falls 97.0–99.4% and allocations
98.0–99.6%. Descending allocation counts are identical before/after; its
wall-time samples remain noisy despite the unchanged full-scan path.

Relevant benchmark:

- `datalog/storage/index_order_limit_benchmark_test.go`

### 6b. Order-aware index selection

**Status:** Datalog-fragment matcher contract plus history/ATEV, history/TAEV,
history/AETV, and history/EATV shapes complete; additional index/order shapes
pending.

`PatternMatcher.Match` now accepts a one-pattern Datalog
`query.Query` fragment rather than adding an order-specific interface or opaque
request metadata. `OrderBy` and `Limit` appear on that fragment only after
structural checks prove pushdown safe. Storage still validates index and CRDT
semantics; custom sources may ignore the ordering request.

The first supported shape is history mode with constant A and
`[?tx :desc] [?e :asc]`, where ATEV physically provides
`[A constant][Tx↓][E][V]`. Latest/as-of must decline because Tx-primary ATEV
does not group E/A for current-state CRDT resolution.

**Measurement** (`BenchmarkHistoryIndexOrderedLimit`, 10,000 raw history datoms,
`benchtime=500ms`, `count=10`, darwin/arm64):

| N | Time before | Time after | Time delta | Memory delta | Alloc delta | Scans before | Scans after |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.752 ms | 13.48 µs | **-99.51%** | -99.44% | -99.31% | 10,000 | 1 |
| 10 | 2.842 ms | 17.91 µs | **-99.37%** | -99.30% | -99.14% | 10,000 | 10 |
| 100 | 2.808 ms | 55.28 µs | **-98.03%** | -97.89% | -97.71% | 10,000 | 100 |
| **Geomean** | **2.800 ms** | **23.72 µs** | **-99.15%** | **-99.07%** | **-98.89%** | | |

The correctness test verifies ATEV selection, exact Datalog order, and at most
N scans. A companion latest-mode test verifies Tx-primary ATEV is declined.

The second supported shape is the unfiltered history transaction log:
`[?e ?a ?v ?tx]` ordered by `[?tx :desc] [?a :asc] [?e :asc]`. TAEV physically
provides `[Tx↓][A][E][V]`; per-operation ElementIDs make Tx itself a total
ordering key. Requiring all four pattern positions to be variables excludes
post-scan filtering, so a limit of N consumes exactly N storage datoms.
Latest/as-of again decline because global Tx order cannot perform current-state
CRDT resolution.

**Measurement** (`BenchmarkHistoryTAEVOrderedLimit`, 10,100 raw history datoms,
`benchtime=500ms`, `count=10`, darwin/arm64):

| N | Time before | Time after | Time delta | Memory delta | Alloc delta | Scans before | Scans after |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.916 ms | 15.46 µs | **-99.47%** | -99.44% | -99.27% | 10,100 | 1 |
| 10 | 2.907 ms | 19.53 µs | **-99.33%** | -99.30% | -99.11% | 10,100 | 10 |
| 100 | 3.445 ms | 57.23 µs | **-98.34%** | -97.86% | -97.70% | 10,100 | 100 |
| **Geomean** | **3.079 ms** | **25.85 µs** | **-99.16%** | **-99.05%** | **-98.86%** | | |

The third supported shape is constant-attribute raw history ordered by entity
and operation: `[?e :event/value ?v ?tx]` with
`[?e :asc] [?tx :desc]`. AETV already provides
`[A constant][E][Tx↓][V]`, so this extension proves and consumes the existing
physical property rather than changing the ordinary index choice. Requiring E,
V, and Tx to remain variables excludes value filtering. Latest/as-of continue
to use AETV for CRDT resolution but do not inherit the raw-history two-key
property.

**Measurement** (`BenchmarkHistoryAETVOrderedLimit`, 10,000 raw attribute
history datoms, `benchtime=500ms`, `count=10`, darwin/arm64):

| N | Time before | Time after | Time delta | Memory delta | Alloc delta | Scans before | Scans after |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.713 ms | 14.42 µs | **-99.47%** | -99.45% | -99.31% | 10,000 | 1 |
| 10 | 2.714 ms | 18.11 µs | **-99.33%** | -99.31% | -99.14% | 10,000 | 10 |
| 100 | 2.983 ms | 51.72 µs | **-98.27%** | -97.89% | -97.70% | 10,000 | 100 |
| **Geomean** | **2.801 ms** | **23.82 µs** | **-99.15%** | **-99.07%** | **-98.89%** | | |

The fourth supported shape mirrors AETV for a constant entity:
`[#identity "…" ?a ?v ?tx]` ordered by `[?a :asc] [?tx :desc]`. EATV provides
`[E constant][A][Tx↓][V]`. The benchmark fixture places 100 attributes × 100
versions under one entity, and the safety contract requires A, V, and Tx to be
variables. Latest/as-of continue using EATV's CRDT-resolved path without the
raw-history two-key property.

**Measurement** (`BenchmarkHistoryEATVOrderedLimit`, 10,000 raw entity-history
datoms, `benchtime=500ms`, `count=10`, darwin/arm64):

| N | Time before | Time after | Time delta | Memory delta | Alloc delta | Scans before | Scans after |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2.740 ms | 13.80 µs | **-99.50%** | -99.38% | -99.28% | 10,000 | 1 |
| 10 | 2.693 ms | 17.84 µs | **-99.34%** | -99.24% | -99.12% | 10,000 | 10 |
| 100 | 2.722 ms | 53.16 µs | **-98.05%** | -97.83% | -97.67% | 10,000 | 100 |
| **Geomean** | **2.718 ms** | **23.57 µs** | **-99.13%** | **-99.00%** | **-98.86%** | | |

Start narrowly:

1. Single-pattern transaction ordering on EATV, ATEV, or TAEV.
2. No joins that can filter a selected row.
3. No aggregation between the scan and limit.
4. CRDT resolution must preserve the claimed order.
5. Requested ordering must be total, or the optimization is declined.

Each supported shape requires differential tests against the existing
materialize-sort-project-limit path with realistic data sizes. Extend to other
index layouts only after the narrow transaction-order case is proven.

Relevant code and design:

- `docs/bugs/resolved/BUG_QUERY_LIMIT_CLAUSE_UNSUPPORTED.md:274-316`
- `datalog/storage/key_encoder_binary.go`
- `datalog/storage/matcher_strategy.go`
- `datalog/storage/history_index_order_limit_test.go`
- `datalog/storage/history_index_order_limit_benchmark_test.go`

## 7. Compile Storage-Bound Hash Matching Once

**Status:** Complete.

The storage hash path previously converted every build and probe key to a
string, sometimes through `fmt`, and rebuilt a symbol-index map plus
pattern-position bindings for every candidate tuple. It now:

- Uses `TupleKeyMap.PutValue`/`GetValue` for typed, allocation-free probe keys.
- Compiles constants and binding tuple indices into a `bindingMatchPlan` once
  per iterator.
- Propagates binding iterator errors instead of silently accepting a partial
  hash set.
- Correctly supports empty strings and content-hashed byte slices as keys.

`BenchmarkStorageHashJoinCompiledMatching` uses a cache-disabled 10,000-datom
AVET scan with 50 bound reference values, `benchtime=500ms`, `count=10`,
darwin/arm64:

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Time | 2.010 ms | 1.867 ms | **-7.16%** |
| Memory | 1.460 MiB | 1.231 MiB | **-15.65%** |
| Allocations | 30.15K | 20.25K | **-32.83%** |

All differences are significant (`time p=0.001`; memory/allocations `p=0.000`,
`n=10`). The baseline CPU profile attributed 3.98% cumulative time to
`valueToHashKey` and 3.10% to per-candidate `matchesWithBindingTuple`; both
disappear from the post-change profile. Storage decoding, CRDT iteration, and
scheduler wakeups now dominate this cache-disabled workload.

Relevant code:

- `datalog/executor/tuple_key.go`
- `datalog/storage/hash_join_matcher.go`
- `datalog/storage/storage_hash_join_compiled_benchmark_test.go`

## 8. Turn the Algebra Optimizer into a Compositional Optimizer

The relational algebra IR is expressive, but the default optimizer currently
registers only decorrelation and the `get-else` scan rewrite. It performs one
bottom-up application per pass, then decompiles to clauses for heuristic
phasing.

The first attempted `Select` rewrite exposed an interface boundary: algebra
decompiles `Select(Join(...))` to a predicate immediately after its dependency,
but greedy phasing then scored another data pattern (~210) above the ready
predicate (5), erasing the rewrite's execution effect. The selected prerequisite
keeps the single `RealizedPlan` architecture and changes only clause scheduling:
once every required symbol is available, a predicate is placed before unrelated
remaining scans. Predicates spanning two scans still wait for both.

`BenchmarkReadyPredicateScheduling`, 10,000 entities with a 99-row selective
filter before a payload scan, `benchtime=500ms`, `count=10`, darwin/arm64:

| Metric | Before | After | Delta |
|---|---:|---:|---:|
| Time | 6.865 ms | 2.550 ms | **-62.85%** |
| Memory | 10.003 MiB | 1.288 MiB | **-87.12%** |
| Allocations | 150.65K | 31.81K | **-78.88%** |

All differences are significant (`p=0.000`, `n=10`). The complex checkpoint is
statistically unchanged: 48.37 → 47.94 ms/op (`p=0.143`), with memory
(`p=0.912`) and allocations (`p=0.810`) unchanged.

True compositional algebra work remains: emit phased Datalog directly or
otherwise preserve tree dependencies across the bridge, add dependency-safe
`Project` pushdown, and run passes to a structural fixpoint with cycle
protection. Only then consider join associativity or DAG-based
common-subexpression elimination.

Relevant code:

- `datalog/algebra/optimize.go:33-84`
- `datalog/algebra/rewrite_decorrelate.go`
- `datalog/planner/algebra_bridge.go:11-56`
- `datalog/planner/clause_utils.go`
- `datalog/storage/ready_predicate_scheduling_benchmark_test.go`

## Do Not Prioritize Yet

- Recent hash-join inner-loop changes are already measured and effective.
- Planning latency is measured in microseconds.
- Scan sharing and entity prefetch are benchmarked as neutral in the default
  workload.
- Symmetric hash join trades throughput for time-to-first-row.
- Streaming hash joins are **8.04% slower**, use **3.78% more memory**, and
  allocate **8.34% more** on the complex checkpoint; keep them opt-in.
- Do not tune the current merge join before its ordering contract is explicit.
  It sorts full tuples lexicographically while extracting a join key by datom
  position. Physical properties should represent the required order first.

## Suggested Sequence

1. **Completed:** typed aggregation keys via `TupleKeyMap`.
2. **Completed:** `PutIfAbsent` across all add-if-absent deduplication paths.
3. **Completed:** bounded Top-N heap reducing CPU and memory while retaining the
   full scan.
4. **Completed:** one-pass same-entity attribute bundle fusion.
5. **Checkpointed:** core relation-property contract, key-aware projection,
   key-preserving natural/semi/anti joins, and OR/fallback/union propagation.
   Cross-branch disjointness, join ordering, and broader storage derivations
   remain.
6. **In progress:** 6a plus history/ATEV, history/TAEV, history/AETV, and
   history/EATV 6b shapes are complete; additional CRDT-safe order-aware index
   selections remain.
7. **Completed:** typed storage hash keys and precompiled pattern/binding slots.
8. **In progress:** ready predicates now run before unrelated scans; phased
   algebra output, `Project` pushdown, and fixpoint rewriting remain.
