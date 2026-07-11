# Janus Datalog Optimization Opportunities

**Reviewed:** 2026-07-11  
**Status:** Item 1 implemented and verified; remaining items require focused
benchmarks before implementation

Janus Datalog has already harvested most obvious iterator, CRDT, index, and
hash-join gains. The next meaningful improvements are concentrated in typed
grouping, explicit physical properties, and proving when set-semantics work can
be omitted.

## Summary

| Rank | Opportunity | Layer | Evidence | Expected payoff | Effort |
|---:|---|---|---|---|---|
| 1 | Replace string aggregation keys with `TupleKeyMap` | Low-level | Implemented and benchmarked | 47.5% faster geomean | Complete |
| 2 | Make duplicate elimination property-aware | Relational algebra | Measured tax and direct code path | High on projections and joins | Medium–high |
| 3 | Introduce a real Top-N physical operator | Relational algebra | Confirmed execution shape | Very high when N ≪ rows | Medium |
| 4 | Fuse whole same-entity attribute bundles | Relational algebra | Single-column fusion already measured | High for EAV star joins | Medium |
| 5 | Propagate statically provable relational properties | Relational algebra | Derivable from existing contracts | Cross-layer | Medium–high |
| 6 | Compile storage-bound hash matching once | Low-level | Code-audit candidate | Medium on cold or uncached joins | Low–medium |
| 7 | Turn the algebra optimizer into a compositional optimizer | Relational algebra | Pass inventory confirmed | Long-term high | High |

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

## 2. Make Duplicate Elimination Property-Aware

Projection always restores set semantics with a hash set, and hash join always
deduplicates output. Many EAV plans preserve a known key, however, so duplicates
are impossible. The projection set-semantics fix previously measured a 25% time
increase and 6.6× memory increase.

Start locally by using `PutIfAbsent` in `DedupIterator`, removing its remaining
double bucket walk. Then propagate candidate keys and uniqueness through
`Scan`, `Select`, `Map`, `Project`, and `Join`. Skip deduplication only when the
property proves that doing so preserves set semantics.

Relevant code and design:

- `datalog/executor/iterator_composition.go:400-425`
- `datalog/executor/join.go:80-92`
- `docs/proposals/TUPLEKEYMAP_OPTIMIZATION.md`

## 3. Introduce a Real Top-N Physical Operator

`ORDER BY` currently collects every tuple and fully sorts it before `LIMIT`
truncates the result. Latest-1 and top-10 workloads therefore remain
O(M log M) time and O(M) memory.

Implement this in stages:

1. Use a bounded heap with input ordinal as a tie-breaker. This gives
   O(M log N) time and O(N) memory for every ordered limit.
2. Recognize index-aligned single-scan cases and stop EATV, ATEV, or TAEV scans
   after N results.
3. Decline pushdown across aggregation or joins that may filter higher-ranked
   rows.

Relevant code and design:

- `datalog/executor/executor_utils.go:125-178`
- `datalog/executor/limit_relation.go:34-61`
- `docs/bugs/resolved/BUG_QUERY_LIMIT_CLAUSE_UNSUPPORTED.md:274-316`

## 4. Fuse Whole Same-Entity Attribute Bundles

The existing cardinality-one fetch fusion is a strong specialization, but it
handles one pattern at a time and materializes a fresh relation after every
attached attribute. K attributes still require K passes and K intermediate
tuple sets.

Recognize a run of `[?e :constant ?fresh]` scans and compile one
multi-attribute attach. Walk the outer tuples once, perform K cache lookups, and
allocate one final-width tuple. Benchmark a crossover where an entity-wide scan
becomes cheaper than K point lookups.

Relevant code and benchmarks:

- `datalog/executor/query_executor.go:389-512`
- `datalog/storage/attr_fetch_bench_test.go`
- `PERFORMANCE_STATUS.md:30`

## 5. Propagate Statically Provable Relational Properties

The query syntax, selected index, schema, and `Relation` type already establish
facts such as candidate keys, uniqueness, ordering, and rewindability, but those
facts are not carried explicitly. Preserving them can enable safe deduplication
and sort elimination without collected statistics or cost-based planning.

Derive properties only from existing contracts:

- `Scan` establishes properties from its pattern, selected index, and schema.
- `Select` preserves them.
- `Map` and `Project` update them conservatively.
- `Join` combines them only where the result follows structurally.

Do not add persistent cardinality statistics, a cost model, or heuristic
overrides. The syntax-visible greedy planner remains the planning model.

Relevant code:

- `datalog/planner/clause_utils.go:602-686`
- `datalog/algebra/types.go:53-84`
- `datalog/executor/relation.go`

## 6. Compile Storage-Bound Hash Matching Once

The storage hash path converts every probe key to a string, sometimes through
`fmt`, and matches candidates by rebuilding a symbol-index map for every
candidate tuple. Both decisions are invariant for the iterator lifetime.

Select a typed key strategy once from the bound datom position, precompute
pattern-to-binding slots, and evaluate candidates with indexed loads. Profile a
cache-disabled storage workload first because warm-cache queries bypass this
path.

Relevant code:

- `datalog/storage/hash_join_matcher.go:443-513`
- `datalog/storage/hash_join_matcher.go:516-572`
- `datalog/storage/hash_join_matcher.go:777-821`

## 7. Turn the Algebra Optimizer into a Compositional Optimizer

The relational algebra IR is expressive, but the default optimizer currently
registers only decorrelation and the `get-else` scan rewrite. It performs one
bottom-up application per pass, then decompiles to clauses for heuristic
phasing.

Add dependency-safe `Select` and `Project` pushdown first. Re-run passes to a
structural fixpoint with cycle protection. Only then consider join
associativity or DAG-based common-subexpression elimination where safety follows
from statically derived properties and explicit structural preconditions.

Relevant code:

- `datalog/algebra/optimize.go:33-84`
- `datalog/algebra/rewrite_decorrelate.go`
- `datalog/planner/algebra_bridge.go:11-56`

## Do Not Prioritize Yet

- Recent hash-join inner-loop changes are already measured and effective.
- Planning latency is measured in microseconds.
- Scan sharing and entity prefetch are benchmarked as neutral in the default
  workload.
- Symmetric hash join trades throughput for time-to-first-row.
- Do not tune the current merge join before its ordering contract is explicit.
  It sorts full tuples lexicographically while extracting a join key by datom
  position. Physical properties should represent the required order first.

## Suggested Sequence

1. **Completed:** typed aggregation keys via `TupleKeyMap`.
2. **Local and measurable:** `DedupIterator` `PutIfAbsent`, bounded Top-N heap.
3. **Cross-layer gains:** multi-attribute fetch fusion, uniqueness propagation,
   index-order Top-N.
4. **Optimizer foundation:** derived ordering and key properties, pushdown
   fixpoint, then structurally safe rewrites.
