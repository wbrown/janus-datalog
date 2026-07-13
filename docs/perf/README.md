# Performance Baselines

Durable benchmark and profile artifacts captured pre-refactor, kept so
"before/after" comparisons survive working-tree changes.

## How to read this directory

Each baseline has a `<benchmark>_baseline_<date>` stem with these
associated files:

| Suffix | Format | What it is |
|--------|--------|------------|
| `_<date>.txt` | raw `go test -bench` output | benchstat-parseable, count=10 |
| `_<config>.prof` | raw pprof | CPU profile, 10s of samples |
| `_<config>_top.txt` | text | `go tool pprof -top -cum -nodecount=40` rendering of the same profile |

## Comparing after a change

Re-run the same benchmark on the changed code at the same count, save
output to a separate file, then:

```
benchstat <baseline>.txt <after>.txt
```

The `_top.txt` files render with the test binary's symbol table. To
re-render a `.prof` file against a different binary:

```
go test -c -o /tmp/exec.test ./datalog/executor/
go tool pprof -top -cum -nodecount=40 /tmp/exec.test <prof>.prof
```

## Current baselines

### `relation_input_parallel_baseline_2026-05-26.txt`

`BenchmarkRelationInputParallel` on the relation-input parallel
iteration path (`executor.executeRealizedWithRelationInputIterationParallel`).
2,400 input tuples, 9,600 datoms, query is a 4-pattern join with `(max ?age)`
aggregation under `:in $ [[?n ?y ?m] ...]`.

Machine: Apple M5, go1.26.3 darwin/arm64.
Commit: `827e872` (main at branch-off point, after PR #84).

n=10 result (benchstat):

| Configuration | sec/op | vs Sequential |
|---------------|--------|---------------|
| Sequential | 522.8m ± 7% | 1.00× |
| Parallel-2Workers | 327.2m ± 4% | 1.60× |
| Parallel-4Workers | 271.3m ± 2% | 1.93× |
| Parallel-8Workers | 258.7m ± 1% | **2.02×** |
| Parallel-16Workers | 286.9m ± 3% | 1.82× |
| Parallel-32Workers | 306.0m ± 3% | 1.71× |

Memory and allocations are flat across worker counts at ~792 MiB and
11.28M allocs per op — the parallel-vs-sequential delta is all time.
Beyond 8 workers, scheduler overhead exceeds the parallelism gain.

### `hash_join_baseline_2026-05-27.txt`

Five hash-join benchmarks (`InputTypes`, `MaterializedVsStreaming`,
`SingleIteration`, `LargeResult`, `Streaming`) covering 25 sub-cases:
materialized × materialized, streaming × materialized, materialized
× streaming, streaming × streaming at sizes 100/1000/5000/10000/50000.

Machine: Apple M5, go1.26.3 darwin/arm64.
Commit: `8e336f2` (main, post PR #85).

n=10. Wall time ~4.5 minutes for the full suite.

Key per-op numbers for representative cases:

| Case | sec/op | B/op | allocs/op |
|------|-------:|-----:|----------:|
| InputTypes/mat_x_mat/size_5000 | 1.12 ms | 2.45 MB | 40,060 |
| InputTypes/stream_x_mat/size_5000 | 1.59 ms | 3.28 MB | 55,083 |
| LargeResult/size_50000 | 13.0 ms | 24.4 MB | 400,293 |

The streaming-probe case adds ~15k allocs vs the both-materialized
case at the same data size (8/op extra per row, consistent with the
`tupleCopy` at `join.go:91-93` and other streaming-mode copies).

Allocations scale linearly with result size: ~8/row in
`LargeResult/size_50000` (400k allocs / 50k rows).

### Hash-join profile signatures

Three `.prof` files for `mat_x_mat/size_5000`, `stream_x_mat/size_5000`,
and `LargeResult/size_50000`.

All three are dominated by GC/scheduler primitives, consistent with
high allocation pressure from the inner loop:

| Symbol | mat×mat 5K | stream×mat 5K | large 50K |
|--------|-----------:|--------------:|----------:|
| `runtime.madvise` (GC pages) | 28.48% flat | 23.21% flat | 16.94% flat |
| `runtime.pthread_cond_wait` | 14.62% flat | 13.94% flat | 14.40% flat |
| `runtime.kevent` (netpoll) | 21.07% flat | 28.19% flat | 8.35% flat |
| `runtime.usleep` | 6.11% flat | 6.10% flat | – |
| `HashJoinWithOptions` (cum) | 8.85% | – | 28.43% |
| `combineTuples` (cum) | – | – | 6.92% |
| `TupleKeyMap.Put` (cum) | – | – | 5.68% |

`combineTuples` and `TupleKeyMap.Put` only surface in the top 40 for
the 50K case where the application work is large enough to register
above scheduler noise. They are the largest application-level hot
spots after GC, validating the targets in
`docs/ideas/HASH_JOIN_HOT_PATH_OPTIMIZATIONS.md` findings #1 and #5.

### `hash_join_buildsize_baseline_2026-05-27.txt`

`BenchmarkHashJoinBuildSize` sweep — 6 build sizes (64..2048) × 8 data
sizes (50..10000) at n=3. Tunes `DefaultHashTableSize`, not the
per-row hot path; kept as a reference so after-state runs can confirm
the chosen default still holds.

### `hash_join_identity_baseline_2026-05-27.txt`

`BenchmarkHashJoinIdentityKeys` + `BenchmarkHashJoinIdentityLargeResult`
at n=10. Identity-keyed analogue of the int64-keyed benchmarks,
exercising the `hashValue`/`ValuesEqual` paths for interned pointer
types — the dominant join-key shape in real datalog queries.

Same machine and commit as the other baseline.

Headline per-op numbers (Identity vs int64 at matched configs):

| Case | int64 | Identity | Δ |
|------|------:|---------:|--:|
| InputTypes/mat_x_mat/size_5000 | 1.12 ms | 1.33 ms | +19% |
| LargeResult/size_50000 | 13.0 ms | 14.95 ms | +15% |

Allocation counts are identical (400,293/op at 50K) — the delta is
entirely CPU on Identity-specific hash and equality paths.

Two profiles captured:
- `hash_join_identity_baseline_mat_x_mat_5000.{prof,_top.txt}`
- `hash_join_identity_baseline_large_50000.{prof,_top.txt}`

Application-level symbols surfacing in the Identity 50K profile that
do not appear in the int64 50K profile:

| Symbol | cum % | Finding |
|--------|------:|---------|
| `executor.combineTuples` | 7.21% | #1 |
| `executor.NewTupleKey` | 6.35% | secondary (multi-col alloc) |
| `executor.hashValue` | 5.72% | #3 |

`hashValue` 5.72% is the FNV-1a loop over the 20-byte SHA1 hash for
`datalog.Identity` values. `NewTupleKey` 6.35% is the
`make([]interface{}, len(indices))` for the multi-symbol key path.
`combineTuples` is type-agnostic and tracks the int64 case closely.

`ValuesEqual`/`reflect.ValueOf` (finding #2) does not surface in
the LargeResult/50K profile. The cost is distributed across ~200k
`ValuesEqual` calls (one per `TupleKeyMap` lookup with a non-empty
bucket), each small enough that no single call site reaches the
noise floor. The high-fanout and duplicate-heavy benchmarks below
stress the same path with denser call counts.

### Cardinality-many and duplicate-heavy profiles

Two additional Identity-keyed benchmarks were added to stress paths
the 1:1-keyed benchmarks don't exercise:

- `BenchmarkHashJoinIdentityHighFanout` — K distinct keys, each
  appearing M times on each side. Build accumulates M-tuple chains
  per key; probe expands by M; result is K·M² rows. Profile:
  `hash_join_identity_baseline_highfanout_100x50.{prof,_top.txt}`
  (250k rows, 50.9 ms/op).
- `BenchmarkHashJoinIdentityDuplicates` — K keys × R reps per side,
  shared payload per key. After join, 99%+ of result rows are
  duplicates that `seen` rejects. Inputs use
  `NewMaterializedRelationNoDedupeWithOptions`. Profile:
  `hash_join_identity_baseline_duplicates_10x100.{prof,_top.txt}`
  (100k raw → 10 unique, 8.7 ms/op).

Application symbols surfacing in each (cum %):

| Symbol | HighFanout 100×50 | Duplicates 10×100 | LargeResult 50K |
|--------|------------------:|------------------:|----------------:|
| `HashJoinWithOptions` | 15.36% | 25.10% | 33.48% |
| `combineTuples` | 4.64% | 12.90% | 7.21% |
| `TupleKeyMap.Put` | 4.71% | – | – |
| `TupleKeyMap.Exists` | – | 5.98% | – |
| `NewTupleKeyFull` | – | 5.13% | – |
| `hashValues` | – | 4.82% | – |
| `tupleValuesEqual` | – | 3.26% | – |
| `ValuesEqual` | – | **2.95%** | – |
| `hashValue` | – | 2.56% | 5.72% |
| `NewTupleKey` | – | – | 6.35% |

`ValuesEqual` at 2.95% in the Duplicates profile is the dedup path
firing 99,990 equality comparisons per benchmark op (one per
rejected duplicate). `reflect.ValueOf` is inlined inside
`ValuesEqual`; its cost rolls up to the caller and constitutes the
bulk of that 2.95%. After finding #2 (move pointer-equality check
to the top of `ValuesEqual`), the 2.95% should drop sharply.

### `hash_join_after_finding1_2026-05-27.txt` + Identity variant

After-state for finding #1 (`combineTuples` projection plan hoisted
out of the inner loop). Same machine and commit base; runs
covering the same 25 int64 sub-benchmarks plus the 19 Identity sub-
benchmarks at n=10.

Geomean deltas:

| Suite | Geomean |
|-------|--------:|
| int64-keyed | -0.80% |
| Identity-keyed | -7.58% |

Largest Identity wins (where `combineTuples` runs hardest per row):

| Benchmark | Δ |
|-----------|--:|
| IdentityDuplicates/keys10/reps100 | -25.09% |
| IdentityDuplicates/keys50/reps50 | -17.21% |
| IdentityHighFanout/keys100/fanout10 | -11.94% |
| IdentityLargeResult/size_10000 | -11.80% |

Profile delta (Duplicates/keys10/reps100): `combineTuples` cum
time 1.66s → 0.98s, a 41% absolute reduction in the function's
CPU. Allocations unchanged across all benchmarks; the per-call
map was already stack-allocated by escape analysis, so wins are
pure CPU savings.

The int64 mid-to-large sizes show small statistically-significant
"regressions" (+3 to +16% at size_5000+) that look like thermal
sequence artifacts: the same shapes show small wins or wash in
the Identity suite, and targeted single-benchmark re-runs in
cleaner thermal state contradict them. A controlled-thermal rerun
would clarify; the architectural benefit (strictly less per-call
work, smaller iterator struct) holds regardless.

Profiles for the Duplicates/keys10/reps100 case (baseline vs
after) live next to each other:

- `hash_join_identity_baseline_duplicates_10x100.{prof,_top.txt}`
- `hash_join_identity_after_finding1_duplicates_10x100.{prof,_top.txt}`
- `hash_join_identity_after_finding1_large_50000.{prof,_top.txt}`

### M3 Ultra rerun (`*_m3ultra_2026-05-28.txt`)

Clean-thermal re-baseline of finding #1 to settle the M5's int64
"regression" question. Machine: Apple M3 Ultra (32 cores),
go1.25.0 darwin/arm64. Baseline = `main` (`362136e`), whose executor
differs from the branch only by finding #1; after = branch tip. Both
suites at n=10.

| Stem | What |
|------|------|
| `hash_join_baseline_m3ultra_2026-05-28.txt` | main, int64 suite |
| `hash_join_identity_baseline_m3ultra_2026-05-28.txt` | main, Identity suite |
| `hash_join_after_finding1_m3ultra_2026-05-28.txt` | branch, int64 suite |
| `hash_join_identity_after_finding1_m3ultra_2026-05-28.txt` | branch, Identity suite |

Geomean deltas (main → branch):

| Suite | M5 Air | M3 Ultra |
|-------|-------:|---------:|
| int64-keyed | -0.80% (noisy) | **-9.08%** |
| Identity-keyed | -7.58% | **-8.82%** |

On the M3 Ultra every int64 sub-benchmark improves (p=0.000, -5.2%
to -13.4%); the size_5000+ shapes that "regressed" on the M5 are now
-5% to -9% wins. **The M5 int64 regressions were thermal sequence
artifacts.** Allocations flat (±0.1%) on both suites. Identity
Duplicates land at -23.9%/-23.5% (≈ the -25% prediction); only
`IdentityLargeResult/size_50000` is a wash (p=0.971). The M3 Ultra is
the verification rig for findings #2–#6 — re-baseline here, not on
the M5 numbers.

### `vbound_validation_baseline_2026-05-29.txt`

`BenchmarkVBoundValidation` + `BenchmarkVBoundValidationSupersession` —
the V-bound CardinalityOne validation path
(`validatingVBoundIterator.validateCandidate`). A query
`[?e :place/type "room"]` (A+V constant, E unbound, CardinalityOne
non-unique) drives one `validateCandidate` call per emitted candidate;
N entities share the queried value. On the baseline each validation is
a per-candidate EATV point seek (Badger ConcatIterator open/close +
IncrRef/DecrRef). The `cache`/`nocache` arms set `DisableCache`
false/true; both pay the AVET candidate scan, so the delta isolates the
validation cost. Supersession variant re-types half the entities so the
candidate count stays N while half are stale (reject path).

Machine: Apple M5, darwin/arm64. n=10.
Baseline commit: `362136e` (main). After: branch
`perf/vbound-validation-cache` — `validateCandidate` gains a latest-mode
CardinalityOne fast path that resolves the current (E, A) value from the
EA cache (`GetOrResolve`) instead of seeking.

After-state file: `vbound_validation_after_2026-05-29.txt`.
Comparison: `vbound_validation_benchstat_2026-05-29.txt`.

benchstat headline (baseline → after):

| Metric | `cache` arm (fast path active) | `nocache` arm (cache disabled) |
|--------|-------------------------------|-------------------------------|
| sec/op | **−64% to −82%** (all p=0.000) | ±9–21%, thermal noise |
| allocs/op | **−70% to −78%** (all p=0.000) | **identical** (p=1.000 / ~) |
| B/op | −79% to −85% | ±0.2% |

The win scales with candidates-per-value: each validation drops from an
EATV seek to an O(1) cache lookup (≈2.85 µs/candidate saved at N=5000),
and the per-candidate Badger iterator allocations disappear (the −70–78%
allocs).

The `nocache` arm is the equivalence control: allocs/op are **byte-for-
byte identical** between baseline and after (the fast path is the only
behavioral change; the scan-loop refactor that accompanies it — adding
`shouldFilterTx` skipping for as-of correctness — is work-neutral in
latest mode). nocache sec/op is not comparable across the two separate
test binaries on this hardware (thermal/GC variance shows small
significant deltas in both directions); allocs/op is the reliable
cross-binary signal and it confirms `nocache` reproduces the baseline.

### `attr_fetch_baseline_2026-05-29.txt`

`BenchmarkAttrFetch` — cost of getting K CardinalityOne attributes off one
entity, two ways. `patterns` runs an anchor `[?e :place/type "room"]` plus
K same-`?e` attribute patterns; each is its own narrow `(?e, vᵢ)` relation
hash-joined on `?e` (K joins). `pull` runs the same anchor with
`(pull ?e [… K attrs])`, fetching all K per entity in one go (no
inter-attribute join). Both emit N rows with the same values and share the
anchor, so the delta is the per-attribute join cost. N ∈ {100, 1000},
K ∈ {1, 3, 6}.

Machine: Apple M5, darwin/arm64. n=10. Base: `5f0e39d` (main).

| N | K | patterns sec/op | pull sec/op | speedup | patterns allocs/op | pull allocs/op |
|--:|--:|----------------:|------------:|--------:|-------------------:|---------------:|
| 100 | 1 | 198.7µs | 121.8µs | 1.6× | 2,545 | 1,828 |
| 100 | 3 | 266.3µs | 154.8µs | 1.7× | 5,210 | 2,638 |
| 100 | 6 | 457.4µs | 212.5µs | 2.2× | 9,212 | 3,843 |
| 1000 | 1 | 1.31ms | 1.14ms | 1.1× | 22,378 | 16,247 |
| 1000 | 3 | 2.73ms | 1.77ms | 1.5× | 46,688 | 24,265 |
| 1000 | 6 | 4.68ms | 2.59ms | 1.8× | 83,164 | 36,291 |

The gap scales with K: one join (K=1) is marginal (1.1–1.6×); six (K=6)
reach 1.8–2.2×. Marginal cost per added attribute at N=1000 ≈ 650–710µs
(patterns) vs 275–315µs (pull) — each same-entity attribute pattern pays a
join about equal to the value fetch. Allocations diverge most: at
K=6/N=1000 patterns allocate 2.3× pull (the per-join `combineTuples` /
`TupleKeyMap` churn). This is the baseline for a same-entity attribute-
fusion optimization that would emit a wide row per entity instead of
hash-joining narrow per-pattern relations; `pull` is the no-join floor
proxy (it builds a nested map where a fused op would build flat tuples, so
the achievable result may land somewhat either side of it).

### `attr_fetch_fusion_2026-05-29.txt`

After-state for same-entity attribute fusion (`EnableAttributeFetchFusion`,
default on). Adds a `patterns-fused` arm to `BenchmarkAttrFetch`: the same
`patterns` query run with fusion enabled, which turns each same-entity
`[?e :const-attr ?fresh]` pattern into a per-tuple `LookupAttribute` binding
attach instead of a match + hash join. n=10, Apple M5 darwin/arm64.

| N | K | patterns | fused | pull | fused vs patterns | patterns allocs | fused allocs |
|--:|--:|---------:|------:|-----:|:-----------------:|----------------:|-------------:|
| 100 | 1 | 91.7µs | 65.6µs | 74.3µs | 1.40× | 2,545 | 1,471 |
| 100 | 3 | 163.7µs | 97.4µs | 99.8µs | 1.68× | 5,210 | 2,230 |
| 100 | 6 | 276.7µs | 145.7µs | 130.4µs | 1.90× | 9,211 | 3,372 |
| 1000 | 1 | 764.5µs | 513.8µs | 680.9µs | 1.49× | 22,376 | 12,280 |
| 1000 | 3 | 1.488ms | 843.7µs | 965.2µs | 1.76× | 46,678 | 18,452 |
| 1000 | 6 | 2.626ms | 1.353ms | 1.350ms | 1.94× | 83,143 | 27,719 |

Fusion is **1.40–1.94× faster** than the join path, scaling with K (each saved
join is a saved hashtable build/probe + `combineTuples` + tuple keys), with
**~2.6–3× fewer allocations** (B/op at K=6/N=1000: 6.06Mi → 2.35Mi). It reaches
the no-join `pull` floor — matching it at K=6 and beating it at K≤3, because
fused emits flat tuples while `pull` builds nested maps (fused also allocates
fewer: 27.7k vs 36.3k at K=6/N=1000). Both fused and unfused use the EA cache
for the per-`(E,A)` lookups; the delta is purely the eliminated joins.

---

## Original relation-input profile signatures

Three `.prof` files capture CPU samples (10s) for Sequential,
Parallel-8Workers, Parallel-32Workers.

The 8→32 worker regression breaks down (cumulative %):

| Symbol | 8 workers | 32 workers | Δ |
|--------|----------:|-----------:|---:|
| `runtime.lock2` (global runtime lock) | 26.00% | 32.58% | +6.58pp |
| `runtime.usleep` (yield-loop sleeping) | 23.45% flat | 30.02% flat | +6.57pp |
| `runtime.osyield` | 18.91% | 22.34% | +3.43pp |
| `executeRealizedWithRelationInputIterationParallel.func1` | 29.52% | 31.58% | +2.06pp |
| `runtime.pthread_cond_wait` | 13.90% flat | 11.30% flat | −2.6pp |
| `runtime.madvise` (GC returning pages) | not in top | 10.83% flat | new |

Same family of scheduler-primitive symbols dominates regardless of
worker count — the goroutine-creation cost (`runtime.newstack` /
`runtime.morestack`) and the M-scheduler lock contention
(`runtime.lock2`) scale with the per-tuple goroutine spawn rate. At
high worker counts on the M5 MacBook Air (4 P-cores + 6 E-cores) the
measurement is dominated by over-subscription effects from running
more goroutines than physical performance cores.
