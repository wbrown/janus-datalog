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
