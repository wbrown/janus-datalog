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

### Profile signatures

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
