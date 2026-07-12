# Planner Options Reference

**Last Updated**: 2026-05-25
**Status**: Current with the clause-based planner

## Overview

Janus Datalog uses a single `planner.PlannerOptions` struct to configure both
query planning and execution. The public `db.Open` API applies sensible defaults
automatically — most users never touch these options:

```go
d, _ := db.Open("path/to/db")
d.Query(`[:find ?name :where [?p :person/name ?name]]`)
```

The options below are for **advanced tuning** via the `storage`/`executor`
packages (e.g. `storage.DefaultPlannerOptions()` + `NewExecutorWithOptions`).

Every option is labeled **Default-active** (turned on by `DefaultPlannerOptions()`)
or **Opt-in** (off by default; you must set it). This labeling is the contract:
if a doc says an option is default-active, `DefaultPlannerOptions()` sets it, and
`TestDefaultPlannerOptions_MatchesDocumentedDefaults` enforces that they agree.

---

## The struct

```go
type PlannerOptions struct {
    Cache *PlanCache // set by the Database

    EnableAlgebraOptimizer    bool // default-active
    EnableScanSharing         bool // opt-in
    EnableEntityPrefetch      bool // opt-in
    EnableAttributeFetchFusion bool // default-active

    EnableIteratorComposition bool // default-active
    EnableTrueStreaming       bool // default-active
    EnableSymmetricHashJoin   bool // opt-in

    EnableParallelSubqueries bool // default-active
    MaxSubqueryWorkers       int  // 0 = runtime.NumCPU()

    EnableStreamingJoins       bool // opt-in
    EnableStreamingAggregation bool // default-active

    IndexNestedLoopThreshold int // default 0
}
```

## Default configuration

`storage.DefaultPlannerOptions()` sets exactly these (everything else is the Go
zero value — i.e. off / nil / 0):

```go
EnableAlgebraOptimizer:     true
EnableScanSharing:          false
EnableEntityPrefetch:       false
EnableAttributeFetchFusion: true
EnableIteratorComposition:  true
EnableTrueStreaming:        true
EnableSymmetricHashJoin:    false
EnableParallelSubqueries:   true
MaxSubqueryWorkers:         0      // runtime.NumCPU()
EnableStreamingJoins:       false
EnableStreamingAggregation: true
IndexNestedLoopThreshold:   0      // always HashJoinScan
```

So, off by default (opt-in): `EnableScanSharing`, `EnableEntityPrefetch`,
`EnableSymmetricHashJoin`, and `EnableStreamingJoins`.

---

## Options reference

### Algebra / subquery optimization

#### EnableAlgebraOptimizer — **default-active**
Relational-algebra IR optimization: the query is compiled to an algebra tree,
optimized (subquery decorrelation, predicate pushdown), and decompiled back to
clauses. This is where decorrelation and pushdown actually happen — there are no
separate knobs for them. Consumed in `planner/planner_clause_based.go`.

#### EnableScanSharing — **opt-in** (default false)
Deduplicates identical unbound scans across subqueries via a shared lazy sequence.
Benchmarked performance-neutral, hence off. Consumed in `executor/executor.go`.

#### EnableEntityPrefetch — **opt-in** (default false)
Warms the EA cache after the first data pattern via `PrefetchEntities`.
Benchmarked performance-neutral, hence off. Consumed in `executor/query_executor.go`.

#### EnableAttributeFetchFusion — **default-active**
Fuses contiguous cardinality-one fetches sharing an already-bound entity into
one tuple traversal. Latest mode only; History and AsOf decline the fusion.
Consumed in `executor/query_executor.go`.

### Streaming

#### EnableIteratorComposition — **default-active**
Lazy evaluation through composed iterators (filter/project/etc.) instead of
materializing intermediates. Consumed in `executor/relation.go`.

#### EnableTrueStreaming — **default-active**
Avoids auto-materialization of `StreamingRelation` (`Size()` returns -1 rather
than consuming the iterator). Consumed in `executor/relation.go`.

#### EnableSymmetricHashJoin — **opt-in** (default false)
Dual-hash-table join for stream-to-stream joins without materializing either
side. Standard hash join is faster when one side can be materialized, so this is
off by default. Consumed in `executor/join.go`.

### Parallel execution

#### EnableParallelSubqueries — **default-active**
Executes RelationInput query iterations in parallel via the executor's bounded
worker loop. Consumed in `executor/executor.go`.

#### MaxSubqueryWorkers — **default 0**
Worker-pool size for parallel subqueries; `0` means `runtime.NumCPU()`. Consumed
in `executor/executor.go`.

### Joins / aggregation

#### EnableStreamingJoins — **opt-in** (default false)
Returns a `StreamingRelation` from joins instead of materializing the result.
On `BenchmarkComplexQueryJoinMaterialization`, enabling it is 8.04% slower,
uses 3.78% more memory, and performs 8.34% more allocations than the default
(`n=10`). Keep it opt-in. Consumed in `executor/join.go`.

#### EnableStreamingAggregation — **default-active**
Streaming aggregation (no full materialization of the input). Consumed in
`executor/aggregation.go`.

### Storage join strategy

#### IndexNestedLoopThreshold — **default 0**
For binding sets of size ≤ threshold the matcher uses index-nested-loop (iterator
reuse with seeks); above it, HashJoinScan. Default `0` means always HashJoinScan
(benchmarks show it wins even at size 1). Consumed in
`storage/hash_join_matcher.go`.

### Plan cache

#### Cache *PlanCache
Shared query-plan cache. Set automatically by the `Database`; you normally don't
set this by hand.

---

## Removed options (historical)

These knobs existed in earlier versions and were **removed in 2026-05** because
they were inert no-ops under the clause-based planner — setting them changed
nothing:

| Removed option | Why |
|----------------|-----|
| `UseClauseBasedPlanner` | There is only one planner now; it is always used. |
| `EnableDynamicReordering` | Phase reordering knob; ignored by the clause-based planner. |
| `EnablePredicatePushdown` | Pushdown now happens unconditionally inside `EnableAlgebraOptimizer`. |
| `EnableFineGrainedPhases` | Phase-granularity knob; ignored by the clause-based planner. |
| `MaxPhases` | Phase-count cap; ignored by the clause-based planner. |
| `EnableSubqueryDecorrelation` | Decorrelation now happens inside `EnableAlgebraOptimizer`. |
| `EnableParallelDecorrelation` | Gated only the retired legacy decorrelation path (deleted). |
| `EnableConditionalAggregateRewriting` | The rewrite now runs unconditionally inside `EnableAlgebraOptimizer`; the standalone flag was inert. |
| `EnableSemanticRewriting` | Removed in 2026-05. Folded `[(year ?t) ?y] [(= ?y N)]` into range predicates, but only worked when the bound time components formed a contiguous suffix from `year` (silently produced wrong results otherwise — e.g. `day(?t) = 5` alone became `1970-01-05`). Redundant in the default configuration because `EnableAlgebraOptimizer`'s decorrelation handles the same bottleneck. Write the range predicate directly if you need it: `[(>= ?t #inst "2025-01-01")] [(< ?t #inst "2026-01-01")]`. |
| `EnableCSE` | Never shipped in `PlannerOptions`. |
| `UseStreamingSubqueryUnion` | Removed with the retired componentized subquery executor. |
| `UseComponentizedSubquery` | Removed with the retired componentized subquery executor. |
| `EnableDebugLogging` | Removed in 2026-07. Join and relation diagnostics are structured annotation events. |
| `EnableStreamingAggregationDebug` | Removed in 2026-07. Aggregation strategy and materialization diagnostics are structured annotation events. |

If your code sets any of these, delete the lines — the fields no longer exist.

---

## Recipes

All recipes start from `storage.DefaultPlannerOptions()` and override only what
they need.

### Default (recommended)

```go
opts := storage.DefaultPlannerOptions()
// Already tuned: algebra optimizer + streaming + parallel subqueries on.
```

### Debugging execution

```go
db, err := storage.NewDatabaseWithOptions(storage.DatabaseOptions{
    Path: "path/to/db",
    AnnotationHandler: func(event annotations.Event) {
        // Handle join/*, aggregation/*, relation/*, and other events.
    },
})
```

### Force index-nested-loop (testing)

```go
opts := storage.DefaultPlannerOptions()
opts.IndexNestedLoopThreshold = 999999 // use IndexNestedLoop for all binding sizes
```

---

## Performance monitoring

Attach an annotation handler (zero overhead when nil):

```go
d, _ := db.Open("path/to/db", db.WithAnnotationHandler(func(e annotations.Event) {
    log.Printf("%s: %v", e.Name, e.Data)
}))
```

Useful event names: `phase/complete`, `join/hash`, `aggregation/executed`.

For deep analysis: `go test -bench=. -cpuprofile=cpu.prof` then `go tool pprof`.

---

## Related documentation

- `ARCHITECTURE.md` — system architecture overview
- `PERFORMANCE_STATUS.md` — current performance state and benchmarks
