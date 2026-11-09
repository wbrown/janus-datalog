# Planner Options Reference

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
    Cache              *PlanCache                 // set by the Database
    MatchAnswersLookup algebra.MatchAnswersLookup // set by the executor

    EnableAlgebraOptimizer      bool // default-active
    EnableScanSharing           bool // default-active
    EnableEntityPrefetch        bool // opt-in
    EnableAttributeFetchFusion bool // default-active

    EnableSymmetricHashJoin    bool // opt-in
    EnableStreamingAggregation bool // default-active

    MaxRuleIterations int // opt-in, 0 = unbounded
    MaxDerivedTuples  int // opt-in, 0 = unbounded

    ScopeWindowCap    int // 0 = executor default, 1000 tuples
    ScopeWindowGrowth int // 0 = executor default, 2
}
```

## Default configuration

`storage.DefaultPlannerOptions()` sets exactly these (everything else is the Go
zero value — i.e. off / nil / 0):

```go
EnableAlgebraOptimizer:     true
EnableScanSharing:          true
EnableEntityPrefetch:       false
EnableAttributeFetchFusion: true
EnableSymmetricHashJoin:    false
EnableStreamingAggregation: true
MaxRuleIterations:          0      // unbounded
MaxDerivedTuples:           0      // unbounded
ScopeWindowCap:             0      // executor default: 1000 tuples
ScopeWindowGrowth:          0      // executor default: 2
```

So, off by default (opt-in): `EnableEntityPrefetch`, `EnableSymmetricHashJoin`,
`MaxRuleIterations` and `MaxDerivedTuples`.

---

## Options reference

### Algebra / subquery optimization

#### EnableAlgebraOptimizer — **default-active**
Relational-algebra IR optimization: the query is compiled to an algebra tree,
optimized (subquery decorrelation, predicate pushdown), and lowered back into a
validated Datalog query before physical planning. This is where decorrelation
and pushdown actually happen — there are no separate knobs for them. Consumed in
`planner/planner_clause_based.go`.

#### MatchAnswersLookup — **set by the executor**
The sources' answer to whether a match of an attribute on a source gives, for
every entity, the one value the source's lookup gives.
`executor.NewExecutorWithOptions` sets it from its matcher's
`CanFuseAttributeFetch`, so a planner asks the sources its executor reads. The
algebra optimizer's get-else scan rewrite applies only where it answers true.
A plan records each answer its rewrites asked, and the plan cache serves the
plan only where the sources give each of those answers again. Nil answers
false, so a planner given none declines every get-else rewrite. Consumed in
`planner/algebra_bridge.go`, `planner/planner_clause_based.go` and
`planner/cache.go`.

#### EnableScanSharing — **default-active**
Serves one query's unbound reads of one scan from one shared sequence. A read
is unbound when no relation of its bindings binds a variable of its pattern.
The plan's site walk lists, per fingerprint (source, pattern, order and
limit), the sites that read it: the unbound data patterns of the plan's
phases, of its forms' frames and of its evaluations' bodies, and the
existence check of each pattern the plan reads in windows. A fingerprint read
at two or more sites, or at a site in a frame that runs once per window, is
shared: the first read opens the scan, every later one reads the tuples the
first realized, and a reader that stops early never closes the scan. The
registry holds the sequence's head until every listed site has been handed it
and every per-window frame listing it has ended, then releases it; a read at
an unlisted site, or after the release, is a scan of its own. A fingerprint
read at one site is not shared. The scan's source closes when it is read to
its end or when the query ends. On the complex checkpoint (Badger, 75
scenarios of 100 tasks), the three subquery bodies' status scans read storage
once, and the query answers in 8.66–9.02 ms against 10.47–11.06 ms with
sharing off, at 1.14 MB more per query for the held scan (n=3 at 10s; verified
2026-11-08, Apple M5). Consumed in `executor/executor.go`, `executor/scan_sites.go`,
`executor/scan_registry.go` and `executor/scan_sharing_matcher.go`.

#### EnableEntityPrefetch — **opt-in** (default false)
Prefetches entities into the EA cache via `PrefetchEntities` before a
fallback window's branches are built, when a branch of one data pattern reads
the subject's entity symbol, from which window on that branch is built from
the cache. It fires above a threshold of 50 entities, and the matcher
prefetches nothing under History. Benchmarked performance-neutral, hence off.
Consumed in `executor/or_fallback_relation.go`.

#### EnableAttributeFetchFusion — **default-active**
Fuses contiguous CardinalityOne patterns sharing an already-bound entity into
one tuple traversal. Fresh variables become attached bindings; literal values
become typed constraints evaluated by cache-backed lookup. It runs in the
latest and as-of views at an attribute the matcher resolves cardinality-one,
declared so or with no definition, reading the datoms at or before an as-of
target. A History matcher, a cardinality-many attribute and a
cardinality-vector attribute decline the fusion. Constant constraints improve focused 1K/10K workloads by
21.9–23.2% time and the production-shaped complex checkpoint by 11.1%.
Consumed in `executor/query_executor.go`.

### Joins and aggregation

#### EnableSymmetricHashJoin — **opt-in** (default false)
Dual-hash-table join for stream-to-stream joins without materializing either
side. Standard hash join is faster when one side can be materialized, so this is
off by default. Consumed in `executor/join.go`.

#### EnableStreamingAggregation — **default-active**
Streaming aggregation (no full materialization of the input). Consumed in
`executor/aggregation.go`.

### Rule evaluation bounds

#### MaxRuleIterations — **opt-in** (default 0)
The fixpoint iterations one stratum of a rule set may run, counted per window
for an invocation inside a form's branch. `0` is unbounded. Exceeded, the
query returns `*query.RuleIterationLimitError` naming the stratum's
predicates, the limit and the iteration reached. Consumed in
`executor/rules.go`.

#### MaxDerivedTuples — **opt-in** (default 0)
The tuples one invocation may derive when it is evaluated, carried across the
windows for an invocation inside a form's branch. A rewritten set's magic
predicates count too. `0` is unbounded. Exceeded, the query returns
`*query.DerivedTupleLimitError` naming the predicates, the limit and the
count reached. Consumed in `executor/rules.go`.

Both bound the evaluation of a rule set the caller does not control. See
[RULES.md](RULES.md). Cancellation is a query option, `WithContext`, not a
planner option.

### Scope windows

An `or-join` or `or-default-join` reads its subject into windows: one tuple,
then `ScopeWindowGrowth` times the last, up to `ScopeWindowCap` tuples. Each
branch runs once per window and the window's tuples probe its result, so the
subject is read once, nothing of it is held beyond the window, and the first
result follows one pull. Both options are consumed in
`executor/or_fallback_relation.go`.

#### ScopeWindowCap — **default 0** (executor default: 1000 tuples)
The most tuples one window holds. `0` takes `executor.SmallBindingBoundary`,
the boundary at or below which the matcher seeks a binding's distinct keys one
by one, so a full window's keys are sought per key.

#### ScopeWindowGrowth — **default 0** (executor default: 2)
The factor each window grows by over the last. `0` or `1` takes 2.

### Plan cache

#### Cache *PlanCache
Shared query-plan cache. Set automatically by the `Database`; you normally don't
set this by hand.

---

## Removed options (historical)

These knobs existed in earlier versions and have been removed. Most were inert
no-ops under the clause-based planner — setting them changed nothing; the rest
give their own reason and date below.

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
| `EnableIteratorComposition` | Removed in 2026-07. Composed iterators are the only implementation of streaming relation operations; the flag's false arm ("materialize then delegate") recursed without bound because `StreamingRelation.Materialize` returns the receiver, and no production path ever ran with the flag off. Materialization is a consumer decision made through `Materialize()`, not a mode. |
| `IndexNestedLoopThreshold` | Removed in 2026-07 with the index-nested-loop join strategy it gated. The strategy had been off by default since 2025-10 on a benchmark that does not survive inspection, and was independently incorrect: its precondition — bindings sorted in index order — is unmet for the Tx and V positions, where `CompareValues` order deliberately differs from key order. Binding-driven scans seek a binding set's distinct keys one by one at or below `executor.SmallBindingBoundary`, and above it use HashJoinScan, or MergeJoin for large high-selectivity entity-position sets. |
| `EnableParallelSubqueries` | Removed in 2026-10 with the per-tuple relation-input loop it switched on. A relation input binds as one group the plan runs once over (`docs/proposals/RELATION_INPUT_PROOF.md`), the query path runs on one goroutine, and concurrency returns at the job's grain under one worker bound (`docs/proposals/CONCURRENCY_PROOF.md`). |
| `MaxSubqueryWorkers` | Removed in 2026-10 with `EnableParallelSubqueries`; it sized that loop's worker pool. |
| `EnableJoinProjectInsertion` | Removed in 2026-11. Inserted a `Project` above an inner join's child onto the symbols live above the join and lowered it as a nested subquery. A projection lowered as a scope carried nothing of the enclosing scope across its boundary: a symbol a clause of the scope bound, the scope's required run, and the failure contract that an evaluation fails a query only at an assignment where every other clause holds. It refused forms and rule bodies and raised failures beside empty clauses, its own benchmark measured the subquery at +60.4% time (`PERFORMANCE_STATUS.md`, "Materialized join-project experiment"), and it was inactive from its first commit. `docs/ideas/JOIN_PROJECT_INSERTION.md` records the idea. |
| `EnableTrueStreaming` | Removed in 2026-11. A `StreamingRelation` answers -1 from `Size()` and is read once in every setting, so the option was read by `ExecutorOptions.populated` alone and selected nothing. |
| `EnableStreamingJoins` | Removed in 2026-11. The hash join answers a streaming relation over its iterator whenever the build holds a tuple, in every setting (`docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md`), so the option selected nothing. |

If your code sets any of these, delete the lines — the fields no longer exist.

---

## Recipes

All recipes start from `storage.DefaultPlannerOptions()` and override only what
they need.

### Default (recommended)

```go
opts := storage.DefaultPlannerOptions()
// Already tuned: algebra optimizer + streaming on.
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
