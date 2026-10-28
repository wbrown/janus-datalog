# Where the rules test's allocations go

`TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation`
(`datalog/storage/rules_random_test.go`) runs 83 random rule sets under each
optimizer mode on `memory-trees`, and for every predicate, node and bound
shape runs a query and an `Explain`. Its native allocation profile says where
the engine's allocations go under rule evaluation. The renderings the numbers
are read from are at the end of this document.

Machine: Apple M5, go1.27.2 darwin/arm64. Tree: `e4e7087`.

```
go test ./datalog/storage -run '^TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation$' -count=1 -memprofile mem.prof
go tool pprof -sample_index=alloc_space -top -nodecount=45 mem.prof
go tool pprof -sample_index=alloc_objects -top -nodecount=30 mem.prof
go tool pprof -sample_index=alloc_space -list '<function>' mem.prof
```

The test allocates 42.4 GB over 700 million objects in 30 seconds, an average
of 60 bytes each. The graphs are a few dozen edges, so this is not tuple data.
It is fixed per-operation overhead of the rule machinery, paid again on every
body run of every fixpoint iteration, and 17 million hash joins. Objects count
as much as bytes here, since GC cost is per pointer scanned.

The top sites, from the profile:

| Site | GB | M objects | What allocates |
|---|---:|---:|---|
| resolveAddWinsSet and under it | 5.6 | 76 | per entity a cursor, an accumulator with two maps, per datom a string key and a state |
| HashJoinWithOptions and under it | 13.7 | 184 | per join an index, a seen map, a doubling results slice, a tuple per match, a result relation |
| MaterializedRelation.Project and under it | 3.4 | 52 | a new slice and a new tuple per row, 24 M projections |
| newMaterializedRelationFromSet and under it | 4.9 | 68 | the relation struct plus properties.clone |
| runPlan's held map | 1.9 | 8.7 | a fresh map copying every derived relation per body run |
| NewUnitRelation | 1.4 | 14 | a constant, allocated 14 M times |
| RelationProperties.clone | 0.8 | 47 | the largest object count in the profile |
| sliceIterator, DataPattern.Symbols, SymbolIndexTable | 1.5 | 63 | per Iterator() call, per pattern execution, per operation |

A row that says "and under it" is the site's cumulative allocation; the other
rows are the site's own.

The levers, in order of bytes:

1. **The add-wins resolution runs its slow path on memory-trees.**
   `resolveAddWinsSet` has an in-tree fast path keyed on the emulator's
   iterator type and one for Badger. The memory-trees scan returns a
   `*memoryTreeIterator`, which matches neither, so it falls to the generic
   `iter.Datom()` loop at the end of `resolveAddWinsSet`. That loop re-encodes
   each already-decoded value into a string key and heap-allocates a state per
   value. The profile shows no samples under the two fast arms and all the
   bytes under the generic one. On top of that, every unbound scan of `:edge`
   re-resolves every entity's set from storage. That is 14.5 million entity
   resolutions for graphs that never change during the queries. The EA cache
   that serves the bound route is never asked on the scan-all-entities route.

2. **Per-relation fixed costs.** The two constructors that take
   `RelationProperties` clone them, on a type whose contract already says
   callers treat them as immutable. Every `Iterator()` on a materialized
   relation heap-allocates its iterator. `DataPattern.Symbols` is recomputed
   on every call for an immutable pattern, 24 million calls. Those three are
   90 million of the 700 million objects and carry no information.

3. **Per-run fixed costs in the stratum loop.** `runPlan` copies the derived
   map into a new map on every body run and derives two contexts and an
   executor struct per run. `view()` rebuilds the derived map and re-wraps
   every set as a new relation on every iteration.

4. **The hash join's output path.** The results slice grows by doubling with
   no capacity hint, the seen map is sized from the smaller input and grows
   past it under fanout, every join result is materialized, and a projection
   of it copies every tuple.

This bears directly on the js/wasm storage leg, where this test dies at the
4 GiB address-space limit. Under js/wasm there is no sysmon (`haveSysmon` in
`runtime/proc.go`), the one P runs the test goroutine without preemption, and
every mark phase is completed by that goroutine's own assists: in the trace
below every cycle's CPU line reads `137/0/0` and its neighbours, assist time
with no background and no idle worker. Each assist updates the GC CPU limiter
(`gcAssistAlloc1` in `runtime/mgcmark.go`), which charges the assist time plus
25% of the window as GC time (`updateLocked` in `runtime/mgclimit.go`). Once
the limiter's one-second bucket fills, assists stop (`gcAssistAlloc`), no mark
worker can be scheduled, no caller of the limiter's update remains, and the
mark never ends: every allocation from then on is retained until memory runs
out. These are read in the go1.27.2 runtime; the limiter's file is dated 2022
and predates both toolchains the gate runs. The assist share that latches the
limiter is proportional to bytes allocated per second of mutator time. The
trace sat at 34% overall, and the latch says the share crossed the threshold
in some window. Halving the volume halves the share, which is the real fix
rather than a runtime setting around it.

Item 1 comes first: the type switch is one mechanical gap, and the
re-resolution is 13% of all bytes on its own.

## Results

### Item 1: the unbound scan reads each entity's set through the cache

`cardinalityManyScanAllEntitiesIterator` resolves each entity's set as the
bound-entity route does: through `Cache.GetOrResolve` under the matcher's
cache key when the matcher has a cache and its view is served by one, and
through `resolveAddWinsSet` otherwise. `TestCardinalityManyUnboundScanUsesCache`
pins the answers before and after a commit and that the scan leaves each
entity's set in the cache. `BenchmarkCardinalityManyUnboundScan` is the
instrument, `[?e :person/tags ?tag]` over N entities of four tags each on
`memory-trees`, the measured loop after a warm-up:

| Arm | allocs/op before | allocs/op after | B/op before | B/op after | ns/op before | ns/op after |
|---|---:|---:|---:|---:|---:|---:|
| N=100/cache | 3,574 | 977 | 186,969 | 41,688 | 87,179 | 39,807 |
| N=100/nocache | 3,574 | 3,574 | 186,969 | 186,969 | 87,751 | 107,917 |
| N=1000/cache | 35,085 | 9,088 | 1,877,722 | 422,030 | 1,107,511 | 435,986 |
| N=1000/nocache | 35,085 | 35,085 | 1,877,722 | 1,877,719 | 1,152,247 | 1,222,502 |

The nocache arm is the equivalence control: its allocations are identical
before and after, so the route's storage resolution is untouched and the
cache arm's delta is the cache. The test's profile under the same command:

| | before | after |
|---|---:|---:|
| alloc_space | 42.39 GB | 37.59 GB |
| alloc_objects | 700.7 M | 634.0 M |
| cardinalityManyScanAllEntitiesIterator.Next, cumulative | 5.76 GB | 0.50 GB |

What remains under the iterator is the output tuple per member. The slow
path inside `resolveAddWinsSet` on `memory-trees` still runs on the first
resolution of each entity's set and under `DisableCache`, History and a
snapshot bound; in this test it no longer reaches the top 25 sites.

### Item 2: the interior constructor holds derived properties as given

`newMaterializedRelationFromSet` holds the `RelationProperties` it is given
without a copy. Every write to a candidate key or an ordering in production
code lands on a fresh local inside the derivation functions before a
relation is constructed, and nothing writes into a relation's properties
afterwards, which is the type's own contract. The exported
`NewMaterializedRelationWithProperties` keeps its copy of external inputs.
`BenchmarkProvenSetConstruction`'s proven-set arm, one construction per
operation, from the renderings below:

| | allocs/op | B/op |
|---|---:|---:|
| before | 3 | 208 |
| after | 1 | 176 |

### Item 3: a scope's run is a field of the context

`runPlan` copied the context's derived map into a new map on every scope
run to hold the required run under `query.ScopeRequired` beside it. The
context carries the scope's run as a field, read by `Derived` under that
symbol and carried by every derivation, and `runPlan` derives the scope's
context through `WithRequiredRun`. `TestContext_AScopesRequiredRunIsHeldBesideTheDerivedRelations`
pins the contract. `runPlan`'s own allocation, 1.95 GB flat over 8,740,466
objects in the baseline renderings below, is gone from the profile.

### The unit relation is one per executor

The verdict that holds, a branch's run over no required run, and a form's
subject over no group answered with a unit relation built per call,
14,396,263 times in the baseline rendering below. `Executor` holds one,
built with its options at construction and carried by the per-query
executor `ExecuteWithRelations` derives; `existenceVerdict` takes the unit
relation it answers with, and the clause executor's sites read the
executor's. `TestExistenceVerdictOverGroups` and
`TestExecutor_HoldsOneUnitRelation` pin it. `NewUnitRelation` is gone from
the profile's top sites.

### The hash join's results are sized by the estimate its seen map uses

The materialized hash join grew its results by doubling from an empty slice.
It makes the slice with the capacity the seen map is sized by, the smaller
of the build count and the probe size when known, and grows past it only
under fanout.

### Totals after every change above

| | before | after |
|---|---:|---:|
| alloc_space | 42.39 GB | 33.04 GB |
| alloc_objects | 700.7 M | 563.0 M |

The five packages the changes reach, `datalog/executor`, `datalog/storage`,
`datalog/db`, `tests` and `cmd/datalog`, pass with the new tests among them.
The test alone under js/wasm, which died at the 4 GiB limit at 74.7 s on the
committed head, completes on this tree:

```
GOFLAGS=-run=TestRules_BoundInvocationsOnRandomRuleSetsMatchTheFreeEvaluation make test-wasm-storage
PATH="/opt/homebrew/Cellar/go/1.27.2/libexec/lib/wasm:$PATH" GOOS=js GOARCH=wasm go test -count=1 -timeout 45m github.com/wbrown/janus-datalog/datalog/storage
ok  	github.com/wbrown/janus-datalog/datalog/storage	153.384s
```

One run is one sample of a rate event: the limiter latches when the assist
share crosses its threshold in some window, so the margin this tree has is
the volume removed, and a later change that raises the share brings the
stall back.
What leads the profile is the relation struct itself in
`newMaterializedRelationFromSet` (4.19 GB over 25.5 million relations, 176
bytes each with `ExecutorOptions` held by value), the seen maps and derived
sets' key maps growing in `TupleKeyMap.PutIfAbsent` (2.65 GB), the join
index per join (`NewJoinIndex` 1.66 GB, `JoinIndex.Add` 1.08 GB), the
projections at the find boundary and the binding projection with their
dedup pass (3.22 GB), the iterator per `Iterator()` call (0.90 GB) and the
joined tuple per match (0.86 GB).

## Renderings

### `BenchmarkCardinalityManyUnboundScan -benchmem`, before and after item 1

Apple M5, darwin/arm64. Before:

```
BenchmarkCardinalityManyUnboundScan/N=100/cache-10         	   13532	     87179 ns/op	  186969 B/op	    3574 allocs/op
BenchmarkCardinalityManyUnboundScan/N=100/nocache-10       	   13162	     87751 ns/op	  186969 B/op	    3574 allocs/op
BenchmarkCardinalityManyUnboundScan/N=1000/cache-10        	    1100	   1107511 ns/op	 1877722 B/op	   35085 allocs/op
BenchmarkCardinalityManyUnboundScan/N=1000/nocache-10      	    1099	   1152247 ns/op	 1877722 B/op	   35085 allocs/op
```

After:

```
BenchmarkCardinalityManyUnboundScan/N=100/cache-10         	   31905	     39807 ns/op	   41688 B/op	     977 allocs/op
BenchmarkCardinalityManyUnboundScan/N=100/nocache-10       	   10000	    107917 ns/op	  186969 B/op	    3574 allocs/op
BenchmarkCardinalityManyUnboundScan/N=1000/cache-10        	    2924	    435986 ns/op	  422030 B/op	    9088 allocs/op
BenchmarkCardinalityManyUnboundScan/N=1000/nocache-10      	     962	   1222502 ns/op	 1877719 B/op	   35085 allocs/op
```

### `BenchmarkProvenSetConstruction -benchmem`, before and after item 2

Apple M5, darwin/arm64. Before, at the committed head `e4e7087`:

```
BenchmarkProvenSetConstruction/tuples=10000/deduplicating-10         	    3558	    304577 ns/op	 1164119 B/op	      37 allocs/op
BenchmarkProvenSetConstruction/tuples=10000/proven-set-10            	24931156	        49.53 ns/op	     208 B/op	       3 allocs/op
BenchmarkProvenSetConstruction/tuples=100000/deduplicating-10        	     390	   2883923 ns/op	 9746346 B/op	     261 allocs/op
BenchmarkProvenSetConstruction/tuples=100000/proven-set-10           	23130801	        52.98 ns/op	     208 B/op	       3 allocs/op
```

After:

```
BenchmarkProvenSetConstruction/tuples=10000/deduplicating-10         	    3930	    304983 ns/op	 1164118 B/op	      37 allocs/op
BenchmarkProvenSetConstruction/tuples=10000/proven-set-10            	38885655	        31.77 ns/op	     176 B/op	       1 allocs/op
BenchmarkProvenSetConstruction/tuples=100000/deduplicating-10        	     410	   2876665 ns/op	 9745757 B/op	     261 allocs/op
BenchmarkProvenSetConstruction/tuples=100000/proven-set-10           	38117624	        32.67 ns/op	     176 B/op	       1 allocs/op
```

### `-sample_index=alloc_space -top -nodecount=45`

```
File: storage.test
Type: alloc_space
Time: 2026-10-27 22:22:01 EDT
Showing nodes accounting for 35.20GB, 83.03% of 42.39GB total
Dropped 486 nodes (cum <= 0.21GB)
Showing top 45 nodes out of 146
      flat  flat%   sum%        cum   cum%
    4.21GB  9.93%  9.93%     4.91GB 11.58%  github.com/wbrown/janus-datalog/datalog/executor.newMaterializedRelationFromSet (inline)
    2.59GB  6.10% 16.03%     2.59GB  6.10%  github.com/wbrown/janus-datalog/datalog/executor.(*TupleKeyMap).PutIfAbsent
    1.95GB  4.60% 20.62%    28.22GB 66.57%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).runPlan
    1.94GB  4.58% 25.20%     1.94GB  4.58%  github.com/wbrown/janus-datalog/datalog/storage.(*addWinsAccumulator).observeDatom
    1.65GB  3.90% 29.10%     1.65GB  3.90%  github.com/wbrown/janus-datalog/datalog/executor.NewJoinIndex (inline)
    1.38GB  3.26% 32.36%     1.38GB  3.26%  github.com/wbrown/janus-datalog/datalog/executor.NewTupleKeyMapWithCapacity (inline)
    1.37GB  3.24% 35.60%     1.37GB  3.24%  github.com/wbrown/janus-datalog/datalog/executor.NewUnitRelation (inline)
    1.37GB  3.24% 38.83%    13.62GB 32.12%  github.com/wbrown/janus-datalog/datalog/executor.HashJoinWithOptions
    1.13GB  2.67% 41.50%     1.13GB  2.67%  github.com/wbrown/janus-datalog/datalog/storage.(*addWinsAccumulator).finish
    1.04GB  2.46% 43.96%     1.04GB  2.46%  github.com/wbrown/janus-datalog/datalog/executor.(*JoinIndex).Add
    1.01GB  2.38% 46.35%     3.42GB  8.06%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Project
    0.91GB  2.14% 48.49%     0.91GB  2.14%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Iterator
    0.90GB  2.12% 50.60%     0.90GB  2.12%  github.com/wbrown/janus-datalog/datalog/storage.(*datomTree).cursor (inline)
    0.87GB  2.05% 52.65%     0.87GB  2.05%  github.com/wbrown/janus-datalog/datalog/executor.combineTuplesIndexed (inline)
    0.79GB  1.86% 54.51%     0.79GB  1.86%  github.com/wbrown/janus-datalog/datalog/executor.RelationProperties.clone (inline)
    0.74GB  1.74% 56.25%    39.82GB 93.94%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).Execute
    0.73GB  1.71% 57.96%     0.73GB  1.71%  github.com/wbrown/janus-datalog/datalog/storage.newAddWinsAccumulator (inline)
    0.65GB  1.53% 59.50%    39.43GB 93.01%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).executeRuleInvocation
    0.53GB  1.26% 60.76%     2.12GB  4.99%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).matchWithBindingsFromCache
    0.53GB  1.24% 62.00%     1.72GB  4.05%  github.com/wbrown/janus-datalog/datalog/executor.deduplicateTuples
    0.51GB  1.21% 63.21%     5.76GB 13.58%  github.com/wbrown/janus-datalog/datalog/storage.(*cardinalityManyScanAllEntitiesIterator).Next
    0.51GB  1.19% 64.40%     6.48GB 15.28%  github.com/wbrown/janus-datalog/datalog/executor.(*ruleEvaluation).runStratum.func2
    0.49GB  1.16% 65.56%     0.49GB  1.16%  github.com/wbrown/janus-datalog/datalog/executor.(*Executor).queryExecutor (inline)
    0.48GB  1.14% 66.70%     1.86GB  4.39%  github.com/wbrown/janus-datalog/datalog/storage.(*storeVersion).scan
    0.48GB  1.13% 67.83%     0.48GB  1.13%  github.com/wbrown/janus-datalog/datalog/storage.boundProbeDatom
    0.46GB  1.07% 68.91%     0.46GB  1.07%  github.com/wbrown/janus-datalog/datalog/executor.(*TupleKeyMap).PutIfAbsentPositions
    0.44GB  1.04% 69.95%     1.51GB  3.57%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationWithProperties
    0.43GB  1.02% 70.97%     0.93GB  2.20%  github.com/wbrown/janus-datalog/datalog/executor.(*derivedSet).absorb.func1
    0.38GB   0.9% 71.87%     2.96GB  6.98%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).next
    0.38GB  0.89% 72.76%     0.43GB  1.02%  github.com/wbrown/janus-datalog/datalog/executor.NewStreamingRelationWithProperties (inline)
    0.38GB  0.89% 73.65%     0.38GB  0.89%  github.com/wbrown/janus-datalog/datalog/planner.Scope.Query
    0.37GB  0.86% 74.51%     0.38GB  0.89%  github.com/wbrown/janus-datalog/datalog/executor.(*matchEmit).projectionFor
    0.36GB  0.85% 75.36%     0.62GB  1.46%  github.com/wbrown/janus-datalog/datalog/executor.(*OrFallbackRelation).Iterator
    0.34GB   0.8% 76.16%     0.66GB  1.55%  github.com/wbrown/janus-datalog/datalog/executor.(*ruleEvaluation).runStratum.func1 (inline)
    0.32GB  0.75% 76.91%    31.22GB 73.66%  github.com/wbrown/janus-datalog/datalog/executor.(*stratumEvaluation).run
    0.31GB  0.73% 77.64%     5.56GB 13.11%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).resolveAddWinsSet
    0.30GB   0.7% 78.34%     0.95GB  2.23%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationWithOptions (inline)
    0.29GB  0.67% 79.01%     0.29GB  0.67%  github.com/wbrown/janus-datalog/datalog/query.SymbolIndexTable
    0.28GB  0.65% 79.67%     0.28GB  0.65%  github.com/wbrown/janus-datalog/datalog/query.(*InternedTupleBuilder).BuildTupleInterned
    0.25GB   0.6% 80.27%     0.65GB  1.54%  github.com/wbrown/janus-datalog/datalog/executor.Select
    0.25GB  0.59% 80.86%     0.25GB  0.59%  github.com/wbrown/janus-datalog/datalog/query.(*DataPattern).Symbols
    0.24GB  0.58% 81.44%     0.24GB  0.58%  github.com/wbrown/janus-datalog/datalog/executor.RelationProperties.renameSymbols
    0.23GB  0.54% 81.98%     0.23GB  0.55%  github.com/wbrown/janus-datalog/datalog/executor.(*Context).WithEnvironment (inline)
    0.23GB  0.53% 82.51%    38.99GB 91.97%  github.com/wbrown/janus-datalog/datalog/executor.(*ruleEvaluation).runStratum
    0.22GB  0.52% 83.03%     0.78GB  1.85%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).executeSubquery
```

### `-sample_index=alloc_objects -top -nodecount=30`

```
File: storage.test
Type: alloc_objects
Time: 2026-10-27 22:22:01 EDT
Showing nodes accounting for 477730088, 68.18% of 700677083 total
Dropped 457 nodes (cum <= 3503385)
Showing top 30 nodes out of 175
      flat  flat%   sum%        cum   cum%
  47344619  6.76%  6.76%   47344619  6.76%  github.com/wbrown/janus-datalog/datalog/executor.RelationProperties.clone (inline)
  32993512  4.71% 11.47%   32993512  4.71%  github.com/wbrown/janus-datalog/datalog/storage.(*addWinsAccumulator).observeDatom
  26238773  3.74% 15.21%  637218032 90.94%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).Execute
  25863775  3.69% 18.90%  629664733 89.87%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).executeRuleInvocation
  25673109  3.66% 22.57%   67938676  9.70%  github.com/wbrown/janus-datalog/datalog/executor.newMaterializedRelationFromSet (inline)
  24184563  3.45% 26.02%   52070141  7.43%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Project
  23691408  3.38% 29.40%   23691408  3.38%  github.com/wbrown/janus-datalog/datalog/query.(*DataPattern).Symbols
  21265532  3.03% 32.43%   21265532  3.03%  github.com/wbrown/janus-datalog/datalog/executor.combineTuplesIndexed (inline)
  20284147  2.89% 35.33%   20284147  2.89%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Iterator
  19021993  2.71% 38.04%   19021993  2.71%  github.com/wbrown/janus-datalog/datalog/query.SymbolIndexTable
  17444503  2.49% 40.53%   17444503  2.49%  github.com/wbrown/janus-datalog/datalog/executor.(*JoinIndex).Add
  17026643  2.43% 42.96%  183573071 26.20%  github.com/wbrown/janus-datalog/datalog/executor.HashJoinWithOptions
  16003194  2.28% 45.25%   16003194  2.28%  github.com/wbrown/janus-datalog/datalog/executor.NewJoinIndex (inline)
  14571499  2.08% 47.33%   14571499  2.08%  github.com/wbrown/janus-datalog/datalog/storage.newAddWinsAccumulator (inline)
  14505447  2.07% 49.40%   14505447  2.07%  github.com/wbrown/janus-datalog/datalog/executor.RelationProperties.renameSymbols
  14396263  2.05% 51.45%   14396263  2.05%  github.com/wbrown/janus-datalog/datalog/executor.NewUnitRelation (inline)
  13611041  1.94% 53.39%   85615995 12.22%  github.com/wbrown/janus-datalog/datalog/storage.(*cardinalityManyScanAllEntitiesIterator).Next
  11354225  1.62% 55.01%  199608803 28.49%  github.com/wbrown/janus-datalog/datalog/executor.Relations.Collapse
  10420497  1.49% 56.50%   76941706 10.98%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).resolveAddWinsSet
   9715951  1.39% 57.89%    9715951  1.39%  github.com/wbrown/janus-datalog/datalog/query.(*InternedTupleBuilder).BuildTupleInterned
   9042219  1.29% 59.18%    9042219  1.29%  github.com/wbrown/janus-datalog/datalog/planner.Scope.Query
   8740466  1.25% 60.43%  463870136 66.20%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).runPlan
   8486926  1.21% 61.64%   16810016  2.40%  github.com/wbrown/janus-datalog/datalog/query.ScopeOf
   7432925  1.06% 62.70%    7432925  1.06%  github.com/wbrown/janus-datalog/datalog/query.Substitution.Rename
   7012382  1.00% 63.70%    7012382  1.00%  github.com/wbrown/janus-datalog/datalog/executor.RelationProperties.project
   6475415  0.92% 64.62%   18941646  2.70%  github.com/wbrown/janus-datalog/datalog/storage.(*storeVersion).scan
   6449196  0.92% 65.54%    6449196  0.92%  github.com/wbrown/janus-datalog/datalog/storage.boundProbeDatom
   6291488   0.9% 66.44%    6291488   0.9%  github.com/wbrown/janus-datalog/datalog/query.functionValue
   6171342  0.88% 67.32%    6171342  0.88%  github.com/wbrown/janus-datalog/datalog/executor.deduplicatedProperties (inline)
   6017035  0.86% 68.18%    6017035  0.86%  github.com/wbrown/janus-datalog/datalog/storage.(*datomTree).cursor (inline)
```

### `-sample_index=alloc_space -list 'storage\.\(\*PatternMatcher\)\.resolveAddWinsSet$'`

```
ROUTINE ======================== github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).resolveAddWinsSet in datalog/storage/set_resolution.go
  318.01MB     5.56GB (flat, cum) 13.11% of Total
         .          .     55:func (m *PatternMatcher) resolveAddWinsSet(eBytes, aBytes []byte, report *scanReport) (*SetResolutionResult, error) {
         .          .     56:	// The caller holds storage projections; both constructors return the
         .          .     57:	// canonical interned pointer for an already-interned value.
         .          .     58:	var e Entity
         .          .     59:	copy(e[:], eBytes)
         .          .     60:	var a Attribute
         .          .     61:	copy(a[:], aBytes)
         .          .     62:
         .          .     63:	// Values live in the index keys; nothing here needs Badger values.
     150MB      150MB     64:	bound := ScanBound{
         .          .     65:		Index: EAVT,
  168.01MB   168.01MB     66:		Prefix: []datalog.Value{
         .          .     67:			datalog.NewIdentityFromHash(e),
         .          .     68:			datalog.InternKeywordFromBytes(a),
         .          .     69:		},
         .          .     70:	}
         .          .     71:	// Names the run for an arm that delegates its whole scan here, and so has
         .          .     72:	// none of its own to declare. An arm that already declared one keeps it:
         .          .     73:	// these reads are subordinate to it.
         .          .     74:	if report != nil && report.run == nil {
         .          .     75:		report.run = &bound
         .          .     76:	}
         .          .     77:	// The scan accrues into the report as it closes, so the arms below that return
         .          .     78:	// nil on a decode or blob failure keep the intake they spent instead of
         .          .     79:	// losing it with the result struct that never gets built.
         .     1.45GB     80:	iter, err := OpenKeyScan(m.reader, report, bound)
         .          .     81:	if err != nil {
         .          .     82:		return nil, err
         .          .     83:	}
         .          .     84:	defer iter.Close()
         .          .     85:
         .   742.54MB     86:	acc := newAddWinsAccumulator(m)
         .          .     87:
         .          .     88:	// The scan prefix fixes E and A of every entry; only the value span,
         .          .     89:	// Tx, and Op vary. The in-tree iterators expose their raw keys, so
         .          .     90:	// those spans are read directly and values decode only for members
         .          .     91:	// that survive resolution. Concrete iterator types keep
         .          .     92:	// the per-entry calls direct and inlinable; iterators from external
         .          .     93:	// Store implementations resolve through Datom() with identical
         .          .     94:	// semantics.
         .          .     95:	if it, ok := iter.(*memoryIterator); ok {
         .          .     96:		if err := scanAddWinsMemory(it, acc); err != nil {
         .          .     97:			return nil, err
         .          .     98:		}
         .          .     99:		if err := it.Error(); err != nil {
         .          .    100:			return nil, err
         .          .    101:		}
         .          .    102:		return finishWithBound(acc, it.blobs, bound)
         .          .    103:	}
         .          .    104:	if blobs, ok, err := scanAddWinsBadger(iter, acc); ok {
         .          .    105:		if err != nil {
         .          .    106:			return nil, err
         .          .    107:		}
         .          .    108:		if err := iter.Error(); err != nil {
         .          .    109:			return nil, err
         .          .    110:		}
         .          .    111:		return finishWithBound(acc, blobs, bound)
         .          .    112:	}
         .          .    113:
         .          .    114:	for iter.Next() {
         .          .    115:		datom, err := iter.Datom()
         .          .    116:		if err != nil {
         .          .    117:			return nil, err
         .          .    118:		}
         .     1.94GB    119:		acc.observeDatom(datom)
         .          .    120:	}
         .          .    121:	if err := iter.Error(); err != nil {
         .          .    122:		return nil, err
         .          .    123:	}
         .     1.13GB    124:	return finishWithBound(acc, nil, bound)
         .          .    125:}
```

### The wasm run under `GODEBUG=gctrace=1`, its last cycles

The test alone under `GOOS=js GOARCH=wasm` on the same tree, through the Node
runner, Homebrew go1.27.2. The cycle lines are consecutive in the run's output;
the line after `gc 537` is the fatal.

```
gc 530 @50.958s 34%: 0.042+245+0.009 ms clock, 0.042+137/0/0+0.009 ms cpu, 158->186->92 MB, 186 MB goal, 0 MB stacks, 0 MB globals, 1 P
gc 531 @51.341s 34%: 0.039+228+0.010 ms clock, 0.039+135/0/0+0.010 ms cpu, 157->185->94 MB, 185 MB goal, 0 MB stacks, 0 MB globals, 1 P
gc 532 @51.705s 34%: 0.044+258+0.010 ms clock, 0.044+145/0/0+0.010 ms cpu, 160->188->94 MB, 188 MB goal, 0 MB stacks, 0 MB globals, 1 P
gc 533 @52.115s 34%: 0.044+241+0.010 ms clock, 0.044+141/0/0+0.010 ms cpu, 161->190->97 MB, 189 MB goal, 0 MB stacks, 0 MB globals, 1 P
gc 534 @52.515s 34%: 0.041+250+0.010 ms clock, 0.041+146/0/0+0.010 ms cpu, 165->194->98 MB, 194 MB goal, 0 MB stacks, 0 MB globals, 1 P
gc 535 @52.923s 34%: 0.040+263+0.012 ms clock, 0.040+151/0/0+0.012 ms cpu, 168->198->100 MB, 197 MB goal, 0 MB stacks, 0 MB globals, 1 P
gc 536 @53.333s 34%: 0.040+253+0.009 ms clock, 0.040+152/0/0+0.009 ms cpu, 171->201->101 MB, 200 MB goal, 0 MB stacks, 0 MB globals, 1 P
gc 537 @53.738s 34%: 0.046+260+0.010 ms clock, 0.046+151/0/0+0.010 ms cpu, 173->204->102 MB, 203 MB goal, 0 MB stacks, 0 MB globals, 1 P
runtime: out of memory: cannot allocate 524288-byte block (4165730304 in use)
```
