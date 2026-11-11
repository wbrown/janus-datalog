# Where a windowed fold's allocations go under a handler

`TestScopeForms_OverTheRecordedGraphAnswerTheRecordAndTheReference`
(`datalog/storage/scope_contracts_graph_test.go`) runs its checks at three
window caps on every leg, under a handler that counts stratum events. Its
check "the fold over three streaming groups sharing no symbol" at a window cap
of one, on the reference leg, is the allocation profile below: a product of
three streaming groups read a window per subject tuple, each window building
its branches and joining them, with every join, collapse, branch build and
subject tuple reported through the handler.

Machine: Apple M5, go1.27.2 darwin/arm64.

```
testfail ./datalog/storage/ -count=1 -run '^TestScopeForms_OverTheRecordedGraphAnswerTheRecordAndTheReference$/^memory-trees/none/cap_1$/^the_fold_over_three_streaming_groups_sharing_no_symbol$' -memprofile mem.prof
go tool pprof -sample_index=alloc_space -top -nodecount=40 mem.prof
```

The check answers 400 tuples. Before the change it allocates 2.97 GB in
15.5 s; the reporting sites are 1.2 GB of it, flat: `reportJoin` 440 MB,
`emitJoinStrategyAnnotation` 280 MB, `reportCacheBuild` 160 MB,
`reportCollapse` 131 MB, the window reader's `reportTuple` and `reportWindow`
96 MB, `annotateRebuild` 37 MB and `fmt.Sprintf` 37 MB under them, with the
matcher's annotation wrapper, `AnnotatedMatcher.Match`, another 169 MB. Every
event is a `map[string]interface{}` built at emission, and four of the sites
render at emission what a reader renders at display: `reportJoin` three string
slices of the symbols' names beside the symbols themselves, the strategy event
two `%T` renderings of the sides and a copy of the join symbols, the matcher's
wrapper two string slices of the binding's and the result's symbols per match,
and `reportTuple` a `fmt.Sprintf("%v", tuple)` of each subject tuple.

## The change

Each site carries what the engine holds, and the formatter renders it at
display.

- `reportJoin` carries `left.symbols`, `right.symbols` and `result.symbols`,
  the slices the relations hold, and no rendering of them. The formatter's
  `JoinHash` line renders the names from the symbols.
- `emitJoinStrategyAnnotation` carries no type rendering of either side, which
  nothing read, and the join symbols as the join holds them.
- `AnnotatedMatcher.Match` and `MatchWithConstraints` carry `binding.symbols`
  and `symbol.order` as the symbols the binding relation and the result hold.
  The formatter's `MatchesToRelations` line renders the relation over
  `symbol.order`.
- `reportTuple` carries the subject tuple under `tuple`, the tuple the window
  holds, which no pull after it writes.

`TestReportJoin_TheEventCarriesEachSidesAndTheResultsSymbols`,
`TestWrapMatcher_BasicAnnotation`, `TestWrapMatcher_WithBindings` and
`TestModuleEventsCarryTheirPayloadsUnderTheForm` (executor) pin what each
event carries, and `TestFormat_AJoinLineNamesEachSidesAndTheResultsSymbols`
and `TestFormat_AMatchLineNamesTheResultsSymbols` (annotations) what the
formatter renders from it.

## Results

The columns are the profiles before the change, with the join and strategy
sites changed alone, and with every site changed, one run each.

| | before | join and strategy | every site |
|---|---:|---:|---:|
| alloc_space | 2.97 GB | 2.39 GB | 2.33 GB |
| the check's run | 15.5 s | 12.4 s | 2.11 s |
| `reportJoin`, flat | 440 MB | 160 MB | 130 MB |
| `emitJoinStrategyAnnotation`, flat | 280 MB | 130 MB | 150 MB |
| `AnnotatedMatcher.Match`, flat | 169 MB | 160 MB | 140 MB |
| `reportTuple`, flat | 55 MB | 50 MB | 40 MB |
| `fmt.Sprintf`, flat | 37 MB | below the top 25 | below the top 25 |

The third run's time against the second's, at bytes within three percent of
each other, has no established cause.

What remains in the reporting sites is the map each event is built as, one
per join, collapse, branch build, window and subject tuple, which a handler
that reads one event name pays for every event.

## Renderings

### Before

```
File: storage.test
Type: alloc_space
Time: 2026-11-10 13:52:49 EST
Showing nodes accounting for 2876.17MB, 96.75% of 2972.70MB total
Dropped 108 nodes (cum <= 14.86MB)
Showing top 40 nodes out of 68
      flat  flat%   sum%        cum   cum%
  440.15MB 14.81% 14.81%  1203.86MB 40.50%  github.com/wbrown/janus-datalog/datalog/executor.reportJoin
  279.62MB  9.41% 24.21%   309.12MB 10.40%  github.com/wbrown/janus-datalog/datalog/executor.emitJoinStrategyAnnotation
  264.56MB  8.90% 33.11%   760.21MB 25.57%  github.com/wbrown/janus-datalog/datalog/executor.hashJoinWithSet
  243.06MB  8.18% 41.29%   713.14MB 23.99%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).MatchWithConstraints
  176.03MB  5.92% 47.21%   176.03MB  5.92%  github.com/wbrown/janus-datalog/datalog/executor.newMaterializedRelationFromSet (inline)
  169.03MB  5.69% 52.90%   893.17MB 30.05%  github.com/wbrown/janus-datalog/datalog/executor.(*AnnotatedMatcher).Match
  160.03MB  5.38% 58.28%   160.03MB  5.38%  github.com/wbrown/janus-datalog/datalog/executor.(*windowedIterator).reportCacheBuild
  131.03MB  4.41% 62.69%  1346.39MB 45.29%  github.com/wbrown/janus-datalog/datalog/executor.reportCollapse
  115.53MB  3.89% 66.57%   115.53MB  3.89%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).matchWithBindingsFromCache.func2
   74.01MB  2.49% 69.06%    77.51MB  2.61%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationFromSet
   73.01MB  2.46% 71.52%    73.01MB  2.46%  github.com/wbrown/janus-datalog/datalog/executor.NewJoinIndex (inline)
   69.51MB  2.34% 73.86%    75.51MB  2.54%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationWithProperties
   56.50MB  1.90% 75.76%   170.52MB  5.74%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).executeRuleInvocation
   55.01MB  1.85% 77.61%    73.51MB  2.47%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).reportTuple
   50.50MB  1.70% 79.31%    50.50MB  1.70%  github.com/wbrown/janus-datalog/datalog/executor.patternMatchQuery (inline)
   46.50MB  1.56% 80.87%    46.50MB  1.56%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Iterator
   42.50MB  1.43% 82.30%    42.50MB  1.43%  github.com/wbrown/janus-datalog/datalog/executor.(*Executor).queryExecutor (inline)
      42MB  1.41% 83.72%   331.56MB 11.15%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).matchWithBindingsFromCache
   41.01MB  1.38% 85.09%    55.01MB  1.85%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).reportWindow
      39MB  1.31% 86.41%       39MB  1.31%  github.com/wbrown/janus-datalog/datalog/executor.(*Context).WithEnvironment (inline)
      37MB  1.24% 87.65%       48MB  1.61%  fmt.Sprintf
   36.51MB  1.23% 88.88%    36.51MB  1.23%  github.com/wbrown/janus-datalog/datalog/storage.annotateRebuild (inline)
      33MB  1.11% 89.99%  2521.08MB 84.81%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).Execute
      30MB  1.01% 91.00%       30MB  1.01%  github.com/wbrown/janus-datalog/datalog/query.SymbolIndexTable
   25.03MB  0.84% 91.84%    25.03MB  0.84%  github.com/wbrown/janus-datalog/datalog/executor.(*TupleSet).grow
   21.50MB  0.72% 92.56%       39MB  1.31%  github.com/wbrown/janus-datalog/datalog/storage.(*storeVersion).scan
   21.50MB  0.72% 93.29%   106.01MB  3.57%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Project
   20.50MB  0.69% 93.98%    20.50MB  0.69%  github.com/wbrown/janus-datalog/datalog/executor.Relations.FindBestForPattern
   17.50MB  0.59% 94.57%    17.50MB  0.59%  github.com/wbrown/janus-datalog/datalog/storage.(*datomTree).cursor (inline)
      15MB   0.5% 95.07%       15MB   0.5%  github.com/wbrown/janus-datalog/datalog/executor.RelationProperties.renameSymbols
      15MB   0.5% 95.58%       15MB   0.5%  github.com/wbrown/janus-datalog/datalog/query.(*DataPattern).Symbols
   11.50MB  0.39% 95.96%  1215.36MB 40.88%  github.com/wbrown/janus-datalog/datalog/executor.Relations.Collapse
   10.50MB  0.35% 96.32%    53.03MB  1.78%  github.com/wbrown/janus-datalog/datalog/executor.(*windowFrame).windowRun
    5.50MB  0.19% 96.50%   111.51MB  3.75%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).ProjectFromPattern
    4.50MB  0.15% 96.65%    43.50MB  1.46%  github.com/wbrown/janus-datalog/datalog/storage.OpenScan
    2.50MB 0.084% 96.74%       46MB  1.55%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).ResolveLWW
    0.50MB 0.017% 96.75%  2961.67MB 99.63%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).next
         0     0% 96.75%       39MB  1.31%  github.com/wbrown/janus-datalog/datalog/executor.(*Context).WithRequiredRun (inline)
         0     0% 96.75%  2963.17MB 99.68%  github.com/wbrown/janus-datalog/datalog/executor.(*CountingIterator).Next
         0     0% 96.75%  1209.36MB 40.68%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).Execute.func1
```

### After the join and strategy sites

```
File: storage.test
Type: alloc_space
Time: 2026-11-10 14:16:56 EST
Showing nodes accounting for 2.17GB, 90.47% of 2.39GB total
Dropped 79 nodes (cum <= 0.01GB)
Showing top 25 nodes out of 70
      flat  flat%   sum%        cum   cum%
    0.25GB 10.55% 10.55%     0.56GB 23.34%  github.com/wbrown/janus-datalog/datalog/executor.hashJoinWithSet
    0.22GB  9.33% 19.87%     0.69GB 28.69%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).MatchWithConstraints
    0.18GB  7.41% 27.28%     0.18GB  7.41%  github.com/wbrown/janus-datalog/datalog/executor.newMaterializedRelationFromSet (inline)
    0.16GB  6.73% 34.01%     0.73GB 30.30%  github.com/wbrown/janus-datalog/datalog/executor.reportJoin
    0.16GB  6.71% 40.73%     0.86GB 35.79%  github.com/wbrown/janus-datalog/datalog/executor.(*AnnotatedMatcher).Match
    0.14GB  5.67% 46.40%     0.14GB  5.67%  github.com/wbrown/janus-datalog/datalog/executor.(*windowedIterator).reportCacheBuild
    0.13GB  5.51% 51.91%     0.13GB  5.51%  github.com/wbrown/janus-datalog/datalog/executor.emitJoinStrategyAnnotation
    0.12GB  4.82% 56.72%     0.12GB  4.82%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).matchWithBindingsFromCache.func2
    0.11GB  4.67% 61.40%     0.85GB 35.40%  github.com/wbrown/janus-datalog/datalog/executor.reportCollapse
    0.08GB  3.20% 64.60%     0.08GB  3.33%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationFromSet
    0.07GB  2.90% 67.50%     0.08GB  3.24%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationWithProperties
    0.06GB  2.67% 70.17%     0.06GB  2.67%  github.com/wbrown/janus-datalog/datalog/executor.NewJoinIndex (inline)
    0.06GB  2.59% 72.76%     0.17GB  7.28%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).executeRuleInvocation
    0.05GB  2.16% 74.92%     0.05GB  2.16%  github.com/wbrown/janus-datalog/datalog/executor.patternMatchQuery (inline)
    0.05GB  1.98% 76.90%     0.07GB  2.88%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).reportTuple
    0.04GB  1.77% 78.68%     0.33GB 13.67%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).matchWithBindingsFromCache
    0.04GB  1.71% 80.39%     0.04GB  1.71%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Iterator
    0.04GB  1.67% 82.07%     0.06GB  2.45%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).reportWindow
    0.04GB  1.53% 83.60%     0.04GB  1.53%  github.com/wbrown/janus-datalog/datalog/executor.(*Context).WithEnvironment
    0.03GB  1.41% 85.00%     0.03GB  1.41%  github.com/wbrown/janus-datalog/datalog/executor.(*Executor).queryExecutor
    0.03GB  1.39% 86.39%     0.03GB  1.39%  github.com/wbrown/janus-datalog/datalog/storage.annotateRebuild (inline)
    0.03GB  1.22% 87.62%     0.03GB  1.22%  github.com/wbrown/janus-datalog/datalog/executor.(*TupleSet).grow
    0.02GB  1.02% 88.64%     0.02GB  1.02%  github.com/wbrown/janus-datalog/datalog/query.SymbolIndexTable
    0.02GB  0.96% 89.59%     1.99GB 82.94%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).Execute
    0.02GB  0.88% 90.47%     0.11GB  4.45%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Project
```

### After every site

```
File: storage.test
Type: alloc_space
Time: 2026-11-10 16:00:23 EST
Showing nodes accounting for 2.10GB, 90.37% of 2.33GB total
Dropped 80 nodes (cum <= 0.01GB)
Showing top 25 nodes out of 71
      flat  flat%   sum%        cum   cum%
    0.24GB 10.49% 10.49%     0.56GB 24.00%  github.com/wbrown/janus-datalog/datalog/executor.hashJoinWithSet
    0.22GB  9.33% 19.82%     0.70GB 30.08%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).MatchWithConstraints
    0.15GB  6.63% 26.45%     0.15GB  6.63%  github.com/wbrown/janus-datalog/datalog/executor.emitJoinStrategyAnnotation
    0.15GB  6.48% 32.93%     0.15GB  6.48%  github.com/wbrown/janus-datalog/datalog/executor.newMaterializedRelationFromSet (inline)
    0.14GB  6.06% 38.99%     0.86GB 36.75%  github.com/wbrown/janus-datalog/datalog/executor.(*AnnotatedMatcher).Match
    0.13GB  5.73% 44.72%     0.70GB 30.08%  github.com/wbrown/janus-datalog/datalog/executor.reportJoin
    0.13GB  5.50% 50.21%     0.13GB  5.50%  github.com/wbrown/janus-datalog/datalog/executor.(*windowedIterator).reportCacheBuild
    0.12GB  5.18% 55.40%     0.83GB 35.85%  github.com/wbrown/janus-datalog/datalog/executor.reportCollapse
    0.10GB  4.32% 59.72%     0.10GB  4.32%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).matchWithBindingsFromCache.func2
    0.08GB  3.38% 63.09%     0.09GB  3.88%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationWithProperties
    0.08GB  3.31% 66.41%     0.08GB  3.46%  github.com/wbrown/janus-datalog/datalog/executor.NewMaterializedRelationFromSet
    0.07GB  2.85% 69.26%     0.07GB  2.85%  github.com/wbrown/janus-datalog/datalog/executor.NewJoinIndex (inline)
    0.06GB  2.47% 71.73%     0.17GB  7.30%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).executeRuleInvocation
    0.06GB  2.41% 74.15%     0.06GB  2.41%  github.com/wbrown/janus-datalog/datalog/executor.(*MaterializedRelation).Iterator
    0.05GB  1.95% 76.10%     0.33GB 14.24%  github.com/wbrown/janus-datalog/datalog/storage.(*PatternMatcher).matchWithBindingsFromCache
    0.04GB  1.91% 78.01%     0.04GB  1.91%  github.com/wbrown/janus-datalog/datalog/storage.annotateRebuild (inline)
    0.04GB  1.87% 79.87%     0.04GB  1.87%  github.com/wbrown/janus-datalog/datalog/executor.patternMatchQuery (inline)
    0.04GB  1.70% 81.57%     0.04GB  1.70%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).reportTuple
    0.04GB  1.57% 83.14%     0.05GB  2.20%  github.com/wbrown/janus-datalog/datalog/executor.(*windowReader).reportWindow
    0.04GB  1.51% 84.65%     0.04GB  1.51%  github.com/wbrown/janus-datalog/datalog/executor.(*Context).WithEnvironment
    0.03GB  1.28% 85.93%     0.03GB  1.28%  github.com/wbrown/janus-datalog/datalog/query.SymbolIndexTable
    0.03GB  1.24% 87.17%     1.96GB 84.32%  github.com/wbrown/janus-datalog/datalog/executor.(*DefaultQueryExecutor).Execute
    0.03GB  1.22% 88.39%     0.03GB  1.22%  github.com/wbrown/janus-datalog/datalog/executor.(*Executor).queryExecutor
    0.02GB  1.04% 89.43%     0.02GB  1.04%  github.com/wbrown/janus-datalog/datalog/executor.(*TupleSet).grow
    0.02GB  0.94% 90.37%     0.02GB  0.94%  github.com/wbrown/janus-datalog/datalog/executor.Relations.FindBestForPattern
```
