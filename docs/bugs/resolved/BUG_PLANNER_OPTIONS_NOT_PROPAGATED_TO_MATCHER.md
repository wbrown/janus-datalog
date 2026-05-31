# BUG: Custom PlannerOptions Do Not Fully Propagate to BadgerMatcher

**Date**: 2026-05-30
**Severity**: Medium — custom execution configurations can diverge between planner/executor and storage relations
**Status**: ✅ RESOLVED (2026-05-31)
**Affected**: `storage.Database.Query`, `storage.Database.Matcher`

## Resolution

Centralized the `PlannerOptions → ExecutorOptions` conversion and made the
default-source matcher honor the database's effective options.

The conversion now has one home: `executor.ExecutorOptionsFromPlanner`, which
copies every `PlannerOptions` field that has an `ExecutorOptions` counterpart —
including `IndexNestedLoopThreshold`, which the executor's own converter
previously dropped (so even `NewExecutorWithOptions` lost a custom threshold).
Three drifting manual copy sites are gone: the executor's
`convertToExecutorOptions` (renamed to the exported `ExecutorOptionsFromPlanner`)
and the two in `storage` (`Database.Matcher` and `Database.matcherWithExecOptions`,
which each omitted `UseStreamingSubqueryUnion`, `UseComponentizedSubquery`,
`EnableScanSharing`, `EnableEntityPrefetch`, `EnableAttributeFetchFusion`).

A new `Database.effectivePlannerOptions()` returns the caller override or the
defaults, and every query-construction path funnels through it: `Matcher()`
delegates to `matcherWithExecOptions(d.effectivePlannerOptions())`, and `Query`,
`NewExecutor`, and `NewExecutorWithOptions` use the same accessor — so the
executor and the matcher's relations always agree on the effective options.
User-supplied `WithSources` matchers are untouched; only the default `$` source
changed.

Regression coverage, both verified to fail before the fix:

- `storage/matcher_planner_options_test.go`
  (`TestDatabaseMatcher_HonorsCustomPlannerOptions`) — a relation produced by
  `db.Matcher()` under custom options carries them (`EnableTrueStreaming`,
  `EnableScanSharing`, `EnableEntityPrefetch`, `IndexNestedLoopThreshold`), not
  defaults.
- `executor/executor_options_propagation_test.go`
  (`TestNewExecutorWithOptions_HonorsIndexNestedLoopThreshold`) — a custom
  `IndexNestedLoopThreshold` survives into the executor's effective options.

## Summary

`Database.Query` honors `Database.plannerOptions` when constructing the
executor, but `Database.Matcher()` always constructs its `BadgerMatcher` from
`DefaultPlannerOptions()`. Because storage matchers attach `ExecutorOptions` to
the `StreamingRelation`s they return, custom options can be applied at the
executor layer while storage-produced relations still carry default options.

This creates configuration drift inside one query.

## Code Evidence

`Database.Query` chooses the query's planner/executor options from
`d.plannerOptions` when present:

```go
var planOpts planner.PlannerOptions
if d.plannerOptions != nil {
	planOpts = *d.plannerOptions
} else {
	planOpts = DefaultPlannerOptions()
}
planOpts.Cache = d.planCache
exec := executor.NewExecutorWithOptions(router, d, planOpts)
```

But the default database source is built before that through `d.Matcher()`:

```go
sources := buildSourceMap(opts.sources, d.Matcher())
```

`d.Matcher()` ignores `d.plannerOptions` and always converts defaults:

```go
opts := DefaultPlannerOptions()
execOpts := executor.ExecutorOptions{
	EnableIteratorComposition:       opts.EnableIteratorComposition,
	EnableTrueStreaming:             opts.EnableTrueStreaming,
	EnableSymmetricHashJoin:         opts.EnableSymmetricHashJoin,
	EnableParallelSubqueries:        opts.EnableParallelSubqueries,
	MaxSubqueryWorkers:              opts.MaxSubqueryWorkers,
	EnableStreamingJoins:            opts.EnableStreamingJoins,
	EnableStreamingAggregation:      opts.EnableStreamingAggregation,
	EnableStreamingAggregationDebug: opts.EnableStreamingAggregationDebug,
	EnableDebugLogging:              opts.EnableDebugLogging,
	IndexNestedLoopThreshold:        opts.IndexNestedLoopThreshold,
}
matcher := NewBadgerMatcherWithOptions(d.store, execOpts)
```

So a query can run with custom `PlannerOptions` in `Executor`, while every
storage relation produced by the default `$` source carries options derived from
defaults.

## Why This Matters

Several relation operations read options from their input relations:

- streaming vs materialized relation behavior
- join strategy flags
- streaming aggregation flags
- debug logging flags
- storage join strategy thresholds

If the executor and matcher disagree about those options, non-default
configurations become difficult to reason about. A user can disable a feature in
`PlannerOptions`, but storage-created relations may still carry defaults that
re-enable or influence downstream behavior.

This is especially suspicious in light of the documented known limitation in
`active/CONDITIONAL_AGGREGATE_STREAMING_DEPENDENCY.md`: disabling streaming is a
non-default path that has shown nondeterministic failures. This options drift is
not proven to be that root cause, but it is the kind of mismatch that makes
non-default execution modes unreliable.

## Expected Behavior

For a single query, all components that consume execution options should see the
same effective options:

1. planner
2. executor
3. matcher
4. relations returned by matcher
5. joins/aggregations/transforms that inherit relation options

If `Database.plannerOptions` is set, the default `$` matcher should be built
from those options, not from `DefaultPlannerOptions()`.

## Actual Behavior

`Database.Query` uses custom options for the executor but gets the default source
from `d.Matcher()`, and `d.Matcher()` always uses defaults.

## Suggested Fix

Centralize the conversion from `planner.PlannerOptions` to
`executor.ExecutorOptions` so `Database.Matcher()` and
`executor.NewExecutorWithOptions()` cannot drift.

Possible direction:

1. Add a `Database.effectivePlannerOptions()` method that returns
   `*d.plannerOptions` or `DefaultPlannerOptions()`, with cache set where
   appropriate.
2. Add a storage-side conversion function or reuse an exported executor
   conversion function instead of manually copying fields in `Database.Matcher()`.
3. Ensure `Database.Query` builds the default matcher using the same effective
   options passed to `NewExecutorWithOptions`.

Be careful with `WithSources`: user-supplied sources may have their own options
and should not necessarily be mutated, but the default database source should be
consistent with the query options.

## Tests Needed

Add tests that force non-default options and assert they reach storage-produced
relations:

1. Construct a `Database` with `PlannerOptions{EnableIteratorComposition:false,
   EnableTrueStreaming:false, EnableStreamingAggregation:false}`.
2. Run a storage-backed query and capture either annotations or relation options
   from a matcher-created relation.
3. Verify the matcher relation options match the custom query options, not
   defaults.
4. Add a regression test for `IndexNestedLoopThreshold`, since that option is
   copied in `Database.Matcher()` but not currently included in
   `executor.convertToExecutorOptions`; this can expose drift in either
   direction.

If the intended design is that matcher options are always defaults, document that
explicitly and make sure executor operations do not infer behavior from relation
options that came from the matcher.
