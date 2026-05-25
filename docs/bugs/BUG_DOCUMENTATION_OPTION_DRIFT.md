# BUG: Documentation and Option Surface Drift From Executable Defaults

**Date**: 2026-05-24
**Severity**: Medium - users can enable stale options or trust performance claims that are no longer wired to defaults
**Status**: Open (documentation/API consistency issue)
**Affected**: `TODO.md`, `PERFORMANCE_STATUS.md`, `DATOMIC_COMPATIBILITY.md`, `storage.DefaultPlannerOptions`, `planner.PlannerOptions`

## Summary

The repository's documentation, planner option names, and executable defaults
have drifted apart.

Several docs describe features as complete, default, or production-ready, while
the current option surface marks related flags as disabled, deprecated, ignored,
or moved to experimental. Conversely, some implemented optimization paths are
documented as part of normal planning but are not enabled by the default database
configuration.

This creates a product/API correctness problem: users and future maintainers can
turn on stale flags, assume optimizations are active when they are not, or trust
performance claims that no longer correspond to the default code path.

## Evidence

### 1. `TODO.md` says conditional aggregate rewriting is complete

```markdown
- **Conditional aggregate rewriting (7.7x faster correlated aggregates)**
- **AETV index for A-primary CRDT resolution**
- **Value elimination: ~50% storage reduction (keys-only storage)**

### Production Readiness: Ready

The engine is **functionally complete, semantically correct, and blazingly fast**.
```

### 2. `PlannerOptions` says the same option is disabled / experimental

```go
type PlannerOptions struct {
    // ...
    EnableConditionalAggregateRewriting bool // DISABLED: Feature moved to experimental/
    EnableAlgebraOptimizer              bool // Enable relational algebra IR optimization
    EnableSubqueryDecorrelation         bool // Deprecated: legacy executor decorrelation is retired; use EnableAlgebraOptimizer
    EnableParallelDecorrelation         bool // Deprecated: no effect without the retired legacy decorrelation path
    // ...
}
```

The public option still exists, but its comment says it is disabled. The docs
still advertise the feature as complete and performance-relevant.

### 3. Semantic rewriting is documented as part of planning, but default options do not enable it

The architecture docs describe time extraction folding as part of planner clause
rewriting:

```markdown
Time extraction folding: `[(year ?t) ?y] + [(= ?y 2025)]` is rewritten to
`[(>= ?t 2025-01-01T00:00:00Z)] + [(< ?t 2026-01-01T00:00:00Z)]`.
```

But the database defaults do not set `EnableSemanticRewriting: true`:

```go
func DefaultPlannerOptions() planner.PlannerOptions {
    return planner.PlannerOptions{
        UseClauseBasedPlanner: true,

        EnableDynamicReordering: true,
        EnablePredicatePushdown: true,
        EnableAlgebraOptimizer:  true,
        // EnableSemanticRewriting is omitted, so the bool default is false.
        // ...
    }
}
```

The implementation checks the flag before rewriting:

```go
func (r *SemanticRewriter) Rewrite(clauses []query.Clause) []query.Clause {
    if !r.options.EnableSemanticRewriting {
        return clauses
    }

    return r.rewriteTimeExtractions(clauses)
}
```

So the documented optimization is not active by default unless another caller
explicitly sets the option.

### 4. Some options are explicitly ignored but still exposed

`PlannerOptions` contains fields that are described as deprecated or ignored:

```go
UseClauseBasedPlanner   bool // Deprecated: always true. Only one planner now.
EnableDynamicReordering bool // Legacy option (ignored by clause-based planner)
MaxPhases               int  // Legacy option (ignored by clause-based planner)
EnableFineGrainedPhases bool // Legacy option (ignored by clause-based planner)
```

Keeping ignored options can be useful for compatibility, but the public docs
should clearly identify them as compatibility no-ops. Otherwise advanced users
will reasonably expect these flags to change planner behavior.

## Expected Behavior

For any documented feature or option, one of the following should be true:

1. The feature is active in the default path and documented as default.
2. The feature is implemented but opt-in, and docs show exactly how to enable it.
3. The feature is experimental/disabled, and docs do not present it as production
   behavior.
4. The option is deprecated/ignored, and docs mark it as a compatibility no-op.

## Actual Behavior

The repository currently mixes all four states:

- docs present some optimizations as complete/default
- option comments mark related flags as disabled or deprecated
- default options omit some documented rewrites
- performance docs contain historical benchmark claims without always tying them
  to the current default configuration

## Why This Matters

### 1. Users Can Tune the Wrong Knobs

An advanced user reading the docs may set `EnableConditionalAggregateRewriting`
or `EnableDynamicReordering` expecting behavior changes. The comments say those
paths are disabled, retired, or ignored.

### 2. Performance Claims Become Hard to Trust

The codebase has unusually detailed performance documentation, but performance
claims need to say whether they apply to:

- current defaults,
- opt-in options,
- archived/retired implementations, or
- benchmarks from a previous architecture.

Without that distinction, the docs overstate confidence in the shipped path.

### 3. Future Bug Triage Gets Noisy

When behavior differs from docs, users report "planner bug" or "optimization
regression" even when the actual issue is a disabled flag or stale doc.

### 4. Sole-Author Codebases Need Executable Truth

This repository contains a lot of valuable design history. The risk is that
history and current truth live side by side without a strong marker separating
them. That is especially dangerous in a database, where stale architecture notes
can lead to wrong assumptions during correctness work.

## Failure Modes

### Failure Mode 1: User Assumes Time Rewrite Is Default

User writes:

```clojure
[:find ?e
 :where [?e :price/time ?t]
        [(year ?t) ?y]
        [(= ?y 2025)]]
```

Docs imply this folds to a range predicate. Default options leave
`EnableSemanticRewriting` false, so the expression path runs normally.

The query is correct but slower than documented.

### Failure Mode 2: User Enables Retired Conditional Aggregate Flag

User reads the performance docs, enables:

```go
planner.PlannerOptions{
    EnableConditionalAggregateRewriting: true,
}
```

The option exists, but the type comment says the feature is disabled/moved to
experimental. The user has no clear way to know whether the flag is live,
ignored, or partially live.

### Failure Mode 3: Maintainer Investigates an Ignored Option

A maintainer sees a benchmark difference and toggles `MaxPhases` or
`EnableFineGrainedPhases`, not realizing the current clause-based planner ignores
those fields.

That wastes debugging time and can produce misleading conclusions.

## Fix Direction

### 1. Create a Single Current-Truth Options Reference

Update `docs/reference/PLANNER_OPTIONS.md` so every option is categorized:

- **Default-active**
- **Opt-in active**
- **Compatibility no-op**
- **Experimental/disabled**
- **Removed in effect; retained for API compatibility**

Each option should state:

- default value from `storage.DefaultPlannerOptions`
- whether it changes current code behavior
- which files consume it
- whether it affects correctness, performance, or observability

### 2. Align Top-Level Docs With Defaults

Update `README.md`, `ARCHITECTURE.md`, `TODO.md`, and
`PERFORMANCE_STATUS.md` to distinguish:

- current default behavior
- optional behavior
- archived historical behavior

Avoid "production-ready" claims immediately next to known disabled/experimental
features unless the caveat is explicit.

### 3. Consider Deprecation Enforcement

For ignored options, consider one of:

1. remove them if API stability is not required,
2. mark them with `Deprecated:` Go doc comments so `go doc` surfaces it, or
3. validate options and emit annotation/warning events when no-op fields are set.

Example:

```go
// Deprecated: ignored by the clause-based planner. Kept only for source
// compatibility with older tests/configuration.
EnableDynamicReordering bool
```

### 4. Add Drift Tests

If docs keep a machine-readable options table, add a small test that verifies
the documented defaults match `DefaultPlannerOptions()`.

At minimum, add tests around high-risk assumptions:

- semantic rewriting default state
- algebra optimizer default state
- conditional aggregate rewriting flag behavior
- deprecated planner flags do not affect plans

## Verification Plan

1. Run a documentation audit for all mentions of:
   - `EnableSemanticRewriting`
   - `EnableConditionalAggregateRewriting`
   - `EnableSubqueryDecorrelation`
   - `EnableDynamicReordering`
   - `MaxPhases`
   - `UseClauseBasedPlanner`

2. For each mention, label it as current/default/opt-in/archived.

3. Add or update tests:
   - `TestDefaultPlannerOptions_DocumentedDefaults`
   - `TestDeprecatedPlannerOptions_DoNotChangePlan`
   - `TestSemanticRewriting_DefaultMatchesDocs`

4. Re-run:

```bash
go test ./...
```

## Related

- `docs/wip/QUERY_EXECUTOR_FEATURE_GAPS.md` already documents some remaining
  QueryExecutor optimization gaps and is more careful about status than the
  top-level roadmap.
- `PERFORMANCE_STATUS.md` contains valuable benchmark history but needs sharper
  labeling of current defaults versus historical/opt-in paths.
- `TODO.md` currently mixes roadmap, marketing summary, and historical status in
  one file; that makes drift more likely.
