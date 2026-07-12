# Legacy Metadata and Datalog IR Gaps

**Reviewed:** 2026-07-11  
**Status:** Inventory and staged cleanup proposal  
**Scope:** Planner/executor control flow, not persisted data compatibility

## Summary

Janus Datalog's active architecture is Datalog-in/Datalog-out:

1. EDN or the query builder produces `*query.Query`.
2. The algebra optimizer rewrites Datalog through compile/optimize/decompile.
3. `ClauseBasedPlanner` emits `RealizedPhase.Query` fragments.
4. `DefaultQueryExecutor` executes those fragments.
5. `PatternMatcher` receives one-pattern Datalog query fragments.

Several older planning structures, opaque metadata paths, compatibility aliases,
and parallel subquery implementations remain around that architecture. They are
not equally problematic. This document separates:

- Confirmed active debt
- High-confidence dead-code candidates
- Intentional compatibility surfaces
- Typed non-Datalog contracts that should remain

The goal is not indiscriminate deletion. Each removal must be proven by tests,
the complex-query checkpoint, and direct call-site evidence.

## Inventory

### 1. `RealizedPhase.Metadata`

`RealizedPhase` is the active planner/executor interchange type:

```go
type RealizedPhase struct {
    Query     *query.Query
    Available []query.Symbol
    Provides  []query.Symbol
    Keep      []query.Symbol
    Metadata  map[string]interface{}
}
```

Known keys:

#### `constant_bindable_inputs` — active

- Written by `planner/planner_clause_based.go`
- Read by `executor/executor.go`
- Structurally tested by `storage/predicate_product_test.go`
- Affects execution semantics by deciding which scalar inputs become constants
  rather than relation groups

This is real behavior hidden behind an untyped string key. It should not be
deleted without a replacement.

Preferred cleanup options:

1. Add a typed `ConstantBindableInputs []query.Symbol` field to
   `RealizedPhase`.
2. Derive the classification directly from the Datalog phase fragment at the
   executor boundary.

Option 2 better preserves Datalog as the IR, but it must not duplicate planner
dependency analysis. This requires an explicit design decision.

#### `conditional_aggregates` — likely dead compatibility read

`executor/executor.go` still scans:

```text
phase.Metadata["conditional_aggregates"]
```

The current planner has no writer for that key. Conditional aggregate semantics
are represented in Datalog by `FindAggregate.Predicate`, which
`QueryExecutor` consumes directly.

This appears to be an observability-only remnant from the removed legacy
executor. Before removal:

- Add a guard proving conditional-aggregate annotations come from the find
  clause.
- Remove the metadata read and dual-representation comments.
- Run conditional aggregation, decorrelation, and complex-query tests.

### 2. `Context` metadata map

`executor.Context` exposes:

```go
SetMetadata(key string, value interface{})
GetMetadata(key string) (interface{}, bool)
```

Current production code contains no metadata writer or reader outside context
delegation. The only direct users are metadata microbenchmarks.

This is a high-confidence dormant infrastructure candidate, with one caveat:
`Context` is exported from the executor package and may be used by advanced
external callers.

Suggested sequence:

1. Search downstream users before removal.
2. Deprecate the methods if external compatibility matters.
3. Remove the map and delegation only after one release boundary.

Do not replace it with another generic context-value mechanism.

### 3. Legacy `QueryPlan` / `Phase` graph

The active `ClauseBasedPlanner` constructs `RealizedPlan` directly. The older
types remain in `planner/types.go`:

- `QueryPlan`
- `Phase`
- `PatternPlan`
- `SubqueryPlan.NestedPlan`
- `DecorrelatedSubqueryPlan`
- `QueryPlan.Realize`

Repository searches find no production construction or assignment of:

- `QueryPlan{...}`
- `SubqueryPlan.NestedPlan`
- `Phase.DecorrelatedSubqueries`

`PatternPlan` still has an active explain-analysis use, so the entire type
family cannot be deleted as one mechanical block.

The old graph is also referenced by the legacy executor-level subquery
functions in `executor/subquery.go`, creating a mutually supporting dead-code
cluster. Remove or migrate the cluster together, not piecemeal.

### 4. Parallel subquery execution implementations

There are two distinct subquery families:

#### Active `DefaultQueryExecutor` implementation

`query_executor.go` executes nested Datalog recursively and supports:

- Default per-combination execution
- Opt-in componentized execution
- Batched, parallel, and sequential strategies
- Streaming/materialized union selection

This path is active and must remain until benchmark evidence selects one
strategy.

Its default branch still emits:

```text
"path": "Legacy QueryExecutor"
```

That label is stale: `QueryExecutor` is now the active architecture. Rename the
annotation value independently of any implementation removal.

#### Executor-level `SubqueryPlan` implementation

`executor/subquery.go` contains another sequential/parallel/batched subsystem
based on `planner.SubqueryPlan` and `QueryPlan.Realize`.

The public executor path does not call `Executor.executeSubquery`; active
subqueries route through `DefaultQueryExecutor.executeSubquery`.

This is a high-confidence removal candidate, but it has internal benchmarks and
tests whose intent may still be valuable. Before deletion:

1. Map each test to the active QueryExecutor equivalent.
2. Move any missing worker-count/error/streaming assertions.
3. Confirm no exported consumer calls `ExecuteSubquery`.
4. Delete the old planner graph and executor subsystem together.

### 5. Planner metadata inside old plan types

The older types contain additional opaque maps:

- `QueryPlan.Metadata`
- `Phase.Metadata`
- `PatternPlan.Metadata`
- `PredicatePlan.Metadata`
- `ExpressionPlan.Metadata`

`PatternPlan.Metadata["storage_constraints"]` participates in the old
`QueryPlan.Realize` reconstruction path. It should be evaluated as part of the
old-plan removal, not independently migrated into the active planner.

The active planner's explain fields are typed and observability-only. They are
not the same problem and should not be removed merely because they are outside
the Datalog query.

## Contracts That Are Not Debt

Not every non-Datalog field violates Datalog-as-IR.

### `Available`, `Provides`, and `Keep`

These are typed phase-boundary symbol contracts. They describe how Datalog
fragments compose and prevent invalid projections or Cartesian products. They
are not opaque optimization hints.

They may eventually be derivable from `Query.In`, `Query.Find`, and
`Query.Where`, but keeping a checked, typed contract is preferable to repeatedly
re-deriving it unless equivalence is proven.

### `Relation.Properties`

Ordering and candidate keys are physical guarantees, not logical query clauses.
They correctly live on the `Relation` interface while using Datalog symbols and
ordering terms as vocabulary.

### Explain fields

`RealizedPhase.Patterns`, `Expressions`, `Predicates`, and `Subqueries` are
typed optional observability data. They do not alter execution semantics.

## Intentional Compatibility Surfaces

These should not be mixed into planner/executor cleanup:

- Legacy compressed blob decoding
- Old export transaction formats and optional CRDT operation fields
- Schemaless data migration behavior
- Public aliases such as `Tuple`, `Result`, and constraint re-exports
- Deprecated public methods retained for API compatibility

Persisted data compatibility requires an explicit migration and support policy.
It is categorically different from dead in-process execution code.

## Staged Cleanup Plan

### Stage 1: Remove the dead conditional-aggregate metadata read

- Pin annotation behavior to `FindAggregate.Predicate`
- Remove `phase.Metadata["conditional_aggregates"]` reads
- Remove comments claiming a dual executor representation

Risk: low.

### Stage 2: Replace `constant_bindable_inputs`

Choose between:

- Typed `RealizedPhase` field
- Deterministic derivation from the Datalog phase fragment

Risk: medium; this affects parameter/environment semantics.

### Stage 3: Retire generic `Context` metadata

- Verify external users
- Deprecate if necessary
- Remove map and delegation

Risk: low internally, compatibility-dependent externally.

### Stage 4: Retire the old plan/subquery cluster

- Migrate useful tests to active QueryExecutor paths
- Remove `QueryPlan`, old `Phase`, nested-plan execution, and their metadata
- Preserve typed explain analysis independently

Risk: high because the cluster is large despite appearing unreachable.

### Stage 5: Remove stale naming and options

- Rename `"Legacy QueryExecutor"` annotation
- Re-benchmark componentized versus default subquery execution
- Keep one implementation only if measurements and semantics agree
- Remove options that no longer select a real production path

Risk: medium.

## Verification Gates

Every stage must pass:

```bash
go test -count=1 ./...
```

Additional required checks:

- Conditional aggregate structural tests
- Input parameter and constant-binding tests
- Subquery sequential/parallel/error-propagation tests
- Optimization matrix result equality
- `BenchmarkComplexQueryCheckpoint`

Current complex-query checkpoint:

- 47.88 ms/op
- 87.57 MiB/op
- 1.118M allocations/op

The property-foundation checkpoint is:

- 48.48 ms/op, statistically unchanged
- 82.80 MiB/op
- 1.088M allocations/op

Cleanup is successful only if semantics remain unchanged and performance does
not regress. Line-count reduction alone is not a success criterion.

## Desired End State

- Planner and algebra optimizer accept and emit Datalog query fragments
- PatternMatcher accepts one-pattern Datalog fragments
- Realized phases contain Datalog plus typed symbol-flow contracts
- Physical guarantees live on `Relation`
- No `map[string]interface{}` participates in active planning/execution control
- One active subquery execution architecture
- Compatibility shims and persisted-format readers are explicitly documented
  rather than confused with dead code
