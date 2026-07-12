# Legacy Metadata and Datalog IR Gaps

**Reviewed:** 2026-07-11  
**Status:** Cleanup complete
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

### 1. `RealizedPhase.Metadata` — removed

`RealizedPhase` is the active planner/executor interchange type:

```go
type RealizedPhase struct {
    Query     *query.Query
    Available []query.Symbol
    Provides  []query.Symbol
    Keep      []query.Symbol
}
```

Constant-bindable scalar inputs are now represented directly as
`query.ScalarInput` entries in each phase's Datalog `:in` clause. Symbols that
must participate in pattern matching remain in the phase `RelationInput`.
The executor reads this distinction without repeating planner analysis.

#### `conditional_aggregates` — removed

Conditional aggregate semantics and rewrite observability now both derive from
`FindAggregate.Predicate` in each phase's Datalog find clause. The dead
`phase.Metadata["conditional_aggregates"]` read and dual-representation comments
were removed. A structural executor test pins the annotation count to the
Datalog representation.

### 2. `Context` metadata and dormant annotation surface — removed

The generic metadata map and its benchmark had no production users. They were
removed without replacement. Dormant annotation methods for old phase,
matching, combination, filtering, and expression paths were removed from the
exported interface at the same breaking cleanup boundary. `Context` now exposes
only lifecycle, active join/collapse annotation, collector, and scan-registry
operations.

### 3. Legacy `QueryPlan` / `Phase` graph — removed

`QueryPlan`, `Phase`, `QueryPlan.Realize`, nested plans, decorrelation plan
records, constraint-reconstruction metadata, and the executor subsystem that
kept them reachable have been removed together.

`PatternPlan`, `PredicatePlan`, `ExpressionPlan`, and `SubqueryPlan` remain only
as typed explain records on `RealizedPhase`; fields used solely by the old graph
were removed.

### 4. Parallel subquery execution implementations

There are two distinct subquery families:

#### Active `DefaultQueryExecutor` implementation

`query_executor.go` executes nested Datalog recursively and supports:

- Default per-combination execution
- Parallel RelationInput iteration through the executor worker loop

The annotation now reports `"Per-combination QueryExecutor"`.

The plan-based executor subsystem and the opt-in componentized subsystem were
removed. Benchmark execution showed the componentized path could not execute
representative correlated subqueries: it searched outer relations for the
nested query's local `:in` names instead of the call-site symbols. Its selector,
batcher, generic worker pool, streaming-union builder, options, tests, and
benchmarks therefore did not represent a valid alternative execution path.
The valid per-combination path completed five two-iteration samples at
43.1–51.7 ms/op for the 1,000-bar case and 329–372 ms/op for the 5,000-bar case;
the componentized cases failed before producing timing samples.

### 5. Planner metadata inside old plan types — removed

The metadata maps on `QueryPlan`, `Phase`, `PatternPlan`, `PredicatePlan`, and
`ExpressionPlan` were removed with the old graph. Active typed explain fields
remain.

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

### Stage 1: Remove the dead conditional-aggregate metadata read — complete

- Pin annotation behavior to `FindAggregate.Predicate`
- Remove `phase.Metadata["conditional_aggregates"]` reads
- Remove comments claiming a dual executor representation

Risk: low.

### Stage 2: Replace `constant_bindable_inputs` — complete

- Preserve scalar binding mode in each phase's Datalog `:in`
- Keep pattern-participating inputs in `RelationInput`
- Remove `RealizedPhase.Metadata`

### Stage 3: Retire generic `Context` metadata — complete

- Removed the metadata map and benchmark
- Removed dormant exported annotation methods
- Retained only operations used by current execution

### Stage 4: Retire the old plan/subquery cluster — complete

- Migrate useful tests to active QueryExecutor paths
- Remove `QueryPlan`, old `Phase`, nested-plan execution, and their metadata
- Preserve typed explain analysis independently

Risk: high because the cluster is large despite appearing unreachable.

### Stage 5: Remove stale naming and options — complete

- Renamed `"Legacy QueryExecutor"` annotation
- Exercised componentized versus default subquery execution
- Removed the componentized path after it failed correlated-query semantics
- Removed options that no longer selected a valid production path

Result: one active subquery architecture and no stale strategy options.

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
