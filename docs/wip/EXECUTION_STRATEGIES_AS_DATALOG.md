# Open Question: Expressing Execution Strategies as Pure Datalog

**Status**: OPEN QUESTION
**Date**: January 2026

## Context

The `ClauseBasedPlanner` (Stage C architecture) produces `*query.Query` fragments that flow to `QueryExecutor`. This creates a clean interface boundary where Datalog is the universal language:

```
Query + Relations → Relations
```

Two optimizations from the old planner have not been implemented in `ClauseBasedPlanner`:

1. **Semantic Rewriting** (`EnableSemanticRewriting`)
2. **Conditional Aggregate Rewriting / Decorrelation** (`EnableConditionalAggregateRewriting`)

The challenge: how do you express these as pure Datalog clause transformations?

## The Tension

These optimizations change *how* a query executes, not *what* it computes. They're execution strategies:

| Optimization | What it does | Challenge |
|--------------|--------------|-----------|
| Semantic rewriting | `[(= (year ?t) 2025)]` → use time range scan | How to express "use range scan" in Datalog? |
| Decorrelation | Batch N identical subqueries into one execution | How to express "batch these" in Datalog? |

The old planner embedded these strategies in its `Phase` structure, which had fields like `StorageConstraints` and `DecorrelatedSubqueries`. The `ClauseBasedPlanner` produces only `*query.Query`, which has no such mechanism.

## Possible Approaches

### 1. Pure Clause-to-Clause Transformation

For semantic rewriting, the predicate `[(= (year ?t) 2025)]` could be rewritten as:

```datalog
[(>= ?t #inst "2025-01-01T00:00:00Z")]
[(< ?t #inst "2026-01-01T00:00:00Z")]
```

This IS a pure Datalog transformation. The executor already knows how to push range predicates to storage. No new constructs needed.

**Open question**: Does this fully capture the optimization? Are there cases where the transformation isn't semantically equivalent?

### 2. New Language Constructs

For decorrelation, the old planner transforms correlated aggregates into "conditional aggregates":

```datalog
;; Before: correlated subquery (executed N times)
[(q [:find (max ?price) :in $ ?symbol :where ...]) [[?max]]]

;; After: conditional aggregate (executed once)
[(max-if ?price ?condition) ?max]
```

But `max-if` isn't standard Datalog. This approach extends the language.

**Open question**: Is there a way to express this without new constructs? Can it be decomposed into existing Datalog operations?

### 3. Executor Pattern Detection

The executor could detect patterns like:
- "This is a correlated subquery that can be batched"
- "This range predicate can use an index scan"

And optimize accordingly, without the planner explicitly marking them.

**Open question**: Does this duplicate planner logic? Is it the executor's job to optimize?

### 4. Metadata/Hints on Query

Add optional metadata to `query.Query` that guides execution:

```go
type Query struct {
    Find  []FindElement
    In    []Input
    Where []Clause
    Hints map[string]interface{}  // Execution hints
}
```

**Open question**: Does this break the "pure Datalog" abstraction? Is there a cleaner way?

## Current Status

- **Old Planner** (default): Has both optimizations, embeds strategies in `Phase` structure
- **ClauseBasedPlanner** (opt-in): Defers these optimizations with TODOs

Both planners produce identical results for queries that don't benefit from these optimizations (verified by `TestPlannerExecutionComparison`).

## Why This Matters

The goal of the Stage C architecture is:

> "Datalog is the universal interface - every layer speaks Query + Relations → Relations"

If execution strategies can be expressed as pure clause transformations, the architecture is clean. If they require escaping into hints or new constructs, the abstraction leaks.

## Questions to Resolve

1. **Semantic rewriting**: Can `[(= (year ?t) 2025)]` → range predicates fully capture the optimization?

2. **Decorrelation**: Can subquery batching be expressed without new language constructs? Is there a decomposition into existing Datalog operations?

3. **Interface boundary**: If hints are needed, where do they live? On `Query`? On `RealizedPhase.Metadata`? Somewhere else?

4. **Executor responsibility**: Should the executor be smart enough to detect optimization opportunities, or should the planner explicitly encode them?

## References

- `docs/wip/PHASE_AS_QUERY_ARCHITECTURE.md` - Stage C architecture proposal
- `docs/archive/completed/PLANNER_COMPARISON.md` - Planner comparison (archived; one planner now — see `docs/reference/PLANNER_OPTIONS.md`)
- `datalog/planner/planner_clause_based.go:102-104` - TODOs for these optimizations
- `datalog/planner/predicate_rewriter.go` - Current semantic rewriting implementation (old planner)
- `datalog/planner/decorrelation.go` - Current decorrelation implementation (old planner)
