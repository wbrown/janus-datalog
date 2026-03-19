# BUG: EBNF TransformPreserveStructure Drops Rewrites in Deep Trees

## Summary

`parse.TransformPreserveStructure` applies transform functions bottom-up but
does not propagate rewritten child nodes to parent nodes in trees with depth > 2.
A transform that rewrites a child node succeeds locally (the transform function
returns a new node), but the parent retains the original child, discarding the
rewrite.

## Impact

The relational algebra optimizer's decorrelation pass identifies correlated
aggregate subqueries correctly and rewrites them (LateralJoin → Join +
decorrelatedScan), but the rewritten nodes are lost during tree reconstruction.
The optimizer reports `changed:false` and returns the original clauses.

This blocks the 194x query speedup (29s → ~150ms) on production data.

## Reproduction

### Working case (shallow tree)

The `TestCorrelatedSubqueryAlgebraOptimizer` test creates a tree with a simpler
structure. The decorrelation transform fires and the rewrite propagates
correctly. Result: 75 results in 84ms (vs 16s baseline).

### Failing case (deep tree)

The production `ListScenarioSummaries` query compiles to a 13-clause tree with
multiple LateralJoin nodes nested inside a larger Join/Select/Map structure.
The decorrelation transform fires (`algebra/decorrelate-apply` annotation
emitted) but the parent nodes retain the original LateralJoin children.

Annotation trace from production profiler:
```
algebra/bridge-begin          clause_count:13
algebra/compiled              tree:LateralJoin(⋈_L(?scenario) +defaults) ...
algebra/decorrelate-check     correlation_vars:[?scenario] has_aggregates:true
algebra/decorrelate-apply     correlation_vars:[?scenario] inner_params:[?s]
algebra/decorrelate-check     correlation_vars:[?scenario] has_aggregates:true
algebra/decorrelate-apply     correlation_vars:[?scenario] inner_params:[?s]
algebra/decorrelate-check     has_aggregates:false
algebra/decorrelate-skip      reason:no aggregates in :find
algebra/optimized             tree:LateralJoin(⋈_L(?scenario) +defaults)  ← UNCHANGED
algebra/bridge-complete       changed:false
```

The transform applies twice but the optimized tree is identical to the input.

## Root Cause (Hypothesis)

`TransformPreserveStructure` walks the tree bottom-up. When a transform function
returns a new node (different from the input), the new node should replace the
old one in its parent's children list. The `TransformedValue` copy fix
(commit 9658a1a) handles the case where a node is PRESERVED (not transformed)
but needs its `TransformedValue` carried forward. The unfixed case is when a
node IS transformed (returns a completely new `*parse.Node` or `*Node`) — the
parent may not pick up the replacement.

Specifically, the `collapseLeftOuterJoinTransform` runs on `Join` nodes AFTER
the `decorrelateTransform` has replaced `LateralJoin` children with
`Join(decorrelatedScan)` nodes. If the parent `Join` node receives the
original children (not the rewritten ones), it sees no `decorrelatedScan`
child and returns unchanged.

## Suggested Investigation

1. Write a minimal EBNF test with a 3-level tree (Root → Parent → Child)
2. Apply a transform that rewrites Child to NewChild
3. Verify Parent receives NewChild (not the original Child) in its children
4. Apply a second transform on Parent that depends on seeing NewChild
5. Verify the second transform fires correctly

## Files

- `datalog/algebra/optimize.go` — calls `TransformPreserveStructure`
- `datalog/algebra/rewrite_decorrelate.go` — the decorrelation transforms
- `ebnf/parse/transform.go` — `TransformPreserveStructure` implementation
- `datalog/planner/algebra_bridge.go` — bridge with annotations
