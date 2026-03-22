# Decorrelation inside Union branches produces Cartesian products

GitHub: #58 (exposed by fixing the NOT clause bug)

## Trigger

A query with `(or ...)` containing a correlated subquery + ground default
produces extra rows (Cartesian product) when the algebra bridge is enabled.
The base executor without the bridge produces the correct result.

## Root cause

The decorrelation pass (`rewrite_decorrelate.go`) transforms correlated
subqueries inside Union branches into uncorrelated ones. This is semantically
incorrect.

### Before decorrelation (inside Union branch)

```clojure
(q [:find (count ?i) :in $ ?p :where ...] $ ?project) [[?count]]
```

Correlated: `:in $ ?p`, runs per outer tuple.

### After decorrelation

```clojure
(q [:find ?p (count ?i) :in $ :where ...] $) [[?project ?count] ...]
```

Uncorrelated: `:in $`, `?p` moved to find. Runs once globally.

### Why this breaks Union semantics

Decorrelation moves the correlation variable from input to output. The
subquery branch's schema changes from `{?count}` to `{?project, ?count}`.
The ground branch's schema remains `{?count}` — no `?project`.

The Union now has branches with incompatible schemas. When joined with the
outer relation:
- Subquery rows have `?project` → join works
- Ground rows lack `?project` → cross product

## Fix

The decorrelation transform checks `ctx.Parent.Rule == RuleUnion` and skips
decorrelation for LateralJoins inside Union nodes. This uses the EBNF
transform framework's `TransformContext.Parent` for structural context.

This is a semantic constraint, not avoidance: decorrelation is only valid when
the LateralJoin result is consumed directly by a Join. Inside a Union, the
result is unioned with branches that may not have the correlation variable.

### Supporting fix

`compileSubquery` in `algebra/compile.go` now always includes correlation
variables in the LateralJoin output, even when compiled without outer context
(`current=nil`, as inside Union branches). Previously, output was only
`bindingSyms` when `current=nil`, causing the Union to have no overlap with
the outer relation for joining.
