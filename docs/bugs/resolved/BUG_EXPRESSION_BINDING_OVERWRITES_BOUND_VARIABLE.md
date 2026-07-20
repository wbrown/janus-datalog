# BUG: Expression output onto an already-bound variable overwrites instead of unifying

**Status**: RESOLVED (2026-07-20). The unification semantics now have a single home — `bindingAlignment` / `alignBinding` / `apply` in `datalog/executor/expression_binding.go` — consumed by every path that applies expression results to relation tuples: all four branches of the per-tuple path (`evaluateExpressionWithLookup`) and the ground-constants path in `executeExpression`, which previously implemented the semantics independently (correctly) and was converted to the shared implementation so the two-copies divergence mechanism itself is gone. `crossJoinWithExpression` was audited: it delegates its expression application to `evaluateExpressionWithLookup` and carried no third copy. Bound positions filter and are never written, so relation properties (ordering, keys) naming them stay valid and only extension symbols invalidate them. Malformed tuple-binding results now error loudly, matching the ground path's wording ("tuple binding requires tuple result" / "tuple mismatch: N values, M variables"), including expansion rows whose arity mismatches the binding — previously a silent skip. All pins green; the full gate (native + wasm) found no test depending on overwrite semantics.

## Symptom

When an expression's binding variable is already bound in the relation — `[(+ ?x 1) ?y]` with `?y` bound by a pattern, or `[(enumerate ?vec) [?idx ?tag]]` with `?idx`/`?tag` bound — the per-tuple evaluation path **replaces** the tuple's existing value with the computed one. Correct Datalog semantics (Datomic unification) treat a bound binding as an equality constraint: the tuple survives iff the computed value equals the existing value, unchanged; otherwise it is dropped. Nothing is ever written over a bound value.

The corruption is silent and produces internally inconsistent rows. In the composed OHLC decorrelation failure, expressions recomputing `?year/?month/?day/?hour` overwrote the join-key columns of rows produced by an under-keyed join, yielding rows whose `?datetime` came from the outer bar while the aggregates belonged to a *different* group — wrong values with no error signal.

## Root cause

`evaluateExpressionWithLookup` (`datalog/executor/relation_ops.go`) diverges from the executor's own documented contract. The ground-constants path of `executeExpression` (`datalog/executor/query_executor.go`) implements the correct semantics and says so: *"For each binding symbol: if already in the relation, filter (unify) instead of extending."* The per-tuple path instead:

1. **Scalar binding, symbol bound**: writes `evalResult` over the bound position.
2. **Tuple binding, all symbols bound**: writes each value over the bound positions.
3. **Multi-row expansion (enumerate), all symbols bound**: emits one row per expansion row, each overwriting the bound positions — turning a membership filter into a fan-out.
4. **Multi-row expansion, partial overlap**: fills only the extension positions and silently *ignores* the expansion values for bound positions — emitting rows whose bound and extension values belong to different expansion rows.

Same clause type, two semantics, selected by which internal path executes.

## Reproducers

Query-level, both planner modes, no subquery — red until this defect itself is fixed, regardless of the planner fix:

- `datalog/executor/expression_unification_test.go` / `TestExpressionOntoBoundVariableUnifies/bound_by_expression`

(The sibling case `bound by pattern` is green today: for a pattern-provided binding, `patternDependsOnPendingExpression` defers the pattern behind the expression, so the join unifies. It pins that adjacent scheduling behavior — the defect needs a provider the pattern gate cannot reorder, i.e. a second expression.)

Function-level, one case per write site — all red:

- `datalog/executor/expression_unification_test.go` / `TestEvaluateExpressionUnifiesBoundBindings`

Composed with the planner defect, outcome-level: the three OHLC reproducers listed in `BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md`.

## Fix

See the Resolution in the Status line: `bindingAlignment` in `datalog/executor/expression_binding.go` is the single home; both former implementations consume it. Compare with `datalog.ValuesEqual`, keep the tuple unchanged on match (once per matching expansion row for multi-row results), drop on mismatch, never write to a bound position.

## Cross-references

- `docs/bugs/resolved/BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md` — the planner half of the composed OHLC failure, including the composition note on why each fix alone masks the other.
- `docs/bugs/resolved/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md` — the decorrelation rewrite whose output first composed the two defects.
