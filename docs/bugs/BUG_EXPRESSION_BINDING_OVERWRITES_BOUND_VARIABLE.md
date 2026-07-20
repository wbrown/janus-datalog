# BUG: Expression output onto an already-bound variable overwrites instead of unifying

**Status**: Open (2026-07-20)

## Symptom

When an expression's binding variable is already bound in the relation —
`[(+ ?x 1) ?y]` with `?y` bound by a pattern, or `[(enumerate ?vec) [?idx ?tag]]`
with `?idx`/`?tag` bound — the per-tuple evaluation path **replaces** the
tuple's existing value with the computed one. Correct Datalog semantics
(Datomic unification) treat a bound binding as an equality constraint: the
tuple survives iff the computed value equals the existing value, unchanged;
otherwise it is dropped. Nothing is ever written over a bound value.

The corruption is silent and produces internally inconsistent rows. In the
composed OHLC decorrelation failure, expressions recomputing
`?year/?month/?day/?hour` overwrote the join-key columns of rows produced by
an under-keyed join, yielding rows whose `?datetime` came from the outer bar
while the aggregates belonged to a *different* group — wrong values with no
error signal.

## Root cause

`evaluateExpressionWithLookup` (`datalog/executor/relation_ops.go`) diverges
from the executor's own documented contract. The ground-constants path of
`executeExpression` (`datalog/executor/query_executor.go`) implements the
correct semantics and says so: *"For each binding symbol: if already in the
relation, filter (unify) instead of extending."* The per-tuple path instead:

1. **Scalar binding, symbol bound**: writes `evalResult` over the bound
   position.
2. **Tuple binding, all symbols bound**: writes each value over the bound
   positions.
3. **Multi-row expansion (enumerate), all symbols bound**: emits one row per
   expansion row, each overwriting the bound positions — turning a
   membership filter into a fan-out.
4. **Multi-row expansion, partial overlap**: fills only the extension
   positions and silently *ignores* the expansion values for bound positions
   — emitting rows whose bound and extension values belong to different
   expansion rows.

Same clause type, two semantics, selected by which internal path executes.

## Reproducers

Query-level, both planner modes, no subquery — red until this defect itself
is fixed, regardless of the planner fix:

- `datalog/executor/expression_unification_test.go` /
  `TestExpressionOntoBoundVariableUnifies/bound_by_expression`

(The sibling case `bound by pattern` is green today: for a pattern-provided
binding, `patternDependsOnPendingExpression` defers the pattern behind the
expression, so the join unifies. It pins that adjacent scheduling behavior —
the defect needs a provider the pattern gate cannot reorder, i.e. a second
expression.)

Function-level, one case per write site — all red:

- `datalog/executor/expression_unification_test.go` /
  `TestEvaluateExpressionUnifiesBoundBindings`

Composed with the planner defect, outcome-level: the three OHLC reproducers
listed in `BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md`.

## Fix direction (pending ratification)

Unify in place at all four sites: compare with `datalog.ValuesEqual`, keep
the tuple unchanged on match (emit once per matching expansion row for
multi-row results), drop on mismatch, never write to a bound position. Audit
the sibling paths for the same divergence before closing:

- `crossJoinWithExpression` (multi-relation expression evaluation)
- any streaming expression-application path

## Cross-references

- `docs/bugs/BUG_UNCORRELATED_SUBQUERY_SCHEDULES_BEFORE_BINDING_PROVIDERS.md`
  — the planner half of the composed OHLC failure, including the composition
  note on why each fix alone masks the other.
- `docs/bugs/BUG_DECORRELATION_PREDICATE_ONLY_INPUT_SYMBOLS.md` — the
  decorrelation rewrite whose output first composed the two defects.
