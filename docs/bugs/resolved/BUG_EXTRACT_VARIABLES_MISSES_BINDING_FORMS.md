# Bug: `ExtractVariables` Missed Expression and Or-Default Binding Forms

**Status**: FIXED (2026-07-04)
**Discovered**: 2026-07-04, when order-by validation became the function's
first production consumer and falsely rejected valid queries

## Symptoms

`parser.ExtractVariables` — the parser's extraction of variables *provided
by* `:where` clauses — was blind to three clause forms that bind variables:

- `*query.Expression` output bindings, scalar and tuple:
  `[(ground 0) ?count]`, `[(+ ?a 1) ?b]`,
  `[(ground [:none :none]) [[?k ?v]]]`
- `*query.OrDefaultClause` (branch intersection, like `OrClause`)
- `*query.OrDefaultJoinClause` (join variables, like `OrJoinClause`)

The blindness compounds inside `(or ...)`: the provided set of an or-clause
is the **intersection** of its branches, so a branch consisting of a ground
expression contributed nothing and emptied the intersection — variables
genuinely bound by *every* branch became invisible. The canonical shape,
from the query that surfaced the bug (`TestPhaserWithDecorrelatedClauses`):

```clojure
(or [(q [:find ?key ?ca ...] $ ?scenario) [[?lastKey ?lastUpdatedAt]]]
    [(ground [:none :none]) [[?lastKey ?lastUpdatedAt]]])
```

`?lastUpdatedAt` is bound by both branches (a subquery tuple binding and an
expression tuple binding), yet `ExtractVariables` reported it unbound.

## Why It Was Latent

The function's only production caller was `ValidateQuery`, which itself has
no production callers — so the incomplete extraction sat unexercised. Had
`ValidateQuery` ever been wired in, its find-variable check would have
produced false "find variable not bound in where clause" errors for any
expression-bound or or-default-bound find variable. The defect surfaced the
moment order-by sort-key validation
(`BUG_ORDER_BY_NON_PROJECTED_VARIABLE_SILENTLY_IGNORED.md`) became the
first production consumer: valid queries ordering by expression-bound
variables were rejected with "order-by variable ?x is not bound in the
query".

## Root Cause

`ExtractVariables`'s clause-type switch (`datalog/parser/parser.go`)
enumerated `DataPattern`, `SubqueryPattern`, `NotClause`, `NotJoinClause`,
`OrClause`, and `OrJoinClause`, and silently skipped every other clause
type. `Expression`, `OrDefaultClause`, and `OrDefaultJoinClause` all bind
variables and had no case. (The skip was structurally silent — an
unmatched clause type contributed nothing rather than failing — so each
newly added clause form since the function was written quietly widened the
gap.)

## Fix

Three cases added, each mirroring the semantics of its sibling:

- `*query.Expression` — the output binding provides (scalar `Symbol` or
  `TupleBinding.Variables`); argument variables are consumed, not provided.
- `*query.OrDefaultClause` — intersection of branch-provided sets, exactly
  as `OrClause`.
- `*query.OrDefaultJoinClause` — exposes `JoinVars`, exactly as
  `OrJoinClause`.

## Test Coverage

- `datalog/parser/order_by_test.go` → `TestOrderByValidation`:
  "expression-bound variable is valid", "variable bound by all or-branches
  is valid", "subquery-bound variable inside or is valid".
- `datalog/planner/algebra_phasing_test.go` →
  `TestPhaserWithDecorrelatedClauses` — the production-shaped query that
  exposed the bug, red before the fix, green after.

## Pattern

A provides-extraction over a clause taxonomy must cover every clause form
that binds, and its default should be loud, not silently empty — the same
lesson as `hashValue`'s default
(`BUG_VECTOR_VALUES_DEGENERATE_HASHING.md`): silent structural fallthrough
converts each future addition to the taxonomy into a latent bug.
