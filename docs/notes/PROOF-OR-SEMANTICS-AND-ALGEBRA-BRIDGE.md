# Or Semantics, the Algebra Bridge, and the Correct Fix for Issue #58

## 1. The Problem Statement

GitHub issue #58 reports:

```
query execution failed: phase 1 failed: clause 12 (not) failed:
NOT clause variables not found in input relation
```

The query structure: pattern + NOT + 6 get-else + 3 OR-with-subquery branches +
comparison binding + order-by. The OR branches use `qb.Or()` which produces
`*query.OrClause` (union semantics), with a subquery branch and a ground default
branch — a fallback pattern.

## 2. What We Fixed (Verified)

### 2.1. NOT → NOT-JOIN in the algebra bridge

**One-line fix**: `compileNot` in `algebra/compile.go` already computed
`joinSyms = sharedSymbols(current.Symbols(), inner.Symbols())` but set
`ExplicitJoin: false`. Changing to `true` makes the decompiler emit
`NotJoinClause` with declared join vars. The executor uses standard scheduling
with zero special cases.

**Verified**: `TestRoundTrip_AntiJoin` passes, `TestNotClauseWithUnboundInnerVar_E2E`
passes, all algebra/planner/executor/parser tests pass.

### 2.2. FormatValueEDN for round-trippable String()

Query `String()` methods used `fmt.Sprintf("%v")` which produced non-parseable
output. Added `FormatValueEDN` and updated all `String()` methods.

### 2.3. Vector constant tagged literals

The parser's vector constant switch didn't handle `edn.NodeTagged`. Added the
case, delegating to `parseTaggedLiteral`. Fixed `#db/id` vs `#identity` tag
mismatch in `FormatValueEDN`.

## 3. The Four Clause Types and Their Semantics

The engine has four OR-family clause types:

| Clause | Semantics | Behavior per outer tuple |
|--------|-----------|------------------------|
| `(or ...)` | Union | Execute ALL branches, union results |
| `(or-join [...] ...)` | Union + explicit join | Same as or, with declared join vars |
| `(or-default ...)` | Fallback | Try branches in order, stop at first match |
| `(or-default-join [[?req ...] ?out ...] ...)` | Fallback + declared interface | Per-group fallback on the required vars; outputs bound by every branch |

**Critical distinction**: union vs fallback.

For a subquery + ground default pattern like:
```clojure
(or [(q [:find (count ?i) :in $ ?p :where ...] $ ?project) [[?count]]]
    [(ground 0) ?count])
```

- **Union**: both branches execute. If the subquery returns `count=2`, the result
  is `{(project, 2), (project, 0)}` — TWO rows per entity. The ground branch
  always matches, so every entity gets an extra row.

- **Fallback**: try the subquery. If it returns results, use them. Otherwise, use
  the ground default. Result: ONE row per entity.

With 3 such ORs using union semantics, the rows multiply: `2 × 2 × 2 = 8` for
entities where the subquery matches. This is correct union behavior.

## 4. Empirical Verification

We ran the exact same `(or ...)` query through both execution paths:

```
TestGetElseComplex_OrSemantics:
  Without algebra bridge: 9 results
  With algebra bridge:    9 results
```

Both paths agree. 9 rows is the correct result for union semantics with this
query structure (8 for proj:1 where both branches match in all 3 ORs, 1 for
proj:2 where both branches produce identical values and deduplicate).

For `(or-default ...)`, both paths produce 2 rows (one per project). This is
also correct.

**Conclusion**: the algebra bridge preserves semantics for both `or` and
`or-default`. There is no algebra bridge bug for union semantics.

## 5. Why the Production Code Gets Wrong Results

The production code (`buildScenarioSummaryQuery`) uses `qb.Or()` for a pattern
that requires fallback semantics:

```go
qb.Or().
    Branch(qb.Subquery(taskStatsSubquery, scenario).BindTuple(...)).
    Branch(qb.TupleGround(0, 0, 0, 0, 0).As(...))
```

This produces `*query.OrClause` (union). The intent is "get task stats, or
default to zeros" — which is fallback. The fix is:

```go
qb.OrDefault().
    Branch(qb.Subquery(taskStatsSubquery, scenario).BindTuple(...)).
    Branch(qb.TupleGround(0, 0, 0, 0, 0).As(...))
```

This is a **caller bug**, not an engine bug.

## 6. Why `or` → `or-join` Conversion is NOT Safe

We attempted to convert `(or ...)` → `(or-join [...] ...)` in the algebra bridge,
mirroring the successful `not` → `not-join` conversion. This broke two existing
tests:

- `TestOrCorrelatedUnionPartialOuterRelation`
- `TestOrCorrelatedUnionWithNestedOrExpression_E2E`

Error: `cannot project: symbol ?related not found in relation`

### 6.1. Why not → not-join works

`NotClause` and `NotJoinClause` go through two separate executor functions built
from the same pieces:

- `executeNotClause` → `filterWithNotClause(ctx, clause, input, joinVars)`, where the join
  variables come from `query.ScopeOf(clause)`
- `executeNotJoinClause` → `filterWithNotJoinClause(ctx, clause, input)`, which classifies
  `clause.JoinVars` itself — subject-carried symbols become the anti-join key,
  environment-bound ones become a body constraint, and a symbol that is neither is a
  loud error

There is no shared filter function. Each calls the same building blocks
independently — `Materialize`, `notBodyBinding`, `getUniqueCombinations`,
`TupleKeyMap`, then the anti-join — and the output schema is identical, which is
what the equivalence argument needs. Three differences are not just provenance:

- **Over-declaration.** `filterWithNotJoinClause` rejects a header symbol the body
  neither produces nor consumes. Bare `not` has no header, so nothing to reject.
- **Classification.** not-join sorts each header symbol into subject-carried
  (becomes the key), environment-bound (deliberately kept *out* of the key, since
  its contribution would be a constant equal on both sides), or neither (error).
  Bare `not` intersects with the input's symbols and errors only if the
  intersection is empty.
- **The body binding's input differs.** Bare `not` passes `joinVars` to
  `notBodyBinding`; not-join passes `query.FreeVariables(clause.Clauses)`.

### 6.2. Why or → or-join does NOT work

`OrClause` and `OrJoinClause` go through DIFFERENT executor paths:

**OrClause path** (`executeOrClause`):
```
branchesNeedCorrelatedExecution?
  → yes: executeOrClauseCorrelatedUnion
       → query.ScopeOf(clause)  // Provides ∪ Correlates
       → findOuterRelation(neededSymbols, groups)
       → NewOrFallbackRelation(branches, outerRel, false)
  → no:  executeOrClauseUnion (independent branch execution)
```

**OrJoinClause path** (`executeOrJoinClause`):
```
branchesNeedCorrelatedExecution?
  → yes: executeOrJoinClauseCorrelatedUnion
       → findOuterRelationBySymbols(joinVarSet, groups)
       → NewOrFallbackRelation(branches, outerRel, false)
       → rel.joinSyms = clause.JoinVars
  → no:  independent branch execution with joinVars projection
```

Key differences:
1. **Outer relation selection**: `findOuterRelation` vs `findOuterRelationBySymbols`
   use different strategies to find the outer relation from groups. If groups aren't
   fully collapsed, they may find different relations.

2. **Output symbol computation**: `OrJoinClause` sets `rel.joinSyms` which constrains
   the output symbols. `OrClause` doesn't.

3. **Uncorrelated path**: `OrJoinClause` projects branch results to join vars.
   `OrClause` doesn't project.

These differences mean `OrClause → OrJoinClause` is NOT a semantics-preserving
transformation at the executor level, unlike `NotClause → NotJoinClause` which is.

### 6.3. What would be needed

To make `or` → `or-join` safe, the executor would need:
- `executeOrJoinClause` to behave identically to `executeOrClause` when join vars
  are present — just using them instead of inferring
- Same outer relation selection, same output schema, same projection

This is a significant executor refactor. The two paths diverged over time and now
have incompatible behavior.

## 7. The 16-Row Discrepancy (Historical)

Early in the investigation, the complex query produced 16 rows. We attributed this
to a "get-else Cartesian product" in the algebra bridge. This was partially correct
but the analysis was muddled.

**What actually happened**: the 16-row result occurred because the algebra bridge's
`compileOrUnion` compiled branches independently without join var propagation. When
decompiled back to `OrClause`, the executor processed the OR branches as independent
unions with no join context. The lack of join vars meant the collapse couldn't properly
merge the OR results with the main relation, producing extra rows.

After adding `collectBranchSymbols` and `inferredJoinVars` to `compileOrUnion`, the
decompiled `OrJoinClause` had proper join context, and the executor produced 9 rows
(matching the base executor). But this conversion also broke the correlated union tests
because the `OrJoinClause` executor path behaves differently.

**Current state**: with the join var inference in `compileOrUnion`, the algebra bridge
produces 9 rows for the `or` tests (correct) but breaks 2 other tests. We cannot ship
this change as-is.

## 8. The Fix (Implemented)

### 8.1. Decorrelation guard

The decorrelation transform in `rewrite_decorrelate.go` now checks
`ctx.Parent.Rule == RuleUnion` before decorrelating. LateralJoins inside
Union nodes are skipped — their per-tuple correlation semantics are
load-bearing for correct union evaluation.

### 8.2. Correlation vars in LateralJoin output

`compileSubquery` in `algebra/compile.go` now always includes correlation
variables in the LateralJoin output. When compiled inside a Union branch
(`current=nil`), the output was previously just binding symbols — the
correlation variable wasn't included. This meant the Union's output had
no overlap with the outer relation for joining.

### 8.3. NOT → NOT-JOIN

`compileNot` sets `ExplicitJoin: true`. The decompiler emits `NotJoinClause`.
Already committed.

### 8.4. Executor simplification

`OrJoinClause` delegates to `executeOrClause` — the executor has one code
path for union OR. Join vars are planner scheduling metadata only.

`OrJoinClause` symbol extraction delegates to `extractOrClauseSymbols` —
same principle.

### 8.5. Downstream code fix

The production code should change `qb.Or()` to `qb.OrDefault()` for
subquery + ground default patterns. Union and fallback are different operations.

## 9. Verification

Full test suite passes (`go test -count=2 ./datalog/...`). Key tests:

- `TestNotClauseWithUnboundInnerVar_E2E` — NOT scheduling fix
- `TestNotClauseComplexQuery_E2E` — full production query structure
- `TestGetElseComplex_OrSemantics` — algebra bridge matches base executor
- `TestGetElseComplex_ParsedOr` / `QBOr` — correct union semantics (9 rows)
- `TestGetElseComplex_ParsedOrDefault` / `QBOrDefault` — correct fallback (2 rows)
- `TestOrCorrelatedUnionPartialOuterRelation` — pre-existing OR test, no regression
- `TestOrCorrelatedUnionWithNestedOrExpression_E2E` — pre-existing OR test, no regression
- `TestRoundTrip_AntiJoin` — NOT → NOT-JOIN round-trip
- `TestRoundTrip_OrUnion` — OR union round-trip
