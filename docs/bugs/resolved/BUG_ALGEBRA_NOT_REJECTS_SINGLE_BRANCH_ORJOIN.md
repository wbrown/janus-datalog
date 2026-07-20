# BUG: algebra bridge rejects a single-branch or-join inside a NOT body — algebra path only

**Status**: RESOLVED (2026-07-20). Fixed by relaxing the union arity invariant to ≥1 at both IR sites (`analyzeUnionBranches` and the schema-refresh arms for Union and LateralUnion, `datalog/algebra/analysis.go`): a single-branch union carrying the declared header interface is the honest IR image of a legal single-branch or/or-join, and the identity-compile alternative was rejected because it would erase the clause from the IR and break the ratified or/or-join decompile round-trip. Clause-level branch minimums (or-default's two branches) remain language rules enforced by the clause's own `Validate` at the boundaries — not IR invariants. Guards: `TestNotClauseWithOrJoinBody` (the NOT-inner reproducer, both modes green) and the new `TestSingleBranchOrJoinExecutesAsItsBranch` (plain positional single-branch or and or-join, exact rows, both modes).

Original entry follows.

**Status (original)**: Open (2026-07-20). Found by the optimizer mode matrix migration of `datalog/executor` (phase 5), which put that package's query tests on the algebra path for the first time. Loud error at planning, no wrong data; the baseline path executes the same query correctly.

## Symptom

`TestNotClauseWithOrJoinBody` (`datalog/executor/not_or_test.go`): a `(not ...)` whose body is an or-join with **one** branch. With the algebra optimizer off, the test passes. With it on:

```
execution failed: query planning failed: algebra optimization failed: algebra compile: NOT inner analysis: Union: union requires at least two branches
```

Reproduction is deterministic: `go test ./datalog/executor -run 'TestNotClauseWithOrJoinBody'` (the `algebra_on` leg stands red in-tree as the regression guard).

## Reading

A single-branch or-join is a legal degenerate form — it is exactly its branch, executed under the header's declared interface — and the parser, the AST, and the baseline executor all accept it. The bridge's NOT-inner analysis compiles the or-join body to a Union node, and the Union's validation demands at least two branches, rejecting the degenerate form at compile time. This is the bridge-divergence class the plan of record names: the algebra IR must accept every shape the language accepts (the same principle, in the other direction, that ruled the correlated or-join round-trip fix — the IR must be neither larger nor smaller than the language). Candidate fix directions for the owner: compile a single-branch union as the branch itself (identity union), or relax the Union arity validation to ≥1; either must preserve the decompile round-trip.

Related family: `BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md`, `BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md` — algebra-path-only rejections of baseline-legal shapes.

## Protocol

Fix direction needs an owner ruling. When fixed, the red `algebra_on` leg of `TestNotClauseWithOrJoinBody` goes green as the regression guard; no assertions were weakened and the query is unchanged from its pre-migration form.
