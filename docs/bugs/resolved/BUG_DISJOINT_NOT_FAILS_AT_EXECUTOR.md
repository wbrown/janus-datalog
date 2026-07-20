# BUG: NOT with a fully-disjoint body fails at the executor, not the planner

**Status**: Resolved (2026-07-19) — owner ruling: **loud planner rejection**, no semantic extension, no new syntax. Reported by external review against `4a43b5d` with a proposed global-anti-join reading; the ruling rejects that reading (below). Pins: `TestFullyDisjointNotRejectedAtPlanning` (`datalog/planner/not_scheduling_test.go`), `TestFullyDisjointNotRejectedAtQueryOutset` (`datalog/storage/correlated_not_join_test.go`, both optimizer paths), `TestParseNotJoinEmptyHeaderRejected` (`datalog/parser/not_or_test.go`).

## Symptom (original)

A `(not [?x :sys/active true])` whose body shares **no** variable with the enclosing query was admitted by the planner and then errored in the executor:

- `ScopeOf(*NotClause)` marks the body's free variables `CorrelatesOptional`; with none of them bindable, `clauseReady` skips them all and schedules the NOT immediately.
- `filterWithNotClause` intersects `FreeVariables(body)` with the input schema, gets zero join vars, and returns "NOT clause variables not found in input relation" — loud, but late, deep in phase execution, and without stating the rule.

On the algebra path the same shape compiled to an anti-join with zero join symbols, which decompiles to a not-join with an **empty header** — a form the parser rejects (`parseJoinVars`: "join vars cannot be empty"), so the bridge was emitting a clause the language cannot express.

Before the NOT scoped-body fix (`d235b76`..`1cb3425`) this shape failed at planning (every body variable was a hard scheduling requirement). The fix relocated the failure to execution without deciding the shape's semantics. Both before and after, the query never returned wrong data.

## The ruling

**Reject at planning.** The reviewer's global-anti-join reading (zero shared variables → "exclude every row iff the body has any solution", NOT EXISTS) is the mathematically continuous extension of the anti-join rule — an anti-join on zero key columns — but it is rejected as language semantics:

1. NOT's anti-join keys on plain `not` are **inferred** from variable overlap. Zero keys flips the clause's quantification from per-tuple to global — so under the global reading, an unbound or mistyped variable in an ordinary correlated NOT silently rewrites the query's quantifier and wipes (or fails to wipe) the entire result. Quantification that changes results must never be decided by inference; this is the same rule that made or-default's correlation keys declared syntax.
2. Nothing is lost: the global existence gate is already expressible in-language as a pure aggregate subquery plus a predicate — `[(q [:find (count ?x) :where ...]) [[?n]]] [(= ?n 0)]` — the established idiom for existence- and latest-wins-style derivations.
3. Datomic parity: Datomic rejects `not` clauses that do not unify with the enclosing query (insufficient binding).

## The fix

Three boundaries, one rule — a NOT body must unify with the enclosing query through at least one variable the query can bind:

- **Planner** (`createPhasesGreedy`, `datalog/planner/clause_phasing.go`): before phasing, a `NotClause` none of whose free variables is an input or provided by any clause is rejected, naming the clause and stating the unification rule. Runs on both optimizer paths (the algebra bridge's decompiled clauses re-enter the same phasing).
- **Algebra bridge** (`compileNot`, `datalog/algebra/compile.go`): zero computed join symbols is an error instead of an `AntiJoin` with no keys — the bridge must not emit a clause (empty-header not-join) the language cannot express.
- **Parser**: already rejected empty not-join headers via `parseJoinVars`; now pinned as part of this ruling.

The executor's zero-join-vars error remains as defense in depth for programmatically constructed plans.
