# BUG: not-join header-completeness validation exists only on the algebra path

**Status**: RESOLVED (2026-07-20). Hoisted to the shared user boundary rather than the planner specifically: `NotJoinClause.Validate()` (`datalog/query/clause_scope.go`) enforces header completeness — every variable the body consumes without binding, including predicate-only inputs, must be declared — using the same body-provided subtraction as `OrDefaultJoinClause.Validate`. Enforced via `query.ValidateStaticClauseShapes` at `parser.ParseQuery`, `qb.Build`, and `executor.ExecuteWithRelations` (pre-planning), so both planner modes reject identically with one message; the algebra bridge's own analysis remains as a backstop. The mode-conditional pin in `datalog/storage/correlated_not_join_test.go` is tightened to both modes, and `TestExecutorEntryRejectsStaticallyInvalidClauses` covers hand-built ASTs. The valid predicate-input shape (body-bound variables consumed by predicates) stays accepted — pinned by `TestCorrelatedNotJoinPredicateInputMatchesUnoptimizedExecution`.

Original entry follows.

**Status (original)**: Open (2026-07-19). Found by the optimizer mode matrix migration's first fixture conversion (`docs/wip/OPTIMIZER_MODE_MATRIX.md`) — the exact divergence class the regime exists to catch. No wrong data on either path; the divergence is where and how clearly the language rule is enforced.

## Symptom

A not-join whose body consumes an outer binding through a predicate without declaring it in the header:

```clojure
[:find ?goal
 :where [?goal :entity/type :type/goal]
        [?setEvent :event/goal ?goal]
        [?setEvent :event/type ?goalSet]
        (not-join [?goal]
          [?termEvent :event/goal ?goal]
          [?termEvent :event/type ?termType]
          [(!= ?termType ?goalSet)])]
```

- **Optimizer on**: rejected at compile with the language rule stated — "not-join header must declare outer requirement ?goalSet used by the body" (`compileNotJoin`, `datalog/algebra/compile.go`).
- **Optimizer off**: the query plans, executes, and fails deep in the executor with "NOT-JOIN inner clause execution failed: cannot resolve right term ?goalSet".

The header-as-complete-interface rule is a language rule from the NOT scoped-body work — `ScopeOf(*NotJoinClause)` declares the header as the entire interface — but its *enforcement* (body externals ⊆ header) was implemented only in the algebra bridge. The baseline planner schedules the clause off the header alone and never checks that the body's consumption is covered.

## The shape of the fix

Same as the disjoint-NOT resolution: language rules are enforced at planning, path-independently. The check — every free variable the not-join body consumes without binding must appear in the header — derives from `ScopeOf`/`branchInterface`-style analysis of the body and belongs where the disjoint-NOT rejection lives (`createPhasesGreedy` or a shared clause-validation pass), so both execution paths inherit it. The algebra bridge's copy then becomes redundant with the planner-level check upstream of it.

Pending an owner go-ahead; not started. Until then, `TestCorrelatedNotJoinRequiresOuterInputsInHeader` runs both modes, requires the error on both, and pins the header-rule message only on the algebra leg, citing this doc.
