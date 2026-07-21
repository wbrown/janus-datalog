# BUG: subquery binding arity is validated at different layers with different messages per optimizer mode

**Status**: RESOLVED (2026-07-20). The ratified boundary-validation design landed: `SubqueryPattern.Validate()` (`datalog/query/clause_scope.go`) owns the static binding-arity rules per binding form (scalar/collection ⇒ inner find arity 1; tuple/relation ⇒ binding arity ≡ inner find arity), recursing into nested inner queries, and `query.ValidateStaticClauseShapes` is the one walk all three user boundaries share — `parser.ParseQuery`, `qb.(*QueryBuilder).Build`, and `executor.ExecuteWithRelations` (before planning, so both modes agree). The general principle is now in force: static properties of the query text are validated at the shared user boundary; mode machinery is never where a static defect is first detected. The execution paths' deeper checks remain as backstops for internally constructed plans. Guards: `TestSubqueryErrorHandling/RelationBindingSymbolMismatch` (canonical message at parse), `TestExecutorEntryRejectsStaticallyInvalidClauses` (hand-built ASTs, both modes). Full native sweep green outside the known decorrelation guards.

Original entry follows.

**Status (original)**: Open (2026-07-20). Found by the optimizer mode matrix migration of `datalog/executor` (phase 5). Both modes correctly reject the malformed query — the matrix invariant (same error/no-error outcome) holds — but each mode detects the same static defect in a different layer with different vocabulary. Fix direction discussed with the owner; ratification pending.

## Symptom

`TestSubqueryErrorHandling/RelationBindingSymbolMismatch` (`datalog/executor/executor_subquery_comprehensive_test.go`): a subquery whose `:find` produces one column bound with a three-variable relation binding — `[(q [:find ?salary ...] $ ?e) [[?a ?b ?c] ...]]`. A malformed query, statically: both arities are literals in the same clause.

- `algebra_off` rejects at execution: `relation binding expects 3 symbols, got 1` (`applyExactlyOneBinding`/`applyBindingForm`, `datalog/executor/subquery.go`)
- `algebra_on` rejects at planning: `algebra compile ... LateralJoin: lateral binding arity 3 does not match inner find arity 1` (schema refresh in the algebra optimizer)

The test pins the baseline's message, so its `algebra_on` leg stands red in-tree as this entry's regression guard. A mode-conditional message assertion (`wantErrAlgebra`) was briefly added and then reverted — it made the leg green by tolerating the divergence rather than recording it, which is backwards.

## Reading

Binding arity is a static property of the query text: the inner query's find arity and the binding form's arity are both present in the clause, in EDN text and in qb-built ASTs alike. Neither data nor planning nor mode is needed to check them. Today the check lives only in mode-dependent machinery — the baseline's binding application at execution time, the bridge's IR schema refresh at plan time — so the two modes disagree on when and how the same defect surfaces. This is the same class as `BUG_NOTJOIN_HEADER_VALIDATION_ONLY_ON_ALGEBRA_PATH.md`: a static clause property enforced somewhere other than the shared user boundary.

## Fix direction (proposed 2026-07-20, owner ratification pending)

The branch's own ratified convention for clause-structure rules is the or-default-join treatment: a `Validate()` on the clause type enforcing its structural rules, called at the three user boundaries — parser, qb builder, executor query entry — so a malformed clause dies at the door with one message in both modes, before any mode-dependent machinery runs. Applied here: the subquery clause's `Validate()` owns the binding-form arity rules (`ScalarBinding`/`CollectionBinding` ⇒ inner find arity 1; `TupleBinding`/`RelationBinding` ⇒ binding arity ≡ inner find arity). The existing late checks remain as backstops for internally constructed plans; they stop being the user-facing contract. The general principle, if ratified, also covers the not-join header hoist: static properties of the query are validated at the user boundary, shared by both modes; mode machinery must never be where a static defect is first detected.

## Protocol

When the boundary validation lands, the red `algebra_on` leg goes green and the test tightens to pin the one canonical boundary message on both legs. No assertions were weakened; the query is unchanged from its pre-migration form.
