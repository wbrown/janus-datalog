# BUG: A typo'd predicate-position function returns silent empty instead of erroring

**Status**: RESOLVED (2026-07-20). Found by external re-re-review at `d6d4721`. Pre-existing — the eval-only guard predates this branch; the C5 registry wiring closed expression position and left predicate position asymmetric.

## Symptom

`parsePredicate`'s default case built a `FunctionPredicate` for any unrecognized name with no registry check. The only guard was `FunctionPredicate.Eval`, which runs per candidate tuple — so when upstream clauses matched nothing, the guard never fired: `[(bogus/never-registered? ?price)]` after an empty or fully-pruned pattern returned `err=nil, n=0` on both modes, indistinguishable from a predicate that legitimately filtered everything. Expression position had no such hole: `parseFunction` validates names against the registry at parse time.

## Fix

The rule has one home: `FunctionPredicate.Validate()` (`datalog/query/predicate.go`) rejects a name with no registered implementation (`query.DefaultRegistry.RegisterImplementation`), enforced through `query.ValidateStaticClauseShapes` — the walk all three user boundaries share (parser `ParseQuery`, qb `Build`, executor `ExecuteWithRelations`), with subquery inner queries covered by `SubqueryPattern.Validate`'s recursion. `parsePredicate`'s default case constructs the `FunctionPredicate` unconditionally and lets the boundary walk reject it, so parsed, qb-built, and hand-built queries get the same message (`unknown predicate function: %s`). Built-in predicate forms (comparisons, `ground`, `missing?`, time extraction, `tx-between`, `str/starts-with?`) take their explicit arms and are unaffected. The per-tuple eval guard remains as the backstop — registration is runtime state, so a name unregistered between validation and evaluation still errors.

## Behavior changes

- A bare non-boolean function in predicate position — `[(* ?qty ?price)]` — is now a parse error. It never functioned (always the per-tuple eval error when reached); the parse-shape pin in `comparator_test.go` converted to an expected-error case in `TestComparatorErrors`.
- Names with registry *metadata* but no implementation (`same-date?`, `str/ends-with?`, `str/contains?`) now require a registered implementation before parsing. They never functioned either — no concrete type and no implementation existed — so the failure moved from per-tuple eval (or silent empty) to the boundary. Parse-structure tests using `same-date?` register a stub first.

## Scope note

Ruled and executed (owner, 2026-07-20): the check lives in `query.ValidateStaticClauseShapes`, closing all three doors in one home per the boundary-validation principle. The registration-is-runtime-state wrinkle is resolved as a boundary contract — "registered by the time the query enters the engine" — stated on `FunctionPredicate.Validate`, with the eval guard as the backstop for post-validation unregistration. The qb builder cannot currently construct a `FunctionPredicate` (`toClause` accepts only qb's own builder types), so its door is covered prospectively by the `Build`-time walk.

## Reproducers (red-first, now green)

- `datalog/parser/predicate_syntax_test.go` / `TestPredicatePositionUnknownFunctionRejectedAtParse` — unknown name rejected at the parse boundary; registered name parses.
- `datalog/parser/comparator_test.go` / `TestComparatorErrors/bare_arithmetic_in_predicate_position` — the converted coverage, asserting the loud message.
- `datalog/executor/static_clause_validation_test.go` / `TestExecutorEntryRejectsStaticallyInvalidClauses/unregistered_predicate_function` — the hand-built-AST door, both modes.
- `datalog/executor/custom_functions_test.go` / `TestCustomFunctionQueryEndToEnd` — registered predicate-position functions still execute end to end, both modes.
