# or-default-join's header is not its complete scope interface

**Status**: Open — ratified follow-up work (2026-07-19). Scheduled after the NOT scoped-body fix lands (see `BUG_NOT_CLAUSE_SCOPED_BODY_SYMBOLS_UNPLANNABLE.md`, steps A-C).

## Current contract (what the code does today)

`(or-default-join [?e] branch1 branch2)` treats its header as the **per-tuple correlation keys only**, not the complete interface:

- **Branch outputs escape unrestricted** — Provides is the *union* of branch provides, whether or not a variable appears in the header.
- The **algebra emitter builds on the leak**: fallback decorrelation emits `(or-default-join [?scenario] [(q ...) [[?scenario ?count ?total] ...]] [(ground [0 0]) [[?count ?total]]])` — the outputs `?count ?total` are bound outside the header by construction.
- The **executor groups fallback decisions by JoinVars** directly.

`query.ScopeOf(*OrDefaultJoinClause)` implements this contract (see the case comment in `datalog/query/clause_scope.go`), pinned by the "or-default-join branch outputs escape past the header" subtest in `clause_scope_test.go`.

## Why it is a defect

1. **Implicit interface.** The form's exposed variables cannot be read from its syntax; they must be computed from branch bodies. This is the same modeling error the NOT scoped-body fix removed for or-join, where the unrestricted branch math leaked branch locals into Provides.
2. **Schema hole.** Union-provides marks a variable "provided" even when only one branch binds it; when the other branch fires, the variable is absent from the produced tuples. Intersection is the sound combine for a form where any single branch may execute.
3. **Non-uniform scoping.** Every other explicit-join form (or-join, not-join) has header-as-complete-interface. One exception form makes `ScopeOf` non-canonical: consumers must know which headers tell the truth.

## Ratified fix (Option 2 from the step-B review)

Header-as-complete-interface, uniformly. Datomic's own or-join required-vars syntax (`(or-join [[?required] ?output] ...)`) confirms the header is meant to carry the full interface, inputs and outputs both.

1. `datalog/query/clause_scope.go`: the `*OrDefaultJoinClause` arm becomes `headerScope(c.JoinVars, c.Branches, intersectSymbolSets)` with `CorrelatesOptional: true`. `headerScope` then classifies header variables bound by every branch as Provides and header variables not bound by all branches as Correlates — exactly the input/output split. Truth-table test updated.
2. **Algebra emitter**: emit complete headers — `(or-default-join [?scenario ?taskCount ?totalTokens ?totalDuration] ...)`.
3. **Executor**: fallback grouping keys become header ∩ bound-at-entry (the input relation's schema) instead of all JoinVars — the same bound-at-entry resolution `filterWithNotClause` performs with `actualJoinVars`. Header variables not bound at entry are outputs and unify outward.
4. Hand-written or-default-join queries in tests whose outputs relied on the leak get their headers completed.
5. Reference docs state the contract for the extension form.

### Sub-decisions

- **(a) Input/output distinction** — **Ratified (2026-07-19)**: **Datomic's explicit required-vars header syntax** (`(or-default-join [[?required] ?output] ...)`), not bound-at-entry runtime resolution. Rationale: or-default is **non-monotone** — the correlation keys quantify the per-group fallback decision, so changing which variables are bound at entry changes results, not just performance. Bound-at-entry makes the clause's meaning depend on scheduling; a non-monotone form's quantification variables belong in its syntax. Consequences if ratified: declared required vars are mandatory correlates (`CorrelatesOptional` becomes false for this form — global fallback is declared by an empty required set, not inferred from unbindability); every branch must bind every output var (validation error, closing the schema hole statically); migration is loud (old correlation-key headers fail the every-branch-binds-outputs validation rather than silently changing meaning); or-join may accept the same syntax for Datomic compatibility, but its flat form stays valid because union is monotone and inference there is sound. Plain `or-default`'s first-pattern-entity correlation quirk is the same inference-of-semantics disease — whether it is retired (per-group fallback requires the `-join` form; plain `or-default` is the global form) rides this ruling.
- **(b) Plain `or-default` union-provides schema hole** — **Resolved (2026-07-19)**: `ScopeOf(*OrDefaultClause)` provides the **intersection** of branch provides, aligning the planner with the executor, which already intersects (`computeOrBranchOutputSymbols`; the relation schema is outer ∪ branch intersection — a union-only symbol never appears in the emitted relation, so union-provides was a planner-side lie). Pinned by the "or-default provides the intersection of branch provides" subtest in `clause_scope_test.go`. Note: or-default-join retains union-provides under its live contract — the same hole, masked for the emitter shape because the union-only symbol is exactly the outer-bound correlation key; it closes with the main fix here.

## Why it was not fixed in the step-B checkpoint

Step B migrates the planner onto canonical `ScopeOf` **semantics-preserving**: every form keeps its live language contract so the migration and the language change land as separate commits with separate tests. Restricting Provides to the header during step B broke every fallback-decorrelation consumer (storage algebra-integration, get-else, qb integration) because the emitter deliberately binds outputs outside the header — the correct contract change requires the emitter and executor moves above, not just the scope arm.
