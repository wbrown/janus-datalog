# BUG: NOT bodies with scoped or existential variables cannot plan (non-bridge path)

**Status**: Open (2026-07-19). Reproducer `TestNotClauseWithOrJoinBody` in `datalog/executor/not_or_test.go` is committed **red** by owner ruling: a known bug's reproducer belongs in the suite, documenting the defect loudly, not quarantined to keep the gate green. The gate is red until this is fixed; that is the honest state.

## Symptom

```clojure
[:find ?v
 :where [?e :item/val ?v]
        (not (or-join [?v] [?d :seen/val ?v]))]
```

fails on the clause-based planner path:

```
query planning failed: phasing failed: cannot create phase: 1 clauses
remaining but none can execute with available symbols
```

A legitimate Datomic shape — NOT over a scoped union — cannot run at all.

## Root cause — three composing defects

1. **`planner/clause_utils.go` `extractOrJoinClauseSymbols`**: Provides is the intersection of branch provides, **not restricted to the declared JoinVars**, contradicting its own doc comment ("OR-JOIN provides exactly the JoinVars"). Branch-local variables (`?d`) leak out of the or-join's scope. Same shape in `extractOrDefaultJoinClauseSymbols`.
2. **`planner/clause_utils.go` `extractNotClauseSymbols`**: NOT's Requires is the union of every inner clause's Requires **and Provides**. Every body-local (existential) variable becomes a scheduling requirement nothing can ever provide. Composed with (1), the leaked `?d` lands in NOT.Requires → unplannable.
3. **`executor/relation_ops.go` `collectInnerVars`** (feeds `executeNotClause`'s anti-join keys): never collects the JoinVars an or-join exposes and never descends explicit-join bodies, so once planning is fixed, `(not (or-join …))` either errors ("NOT clause has no variables to join on") or under-correlates — the inner or-join executes without the outer `?v` binding and matches too much, filtering too many outer rows.

## Breadth

Defect (2) alone implies plain `(not [?d :seen/val ?v])` — existential `?d` — is unplannable on the same path: the pattern's Provides `{?d ?v}` becomes NOT.Requires, and `?d` is never available. Not yet reproduced separately; the existing NOT tests pass only because every body variable in them is outer-provided.

The algebra bridge normalizes NOT → NOT-JOIN with explicit join vars before this code runs, which is why the bridge-enabled path works. The non-bridge path is a live, supported configuration and must be correct independently (owner ruling, 2026-07-19: states the algebra optimizer normalizes away are not unreachable).

## Fix plan (owner-ratified 2026-07-19: the most correct fix, cost not a criterion)

### The modeling error

Every clause form has a well-defined **scope interface** — the variables it exposes to its enclosing scope:

| Form | Interface |
|---|---|
| data pattern | all its variables |
| `not-join` / `or-join` / `or-default-join` | exactly the declared header (language contract: the header lists every outer-facing variable; body variables are existentially quantified inside) |
| plain `not` / `or` | the body's/branches' free variables — Datomic's rule: all of them that the enclosing query binds unify; the rest are existential |
| `SubqueryPattern` | variable inputs (consumed) + `Binding.BoundVariables()` (provided); the inner `:where` is a different scope |
| predicates | `RequiredSymbols()` (consumed), nothing provided |

`ClauseSymbols{Requires, Provides}` tries to make scheduling a clause-local property. But a plain NOT's scheduling constraint is a property of the clause **in its query**: it must run after every free variable *the rest of the query can bind* is bound, and "can bind" is unknowable clause-locally. The code compensated by over-requiring (all body variables, provides included), turning existentials into unsatisfiable preconditions — the planning failure. `extractOrJoinClauseSymbols` compounds it by leaking branch-locals through Provides, violating the header contract its own comment states.

Scoping knowledge currently has **four implementations** that can (and here, do) disagree: the algebra bridge's join-var inference, the planner `extractClauseSymbols` family, the executor's `collectInnerVars`, and the parser's `ExtractVariables`.

### The plan

1. **`query.FreeVariables(clause)` — one canonical scoping definition**, in the package where the clause types live, per the table above, computed recursively through itself for compound bodies, over the closed taxonomy with a loud default. Scope-locals cannot leak because the definition never surfaces them. The four existing copies become consumers of this one definition — the `WalkClauses` consolidation applied to semantics instead of traversal.
2. **`ClauseSymbols` reshaped to tell the truth**: `Provides` (binds into the enclosing scope) and `Correlates` (free variables that unify with the enclosing scope when bound there) — replacing the lie that every free variable is a hard precondition. NOT: provides nothing, correlates on `FreeVariables(body)`. Or-join: provides its header ∩ what all branches produce; correlates on the remainder plus branch externals.
3. **Scheduling resolves `Correlates` against the query.** At planning entry compute once: `bindable = inputs ∪ ⋃ Provides(all clauses)`. A filtering clause is ready when `Correlates ∩ bindable ⊆ available`. Existentials drop out arithmetically — no special cases — while correctness holds exactly: the clause waits for every variable the query *can* bind, which is Datomic's unification rule expressed as a scheduling condition.
4. **The executor uses the same definition.** `executeNotClause`'s anti-join keys become `FreeVariables(body) ∩ input schema`; `collectInnerVars` becomes that call or is deleted. Planner and executor can no longer drift — there is nothing left to drift between.

### Rejected alternative: reuse the algebra bridge's inference

The bridge's inference is the fourth copy — the least-wrong one. Wiring the planner to it makes the non-bridge path depend on the optimizer layer (inverted layering) and leaves the executor and parser copies alive to disagree later. Under this plan the bridge itself becomes a consumer of `FreeVariables`, and the non-bridge path is correct independently (owner ruling 2026-07-19: states the optimizer normalizes away are not unreachable).

### What this subsumes

All three layers of this bug; audit finding D8 (the dispatch's silent default and missing predicate arms collapse into one `Correlates = RequiredSymbols()` predicate arm with a loud default); and the future class where a new clause form gets scoping wrong in one consumer — a new form must define `FreeVariables` once or panic everywhere.

### Protocol

Test-first per step: unit tests pinning `FreeVariables` per form (including nested compounds and subquery scope opacity); structure tests for the `Correlates` scheduling rule; phase-contract validator and explain analysis updated with the reshaping. The two committed reproducers — `TestNotClauseWithOrJoinBody` (red in-tree) and the existential-pattern sibling described under Breadth — go green last, and the gate goes green with them.

### Cost (stated, not a criterion)

`ClauseSymbols` consumers all move: phasing, phase-contract validation, explain analysis, plus the four scoping-knowledge sites. Largest planner change since the 2026-07 audit began.
