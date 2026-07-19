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

## Fix sketch (needs owner sign-off before implementation)

- (1) Restrict or-join and or-default-join Provides to `JoinVars ∩ (branch intersection)`; everything else is scoped away.
- (2) NOT.Requires must be the body's *correlation* variables, not all body variables. Shared-with-outer needs query context the per-clause extractor lacks — either thread the enclosing clause list through, or reuse the algebra bridge's join-var inference on the non-bridge path.
- (3) `collectInnerVars` gathers the declared JoinVars from explicit-join forms while continuing not to descend their scoped bodies.

Each layer test-first; the reproducer above goes green last.
