# BUG: missing? on a matcher without entity lookup returns silent empty instead of a loud error

**Date**: 2026-07-20 **Severity**: Medium — silent wrong answer (worst class), but only reachable on matchers that implement no `LookupAttribute`; every production storage matcher implements it **Status**: Open — committed-red reproducer in the tree by owner ruling **Affected**: database-function predicates (`missing?`, and presumably `get-else`/`get-some` in predicate-adjacent positions) executed against a `PatternMatcher` that does not implement `EntityLookupMatcher`

## Symptom

A query with a leading input-bound `missing?` on `executor.MemoryPatternMatcher` completes with `err=nil` and zero rows, on **both** optimizer modes:

```clojure
[:find ?name
 :in $ ?goal
 :where [(missing? $ ?goal :goal/flag)]
        [?goal :entity/name ?name]]
```

with `?goal` bound to an entity that has a `:entity/name` and no `:goal/flag`. The correct row exists (the same query returns it on BadgerMatcher — `datalog/storage` `TestLeadingMissingWithInBoundEntity`, green both modes). On the lookup-less matcher the result is empty with no error: a silent wrong answer.

## Mechanism (established 2026-07-20 by instrumenting the reproducer with annotations)

The reproducer exposed **two distinct defects**, selected by whether an annotation handler is attached — attaching one changes the query's result (`rows=0` bare vs `rows=1` annotated), because the collector wraps the matcher and the wrapper changes capability dispatch.

**Path A — bare matcher (observed `rows=0, err=nil`): the loud error fires and is laundered twice.**

1. The phase's first clause is the predicate; groups = the `:in`-bound input relation `{?goal}`.
2. `executePredicate` (`query_executor.go:987-996`): the bare `IndexedMemoryMatcher` is not an `EntityLookupMatcher`, so `lookup` is nil, and `filterWithPredicateAndLookup` runs `pred.Eval` per tuple — which **errors loudly** ("database function predicate requires database access").
3. `filterWithPredicateAndLookup` (`relation_ops.go`) **defers** that error onto its empty result relation (`m.err = iterErr`) per the deferred-error convention. Nothing returns an error up.
4. The pattern clause hands that errored-empty relation to `IndexedMemoryMatcher.Match` as a binding: line 204's `bindingRel.Size() == 0` branch treats it as "no relevant bindings," **falls back to an unbound index scan, and never reads the relation's deferred error**.
5. Collapse hash-joins the unbound scan against the errored-empty group → 0 rows; the join's empty build side never surfaces the deferred error either.
6. API result: 0 rows, err=nil. The mandated loud error existed at step 2 and was dropped by two separate consumers of the errored relation.

**Path B — annotation handler attached (observed `rows=1, err=nil`): the wrapper lies about lookup capability.**

1. A collector wraps the matcher (`WrapMatcher` → `AnnotatedMatcher`), whose method set unconditionally satisfies `EntityLookupMatcher`.
2. `executePredicate`'s `e.matcher.(EntityLookupMatcher)` now succeeds; `missing?` evaluates via `EvalWithLookup`.
3. `AnnotatedMatcher.LookupAttribute` (`annotated_matcher.go:210`): when the underlying matcher is not an `EntityLookupMatcher`, it returns `(nil, false, nil)` — *attribute absent, no error*. Every lookup on a lookup-less base silently answers "absent," so **`missing?` is true for every entity**, including ones that have the attribute.
4. The reproducer's row survives (accidentally correct for the flag-less entity; an entity *with* the flag would also survive — a wrong row). rows=1, err=nil.

Path B also breaks the observability invariant on its own: attaching an annotation handler must never change query results.

Adjacent sites in the same family, noted but not exercised by this reproducer: `executePredicate`'s `len(relevantRels) == 0` skip (`query_executor.go:982-985`) silently declines to apply a predicate with no annotation event; `boundDatomIterator.Error()` (`indexed_memory_matcher.go:89`) returns nil unconditionally.

## Reproducer (committed red by owner ruling)

- `datalog/executor/not_or_test.go` / `TestMissingOnLookupLessMatcherFailsLoudly` — asserts `err != nil` on both modes and logs the execution event stream on failure; currently both legs fail (annotated context: `rows=1`). Running it with `NewContext(nil)` instead exhibits Path A (`rows=0`).

The capable-matcher contrast pin is `datalog/storage/database_function_integration_test.go` / `TestLeadingMissingWithInBoundEntity` (green both modes — `BadgerMatcher` implements the lookup, so the wrapper's delegation is honest there).

## Fix direction (owner ruling pending)

Both paths need the failure made loud at the point the capability is discovered missing, and the laundering consumers fixed regardless:

- `AnnotatedMatcher` (and any capability-forwarding wrapper) must not satisfy `EntityLookupMatcher` when its underlying matcher does not — capability interfaces must be conditional on the wrapped matcher (the same class of question as the `SourceRouter`'s delegation), or the fallback must return an error, never `(nil, false, nil)`.
- The deferred-error convention's consumers must honor it: `IndexedMemoryMatcher.Match`'s empty-binding fallback and the collapse/join empty-side paths must surface a binding relation's deferred error instead of proceeding.

## Discovery context

Found 2026-07-20 while pinning the external review's algebra clause-ordering finding: the `missing?` leg of the new in-bound-correlate pin failed on baseline where NOT/not-join legs passed. Initially mis-triaged as fixture incapability and the reproducer briefly deleted; the owner caught the retraction — the incapability explains why the query cannot succeed, not why it failed *silently* — and ruled the test restored red. See `feedback_evidence_never_downgraded` in the project memory for the process lesson.
