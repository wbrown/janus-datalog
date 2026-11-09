# Join-project insertion

**Status:** Removed 2026-11-08. This document is the record of the idea, what
was built for it, what it measured, and why it came out.

## The idea

The relational "project early" rewrite. When the two sides of an inner join
carry symbols nothing above the join reads, narrow each side to the symbols
still live, plus the join keys, before joining. The join then carries smaller
tuples, and a side whose dead symbols fan out collapses to its distinct live
tuples first, since a projection is a set.

The flagship shape:

```clojure
[:find ?entity ?payload
 :where [?entity :item/score ?score]
        [(> ?score 90)]
        [?entity :item/payload ?payload]]
```

`?score` is dead above the join of the first two clauses with the third. The
rewrite projects that side onto `?entity`, and the bridge lowers the `Project`
back into Datalog as an uncorrelated subquery:

```clojure
[(q [:find ?entity :where [?entity :item/score ?score] [(> ?score 90)]] $) [[?entity]]]
[?entity :item/payload ?payload]
```

## What was built

- `algebra.InsertJoinProjects(root, terminal)` walked the optimized algebra
  tree with the set of symbols live above each node, the find and sort
  symbols at the root, a `Select`'s or `Map`'s inputs added on the way down.
  At each inner `Join` it wrapped a child in a `Project` onto
  `live ∪ joinSymbols` when that dropped a symbol. Compound operators were
  left alone. A child whose projection would be the join's own output beside
  a partner carrying nothing beyond the join symbols was left alone too,
  since its subquery planned again is the join again.
- `decompileProject` lowered a `Project` over clauses as
  `[(q [:find <kept> :in $ … :where <child>] $ …) [[<kept>]]]`, declaring `$`
  and, when the child used rules, `%`.
- `PlannerOptions.EnableJoinProjectInsertion`, default off, ran the pass in
  `optimizeViaAlgebra` after the equivalence passes, and the plan cache keyed
  on it.
- Two guards kept it off where its lowering was known to lose a binding: a
  query with a value input, and a scope planned under symbols bound before
  it.
- Provenance: a `RewriteRecord` under the pass name and the events
  `algebra/join-project-{apply,skip}`.

## What it measured

On its own benchmark, 2,000 entities, scan → selective predicate → project →
join, algebra optimization on in both legs (`PERFORMANCE_STATUS.md`,
"Materialized join-project experiment", 2026-07-12):

| Metric | Flat | Materialized project | Delta |
|--------|-----:|---------------------:|------:|
| Median time | 0.913 ms/op | 1.464 ms/op | +60.4% |
| Memory | 1.627 MiB/op | 2.614 MiB/op | +60.7% |
| Allocations | 20.38K/op | 36.64K/op | +79.8% |

The narrower tuple did not repay the nested subquery's execution, and the
option stayed off from the commit that introduced it.

## Why it was removed

A `Project` is the same tuples under fewer symbols. A subquery is a scope: its
own inputs, its own required run, its own evaluation. The lowering turned the
first into the second, and three things the enclosing scope owns do not cross
a scope boundary. The optimizer-mode matrix, which runs every engine test
under every option, found all three the first time the option ran outside its
own tests.

1. **A symbol a clause of the same scope binds.** A form's header requiring
   `?e` that the other join side binds has nothing binding `?e` inside the
   subquery: `cannot order clauses: (or-default-join [[?e]] …) waits on ?e (no
   input or clause binds it)`. The guards caught this dependency from an
   `:in` value and from a bound symbol, and missed it from a clause.
2. **The scope's required run.** A nested scope's query opens with
   `(scope/required R)`, the run the executor holds for it
   (`query.ScopeRequired`). Inside the subquery that clause names a run the
   new scope does not hold: `rule invocation (scope/required ?x): symbol ?x
   not found among the symbols []`, or `unknown rule scope/required` where the
   nested body met the magic rewrite.
3. **The failure contract.** An evaluation failure fails a query only at an
   assignment where every other clause holds
   (`docs/proposals/PULL_BASED_HASH_JOIN_PROOF.md` §1, T5, T10). The subquery
   evaluated the child's expressions over the child's own extent before the
   join decided which assignments exist, so `[(+ ?n 1) ?m]` failed beside an
   empty pattern: `+: operand Bob (string) is not a number`.

Threading bindings into the subquery, the completion the option's description
promised, would have answered the first alone. `scope/required` is not a rule
and cannot be passed as an input, and the third break is the evaluation
boundary itself. The one lowering that preserves all three is the one with no
boundary: the child's clauses flattened back where they stood, which is what
the bridge emits without a `Project`. Within Datalog in, Datalog out, a
projection over clauses has no sound lowering except erasure, so the
decompiler now refuses a `Project` over clauses and lowers only the childless
placeholder the compiler makes.

## Where the idea belongs

Narrowing a join side's tuples and deduplicating them is how a relation is
held, which is the executor's (`CLAUDE.md`, "The Plan States Meaning; the
Executor Carries It Out"). The executor projects at every phase boundary
through `Keep` (`computeKeepSymbols`), and the key-preserving join properties
(`PERFORMANCE_STATUS.md`, "Key-preserving join properties") let a join's
streaming projection omit deduplication where a candidate key survives. An
in-phase projection at a join, if it is ever wanted, is an executor mechanism
reading liveness from the plan, measured on the complex checkpoint, never a
planner rewrite that lowers to a scope.

## Record

- `6b5f245` (2026-07-13): the pass, the option, the value-input guard, the
  plan-cache key and the negative measurement, in one commit.
- `ab6d7b4` (2026-07-13): the direct `RealizedPlan` emitter removed, the
  bridge restored to Query → Query, and `Project` lowered as a
  relation-binding subquery.
- `d3de083` (2026-07-13): the fan-out deduplication differential.
- 2026-07-20: the first version of the clause-ordering fix broke the pass's
  selective-child lowering and was narrowed
  (`docs/bugs/resolved/BUG_ALGEBRA_ORDERING_IN_BOUND_CORRELATE_FOLDS_FIRST.md`).
- 2026-07-22: provenance records and events for the pass.
- `2916b5d` (2026-10-23): the bound-symbols guard in `planWithBindings`.
- 2026-11-08: the pairwise optimizer-mode matrix runs the option across the
  storage and executor suites for the first time, the three breaks above
  surface, and the option, the pass, the lowering, the guards and the
  provenance are removed.
