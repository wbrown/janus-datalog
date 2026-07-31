# Correct-by-Construction Plans — the topology the planner computes, expressed in Datalog

**Status**: Discussion record, raised by the owner 2026-07-22; pending ratification and scheduling. The open questions are in *Open questions for ratification* below — this document owns them. The owner's position, verified below rather than assumed: the plan IR remains expressible as Datalog — no separate plan language. Companions: `PHASE_AS_QUERY_ARCHITECTURE.md` (the architecture this completes), `RELATION_ALGEBRA_REUNIFICATION.md` (the operator surface plan fragments consume), `docs/TALE_AND_LESSONS_OF_RELATIONS_AS_EXCHANGE.md` (why the exchange is relational).

## The observation

The 2026-07 correctness campaign's executor bugs clustered in one layer: the machinery that decides, at runtime, *which relations a clause operates on*. Mid-plan, the working state is a set of disjoint relation groups; the clause loop improvises the topology — per-clause subject discovery (`findOuterRelation`, the `relevantRels` partitions), runtime bridging (the theta-join for predicates spanning groups, the anti-join bridging for `not`), loud unbound checks, find-boundary absorption of residual groups, and the strict phase contracts that police it all after the fact. Every one of those exists because `RealizedPhase` specifies **symbol flow** (`Available`/`Provides`/`Keep`) but not **group topology**: phases receive the previous phase's output as one undifferentiated relation bundle, and the executor re-derives at runtime which pieces each clause consumes.

Meanwhile the algebra path *constructs* the topology explicitly — and then discards it at decompile, handing the clause-loop executor a flat clause list to re-derive greedily. The sound computation is the one thrown away.

## The allegation, and its retraction with evidence

When this was first raised, the recorded counter-position (Claude's) was that Datalog cannot express what the planner needs to hand the executor — that group topology is about relations, not symbols, and the query language speaks symbols. That claim confused a property of `RealizedPhase`-as-shaped with a property of the language. The language already has the construct that names a relation and hands it to a query: **the relation input**. The claim was then tested against the engine (2026-07-22, tree-built CLI, both planner modes), and every fragment shape the construction needs executes correctly today:

**Shared-symbol unification across inputs is the natural join** — two inputs binding the same variable intersect:

```
[:find ?x :in $ [?x ...] [?x ...] :where [(> ?x 0)]]
  inputs [1 2 3], [2 3 4]  →  {2, 3}
```

**Disjoint inputs bridged only by a spanning predicate execute as the theta-join** (both `-optimize` modes):

```
[:find ?x ?y :in $ [?x ...] [?y ...] :where [(< ?x ?y)]]
  inputs [1 2], [0 3]  →  {(1 3), (2 3)}
```

**A `not-join` spanning two disjoint inputs executes as the bridged anti-join** (the shape the `BUG_NOT_SPANNING_DISJOINT_GROUPS_APPLIED_PER_GROUP` fix made correct):

```
[:find ?e ?outer :in $ [?e ...] [?outer ...]
 :where (not-join [?e ?outer] [?x :entity/name ?e] [?x :entity/flag ?outer])]
  inputs ["a" "b"], ["hot"], empty store  →  {("a" "hot"), ("b" "hot")}
```

There is no expressiveness gap. (One incidental defect surfaced during verification, at the CLI layer, not the engine: `-in` rejects *relation*-shaped EDN inputs — `BUG_CLI_RELATION_INPUT_EDN_REJECTED`.)

## The construction

A plan is a **DAG of Datalog query fragments**. Each fragment's `:in` enumerates the specific prior outputs it consumes as relation inputs; its `:find` declares the group it produces; edges of the DAG are those named input/output bindings. Every piece of today's improvised topology becomes a declared property of a fragment:

| Improvised today (executor, runtime) | Declared under the construction (planner, plan time) |
|---|---|
| Subject discovery (`findOuterRelation`, `relevantRels`) | which groups appear in the fragment's `:in` list |
| Join of groups during Collapse | a fragment with two relation inputs sharing symbols — multi-input unification *is* the join |
| Bridging theta-join for a spanning predicate | disjoint relation inputs + the predicate clause |
| Anti-join bridging for spanning `not` | `not-join` with the full key header over the named inputs |
| Find-boundary absorption | the terminal fragment's `:find` over the groups the planner feeds it |
| Strict phase contracts (runtime validation) | `:in`/`:find` agreement across the DAG, validated once at plan construction |

The environment stays exactly as ruled — one single-tuple relation on the executor context, ambient in every fragment's scope, never re-declared per fragment. `BindQueryInputs` already performs the per-fragment input binding; the executor's remaining job per fragment is: bind declared inputs, execute the fragment, label the output. No topology improvisation remains for it to get wrong.

Phase-as-query was this architecture stopped one step short: the phases are queries, but their inputs are anonymous. The change is to name them.

## The granularity dial

Fragment size is the planner's dial, and it resolves the adaptivity cost flagged when item 25 was raised:

- **One algebraic step per fragment**: the executor is trivial and everything is static — but join order freezes at plan time, so dynamic Collapse's implicit adaptive join ordering is lost and the statistics roadmap becomes load-bearing.
- **Coarse fragments**: a multi-pattern fragment keeps greedy Collapse as a *fragment-internal* concern, preserving adaptive pattern-join micro-ordering where it pays.

The campaign's bugs lived entirely in the first category's territory — compound-clause subjects, bridging, boundary absorption. Those must be declared. Pattern-join micro-order must merely be fast, and can stay adaptive inside fragment walls. The construction therefore does not force the statistics question on day one: declare the topology at clause-scope boundaries; keep fragments coarse between them.

## What this changes, and what it does not

Changes: `RealizedPlan` phases gain named relation inputs (the DAG edges); the planner emits the topology it already computes (the algebra path's construction stops being discarded at decompile); the executor's subject-discovery/bridging/absorption machinery retires per fragment conversion; the strict phase contracts move from per-execution checking to one plan-construction validation.

Unchanged: the query language (the IR is Datalog — verified above); the environment rulings; the Relation exchange; the operator surface (`RELATION_ALGEBRA_REUNIFICATION.md` — fragments consume the same algebra); storage.

## Open questions for ratification

1. Fragment granularity policy: where exactly the planner draws fragment boundaries (proposed: at compound-clause scope boundaries; coarse in between).
2. The dual-mode matrix's role afterward: with topology constructed rather than improvised, the baseline mode's value shifts toward a reference evaluator against constructed plans (`docs/wip/CORRECTNESS_MEASUREMENT.md`'s tier, if ratified).
3. Sequencing against the reunification arc (item 20): the fragments consume the operator surface, so the reunification's single-homed operators land first.
