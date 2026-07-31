# Correctness Measurement — the oracle hierarchy

**Status**: Proposed 2026-07-20; pending owner ruling. Companion to `docs/wip/OPTIMIZER_MODE_MATRIX.md`, which built tier 2 of this hierarchy and whose migration campaign supplied the evidence for the rest.

## The problem this solves

A test measures correctness exactly to the degree that its expected value was derived independently of the system under test. "Tests pass" is an indicator, not an objective: a green leg shipped nil-corrupted rows for the entire life of the algebra path's or-default machinery (`docs/bugs/resolved/BUG_BASELINE_ORDEFAULT_SUBQUERY_NIL_AGGREGATE.md`), while the red leg beside it was the only honest signal. The decorrelation pure-vs-grouped category error passed value assertions for months because small fixtures produced accidentally-correct numbers. The bridge's single-branch or-join and or-default-first rejections survived until a migration *happened* to execute those shapes on the algebra path.

Each of those is a measurement failure with a name: a dependent oracle, an under-powered fixture, an unenumerated input domain. This document defines the measurement system that makes them structural rather than accidental catches.

## What is being measured

For every query the language accepts, over any database state:

1. **Result correctness**: the result set matches the language's semantics.
2. **Path equivalence**: every execution path (baseline planner, algebra optimizer, and any future path) produces the same result set and the same error/no-error outcome.
3. **Transformation fidelity**: each optimization performed is the transformation it claims to be, applied only where its preconditions hold.

**Outcome equivalence is defined as**: result sets equal under `datalog.ValuesEqual` (order-insensitive unless `:order-by` is present), or both paths erroring — error *presence* is the contract; message text and detection stage are not (see `BUG_SUBQUERY_BINDING_ARITY_VALIDATED_AT_DIFFERENT_LAYERS` for where stage divergence is itself a defect).

## The tiers

Ranked by independence of the oracle. Every test should be attributable to a tier; the tiers are complementary, not alternatives.

### Tier 1 — hand-computed expected values (exists; the anchor)

Expected values derived by human arithmetic on the fixture (`int64(300)` because 100+200). Fully independent; does not scale; small fixtures hide category errors. Every feature keeps a bed of these. They also bootstrap tier 4: the reference evaluator is itself pinned by tier-1 cases before anything trusts it.

### Tier 2 — two-implementation differential (exists: the optimizer mode matrix)

Baseline ≡ algebra on ~550 tests across eight packages. Measures equivalence, not correctness — blind to identical wrongness on both paths. Seven bugs in one campaign is its track record; its axis convention (`optimizerModes`, per-package copies, structure pins declare their mode) is the template every later tier reuses.

### Tier 3 — structural assertion via annotations (exists)

Asserting the transformation, not the result: decorrelated execution counts, derived relation keys, branch narrowing, fusion events. This is "optimizations must preserve semantics; test structure, not just outcomes" mechanized. Blind to whether the transformed plan computes the right rows — that is tiers 1/2/4's job; tier 3 exists because value equality cannot see a category error that happens to produce the right numbers on this fixture.

### Tier 4 — reference evaluator (missing; the real oracle)

A third implementation that is a direct transcription of the language semantics: nested-loop pattern scans over MemoryStore, materialize everything, hash nothing, natural join for shared symbols, anti-join for NOT, union for or, short-circuit branch evaluation for or-default, fold groups for aggregates, per-combination execution for subqueries. No planner, no phases, no streaming, no caches. Slow and correct-by-inspection: each clause form maps to its textbook operator, in one place, readable in a sitting.

The measurable statement becomes **baseline ≡ algebra ≡ reference**: a surviving shared bug now requires all three implementations to agree on the same mistake, and the third is simple enough to audit by reading. This is the methodology of SQLite's logic tests and the NoREC/TLP line of query-engine fuzzing.

Constraints:

- The reference imports core types, the parser, and MemoryStore — never `planner`, `algebra`, or `executor`. (Consequence: the parser is shared by all paths, so parse-level semantics remain tier-1/tier-5 territory; the reference measures evaluation, not parsing.)
- Closed clause taxonomy with a loud default: a clause form the reference does not implement is a loud error, never a skip — a new language feature is not done until the reference evaluates it or explicitly refuses it.
- Bootstrap: the reference passes the existing tier-1 corpus before it referees anything.

### Tier 5 — properties and metamorphic relations (missing; oracle-free)

Statements that must hold without knowing the answer. Each initial property targets a bug class this repository has actually shipped:

- **Clause-order invariance**: for any query, any permutation of `:where` clauses produces the same outcome. This is `BUG_ALGEBRA_BRIDGE_COMPILES_IN_SOURCE_ORDER.md` stated as a property; it is also the cheapest item in this document.
- **Partition consistency** (TLP): for any query Q with result R and any applicable predicate p, R equals the union of Q+`[(p)]` and Q+`(not [(p)])`. Catches row loss in filters and joins — the correlated or-join drop class.
- **Aggregate homomorphism**: an aggregate over a group equals the aggregate of aggregates over any partition of the group (sum/count/min/max each with their fold). The decorrelation family's natural check.
- **Irrelevant-datom monotonicity**: adding a datom matched by no pattern in Q leaves Q's result unchanged.
- **Write idempotence**: asserting an existing datom is a no-op for every query (set semantics; CRDT add-wins).
- **Temporal consistency**: `AsOf(latest)` ≡ the current view; History with per-(E,A) CRDT resolution applied ≡ the current view (Pillar 1 as a property).

Properties run each query shape through all execution paths (they compose with tiers 2/4 rather than replacing them).

### Tier 6 — grammar-driven generation (missing; the enumeration)

A generator over the clause grammar — patterns × NOT/not-join × or/or-join/or-default × subqueries with every binding form × every `:in` form × aggregates × expressions × order-by/limit — deliberately including the degenerate corners: single branches, compound-first clauses, empty-provides shapes, predicate-only correlation. Generated queries run over small value-domain-complete fixtures and are checked by tiers 2+4 and applicable tier-5 properties.

The two batch-A bugs (single-branch or-join in NOT; or-default as first clause) are exactly the corners a grammar walk reaches in minutes and hand-written suites reach by accident. Determinism: fixed seeds (the repo already bans wall-clock/randomness in workflows for replay; the same applies here), bounded case counts per CI run, and a shrinker so every failure lands as a minimal reproducer. Protocol: a shrunk divergence is promoted to a permanent pinned test plus a `docs/bugs/` ledger entry — generation finds, pins remember.

## Staged plan

1. **S1 — clause-order invariance harness.** Pure test-side; applies the permutation property to the existing query corpus (the matrix packages already enumerate the queries). Known-red against the source-order bug, so it lands with that ledger entry as its anchor and the algebra leg's failures as its initial catch. Days of work.
2. **S2 — reference evaluator + three-way differential harness.** New package (proposed: `datalog/reference`), bootstrap against the tier-1 corpus, then a harness that runs matrix-migrated fixtures through all three evaluators. The harness reuses the `optimizerModes` convention, extended by a reference leg.
3. **S3 — grammar generator** feeding the three-way harness, seeded, budgeted, with shrinking and the promotion protocol.
4. **S4 — metamorphic suites** (partition consistency, aggregate homomorphism, monotonicity, idempotence, temporal), each as an ordinary test file over the generator's fixtures.

Every stage lands as ordinary `go test` inside the existing gate; the wasm leg inherits all of it unchanged, as the matrix did.

## Costs and limits

- The reference evaluator is a permanent maintenance obligation: every language addition implements its reference semantics or is loudly refused there. This is a feature — it forces each new construct to state its semantics executably — but it is real ongoing cost.
- Generated-test time must be budgeted (seeded case counts per run); the full grammar is never "done," only sampled deeper over time.
- Tier 4 cannot referee the parser (shared), result presentation (Pull rendering is boundary formatting), or storage encodings (MemoryStore is shared with the system under test at the bottom). Those remain tier-1/tier-5 territory, stated here so the blind spots are chosen rather than discovered.
- Performance is out of scope by definition: these tiers measure the correctness-equivalence class within which benchmarks are then meaningful (`PERFORMANCE_STATUS.md` owns the rest).

## Interaction with existing regimes

- The optimizer mode matrix is tier 2 of this hierarchy; its conventions (per-package axis, structure-pin exemptions, divergence protocol: leave red, ledger, never weaken) carry over to the three-way harness verbatim.
- The annotation system is tier 3's instrument; new optimizations must keep emitting the events their structural pins assert.
- The subquery-uniformity and Datomic-parity rulings (`TODO.md`) define the grammar tier 6 enumerates; the collection-aggregate substrate and `[?coll ...]` binding land with reference implementations per the tier-4 rule.
- `CLAUDE.md`'s testing guidance already names differential testing against reference implementations and randomized query fuzzing; this document is the concrete design for both.
