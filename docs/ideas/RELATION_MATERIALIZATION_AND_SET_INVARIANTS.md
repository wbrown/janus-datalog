# Relation Materialization and Set Invariants

**Status:** Active design and optimization record  
**Last reviewed:** 2026-07-13

This document records the verified laws, audit findings, completed changes, and
remaining work around Relation replay, physical realization, set semantics, and
tuple deduplication.

It is intentionally stricter than a list of possible optimizations. Every
proposed removal of realization or deduplication must identify the relational
proof that makes the work unnecessary and the iterator/error contracts that must
remain intact.

## Core Laws

### A Relation is a set

`Relation` has set semantics. A complete tuple appears at most once.

Physical operator state and raw tuple streams may contain repeated tuples while
an operation is being evaluated. Before the result is exposed as a `Relation`,
the producing operation must establish set semantics.

This means:

- Selection preserves setness.
- Deterministic extension preserves setness because the complete source tuple
  remains embedded in the output.
- Sorting and limiting preserve setness.
- Semi-join and anti-join preserve the left set.
- Natural join and Cartesian product of sets produce sets.
- Grouped aggregation produces at most one tuple per group.
- Projection can merge distinct source tuples and must restore set semantics
  unless a retained key proves injectivity.
- Union can merge equal tuples from different branches and must restore set
  semantics.

Setness is not a `RelationProperties` flag. It is part of the `Relation`
contract. Candidate keys are stronger proofs used to show that operations such
as projection remain injective.

### Materialize guarantees replayability

`Materialize()` guarantees that the returned Relation can be iterated again. It
does not require eager conversion when replay is already available.

- `MaterializedRelation` returns itself.
- `StreamingRelation` enables lazy caching and returns itself.
- `LazySeqRelation` already has shared lazy replay and returns itself.
- Complete-data operations such as full sorting may still realize all tuples
  explicitly.

Replayability and eager realization are separate concerns.

### Value-domain validation is independent from deduplication

Relations contain only Datalog values. Raw/public tuple-entry boundaries validate
that closed domain. Deduplication also hashes values and therefore used to enforce
the domain accidentally, but derived proven-set construction must not repeat a
full validation traversal over values already carried by a Relation.

Internal set-preserving operators trust their input Relation. Arbitrary raw tuple
constructors validate and, when necessary, deduplicate before exposing a
Relation.

## Why Realization Exists

### Semantically required

| Operation | Required state | Reason |
|---|---|---|
| Hash join | Build-side hash state | Build tuples must survive until probing finishes |
| Unordered grouped aggregation | One state per group | Any later input tuple may update an earlier group |
| Full sort | Complete tuple sequence | Requested order cannot be emitted before all comparisons are known |
| Top-N | At most N tuples | Full input must be considered without proven source order |
| Fallback branch cache | Keyed branch tuples | One branch execution is probed for many outer tuples |
| CRDT resolution | Per-value or per-element state | Historical operations must resolve before Relation creation |
| Public tuple collection | Complete output slice | The caller explicitly requested all tuples |

These are not targets for broad removal. They may still support narrower
improvements such as better key proofs, bounded state, or ordered input.

### Required for replay under current algorithms

| Site | Why replay is required | Current boundary |
|---|---|---|
| Pattern binding | Matcher consumes bindings; collapse later joins them | Selected binding relation must be replayable |
| Join-key narrowing | One pass extracts keys; another drives fallback tuples | Outer relation is realized only for narrowable branches |
| Non-final phase handoff | Later matching and collapse may both consume output | Exact phase result becomes replayable |
| Correlated subquery sources | Input combinations are extracted; outer query later reuses sources | Source groups use lazy caches |
| Nested-loop product inner operands | Inner operands are reopened for each outer tuple | Non-left operands use lazy replay |

The proof obligation is not “this code used Materialize before.” It is “this
algorithm performs more than one traversal of this exact relation.”

### Conservative or avoidable

The audits found these high-confidence cases:

1. Set-preserving operators feeding deduplicating constructors.
2. Explicit typed seen sets followed by a second constructor dedup pass.
3. Single-use products realized immediately before another typed dedup pass.
4. Single outer relation groups eagerly realized before fallback execution.
5. Lazy sequence relations drained merely to satisfy replayability.
6. Annotation code invoking `Size()` on wrappers whose size computation realizes
   data.
7. All relations sharing a pattern symbol made replayable even when the matcher
   consumes only one selected binding relation.
8. Relation-input results fully retained before global finalization.

## Deduplication Rules

### Required

Deduplication remains required when an operation can map distinct tuples to one
complete output tuple:

- Projection without a retained key.
- Union across overlapping branches.
- Correlated union when branches can overlap.
- Final union across RelationInput executions when input bindings are absent
  from the output.
- Input-combination extraction when requested input symbols do not contain a
  source key.
- CRDT operation resolution before Relation creation.

### Not required

No new deduplication is required for:

- Selection or filtering.
- Deterministic fresh-symbol extension.
- Sorting.
- Limiting or Top-N selection.
- Semi-join or anti-join.
- Natural join and Cartesian product when both inputs honor the Relation set
  contract.
- One-result-per-group aggregation output.
- Realization or replay of an existing Relation.

### Candidate keys

Candidate keys prove more than setness. Current proof origins include:

- Current/as-of CardinalityOne scans with proven entity uniqueness.
- Validated AVET scans.
- Group symbols after grouped aggregation.
- Complete output symbols after explicit typed deduplication.
- Preserved outer keys through safe OR/fallback shapes.
- Join keys derived by one-to-one or many-to-one natural joins.

Proofs are consumed by:

- Projection dedup elision.
- Unique hash-build specialization.
- Hash-join result dedup elision.
- Semi/anti join result construction.
- OR/fallback dedup decisions.

Relation bindings apply positional ρ-renaming to ordering and key symbols.

## Completed Changes

### Same-entity constant constraints

When an entity is already bound, proven CardinalityOne literal constraints use
cache-backed lookup and typed equality rather than storage match plus hash join.

Focused 1K/10K results:

- 21.9–23.2% faster.
- 35.3–38.8% less memory.
- 42.3–43.4% fewer allocations.

Complex checkpoint:

- 11.1% faster.
- 21.8% less memory.
- 23.2% fewer allocations.

### Correlated OR outer replacement

Correlated OR/fallback results already contain their selected outer tuples.
QueryExecutor now replaces consumed outer groups instead of appending the result
and joining it back to the same data.

Complex checkpoint:

- 11.3% faster.
- 8.3% less memory.
- 10.6% fewer allocations.

Outer iterator, branch iterator, cache-build, and close failures now propagate
instead of being interpreted as a missing branch.

### Lazy outer selection

A single selected outer relation remains lazy. Four of five complex-query
fallback chains stay streaming; one still realizes for join-key narrowing.

Full-result performance is neutral. This is retained as replayability groundwork,
not a performance claim.

### Correlated-subquery product streaming

Correlated-subquery source relations remain lazily replayable. Their combined
product is single-use and now streams directly into typed input-combination
deduplication.

The first benchmark used repeated one-symbol inputs and was rejected because it
violated the Relation set contract. The corrected benchmark uses unique complete
tuples with repeated projected input values.

| Product tuples | Time improvement | Memory improvement | Allocation improvement |
|---:|---:|---:|---:|
| 10,000 | 44.2% | 37.0% | 14.3% |
| 100,000 | 39.6% | 38.7% | 14.3% |

The production complex checkpoint has no multi-group correlated input product
and is unchanged by this optimization.

### Proven-set construction

An internal constructor wraps tuples already proven to form a valid Relation set
without traversing them. Raw tuple constructors still validate and deduplicate.

Migrated paths include:

- Selection and predicate filtering.
- Deterministic extension.
- Key-preserving materialized projection.
- Sorting, Top-N, and limits.
- Phase realization.
- Same-entity attribute fusion.
- Join-key extraction.
- Union and fallback realization.
- Grouped aggregation output.
- Lazy sequence and prepended replay.

Focused constructor measurement:

| Tuples | Deduplicating constructor | Proven-set constructor |
|---:|---:|---:|
| 10,000 | 474.4 µs, 1.38 MiB, 10.0K allocations | 16.5 ns, 8 B, 1 allocation |
| 100,000 | 4.260 ms, 12.45 MiB, 100.3K allocations | 16.8 ns, 8 B, 1 allocation |

Complex checkpoint:

- 5.1% faster.
- 15.6% less memory.
- 8.6% fewer allocations.

### Lazy sequence replayability

`LazySeqRelation.Materialize()` returns itself without advancing the source.
Projection, predicate filtering, and deterministic extension construct lazy
derived relations. Random access realizes only the required prefix. Full sorting
retains explicit complete realization.

## Correctness Hardening Completed

The audits exposed correctness defects that were fixed before broadening lazy
execution:

- Early-close streaming caches now report and replay an incomplete-realization
  error instead of publishing a truncated cache.
- Predicate evaluation errors propagate instead of acting like a false result.
- Hash-join build and probe close errors propagate.
- Streaming transforms use relation-owned iterators and preserve cache/single-use
  behavior.
- Unknown-size scalar, tuple, relation, and collection inputs bind correctly.
- `StreamingRelation.Get` performs actual random-access realization.
- OR/fallback outer and branch/cache errors propagate.
- A shuffled cache test now uses canonical identity bytes instead of an
  interning-order-dependent display string.

## Remaining Materialization Opportunities

### 1. Lazy same-entity fusion output

Attribute fetch and constraint fusion still builds a complete output slice.
Each tuple is independent and properties are preserved, so a lazy fused iterator
is plausible. This is the strongest remaining eager full-input boundary in the
current profile.

Required tests:

- Attribute bundle and literal constraint differentials.
- Early close and satisfied-order limit behavior.
- Iterator and lookup errors.
- Tuple workspace copying.
- Property preservation.

### 2. Matcher-selected binding replay only

Pattern execution currently marks every relation sharing any pattern symbol
replayable. Storage matching selects one binding relation; other shared groups
are consumed later only by collapse.

A safe change requires the matcher to report which relation it consumed or the
executor to select it before calling the matcher.

### 3. Streaming RelationInput result union

Parallel and sequential RelationInput execution collect all per-input results
before global ordering and limiting. A streaming typed-set union could feed
Top-N or an already-satisfied limit directly.

Required constraints:

- Deterministic error priority.
- Worker cancellation on early close.
- Tuple workspace copying.
- Typed set semantics across input executions.
- Global, not per-input, finalization.

### 4. Property-aware constructor migration

Many production calls still use property-less materialized constructors. They
must be classified as:

- Raw arbitrary tuple input: validate and deduplicate.
- Empty/singleton: set by construction.
- Existing Relation realization: preserve set and properties.
- Operator output with a proof: use proven-set construction.
- Deliberately non-normalized physical state: do not expose as Relation until
  normalized.

### 5. Annotation realization hazards

Some annotation paths call `Size()` on wrapper relations whose implementation
realizes data. Observability must use non-consuming metadata or report unknown;
enabling annotations must not change execution timing or memory behavior.

### 6. Ordered group-at-a-time aggregation

Unordered grouping requires one state per group. When input ordering contains the
complete grouping prefix, completed groups could be emitted as the key changes.
The existing unordered path remains necessary.

## Materialization That Should Remain

Do not broadly remove:

- Hash build state.
- Fallback branch indexes.
- Join-key-narrowing outer replay.
- Unordered grouping state.
- Full sort without satisfied ordering.
- Bounded Top-N state.
- RelationInput final set normalization.
- Projection deduplication without an injectivity proof.
- Union deduplication across potentially overlapping branches.
- CRDT operation-resolution state.
- Public tuple collection requested by the caller.

## Mandatory Gates

Every realization or deduplication change must include:

1. Exact semantic differential against the previous path.
2. Empty, singleton, repeated projected value, and large-input cases.
3. Iterator error and close-error propagation.
4. Workspace-reusing iterator coverage.
5. Candidate-key and ordering assertions.
6. An annotation proving the intended physical path.
7. A focused benchmark using valid Relation inputs.
8. `BenchmarkComplexQueryCheckpoint`.
9. Full, race, fixed-seed, and repeated shuffled test gates.

Benchmarks that violate Relation set semantics are invalid and must be rejected,
even if they produce a convenient result.

## Code and Test Map

Core contracts:

- `datalog/executor/relation.go`
- `datalog/executor/relation_properties.go`
- `datalog/executor/lazy_seq.go`
- `datalog/executor/lazy_seq_relation.go`

Query execution:

- `datalog/executor/query_executor.go`
- `datalog/executor/subquery.go`
- `datalog/executor/or_fallback_relation.go`
- `datalog/executor/executor.go`

Storage integration:

- `datalog/storage/matcher_relations.go`
- `datalog/storage/relation_properties.go`
- `datalog/storage/cache.go`

Focused regression and benchmark coverage:

- `datalog/executor/iterator_contract_hardening_test.go`
- `datalog/executor/bind_query_inputs_streaming_test.go`
- `datalog/executor/or_outer_replacement_test.go`
- `datalog/executor/or_outer_selection_test.go`
- `datalog/executor/subquery_input_product_test.go`
- `datalog/executor/subquery_input_product_benchmark_test.go`
- `datalog/executor/relation_set_construction_benchmark_test.go`
- `datalog/storage/optimization_matrix_test.go`

